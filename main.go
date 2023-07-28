package main

import (
	"bufio"
	"bytes"
	"context"
	"customerio_challenge/stream"
	"customerio_challenge/utils"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof" // Import this package for pprof
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

const inputFile = "./data/messages.2.data"
const verifyFile = "./data/verify.2.csv"

const outputFile = "./output/summaryb.txt"
const dbFileName = "customerio.db"
const dbBucketName = "UserBucket"
const attrBucketName = "AttrBucket"

type UserEventCount map[string]int64

// final data structure for summary
type Summary struct {
	id           string
	attributes   map[string]string
	eventSummary UserEventCount
}

type UserAttrs struct {
	lastUpdated int64
	attributes  map[string]string
}

func NewUserAttrs() *UserAttrs {
	return &UserAttrs{
		attributes: make(map[string]string),
	}
}

// this is for handling user without any events
func EmptyEventSummary(userId string, attrs map[string]string) *Summary {
	return &Summary{
		id:           userId,
		attributes:   attrs,
		eventSummary: UserEventCount{},
	}
}

func NewSummary(userId string, attrs map[string]string, events UserEventCount) *Summary {
	return &Summary{
		id:           userId,
		attributes:   attrs,
		eventSummary: events,
	}
}

// this data is used for batch writing
type BatchData struct {
	attrs  map[string]*UserAttrs
	events map[string]UserEventCount
}

var shouldResume = flag.Bool("resume", false, "Add resume parameter to resume the process interrupted before.")

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		cancel()
	}()

	// Handling resume process
	if !*shouldResume {
		// remove old db files
		err := os.Remove(dbFileName)
		if err != nil {
			fmt.Println("failed to clear up db file")
		}
	}

	// init local file db to save intermediary data or interrupted process
	db, err := bolt.Open(dbFileName, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	// init cache objects
	users := make(map[string]*UserAttrs)

	// //map store for event data counts
	events := make(map[string]UserEventCount)

	// //event ids cache for avoiding duplidates
	eids := make(map[string]bool)
	from := int64(0)

	// if resume previous process, we load cache data from db
	if *shouldResume {
		from = utils.RetrieveProcess(db)

		// //event ids cache for avoiding duplidates
		eids, _ = utils.ReadProcessedEvents(db)

		lastUpdates, _ := utils.ReadUserAttrs(db)
		for userId, lastUpdated := range lastUpdates {
			users[userId] = &UserAttrs{
				lastUpdated: lastUpdated,
				attributes:  make(map[string]string),
			}
		}
	}

	input, err := os.Open(inputFile)
	if err != nil {
		log.Fatal(err)
	}

	ch, err := stream.Process(ctx, input, from)
	if err != nil {
		log.Fatal(err)
	}

	if err := ctx.Err(); err != nil {
		log.Fatal(err)
	}

	//create db batch channel for separate writing data process
	dbBatchChan := make(chan BatchData)
	var wg sync.WaitGroup
	wg.Add(1)
	go saveBatch(&wg, dbBatchChan, db)

	// manual counter for streaming records
	n := 1
	var last *stream.Record
	for rec := range ch {
		//debug
		if n == 1 {
			fmt.Printf("processing rec id: %v, offset: %d\n", rec.ID, rec.Position) //10668513
		}
		// handle duplicate event ids
		if _, exists := eids[rec.ID]; exists {
			// fmt.Printf("duplicate event id: %s. Skipping...\n", rec.ID)
			continue
		} else {
			eids[rec.ID] = true
		}

		handleStreamData(rec, users, events)
		last = rec

		// enable flushing memory data to disk
		if n >= 5000 {
			dbBatchChan <- BatchData{
				attrs:  users,
				events: events,
			}

			// purge memory
			p := make(map[string]*UserAttrs)
			for k, v := range users {
				//clean attr data but keep last updated
				attrs := NewUserAttrs()
				attrs.lastUpdated = v.lastUpdated
				p[k] = attrs
			}
			users = p
			events = make(map[string]UserEventCount)
			n = -1
		}

		n++
	}

	//write remaining data from memory into db
	dbBatchChan <- BatchData{
		attrs:  users,
		events: events,
	}
	close(dbBatchChan)

	// wait for the batch saving to complete
	wg.Wait()

	//Save last record offset position for resume
	utils.SaveProcess(last.Position, db)
	utils.SaveProcessedEvents(eids, db)

	// create channel for incremental writing data to final outputs
	writeChann := make(chan string)
	go readBatch(db, writeChann)

	// write summary
	file, err := os.Create(outputFile)
	if err != nil {
		fmt.Println("Error creating file:", err)
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	for line := range writeChann {
		_, err := writer.WriteString(line + "\n")
		if err != nil {
			fmt.Println("Error writing to file:", err)
		}
	}

	writer.Flush()
	fmt.Println("Summeries created.")

	//validate
	if err := validate(outputFile, verifyFile); err != nil {
		log.Fatal(err)
	}

}

func handleStreamData(rec *stream.Record, users map[string]*UserAttrs, events map[string]UserEventCount) {
	if rec.Type == "event" {
		if evt, ok := events[rec.UserID]; ok {
			if val, ok := evt[rec.Name]; ok {
				evt[rec.Name] = val + 1
			} else {
				evt[rec.Name] = 1
			}
		} else {
			events[rec.UserID] = UserEventCount{
				rec.Name: 1,
			}
		}
	} else if rec.Type == "attributes" {
		var userAttrs *UserAttrs
		if attrs, ok := users[rec.UserID]; ok {
			if attrs.lastUpdated > rec.Timestamp {
				return
			}
			// Only update attributes if it's newer record
			userAttrs = attrs
		} else {
			userAttrs = NewUserAttrs()
		}

		// update attribute values
		for key, val := range rec.Data {
			userAttrs.attributes[key] = val
		}
		userAttrs.lastUpdated = rec.Timestamp
		users[rec.UserID] = userAttrs
	} else {
		fmt.Printf("Unknown event data %s. Skipping \n", rec.Type)
	}

}

func saveBatch(wg *sync.WaitGroup, batchChann <-chan BatchData, db *bolt.DB) {
	defer wg.Done()
	for data := range batchChann {
		fmt.Printf("%s: Writing data to DB\n", time.Now().Format("2006-01-02 15:04:05"))

		err := db.Update(func(tx *bolt.Tx) error {
			bucket, e := tx.CreateBucketIfNotExists([]byte(dbBucketName))
			if e != nil {
				fmt.Println("creating user bucket failed. DB writes aborted")
				return e
			}

			// write attr
			attrBucket, e := bucket.CreateBucketIfNotExists([]byte(attrBucketName))
			if e != nil {
				fmt.Println("creating user attributes bucket failed. DB writes aborted")
				return e
			}
			for userId, attr := range data.attrs {
				prevUpdated, _ := utils.GetInt(bucket, userId)

				// only update attr if last updated is newer
				if attr.lastUpdated >= prevUpdated {
					prefix := userId + ":Attrs:"

					for k, v := range attr.attributes {
						attrBucket.Put([]byte(prefix+k), []byte(v))
					}
					// Convert lastUpdated to byte slice
					e = utils.PutInt(bucket, userId, attr.lastUpdated)
					if e != nil {
						fmt.Println("error writing user attr data.", e.Error())
						return e
					}
				}
			}

			// write events
			for userId, events := range data.events {
				prefix := userId + ":Events:"

				for k, v := range events {
					oldCount, _ := utils.GetInt(attrBucket, prefix+k)
					e = utils.PutInt(attrBucket, prefix+k, oldCount+v)
					if e != nil {
						fmt.Println("error writing user events data.", e.Error())
						return e
					}
				}
			}

			return nil
		})

		if err != nil {
			fmt.Println("db operation has failed.", err.Error())
		}
	}
	fmt.Println("saveBatch exit")

}

// Read data for writing out summary
func readBatch(db *bolt.DB, writeChan chan<- string) {
	fmt.Println("Reading data from DB")
	defer close(writeChan)

	db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(dbBucketName))
		attrBucket := bucket.Bucket([]byte(attrBucketName))
		bucket.ForEach(func(k, v []byte) error {
			//iterate through each userId
			userId := string(k)

			// skip nested bucket name
			if userId != attrBucketName {
				attrPrefix := []byte(userId + ":Attrs:")
				line := userId + ","

				c := attrBucket.Cursor()
				for key, val := c.Seek(attrPrefix); key != nil && bytes.HasPrefix(key, attrPrefix); key, val = c.Next() {
					attrName := strings.Split(string(key), ":")[2]
					line += attrName + "=" + string(val) + ","
				}

				evtPrefix := []byte(userId + ":Events:")
				for key, val := c.Seek(evtPrefix); key != nil && bytes.HasPrefix(key, evtPrefix); key, val = c.Next() {
					evtName := strings.Split(string(key), ":")[2]
					line += evtName + "=" + strconv.Itoa(int(binary.BigEndian.Uint64(val))) + ","
				}

				writeChan <- line[:len(line)-1]
			}

			return nil
		})
		return nil
	})
}

// Quick validation of expected and received input.
func validate(have, want string) error {
	f1, err := os.Open(have)
	if err != nil {
		return err
	}
	defer f1.Close()

	f2, err := os.Open(want)
	if err != nil {
		return err
	}
	defer f2.Close()

	s1 := bufio.NewScanner(f1)
	s2 := bufio.NewScanner(f2)
	for s1.Scan() {
		if !s2.Scan() {
			return fmt.Errorf("want: insufficient data")
		}
		t1 := s1.Text()
		t2 := s2.Text()
		if t1 != t2 {
			return fmt.Errorf("have/want: difference\n%s\n%s", t1, t2)
		}
	}
	if s2.Scan() {
		return fmt.Errorf("have: insufficient data")
	}
	if err := s1.Err(); err != nil {
		return err
	}
	if err := s2.Err(); err != nil {
		return err
	}
	return nil
}
