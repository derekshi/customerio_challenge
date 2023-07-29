package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"

	"customerio_challenge/stream"
)

const inputFile = "./data/messages.2.data"
const outputFile = "./data/summarya.txt"
const verifyFile = "./data/verify.2.csv"

type UserEventCount map[string]int

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

func (s *Summary) OutputLine() string {
	line := s.id + ","

	// sort by attribute keys
	attrKeys := make([]string, 0, len(s.attributes))
	for key := range s.attributes {
		attrKeys = append(attrKeys, key)
	}

	sort.Strings(attrKeys)

	for _, key := range attrKeys {
		line += key + "=" + s.attributes[key] + ","
	}

	//sort by event keys
	mapKeys := make([]string, 0, len(s.eventSummary))
	for key := range s.eventSummary {
		mapKeys = append(mapKeys, key)
	}

	sort.Strings(mapKeys)
	for _, key := range mapKeys {
		line += key + "=" + strconv.Itoa(s.eventSummary[key]) + ","
	}

	return line[:len(line)-1] // remove ending comma
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	input, err := os.Open(inputFile)
	if err != nil {
		log.Fatal(err)
	}

	ch, err := stream.Process(ctx, input, 0)
	if err != nil {
		log.Fatal(err)
	}

	if err := ctx.Err(); err != nil {
		log.Fatal(err)
	}

	//map store for user attributes data
	users := make(map[string]*UserAttrs)

	//map store for event data counts
	events := make(map[string]UserEventCount)

	//event ids cache for avoiding duplidates
	eids := make(map[string]bool)

	for rec := range ch {
		switch rec.Type {
		case "attributes":
			handleAttrs(rec, users)
		case "event":
			handleEvent(rec, events, eids)
		default:
			fmt.Printf("Unknown record type: %s. Skipping.\n", rec.Type)
		}
	}

	summaries := consolidate(users, events)

	if err := writeSummary(summaries); err != nil {
		log.Fatal(err)
	}

	//validate
	if err := validate(outputFile, verifyFile); err != nil {
		log.Fatal(err)
	}

}

func handleEvent(rec *stream.Record, events map[string]UserEventCount, eids map[string]bool) {
	// handle duplicate event ids
	if _, exists := eids[rec.ID]; exists {
		fmt.Printf("duplicate event id: %s. Skipping...\n", rec.ID)
		return
	} else {
		eids[rec.ID] = true
	}

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
}

func handleAttrs(rec *stream.Record, users map[string]*UserAttrs) {
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
}

// merge user attributes with user events
func consolidate(users map[string]*UserAttrs, events map[string]UserEventCount) []Summary {
	summaries := []Summary{}
	for userId, attrs := range users {
		if evt, ok := events[userId]; ok {
			summaries = append(summaries, *NewSummary(userId, attrs.attributes, evt))
		} else {
			summaries = append(summaries, *EmptyEventSummary(userId, attrs.attributes))
		}
	}
	// sort the summary by userId
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].id < summaries[j].id
	})

	return summaries
}

// write summaries to output file line by line
func writeSummary(summries []Summary) error {
	file, err := os.Create(outputFile)
	if err != nil {
		fmt.Println("Error creating file:", err)
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	for _, s := range summries {
		_, err := writer.WriteString(s.OutputLine() + "\n")
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return err
		}
	}

	writer.Flush()
	fmt.Println("Summaries created.")
	return nil
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
