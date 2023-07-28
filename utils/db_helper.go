package utils

import (
	"encoding/binary"
	"fmt"
	"strings"

	bolt "go.etcd.io/bbolt"
)

const ProcessBucketName = "ProcessBucket"
const processKey = "readOffset"
const dbBucketName = "UserBucket"
const attrBucketName = "AttrBucket"

// Helper function to store an integer in the bucket.
func PutInt(bucket *bolt.Bucket, key string, value int64) error {
	buf := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(buf, uint64(value))
	return bucket.Put([]byte(key), buf)
}

// Helper function to retrieve an integer from the bucket.
func GetInt(bucket *bolt.Bucket, key string) (int64, error) {
	value := bucket.Get([]byte(key))
	if value == nil {
		return 0, fmt.Errorf("key not found: %s", key)
	}

	return int64(binary.BigEndian.Uint64(value)), nil
}

func RetrieveProcess(db *bolt.DB) int64 {
	offset := int64(0)
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ProcessBucketName))
		if b != nil {
			res, e := GetInt(b, processKey)
			if e != nil {
				fmt.Println("Failed to get process from db. Process start with beginning.")
			} else {
				offset = res
			}
		} else {
			fmt.Println("Process start with beginning.")
		}

		return nil
	})

	return offset
}

func SaveProcess(offset int64, db *bolt.DB) error {
	db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(ProcessBucketName))
		if err != nil {
			fmt.Println("Fail to create process bucket.")
			return err
		}

		err = PutInt(b, processKey, offset)
		if err != nil {
			fmt.Println("Fail to save process status.")
			return err
		}

		return nil
	})
	fmt.Printf("Process offset %v saved.\n", offset)
	return nil
}

func SaveProcessedEvents(eids map[string]bool, db *bolt.DB) error {
	keys := make([]string, 0, len(eids))
	for k := range eids {
		keys = append(keys, k)
	}

	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(ProcessBucketName))
		if err != nil {
			return err
		}
		err = b.Put([]byte("EventIds"), []byte(strings.Join(keys, ",")))
		if err != nil {
			fmt.Println("Failed to saving processed events.")
			return err
		}

		return nil
	})

}

func ReadProcessedEvents(db *bolt.DB) (map[string]bool, error) {
	result := make(map[string]bool)

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(ProcessBucketName))
		if b == nil {
			return fmt.Errorf("process bucket not found")
		}

		res := b.Get([]byte("EventIds"))
		if res != nil {
			ids := strings.Split(string(res), ",")
			for _, k := range ids {
				result[k] = true
			}
		}

		return nil
	})

	return result, nil
}

func ReadUserAttrs(db *bolt.DB) (map[string]int64, error) {
	result := make(map[string]int64)

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(dbBucketName))
		if b == nil {
			return fmt.Errorf("user bucket not found")
		}

		b.ForEach(func(k, v []byte) error {
			//iterate through each userId
			userId := string(k)

			// skip nested bucket name
			if userId != attrBucketName {
				prevUpdated, _ := GetInt(b, userId)
				result[userId] = prevUpdated
			}

			return nil
		})

		return nil
	})

	return result, nil
}
