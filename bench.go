package main

import (
	"fmt"
	"log"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

func generateData(n int) ([][]byte, [][]byte) {
	keys := make([][]byte, n)
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key_%d", i))
		values[i] = []byte(fmt.Sprintf("value_%d", i))
	}
	return keys, values
}

func testRocksDBBatchInsert(rdb *RocksDB, keys, values [][]byte) time.Duration {
	startTime := time.Now()

	batch := NewBatch(rdb)
	for i, key := range keys {
		if err := batch.Put(key, values[i]); err != nil {
			log.Fatalf("Error putting key-value pair: %v", err)
		}
	}

	if err := batch.Write(); err != nil {
		log.Fatalf("Error writing batch: %v", err)
	}

	return time.Since(startTime)
}

func testLevelDBBatchInsert(ldb *leveldb.DB, keys, values [][]byte) time.Duration {
	startTime := time.Now()

	batch := new(leveldb.Batch)
	for i, key := range keys {
		batch.Put(key, values[i])
	}

	if err := ldb.Write(batch, nil); err != nil {
		log.Fatalf("Error writing batch: %v", err)
	}

	return time.Since(startTime)
}

func testRocksDBBatchInsertWithBatchSize(rdb *RocksDB, keys, values [][]byte, batchSize, totalRecords int) time.Duration {
	startTime := time.Now()

	for i := 0; i < totalRecords; i += batchSize {
		batch := NewBatch(rdb)
		end := i + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		for _, key := range keys[i:end] {
			if err := batch.Put(key, values[i]); err != nil {
				log.Fatalf("Error putting key-value pair: %v", err)
			}
		}

		if err := batch.Write(); err != nil {
			log.Fatalf("Error writing batch: %v", err)
		}
		batch.Reset()
	}

	return time.Since(startTime)
}

func testLevelDBBatchInsertWithBatchSize(ldb *leveldb.DB, keys, values [][]byte, batchSize, totalRecords int) time.Duration {
	startTime := time.Now()

	for i := 0; i < totalRecords; i += batchSize {
		batch := new(leveldb.Batch)
		end := i + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		for _, key := range keys[i:end] {
			batch.Put(key, values[i])
		}

		if err := ldb.Write(batch, nil); err != nil {
			log.Fatalf("Error writing batch: %v", err)
		}
		batch.Reset()
	}

	return time.Since(startTime)
}
