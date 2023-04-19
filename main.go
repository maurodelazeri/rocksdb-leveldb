package main

/*
#include <stdlib.h>
#include "rocksdb/c.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"log"
	"unsafe"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/syndtr/goleveldb/leveldb"
)

// RocksDB represents a RocksDB key-value store
type RocksDB struct {
	db *C.rocksdb_t
}

func OpenRocksDB(path string) (*RocksDB, error) {
	options := C.rocksdb_options_create()
	C.rocksdb_options_set_create_if_missing(options, 1)

	var errStr *C.char
	db := C.rocksdb_open(options, C.CString(path), &errStr)

	if errStr != nil {
		err := errors.New(C.GoString(errStr))
		C.free(unsafe.Pointer(errStr))
		return nil, fmt.Errorf("failed to open RocksDB: %w", err)
	}

	return &RocksDB{db: db}, nil
}

func main() {
	// Open the RocksDB instance
	rdb, err := OpenRocksDB("test.db")
	if err != nil {
		log.Fatalf("Failed to open RocksDB: %v", err)
	}
	defer rdb.Close()

	// Open LevelDB
	ldb, err := leveldb.OpenFile("leveldb_test", nil)
	if err != nil {
		log.Fatalf("Error opening LevelDB: %v", err)
	}
	defer ldb.Close()

	{
		// Generate data
		n := 10000000 // number of key-value pairs
		keys, values := generateData(n)

		// Test RocksDB batch insert
		rocksdbDuration := testRocksDBBatchInsert(rdb, keys, values)
		fmt.Printf("RocksDB batch insert of %d key-value pairs took %v\n", n, rocksdbDuration)

		// Test LevelDB batch insert
		leveldbDuration := testLevelDBBatchInsert(ldb, keys, values)
		fmt.Printf("LevelDB batch insert of %d key-value pairs took %v\n", n, leveldbDuration)
	}

	return

	{
		// Create a batch instance
		batch := NewBatch(rdb)

		// Perform batch insert
		keys := [][]byte{[]byte("mauro"), []byte("key2"), []byte("key3")}
		values := [][]byte{[]byte("delazeri"), []byte("value2"), []byte("value3")}
		for i, key := range keys {
			if err := batch.Put(key, values[i]); err != nil {
				fmt.Println("Error putting key-value pair:", err)
				return
			}
		}

		// Write the batch
		if err := batch.Write(); err != nil {
			fmt.Println("Error writing batch:", err)
			return
		}
		batch.Reset()

		// Perform batch delete
		for _, key := range keys {
			if err := batch.Delete(key); err != nil {
				fmt.Println("Error deleting key:", err)
				return
			}
		}

		// Write the batch
		if err := batch.Write(); err != nil {
			fmt.Println("Error writing batch:", err)
			return
		}

		batch.Reset()
	}

	// Put a key-value pair
	err = rdb.Put([]byte("key1"), []byte("value1"))
	if err != nil {
		log.Fatalf("Failed to put data: %v", err)
	}

	// Get the value of a key
	value, err := rdb.Get([]byte("key1"))
	if err != nil {
		log.Fatalf("Failed to get data: %v", err)
	}
	fmt.Printf("Value of key1: %s\n", value)

	// Check if a key exists
	has, err := rdb.Has([]byte("key1"))
	if err != nil {
		log.Fatalf("Failed to check existence of key: %v", err)
	}
	fmt.Printf("Key1 exists: %v\n", has)

	// Delete a key
	err = rdb.Delete([]byte("key1"))
	if err != nil {
		log.Fatalf("Failed to delete key: %v", err)
	}

	// Check if the key exists after deletion
	has, err = rdb.Has([]byte("key1"))
	if err != nil {
		log.Fatalf("Failed to check existence of key: %v", err)
	}
	fmt.Printf("Key1 exists after deletion: %v\n", has)
}

func Open(path string) (*RocksDB, error) {
	var (
		err  *C.char
		opts = C.rocksdb_options_create()
	)

	C.rocksdb_options_set_create_if_missing(opts, 1)
	db := C.rocksdb_open(opts, C.CString(path), &err)
	if err != nil {
		return nil, errors.New(C.GoString(err))
	}

	return &RocksDB{db: db}, nil
}

func (r *RocksDB) Close() {
	C.rocksdb_close(r.db)
}

func (r *RocksDB) Put(key, value []byte) error {
	var err *C.char
	opts := C.rocksdb_writeoptions_create()
	defer C.rocksdb_writeoptions_destroy(opts)

	k := C.CString(string(key))
	v := C.CString(string(value))
	defer C.free(unsafe.Pointer(k))
	defer C.free(unsafe.Pointer(v))

	C.rocksdb_put(r.db, opts, k, C.size_t(len(key)), v, C.size_t(len(value)), &err)
	if err != nil {
		return errors.New(C.GoString(err))
	}

	return nil
}

func (r *RocksDB) Get(key []byte) ([]byte, error) {
	var (
		err  *C.char
		vlen C.size_t
		opts = C.rocksdb_readoptions_create()
	)

	defer C.rocksdb_readoptions_destroy(opts)

	k := C.CString(string(key))
	defer C.free(unsafe.Pointer(k))

	v := C.rocksdb_get(r.db, opts, k, C.size_t(len(key)), &vlen, &err)
	defer C.free(unsafe.Pointer(v))

	if err != nil {
		return nil, errors.New(C.GoString(err))
	}

	return C.GoBytes(unsafe.Pointer(v), C.int(vlen)), nil
}

func (r *RocksDB) Has(key []byte) (bool, error) {
	value, err := r.Get(key)
	if err != nil {
		return false, err
	}

	if len(value) > 0 {
		return true, nil
	}

	return false, nil
}

func (r *RocksDB) Delete(key []byte) error {
	var err *C.char
	opts := C.rocksdb_writeoptions_create()
	defer C.rocksdb_writeoptions_destroy(opts)

	k := C.CString(string(key))
	defer C.free(unsafe.Pointer(k))

	C.rocksdb_delete(r.db, opts, k, C.size_t(len(key)), &err)
	if err != nil {
		return errors.New(C.GoString(err))
	}

	return nil
}

func NewBatch(rdb *RocksDB) *rdbBatch {
	wopts := C.rocksdb_writeoptions_create()
	batch := C.rocksdb_writebatch_create()
	return &rdbBatch{
		db:    rdb.db,
		b:     batch,
		wopts: wopts,
		data:  make([]*rdbBatchOp, 0),
		size:  0,
	}
}

func cerror(cerr *C.char) error {
	if cerr == nil {
		return nil
	}
	err := errors.New(C.GoString(cerr))
	C.free(unsafe.Pointer(cerr))
	return err
}

type rdbBatchOp struct {
	del   bool
	key   []byte
	value []byte
}

type rdbBatch struct {
	db    *C.rocksdb_t
	b     *C.rocksdb_writebatch_t
	wopts *C.rocksdb_writeoptions_t
	data  []*rdbBatchOp
	size  int
}

func (b *rdbBatch) Put(key, value []byte) error {
	k := C.CString(string(key))
	v := C.CString(string(value))
	defer C.free(unsafe.Pointer(k))
	defer C.free(unsafe.Pointer(v))

	C.rocksdb_writebatch_put(b.b, k, C.size_t(len(key)), v, C.size_t(len(value)))
	b.data = append(b.data, &rdbBatchOp{del: false, key: key, value: value})
	b.size += len(value)
	return nil
}

func (b *rdbBatch) Delete(key []byte) error {
	k := C.CString(string(key))
	defer C.free(unsafe.Pointer(k))

	C.rocksdb_writebatch_delete(b.b, k, C.size_t(len(key)))
	b.data = append(b.data, &rdbBatchOp{del: true, key: key, value: nil})
	b.size += 1
	return nil
}

func (b *rdbBatch) Write() error {
	var cerr *C.char
	C.rocksdb_write(b.db, b.wopts, b.b, &cerr)
	return cerror(cerr)
}

func (b *rdbBatch) ValueSize() int {
	return b.size
}

func (b *rdbBatch) Reset() {
	C.rocksdb_writebatch_destroy(b.b)
	b.b = C.rocksdb_writebatch_create()
	b.data = nil
	b.size = 0
}

// Replay replays the batch contents.
func (b *rdbBatch) Replay(w ethdb.KeyValueWriter) error {
	for _, i := range b.data {
		if i.del {
			w.Delete(i.key)
		} else {
			w.Put(i.key, i.value)
		}
	}
	return nil
}
