# rocksdb-leveldb
Performance test of rockddb vs leveldb

 ```
$ CGO_LDFLAGS="-L/usr/local/lib/ -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" go run *.go

RocksDB batch insert of 10000000 key-value pairs took 5.731581767s

LevelDB batch insert of 10000000 key-value pairs took 1m13.510031562s
```
