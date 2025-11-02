package internals

import (
	"sync"

	"github.com/Ankit-1204/FlintDB.git/internals/formats"
	"github.com/Ankit-1204/FlintDB.git/internals/memtable"
	"github.com/Ankit-1204/FlintDB.git/internals/wal"
)

type Database struct {
	table  *memtable.MemTable
	wal    chan *formats.LogAppend
	mu     sync.RWMutex
	dbName string
}

func Open(dbName string) (*Database, error) {
	table := memtable.Start()
	appChannel := make(chan *formats.LogAppend, 10)
	go wal.StartLog(appChannel, dbName)

	db := Database{dbName: dbName, table: table, wal: appChannel}
	return &db, nil

}

func (db *Database) Get(key string) []byte {
	db.mu.RLock()
	defer db.mu.RUnlock()
	val := db.table.Search(key)
	return val
}

func (db *Database) Put(key string, value []byte) error {
	// look into batched writes
	db.mu.Lock()
	defer db.mu.Unlock()
	Msg := &formats.LogAppend{Key: key, Payload: value, Operation: "P", Done: make(chan error)}
	db.wal <- Msg
	err := <-Msg.Done
	if err != nil {
		return err
	}
	err = db.table.Insert(key, value)
	if err != nil {
		return err
	}
	return nil
}
