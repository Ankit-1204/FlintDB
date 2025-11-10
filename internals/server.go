package internals

import (
	"fmt"
	"os"
	"sync"

	"github.com/Ankit-1204/FlintDB.git/internals/formats"
	"github.com/Ankit-1204/FlintDB.git/internals/memtable"
	"github.com/Ankit-1204/FlintDB.git/internals/wal"
)

type Database struct {
	table      *memtable.MemTable
	wal        chan *formats.LogAppend
	replayChan chan []formats.LogAppend
	mu         sync.RWMutex
	ssMu       sync.RWMutex
	dbName     string
}

func Open(dbName string) (*Database, error) {
	_, err := os.Stat(dbName)

	if os.IsNotExist(err) {
		fmt.Printf("Folder '%s' does not exist.\n", dbName)
		err := os.Mkdir(dbName, 0755)
		if err != nil {
			fmt.Println("Error creating directory:", err)
			return nil, err
		}
	}
	table := memtable.Start(dbName)
	appChannel := make(chan *formats.LogAppend, 10)
	replayChan := make(chan []formats.LogAppend)
	db := Database{dbName: dbName, table: table, wal: appChannel, replayChan: replayChan}
	go wal.StartLog(appChannel, replayChan, dbName)
	db.replayCreate()
	return &db, nil

}
func (db *Database) replayCreate() error {
	msg := <-db.replayChan
	if len(msg) > 0 {
		for _, entry := range msg {
			switch entry.Operation {
			case "P":
				err := db.table.Insert(entry.Key, entry.Payload)
				if err != nil {
					return err
				}
			case "G":
				continue
			}
		}
	}
	return nil
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
