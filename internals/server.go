package internals

import (
	"fmt"
	"os"
	"sync"

	"github.com/Ankit-1204/FlintDB.git/internals/formats"
	"github.com/Ankit-1204/FlintDB.git/internals/memtable"
	"github.com/Ankit-1204/FlintDB.git/internals/sstable"
	"github.com/Ankit-1204/FlintDB.git/internals/wal"
)

type Database struct {
	table          *memtable.MemTable
	wal            chan *formats.LogAppend
	replayChan     chan []formats.LogAppend
	flushChan      chan *memtable.MemTable
	compactionChan chan bool
	mu             sync.RWMutex
	ssMu           sync.RWMutex
	dbName         string
	ssVersion      *formats.SSVersion
	MaxMemSize     int64
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
	if editSlice, err := sstable.ReadManifest(dbName); err != nil {
		return nil, err
	}
	table := memtable.Start(dbName)
	appChannel := make(chan *formats.LogAppend, 10)
	replayChan := make(chan []formats.LogAppend)
	flushChan := make(chan *memtable.MemTable)
	db := Database{dbName: dbName, table: table, wal: appChannel, replayChan: replayChan, flushChan: flushChan, MaxMemSize: 32 * 1024 * 1024}
	go wal.StartLog(appChannel, replayChan, dbName)

	// Note to self: orphaned sstables not detected for calculating nextNum
	if err = db.getVersion(editSlice); err != nil {
		return nil, err
	}
	db.replayCreate()
	go db.flushQueue()
	return &db, nil

}

func (db *Database) getVersion(editSlice []formats.ManifestEdit) error {
	ssVersion := formats.SSVersion{LevelMap: make(map[int][]formats.ManifestFile), Next_number: 0}
	for _, val := range editSlice {
		if val.Add != nil {
			mval := ssVersion.LevelMap[val.Add.Level]
			mval = append(mval, *val.Add)
			ssVersion.LevelMap[val.Add.Level] = mval
		} else {
			level := val.Delete.Level
			files := ssVersion.LevelMap[level]
			idx := -1
			for i, f := range files {
				if f.File_number == val.Delete.File_number {
					idx = i
					break
				}
			}

			if idx == -1 {
				return fmt.Errorf("delete: file %d not found in level %d", val.Delete.File_number, level)
			}
			files = append(files[:idx], files[idx+1:]...)

			ssVersion.LevelMap[level] = files
		}
		ssVersion.Next_number = max(ssVersion.Next_number, val.Next_number)
	}
	db.ssMu.Lock()
	defer db.ssMu.Unlock()
	db.ssVersion = &ssVersion
	return nil
}
func (db *Database) replayCreate() error {
	msg := <-db.replayChan
	if len(msg) > 0 {
		for _, entry := range msg {
			switch entry.Operation {
			case "P":
				err := db.table.Insert(entry.Key, entry.Payload, int(entry.Seq), entry.Tombstone)
				if err != nil {
					return err
				}
			case "D":
				if err := db.table.Insert(entry.Key, nil, int(entry.Seq), true); err != nil {
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
	err = db.table.Insert(key, value, int(Msg.Seq), Msg.Tombstone)
	if err != nil {
		return err
	}
	if db.table.Size > db.MaxMemSize {
		old := db.table
		db.table = memtable.Start(db.dbName)
		// so if the channel is full, we can spawn a goroutine that will push to that channel. neat
		select {
		case db.flushChan <- old:
		default:
			go func(old *memtable.MemTable) {
				db.flushChan <- old
			}(old)
		}
	}
	return nil
}

func (db *Database) flushQueue() {
	for table := range db.flushChan {
		db.ssMu.Lock()
		nextSeq := db.ssVersion.Next_number
		db.ssVersion.Next_number++
		db.ssMu.Unlock()
		file, err := sstable.Snap(table, nextSeq)
		if err != nil {
			fmt.Println(err)
		}

		edits := make([]formats.ManifestEdit, 0)
		edits = append(edits, formats.ManifestEdit{Add: file, Next_number: nextSeq + 1})
		err = sstable.AppendManifest(db.dbName, edits)
		db.ssMu.Lock()
		levelFiles := db.ssVersion.LevelMap[0]
		levelFiles = append(levelFiles, *file)
		db.ssVersion.LevelMap[0] = levelFiles
		db.ssMu.Unlock()
	}
}

func (db *Database) compactionQueue() {

}
