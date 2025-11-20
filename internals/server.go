package internals

import (
	"bytes"
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

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
	nextFileNumber uint64
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
	db := Database{dbName: dbName, table: table, wal: appChannel, replayChan: replayChan, flushChan: flushChan, MaxMemSize: 32 * 1024 * 1024, nextFileNumber: 0}
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
	ssVersion := formats.SSVersion{LevelMap: make(map[int][]formats.ManifestFile)}
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
		atomic.StoreUint64(&db.nextFileNumber, max(db.nextFileNumber, uint64(val.Next_number)))

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

		nextSeq := atomic.AddUint64(&db.nextFileNumber, 1) - 1

		file, err := sstable.Snap(table, int(nextSeq))
		if err != nil {
			fmt.Println(err)
		}

		edits := make([]formats.ManifestEdit, 0)
		edits = append(edits, formats.ManifestEdit{Add: file, Next_number: int(nextSeq + 1)})
		err = sstable.AppendManifest(db.dbName, edits)
		db.ssMu.Lock()
		levelFiles := db.ssVersion.LevelMap[0]
		levelFiles = append(levelFiles, *file)
		db.ssVersion.LevelMap[0] = levelFiles
		db.ssMu.Unlock()
	}
}

func (db *Database) pickCandidate() *formats.CompactionCandidate {
	db.ssMu.RLock()
	defer db.ssMu.RUnlock()
	const fileFactor = 4

	maxLevel := 0
	for l := range db.ssVersion.LevelMap {
		if l > maxLevel {
			maxLevel = l
		}
	}
	for level := 0; level <= maxLevel; level++ {
		val, ok := db.ssVersion.LevelMap[level]
		if !ok || len(val) <= fileFactor {
			continue
		}
		tmp := make([]formats.ManifestFile, len(val))
		copy(tmp, val)
		sort.Slice(tmp, func(i, j int) bool { return tmp[i].File_size < tmp[j].File_size })

		selCount := fileFactor
		if len(tmp) < selCount {
			selCount = len(tmp)
		}
		sel := make([]formats.ManifestFile, selCount)
		copy(sel, tmp[:selCount])

		return &formats.CompactionCandidate{Level: level, Files: sel}
	}
	return nil
}

func (db *Database) performCompaction(candidate *formats.CompactionCandidate) error {

	targetLevel := candidate.Level + 1

	var iterators []*formats.SSIterator
	for _, mf := range candidate.Files {
		ssr, err := sstable.OpenSStable(mf.File_number)
		it := &formats.SSIterator{
			Reader:       ssr,
			CurrentIndex: 0,
		}
		if ok := it.Next(); !ok {
			// no entries / error reading — close underlying file and skip
			_ = ssr.File.Close()
			continue
		}
		if err != nil {
			return fmt.Errorf("open sstable %d: %w", mf.File_number, err)
		}
		iterators = append(iterators, it)
	}
	if len(iterators) == 0 {
		return nil
	}

	h := &formats.MinHeap{}
	heap.Init(h)

	for idx, val := range iterators {
		if val.Next() {
			heap.Push(h, &formats.HeapItem{Key: val.Key(), Value: val.Value(), Seq: val.Seq(), Tombstone: val.Tomb(), IteratorIndex: idx})
		}
	}
	var curKey []byte
	var best formats.DataBlock
	var merged []formats.DataBlock
	pushBest := func() {
		if best.Key == nil {
			return
		}
		// copy key/value so later buffer reuse from iterators doesn't bite us
		k := make([]byte, len(best.Key))
		copy(k, best.Key)
		v := make([]byte, len(best.Value))
		copy(v, best.Value)
		merged = append(merged, formats.DataBlock{
			Key:       k,
			Value:     v,
			Seq:       best.Seq,
			Tombstone: best.Tombstone,
		})
		best = formats.DataBlock{} // reset
	}
	for h.Len() > 0 {
		itItem := heap.Pop(h).(*formats.HeapItem)

		if curKey == nil {
			// first key seen
			curKey = make([]byte, len(itItem.Key))
			copy(curKey, itItem.Key)
			best = formats.DataBlock{Key: itItem.Key, Value: itItem.Value, Seq: itItem.Seq, Tombstone: itItem.Tombstone}
		} else if bytes.Equal(itItem.Key, curKey) {
			// same key — pick item with larger seq
			if itItem.Seq > best.Seq {
				best = formats.DataBlock{Key: itItem.Key, Value: itItem.Value, Seq: itItem.Seq, Tombstone: itItem.Tombstone}
			}
		} else {
			// new key encountered: push the best for prev key
			pushBest()
			curKey = make([]byte, len(itItem.Key))
			copy(curKey, itItem.Key)
			best = formats.DataBlock{Key: itItem.Key, Value: itItem.Value, Seq: itItem.Seq, Tombstone: itItem.Tombstone}
		}

		// advance the iterator that produced the heap item
		srcIt := iterators[itItem.IteratorIndex]
		if srcIt.Next() {
			heap.Push(h, &formats.HeapItem{
				Key:           srcIt.Key(),
				Value:         srcIt.Value(),
				Seq:           srcIt.Seq(),
				Tombstone:     srcIt.Tomb(),
				IteratorIndex: itItem.IteratorIndex,
			})
		} else {
			// iterator exhausted or read error — close underlying file
			if srcIt.Reader != nil && srcIt.Reader.File != nil {
				_ = srcIt.Reader.File.Close()
			}
		}
	}
	pushBest()
	if len(merged) == 0 {
		return nil
	}
	newFileNum := int(atomic.AddUint64(&db.nextFileNumber, 1) - 1)
	if err := sstable.WriteSegment(merged, newFileNum, db.dbName); err != nil {
		return fmt.Errorf("write merged sstable: %w", err)
	}
	newPath := filepath.Join(db.dbName, "sstable", fmt.Sprintf("sstable-%d", newFileNum))
	st, err := os.Stat(newPath)
	if err != nil {
		return fmt.Errorf("stat new sstable file: %w", err)
	}
	smallest := merged[0].Key
	largest := merged[len(merged)-1].Key
	addFile := formats.ManifestFile{
		File_number: newFileNum,
		SmallestKey: smallest,
		LargestKey:  largest,
		Level:       targetLevel,
		File_size:   int(st.Size()),
	}
	var edits []formats.ManifestEdit
	edits = append(edits, formats.ManifestEdit{
		Add:         &addFile,
		Delete:      nil,
		Next_number: newFileNum + 1,
	})
	for _, old := range candidate.Files {
		del := formats.ManifestEdit{
			Add: nil,
			Delete: &formats.ManifestDelFile{
				File_number: old.File_number,
				Level:       old.Level,
			},
			Next_number: 0,
		}
		edits = append(edits, del)
	}
	if err := sstable.AppendManifest(db.dbName, edits); err != nil {
		// don't delete old files,, keep them until manifest records deletion
		return fmt.Errorf("append manifest edits: %w", err)
	}
	db.ssMu.Lock()
	db.ssVersion.LevelMap[targetLevel] = append(db.ssVersion.LevelMap[targetLevel], addFile)
	for _, old := range candidate.Files {
		files := db.ssVersion.LevelMap[old.Level]
		newFiles := make([]formats.ManifestFile, 0, len(files))
		for _, f := range files {
			if f.File_number != old.File_number {
				newFiles = append(newFiles, f)
			}
		}
		db.ssVersion.LevelMap[old.Level] = newFiles
	}
	db.ssMu.Unlock()
	for _, old := range candidate.Files {
		oldPath := filepath.Join(db.dbName, "sstable", fmt.Sprintf("sstable-%d", old.File_number))
		_ = os.Remove(oldPath)
	}
	return nil
}

func (db *Database) compactionProcess() error {
	cand := db.pickCandidate()
	if cand != nil {
		if err := db.performCompaction(cand); err != nil {
			fmt.Println(err)
			return err
		}
	}
	return nil
}
