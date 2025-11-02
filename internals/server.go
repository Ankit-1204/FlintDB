package internals

import (
	"github.com/Ankit-1204/FlintDB.git/internals/formats"
	"github.com/Ankit-1204/FlintDB.git/internals/memtable"
	"github.com/Ankit-1204/FlintDB.git/internals/wal"
)

func Start() {
	db := memtable.Start()
	appendChan := make(chan formats.LogAppend, 10)
	walTable := wal.LogManager{Msg: appendChan}
	go walTable.StartLog()
}
