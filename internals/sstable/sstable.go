package sstable

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Ankit-1204/FlintDB.git/internals/memtable"
)

// both snaP and read need manifest for file read. files should be numbered sequentially. latest file number known how?
func readManifest(dbName string) error {
	mPath := filepath.Join(dbName, "sstable", "manifest.json")
	dirPath := filepath.Dir(mPath)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}
	file, err := os.OpenFile(mPath, os.O_APPEND|os.O_CREATE, 0644)
}

func Snap(m *memtable.MemTable) {

}

func Read() {

}
