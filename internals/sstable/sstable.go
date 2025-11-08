package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Ankit-1204/FlintDB.git/internals/formats"
	"github.com/Ankit-1204/FlintDB.git/internals/memtable"
)

// both snaP and read need manifest for file read. files should be numbered sequentially. latest file number known how?
func readManifest(dbName string) ([]formats.ManifestEdit, error) {
	mPath := filepath.Join(dbName, "sstable", "manifest")
	dirPath := filepath.Dir(mPath)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}
	file, err := os.OpenFile(mPath, os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer file.Close()
	var edits []formats.ManifestEdit
	reader := bufio.NewReader(file)
	for {
		var payHeader uint32
		if err = binary.Read(reader, binary.LittleEndian, &payHeader); err != nil {
			return edits, err
		}
		payload := make([]byte, payHeader)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return edits, err
		}
		var cEdit formats.ManifestEdit
		// decoder basically decodes(struct etc), while reader actually does the reading
		decoder := gob.NewDecoder(bytes.NewReader(payload))
		if err = decoder.Decode(&cEdit); err != nil {
			return edits, err
		}
	}

}

func Snap(m *memtable.MemTable) {

}

func Read() {

}
