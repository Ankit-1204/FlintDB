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
func WriteSegment(dataBlocks []formats.DataBlock, nextSeq int, dbname string) error {
	indexTable := make([]formats.IndexBlock, 0)
	filename := fmt.Sprintf("sstable-%d", nextSeq)
	tempName := filename + ".tmp"
	tempPath := filepath.Join(dbname, "sstable", tempName)
	path := filepath.Join(dbname, "sstable", filename)
	file, err := os.OpenFile(tempPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	var pos uint64
	pos = 0
	for _, block := range dataBlocks {

		indexBlock := formats.IndexBlock{Key: block.Key, Offset: (pos)}

		var data bytes.Buffer
		if err = binary.Write(&data, binary.LittleEndian, uint32(len(block.Key))); err != nil {
			return err
		}
		data.Write(block.Key)
		if err = binary.Write(&data, binary.LittleEndian, uint32(len(block.Value))); err != nil {
			return err
		}
		data.Write(block.Value)
		payload := data.Bytes()
		indexTable = append(indexTable, indexBlock)
		n, err := writer.Write(payload)
		if err != nil {
			writer.Flush()
			file.Close()
			os.Remove(tempPath)
			return err
		}
		if n != len(payload) {
			writer.Flush()
			file.Close()
			os.Remove(tempPath)
			return io.ErrShortWrite
		}

		indexTable = append(indexTable, formats.IndexBlock{
			Key:    block.Key,
			Offset: (pos),
		})
		pos += uint64(n)
		if err := writer.Flush(); err != nil {
			file.Close()
			os.Remove(tempPath)
			return err
		}

	}
	indexStart := pos
	var indexBuf bytes.Buffer
	if err = binary.Write(&indexBuf, binary.LittleEndian, uint32(len(indexTable))); err != nil {
		return err
	}
	for _, indexBlock := range indexTable {
		if err = binary.Write(&indexBuf, binary.LittleEndian, uint32(len(indexBlock.Key))); err != nil {
			return err
		}
		indexBuf.Write(indexBlock.Key)
		if err = binary.Write(&indexBuf, binary.LittleEndian, (indexBlock.Offset)); err != nil {
			return err
		}
	}
	idxBytes := indexBuf.Bytes()
	if _, err := file.Write(idxBytes); err != nil {
		file.Close()
		os.Remove(tempPath)
		return err
	}
	footer := make([]byte, 16)
	binary.LittleEndian.PutUint64(footer[0:8], indexStart)
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(idxBytes)))
	if _, err := file.Write(footer); err != nil {
		file.Close()
		os.Remove(tempPath)
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tempPath)
		return err
	}
	if err := file.Close(); err != nil {
		os.Remove(tempPath)
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return err
	}
	return nil
}

func Snap(m *memtable.MemTable) error {
	dataBlocks := m.InOrderSlice()
	if editSlice, err := readManifest(m.Dbname); err != nil {
		return err
	}
	// add code for summarizing the version from edit logs

}

func Read() {

}
