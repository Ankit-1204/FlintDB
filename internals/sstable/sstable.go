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

	"github.com/Ankit-1204/FlintDB/internals/formats"
	"github.com/Ankit-1204/FlintDB/internals/memtable"
)

// both snaP and read need manifest for file read. files should be numbered sequentially. latest file number known how?
func ReadManifest(dbName string) ([]formats.ManifestEdit, error) {
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
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return edits, err
		}
		payload := make([]byte, payHeader)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return edits, err
		}
		var cEdit formats.ManifestEdit
		// decoder basically decodes(struct etc), while reader actually does the reading
		decoder := gob.NewDecoder(bytes.NewReader(payload))
		if err = decoder.Decode(&cEdit); err != nil {
			return edits, err
		}
	}
	return edits, nil
}

func AppendManifest(dbName string, edits []formats.ManifestEdit) error {
	mPath := filepath.Join(dbName, "sstable", "manifest")
	dirPath := filepath.Dir(mPath)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}
	file, err := os.OpenFile(mPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return err
	}
	writer := bufio.NewWriter(file)
	defer file.Close()
	for _, adds := range edits {
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		if err := encoder.Encode(adds); err != nil {
			return err
		}
		payload := buf.Bytes()
		if err = binary.Write(writer, binary.LittleEndian, uint32(len(payload))); err != nil {
			return err
		}
		if n, err := writer.Write(payload); err != nil {
			fmt.Printf("written %d bytes", n)
			return err
		}
	}
	if err = writer.Flush(); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	return nil
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
		if err = binary.Write(&data, binary.LittleEndian, block.Seq); err != nil {
			return err
		}
		if block.Tombstone {
			data.WriteByte(1)
		} else {
			data.WriteByte(0)
		}
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

func Snap(m *memtable.MemTable, nextSeq int) (*formats.ManifestFile, error) {
	dataBlocks := m.InOrderSlice()
	if err := WriteSegment(dataBlocks, nextSeq, m.Dbname); err != nil {
		return nil, err
	}
	file := formats.ManifestFile{File_number: nextSeq, SmallestKey: dataBlocks[0].Key, LargestKey: dataBlocks[len(dataBlocks)-1].Key, Level: 0, File_size: int(m.Size)}
	return &file, nil
}

func OpenSStable(File_number int) (*formats.SStableReader, error) {
	filename := fmt.Sprintf("sstable-%d", File_number)
	file, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	_, err = file.Seek(-16, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	bufFoot := make([]byte, 16)
	if n, err := io.ReadFull(file, bufFoot); err != nil {
		fmt.Printf("read %d bits", n)
		return nil, err
	}
	indexStart := int64(binary.LittleEndian.Uint64(bufFoot[0:8]))
	indexLen := int64(binary.LittleEndian.Uint64(bufFoot[8:16]))

	_, err = file.Seek(indexStart+4, io.SeekStart)
	if err != nil {
		return nil, err
	}
	indexTable := make([]formats.IndexBlock, 0)
	runSize := indexLen - 4
	for runSize > 0 {
		var keyLen uint32
		if err = binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}
		runSize -= 4
		key := make([]byte, keyLen)
		if n, err := io.ReadFull(file, key); err != nil {
			fmt.Printf("read %d bits", n)
			return nil, err
		}
		runSize -= int64(keyLen)
		var offset uint64
		if err := binary.Read(file, binary.LittleEndian, &offset); err != nil {
			return nil, fmt.Errorf("read offset: %w", err)
		}
		runSize -= 8
		indexTable = append(indexTable, formats.IndexBlock{Key: key, Offset: offset})
	}
	ssIterator := formats.SStableReader{File: file, IndexTable: indexTable}
	return &ssIterator, nil

}
