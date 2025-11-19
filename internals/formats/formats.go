package formats

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type LogAppend struct {
	Key       string
	Payload   []byte
	Operation string
	Done      chan error
	Seq       uint64
	Tombstone bool
}

type DataBlock struct {
	Key       []byte
	Value     []byte
	Seq       uint64
	Tombstone bool
}

type IndexBlock struct {
	Key    []byte
	Offset uint64
}

type ManifestFile struct {
	File_number int
	SmallestKey []byte
	LargestKey  []byte
	Level       int
	File_size   int
}

type ManifestDelFile struct {
	File_number int
	Level       int
}

type ManifestEdit struct {
	Add         *ManifestFile
	Delete      *ManifestDelFile
	Next_number int
}

type SSVersion struct {
	LevelMap map[int][]ManifestFile
}

type CompactionCandidate struct {
	Level int
	Files []ManifestFile
}

type SStableReader struct {
	File       *os.File
	IndexTable []IndexBlock
}

type SSIterator struct {
	Reader        *SStableReader
	CurrentIndex  int
	CurrentKey    []byte
	CurrentValue  []byte
	CurrentSeq    uint64
	CurrentTomb   bool
	CurrentOffset int64
}

func (it *SSIterator) Next() bool {
	if it.CurrentIndex >= len(it.Reader.IndexTable)-1 {
		return false
	}

	it.CurrentIndex++
	it.CurrentKey = it.Reader.IndexTable[it.CurrentIndex].Key
	it.CurrentOffset = int64(it.Reader.IndexTable[it.CurrentIndex].Offset)

	if _, err := it.Reader.File.Seek(it.CurrentOffset, io.SeekStart); err != nil {
		fmt.Println(err)
		return false
	}

	var keyLen uint32
	if err := binary.Read(it.Reader.File, binary.LittleEndian, &keyLen); err != nil {
		fmt.Println(err)
		return false
	}

	if _, err := it.Reader.File.Seek(int64(keyLen), io.SeekCurrent); err != nil {
		fmt.Println(err)
		return false
	}

	var valueLen uint32
	if err := binary.Read(it.Reader.File, binary.LittleEndian, &valueLen); err != nil {
		fmt.Println(err)
		return false
	}

	it.CurrentValue = make([]byte, valueLen)
	if _, err := io.ReadFull(it.Reader.File, it.CurrentValue); err != nil {
		fmt.Println(err)
		return false
	}

	if err := binary.Read(it.Reader.File, binary.LittleEndian, &it.CurrentSeq); err != nil {
		fmt.Println(err)
		return false
	}

	tombByte := make([]byte, 1)
	if _, err := io.ReadFull(it.Reader.File, tombByte); err != nil {
		fmt.Println(err)
		return false
	}
	it.CurrentTomb = tombByte[0] == 1

	return true
}

func (it *SSIterator) Key() []byte {
	return it.CurrentKey
}

func (it *SSIterator) Value() []byte {
	return it.CurrentValue
}

func (it *SSIterator) Seq() uint64 {
	return it.CurrentSeq
}

func (it *SSIterator) Tomb() bool {
	return it.CurrentTomb
}
