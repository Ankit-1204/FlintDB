package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"engine/internals/formats"
	"fmt"
	"io"
	"os"
	"sync"
)

type WriterInterface interface {
	Append()
}

type ReaderInterface interface {
	Read()
}

type LogManager struct {
	file          *os.File
	msg           chan formats.LogAppend
	ManifestPath  string
	ManifestState Manifest
	lock          sync.Mutex
}
type Manifest struct {
	File []string `json:"file"`
	Seq  int      `json:"seq"`
}

func readManifest(path string) Manifest {
	file, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
	}
	decoder := json.NewDecoder(file)
	var fileData Manifest
	err = decoder.Decode(&fileData)

	return fileData

}

func writeManifest(path string, currFile Manifest) {
	//  to write
}
func (m *LogManager) StartLog(com chan formats.LogAppend) error {
	m.msg = com
	m.ManifestState = Manifest{make([]string, 0), 0}
	m.ManifestPath = "internals/wal/manifest.json"
	var fileName string
	m.ManifestState = readManifest(m.ManifestPath)
	fileName = m.ManifestState.File[len(m.ManifestState.File)-1]
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	m.file = file
	writer := bufio.NewWriter(m.file)

	if err != nil {
		fmt.Println(err)
		return err
	}
	for msg := range com {
		var buf bytes.Buffer
		// uint32 wshould mean 4bytes
		binary.Write(&buf, binary.LittleEndian, uint32(len(msg.Key)))
		buf.Write([]byte(msg.Key))
		// binary.Write only writes a fixed sized data (like uint32, basically its always 4bytes)
		binary.Write(&buf, binary.LittleEndian, uint32(len(msg.Payload)))
		buf.Write([]byte(msg.Payload))
		buf.Write([]byte(msg.Operation))

		record := buf.Bytes()
		writer.Write(record)
		if err := writer.Flush(); err != nil {
			fmt.Println(err)
			return err
		}
		m.file.Sync()
		fileInfo, err := os.Stat(fileName)
		if err != nil {
			fmt.Println(err)
			return err
		}
		if size := fileInfo.Size(); size >= 32000 {
			if err := m.file.Sync(); err != nil {
				fmt.Println(err)
				return err
			}
			m.file.Close()
			m.file = nil
			m.ManifestState.Seq++
			fileName = fmt.Sprintf("%d.log", m.ManifestState.Seq)
			file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				fmt.Println(err)
				return err
			}
			m.file = file
			m.ManifestState.File = append(m.ManifestState.File, fileName)
			writer = bufio.NewWriter(m.file)
			writeManifest(m.ManifestPath, m.ManifestState)
		}

	}
	return nil

}

func (m *LogManager) Replay() []formats.LogAppend {
	fileArray := readManifest(m.ManifestPath)
	records := make([]formats.LogAppend, 0)
	for _, entry := range fileArray.File {
		f, err := os.Open(entry)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		reader := bufio.NewReader(f)
		for {
			var keylen uint32
			var loadlen uint32
			// binary.read works only for fixed slice value/slice
			if err = binary.Read(reader, binary.LittleEndian, keylen); err != nil {
				break
			}
			key := make([]byte, keylen)
			if _, err = io.ReadFull(reader, key); err != nil {
				break
			}
			// ReadFull only reads uptill the length of the buffer provided
			if err = binary.Read(reader, binary.LittleEndian, loadlen); err != nil {
				break
			}
			payload := make([]byte, loadlen)
			if _, err = io.ReadFull(reader, payload); err != nil {
				break
			}
			operation := make([]byte, 1)
			if _, err = io.ReadFull(reader, operation); err != nil {
				break
			}

			newRecord := formats.LogAppend{Key: string(key), Payload: payload, Operation: string(operation)}
			records = append(records, newRecord)
		}
		f.Close()
	}
	return records
}
