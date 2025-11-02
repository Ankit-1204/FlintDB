package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/Ankit-1204/FlintDB.git/internals/formats"
	"github.com/google/uuid"
)

type WriterInterface interface {
	Append()
}

type ReaderInterface interface {
	Read()
}

type LogManager struct {
	dbName        string
	File          *os.File
	Msg           chan *formats.LogAppend
	ManifestPath  string
	ManifestState Manifest
	lock          sync.Mutex
}
type Manifest struct {
	File []string `json:"file"`
	Seq  int      `json:"seq"`
}

func readManifest(path string) Manifest {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		fmt.Println(err)
	}
	decoder := json.NewDecoder(file)
	fileData := Manifest{make([]string, 0), 0}
	err = decoder.Decode(&fileData)
	if err != nil {
		fmt.Println(err)
	}
	return fileData

}

func writeManifest(path string, currFile Manifest) error {
	//  to write
	randName := uuid.NewString()
	tempfile, err := os.Create(randName)
	if err != nil {
		return err
	}
	defer tempfile.Close()

	writer := bufio.NewWriter(tempfile)
	encoder := json.NewEncoder(writer)
	if err = encoder.Encode(currFile); err != nil {
		fmt.Println(err)
		return err
	}
	writer.Flush()
	tempfile.Sync()
	if err = tempfile.Close(); err != nil {
		fmt.Println(err)
		return err
	}
	if err = os.Rename(tempfile.Name(), path); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
func StartLog(appChannel chan *formats.LogAppend, dbName string) error {
	m := LogManager{}
	m.Msg = appChannel
	m.dbName = dbName
	m.ManifestState = Manifest{make([]string, 0), 0}
	m.ManifestPath = fmt.Sprintf(m.dbName, "/manifest.json")
	var fileName string
	m.ManifestState = readManifest(m.ManifestPath)
	if len(m.ManifestState.File) > 0 {
		fileName = m.ManifestState.File[len(m.ManifestState.File)-1]
	} else {
		fileName = fmt.Sprintf("%d.log", m.ManifestState.Seq)
		m.ManifestState.File = append(m.ManifestState.File, fileName)
		writeManifest(m.ManifestPath, m.ManifestState)
	}

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	m.File = file
	writer := bufio.NewWriter(m.File)

	if err != nil {
		fmt.Println(err)
		return err
	}
	for msg := range m.Msg {
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
			msg.Done <- err
		}
		m.File.Sync()
		fileInfo, err := os.Stat(fileName)
		if err != nil {
			fmt.Println(err)
			msg.Done <- err
		}
		msg.Done <- nil
		if size := fileInfo.Size(); size >= 32000 {
			if err := m.File.Sync(); err != nil {
				fmt.Println(err)
				return err
			}
			m.File.Close()
			m.File = nil
			m.ManifestState.Seq++
			fileName = fmt.Sprintf("%d.log", m.ManifestState.Seq)
			file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				fmt.Println(err)
				return err
			}
			m.File = file
			m.ManifestState.File = append(m.ManifestState.File, fileName)
			writer = bufio.NewWriter(m.File)
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
			if err = binary.Read(reader, binary.LittleEndian, &keylen); err != nil {
				break
			}
			key := make([]byte, keylen)
			if _, err = io.ReadFull(reader, key); err != nil {
				break
			}
			// ReadFull only reads uptill the length of the buffer provided
			if err = binary.Read(reader, binary.LittleEndian, &loadlen); err != nil {
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
