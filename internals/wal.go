package internals

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
)

type WriterInterface interface {
	Append()
}

type ReaderInterface interface {
	Read()
}

type LogManager struct {
	file         *os.File
	msg          chan LogAppend
	fileNumber   int
	ManifestPath string
}
type Manifest struct {
	File string `json:"file"`
	Seq  int    `json:"seq"`
}

func readManifest(path string) (string, int) {
	file, err := os.ReadFile(path)
	if err != nil {
		fmt.Println(err)
	}
	var fileData Manifest
	if err = json.Unmarshal(file, &fileData); err != nil {
		fmt.Println(err)
	}
	return fileData.File, fileData.Seq
}

func writeManifest(path string, currFile Manifest) {
	//  to write
}
func (m *LogManager) StartLog(com chan LogAppend, manPath string) {
	m.msg = com
	var fileName string
	fileName, m.fileNumber = readManifest(manPath)
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	m.file = file
	writer := bufio.NewWriter(m.file)

	if err != nil {
		fmt.Println(err)
	}
	for msg := range com {

		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, uint32(len(msg.key)))
		buf.Write([]byte(msg.key))
		binary.Write(&buf, binary.LittleEndian, uint32(len(msg.payload)))
		buf.Write([]byte(msg.payload))
		buf.Write([]byte(msg.operation))

		record := buf.Bytes()
		writer.Write(record)
		fileInfo, err := os.Stat(fileName)
		if err != nil {
			fmt.Println(err)
		}
		if size := fileInfo.Size(); size >= 32000 {
			if err := m.file.Sync(); err != nil {
				fmt.Println(err)
			}
			m.file.Close()
			m.file = nil
			m.fileNumber++
			fileName = fmt.Sprintf("%d.log", m.fileNumber)
			file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				fmt.Println(err)
			}
			m.file = file
			updateManifest := Manifest{fileName, m.fileNumber}
			writeManifest(m.ManifestPath, updateManifest)
		}

	}

}
