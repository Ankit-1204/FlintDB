package internals

import (
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
	file       *os.File
	msg        chan string
	fileNumber int
}

func (m *LogManager) StartLog(com chan string, fileName string, fileNumber int) {
	m.msg = com
	m.fileNumber = fileNumber
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	m.file = file

	if err != nil {
		fmt.Println(err)
	}
	for msg := range com {
		// remaining code
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
		}

	}

}
