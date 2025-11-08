package formats

type LogAppend struct {
	Key       string
	Payload   []byte
	Operation string
	Done      chan error
}

type DataBlock struct {
	Key   []byte
	Value []byte
}

type IndexTable struct {
	Key    []byte
	Offset int
}

type ManifestAddFile struct {
	File_number int
	SmallestKey []byte
	LargestKey  []byte
}

type ManifestDelFile struct {
	File_number int
}

type Sstable_files struct {
	Live_files  []int
	Next_number int
}
