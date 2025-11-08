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

type ManifestEdit struct {
	Add         []*ManifestAddFile
	Delete      []*ManifestDelFile
	Next_number int
}

type Sstable_edits struct {
	Edits       []*ManifestEdit
	Next_number int
}
