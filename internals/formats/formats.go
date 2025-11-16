package formats

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
	LevelMap    map[int][]ManifestFile
	Next_number int
}
