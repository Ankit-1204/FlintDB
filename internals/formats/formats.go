package formats

type LogAppend struct {
	Key       string
	Payload   []byte
	Operation string
	Done      chan error
}
