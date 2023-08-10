package fs

type File interface {
	ReadAt([]byte, int64) (int, error)
	Write([]byte) error
	Truncate(int64) error
	Size() int64
	Sync() error
	Close() error
}

func Open(name string) (File, error) {
	return nil, nil
}
