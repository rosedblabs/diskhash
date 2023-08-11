package fs

type File interface {
	ReadAt([]byte, int64) (int, error)
	WriteAt([]byte, int64) (int, error)
	Truncate(int64) error
	Size() int64
	Sync() error
	Close() error
}

type FileSystem = byte

const (
	OSFileSystem FileSystem = iota
)

func Open(name string, fs FileSystem) (File, error) {
	switch fs {
	case OSFileSystem:
		return openOSFile(name)
	}
	return nil, nil
}
