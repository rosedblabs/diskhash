package fs

import "os"

type OSFile struct {
	fd *os.File
}

func openOSFile(name string, flag int, perm os.FileMode) (File, error) {
	fd, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &OSFile{
		fd: fd,
	}, nil
}

func (of *OSFile) ReadAt(b []byte, off int64) (n int, err error) {
	return of.fd.ReadAt(b, off)
}

func (of *OSFile) Write([]byte) error {
	return nil
}

func (of *OSFile) Truncate(size int64) error {
	return nil
}

func (of *OSFile) Size() int64 {
	stat, _ := of.fd.Stat()
	return stat.Size()
}

func (of *OSFile) Sync() error {
	return of.fd.Sync()
}

func (of *OSFile) Close() error {
	return of.fd.Close()
}
