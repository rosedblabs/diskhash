package diskhash

import "os"

type bucket struct {
}

type bucketIterator struct {
	currentFile  *os.File
	overflowFile *os.File
	offset       int64
}
