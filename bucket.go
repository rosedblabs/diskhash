package diskhash

import (
	"github.com/lotusdblabs/diskhash/fs"
	"io"
)

const slotsPerBucket = 31

type Slot struct {
	Hash  uint32
	Value []byte
}

type bucket struct {
	slots       [slotsPerBucket]*Slot
	overflowOff int64
	file        fs.File
	offset      int64

	// fields used by write
	newSlot      *Slot
	newSlotIndex int
}

type bucketIterator struct {
	currentFile  fs.File
	overflowFile fs.File
	offset       int64
}

func bucketOffset(bucketNo uint32) int64 {
	return int64(fileHeaderSize) + int64(bucketSize*bucketNo)
}

func (t *Table) newBucketIterator(startBucket uint32) *bucketIterator {
	return &bucketIterator{
		currentFile:  t.mainFile,
		overflowFile: t.overflowFile,
		offset:       bucketOffset(startBucket),
	}
}

func (bi *bucketIterator) next() (*bucket, error) {
	//  end of to iterate
	if bi.offset == 0 {
		return nil, io.EOF
	}

	// 解析得到 slots 和 overflow bucket 的 offset
	bucket, err := bi.readBucket()
	if err != nil {
		return nil, err
	}

	// 下一次从 overflow 文件中开始读取
	// set next read state
	bi.offset = bucket.overflowOff
	bi.currentFile = bi.overflowFile
	return bucket, nil
}

func (bi *bucketIterator) readBucket() (*bucket, error) {
	bucket := make([]byte, bucketSize)
	_, err := bi.currentFile.ReadAt(bucket, bi.offset)
	if err != nil {
		return nil, err
	}
	return nil, err
}

func (b *bucket) writeSlot() {
	if b.newSlotIndex == slotsPerBucket {
		// insert it into overflow file
	}

}
