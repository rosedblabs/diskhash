package diskhash

import (
	"encoding/binary"
	"io"

	"github.com/lotusdblabs/diskhash/fs"
)

const slotsPerBucket = 31

type Slot struct {
	Hash  uint32
	Value []byte
}

type bucket struct {
	slots      [slotsPerBucket]Slot
	nextOffset int64

	// fields used by write slot
	offset       int64
	file         fs.File
	newSlot      *Slot
	newSlotIndex int
}

type bucketIterator struct {
	currentFile  fs.File
	overflowFile fs.File
	offset       int64
	slotValueLen uint32
}

func bucketOffset(bucketIndex uint32) int64 {
	return int64(fileHeaderSize) + int64(bucketSize*bucketIndex)
}

func (t *Table) newBucketIterator(startBucket uint32) *bucketIterator {
	return &bucketIterator{
		currentFile:  t.mainFile,
		overflowFile: t.overflowFile,
		offset:       bucketOffset(startBucket),
		slotValueLen: t.options.SlotValueLength,
	}
}

func (bi *bucketIterator) next() (*bucket, error) {
	//  got the end of iteration
	if bi.offset == 0 {
		return nil, io.EOF
	}

	// read the bucket and get all solts in it
	bucket, err := bi.readBucket()
	if err != nil {
		return nil, err
	}

	// move to next bucket
	bi.offset = bucket.nextOffset
	bi.currentFile = bi.overflowFile
	return bucket, nil
}

func (bi *bucketIterator) readBucket() (*bucket, error) {
	bucketBuf := make([]byte, bucketSize)
	if _, err := bi.currentFile.ReadAt(bucketBuf, bi.offset); err != nil {
		return nil, err
	}

	b := &bucket{file: bi.currentFile, offset: bi.offset}
	// parse and get slots in the bucket
	for i := 0; i < slotsPerBucket; i++ {
		_ = bucketBuf[4+bi.slotValueLen]
		b.slots[i].Hash = binary.LittleEndian.Uint32(bucketBuf[:4])
		if b.slots[i].Hash != 0 {
			b.slots[i].Value = bucketBuf[4 : 4+bi.slotValueLen]
		}
		bucketBuf = bucketBuf[4+bi.slotValueLen:]
	}
	// the last 8 bytes is the offset of next bucket
	b.nextOffset = int64(binary.LittleEndian.Uint64(bucketBuf[:8]))

	return b, nil
}

func (b *bucket) writeSlot() error {
	slotLen := 4 + len(b.newSlot.Value)
	buf := make([]byte, slotLen)
	binary.LittleEndian.PutUint32(buf[:4], b.newSlot.Hash)
	copy(buf[4:], b.newSlot.Value)

	writeOff := b.offset + int64((b.newSlotIndex)*slotLen)
	_, err := b.file.WriteAt(buf, writeOff)

	return err
}
