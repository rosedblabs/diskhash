package diskhash

import (
	"encoding/binary"
	"io"

	"github.com/lotusdblabs/diskhash/fs"
)

const slotsPerBucket = 31

type bucket struct {
	slots      [slotsPerBucket]Slot
	offset     int64
	nextOffset int64
	file       fs.File
}

type bucketIterator struct {
	currentFile  fs.File
	overflowFile fs.File
	offset       int64
	slotValueLen uint32
}

type Slot struct {
	Hash  uint32
	Value []byte
}

type slotWriter struct {
	currentBucket    *bucket
	currentSlotIndex int
	prevBuckets      []*bucket
	overwrite        bool
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

func (sw *slotWriter) insertSlot(sl Slot, t *Table) error {
	if sw.currentSlotIndex == slotsPerBucket {
		nextBucket, err := t.createOverflowBucket()
		if err != nil {
			return err
		}
		sw.currentBucket.nextOffset = nextBucket.offset
		sw.prevBuckets = append(sw.prevBuckets, sw.currentBucket)
		sw.currentBucket = nextBucket
		sw.currentSlotIndex = 0
	}
	sw.currentBucket.slots[sw.currentSlotIndex] = sl
	sw.currentSlotIndex++
	return nil
}

func (sw *slotWriter) writeSlots() error {
	for i := len(sw.prevBuckets) - 1; i >= 0; i-- {
		if err := sw.prevBuckets[i].write(); err != nil {
			return err
		}
	}
	return sw.currentBucket.write()
}

func (sw *slotWriter) writeSingleSlot() error {
	if len(sw.prevBuckets) > 0 {
		writeOff := sw.prevBuckets[0].offset + bucketSize - 8
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(sw.prevBuckets[0].nextOffset))
		_, err := sw.prevBuckets[0].file.WriteAt(buf, writeOff)
		if err != nil {
			return err
		}
	}

	slot := sw.currentBucket.slots[sw.currentSlotIndex-1]
	buf := make([]byte, len(slot.Value)+4)
	binary.LittleEndian.PutUint32(buf[:4], slot.Hash)
	copy(buf[4:], slot.Value)
	_, err := sw.currentBucket.file.WriteAt(buf, sw.currentBucket.offset)

	return err
}

func (b *bucket) write() error {
	buf := make([]byte, bucketSize)
	data := buf
	for i := 0; i < slotsPerBucket; i++ {
		slot := b.slots[i]
		binary.LittleEndian.PutUint32(buf[:4], slot.Hash)
		copy(buf[4:4+len(slot.Value)], slot.Value)
		buf = buf[4+len(slot.Value):]
	}
	binary.LittleEndian.PutUint64(buf[:8], uint64(b.nextOffset))

	_, err := b.file.WriteAt(data, b.offset)
	return err
}
