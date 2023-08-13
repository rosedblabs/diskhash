package diskhash

import (
	"encoding/binary"
	"github.com/rosedblabs/diskhash/fs"
	"io"
)

const slotsPerBucket = 31

type bucket struct {
	slots      [slotsPerBucket]Slot
	offset     int64
	nextOffset int64
	file       fs.File
	bucketSize uint32
}

type bucketIterator struct {
	currentFile  fs.File
	overflowFile fs.File
	offset       int64

	slotValueLen uint32
	bucketSize   uint32
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

func (t *Table) bucketOffset(bucketIndex uint32) int64 {
	return int64((bucketIndex + 1) * t.meta.BucketSize)
}

func (t *Table) newBucketIterator(startBucket uint32) *bucketIterator {
	return &bucketIterator{
		currentFile:  t.primaryFile,
		overflowFile: t.overflowFile,
		offset:       t.bucketOffset(startBucket),
		slotValueLen: t.options.SlotValueLength,
		bucketSize:   t.meta.BucketSize,
	}
}

func (bi *bucketIterator) next() (*bucket, error) {
	// we skip the first bucket size in both primary and overflow file,
	// so the bucket offset will never be 0.
	if bi.offset == 0 {
		return nil, io.EOF
	}

	// read the bucket and get all solts in it
	bucket, err := bi.readBucket()
	if err != nil {
		return nil, err
	}

	// move to next overflow bucket
	bi.offset = bucket.nextOffset
	bi.currentFile = bi.overflowFile
	return bucket, nil
}

func (bi *bucketIterator) readBucket() (*bucket, error) {
	// read an entire bucket with all slots
	bucketBuf := make([]byte, bi.bucketSize)
	if _, err := bi.currentFile.ReadAt(bucketBuf, bi.offset); err != nil {
		return nil, err
	}

	b := &bucket{file: bi.currentFile, offset: bi.offset, bucketSize: bi.bucketSize}
	// parse and get slots in the bucket
	for i := 0; i < slotsPerBucket; i++ {
		_ = bucketBuf[4+bi.slotValueLen]
		b.slots[i].Hash = binary.LittleEndian.Uint32(bucketBuf[:4])
		if b.slots[i].Hash != 0 {
			b.slots[i].Value = bucketBuf[4 : 4+bi.slotValueLen]
		}
		bucketBuf = bucketBuf[4+bi.slotValueLen:]
	}

	// the last 8 bytes is the offset of next overflow bucket
	b.nextOffset = int64(binary.LittleEndian.Uint64(bucketBuf[:8]))

	return b, nil
}

func (sw *slotWriter) insertSlot(sl Slot, t *Table) error {
	// if we exeed the slotsPerBucket, we need to create a new overflow bucket
	// and link it to the current bucket
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

func (b *bucket) write() error {
	buf := make([]byte, b.bucketSize)
	data := buf
	// write all slots to the buffer
	for i := 0; i < slotsPerBucket; i++ {
		slot := b.slots[i]
		binary.LittleEndian.PutUint32(buf[:4], slot.Hash)
		copy(buf[4:4+len(slot.Value)], slot.Value)

		buf = buf[4+len(slot.Value):]
	}
	// write the offset of next overflow bucket
	binary.LittleEndian.PutUint64(buf[:8], uint64(b.nextOffset))

	_, err := b.file.WriteAt(data, b.offset)
	return err
}

func (b *bucket) removeSlot(slotIndex int) {
	i := slotIndex
	for ; i < slotsPerBucket-1; i++ {
		b.slots[i] = b.slots[i+1]
	}
	b.slots[i] = Slot{}
}
