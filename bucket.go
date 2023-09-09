package diskhash

import (
	"encoding/binary"
	"io"

	"github.com/rosedblabs/diskhash/fs"
)

// bucket is the basic unit of a file in diskhash.
// each file contains 31 slots at most.
type bucket struct {
	slots      [slotsPerBucket]Slot // 31 slots now
	offset     int64                // the offset of the bucket in the file
	nextOffset int64                // the offset of the next overflow bucket
	file       fs.File              // the file that contains the bucket
	bucketSize uint32
}

// bucketIterator is used to iterate all buckets in hash table.
type bucketIterator struct {
	currentFile  fs.File
	overflowFile fs.File
	offset       int64

	slotValueLen uint32
	bucketSize   uint32
}

// Slot is the basic unit of a bucket.
// each slot contains a key hash and a value.
type Slot struct {
	Hash  uint32 // the hash of the key
	Value []byte // raw value
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

// readBucket reads a bucket from the current file.
func (bi *bucketIterator) readBucket() (*bucket, error) {
	// read an entire bucket with all slots
	bucketBuf := make([]byte, bi.bucketSize)
	if _, err := bi.currentFile.ReadAt(bucketBuf, bi.offset); err != nil {
		return nil, err
	}

	b := &bucket{file: bi.currentFile, offset: bi.offset, bucketSize: bi.bucketSize}
	// parse and get slots in the bucket
	for i := 0; i < slotsPerBucket; i++ {
		_ = bucketBuf[hashLen+bi.slotValueLen]
		b.slots[i].Hash = binary.LittleEndian.Uint32(bucketBuf[:hashLen])
		if b.slots[i].Hash != 0 {
			b.slots[i].Value = bucketBuf[hashLen : hashLen+bi.slotValueLen]
		}
		bucketBuf = bucketBuf[hashLen+bi.slotValueLen:]
	}

	// the last 8 bytes is the offset of next overflow bucket
	b.nextOffset = int64(binary.LittleEndian.Uint64(bucketBuf[:nextOffLen]))

	return b, nil
}

func (sw *slotWriter) insertSlot(sl Slot, t *Table) error {
	// if we exceed the slotsPerBucket, we need to create a new overflow bucket
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

// write all slots in the bucket to the file.
func (b *bucket) write() error {
	buf := make([]byte, b.bucketSize)
	// write all slots to the buffer
	var index = 0
	for i := 0; i < slotsPerBucket; i++ {
		slot := b.slots[i]

		binary.LittleEndian.PutUint32(buf[index:index+hashLen], slot.Hash)
		copy(buf[index+hashLen:index+hashLen+len(slot.Value)], slot.Value)

		index += hashLen + len(slot.Value)
	}

	// write the offset of next overflow bucket
	binary.LittleEndian.PutUint64(buf[len(buf)-nextOffLen:], uint64(b.nextOffset))

	_, err := b.file.WriteAt(buf, b.offset)
	return err
}

// remove a slot from the bucket, and move all slots after it forward
// to fill the empty slot.
func (b *bucket) removeSlot(slotIndex int) {
	i := slotIndex
	for ; i < slotsPerBucket-1; i++ {
		b.slots[i] = b.slots[i+1]
	}
	b.slots[i] = Slot{}
}
