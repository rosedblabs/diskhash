package diskhash

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/rosedblabs/diskhash/fs"
	"github.com/spaolacci/murmur3"
)

const (
	primaryFileName     = "HASH.PRIMARY"
	overflowFileName    = "HASH.OVERFLOW"
	metaFileName        = "HASH.META"
	bucketNextOffsetLen = 8
)

type MatchKeyFunc func(Slot) (bool, error)

type Table struct {
	primaryFile  fs.File
	overflowFile fs.File
	metaFile     fs.File
	meta         *tableMeta
	mu           *sync.RWMutex
	options      Options
}

type tableMeta struct {
	Level            uint8
	SplitBucketIndex uint32
	NumBuckets       uint32
	NumKeys          uint32
	SlotValueLength  uint32
	BucketSize       uint32
	FreeBuckets      []int64
}

func Open(options Options) (*Table, error) {
	t := &Table{
		mu:      new(sync.RWMutex),
		options: options,
	}

	// create data directory if not exist
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// open meta file and read metadata info
	if err := t.readMeta(); err != nil {
		return nil, err
	}

	// open and init primary file
	primaryFile, err := t.openFile(primaryFileName)
	if err != nil {
		return nil, err
	}
	// init first bucket if the primary file is empty
	if primaryFile.Size() == int64(t.meta.BucketSize) {
		if err := primaryFile.Truncate(int64(t.meta.BucketSize)); err != nil {
			_ = primaryFile.Close()
			return nil, err
		}
	}
	t.primaryFile = primaryFile

	// open overflow file
	overflowFile, err := t.openFile(overflowFileName)
	if err != nil {
		return nil, err
	}
	t.overflowFile = overflowFile

	return t, nil
}

func (t *Table) readMeta() error {
	// init meta file if not exist
	if t.metaFile == nil {
		file, err := fs.Open(filepath.Join(t.options.DirPath, metaFileName), fs.OSFileSystem)
		if err != nil {
			return err
		}
		t.meta = &tableMeta{
			NumBuckets:      1,
			SlotValueLength: t.options.SlotValueLength,
		}
		t.meta.BucketSize = slotsPerBucket*(4+t.meta.SlotValueLength) + bucketNextOffsetLen
		t.metaFile = file
	} else {
		decoder := json.NewDecoder(t.metaFile)
		if err := decoder.Decode(t.meta); err != nil {
			return err
		}
		// we require that the slot value length must be equal to the length specified in the options,
		// once the slot value length is set, it cannot be changed.
		if t.meta.SlotValueLength != t.options.SlotValueLength {
			return errors.New("slot value length mismatch")
		}
	}

	return nil
}

func (t *Table) writeMeta() error {
	encoder := json.NewEncoder(t.metaFile)
	return encoder.Encode(t.meta)
}

func (t *Table) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.writeMeta(); err != nil {
		return err
	}

	_ = t.primaryFile.Close()
	_ = t.overflowFile.Close()
	_ = t.metaFile.Close()
	return nil
}

func (t *Table) Put(key, value []byte, matchKey MatchKeyFunc) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// the value length must be equal to the length specified in the options
	if len(value) != int(t.meta.SlotValueLength) {
		return errors.New("value length must be equal to the length specified in the options")
	}

	// get the slot writer to write the new slot,
	// it will get the corresponding bucket according to the key hash,
	// and find an empty slot to insert.
	// If there are no empty slots, a overflow bucket will be created.
	keyHash := getKeyHash(key)
	slot := &Slot{Hash: keyHash, Value: value}
	sw, err := t.getSlotWriter(slot.Hash, matchKey)
	if err != nil {
		return err
	}

	// write the new slot to the bucket
	if err = sw.insertSlot(*slot, t); err != nil {
		return err
	}
	if err := sw.writeSlots(); err != nil {
		return err
	}
	// if the slot already exists, no need to update meta
	// because the number of keys has not changed
	if sw.overwrite {
		return nil
	}

	t.meta.NumKeys++
	// split if the load factor is exceeded
	keyRatio := float64(t.meta.NumKeys) / float64(t.meta.NumBuckets*slotsPerBucket)
	if keyRatio > t.options.LoadFactor {
		if err := t.split(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) getSlotWriter(keyHash uint32, matchKey MatchKeyFunc) (*slotWriter, error) {
	sw := &slotWriter{}
	bi := t.newBucketIterator(t.getKeyBucket(keyHash))
	// iterate all slots in the bucket and the overflow buckets,
	// find the slot to insert.
	for {
		b, err := bi.next()
		if err == io.EOF {
			return nil, errors.New("failed to put new slot")
		}
		if err != nil {
			return nil, err
		}

		sw.currentBucket = b
		// iterate all slots in current bucket, find the existing or empty slot
		for i, slot := range b.slots {
			// find an empty slot to insert
			if slot.Hash == 0 {
				sw.currentSlotIndex = i
				return sw, nil
			}
			if slot.Hash != keyHash {
				continue
			}
			match, err := matchKey(slot)
			if err != nil {
				return nil, err
			}
			// key already exists, overwrite the value
			if match {
				sw.currentSlotIndex, sw.overwrite = i, true
				return sw, nil
			}
		}
		// no empty slot in the bucket and its all overflow buckets,
		// create a new overflow bucket.
		if b.nextOffset == 0 {
			sw.currentSlotIndex = slotsPerBucket
			return sw, nil
		}
	}
}

func (t *Table) Get(key []byte, matchKey MatchKeyFunc) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// get the bucket according to the key hash
	keyHash := getKeyHash(key)
	startBucket := t.getKeyBucket(keyHash)
	bi := t.newBucketIterator(startBucket)
	// iterate all slots in the bucket and the overflow buckets,
	// find the slot to get.
	for {
		b, err := bi.next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		for _, slot := range b.slots {
			if slot.Hash == 0 {
				break
			}
			if slot.Hash != keyHash {
				continue
			}
			if match, err := matchKey(slot); match || err != nil {
				return err
			}
		}
	}
}

func (t *Table) Delete(key []byte, matchKey MatchKeyFunc) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	keyHash := getKeyHash(key)
	bi := t.newBucketIterator(t.getKeyBucket(keyHash))
	for {
		b, err := bi.next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		for i, slot := range b.slots {
			if slot.Hash == 0 {
				break
			}
			if slot.Hash != keyHash {
				continue
			}
			match, err := matchKey(slot)
			if err != nil {
				return err
			}
			if !match {
				continue
			}
			// now we find the slot to delete, remove it from the bucket
			b.removeSlot(i)
			if err := b.write(); err != nil {
				return err
			}
			t.meta.NumKeys--
			return nil
		}
	}
}

// get the hash value according to the key
func getKeyHash(key []byte) uint32 {
	return murmur3.Sum32(key)
}

// get the bucket according to the key
func (t *Table) getKeyBucket(keyHash uint32) uint32 {
	bucketIndex := keyHash & ((1 << t.meta.Level) - 1)
	if bucketIndex < t.meta.SplitBucketIndex {
		return keyHash & ((1 << (t.meta.Level + 1)) - 1)
	}
	return bucketIndex
}

func (t *Table) openFile(name string) (fs.File, error) {
	file, err := fs.Open(filepath.Join(t.options.DirPath, name), fs.OSFileSystem)
	if err != nil {
		return nil, err
	}
	// init file header
	if file.Size() == 0 {
		if err := file.Truncate(int64(t.meta.BucketSize)); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return file, nil
}

func (t *Table) split() error {
	splitBucketIndex := t.meta.SplitBucketIndex
	splitSlotWriter := &slotWriter{
		currentBucket: &bucket{
			file:       t.primaryFile,
			offset:     t.bucketOffset(splitBucketIndex),
			bucketSize: t.meta.BucketSize,
		},
	}

	// create a new bucket
	newSlotWriter := &slotWriter{
		currentBucket: &bucket{
			file:       t.primaryFile,
			offset:     t.primaryFile.Size(),
			bucketSize: t.meta.BucketSize,
		},
	}
	if err := t.primaryFile.Truncate(int64(t.meta.BucketSize)); err != nil {
		return err
	}

	t.meta.SplitBucketIndex++
	if t.meta.SplitBucketIndex == 1<<t.meta.Level {
		t.meta.Level++
		t.meta.SplitBucketIndex = 0
	}

	// iterate all slots in the split bucket and the overflow buckets,
	// rewrite all slots.
	var freeBuckets []int64
	bi := t.newBucketIterator(splitBucketIndex)
	for {
		b, err := bi.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		for _, slot := range b.slots {
			if slot.Hash == 0 {
				break
			}
			var insertErr error
			if t.getKeyBucket(slot.Hash) == splitBucketIndex {
				insertErr = splitSlotWriter.insertSlot(slot, t)
			} else {
				insertErr = newSlotWriter.insertSlot(slot, t)
			}
			if insertErr != nil {
				return insertErr
			}
		}
		// if the splitBucket has overflow buckets, and these buckets are no longer used,
		// because all slots has been rewritten, so we can free these buckets.
		if b.nextOffset != 0 {
			freeBuckets = append(freeBuckets, b.nextOffset)
		}
	}

	// collect the free buckets
	if len(freeBuckets) > 0 {
		t.meta.FreeBuckets = append(t.meta.FreeBuckets, freeBuckets...)
	}

	// write all slots to the file
	if err := splitSlotWriter.writeSlots(); err != nil {
		return err
	}
	if err := newSlotWriter.writeSlots(); err != nil {
		return err
	}

	t.meta.NumBuckets++
	return nil
}

func (t *Table) createOverflowBucket() (*bucket, error) {
	var offset int64
	if len(t.meta.FreeBuckets) > 0 {
		offset = t.meta.FreeBuckets[0]
		t.meta.FreeBuckets = t.meta.FreeBuckets[1:]
	} else {
		offset = t.overflowFile.Size()
		err := t.overflowFile.Truncate(int64(t.meta.BucketSize))
		if err != nil {
			return nil, err
		}
	}

	return &bucket{
		file:       t.overflowFile,
		offset:     offset,
		bucketSize: t.meta.BucketSize,
	}, nil
}
