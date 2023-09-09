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
	primaryFileName  = "HASH.PRIMARY"
	overflowFileName = "HASH.OVERFLOW"
	metaFileName     = "HASH.META"
	slotsPerBucket   = 31
	nextOffLen       = 8
	hashLen          = 4
)

// MatchKeyFunc is used to determine whether the key of the slot matches the stored key.
// And you must supply the function to the Put/Get/Delete methods.
//
// Why we need this function?
//
// When we store the data in the hash table, we only store the hash value of the key, and the raw value.
// So when we get the data from hash table, even if the hash value of the key matches, that doesn't mean
// the key matches because of hash collision.
// So we need to provide a function to determine whether the key of the slot matches the stored key.
type MatchKeyFunc func(Slot) (bool, error)

// Table is a hash table that stores data on disk.
// It consists of two files, the primary file and the overflow file.
// Each file is divided into multiple buckets, each bucket contains multiple slots.
type Table struct {
	primaryFile  fs.File
	overflowFile fs.File
	metaFile     fs.File // meta file stores the metadata of the hash table
	meta         *tableMeta
	mu           *sync.RWMutex // protect the table when multiple goroutines access it
	options      Options
}

// tableMeta is the metadata of the hash table.
type tableMeta struct {
	Level            uint8
	SplitBucketIndex uint32
	NumBuckets       uint32
	NumKeys          uint32
	SlotValueLength  uint32
	BucketSize       uint32
	FreeBuckets      []int64
}

// Open opens a hash table.
// If the hash table does not exist, it will be created automatically.
// It will open the primary file, the overflow file and the meta file.
func Open(options Options) (*Table, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

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

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("dir path cannot be empty")
	}
	if options.SlotValueLength <= 0 {
		return errors.New("slot value length must be greater than 0")
	}
	if options.LoadFactor < 0 || options.LoadFactor > 1 {
		return errors.New("load factor must be between 0 and 1")
	}
	return nil
}

// read the metadata info from the meta file.
// if the file is empty, init the metadata info.
func (t *Table) readMeta() error {
	file, err := fs.Open(filepath.Join(t.options.DirPath, metaFileName), fs.OSFileSystem)
	if err != nil {
		return err
	}
	t.metaFile = file
	t.meta = &tableMeta{}

	// init meta file if not exist
	if file.Size() == 0 {
		t.meta.NumBuckets = 1
		t.meta.SlotValueLength = t.options.SlotValueLength
		t.meta.BucketSize = slotsPerBucket*(hashLen+t.meta.SlotValueLength) + nextOffLen
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

// write the metadata info to the meta file in json format.
func (t *Table) writeMeta() error {
	encoder := json.NewEncoder(t.metaFile)
	return encoder.Encode(t.meta)
}

// Close closes the files of the hash table.
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

// Sync flushes the data of the hash table to disk.
func (t *Table) Sync() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.primaryFile.Sync(); err != nil {
		return err
	}

	if err := t.overflowFile.Sync(); err != nil {
		return err
	}

	return nil
}

// Put puts a new ke/value pair to the hash table.
// the parameter matchKey is described in the MatchKeyFunc.
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
	// If there are no empty slots, an overflow bucket will be created.
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

// find a free slot position to insert the new slot.
// return the slot writer.
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
			// if the slot hash value is not equal to the key hash value,
			// which means the key will never be matched, so we can skip it.
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
		// no empty slot in the bucket and it's all overflow buckets,
		// create a new overflow bucket.
		if b.nextOffset == 0 {
			sw.currentSlotIndex = slotsPerBucket
			return sw, nil
		}
	}
}

// Get gets the value of the key from the hash table.
// the parameter matchKey is described in the MatchKeyFunc.
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
			// if the slot hash value is 0, which means the subsequent slots are all empty,
			// (why? when we write a new slot, we will iterate from the beginning of the bucket, find an empty slot to insert,
			// when we remove a slot, we will move the subsequent slots forward, so all non-empty slots will be continuous)
			// so we can skip the current bucket and move to the next bucket.
			if slot.Hash == 0 {
				break
			}
			// if the slot hash value is not equal to the key hash value,
			// which means the key will never be matched, so we can skip it.
			if slot.Hash != keyHash {
				continue
			}
			if match, err := matchKey(slot); match || err != nil {
				return err
			}
		}
	}
}

// Delete deletes the key from the hash table.
// the parameter matchKey is described in the MatchKeyFunc.
func (t *Table) Delete(key []byte, matchKey MatchKeyFunc) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// get the bucket according to the key hash
	keyHash := getKeyHash(key)
	bi := t.newBucketIterator(t.getKeyBucket(keyHash))
	// iterate all slots in the bucket and the overflow buckets,
	// find the slot to delete.
	for {
		b, err := bi.next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// the following code is similar to the Get method
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

// Size returns the number of keys in the hash table.
func (t *Table) Size() uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.meta.NumKeys
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
	// init (dummy) file header
	// the first bucket size in the file is not used, so we just init it.
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
