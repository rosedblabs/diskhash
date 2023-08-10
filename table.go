package diskhash

import (
	"errors"
	"io"
	"path/filepath"
	"sync"

	"github.com/lotusdblabs/diskhash/fs"
	"github.com/spaolacci/murmur3"
)

const (
	mainFileName     = "INDEX.MAIN"
	overflowFileName = "INDEX.OVERFLOW"

	fileHeaderSize = 512
	bucketSize     = 512
)

type MatchKeyFunc func(*Slot) (bool, error)

type Table struct {
	mainFile         fs.File
	overflowFile     fs.File
	mu               *sync.RWMutex
	level            uint8
	splitBucketIndex uint32
	numBuckets       uint32
}

type tableMeta struct {
	splitBucketIndex int
	numBuckets       int
}

func Open(options Options) (*Table, error) {
	t := &Table{
		mu:               new(sync.RWMutex),
		level:            0,
		splitBucketIndex: 0,
	}

	// open and init main file
	mainFile, err := openFile(filepath.Join(options.DirPath, mainFileName))
	if err != nil {
		return nil, err
	}
	// init first bucket
	if mainFile.Size() == fileHeaderSize {
		if err := mainFile.Truncate(bucketSize); err != nil {
			_ = mainFile.Close()
		}
	} else if err = t.readMetadata(); err != nil {
		return nil, err
	}
	t.mainFile = mainFile

	// open overflow file
	overflowFile, err := openFile(filepath.Join(options.DirPath, overflowFileName))
	if err != nil {
		return nil, err
	}
	t.overflowFile = overflowFile

	return t, nil
}

func (t *Table) Put(key, value []byte, matchKey MatchKeyFunc) error {
	keyHash := getKeyHash(key)
	slot := &Slot{Hash: keyHash, Value: value}
	insertionBucket, err := t.getInsertionBucket(slot, matchKey)
	if err != nil {
		return err
	}
	insertionBucket.newSlot = slot

	// write
	return nil
}

func (t *Table) getInsertionBucket(newSlot *Slot, matchKey MatchKeyFunc) (*bucket, error) {
	bucketIterator := t.newBucketIterator(t.getKeyBucket(newSlot.Hash))
	for {
		bucket, err := bucketIterator.next()
		if err == io.EOF {
			return nil, errors.New("failed to put new slot")
		}
		if err != nil {
			return nil, err
		}

		for i, slot := range bucket.slots {
			// find an empty slot to insert
			if slot.Hash == 0 {
				bucket.newSlotIndex = i
				return bucket, nil
			}
			if slot.Hash != newSlot.Hash {
				continue
			}
			match, err := matchKey(slot)
			if err != nil {
				return bucket, err
			}
			if match {
				bucket.newSlotIndex = i
			}
		}
		if bucket.overflowOff == 0 {
			bucket.newSlotIndex = slotsPerBucket
			return bucket, nil
		}
	}
}

func (t *Table) Get() error {
	return nil
}

func (t *Table) Delete() error {
	return nil
}

// get the hash value according to the key
func getKeyHash(key []byte) uint32 {
	return murmur3.Sum32(key)
}

// get the bucket according to the key
func (t *Table) getKeyBucket(keyHash uint32) uint32 {
	b := keyHash & ((1 << t.level) - 1)
	if b < t.splitBucketIndex {
		return keyHash & ((1 << (t.level + 1)) - 1)
	}
	return b
}

func (t *Table) readMetadata() error {
	return nil
}

func openFile(name string) (fs.File, error) {
	file, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	if file.Size() == 0 {
		if err := file.Truncate(fileHeaderSize); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return file, nil
}
