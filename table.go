package diskhash

import (
	"errors"
	"io"
	"os"
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

type MatchKeyFunc func(Slot) (bool, error)

type Table struct {
	mainFile     fs.File
	overflowFile fs.File
	mu           *sync.RWMutex
	options      Options
	meta         *tableMeta
}

type tableMeta struct {
	Level            uint8
	SplitBucketIndex uint32
	NumBuckets       uint32
	NumKeys          uint32
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

	// open and init main file
	mainFile, err := openFile(filepath.Join(options.DirPath, mainFileName))
	if err != nil {
		return nil, err
	}
	// init first bucket
	if mainFile.Size() == fileHeaderSize {
		if err := mainFile.Truncate(bucketSize); err != nil {
			_ = mainFile.Close()
			return nil, err
		}
	} else if err = t.readMeta(); err != nil {
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
	insertionBucket, err := t.getInsertionBucket(slot.Hash, matchKey)
	if err != nil {
		return err
	}
	insertionBucket.newSlot = slot

	// write the new slot to the bucket
	if err := insertionBucket.writeSlot(); err != nil {
		return err
	}
	t.meta.NumKeys++

	ratio := float64(t.meta.NumKeys) / float64(t.meta.NumBuckets*slotsPerBucket)
	// expand if necessary
	if ratio > t.options.LoadFactor {
		if err := t.expand(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) getInsertionBucket(keyHash uint32, matchKey MatchKeyFunc) (*bucket, error) {
	bucketIterator := t.newBucketIterator(t.getKeyBucket(keyHash))
	for {
		b, err := bucketIterator.next()
		if err == io.EOF {
			return nil, errors.New("failed to put new slot")
		}
		if err != nil {
			return nil, err
		}

		// iterate all slots in the bucket, find the slot to insert
		for i, slot := range b.slots {
			// find an empty slot to insert
			if slot.Hash == 0 {
				b.newSlotIndex = i
				return b, nil
			}
			if slot.Hash != keyHash {
				continue
			}
			match, err := matchKey(slot)
			if err != nil {
				return nil, err
			}
			if match {
				b.newSlotIndex = i
				return b, nil
			}
		}

		if b.nextOffset == 0 {
			offset := t.overflowFile.Size()
			if err := t.overflowFile.Truncate(bucketSize); err != nil {
				return nil, err
			}
			overflowBucket := &bucket{
				file:         t.overflowFile,
				newSlotIndex: 0,
				offset:       offset,
			}
			return overflowBucket, nil
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
	b := keyHash & ((1 << t.meta.Level) - 1)
	if b < t.meta.SplitBucketIndex {
		return keyHash & ((1 << (t.meta.Level + 1)) - 1)
	}
	return b
}

func (t *Table) readMeta() error {
	return nil
}

func openFile(name string) (fs.File, error) {
	file, err := fs.Open(name, fs.OSFileSystem)
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

func (t *Table) expand() error {
	return nil
}
