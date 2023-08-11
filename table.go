package diskhash

import (
	"encoding/json"
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

	meta, err := readMeta(mainFile)
	if err != nil {
		return nil, err
	}
	t.meta = meta

	// init first bucket
	if mainFile.Size() == fileHeaderSize {
		if err := mainFile.Truncate(bucketSize); err != nil {
			_ = mainFile.Close()
			return nil, err
		}
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

func readMeta(file fs.File) (*tableMeta, error) {
	var meta *tableMeta
	if file.Size() == fileHeaderSize {
		meta = &tableMeta{
			Level:            0,
			SplitBucketIndex: 0,
			NumBuckets:       1,
			NumKeys:          0,
		}
	} else {
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(meta); err != nil {
			return nil, err
		}
	}
	return meta, nil
}

func (t *Table) writeMeta() error {
	encoder := json.NewEncoder(t.mainFile)
	return encoder.Encode(t.meta)
}

func (t *Table) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.writeMeta(); err != nil {
		return err
	}

	_ = t.overflowFile.Close()
	_ = t.mainFile.Close()
	return nil
}

func (t *Table) Put(key, value []byte, matchKey MatchKeyFunc) error {
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
	if err := sw.writeSingleSlot(); err != nil {
		return err
	}
	t.meta.NumKeys++
	if sw.overwrite {
		return nil
	}

	keyRatio := float64(t.meta.NumKeys) / float64(t.meta.NumBuckets*slotsPerBucket)
	// expand if necessary
	if keyRatio > t.options.LoadFactor {
		if err := t.expand(); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) getSlotWriter(keyHash uint32, matchKey MatchKeyFunc) (*slotWriter, error) {
	sw := &slotWriter{}
	bi := t.newBucketIterator(t.getKeyBucket(keyHash))
	for {
		b, err := bi.next()
		if err == io.EOF {
			return nil, errors.New("failed to put new slot")
		}
		if err != nil {
			return nil, err
		}

		sw.currentBucket = b
		// iterate all slots in the bucket, find the slot to insert
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
			if match {
				sw.currentSlotIndex, sw.overwrite = i, true
				return sw, nil
			}
		}
		if b.nextOffset == 0 {
			sw.currentSlotIndex = slotsPerBucket
			return sw, nil
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

func openFile(name string) (fs.File, error) {
	file, err := fs.Open(name, fs.OSFileSystem)
	if err != nil {
		return nil, err
	}
	// init file header
	if file.Size() == 0 {
		if err := file.Truncate(fileHeaderSize); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return file, nil
}

func (t *Table) expand() error {
	updatedBucketIndex := t.meta.SplitBucketIndex
	updatedSlotWriter := &slotWriter{
		currentBucket: &bucket{
			file:   t.mainFile,
			offset: bucketOffset(updatedBucketIndex),
		},
	}

	newSlotWriter := &slotWriter{
		currentBucket: &bucket{
			file:   t.mainFile,
			offset: t.mainFile.Size(),
		},
	}

	if err := t.mainFile.Truncate(bucketSize); err != nil {
		return err
	}

	t.meta.SplitBucketIndex++
	if t.meta.SplitBucketIndex == 1<<t.meta.Level {
		t.meta.Level++
		t.meta.SplitBucketIndex = 0
	}

	bi := t.newBucketIterator(updatedBucketIndex)
	for {
		b, err := bi.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		for _, slot := range b.slots {
			var insertErr error
			if t.getKeyBucket(slot.Hash) == updatedBucketIndex {
				insertErr = updatedSlotWriter.insertSlot(slot, t)
			} else {
				insertErr = newSlotWriter.insertSlot(slot, t)
			}
			if insertErr != nil {
				return insertErr
			}
		}
	}

	if err := updatedSlotWriter.writeSlots(); err != nil {
		return err
	}
	if err := newSlotWriter.writeSlots(); err != nil {
		return err
	}

	t.meta.NumBuckets++
	return nil
}

func (t *Table) createOverflowBucket() (*bucket, error) {
	return nil, nil
}
