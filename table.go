package diskhash

import "sync"

type Table struct {
	level            int
	splitBucketIndex int
	mu               *sync.RWMutex
}

func New(options Options) *Table {
	return &Table{
		level:            0,
		splitBucketIndex: 0,
		mu:               new(sync.RWMutex),
	}
}

func (t *Table) Put() error {
	return nil
}

func (t *Table) Get() error {
	return nil
}

func (t *Table) Delete() error {
	return nil
}

// get the bucket according to the key
func (t *Table) getKeyBucket() int {
	return -1
}
