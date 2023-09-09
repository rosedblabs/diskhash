package diskhash

import "os"

// Options is used to create a new diskhash table.
type Options struct {
	// DirPath is the directory path to store the hash table files.
	DirPath string

	// SlotValueLength is the length of the value in each slot.
	// Your value lenght must be equal to the value length you set when creating the table.
	SlotValueLength uint32

	// LoadFactor is the load factor of the hash table.
	// The load factor is the ratio of the number of elements in the hash table to the table size.
	// If the ratio is greater than the load factor, the hash table will be expanded automatically.
	// The default value is 0.7.
	LoadFactor float64
}

// DefaultOptions is the default options.
var DefaultOptions = Options{
	DirPath:         os.TempDir(),
	SlotValueLength: 0,
	LoadFactor:      0.7,
}
