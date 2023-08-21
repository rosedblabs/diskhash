# diskhash
on-disk hash table index(mainly for WAL).

## When you will need it?
if you are using [WAL](https://github.com/rosedblabs/wal) to store your data,

> wal: https://github.com/rosedblabs/wal

you will get the positions to get the data from WAL, the common way to store the positions is to use an in-memory index, but if you have a large amount of data, the index will be very large, and it will take a lot of time to load the index into memory when you restart the program.

so, you can use diskhash to store the index on disk.

## Can be used as a general hash table index(without wal)?

yes, you can use it as an on-disk hash table index, but the restriction is that the value must be fixed size.
you can set the value size when you create the index, and once you set the value size, you can't change it.

But don't set the value size too large(1KB), the disk size maybe increase dramatically because of the write amplification.
**it is suitable for storing some metadata of your system.**

## Design Overview
The diskhash consists of two disk files: main and overflow.
The file format is as follows:
```
File Format:
+---------------+---------------+---------------+---------------+-----+----------------+
|    (unused)   |    bucket0    |    bucket1    |    bucket2    | ... |     bucketN    |
+---------------+---------------+---------------+---------------+-----+----------------+
```

A file is divided into multiple buckets, if the table reaches the load factor, a new bucket will be appended to the end of the file.
A bucket contains 31 slots, and an overflow offset which points to the overflow file buckets.
```
Bucket Format:
+---------------+---------------+---------------+---------------+-----+----------------+-----------------+
|    slot0      |    slot1      |    slot2      |    slot3      | ... |     slotN      | overflow_offset |
+---------------+---------------+---------------+---------------+-----+----------------+-----------------+
```

A slot contains a key hash value, and user-defined value.
```
Slot Format:
+-----------------------+--------------------------------+
|      key_hash(4B)     |          value(N Bytes)        |
+-----------------------+--------------------------------+
```

## Getting Started
```go
package main

import (
	"fmt"
	"github.com/rosedblabs/diskhash"
	"strings"
)

func main() {
    // open the table, specify the slot value length,
    // remember that you can't change it once you set it, and all values must be the same length.
	options := diskhash.DefaultOptions
	options.DirPath = "/tmp/diskhash-test"
	options.SlotValueLength = 10
	table, err := diskhash.Open(options)
	if err != nil {
		panic(err)
	}

    // don't forget to close the table!!!
    // some meta info will be saved when you close the table.
	defer func() {
		_ = table.Close()
	}()

    // put a key-value pair into the table.
    // the MatchKey function will be called when the key is matched.
    // Why we need the MatchKey function?
    // because the key may be hashed to the same slot with another key(even though the probability is very low),
    // so we need to check if the key is matched.
	err = table.Put([]byte("key1"), []byte(strings.Repeat("v", 10)), func(slot diskhash.Slot) (bool, error) {
		return true, nil
	})
	if err != nil {
		panic(err)
	}

	err = table.Get([]byte("key1"), func(slot diskhash.Slot) (bool, error) {
		fmt.Println("val =", string(slot.Value))
		return true, nil
	})
	if err != nil {
		panic(err)
	}

	err = table.Delete([]byte("key1"), func(slot diskhash.Slot) (bool, error) {
		return true, nil
	})
	if err != nil {
		panic(err)
	}
}
```
