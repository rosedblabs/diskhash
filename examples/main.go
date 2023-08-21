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
