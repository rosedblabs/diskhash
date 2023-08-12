package diskhash

import (
	"fmt"
	"strings"
	"testing"

	"github.com/spaolacci/murmur3"
)

func TestOpen(t *testing.T) {
	options := Options{DirPath: "/tmp/disk-hash", SlotValueLength: 20, LoadFactor: 0.7}
	table, err := Open(options)
	if err != nil {
		t.Error(err)
	}
	defer table.Close()

	matchKey := func(slot Slot) (bool, error) {
		return false, nil
	}

	for i := 0; i < 30000; i++ {
		table.Put(GetTestKey(i), []byte(strings.Repeat("X", 20)), matchKey)
	}

	for i := 0; i < 5; i++ {
		key := GetTestKey(i)
		match := func(slot Slot) (bool, error) {
			if slot.Hash == murmur3.Sum32(key) {
				t.Log("find ", slot)
				return true, nil
			}
			return false, nil
		}

		err := table.Get(key, match)
		if err != nil {
			t.Error(err)
		}
	}
}

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("diskhash-test-key-%09d", i))
}
