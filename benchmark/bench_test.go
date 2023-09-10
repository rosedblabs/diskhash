package benchmark

import (
	"fmt"
	"github.com/spaolacci/murmur3"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/rosedblabs/diskhash"
	"github.com/stretchr/testify/assert"
)

func newTable() (*diskhash.Table, func()) {
	options := diskhash.DefaultOptions
	options.SlotValueLength = 20
	options.DirPath = "/tmp/diskhash-bench"
	table, err := diskhash.Open(options)
	if err != nil {
		panic(err)
	}

	return table, func() {
		_ = table.Close()
		_ = os.RemoveAll(options.DirPath)
	}
}

func BenchmarkPut(b *testing.B) {
	table, destroy := newTable()
	defer destroy()

	b.ResetTimer()
	b.ReportAllocs()

	value := []byte(strings.Repeat("d", 20))
	for i := 0; i < b.N; i++ {
		err := table.Put(GetTestKey(i), value, func(slot diskhash.Slot) (bool, error) {
			return false, nil
		})
		assert.Nil(b, err)
	}
}

func BenchmarkGet(b *testing.B) {
	table, destroy := newTable()
	defer destroy()

	value := []byte(strings.Repeat("d", 20))
	for i := 0; i < 100000; i++ {
		err := table.Put(GetTestKey(i), value, func(slot diskhash.Slot) (bool, error) {
			return false, nil
		})
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := GetTestKey(rand.Intn(100000))
		err := table.Get(key, func(slot diskhash.Slot) (bool, error) {
			hash := murmur3.Sum32(key)
			if hash == slot.Hash {
				return true, nil
			}
			return false, nil
		})
		assert.Nil(b, err)
	}
}

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("diskhash-test-key-%09d", i))
}
