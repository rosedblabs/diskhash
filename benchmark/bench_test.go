package benchmark

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/rosedblabs/diskhash"
	"github.com/stretchr/testify/assert"
)

func newDB() (*diskhash.Table, func()) {
	options := diskhash.DefaultOptions
	options.SlotValueLength = 16
	options.DirPath = "/tmp/diskhash-bench"
	db, err := diskhash.Open(options)
	if err != nil {
		panic(err)
	}

	return db, func() {
		_ = db.Close()
		_ = os.RemoveAll(options.DirPath)
	}
}

func BenchmarkPut(b *testing.B) {
	db, closer := newDB()
	defer closer()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(GetTestKey(i), []byte(strings.Repeat("d", 16)), func(slot diskhash.Slot) (bool, error) {
			return false, nil
		})
		assert.Nil(b, err)
	}
}

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("diskhash-test-key-%09d", i))
}
