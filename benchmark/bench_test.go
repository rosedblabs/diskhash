package benchmark

import (
	"fmt"
	"github.com/rosedblabs/diskhash"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

var t *diskhash.Table

func init() {
	options := diskhash.DefaultOptions
	options.SlotValueLength = 16
	options.DirPath = "/tmp/diskhash-bench"
	var err error
	t, err = diskhash.Open(options)
	if err != nil {
		panic(err)
	}
}

func BenchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := t.Put(GetTestKey(i), []byte(strings.Repeat("d", 16)), func(slot diskhash.Slot) (bool, error) {
			return false, nil
		})
		assert.Nil(b, err)
	}
}

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("diskhash-test-key-%09d", i))
}
