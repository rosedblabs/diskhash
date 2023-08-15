package diskhash

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destroyTable(t *Table) {
	_ = t.Close()
	_ = os.RemoveAll(t.options.DirPath)
}

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("diskhash-test-key-%09d", i))
}

func TestOpen(t *testing.T) {
	dir, err := os.MkdirTemp("", "diskhash-test-open")
	assert.Nil(t, err)

	options := DefaultOptions
	options.DirPath = dir
	options.SlotValueLength = 10

	table, err := Open(options)
	assert.Nil(t, err)

	// if table is already open, reopening table raise error
	_, err = Open(options)
	assert.Equal(t, ErrDatabaseIsUsing, err)

	destroyTable(table)

	// after close table, open table again
	table, err = Open(options)
	assert.Nil(t, err)
	destroyTable(table)
}

func TestTable_Put(t *testing.T) {
	t.Run("16B-1000000", func(t *testing.T) {
		testTablePutOrGet(t, 16, 1000000, false)
	})
	t.Run("20B-100000", func(t *testing.T) {
		testTablePutOrGet(t, 16, 1000000, false)
	})
	t.Run("1K-50000", func(t *testing.T) {
		testTablePutOrGet(t, 1024, 50000, false)
	})
	t.Run("4K-50000", func(t *testing.T) {
		testTablePutOrGet(t, 4*1024, 50000, false)
	})
}

func TestTable_Get(t *testing.T) {
	t.Run("16B-1000000", func(t *testing.T) {
		testTablePutOrGet(t, 16, 1000000, true)
	})
	t.Run("20B-1000000", func(t *testing.T) {
		testTablePutOrGet(t, 16, 1000000, true)
	})
	t.Run("1K-50000", func(t *testing.T) {
		testTablePutOrGet(t, 1024, 50000, true)
	})
	t.Run("4K-50000", func(t *testing.T) {
		testTablePutOrGet(t, 4*1024, 50000, true)
	})
}

func testTablePutOrGet(t *testing.T, valueLen uint32, count int, needGet bool) {
	name := "put"
	if needGet {
		name = "get"
	}
	dir, err := os.MkdirTemp("", "diskhash-test-"+name)
	assert.Nil(t, err)

	options := DefaultOptions
	options.DirPath = dir
	options.SlotValueLength = valueLen
	table, err := Open(options)
	assert.Nil(t, err)
	defer destroyTable(table)

	value := []byte(strings.Repeat("D", int(valueLen)))
	for i := 0; i < count; i++ {
		key := GetTestKey(i)
		err = table.Put(key, value, func(slot Slot) (bool, error) {
			return false, nil
		})
		assert.Nil(t, err)
	}

	if needGet {
		for i := 0; i < count; i++ {
			key := GetTestKey(i)
			var res []byte
			matchKey := func(slot Slot) (bool, error) {
				if getKeyHash(key) == slot.Hash {
					res = make([]byte, len(slot.Value))
					copy(res, slot.Value)
					return true, nil
				}
				return false, nil
			}
			err := table.Get(key, matchKey)
			assert.Equal(t, value, res)
			assert.Nil(t, err)
		}
	}
}

func TestTableCRUD(t *testing.T) {
	dir, err := os.MkdirTemp("", "diskhash-test-crud")
	assert.Nil(t, err)

	options := DefaultOptions
	options.DirPath = dir
	options.SlotValueLength = 32
	table, err := Open(options)
	assert.Nil(t, err)
	defer destroyTable(table)

	for i := 0; i < 100; i++ {
		var cur []byte

		getFunc := func(slot Slot) (bool, error) {
			cur = slot.Value
			return false, nil
		}
		updateFunc := func(slot Slot) (bool, error) {
			return false, nil
		}

		key := GetTestKey(i)
		value := []byte(strings.Repeat("D", 32))

		// put
		err = table.Put(key, value, updateFunc)
		assert.Nil(t, err)

		// get
		err = table.Get(key, getFunc)
		assert.Nil(t, err)
		assert.Equal(t, value, cur)

		// put different value
		value = []byte(strings.Repeat("A", 32))
		err = table.Put(key, value, updateFunc)
		assert.Nil(t, err)

		// get after put different value
		err = table.Get(key, getFunc)
		assert.Nil(t, err)
		assert.Equal(t, value, cur)

		// delete
		err = table.Delete(key, updateFunc)
		assert.Nil(t, err)
	}
}
