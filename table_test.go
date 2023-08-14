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
	defer destroyTable(table)

	err = table.Close()
	assert.Nil(t, err)
}

func TestTable_Put(t *testing.T) {
	t.Run("16B-1000000", func(t *testing.T) {
		testTableBaisc(t, 16, 1000000, false, false, false)
	})
	t.Run("20B-100000", func(t *testing.T) {
		testTableBaisc(t, 16, 1000000, false, false, false)
	})
	t.Run("1K-50000", func(t *testing.T) {
		testTableBaisc(t, 1024, 50000, false, false, false)
	})
	t.Run("4K-50000", func(t *testing.T) {
		testTableBaisc(t, 4*1024, 50000, false, false, false)
	})
}

func TestTable_Get(t *testing.T) {
	t.Run("16B-1000000", func(t *testing.T) {
		testTableBaisc(t, 16, 1000000, true, false, false)
	})
	t.Run("20B-1000000", func(t *testing.T) {
		testTableBaisc(t, 16, 1000000, true, false, false)
	})
	t.Run("1K-50000", func(t *testing.T) {
		testTableBaisc(t, 1024, 50000, true, false, false)
	})
	t.Run("4K-50000", func(t *testing.T) {
		testTableBaisc(t, 4*1024, 50000, true, false, false)
	})
}

func TestTable_Delete(t *testing.T) {
	t.Run("16B-1000000", func(t *testing.T) {
		testTableBaisc(t, 16, 1000000, false, true, false)
	})
	t.Run("20B-1000000", func(t *testing.T) {
		testTableBaisc(t, 16, 1000000, false, true, false)
	})
	t.Run("1K-50000", func(t *testing.T) {
		testTableBaisc(t, 1024, 50000, false, true, false)
	})
	t.Run("4K-50000", func(t *testing.T) {
		testTableBaisc(t, 4*1024, 50000, false, true, false)
	})
}

func TestTable_Update(t *testing.T) {
	t.Run("16B-1000000", func(t *testing.T) {
		testTableBaisc(t, 16, 1000000, false, false, true)
	})
	t.Run("20B-1000000", func(t *testing.T) {
		testTableBaisc(t, 16, 1000000, false, false, true)
	})
	t.Run("1K-50000", func(t *testing.T) {
		testTableBaisc(t, 1024, 50000, false, false, true)
	})
	t.Run("4K-50000", func(t *testing.T) {
		testTableBaisc(t, 4*1024, 50000, false, false, true)
	})
}

func testTableBaisc(t *testing.T, valueLen uint32, count int, needGet, needDelete, needUpdate bool) {
	dir, err := os.MkdirTemp("", "diskhash-test")
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

	getValue := func(target []byte) {
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
			assert.Equal(t, target, res)
			assert.Nil(t, err)
		}
	}

	if needGet {
		getValue(value)
	}

	if needDelete {
		for i := 0; i < count; i++ {
			key := GetTestKey(i)
			matchKey := func(slot Slot) (bool, error) {
				if getKeyHash(key) == slot.Hash {
					return true, nil
				}
				return false, nil
			}
			err := table.Delete(key, matchKey)
			assert.Nil(t, err)
		}
		getValue(nil)
	}

	if needUpdate {
		newValue := []byte(strings.Repeat("H", int(valueLen)))
		for i := 0; i < count; i++ {
			key := GetTestKey(i)
			matchKey := func(slot Slot) (bool, error) {
				if getKeyHash(key) == slot.Hash {
					return true, nil
				}
				return false, nil
			}
			err := table.Put(key, newValue, matchKey)
			assert.Nil(t, err)
		}
		getValue(newValue)
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
