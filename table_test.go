package diskhash

import "testing"

func TestOpen(t *testing.T) {
	options := Options{DirPath: "/tmp/disk-hash", SlotValueLength: 8}
	table, err := Open(options)
	if err != nil {
		t.Error(err)
	}

	t.Log(table)

	table.Put([]byte("name1"), []byte("roseduan"), func(slot Slot) (bool, error) {
		return true, nil
	})
}

func TestTable_Put(t *testing.T) {
	b := bucket{}
	t.Log(b.slots[3].Value)
}
