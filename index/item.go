package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

type Item struct {
	Key []byte
	Pos *data.LogRecordPos
}

func (i Item) Less(than btree.Item) bool {
	return bytes.Compare(i.Key, than.(*Item).Key) == -1
}
