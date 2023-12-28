package index

import (
	"bitcask-go/data"
)

type DBIndexType byte

const (
	BTree DBIndexType = iota
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
	IsExist(key []byte) bool
}

// NewIndexer 工厂方法，根据类型，创建对应的内存索引
func NewIndexer(indexType DBIndexType) Indexer {
	switch indexType {
	case BTree:
		return NewBTree()
	default:
		return NewBTree()
	}
}
