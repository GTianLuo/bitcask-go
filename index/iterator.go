package index

import "bitcask-go/data"

// Iterator 迭代器抽象接口
type Iterator interface {
	// Rewind 回到迭代器起始位置
	Rewind()
	// Seek 根据传入的key，从第一个大于(小于)等于该key的位置遍历
	Seek(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() *data.LogRecordPos
	Close()
}
