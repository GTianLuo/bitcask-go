package index

import "bitcask-go/data"

type Indexer interface {
	Put(key []byte, pos *data.LogRecord) bool
	Get(key []byte) *data.LogRecord
	Delete(key []byte) bool
}
