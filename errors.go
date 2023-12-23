package bitcask_go

import "errors"

var (
	ErrKeyIsNilOrEmpty = errors.New("the key is nil or empty")

	ErrDBAppendFailed = errors.New("db key-value append error")

	ErrIndexUpdateFailed = errors.New("db memory index update failed")

	ErrReadKeyNotFound = errors.New("read key not found")

	ErrDataFileNotFound = errors.New("data file not found")
)
