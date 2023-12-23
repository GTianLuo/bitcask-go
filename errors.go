package bitcask_go

import "errors"

var (
	ErrKeyIsNilOrEmpty   = errors.New("the key is nil or empty")
	ErrDBAppendFailed    = errors.New("db key-value append error")
	ErrIndexUpdateFailed = errors.New("db memory index update failed")
	ErrReadKeyNotFound   = errors.New("read key is not found")
	ErrDataFileNotFound  = errors.New("data file is not found")

	ErrDBDirEmpty      = errors.New("config error: empty db directory path")
	ErrDBFileMaxSize   = errors.New("config error: illegal file max size")
	ErrDataFileDamaged = errors.New("the data file is damaged")
)
