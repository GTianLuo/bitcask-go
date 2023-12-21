package fio

const FileDataPerm = 0644

// IOManager 抽象的IO管理接口，目前只支持标准文件IO
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	// Sync 同步数据到磁盘
	Sync() error
	Close() error
}
