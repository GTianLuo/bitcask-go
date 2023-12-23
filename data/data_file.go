package data

import "bitcask-go/fio"

// DataFile 数据日志文件实例
type DataFile struct {
	FileId    uint32        // 文件编号
	WriteOff  uint64        // 已经写入的数据长度
	IOManager fio.IOManager // 该文件的操作接口
}

func OpenDataFile(dirPath string, fid uint32) (*DataFile, error) {
	return nil, nil
}

// WriteDataFile 往文件中写入数据
func (file *DataFile) Write(data []byte) error {
	return nil
}

// Sync 持久化数据文件
func (file *DataFile) Sync() error {
	return nil
}

func (file *DataFile) Read(offset uint64) (*LogRecord, error) {
	return nil, nil
}