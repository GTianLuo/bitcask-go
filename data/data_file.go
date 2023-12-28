package data

import (
	"bitcask-go/fio"
	"fmt"
	"path"
)

const DataFileSubffix = ".data"
const DataFileFormat = "%09d%s"

// DataFile 数据日志文件实例
type DataFile struct {
	FileId   uint32         // 文件编号
	WriteOff uint64         // 已经写入的数据长度
	codec    LogRecordCodec // 编解码器，内部隐藏了文件操作细节
}

// OpenDataFile 打开数据文件，封装dataFile对象
func OpenDataFile(dirPath string, fid uint32) (*DataFile, error) {

	fileName := path.Join(dirPath, fmt.Sprintf(DataFileFormat, fid, DataFileSubffix))

	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	dataFile := &DataFile{
		FileId: fid,
		codec:  NewLogRecordCodec(ioManager),
	}
	return dataFile, nil
}

func (file *DataFile) EncodeLogRecordSize(logRecord *LogRecord) int {
	return file.codec.EncodeLogRecordSize(logRecord)
}

// WriteLogRecord  往文件中写入数据
func (file *DataFile) WriteLogRecord(logRecord *LogRecord) (int, error) {
	size, err := file.codec.EncodeLogRecord(logRecord)
	if err != nil {
		return 0, err
	}
	// 更新文件偏移量
	file.WriteOff += uint64(size)
	return size, err
}

// Sync 持久化数据文件
func (file *DataFile) Sync() error {
	return file.codec.Sync()
}

func (file *DataFile) ReadLogRecord(offset int64) (*LogRecord, int, error) {
	return file.codec.DecodeLogRecord(offset)
}
