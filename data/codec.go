package data

import "bitcask-go/fio"

// LogRecordCodec 日志记录编解码器
type LogRecordCodec interface {
	// EncodeLogRecord 序列化LogRecord，返回序列化后的长度
	EncodeLogRecord(lr *LogRecord) (int, error)
	// EncodeLogRecordSize 获取编码后的长度
	EncodeLogRecordSize(lr *LogRecord) int
	// DecodeLogRecord 从io流反序列化LogRecord
	DecodeLogRecord(offset int64) (*LogRecord, int, error)
	// Sync 刷盘
	Sync() error
	// Close 关闭流
	Close() error
}

func NewLogRecordCodec(io fio.IOManager) LogRecordCodec {
	return newBinaryCodec(io)
}
