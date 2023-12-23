package data

type LogRecordType byte

const (
	LogRecordDelete LogRecordType = iota
	LogRecordNormal
)

// LogRecordPos 数据在文件中的位置
type LogRecordPos struct {
	// Fid 文件id
	Fid uint32
	// Offset 在文件中的偏移量
	Offset uint64
}

// LogRecord 存储在文件中的数据日志记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordEncode 对日志文件进行编码，返回字节数组和长度
func (logRecord *LogRecord) LogRecordEncode() ([]byte, uint64) {
	return nil, 0
}
