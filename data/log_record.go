package data

// LogRecord 数据在文件中的位置
type LogRecord struct {
	// Fid 文件id
	Fid uint32
	// Offset 在文件中的偏移量
	Offset uint32
}
