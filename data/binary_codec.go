package data

import (
	"bitcask-go/fio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

type BinaryCodec struct {
	ioManager fio.IOManager
}

// LogRecordHeaderMaxSize 日志记录头部最大长度
const LogRecordHeaderMaxSize = 15

// 日志记录头部，不对外暴露
type logRecordHeader struct {
	crc           uint32
	logRecordType LogRecordType
	keySize       uint32
	valueSize     uint32
}

func newBinaryCodec(io fio.IOManager) *BinaryCodec {
	return &BinaryCodec{
		ioManager: io,
	}
}

// EncodeLogRecord 二进制编码
// +----------+---------+----------+------------+----------+----------+
// | crc校验值 | type类型 | key size | value size |    key   |   value  |
// +----------+---------+----------+------------+----------+----------+
//     4字节      1字节  变长(最大5字节) 变长(最大5字节)   变长       变长
func (b *BinaryCodec) EncodeLogRecord(lr *LogRecord) (int, error) {
	header := make([]byte, LogRecordHeaderMaxSize)
	// 第五个字节存储type
	header[4] = byte(lr.Type)

	// 后面字节存储key size 和 value size
	index := 5
	index += binary.PutVarint(header[index:], int64(len(lr.Key)))
	index += binary.PutVarint(header[index:], int64(len(lr.Value)))

	size := index + len(lr.Key) + len(lr.Value)
	encBytes := make([]byte, size)
	// 拷贝header
	copy(encBytes[:index], header[:index])
	// 拷贝key和value
	copy(encBytes[index:], lr.Key)
	copy(encBytes[index+len(lr.Key):], lr.Value)

	// 对所有数据计算一个CRC冗余码
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	// 写入IO流
	_, err := b.ioManager.Write(encBytes)
	if err != nil {
		return 0, err
	}

	return size, nil
}

// EncodeLogRecordSize 获取编码后的长度
func (b *BinaryCodec) EncodeLogRecordSize(lr *LogRecord) int {

	// 编码长度只有key size ，value size 是变长的
	size := 5 // type + crc
	// 计算key size ，value size 实际长度
	bytes := make([]byte, 10)
	size += binary.PutVarint(bytes, int64(len(lr.Key)))
	size += binary.PutVarint(bytes, int64(len(lr.Value)))

	size += len(lr.Key)
	size += len(lr.Value)
	return size
}

// DecodeLogRecord 二进制解码
func (b *BinaryCodec) DecodeLogRecord(offset int64) (*LogRecord, int, error) {

	size, err := b.ioManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 可能文件剩余内容小于LogRecordHeaderMaxSize，防止EOF
	currentLRMaxSize := LogRecordHeaderMaxSize
	if size-offset < LogRecordHeaderMaxSize {
		currentLRMaxSize = int(size - offset)
	}

	if currentLRMaxSize == 0 {
		return nil, 0, io.EOF
	}
	// 读头部信息
	header := make([]byte, currentLRMaxSize)
	if _, err = b.ioManager.Read(header, offset); err != nil {
		return nil, 0, err
	}

	crc := binary.LittleEndian.Uint32(header)
	lrType := header[4]

	// 读key size 和 value size
	index := 5
	keySize, n := binary.Varint(header[index:])
	index += n

	valueSize, n := binary.Varint(header[index:])
	index += n

	// 读取key value
	key := make([]byte, keySize)
	n, err = b.ioManager.Read(key, offset+int64(index))
	if err != nil {
		return nil, 0, err
	}

	value := make([]byte, valueSize)
	n, err = b.ioManager.Read(value, offset+int64(index)+keySize)
	if err != nil {
		return nil, 0, err
	}

	logRecord := &LogRecord{
		Type:  LogRecordType(lrType),
		Key:   key,
		Value: value,
	}

	if !b.checkLogRecordCRC(header[:index], logRecord, crc) {
		return nil, 0, errors.New("the data file is damaged")
	}
	return logRecord, index + int(keySize+valueSize), nil
}

// 校验LogRecord CRC码，防止文件已经被破坏
func (b *BinaryCodec) checkLogRecordCRC(header []byte, lr *LogRecord, crc uint32) bool {
	currentCRC := crc32.ChecksumIEEE(header[4:])
	currentCRC = crc32.Update(currentCRC, crc32.IEEETable, lr.Key)
	currentCRC = crc32.Update(currentCRC, crc32.IEEETable, lr.Value)
	return currentCRC == crc
}

func (b *BinaryCodec) Sync() error {
	return b.ioManager.Sync()
}
