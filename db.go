package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"bitcask-go/utils"
	"sync"
)

// DB bitcask引擎的实例
type DB struct {
	mu               *sync.RWMutex
	activityDataFile *data.DataFile            // 当前活跃的数据文件，可读写
	oldDataFiles     map[uint32]*data.DataFile // 旧的数据文件，只读
	options          *Options                  // 用户配置选项
	index            index.Indexer             // 内存索引
}

// Put 写入数据
func (db *DB) Put(key []byte, value []byte) error {

	// 判断key是否有效
	if !utils.IsValidKey(key) {
		return ErrKeyIsNilOrEmpty
	}

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 写入数据
	recordPos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引下标
	if ok := db.index.Put(key, recordPos); !ok {
		return ErrDBAppendFailed
	}
	return nil
}

// 追加日志记录
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	db.mu.Lock()
	defer db.mu.Unlock()
	// 初始化活跃文件
	if db.activityDataFile == nil {
		if err := db.setActivityDataFile(); err != nil {
			return nil, err
		}
	}

	// 对LogRecord编码
	encodeLogRecord, size := logRecord.LogRecordEncode()

	// 判断当前活跃文件是否达到阈值,达到阈值需要打开新的活跃文件
	if db.activityDataFile.WriteOff+size >= db.options.FileMaxSize {

		// 持久化数据文件
		if err := db.activityDataFile.Sync(); err != nil {
			return nil, err
		}

		// 保存当前活跃文件到旧文件中
		db.oldDataFiles[db.activityDataFile.FileId] = db.activityDataFile

		// 更新活跃文件
		if err := db.setActivityDataFile(); err != nil {
			return nil, err
		}
	}

	// 写入数据
	if err := db.activityDataFile.Write(encodeLogRecord); err != nil {
		return nil, err
	}

	// 每次写入数据立马刷盘
	if db.options.DBSync == Always {
		if err := db.activityDataFile.Sync(); err != nil {
			return nil, err
		}
	}

	return &data.LogRecordPos{
		db.activityDataFile.FileId,
		db.activityDataFile.WriteOff,
	}, nil

}

// 初始化活跃文件或打新的活跃文件
// 该方法必须在加锁的条件下调用
func (db *DB) setActivityDataFile() error {

	var initailFid uint32 = 0
	if db.activityDataFile == nil {
		initailFid = db.activityDataFile.FileId + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DBFileDir, initailFid)
	if err != nil {
		return err
	}
	db.activityDataFile = dataFile
	return nil
}

// Read 读数据
func (db *DB) Read(key []byte) ([]byte, error) {

	// 判断key是否合法
	if !utils.IsValidKey(key) {
		return nil, ErrKeyIsNilOrEmpty
	}
	// 查询内存索引
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrReadKeyNotFound
	}
	// 文件中查询
	logRecord, err := db.read(logRecordPos)
	if err != nil {
		return nil, err
	}
	return logRecord.Value, nil
}

// 根据文件索引读数据
// 该方法类会对db加锁
func (db *DB) read(pos *data.LogRecordPos) (*data.LogRecord, error) {

	db.mu.RLock()
	db.mu.RUnlock()
	// 查询读数据所在文件
	belongFile := db.activityDataFile
	if belongFile == nil || belongFile.FileId != pos.Fid {
		belongFile = db.oldDataFiles[pos.Fid]
	}

	// 查询文件不存在
	if belongFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, err := belongFile.Read(pos.Offset)
	if err != nil {
		return nil, err
	}

	// 判断该数据是否已经被删除
	if logRecord.Type == data.LogRecordDelete {
		return nil, ErrReadKeyNotFound
	}

	return logRecord, nil
}

