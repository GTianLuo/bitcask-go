package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"bitcask-go/utils"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask引擎的实例
type DB struct {
	mu               *sync.RWMutex
	activityDataFile *data.DataFile            // 当前活跃的数据文件，可读写
	oldDataFiles     map[uint32]*data.DataFile // 旧的数据文件，只读
	options          *Options                  // 用户配置选项
	index            index.Indexer             // 内存索引
	fids             []int                     // 保存db数据文件序号的数组，有序
}

func Start(options *Options) (*DB, error) {
	// 校验配置项
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 校验数据目录是否存在，不存在则创建
	if _, err := os.Stat(options.DBFileDir); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DBFileDir, os.ModeDir); err != nil {
			return nil, err
		}
	}

	// 创建db
	db := &DB{
		oldDataFiles: make(map[uint32]*data.DataFile),
		options:      options,
		mu:           new(sync.RWMutex),
		index:        index.NewIndexer(options.DBIndex),
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 加载内存索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Put 写入数据
func (db *DB) Put(key []byte, value []byte) error {

	// 判断key是否有效
	if !utils.IsValidKey(key) {
		return ErrKeyIsNilOrEmpty
	}

	// 这里不需要判断key是否存在,如果put已存在的key,相当于更新数据

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

	// 获取LogRecord编码后的长度
	size := db.activityDataFile.EncodeLogRecordSize(logRecord)

	// 判断当前活跃文件是否达到阈值,达到阈值需要打开新的活跃文件
	if db.activityDataFile.WriteOff+uint64(size) >= db.options.FileMaxSize {

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
	if _, err := db.activityDataFile.WriteLogRecord(logRecord); err != nil {
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
		db.activityDataFile.WriteOff - uint64(size),
	}, nil

}

// 初始化活跃文件或打新的活跃文件
// 该方法必须在加锁的条件下调用
func (db *DB) setActivityDataFile() error {

	var initailFid uint32 = 1
	if db.activityDataFile != nil {
		initailFid = db.activityDataFile.FileId + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DBFileDir, initailFid)
	if err != nil {
		return err
	}
	db.activityDataFile = dataFile
	return nil
}

// Get 读数据
func (db *DB) Get(key []byte) ([]byte, error) {

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
	logRecord, err := db.getLogRecordByPosition(logRecordPos)
	if err != nil {
		return nil, err
	}
	return logRecord.Value, nil
}

// 根据文件索引读数据
// 该方法类会对db加锁
func (db *DB) getLogRecordByPosition(pos *data.LogRecordPos) (*data.LogRecord, error) {

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

	logRecord, _, err := belongFile.ReadLogRecord(int64(pos.Offset))
	if err != nil {
		return nil, err
	}

	// 判断该数据是否已经被删除
	if logRecord.Type == data.LogRecordDelete {
		return nil, ErrReadKeyNotFound
	}

	return logRecord, nil
}

func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	logRecord, err := db.getLogRecordByPosition(pos)
	if err != nil {
		return nil, err
	}
	return logRecord.Value, nil
}

//  校验配置项
func checkOptions(options *Options) error {

	if options.DBFileDir == "" {
		return ErrDBDirEmpty
	}

	if options.FileMaxSize <= 0 {
		return ErrDBFileMaxSize
	}
	return nil
}

// 加载数据文件
func (db *DB) loadDataFiles() error {
	fileInfos, err := ioutil.ReadDir(db.options.DBFileDir)
	if err != nil {
		return err
	}

	// 遍历所有文件，获取DB数据文件的编号
	fids := make([]int, 0)
	for _, fileInfo := range fileInfos {
		fileName := fileInfo.Name()
		if strings.HasSuffix(fileName, data.DataFileSubffix) {
			fid, err := strconv.Atoi(strings.Split(fileName, ".")[0])
			// 文件损坏
			if err != nil {
				return ErrDataFileDamaged
			}
			fids = append(fids, fid)
		}
	}

	//对文件序号排序
	sort.Ints(fids)

	db.fids = fids
	// 打开所有DB数据文件
	for i, fid := range fids {
		dataFile, err := data.OpenDataFile(db.options.DBFileDir, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fids)-1 { // 序号最大的文件，为活跃文件
			db.activityDataFile = dataFile
		} else {
			db.oldDataFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载内存索引
func (db *DB) loadIndexFromDataFiles() error {

	for i, fid := range db.fids {
		var dataFile *data.DataFile
		if i != len(db.fids)-1 { // 当前是旧数据文件
			dataFile = db.oldDataFiles[uint32(fid)]
		} else {
			dataFile = db.activityDataFile
		}

		var offset uint64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(int64(offset))
			if err != nil {
				// 读到文件末尾了
				if err == io.EOF {
					break
				}
				return err
			}

			logRecordPos := &data.LogRecordPos{
				Fid:    uint32(fid),
				Offset: offset,
			}

			// 更新内存索引
			if logRecord.Type == data.LogRecordNormal {
				db.index.Put(logRecord.Key, logRecordPos)
			} else if logRecord.Type == data.LogRecordDelete {
				db.index.Delete(logRecord.Key)
			}

			// 更新偏移量
			offset += uint64(size)
		}
		// 如果当前是活跃文件，更新写入偏移量
		if fid == len(db.fids) {
			dataFile.WriteOff = offset
		}
	}
	return nil
}

// Delete 删除key-value
func (db *DB) Delete(key []byte) error {

	// 判断key是否有效
	if !utils.IsValidKey(key) {
		return ErrKeyIsNilOrEmpty
	}
	// 在内存索引中查询数据位置
	lrPos := db.index.Get(key)
	if lrPos == nil {
		return nil
	}
	// 构建删除后的数据
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDelete,
	}
	// 在数据文件中追加该删除记录
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 删除内存索引
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Close() error {

	db.mu.Lock()
	defer db.mu.Unlock()
	// 刷新活跃文件
	if db.activityDataFile != nil {
		if err := db.activityDataFile.Sync(); err != nil {
			return err
		}
	}
	// 关闭所有文件
	if err := db.activityDataFile.Close(); err != nil {
		return err
	}
	for _, file := range db.oldDataFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Fold 遍历所有数据，并执行用户指定的操作fn，fn返回错误时终止
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	it := db.index.Iterator(false)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		// 该方法加锁了，这里不用加
		value, err := db.getValueByPosition(it.Value())
		if err != nil {
			return err
		}
		if !fn(it.Key(), value) {
			break
		}
	}
	return nil
}

// ListKeys 获取所有的key
func (db *DB) ListKeys() (rnt [][]byte) {
	it := db.index.Iterator(false)
	for it.Rewind(); it.Valid(); it.Next() {
		rnt = append(rnt, it.Key())
	}
	return
}

// Sync 将内存数据刷入磁盘
func (db *DB) Sync() error {
	db.mu.Lock()
	db.mu.Unlock()
	if db.activityDataFile != nil {
		return db.activityDataFile.Sync()
	}
	return nil
}
