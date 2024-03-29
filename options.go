package bitcask_go

import "bitcask-go/index"

type DBSyncType byte

const (
	Always DBSyncType = iota
)

type Options struct {
	DBFileDir   string            // DB文件保存地址
	FileMaxSize uint64            // 当个DB文件最大长度
	DBSync      DBSyncType        // 刷盘策略
	DBIndex     index.DBIndexType // 索引类型
}

var DefaultOptions = &Options{
	DBFileDir:   "/tmp/bitcask-db",
	FileMaxSize: 256 * 1024 * 1024, //256MB
	DBSync:      Always,
	DBIndex:     index.BTree,
}
