package bitcask_go

import (
	"bitcask-go/utils"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activityDataFile != nil {
			_ = db.Close()
		}
		err := os.RemoveAll(db.options.DBFileDir)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DBFileDir = dir
	db, err := Start(opts)
	//	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	fmt.Println(dir)
	opts.DBFileDir = dir
	opts.FileMaxSize = 1024 * 1024 * 1 // 1MB
	db, err := Start(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrKeyIsNilOrEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.写到数据文件进行了转换
	for i := 0; i < 7000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(300))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.oldDataFiles))

	// 6.重启后再 Put 数据
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Start(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-get")
	opts.DBFileDir = dir
	opts.FileMaxSize = 1 * 1024 * 1024
	db, err := Start(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常读取一条数据
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrReadKeyNotFound, err)

	// 3.值被重复 Put 后在读取
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4.值被删除后再 Get
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrReadKeyNotFound, err)

	// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	for i := 100; i < 7100; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(300))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.oldDataFiles))
	val5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// 7. 重启前读旧数据文件
	val6, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrReadKeyNotFound, err)

	// 7.重启后，前面写入的数据都能拿到
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Start(opts)

	val9, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val9)
	assert.Equal(t, val1, val9)

	val10, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val10)
	assert.Equal(t, val3, val10)

	val11, err := db2.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val11))
	assert.Equal(t, ErrReadKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DBFileDir = dir
	db, err := Start(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrReadKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsNilOrEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.重启之后，再进行校验
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Start(opts)
	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrReadKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DBFileDir = dir
	db, err := Start(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// 数据库为空
	keys1 := db.ListKeys()
	assert.Equal(t, 0, len(keys1))

	// 只有一条数据
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	// 有多条数据
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.RandomValue(20))
	assert.Nil(t, err)

	keys3 := db.ListKeys()
	assert.Equal(t, 4, len(keys3))
	for _, k := range keys3 {
		assert.NotNil(t, k)
	}
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DBFileDir = dir
	db, err := Start(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	})
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DBFileDir = dir
	db, err := Start(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

}

func TestDB_Sync(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DBFileDir = dir
	db, err := Start(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}
