package file_io

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestNewFileIOManager(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("./", "test.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("./", "test.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	count, err := fio.Write([]byte("hello world"))
	assert.Nil(t, err)
	fmt.Println(count)

	count, err = fio.Write([]byte(""))
	assert.Nil(t, err)
	fmt.Println(count)

	count, err = fio.Write([]byte("你好世界"))
	assert.Nil(t, err)
	fmt.Println(count)
}

func TestFileIO_Read(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("./", "test.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	b1 := make([]byte, 11)
	_, err = fio.Read(b1, 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte("hello world"), b1)
}

func TestFileIO_Sync(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("./", "test.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}
