package file_io

import (
	"bitcask-go/fio"
	"os"
)

type FileIO struct {
	fd *os.File
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fio.FileDataPerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (f *FileIO) Read(bytes []byte, off int64) (int, error) {
	return f.fd.ReadAt(bytes, off)
}

func (f *FileIO) Write(bytes []byte) (int, error) {
	return f.fd.Write(bytes)
}

func (f *FileIO) Sync() error {
	return f.fd.Sync()
}

func (f *FileIO) Close() error {
	return f.fd.Close()
}
