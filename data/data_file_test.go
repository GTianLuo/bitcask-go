package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile("./", 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
}

func TestDataFile_EncodeLogRecordSize(t *testing.T) {
	dataFile, err := OpenDataFile("./", 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	logRecord := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordNormal,
	}

	size := dataFile.EncodeLogRecordSize(logRecord)
	t.Log(size)
}

func TestDataFile_WriteLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile("./", 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	logRecord := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordNormal,
	}

	size, err := dataFile.WriteLogRecord(logRecord)

	assert.Nil(t, err)
	assert.Equal(t, size, 17)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile("./", 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	logRecord := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("world"),
		Type:  LogRecordNormal,
	}

	readLogRecord, size, err := dataFile.ReadLogRecord(17)
	assert.Nil(t, err)
	assert.Equal(t, size, 17)
	assert.Equal(t, logRecord.Type, readLogRecord.Type)
	assert.Equal(t, logRecord.Key, readLogRecord.Key)
	assert.Equal(t, logRecord.Value, readLogRecord.Value)
}
