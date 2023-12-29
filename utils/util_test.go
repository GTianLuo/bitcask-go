package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTestKey(t *testing.T) {
	assert.Equal(t, 20, len([]byte(GetTestKey(1))))
	assert.Equal(t, []byte("bitcask-go-000000001"), GetTestKey(1))
	assert.Equal(t, []byte("bitcask-go-000000002"), GetTestKey(2))
	assert.Equal(t, []byte("bitcask-go-000000003"), GetTestKey(3))
	assert.Equal(t, []byte("bitcask-go-000000049"), GetTestKey(49))
}

func TestRandomValue(t *testing.T) {
	assert.Equal(t, 100, len(RandomValue(100)))
	assert.Equal(t, 1000, len(RandomValue(1000)))
	assert.Equal(t, 0, len(RandomValue(0)))
	assert.Equal(t, 24, len(RandomValue(24)))
}
