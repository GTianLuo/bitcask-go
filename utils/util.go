package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	strRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	letter  = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
)

func IsValidKey(key []byte) bool {
	if key == nil || len(key) == 0 {
		return false
	}
	return true
}

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-go-%09d", i))
}

func RandomValue(n int) []byte {
	value := make([]byte, n)
	for i, _ := range value {
		value[i] = letter[rand.Intn(len(letter))]
	}
	return value
}
