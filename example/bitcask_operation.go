package main

import (
	bitcask_go "bitcask-go"
	"fmt"
)

func main() {
	db, err := bitcask_go.Start(bitcask_go.DefaultOptions)
	if err != nil {
		panic(err)
	}

	//if err := db.Put([]byte("key1"), []byte("1111111111111111111111111111")); err != nil {
	//	panic(err)
	//}
	//
	//if err := db.Put([]byte("key2"), []byte("22222222222222222222")); err != nil {
	//	panic(err)
	//}
	//
	//if err := db.Put([]byte("key3"), []byte("行事件发生纠纷和")); err != nil {
	//	panic(err)
	//}

	value, err := db.Get([]byte("key2"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(value))

	value, err = db.Get([]byte("key3"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(value))

	value, err = db.Get([]byte("key1"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(value))
}
