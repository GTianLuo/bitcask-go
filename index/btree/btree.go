package btree

import (
	"bitcask-go/data"
	"github.com/google/btree"
	"sync"
)

type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *Btree {
	return &Btree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (b *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{
		Key: key,
		Pos: pos,
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.tree.ReplaceOrInsert(it)
	return true
}

func (b *Btree) Get(key []byte) *data.LogRecordPos {
	b.lock.RLock()
	defer b.lock.RUnlock()
	it := &Item{Key: key}
	btreeItem := b.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(Item).Pos
}

func (b *Btree) Delete(key []byte) bool {

	b.lock.Lock()
	defer b.lock.Unlock()
	it := &Item{Key: key}
	btreeItem := b.tree.Delete(it)
	if btreeItem == nil {
		return false
	}
	return true
}
