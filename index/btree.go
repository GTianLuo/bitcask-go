package index

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
	return btreeItem.(*Item).Pos
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

// IsExist 判断key是否已经存在
func (b *Btree) IsExist(key []byte) bool {
	b.lock.RLock()
	defer b.lock.RUnlock()
	it := &Item{Key: key}
	return b.tree.Has(it)
}

func (b *Btree) Iterator(reverse bool) Iterator {
	if b == nil {
		return nil
	}
	b.lock.RLock()
	defer b.lock.RUnlock()
	return newBTreeIterator(b.tree, reverse)
}

type BTreeIterator struct {
	reverse bool    // 是否反向遍历
	index   int     // 当前遍历到的位置
	items   []*Item // 有序结果集
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *BTreeIterator {

	iterator := &BTreeIterator{
		reverse: reverse,
		index:   0,
		items:   make([]*Item, 0),
	}
	saveValue := func(item btree.Item) bool {
		iterator.items = append(iterator.items, item.(*Item))
		return true
	}

	if !reverse {
		tree.Ascend(saveValue)
	} else {
		tree.Descend(saveValue)
	}
	return iterator
}

func (it *BTreeIterator) Rewind() {
	it.index = 0
}

func (it *BTreeIterator) Seek(key []byte) {
	if !it.reverse {
		for i, item := range it.items {
			if !item.Less(&Item{Key: key}) {
				it.index = i
			}
		}
	} else {
		for i, item := range it.items {
			if item.Less(&Item{Key: key}) {
				it.index = i
			}
		}
	}
}

func (it *BTreeIterator) Next() {
	it.index++
}

func (it *BTreeIterator) Valid() bool {
	return it.index < len(it.items)
}

func (it *BTreeIterator) Key() []byte {
	return it.items[it.index].Key
}

func (it *BTreeIterator) Value() *data.LogRecordPos {
	return it.items[it.index].Pos
}

func (it *BTreeIterator) Close() {
	it.items = nil
}
