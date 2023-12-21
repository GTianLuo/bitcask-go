package btree

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	btree := NewBTree()

	res := btree.Put(nil, &data.LogRecord{1, 11})
	assert.True(t, res)

	res = btree.Put([]byte("iii"), &data.LogRecord{2, 22})
	assert.True(t, res)
}

func TestBtree_Get(t *testing.T) {
	btree := NewBTree()

	res := btree.Put(nil, &data.LogRecord{1, 11})
	assert.True(t, res)

	res = btree.Put([]byte("iii"), &data.LogRecord{2, 22})
	assert.True(t, res)

	value := btree.Get(nil)
	assert.Equal(t, &data.LogRecord{1, 11}, value)

	btree.Put(nil, &data.LogRecord{2, 22})
	value = btree.Get(nil)
	assert.Equal(t, &data.LogRecord{2, 22}, value)

	value = btree.Get([]byte("iii"))
	assert.Equal(t, &data.LogRecord{2, 22}, value)

	btree.Put([]byte("iii"), &data.LogRecord{3, 33})
	value = btree.Get([]byte("iii"))
	assert.Equal(t, &data.LogRecord{3, 33}, value)

}

func TestBtree_Delete(t *testing.T) {
	btree := NewBTree()

	res := btree.Put(nil, &data.LogRecord{1, 11})
	assert.True(t, res)

	res = btree.Put([]byte("iii"), &data.LogRecord{2, 22})
	assert.True(t, res)

	res = btree.Delete(nil)
	assert.True(t, res)

	res = btree.Delete([]byte("iii"))
	assert.True(t, res)
}
