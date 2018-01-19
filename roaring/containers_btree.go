package roaring

import (
	"io"
)

func cmp(a, b uint64) int {
	return int(a - b)
}

func NewBTreeContainers() *BTreeContainers {
	return &BTreeContainers{
		tree: TreeNew(cmp),
	}
}

type BTreeContainers struct {
	tree *Tree

	lastKey       uint64
	lastContainer *container
}

func (btc *BTreeContainers) Get(key uint64) *container {
	// Check the last* cache for same container.
	if key == btc.lastKey && btc.lastContainer != nil {
		return btc.lastContainer
	}

	var c *container
	el, ok := btc.tree.Get(key)
	if ok {
		c = el
		btc.lastKey = key
		btc.lastContainer = c
	}
	return c
}

func (btc *BTreeContainers) Put(key uint64, c *container) {
	// If a mapped container is added to the tree, reset the
	// lastContainer cache so that the cache is not pointing
	// at a read-only mmap.
	if c.mapped {
		btc.lastContainer = nil
	}
	btc.tree.Set(key, c)
}

type updater struct {
	key           uint64
	containerType byte
	n             int
	mapped        bool
}

func (u updater) update(oldV *container, exists bool) (*container, bool) {
	// update the existing container
	if exists {
		oldV.containerType = u.containerType
		oldV.n = u.n
		oldV.mapped = u.mapped
		return oldV, true
	}
	return &container{
		containerType: u.containerType,
		n:             u.n,
		mapped:        u.mapped,
	}, true
}

func (btc *BTreeContainers) PutContainerValues(key uint64, containerType byte, n int, mapped bool) {
	a := updater{key, containerType, n, mapped}
	btc.tree.Put(key, a.update)
}

func (btc *BTreeContainers) Remove(key uint64) {
	btc.tree.Delete(key)
}

func (btc *BTreeContainers) GetOrCreate(key uint64) *container {
	// Check the last* cache for same container.
	if key == btc.lastKey && btc.lastContainer != nil {
		return btc.lastContainer
	}

	btc.lastKey = key
	v, ok := btc.tree.Get(key)
	if !ok {
		cont := newContainer()
		btc.tree.Set(key, cont)
		btc.lastContainer = cont
		return cont
	}

	btc.lastContainer = v
	return btc.lastContainer
}

func (btc *BTreeContainers) Clone() Containers {
	nbtc := NewBTreeContainers()

	itr, err := btc.tree.SeekFirst()
	if err == io.EOF {
		return nbtc
	}
	for {
		k, v, err := itr.Next()
		if err == io.EOF {
			break
		}
		nbtc.tree.Set(k, v.clone())
	}
	return nbtc
}

func (btc *BTreeContainers) Last() (key uint64, c *container) {
	if btc.tree.Len() == 0 {
		return 0, nil
	}
	k, v := btc.tree.Last()
	return k, v
}

func (btc *BTreeContainers) Size() int {
	return btc.tree.Len()
}

func (btc *BTreeContainers) Iterator(key uint64) (citer Contiterator, found bool) {
	e, ok := btc.tree.Seek(key)
	if ok {
		found = true
	}

	return &BTCIterator{
		e: e,
	}, found
}

type BTCIterator struct {
	e   *Enumerator
	key uint64
	val *container
}

func (i *BTCIterator) Next() bool {

	k, v, err := i.e.Next()
	if err == io.EOF {
		return false
	}
	i.key = k
	i.val = v
	return true
}

func (i *BTCIterator) Value() (uint64, *container) {
	if i.val == nil {
		return 0, nil
	}
	return i.key, i.val
}
