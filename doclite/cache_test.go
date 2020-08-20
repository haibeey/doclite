package doclite

import (
	"testing"
)

func TestCache(t *testing.T) {
	var (
		numOfInsert = 100
	)
	MaxCacheSize = 100
	node := &Node{document: &Document{id: int64(100)}}
	db := &DB{metadata: &Meta{}, isTesting: true}
	c := NewCache(db, db.newBtree(""))
	c.node = node
	c.ids = make(map[int64]*Node)
	node.children = c

	for i := 0; i < numOfInsert; i++ {
		n := &Node{document: &Document{id: int64(i)}}
		node.children.Add(n)
	}

	if c.currentCapacity != numOfInsert {
		t.Errorf("wrong cache size")
	}

	for i := 0; i < numOfInsert; i += 10 {
		n, err := node.children.get(int64(i))

		if err != nil {
			t.Errorf("%v error while fetching data from cache", err)
		}
		if n.document.id != int64(i) {
			t.Errorf("wrong document id")
		}
	}

	for i := 0; i < numOfInsert; i += 10 {

		currentCapacity := node.children.currentCapacity
		n, err := node.children.get(int64(i))
		if err != nil {
			t.Errorf("%v error while fetching data from cache", err)
		}
		node.children.remove(n)

		if currentCapacity-node.children.currentCapacity != 1 {
			t.Errorf("node not remove")
		}
	}

}
