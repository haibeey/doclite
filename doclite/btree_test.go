package doclite

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var (
	randGen = rand.New(rand.NewSource(time.Now().UnixNano()))
)

func testBtree(bt *Btree, t *testing.T) *Btree {
	var (
		numOfInsert = 100
	)
	for i := 0; i < numOfInsert; i++ {
		bt.Insert([]byte(fmt.Sprintf("%d docklite", i)))
	}

	if bt.NumDocuments != int64(numOfInsert) {
		t.Errorf("%d is not  equal to number of docs inserted", bt.NumDocuments)
	}
	add := 0
	if numOfInsert%MinKeys != 0 {
		add++
	}
	if bt.NumRoots != numOfInsert/MinKeys+add {
		t.Errorf("%d is not  equal to number of root docs", bt.NumRoots)
	}

	x := 0
	pageNo := bt.Pages[x]
	for i := 1; i <= numOfInsert; i++ {
		if i%MinKeys == 0 {
			x++
			pageNo = bt.Pages[x]
		}
		n, err := bt.Find(int64(i))
		if err != nil {
			t.Errorf("%v getting data failed", err)
		}
		if i%MinKeys != 0 && n.document.offset != pageNo*pageSize+int64(((i-1)%MinKeys)*dataSize) {
			t.Errorf("unmatching offset")
		}
	}

	bt.InsertSubCollection("newcollection")

	return bt.Get("newcollection")
}
func TestBtree(t *testing.T) {
	db := &DB{overflows: []overflowNode{}, metadata: &Meta{}, isTesting: true}
	bt := db.newBtree()
	bt1 := testBtree(bt, t)

	if bt.Get("newcollection") == nil {
		t.Errorf("collection newcollection not found")
	}
	testBtree(bt1, t)

	data, err := json.Marshal(bt)
	if err != nil {
		t.Errorf("%v failed marshaling btree", err)
	}
	fmt.Println(string(data))
}

func TestBinarySearch(t *testing.T) {
	nodes := []*Node{}
	ids := []int64{}
	for i := 0; i < 100; i++ {
		id := int64(i)
		ids = append(ids, id)
		doc := &Document{id: id, data: []byte{}}
		n := &Node{document: doc}
		nodes = append(nodes, n)
	}
	for i := 0; i < 100; i++ {
		if ids[i] != nodes[binarySearch(ids[i], nodes, 100)].document.id {
			t.Errorf(" nodes not sorted %d %d", i, binarySearch(ids[i], nodes, 100))
		}
	}

	list := []*Node{&Node{document: &Document{id: 1, data: []byte{}}}, &Node{document: &Document{id: 33, data: []byte{}}}, &Node{document: &Document{id: 65, data: []byte{}}}, &Node{document: &Document{id: 97, data: []byte{}}}}

	if binarySearch(int64(3), list, 4) != 0 {
		t.Errorf(" wrong node")
	}

	if binarySearch(int64(40), list, 4) != 1 {
		t.Errorf(" wrong node")
	}

	if binarySearch(int64(78), list, 4) != 2 {
		t.Errorf(" wrong node")
	}

	if binarySearch(int64(98), list, 4) != 3 {
		t.Errorf(" wrong node")
	}
}
