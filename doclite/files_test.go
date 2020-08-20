package doclite

import (
	"os"
	"strings"
	"testing"
)

var numOfInsert = 100

func TestFile(t *testing.T) {

	defer os.Remove("filetest")
	defer os.Remove("filetestoverflow")

	for add := -100; add <= 100; add++ {
		testFile(add, t)
	}
}

func testFile(add int, t *testing.T) {
	node := &Node{document: &Document{id: int64(100)}}
	db := &DB{metadata: &Meta{}, isTesting: true}
	c := NewCache(db, db.newBtree(""))
	c.node = node
	c.ids = make(map[int64]*Node)
	node.children = c

	nodes := make([]*Node, 0)
	data := []byte(strings.Repeat("F", dataSize+add))
	os.Remove("filetest")
	os.Remove("filetestoverflow")
	db.file = openFile("filetest", os.O_RDWR|os.O_CREATE)
	db.overflowfile = openFile("filetestoverflow", os.O_RDWR|os.O_CREATE)
	db.overflows=make(map[string][]*overflowNode)

	for i := 1; i <= numOfInsert; i++ {
		n := &Node{document: &Document{id: int64(i), data: data, offset: int64(i * dataSize)}}
		nodes = append(nodes, n)
		err := node.children.write(n)
		if err != nil {
			t.Errorf("Error while writing data %v", err)
		}
	}

	for i := 0; i < numOfInsert; i++ {
		buf, err := node.children.read(nodes[i])
		if err != nil {
			t.Errorf("Error while reading data %v", err)
		}
		if dataSize+add-len(buf) > 1 {
			t.Errorf("Size of data read doesn't match size of data inserted %d %d %d", len(buf), dataSize+add, add)
			return
		}
	}
	for i := 0; i < numOfInsert; i++ {
		node.children.Delete(nodes[i].document.id)
	}
}
