package doclite

import (
	"os"
	"strings"
	"testing"
)

func TestFile(t *testing.T) {
	var numOfInsert = 100
	node := &Node{document: &Document{id: int64(100)}}
	dbfile := openFile("filetest", os.O_RDWR|os.O_CREATE)
	overflowfile := openFile("filetestoverflow", os.O_RDWR|os.O_CREATE)
	defer os.Remove("filetest")
	defer os.Remove("filetestoverflow")
	db := &DB{overflows: []overflowNode{}, metadata: &Meta{}, file: dbfile, overflowfile: overflowfile}
	db.isTesting = true
	c := NewCache(db)
	c.node = node
	c.ids = make(map[int64]*Node)
	node.children = c

	for add := -100; add <= 100; add++ {
		nodes := make([]*Node, 0)
		data := []byte(strings.Repeat("F", dataSize+add))
		db.overflows = []overflowNode{}
		for i := 0; i < numOfInsert; i++ {
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

			if len(buf) != dataSize+add {
				t.Errorf("Size of data read doesn't match size of data inserted %d %d", len(buf), dataSize+add)
			}
		}
	}

}
