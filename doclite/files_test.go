package doclite

import (
	"os"
	"strings"
	"testing"
	"encoding/json"
)

var numOfInsert = 10

func TestFile(t *testing.T) {

	defer os.Remove("filetest")
	defer os.Remove("filetest.overflow")

	for i:=0;i<3;i++{
		for add := -10; add <= 10; add++ {
			testFile(add, t)
		}
	}

}

func testFile(add int, t *testing.T) {
	node := &Node{document: &Document{id: int64(100)}}
	db := OpenDB("filetest")
	c := NewCache(db, db.rootTree)
	c.node = node
	c.ids = make(map[int64]*Node)
	node.children = c

	type simpleStruct struct{
		Name string
	}

	nodes := make([]*Node, 0)

	for i := 1; i <= numOfInsert; i++ {
		ss:=simpleStruct{Name:strings.Repeat("F", dataSize+add)}
		buf, err := json.Marshal(ss)
		if err != nil {
			continue
		}
		
		n ,err:= db.rootTree.Find(db.rootTree.Insert(buf))
		if err != nil {
			t.Errorf("Error while writing data %v", err)
		}
		nodes = append(nodes, n)
		
	}

	ss:=&simpleStruct{}
	for i := 0; i < numOfInsert; i++ {
		buf:= nodes[i].document.data

		if dataSize+add-len(buf) > 1 {
			t.Errorf("Size of data read doesn't match size of data inserted %d %d %d", len(buf), dataSize+add, add)
			return
		}
		err:=json.Unmarshal(buf,ss)
		if err!=nil{
			t.Errorf("%s",err)
		}
	}
	for i := 0; i < numOfInsert; i++ {
		node.children.Delete(nodes[i].document.id)
	}

	db.Close()
}
