package doclite

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

var numOfInsert = 10

func TestFile(t *testing.T) {

	for i := 0; i < 3; i++ {
		for add := 0; add <= 10; add++ {
			testFile(add, t)
		}
	}

}

func testFile(add int, t *testing.T) {
	// Remove stale files from any previous run so each iteration starts clean.
	os.Remove("filetest")
	os.Remove("filetest.overflow")

	defer os.Remove("filetest")
	defer os.Remove("filetest.overflow")

	db, err := OpenDB("filetest")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	type simpleStruct struct {
		Name string
	}

	nodes := make([]*Node, 0)

	for i := 1; i <= numOfInsert; i++ {
		ss := simpleStruct{Name: strings.Repeat("F", dataSize+add)}
		buf, err := json.Marshal(ss)
		if err != nil {
			continue
		}

		id, insertErr := db.rootTree.Insert(buf)
		if insertErr != nil {
			t.Errorf("Error while inserting data %v", insertErr)
			continue
		}
		n, err := db.rootTree.Find(id)
		if err != nil {
			t.Errorf("Error while finding data %v", err)
			continue
		}
		if n == nil {
			t.Errorf("Find returned nil for id %d", id)
			continue
		}
		nodes = append(nodes, n)
	}

	ss := &simpleStruct{}
	for i := 0; i < len(nodes); i++ {
		buf := nodes[i].document.data

		if dataSize+add-len(buf) > 1 {
			t.Errorf("Size of data read doesn't match size of data inserted %d %d %d", len(buf), dataSize+add, add)
			return
		}
		err := json.Unmarshal(buf, ss)
		if err != nil {
			t.Errorf("%s", err)
		}
	}
	for i := 0; i < len(nodes); i++ {
		db.rootTree.Delete(nodes[i].document.id)
	}

	db.Close()
}