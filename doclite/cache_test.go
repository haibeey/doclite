package doclite

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCache(t *testing.T) {
	var (
		numOfInsert = 10
	)
	MaxCacheSize = 100
	node := &Node{document: &Document{id: int64(100)}}
	db := &DB{metadata: &Meta{}}
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

/*
TestFindNodesPointerAliasing verifies that FindNodes returns a slice of independent
objects — no two elements share the same underlying pointer. This is a regression
test for the bug where all documents were unmarshaled into the same object variable
and that same reference was appended on each iteration, making all results identical.

AC: At least three matching documents are inserted, FindNodes is called, and each
element in the returned slice contains the correct distinct data.
*/
func TestFindNodesPointerAliasing(t *testing.T) {
	MaxCacheSize = 100

	db := &DB{metadata: &Meta{}}
	tree := db.newBtree("")

	// Insert documents with distinct data into the same root so they end up in one cache.
	// We insert at least 3 documents that all share a common field "Type" so they
	// match a filter, but each has a unique "Name" field.
	type testDoc struct {
		Type string
		Name string
	}

	docs := []testDoc{
		{Type: "user", Name: "alice"},
		{Type: "user", Name: "bob"},
		{Type: "user", Name: "charlie"},
	}

	rootNode := tree.createNode(int64(1), []byte("{}"), true)
	tree.addRoot(rootNode)

	for i, d := range docs {
		data, _ := json.Marshal(d)
		node := &Node{
			document: &Document{
				id:   int64(i + 2), // children are at root.id+1, root.id+2, ...
				data: data,
			},
		}
		rootNode.children.Add(node)
		rootNode.numChildren++
	}

	// Call FindNodes with a filter that matches all three documents.
	filter := map[string]interface{}{"Type": "user"}
	dummyObject := map[string]interface{}{}
	results, _ := rootNode.children.FindNodes(filter, dummyObject, 0)

	// AC: count matches actual matching documents
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// AC: each element contains distinct, correct data
	names := make(map[string]bool)
	for i, r := range results {
		doc, ok := r.(map[string]interface{})
		if !ok {
			t.Errorf("result[%d] is not a map, got %T", i, r)
			continue
		}
		name, ok := doc["Name"].(string)
		if !ok {
			t.Errorf("result[%d] missing string Name field", i)
			continue
		}
		if names[name] {
			t.Errorf("result[%d] has duplicate name %q — pointer aliasing detected", i, name)
		}
		names[name] = true
	}

	// Verify all expected names are present
	for _, d := range docs {
		if !names[d.Name] {
			t.Errorf("expected name %q not found in results", d.Name)
		}
	}

	// AC: no two elements share the same underlying pointer.
	// For maps, Go's reflect.Value.Pointer() gives the underlying data pointer,
	// so two different map variables will have different pointers.
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			ri := reflect.ValueOf(results[i])
			rj := reflect.ValueOf(results[j])
			if ri.Pointer() == rj.Pointer() {
				t.Errorf("results[%d] and results[%d] share the same underlying pointer (aliasing)", i, j)
			}
		}
	}
}
