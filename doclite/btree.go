package doclite

import (
	"errors"
	"fmt"
	"sync"
)

const (
	// MinKeys minimum number of keys in a node
	// this automatically makes the upper bound of 2 * Minkeys
	MinKeys = pageSize / dataSize

	// BtreeMaxSize is maximum size of disk space btree could take
	// before it overflows
	BtreeMaxSize = 10000000
)

// A Btree object
type Btree struct {
	NumDocuments   int64
	nDocMutex      sync.Mutex
	NumRoots       int
	SubCollections map[string]*Btree
	Pool           []int64         // pool of available ids
	findPool       map[int64]int64 //in memory pool finder
	Pages          []int64         // the pages this bree occupies

	roots         []*Node
	db            *DB
	initBtreeRoot bool
}

// The Node in our btree
type Node struct {
	numChildren int
	children    *Cache // nil for child Nodes

	isRoot bool
	isLeaf bool
	// if this is a leaf node then we have a collection in this node.
	// children would also be empty
	document *Document
}

func (t *Btree) incNumDocs() {
	t.nDocMutex.Lock()
	t.NumDocuments++
	t.nDocMutex.Unlock()
}

// createNode Creates a new B-tree
func (t *Btree) createNode(id int64, data []byte, isRoot bool) *Node {
	doc := &Document{id: id, data: data}
	if isRoot {
		c := NewCache(t.db)
		c.node = &Node{children: c, document: doc, isRoot: isRoot}
		return c.node
	}
	return &Node{document: doc}
}

// InsertSubCollection insert a newSubCollection to this btree
func (t *Btree) InsertSubCollection(name string) {
	_, ok := t.SubCollections[name]
	if !ok {
		t.SubCollections[name] = t.db.newBtree()
		t.db.metadata.incCollections()
	}
}

// Get returns a subcollection
func (t *Btree) Get(name string) *Btree {

	tree, ok := t.SubCollections[name]
	if !ok {
		t.InsertSubCollection(name)
		return t.Get(name)
	}

	if !tree.initBtreeRoot {
		tree.db = t.db
		tree.findPool = make(map[int64]int64)
		tree.diskInitBtree()
	}
	tree.initBtreeRoot = true
	return tree
}

func (t *Btree) diskInitBtree() {
	for i := 0; i < t.NumRoots; i++ {
		data := []byte{}
		node := t.createNode(maxInt64(int64(i)*MinKeys, 1), data, true)
		node.document.offset = t.Pages[i] * pageSize
		fmt.Println(node.document.offset)
		node.document.data, _ = node.children.read(node)
		node.numChildren = MinKeys
		if i+1 == t.NumRoots {
			node.numChildren = int(t.NumDocuments % int64(MinKeys))
		}
		t.roots = append(t.roots, node)
	}
	for i := 0; i < len(t.Pool); i++ {
		t.findPool[t.Pool[i]] = t.Pool[i]
	}
}

// Insert an item into the btree with the specified key
func (t *Btree) Insert(data []byte) int64 {
	id := t.NumDocuments + 1
	lp := len(t.Pool)
	fromPool := false
	if lp > 0 {
		id = t.Pool[lp-1]
		t.Pool = t.Pool[:lp-1]
		fromPool = true
	}
	node := t.createNode(id, data, true)
	if t.NumDocuments == 0 {
		// The root parent is nil
		t.roots = append(t.roots, node)
		t.NumRoots++
		t.db.metadata.incPages()
		t.Pages = append(t.Pages, t.db.metadata.NumPages)
		node.document.offset = t.db.metadata.NumPages * pageSize
		t.db.metadata.OverflowDataOffset = maxInt64(node.document.offset+pageSize, t.db.metadata.OverflowDataOffset)

	} else {
		if (id-1)%MinKeys == 0 {
			if fromPool {
				t.Update(id, data)
			} else {
				t.roots = append(t.roots, node)
				t.NumRoots++
				t.db.metadata.incPages()
				t.Pages = append(t.Pages, t.db.metadata.NumPages)
				node.document.offset = int64(t.db.metadata.NumPages * pageSize)
				t.db.metadata.OverflowDataOffset = maxInt64(node.document.offset+pageSize, t.db.metadata.OverflowDataOffset)
			}
		} else {
			node.isRoot = false
			nodeToInsert, err := t.findFitingNode(id)
			if err != nil {
				return -1
			}
			node.document.offset = nodeToInsert.document.offset + int64(dataSize*((id-1)%MinKeys))
			nodeToInsert.insertNonFull(node)
			if !fromPool {
				nodeToInsert.numChildren++
			}
		}
	}

	if !fromPool {
		t.incNumDocs()
	}
	return id
}

// returns the best fitted leafs parent node to insert this id in terms of best pos
func (t *Btree) findFitingNode(id int64) (*Node, error) {
	index := binarySearch(id, t.roots, t.NumRoots)
	if index < t.NumRoots {
		return t.roots[index], nil
	}
	if len(t.roots) <= 0 {
		return nil, errors.New("not found")
	}
	return t.roots[0], nil
}

// Delete an item into the btree with the specified key
func (t *Btree) Delete(id int64) {
	nodeToDeleteFrom, err := t.findFitingNode(id)
	if err != nil {
		return
	}
	nodeToDeleteFrom.children.Delete(id)
	_, ok := t.findPool[id]
	if !ok {
		t.Pool = append(t.Pool, id)
		t.findPool[id] = id
	}
}

// InsertOrUpdate update an item in the btree with the specified key if found
// insert it with a new id if found
func (t *Btree) InsertOrUpdate(id int64, data []byte) int64 {
	node, err := t.Find(id)
	if err != nil {
		return t.Insert(data)
	}
	node.document.data = data
	return id
}

// Update an item into the btree with the specified key
func (t *Btree) Update(id int64, data []byte) error {
	node, err := t.Find(id)
	if err != nil {
		return err
	}
	node.document.data = data
	return err
}

// Find an item into the btree with the specified key
func (t *Btree) Find(id int64) (*Node, error) {
	parent, err := t.findFitingNode(id)
	if err != nil {
		return nil, err
	}
	if parent.document.id == id {
		return parent, nil
	}
	return parent.children.get(id)
}

/*
FindAll finds items in the btree matching a filter and a doc
*/
func (t *Btree) FindAll(filter, doc interface{}) *Cursor {
	cur := NewCur()
	for _, node := range t.roots {
		cur.addCacheCursor(newCacheCur(node, filter, doc))
	}
	return cur
}

/*
DeleteAll deletes all document matching a criterion
*/
func (t *Btree) DeleteAll(filter, doc interface{}) {
	for _, node := range t.roots {
		for _, id := range node.children.DeleteAll(filter, doc) {
			_, ok := t.findPool[id]
			if !ok {
				t.Pool = append(t.Pool, id)
				t.findPool[id] = id
			}
		}
	}
}

// ShowCase is used to tanverse this node
func (t *Btree) ShowCase() {
	fmt.Println(t.Pool)
	fmt.Println(t.roots)
	fmt.Println(t.Pages)
	for _, n := range t.roots {
		fmt.Println(n.document.id)
		for _, c := range n.children.ids {
			if c != nil {
				fmt.Println(c.document.id)
			} else {
				fmt.Println(n, c, n.children)
			}
		}
	}
	fmt.Println(t.NumDocuments)
}

// Save insert all cache node to into disk
func (t *Btree) Save() {
	for _, n := range t.roots {
		n.save()
	}
	for _,subT:=range t.SubCollections{
		subT.Save()
	}
}

func (n *Node) String() string {
	return fmt.Sprintf("%d %d", n.document.id, n.document.offset)
}

func (n *Node) insertNonFull(node *Node) {
	node.isLeaf = true
	n.children.Add(node)
}

func (n *Node) save() {
	n.children.Save()
}

//Doc returns the document for a node
func (n *Node) Doc() *Document {
	return n.document
}
