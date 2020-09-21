package doclite

import (
	"encoding/json"
	"errors"
	"github.com/gammazero/deque"
	"reflect"
)

var (
	// MaxCacheSize is max amount of document that can be in memory
	MaxCacheSize = 30
	deleted      = []byte("deleted")
)

// Document  type representing a document
type Document struct {
	id        int64
	data      []byte
	offset    int64 // the seek pos for this data on disk
	dirty     bool
	isDeleted bool
}

//Data returns the conataining byte of a document for decoding
func (d *Document) Data() []byte {
	return d.data
}

func (d *Document) checkDeleted() {
	if d.data == nil {
		return
	}
	if len(d.data) != len(deleted) {
		return
	}
	for i := 0; i < len(deleted); i++ {
		if d.data[i] != deleted[i] {
			return
		}
	}
	d.isDeleted = true
}

// Cache to hold document in memory .uses LRU cache policy
type Cache struct {
	node            *Node // root node owning this cache
	db              *DB
	currentCapacity int
	nodes           deque.Deque
	ids             map[int64]*Node
	tree            *Btree
}

//NewCache initilize a new cache
func NewCache(db *DB, tree *Btree) *Cache {
	c := &Cache{db: db, tree: tree}
	c.ids = make(map[int64]*Node)
	return c
}

//Add insert a new item into this cache
func (c *Cache) Add(n *Node) {
	_, ok := c.ids[n.document.id]
	if !ok {
		if c.currentCapacity == MaxCacheSize {
			docToRemove := c.nodes.PopBack().(*Node)
			delete(c.ids, docToRemove.document.id)
			c.write(docToRemove)
			c.currentCapacity--
		}
	} else {
		c.remove(n)
	}
	c.nodes.PushFront(n)
	c.ids[n.document.id] = n
	c.currentCapacity++
}

func (c *Cache) remove(n *Node) error { //slow
	var i int
	for i = 0; i < c.nodes.Len(); i++ {
		node := c.nodes.At(i).(*Node)
		if n.document.id == node.document.id {
			break
		}
	}

	if i == 0 && c.nodes.At(i).(*Node) == n {
		c.nodes.PopFront()
		c.currentCapacity--
		return nil
	}
	if i != c.nodes.Len() {
		c.currentCapacity--
		c.nodes.Set(i, c.nodes.Back())
		c.nodes.PopBack()
		return nil
	}
	return errors.New("Not Found")
}

func (c *Cache) get(id int64) (*Node, error) {
	var err error

	if id == c.node.document.id {
		return c.node, nil
	}

	if id > c.node.document.id && id-c.node.document.id > int64(c.node.numChildren) {
		return nil, err
	}

	n, ok := c.ids[id]
	if ok {
		return n, nil
	}

	doc := &Document{id: id, data: []byte{}}
	n = &Node{document: doc}
	n.document.offset = c.node.document.offset + dataSize*((id-1)%MinKeys)
	doc.data, err = c.read(n)

	doc.checkDeleted()
	c.Add(n)
	return n, err
}

//Delete remove item into this cache
func (c *Cache) Delete(id int64) {
	if id == c.node.document.id {
		c.node.document.data = []byte("deleted")
		c.write(c.node)
		return
	}
	n, err := c.get(id)
	if err != nil {
		return
	}
	if n != nil {
		if err := c.remove(n); err == nil {
			delete(c.ids, n.document.id)
			n.document.data = []byte("deleted")
			c.write(n)
		}
	}
}

func checkMatch(filter, content map[string]interface{}) bool {
	for key, val := range filter {
		docVal, ok := content[key]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(docVal, val) {
			return false
		}
	}
	return true
}

// Find gets all nodes matching a criterions specified by filter
func (c *Cache) Find(filter interface{}, start int) ([]interface{}, int) {
	countOfFound := 0
	nodes := []interface{}{}
	filterMap := toMap(filter)
	for i := start; i < c.node.numChildren; i++ {
		if countOfFound == lookAheadNodeSize {
			return nodes, i
		}

		countOfFound++
		n, err := c.get(c.node.document.id + int64(i+1))
		if n == nil || err != nil {
			continue
		}
		buf := n.document.data
		d:=make(map[string]interface{})
		err = json.Unmarshal(buf, &d)

		if err != nil {
			continue
		}
		docMap := toMap(d)
		if !checkMatch(filterMap, docMap) {
			continue
		}

		nodes = append(nodes, docMap)
	}

	return nodes, c.node.numChildren
}

func (c *Cache) checkRootMatched(filter interface{}) interface{} {
	buf := c.node.document.data
	filterMap := toMap(filter)
	d:=make(map[string]interface{})
	err := json.Unmarshal(buf, &d)

	if err != nil {
		return nil
	}
	docMap := toMap(d)
	if !checkMatch(filterMap, docMap) {
		return nil
	}
	return docMap
}

//DeleteAll deletes all document matching filter
func (c *Cache) DeleteAll(filter interface{}, doc interface{}) []int64 {
	filterMap := toMap(filter)
	ids := make([]int64, 0)
	for i := 0; i <= c.node.numChildren; i++ {
		n, err := c.get(c.node.document.id + int64(i))
		if n == nil || err != nil {
			continue
		}
		buf := n.document.data
		err = json.Unmarshal(buf, &doc)
		if err != nil {
			continue
		}
		docMap := toMap(doc)
		if checkMatch(filterMap, docMap) {
			c.Delete(n.document.id)
			ids = append(ids, n.document.id)
		}
	}
	return ids
}

// Save insert this cache to into disk
func (c *Cache) Save() {
	c.write(c.node)
	for i := 0; i < c.nodes.Len(); i++ {
		c.write(c.nodes.At(i).(*Node))
	}
}
