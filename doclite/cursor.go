package doclite

//import "fmt"

const (
	lookAheadNodeSize = 30
)

//CacheCursor object supplies matching Nodes for a search
//export CacheCursor
type CacheCursor struct {
	rootNode *Node
	// nodes that matches the supplied filter
	matchingNodes []interface{}
	filter        interface{}
	exhausted     int // the amount of nodes serched so far
}

func newCacheCur(root *Node, filter interface{}) *CacheCursor {
	docMatched := root.children.checkRootMatched(filter)
	matchingNodes := []interface{}{}
	if docMatched != nil {
		matchingNodes = append(matchingNodes, docMatched)
	}
	return &CacheCursor{
		rootNode:      root,
		matchingNodes: matchingNodes,
		filter:        filter,
	}
}

func (cc *CacheCursor) renew() {
	cc.matchingNodes = []interface{}{}
	cc.exhausted = 0
}

func (cc *CacheCursor) next() interface{} {
	if len(cc.matchingNodes) <= lookAheadNodeSize {
		nodes, exhausted := cc.rootNode.children.Find(cc.filter, cc.exhausted)
		cc.exhausted = exhausted
		cc.matchingNodes = append(cc.matchingNodes, nodes...)
	}
	if len(cc.matchingNodes) <= 0 {
		return nil
	}
	doc := cc.matchingNodes[0]

	cc.matchingNodes = cc.matchingNodes[1:]
	return doc
}

//Cursor object supplies matching Nodes for a search
//export Cursor
type Cursor struct {
	cacheCursors       []*CacheCursor
	servingCacheCursor *CacheCursor
}

//NewCur returns a new Cursor object
func NewCur() *Cursor {
	return &Cursor{cacheCursors: []*CacheCursor{}}
}

//Next return the next matching node of filter
func (c *Cursor) Next() interface{} {
	if len(c.cacheCursors) <= 0 {
		return nil
	}
	doc := c.servingCacheCursor.next()
	if doc == nil {
		c.cacheCursors = c.cacheCursors[1:]
		if len(c.cacheCursors) > 0 {
			c.servingCacheCursor = c.cacheCursors[0]
			return c.Next()
		}
	}
	return doc
}

func (c *Cursor) addCacheCursor(cc *CacheCursor) {
	if len(c.cacheCursors) <= 0 {
		c.servingCacheCursor = cc
	}
	c.cacheCursors = append(c.cacheCursors, cc)
}
