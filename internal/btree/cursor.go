package btree

import "bytes"

type cframe struct {
	b []byte
	i int
}

// Cursor moves over the keys of a tree in byte order.
//
// If the tree changes while the cursor is open, the cursor finds its
// place again from the last key it returned.
type Cursor struct {
	t     *Tree
	stack []cframe
	ver   uint64
	key   []byte
	err   error
}

// Cursor returns a new cursor. Call First or Seek before use.
func (t *Tree) Cursor() *Cursor { return &Cursor{t: t} }

// First moves to the first key.
func (c *Cursor) First() bool { return c.Seek(nil) }

// Seek moves to the first key at or above key.
func (c *Cursor) Seek(key []byte) bool {
	c.stack = c.stack[:0]
	c.ver = c.t.ver
	c.key = nil
	pg := c.t.root
	for {
		b, err := c.t.page(pg)
		if err != nil {
			return c.fail(err)
		}
		if pageType(b) == typeBranch {
			i := searchBranch(b, key)
			c.stack = append(c.stack, cframe{b, i})
			pg = branchChild(b, i)
			continue
		}
		i, _ := searchLeaf(b, key)
		c.stack = append(c.stack, cframe{b, i})
		break
	}
	if c.top().i >= nkeys(c.top().b) {
		return c.nextLeaf()
	}
	return c.load()
}

// Next moves to the next key.
func (c *Cursor) Next() bool {
	if c.err != nil || len(c.stack) == 0 {
		return false
	}
	if c.ver != c.t.ver {
		last := c.key
		if !c.Seek(last) {
			return false
		}
		if !bytes.Equal(c.key, last) {
			return true
		}
	}
	c.top().i++
	if c.top().i >= nkeys(c.top().b) {
		return c.nextLeaf()
	}
	return c.load()
}

func (c *Cursor) top() *cframe { return &c.stack[len(c.stack)-1] }

// nextLeaf moves to the first key of the next non-empty leaf.
func (c *Cursor) nextLeaf() bool {
	for {
		c.stack = c.stack[:len(c.stack)-1]
		if len(c.stack) == 0 {
			c.key = nil
			return false
		}
		f := c.top()
		if f.i >= nkeys(f.b) {
			continue
		}
		f.i++
		pg := branchChild(f.b, f.i)
		for {
			b, err := c.t.page(pg)
			if err != nil {
				return c.fail(err)
			}
			c.stack = append(c.stack, cframe{b, 0})
			if pageType(b) == typeLeaf {
				break
			}
			pg = branchChild(b, 0)
		}
		if nkeys(c.top().b) > 0 {
			return c.load()
		}
	}
}

func (c *Cursor) load() bool {
	f := c.top()
	c.key = clone(leafKey(f.b, f.i))
	return true
}

func (c *Cursor) fail(err error) bool {
	c.err = err
	c.stack = c.stack[:0]
	c.key = nil
	return false
}

// Valid reports whether the cursor is at a key.
func (c *Cursor) Valid() bool { return c.key != nil && c.err == nil }

// Key returns the current key. The caller must not change it.
func (c *Cursor) Key() []byte { return c.key }

// Value returns a copy of the current value.
func (c *Cursor) Value() ([]byte, error) {
	if !c.Valid() {
		return nil, c.err
	}
	if c.ver != c.t.ver {
		v, _, err := c.t.Get(c.key)
		return v, err
	}
	f := c.top()
	return c.t.readVal(leafVal(f.b, f.i))
}

// Err returns the error that stopped the cursor, if any.
func (c *Cursor) Err() error { return c.err }
