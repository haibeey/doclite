// Package btree implements a B+tree on top of the pager. Keys are byte
// strings in byte order. Values can be of any size: a large value is kept
// in a chain of overflow pages.
package btree

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/haibeey/doclite/internal/pager"
)

// Reader gives read access to pages.
type Reader interface {
	Page(pager.PgNo) ([]byte, error)
	PageSize() int
}

// Writer gives write access to pages.
type Writer interface {
	Reader
	PageForWrite(pager.PgNo) ([]byte, error)
	Allocate() (pager.PgNo, []byte, error)
	Free(pager.PgNo) error
}

// ErrKeyTooLarge is returned for a key longer than MaxKeySize.
var ErrKeyTooLarge = errors.New("doclite: key is too large")

var errReadOnly = errors.New("doclite: tree is read-only")

// Tree is a B+tree. A Tree is not safe for use by many goroutines.
type Tree struct {
	r    Reader
	w    Writer // nil for a read-only tree
	root pager.PgNo
	ver  uint64 // changes on each write; cursors use it
}

// Create makes a new, empty tree and returns its root page.
func Create(w Writer) (pager.PgNo, error) {
	pg, b, err := w.Allocate()
	if err != nil {
		return 0, err
	}
	(&node{leaf: true}).encode(b)
	return pg, nil
}

// Open returns the tree with root page root. The tree can be changed only
// if r is also a Writer.
func Open(r Reader, root pager.PgNo) *Tree {
	t := &Tree{r: r, root: root}
	if w, ok := r.(Writer); ok {
		t.w = w
	}
	return t
}

// Root returns the root page. It changes when the root splits or shrinks.
func (t *Tree) Root() pager.PgNo { return t.root }

func usable(r Reader) int { return r.PageSize() - trailerSize }

// maxLeafCell is the largest leaf cell (with its slot) that is kept on
// the page. At least four cells fit on a page, so a split always works.
func maxLeafCell(r Reader) int { return (usable(r)-pageHeader)/4 - 2 }

// MaxKeySize returns the longest key for a page size.
func MaxKeySize(pageSize int) int {
	u := pageSize - trailerSize
	return (u-pageHeader)/4 - 2 - leafCellHeader - ovfRefSize
}

// MaxKeySize returns the longest key that the tree accepts.
func (t *Tree) MaxKeySize() int { return MaxKeySize(t.r.PageSize()) }

func (t *Tree) page(pg pager.PgNo) ([]byte, error) {
	b, err := t.r.Page(pg)
	if err != nil {
		return nil, err
	}
	if typ := pageType(b); typ != typeLeaf && typ != typeBranch {
		return nil, fmt.Errorf("%w: page %d is not a tree page", pager.ErrCorrupt, pg)
	}
	return b, nil
}

// searchBranch returns the index of the child that holds key.
func searchBranch(b []byte, key []byte) int {
	return sort.Search(nkeys(b), func(i int) bool { return bytes.Compare(key, branchKey(b, i)) < 0 })
}

// searchLeaf returns the index of the first key at or above key.
func searchLeaf(b []byte, key []byte) (int, bool) {
	n := nkeys(b)
	i := sort.Search(n, func(i int) bool { return bytes.Compare(leafKey(b, i), key) >= 0 })
	return i, i < n && bytes.Equal(leafKey(b, i), key)
}

// Get returns a copy of the value of key.
func (t *Tree) Get(key []byte) ([]byte, bool, error) {
	pg := t.root
	for {
		b, err := t.page(pg)
		if err != nil {
			return nil, false, err
		}
		if pageType(b) == typeBranch {
			pg = branchChild(b, searchBranch(b, key))
			continue
		}
		i, ok := searchLeaf(b, key)
		if !ok {
			return nil, false, nil
		}
		v, err := t.readVal(leafVal(b, i))
		return v, err == nil, err
	}
}

func (t *Tree) readVal(raw []byte, ovf bool) ([]byte, error) {
	if !ovf {
		return clone(raw), nil
	}
	total := int(le.Uint32(raw))
	out := make([]byte, 0, total)
	for pg := pager.PgNo(le.Uint32(raw[4:])); pg != 0; {
		b, err := t.r.Page(pg)
		if err != nil {
			return nil, err
		}
		n := int(le.Uint32(b[8:]))
		if pageType(b) != typeOverflow || ovfHeader+n > usable(t.r) || len(out)+n > total {
			return nil, fmt.Errorf("%w: bad overflow page %d", pager.ErrCorrupt, pg)
		}
		out = append(out, b[ovfHeader:ovfHeader+n]...)
		pg = pager.PgNo(le.Uint32(b[4:]))
	}
	if len(out) != total {
		return nil, fmt.Errorf("%w: overflow chain is short", pager.ErrCorrupt)
	}
	return out, nil
}

// makeVal returns the cell value for v. A value that does not fit in
// the cell is written to new overflow pages.
func (t *Tree) makeVal(key, v []byte) (val, error) {
	if 2+leafCellHeader+len(key)+len(v) <= maxLeafCell(t.r) {
		return val{data: clone(v)}, nil
	}
	per := usable(t.r) - ovfHeader
	var first, prev pager.PgNo
	var prevB []byte
	for off := 0; off < len(v); off += per {
		pg, b, err := t.w.Allocate()
		if err != nil {
			return val{}, err
		}
		end := min(off+per, len(v))
		b[0] = typeOverflow
		le.PutUint32(b[8:], uint32(end-off))
		copy(b[ovfHeader:], v[off:end])
		if prev == 0 {
			first = pg
		} else {
			le.PutUint32(prevB[4:], uint32(pg))
		}
		prev, prevB = pg, b
	}
	ref := make([]byte, ovfRefSize)
	le.PutUint32(ref, uint32(len(v)))
	le.PutUint32(ref[4:], uint32(first))
	return val{data: ref, ovf: true}, nil
}

func (t *Tree) freeVal(v val) error {
	if !v.ovf {
		return nil
	}
	for pg := pager.PgNo(le.Uint32(v.data[4:])); pg != 0; {
		b, err := t.r.Page(pg)
		if err != nil {
			return err
		}
		if pageType(b) != typeOverflow {
			return fmt.Errorf("%w: bad overflow page %d", pager.ErrCorrupt, pg)
		}
		next := pager.PgNo(le.Uint32(b[4:]))
		if err := t.w.Free(pg); err != nil {
			return err
		}
		pg = next
	}
	return nil
}

type step struct {
	pg  pager.PgNo
	idx int // the child index taken in this branch
}

// descend returns the path from the root to the leaf that holds key. The
// last step is the leaf.
func (t *Tree) descend(key []byte) ([]step, []byte, error) {
	var path []step
	pg := t.root
	for {
		b, err := t.page(pg)
		if err != nil {
			return nil, nil, err
		}
		if pageType(b) == typeLeaf {
			return append(path, step{pg: pg}), b, nil
		}
		i := searchBranch(b, key)
		path = append(path, step{pg, i})
		pg = branchChild(b, i)
	}
}

// Put sets the value of key.
func (t *Tree) Put(key, value []byte) error {
	if t.w == nil {
		return errReadOnly
	}
	if len(key) > t.MaxKeySize() {
		return ErrKeyTooLarge
	}
	path, b, err := t.descend(key)
	if err != nil {
		return err
	}
	i, found := searchLeaf(b, key)
	v, err := t.makeVal(key, value)
	if err != nil {
		return err
	}
	t.ver++
	if !found {
		wb, err := t.w.PageForWrite(path[len(path)-1].pg)
		if err != nil {
			return err
		}
		if insertLeafCell(wb, i, key, v) {
			return nil
		}
		b = wb
	}
	n := decode(b)
	if found {
		if err := t.freeVal(n.vals[i]); err != nil {
			return err
		}
		n.vals[i] = v
	} else {
		n.keys = insertAt(n.keys, i, clone(key))
		n.vals = insertAt(n.vals, i, v)
	}
	return t.store(path, len(path)-1, n)
}

// store writes node n at path[d], and splits it if it does not fit.
func (t *Tree) store(path []step, d int, n *node) error {
	pg := path[d].pg
	if n.size() <= usable(t.r) {
		return t.write(pg, n)
	}
	left, right, sep := split(n)
	if err := t.write(pg, left); err != nil {
		return err
	}
	rpg, rb, err := t.w.Allocate()
	if err != nil {
		return err
	}
	right.encode(rb)
	if d == 0 {
		root := &node{keys: [][]byte{sep}, kids: []pager.PgNo{pg, rpg}}
		npg, nb, err := t.w.Allocate()
		if err != nil {
			return err
		}
		root.encode(nb)
		t.root = npg
		return nil
	}
	pb, err := t.page(path[d-1].pg)
	if err != nil {
		return err
	}
	parent := decode(pb)
	idx := path[d-1].idx
	parent.keys = insertAt(parent.keys, idx, sep)
	parent.kids = insertAt(parent.kids, idx+1, rpg)
	return t.store(path, d-1, parent)
}

func (t *Tree) write(pg pager.PgNo, n *node) error {
	b, err := t.w.PageForWrite(pg)
	if err != nil {
		return err
	}
	n.encode(b)
	return nil
}

// split cuts an over-full node into two halves of about the same size.
// sep is the first key of the right half.
func split(n *node) (left, right *node, sep []byte) {
	total := n.size()
	acc, m := pageHeader, 0
	for m < len(n.keys)-1 {
		acc += n.cellSize(m)
		m++
		if acc >= total/2 {
			break
		}
	}
	if n.leaf {
		m = max(1, m)
		left = &node{leaf: true, keys: n.keys[:m:m], vals: n.vals[:m:m]}
		right = &node{leaf: true, keys: n.keys[m:], vals: n.vals[m:]}
		return left, right, clone(n.keys[m])
	}
	// In a branch, key m moves up to the parent.
	m = min(max(1, m), len(n.keys)-2)
	left = &node{keys: n.keys[:m:m], kids: n.kids[: m+1 : m+1]}
	right = &node{keys: n.keys[m+1:], kids: n.kids[m+1:]}
	return left, right, n.keys[m]
}

// Delete removes key. It reports whether the key was in the tree.
func (t *Tree) Delete(key []byte) (bool, error) {
	if t.w == nil {
		return false, errReadOnly
	}
	path, b, err := t.descend(key)
	if err != nil {
		return false, err
	}
	i, found := searchLeaf(b, key)
	if !found {
		return false, nil
	}
	n := decode(b)
	if err := t.freeVal(n.vals[i]); err != nil {
		return false, err
	}
	n.keys = removeAt(n.keys, i)
	n.vals = removeAt(n.vals, i)
	t.ver++
	return true, t.rebalance(path, len(path)-1, n)
}

// rebalance writes node n at path[d]. If n is small, it is merged with
// a sibling when the two fit on one page.
func (t *Tree) rebalance(path []step, d int, n *node) error {
	pg := path[d].pg
	if d == 0 {
		if !n.leaf && len(n.keys) == 0 {
			t.root = n.kids[0]
			return t.w.Free(pg)
		}
		return t.write(pg, n)
	}
	if n.size() >= usable(t.r)/4 {
		return t.write(pg, n)
	}
	pb, err := t.page(path[d-1].pg)
	if err != nil {
		return err
	}
	parent := decode(pb)
	idx := path[d-1].idx
	if len(parent.kids) < 2 {
		return t.write(pg, n)
	}
	li := idx - 1 // merge kids[li] and kids[li+1]
	if idx == 0 {
		li = 0
	}
	sibPg := parent.kids[li]
	if li == idx {
		sibPg = parent.kids[li+1]
	}
	sb, err := t.page(sibPg)
	if err != nil {
		return err
	}
	sib := decode(sb)
	left, right := sib, n
	if li == idx {
		left, right = n, sib
	}
	merged := &node{leaf: n.leaf}
	merged.keys = append(append(merged.keys, left.keys...), right.keys...)
	if n.leaf {
		merged.vals = append(append(merged.vals, left.vals...), right.vals...)
	} else {
		merged.keys = append(append(append([][]byte{}, left.keys...), parent.keys[li]), right.keys...)
		merged.kids = append(append(merged.kids, left.kids...), right.kids...)
	}
	if merged.size() > usable(t.r) {
		return t.write(pg, n)
	}
	if err := t.write(parent.kids[li], merged); err != nil {
		return err
	}
	if err := t.w.Free(parent.kids[li+1]); err != nil {
		return err
	}
	parent.keys = removeAt(parent.keys, li)
	parent.kids = removeAt(parent.kids, li+1)
	return t.rebalance(path, d-1, parent)
}

// Drop frees every page of the tree.
func (t *Tree) Drop() error {
	if t.w == nil {
		return errReadOnly
	}
	err := t.drop(t.root)
	t.ver++
	return err
}

func (t *Tree) drop(pg pager.PgNo) error {
	b, err := t.page(pg)
	if err != nil {
		return err
	}
	n := decode(b)
	if n.leaf {
		for _, v := range n.vals {
			if err := t.freeVal(v); err != nil {
				return err
			}
		}
	} else {
		for _, k := range n.kids {
			if err := t.drop(k); err != nil {
				return err
			}
		}
	}
	return t.w.Free(pg)
}

// Check verifies the structure of the tree. It calls visit for each page
// that the tree uses, and returns the number of keys.
func (t *Tree) Check(visit func(pager.PgNo) error) (int, error) {
	leafDepth := -1
	count := 0
	var walk func(pg pager.PgNo, lo, hi []byte, depth int) error
	walk = func(pg pager.PgNo, lo, hi []byte, depth int) error {
		if err := visit(pg); err != nil {
			return err
		}
		b, err := t.page(pg)
		if err != nil {
			return err
		}
		n := nkeys(b)
		var prev []byte
		for i := 0; i < n; i++ {
			var k []byte
			if pageType(b) == typeLeaf {
				k = leafKey(b, i)
			} else {
				k = branchKey(b, i)
			}
			if (prev != nil && bytes.Compare(prev, k) >= 0) ||
				(lo != nil && bytes.Compare(k, lo) < 0) ||
				(hi != nil && bytes.Compare(k, hi) >= 0) {
				return fmt.Errorf("%w: keys out of order on page %d", pager.ErrCorrupt, pg)
			}
			prev = k
		}
		if pageType(b) == typeLeaf {
			if leafDepth == -1 {
				leafDepth = depth
			} else if leafDepth != depth {
				return fmt.Errorf("%w: leaves at different depths", pager.ErrCorrupt)
			}
			count += n
			for i := 0; i < n; i++ {
				raw, ovf := leafVal(b, i)
				if !ovf {
					continue
				}
				for opg := pager.PgNo(le.Uint32(raw[4:])); opg != 0; {
					if err := visit(opg); err != nil {
						return err
					}
					ob, err := t.r.Page(opg)
					if err != nil {
						return err
					}
					if pageType(ob) != typeOverflow {
						return fmt.Errorf("%w: bad overflow page %d", pager.ErrCorrupt, opg)
					}
					opg = pager.PgNo(le.Uint32(ob[4:]))
				}
				if _, err := t.readVal(raw, true); err != nil {
					return err
				}
			}
			return nil
		}
		for i := 0; i <= n; i++ {
			clo, chi := lo, hi
			if i > 0 {
				clo = branchKey(b, i-1)
			}
			if i < n {
				chi = branchKey(b, i)
			}
			if err := walk(branchChild(b, i), clo, chi, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	err := walk(t.root, nil, nil, 0)
	return count, err
}

func insertAt[T any](s []T, i int, v T) []T {
	s = append(s, v)
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}

func removeAt[T any](s []T, i int) []T {
	return append(s[:i], s[i+1:]...)
}
