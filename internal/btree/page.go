package btree

import (
	"encoding/binary"

	"github.com/haibeey/doclite/internal/pager"
)

// Page layout. The last 4 bytes of a page are the pager checksum.
//
//	0 type   uint8
//	1 flags  uint8
//	2 nkeys  uint16
//	4 right  uint32 (branch: right-most child; others: next page)
//	8 slots  [nkeys]uint16 (offsets of the cells)
//	  cells
//
// Leaf cell:   keyLen uint16, valLen uint32, key, value
// Branch cell: child uint32, keyLen uint16, key
//
// If valLen has ovfFlag set, the value is in an overflow chain and the
// cell holds totalLen uint32 and firstPage uint32.
//
// Overflow page: type, next page at 4, used bytes at 8, data from 12.
//
// In a branch, child i holds the keys below key i, and the right-most
// child holds the keys at or above the last key.
const (
	typeLeaf     = 1
	typeBranch   = 2
	typeOverflow = 3

	pageHeader     = 8
	leafCellHeader = 6
	branchCellHdr  = 6
	ovfHeader      = 12
	ovfRefSize     = 8
	ovfFlag        = 1 << 31
	trailerSize    = 4
)

var le = binary.LittleEndian

func pageType(b []byte) byte   { return b[0] }
func nkeys(b []byte) int       { return int(le.Uint16(b[2:])) }
func slot(b []byte, i int) int { return int(le.Uint16(b[pageHeader+2*i:])) }

func rightChild(b []byte) pager.PgNo { return pager.PgNo(le.Uint32(b[4:])) }

func leafKey(b []byte, i int) []byte {
	o := slot(b, i)
	kl := int(le.Uint16(b[o:]))
	return b[o+leafCellHeader : o+leafCellHeader+kl]
}

// leafVal returns the raw value of cell i: the value itself, or the
// overflow reference if ovf is true.
func leafVal(b []byte, i int) (raw []byte, ovf bool) {
	o := slot(b, i)
	kl := int(le.Uint16(b[o:]))
	vl := le.Uint32(b[o+2:])
	start := o + leafCellHeader + kl
	if vl&ovfFlag != 0 {
		return b[start : start+ovfRefSize], true
	}
	return b[start : start+int(vl)], false
}

func branchChild(b []byte, i int) pager.PgNo {
	if i == nkeys(b) {
		return rightChild(b)
	}
	return pager.PgNo(le.Uint32(b[slot(b, i):]))
}

func branchKey(b []byte, i int) []byte {
	o := slot(b, i)
	kl := int(le.Uint16(b[o+4:]))
	return b[o+branchCellHdr : o+branchCellHdr+kl]
}

// node is the decoded form of a leaf or branch page. It is used to change
// a page. Reads work on the page bytes directly.
type node struct {
	leaf bool
	keys [][]byte
	vals []val        // leaf only
	kids []pager.PgNo // branch only, len(keys)+1
}

type val struct {
	data []byte // the value, or the overflow reference
	ovf  bool
}

func decode(b []byte) *node {
	n := nkeys(b)
	nd := &node{leaf: pageType(b) == typeLeaf, keys: make([][]byte, n)}
	if nd.leaf {
		nd.vals = make([]val, n)
		for i := 0; i < n; i++ {
			nd.keys[i] = clone(leafKey(b, i))
			raw, ovf := leafVal(b, i)
			nd.vals[i] = val{clone(raw), ovf}
		}
		return nd
	}
	nd.kids = make([]pager.PgNo, n+1)
	for i := 0; i < n; i++ {
		nd.keys[i] = clone(branchKey(b, i))
		nd.kids[i] = branchChild(b, i)
	}
	nd.kids[n] = rightChild(b)
	return nd
}

func (n *node) cellSize(i int) int {
	if n.leaf {
		return 2 + leafCellHeader + len(n.keys[i]) + len(n.vals[i].data)
	}
	return 2 + branchCellHdr + len(n.keys[i])
}

func (n *node) size() int {
	s := pageHeader
	for i := range n.keys {
		s += n.cellSize(i)
	}
	return s
}

// encode writes n into page b. The caller checks that n fits.
func (n *node) encode(b []byte) {
	clear(b)
	if n.leaf {
		b[0] = typeLeaf
	} else {
		b[0] = typeBranch
		le.PutUint32(b[4:], uint32(n.kids[len(n.keys)]))
	}
	le.PutUint16(b[2:], uint16(len(n.keys)))
	// Cells fill the page from the end, so that a new slot and a new
	// cell can be added without moving the other cells.
	o := len(b) - trailerSize
	for i, k := range n.keys {
		if n.leaf {
			o -= leafCellHeader + len(k) + len(n.vals[i].data)
			putLeafCell(b[o:], k, n.vals[i])
		} else {
			o -= branchCellHdr + len(k)
			le.PutUint32(b[o:], uint32(n.kids[i]))
			le.PutUint16(b[o+4:], uint16(len(k)))
			copy(b[o+branchCellHdr:], k)
		}
		le.PutUint16(b[pageHeader+2*i:], uint16(o))
	}
}

func putLeafCell(b []byte, k []byte, v val) {
	le.PutUint16(b, uint16(len(k)))
	vl := uint32(len(v.data))
	if v.ovf {
		vl = ovfFlag
	}
	le.PutUint32(b[2:], vl)
	copy(b[leafCellHeader:], k)
	copy(b[leafCellHeader+len(k):], v.data)
}

// insertLeafCell adds a cell at index i of leaf page b without a decode.
// It returns false if there is no free space between the slots and the
// cells; then the caller rewrites the whole page.
func insertLeafCell(b []byte, i int, k []byte, v val) bool {
	n := nkeys(b)
	low := len(b) - trailerSize
	for j := 0; j < n; j++ {
		low = min(low, slot(b, j))
	}
	size := leafCellHeader + len(k) + len(v.data)
	if low-size < pageHeader+2*(n+1) {
		return false
	}
	o := low - size
	putLeafCell(b[o:], k, v)
	copy(b[pageHeader+2*(i+1):pageHeader+2*(n+1)], b[pageHeader+2*i:pageHeader+2*n])
	le.PutUint16(b[pageHeader+2*i:], uint16(o))
	le.PutUint16(b[2:], uint16(n+1))
	return true
}

func clone(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
