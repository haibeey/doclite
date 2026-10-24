package doclite

import (
	"bytes"
	"encoding/json"
	"math"
	"sort"

	"github.com/haibeey/doclite/internal/btree"
	"github.com/haibeey/doclite/internal/pager"
)

// index is a secondary index on one field path. Its keys are the
// encoded field value followed by the 8-byte document id. If the field
// is an array, each element is a key.
type index struct {
	field string
	path  []string
	tree  *btree.Tree
}

// Index key value tags. The order of the tags is the sort order of the
// kinds.
const (
	tagNull   = 0x05
	tagFalse  = 0x10
	tagTrue   = 0x11
	tagNumber = 0x20
	tagString = 0x30
)

// maxIndexString returns how many bytes of a string go into an index
// key. Longer strings are cut; the filter then checks the full value.
func maxIndexString(t *btree.Tree) int { return (t.MaxKeySize() - 8 - 3) / 2 }

// encodeIndexValue returns the order-preserving encoding of v. Objects
// and arrays can not be encoded.
func encodeIndexValue(v any, maxStr int) ([]byte, bool) {
	switch x := v.(type) {
	case nil:
		return []byte{tagNull}, true
	case bool:
		if x {
			return []byte{tagTrue}, true
		}
		return []byte{tagFalse}, true
	case json.Number:
		f, err := x.Float64()
		if err != nil && !math.IsInf(f, 0) {
			return nil, false
		}
		if f == 0 {
			f = 0 // no negative zero
		}
		bits := math.Float64bits(f)
		if f >= 0 {
			bits ^= 1 << 63
		} else {
			bits = ^bits
		}
		b := make([]byte, 9)
		b[0] = tagNumber
		for i := 0; i < 8; i++ {
			b[1+i] = byte(bits >> (56 - 8*i))
		}
		return b, true
	case string:
		if len(x) > maxStr {
			x = x[:maxStr]
		}
		b := make([]byte, 0, len(x)+3)
		b = append(b, tagString)
		for i := 0; i < len(x); i++ {
			b = append(b, x[i])
			if x[i] == 0 {
				b = append(b, 0xFF)
			}
		}
		return append(b, 0, 0), true
	}
	return nil, false
}

// keys returns the index keys of a document.
func (ix *index) keys(doc any, id int64) [][]byte {
	vals, found := lookup(doc, ix.path)
	if !found {
		return nil
	}
	maxStr := maxIndexString(ix.tree)
	seen := map[string]bool{}
	var out [][]byte
	for _, v := range vals {
		enc, ok := encodeIndexValue(v, maxStr)
		if !ok {
			continue
		}
		k := append(enc, idKey(id)...)
		if !seen[string(k)] {
			seen[string(k)] = true
			out = append(out, k)
		}
	}
	return out
}

func (s *collState) indexAdd(id int64, doc any) error {
	for _, ix := range s.indexes {
		for _, k := range ix.keys(doc, id) {
			if err := ix.tree.Put(k, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *collState) indexRemove(id int64, doc any) error {
	for _, ix := range s.indexes {
		for _, k := range ix.keys(doc, id) {
			if _, err := ix.tree.Delete(k); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *collState) findIndex(field string) int {
	for i, ix := range s.indexes {
		if ix.field == field {
			return i
		}
	}
	return -1
}

// checkIndexes verifies that each index holds exactly the keys of the
// documents.
func (s *collState) checkIndexes(visit func(pg pager.PgNo) error) error {
	for _, ix := range s.indexes {
		n, err := ix.tree.Check(visit)
		if err != nil {
			return err
		}
		want := 0
		c := s.tree.Cursor()
		for ok := c.First(); ok; ok = c.Next() {
			raw, err := c.Value()
			if err != nil {
				return err
			}
			doc, err := decodeJSON(raw)
			if err != nil {
				return corruptf("document %d is not valid JSON", keyID(c.Key()))
			}
			for _, k := range ix.keys(doc, keyID(c.Key())) {
				if _, ok, err := ix.tree.Get(k); err != nil || !ok {
					return corruptf("index %q has no key for document %d", ix.field, keyID(c.Key()))
				}
				want++
			}
		}
		if err := c.Err(); err != nil {
			return err
		}
		if n != want {
			return corruptf("index %q has %d keys, want %d", ix.field, n, want)
		}
	}
	return nil
}

// keyRange is a range of index keys from lo to hi, both included. If
// prefix is set, the range is the keys that start with it.
type keyRange struct {
	prefix []byte
	lo, hi []byte
}

type planChoice struct {
	ix     *index
	ranges []keyRange
	score  int
}

// plan finds an index that can answer part of filter f. It returns the
// ids of the documents that can match, in order. ok is false if no index
// can be used; then the caller scans the collection. The caller must
// still test each document against the full filter.
func (s *collState) plan(f map[string]any) (ids []int64, ok bool, err error) {
	if len(s.indexes) == 0 || len(f) == 0 {
		return nil, false, nil
	}
	var best planChoice
	var consider func(m map[string]any)
	consider = func(m map[string]any) {
		for key, v := range m {
			if key == "$and" {
				if list, ok := v.([]any); ok {
					for _, x := range list {
						if sm, ok := x.(map[string]any); ok {
							consider(sm)
						}
					}
				}
				continue
			}
			i := s.findIndex(key)
			if i < 0 {
				continue
			}
			if c := planField(s.indexes[i], v); c.score > best.score {
				best = c
			}
		}
	}
	consider(f)
	if best.ix == nil {
		return nil, false, nil
	}
	seen := map[int64]bool{}
	c := best.ix.tree.Cursor()
	for _, r := range best.ranges {
		start := r.lo
		if r.prefix != nil {
			start = r.prefix
		}
		for ok := c.Seek(start); ok; ok = c.Next() {
			k := c.Key()
			if r.prefix != nil && !bytes.HasPrefix(k, r.prefix) {
				break
			}
			if r.prefix == nil && bytes.Compare(k, r.hi) > 0 {
				break
			}
			if id := keyID(k[len(k)-8:]); !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		if err := c.Err(); err != nil {
			return nil, false, err
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, true, nil
}

// indexable reports whether an equality on v can use an index. Missing
// fields are not in the index, so null can not.
func indexable(v any) bool {
	switch v.(type) {
	case bool, string, json.Number:
		return true
	}
	return false
}

func planField(ix *index, v any) planChoice {
	maxStr := maxIndexString(ix.tree)
	eq := func(x any) planChoice {
		enc, _ := encodeIndexValue(x, maxStr)
		return planChoice{ix: ix, ranges: []keyRange{{prefix: enc}}, score: 3}
	}
	ops, isOps := isOperatorDoc(v)
	if !isOps {
		if indexable(v) {
			return eq(v)
		}
		return planChoice{}
	}
	if x, ok := ops["$eq"]; ok && indexable(x) {
		return eq(x)
	}
	if list, ok := ops["$in"].([]any); ok && len(list) > 0 {
		c := planChoice{ix: ix, score: 2}
		for _, x := range list {
			if !indexable(x) {
				return planChoice{}
			}
			enc, _ := encodeIndexValue(x, maxStr)
			c.ranges = append(c.ranges, keyRange{prefix: enc})
		}
		return c
	}
	var lo, hi any
	for _, op := range []string{"$gt", "$gte"} {
		if x, ok := ops[op]; ok {
			lo = x
		}
	}
	for _, op := range []string{"$lt", "$lte"} {
		if x, ok := ops[op]; ok {
			hi = x
		}
	}
	tag := byte(0)
	for _, b := range []any{lo, hi} {
		var t byte
		switch b.(type) {
		case nil:
			continue
		case json.Number:
			t = tagNumber
		case string:
			t = tagString
		default:
			return planChoice{}
		}
		if tag != 0 && t != tag {
			return planChoice{}
		}
		tag = t
	}
	if tag == 0 {
		return planChoice{}
	}
	// Bounds are included even for $gt and $lt: long strings are cut in
	// the index, and the filter removes the extra documents.
	r := keyRange{lo: []byte{tag}, hi: []byte{tag + 1}}
	if lo != nil {
		r.lo, _ = encodeIndexValue(lo, maxStr)
	}
	if hi != nil {
		enc, _ := encodeIndexValue(hi, maxStr)
		r.hi = append(enc, bytes.Repeat([]byte{0xFF}, 9)...)
	}
	return planChoice{ix: ix, ranges: []keyRange{r}, score: 1}
}
