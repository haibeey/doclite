package doclite

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"github.com/haibeey/doclite/internal/btree"
)

// Collection is a handle to a named set of documents. A handle from
// DB.Collection runs each call in its own transaction. A handle from
// Tx.Collection runs in that transaction.
//
// Each document has an int64 id that Insert returns. Ids start at 1 and
// are never used again in the collection, even after a delete.
type Collection struct {
	db   *DB
	tx   *Tx
	name string
}

// Name returns the full name of the collection.
func (c *Collection) Name() string { return c.name }

// Collection returns a sub-collection. Its full name is the name of c, a
// slash, and name.
func (c *Collection) Collection(name string) *Collection {
	return &Collection{db: c.db, tx: c.tx, name: c.name + "/" + name}
}

// write runs fn in a write transaction. If create is false and the
// collection does not exist, fn gets a nil state.
func (c *Collection) write(create bool, fn func(tx *Tx, s *collState) error) error {
	do := func(tx *Tx) error {
		if !tx.Writable() {
			return ErrTxReadOnly
		}
		s, err := tx.coll(c.name, create)
		if err != nil {
			return err
		}
		if !s.exists {
			s = nil
		}
		return fn(tx, s)
	}
	if c.tx != nil {
		return do(c.tx)
	}
	return c.db.Update(do)
}

// read runs fn in a read transaction. s is nil if the collection does
// not exist.
func (c *Collection) read(fn func(tx *Tx, s *collState) error) error {
	do := func(tx *Tx) error {
		s, err := tx.coll(c.name, false)
		if err != nil {
			return err
		}
		if !s.exists {
			s = nil
		}
		return fn(tx, s)
	}
	if c.tx != nil {
		return do(c.tx)
	}
	return c.db.View(do)
}

// Insert adds a document and returns its id. doc must encode to a JSON
// object; it can also be a json.RawMessage or a []byte of JSON text.
func (c *Collection) Insert(doc any) (int64, error) {
	raw, err := encodeDoc(doc)
	if err != nil {
		return 0, err
	}
	var id int64
	err = c.write(true, func(tx *Tx, s *collState) error {
		id, err = s.insert(tx, raw)
		return err
	})
	return id, err
}

// InsertMany adds the documents of a slice in one transaction and
// returns their ids.
func (c *Collection) InsertMany(docs any) ([]int64, error) {
	v := reflect.ValueOf(docs)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, errors.New("doclite: InsertMany needs a slice")
	}
	raws := make([][]byte, v.Len())
	for i := range raws {
		raw, err := encodeDoc(v.Index(i).Interface())
		if err != nil {
			return nil, fmt.Errorf("document %d: %w", i, err)
		}
		raws[i] = raw
	}
	ids := make([]int64, len(raws))
	err := c.write(true, func(tx *Tx, s *collState) error {
		for i, raw := range raws {
			id, err := s.insert(tx, raw)
			if err != nil {
				return err
			}
			ids[i] = id
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *collState) insert(tx *Tx, raw []byte) (int64, error) {
	doc, err := s.decodeForIndex(raw)
	if err != nil {
		return 0, err
	}
	s.meta.Seq++
	id := s.meta.Seq
	if err := tx.fail(s.tree.Put(idKey(id), raw)); err != nil {
		return 0, err
	}
	if err := tx.fail(s.indexAdd(id, doc)); err != nil {
		return 0, err
	}
	s.meta.Count++
	s.dirty = true
	return id, nil
}

// decodeForIndex decodes raw if the collection has indexes.
func (s *collState) decodeForIndex(raw []byte) (any, error) {
	if len(s.indexes) == 0 {
		return nil, nil
	}
	return decodeJSON(raw)
}

// FindOne decodes the document with the given id into out. It returns
// ErrNotFound if there is no such document.
func (c *Collection) FindOne(id int64, out any) error {
	return c.read(func(tx *Tx, s *collState) error {
		if s == nil {
			return ErrNotFound
		}
		raw, ok, err := s.tree.Get(idKey(id))
		if err != nil {
			return err
		}
		if !ok {
			return ErrNotFound
		}
		return json.Unmarshal(raw, out)
	})
}

// UpdateOne replaces the document with the given id. It returns
// ErrNotFound if there is no such document.
func (c *Collection) UpdateOne(id int64, doc any) error {
	raw, err := encodeDoc(doc)
	if err != nil {
		return err
	}
	return c.write(false, func(tx *Tx, s *collState) error {
		if s == nil {
			return ErrNotFound
		}
		return s.replace(tx, id, raw)
	})
}

func (s *collState) replace(tx *Tx, id int64, raw []byte) error {
	old, ok, err := s.tree.Get(idKey(id))
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}
	if len(s.indexes) > 0 {
		oldDoc, err := decodeJSON(old)
		if err != nil {
			return corruptf("document %d is not valid JSON", id)
		}
		newDoc, err := decodeJSON(raw)
		if err != nil {
			return err
		}
		if err := tx.fail(s.indexRemove(id, oldDoc)); err != nil {
			return err
		}
		if err := tx.fail(s.indexAdd(id, newDoc)); err != nil {
			return err
		}
	}
	s.dirty = true
	return tx.fail(s.tree.Put(idKey(id), raw))
}

// DeleteOne removes the document with the given id. It returns
// ErrNotFound if there is no such document.
func (c *Collection) DeleteOne(id int64) error {
	return c.write(false, func(tx *Tx, s *collState) error {
		if s == nil {
			return ErrNotFound
		}
		return s.delete(tx, id)
	})
}

func (s *collState) delete(tx *Tx, id int64) error {
	old, ok, err := s.tree.Get(idKey(id))
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}
	if len(s.indexes) > 0 {
		doc, err := decodeJSON(old)
		if err != nil {
			return corruptf("document %d is not valid JSON", id)
		}
		if err := tx.fail(s.indexRemove(id, doc)); err != nil {
			return err
		}
	}
	if _, err := s.tree.Delete(idKey(id)); err != nil {
		return tx.fail(err)
	}
	s.meta.Count--
	s.dirty = true
	return nil
}

// DeleteMany removes all documents that match filter and returns how
// many it removed.
func (c *Collection) DeleteMany(filter any) (int64, error) {
	m, f, err := compileFilter(filter)
	if err != nil {
		return 0, err
	}
	var n int64
	err = c.write(false, func(tx *Tx, s *collState) error {
		if s == nil {
			return nil
		}
		var ids []int64
		err := s.scan(m, f, func(id int64, _ []byte) error {
			ids = append(ids, id)
			return nil
		})
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := s.delete(tx, id); err != nil {
				return err
			}
		}
		n = int64(len(ids))
		return nil
	})
	return n, err
}

// Count returns the number of documents that match filter. A nil filter
// counts all documents at once, without a scan.
func (c *Collection) Count(filter any) (int64, error) {
	m, f, err := compileFilter(filter)
	if err != nil {
		return 0, err
	}
	var n int64
	err = c.read(func(tx *Tx, s *collState) error {
		if s == nil {
			return nil
		}
		if _, all := m.(matchAll); all {
			n = s.meta.Count
			return nil
		}
		return s.scan(m, f, func(int64, []byte) error {
			n++
			return nil
		})
	})
	return n, err
}

// FindAll decodes all documents that match filter into out, which must
// be a pointer to a slice.
func (c *Collection) FindAll(filter any, out any) error {
	m, f, err := compileFilter(filter)
	if err != nil {
		return err
	}
	pv := reflect.ValueOf(out)
	if pv.Kind() != reflect.Pointer || pv.Elem().Kind() != reflect.Slice {
		return errors.New("doclite: FindAll needs a pointer to a slice")
	}
	sv := pv.Elem()
	et := sv.Type().Elem()
	return c.read(func(tx *Tx, s *collState) error {
		res := reflect.MakeSlice(sv.Type(), 0, 0)
		if s != nil {
			err := s.scan(m, f, func(_ int64, raw []byte) error {
				e := reflect.New(et)
				if err := json.Unmarshal(raw, e.Interface()); err != nil {
					return err
				}
				res = reflect.Append(res, e.Elem())
				return nil
			})
			if err != nil {
				return err
			}
		}
		sv.Set(res)
		return nil
	})
}

// FindFirst decodes the matching document with the lowest id into out
// and returns its id. It returns ErrNotFound if none match.
func (c *Collection) FindFirst(filter any, out any) (int64, error) {
	m, f, err := compileFilter(filter)
	if err != nil {
		return 0, err
	}
	var id int64
	err = c.read(func(tx *Tx, s *collState) error {
		if s == nil {
			return ErrNotFound
		}
		err := s.scan(m, f, func(i int64, raw []byte) error {
			id = i
			if err := json.Unmarshal(raw, out); err != nil {
				return err
			}
			return errStop
		})
		if err == nil {
			return ErrNotFound
		}
		if err == errStop {
			return nil
		}
		return err
	})
	return id, err
}

// Find returns a cursor over the documents that match filter, in id
// order. The cursor must be closed, or read to the end, so that its read
// transaction ends.
func (c *Collection) Find(filter any) (*Cursor, error) {
	m, f, err := compileFilter(filter)
	if err != nil {
		return nil, err
	}
	tx, own := c.tx, false
	if tx == nil {
		if tx, err = c.db.Begin(false); err != nil {
			return nil, err
		}
		own = true
	}
	s, err := tx.coll(c.name, false)
	if err != nil {
		if own {
			tx.Rollback()
		}
		return nil, err
	}
	cur := &Cursor{tx: tx, own: own, m: m}
	if s.exists {
		cur.it, err = s.iter(f)
		if err != nil {
			if own {
				tx.Rollback()
			}
			return nil, err
		}
	}
	if own {
		runtime.SetFinalizer(cur, (*Cursor).Close)
	}
	return cur, nil
}

// CreateIndex adds an index on a field path, such as "age" or
// "address.city". Queries on the field with equality, $in or a range
// then read the index instead of every document. It does nothing if the
// index exists.
func (c *Collection) CreateIndex(field string) error {
	if field == "" || strings.HasPrefix(field, "$") {
		return fmt.Errorf("doclite: bad index field %q", field)
	}
	return c.write(true, func(tx *Tx, s *collState) error {
		if s.findIndex(field) >= 0 {
			return nil
		}
		root, err := btree.Create(tx.w)
		if err != nil {
			return tx.fail(err)
		}
		ix := &index{field: field, path: splitPath(field), tree: btree.Open(tx.w, root)}
		cur := s.tree.Cursor()
		for ok := cur.First(); ok; ok = cur.Next() {
			raw, err := cur.Value()
			if err != nil {
				return tx.fail(err)
			}
			doc, err := decodeJSON(raw)
			if err != nil {
				return tx.fail(corruptf("document %d is not valid JSON", keyID(cur.Key())))
			}
			for _, k := range ix.keys(doc, keyID(cur.Key())) {
				if err := ix.tree.Put(k, nil); err != nil {
					return tx.fail(err)
				}
			}
		}
		if err := cur.Err(); err != nil {
			return tx.fail(err)
		}
		s.indexes = append(s.indexes, ix)
		s.meta.Indexes = append(s.meta.Indexes, indexMeta{Field: field, Root: root})
		s.dirty = true
		return nil
	})
}

// DropIndex removes the index on field.
func (c *Collection) DropIndex(field string) error {
	return c.write(false, func(tx *Tx, s *collState) error {
		i := -1
		if s != nil {
			i = s.findIndex(field)
		}
		if i < 0 {
			return fmt.Errorf("%w: no index on %q", ErrNotFound, field)
		}
		if err := tx.fail(s.indexes[i].tree.Drop()); err != nil {
			return err
		}
		s.indexes = append(s.indexes[:i], s.indexes[i+1:]...)
		s.meta.Indexes = append(s.meta.Indexes[:i], s.meta.Indexes[i+1:]...)
		s.dirty = true
		return nil
	})
}

// Indexes returns the fields that have an index.
func (c *Collection) Indexes() ([]string, error) {
	var out []string
	err := c.read(func(tx *Tx, s *collState) error {
		if s != nil {
			for _, ix := range s.indexes {
				out = append(out, ix.field)
			}
		}
		return nil
	})
	return out, err
}

// Drop removes the collection, its indexes and its sub-collections.
func (c *Collection) Drop() error {
	return c.write(false, func(tx *Tx, s *collState) error {
		if s == nil {
			return nil
		}
		return tx.drop(c.name)
	})
}

// scan calls fn for each document that matches, in id order.
func (s *collState) scan(m matcher, f map[string]any, fn func(id int64, raw []byte) error) error {
	it, err := s.iter(f)
	if err != nil {
		return err
	}
	for {
		id, raw, ok, err := it.next()
		if err != nil || !ok {
			return err
		}
		if match, err := matchRaw(m, raw); err != nil {
			return err
		} else if match {
			if err := fn(id, raw); err != nil {
				return err
			}
		}
	}
}

func matchRaw(m matcher, raw []byte) (bool, error) {
	if _, all := m.(matchAll); all {
		return true, nil
	}
	if t, ok := m.(*topMatcher); ok {
		match, err := t.matchRaw(raw)
		if err != nil {
			return false, corruptf("document is not valid JSON")
		}
		return match, nil
	}
	doc, err := decodeJSON(raw)
	if err != nil {
		return false, corruptf("document is not valid JSON")
	}
	return m.match(doc), nil
}

// docIter returns documents one at a time.
type docIter interface {
	next() (id int64, raw []byte, ok bool, err error)
}

// iter returns an iterator over the documents that can match f. It uses
// an index if one fits.
func (s *collState) iter(f map[string]any) (docIter, error) {
	ids, ok, err := s.plan(f)
	if err != nil {
		return nil, err
	}
	if ok {
		return &idIter{tree: s.tree, ids: ids}, nil
	}
	return &scanIter{c: s.tree.Cursor()}, nil
}

type scanIter struct {
	c       *btree.Cursor
	started bool
}

func (it *scanIter) next() (int64, []byte, bool, error) {
	var ok bool
	if !it.started {
		it.started = true
		ok = it.c.First()
	} else {
		ok = it.c.Next()
	}
	if !ok {
		return 0, nil, false, it.c.Err()
	}
	raw, err := it.c.Value()
	if err != nil {
		return 0, nil, false, err
	}
	return keyID(it.c.Key()), raw, true, nil
}

type idIter struct {
	tree *btree.Tree
	ids  []int64
}

func (it *idIter) next() (int64, []byte, bool, error) {
	for len(it.ids) > 0 {
		id := it.ids[0]
		it.ids = it.ids[1:]
		raw, ok, err := it.tree.Get(idKey(id))
		if err != nil {
			return 0, nil, false, err
		}
		if ok {
			return id, raw, true, nil
		}
	}
	return 0, nil, false, nil
}

// Cursor moves over the result of Find.
//
//	cur, err := users.Find(doclite.M{"age": doclite.M{"$gt": 30}})
//	if err != nil { ... }
//	defer cur.Close()
//	for cur.Next() {
//		var u User
//		if err := cur.Decode(&u); err != nil { ... }
//	}
//	if err := cur.Err(); err != nil { ... }
type Cursor struct {
	tx   *Tx
	own  bool
	m    matcher
	it   docIter
	id   int64
	raw  []byte
	err  error
	done bool
}

// Next moves to the next matching document. It returns false at the end
// or on an error; then the cursor is closed.
func (c *Cursor) Next() bool {
	if c.done {
		return false
	}
	if c.it == nil {
		c.Close()
		return false
	}
	if c.tx.done {
		c.err = ErrTxDone
		c.Close()
		return false
	}
	for {
		id, raw, ok, err := c.it.next()
		if err == nil && ok {
			var match bool
			if match, err = matchRaw(c.m, raw); err == nil && !match {
				continue
			}
		}
		if err != nil || !ok {
			c.err = err
			c.Close()
			return false
		}
		c.id, c.raw = id, raw
		return true
	}
}

// ID returns the id of the current document.
func (c *Cursor) ID() int64 { return c.id }

// Decode decodes the current document into out.
func (c *Cursor) Decode(out any) error {
	if c.raw == nil {
		return errors.New("doclite: cursor is not on a document")
	}
	return json.Unmarshal(c.raw, out)
}

// Raw returns the JSON text of the current document.
func (c *Cursor) Raw() json.RawMessage { return c.raw }

// Err returns the error that stopped the cursor, if any.
func (c *Cursor) Err() error { return c.err }

// Close ends the cursor. It is safe to call more than once.
func (c *Cursor) Close() error {
	if c.done {
		return nil
	}
	c.done = true
	c.raw = nil
	if c.own {
		runtime.SetFinalizer(c, nil)
		c.tx.Rollback()
	}
	return nil
}
