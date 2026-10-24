package doclite

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/haibeey/doclite/internal/btree"
	"github.com/haibeey/doclite/internal/pager"
)

var le = binary.LittleEndian

// Tx is a transaction. A read transaction sees the database as it was
// when it began. A write transaction sees its own changes.
//
// A Tx must not be used by many goroutines at the same time. Call Commit
// or Rollback when done; until then a read transaction stops doclite
// from copying the WAL into the database file.
type Tx struct {
	db      *DB
	r       pageReader
	w       *pager.WriteTxn
	catalog *btree.Tree
	colls   map[string]*collState
	err     error // a failed write; the transaction can only roll back
	done    bool
}

type pageReader interface {
	btree.Reader
	PageCount() int
	FreeList() ([]pager.PgNo, error)
}

// collMeta is the catalog record of a collection.
type collMeta struct {
	Root    pager.PgNo  `json:"root"`
	Seq     int64       `json:"seq"`
	Count   int64       `json:"count"`
	Indexes []indexMeta `json:"indexes,omitempty"`
}

type indexMeta struct {
	Field string     `json:"field"`
	Root  pager.PgNo `json:"root"`
}

// collState is the state of a collection inside one transaction.
type collState struct {
	name    string
	meta    collMeta
	tree    *btree.Tree
	indexes []*index
	exists  bool
	dirty   bool // the catalog record must be written at commit
}

func newTx(db *DB, r pageReader, w *pager.WriteTxn, meta []byte) *Tx {
	tx := &Tx{db: db, r: r, w: w, colls: make(map[string]*collState)}
	if root := le.Uint32(meta[metaCatalogRoot:]); root != 0 {
		tx.catalog = btree.Open(r, pager.PgNo(root))
	}
	return tx
}

// Writable reports whether tx is a write transaction.
func (tx *Tx) Writable() bool { return tx.w != nil }

// Collection returns a handle to a collection in this transaction.
func (tx *Tx) Collection(name string) *Collection {
	return &Collection{db: tx.db, tx: tx, name: name}
}

// Collections returns the names of all collections, in order.
func (tx *Tx) Collections() ([]string, error) {
	if tx.done {
		return nil, ErrTxDone
	}
	set := map[string]bool{}
	if tx.catalog != nil {
		c := tx.catalog.Cursor()
		for ok := c.First(); ok; ok = c.Next() {
			set[string(c.Key())] = true
		}
		if err := c.Err(); err != nil {
			return nil, err
		}
	}
	for name, s := range tx.colls {
		set[name] = s.exists
	}
	var names []string
	for name, ok := range set {
		if ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// Commit saves the changes of a write transaction. For a read
// transaction it is the same as Rollback.
func (tx *Tx) Commit() error {
	if tx.done {
		return ErrTxDone
	}
	if tx.w == nil {
		return tx.Rollback()
	}
	if tx.err != nil {
		tx.Rollback()
		return fmt.Errorf("doclite: transaction rolled back after an earlier error: %w", tx.err)
	}
	if err := tx.flush(); err != nil {
		tx.Rollback()
		return err
	}
	tx.done = true
	return tx.w.Commit()
}

// Rollback ends the transaction and discards its changes.
func (tx *Tx) Rollback() error {
	if tx.done {
		return ErrTxDone
	}
	tx.done = true
	if tx.w != nil {
		tx.w.Rollback()
	} else {
		tx.r.(*pager.Snapshot).Close()
	}
	return nil
}

func (tx *Tx) run(fn func(*Tx) error, write bool) (err error) {
	defer func() {
		if p := recover(); p != nil {
			if !tx.done {
				tx.Rollback()
			}
			panic(p)
		}
	}()
	if err := fn(tx); err != nil {
		if !tx.done {
			tx.Rollback()
		}
		return err
	}
	if tx.done {
		return ErrTxDone
	}
	if write {
		return tx.Commit()
	}
	return tx.Rollback()
}

// fail marks the transaction as failed if err is not nil. A failed write
// can leave the transaction half done, so it can only roll back.
func (tx *Tx) fail(err error) error {
	if err != nil && tx.err == nil {
		tx.err = err
	}
	return err
}

// flush writes the catalog records of changed collections.
func (tx *Tx) flush() error {
	oldRoot := tx.catalog.Root()
	for name, s := range tx.colls {
		if !s.dirty {
			continue
		}
		if !s.exists {
			if _, err := tx.catalog.Delete([]byte(name)); err != nil {
				return err
			}
			continue
		}
		s.meta.Root = s.tree.Root()
		for i, ix := range s.indexes {
			s.meta.Indexes[i].Root = ix.tree.Root()
		}
		b, err := json.Marshal(&s.meta)
		if err != nil {
			return err
		}
		if err := tx.catalog.Put([]byte(name), b); err != nil {
			return err
		}
	}
	if root := tx.catalog.Root(); root != oldRoot {
		meta := make([]byte, pager.MetaSize)
		copy(meta, tx.w.Meta())
		le.PutUint32(meta[metaCatalogRoot:], uint32(root))
		tx.w.SetMeta(meta)
	}
	return nil
}

const maxNameLen = 200

func validName(name string) bool {
	if name == "" || len(name) > maxNameLen || !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
		return false
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" {
			return false
		}
	}
	return true
}

// coll returns the state of collection name. If create is true, a
// missing collection is made.
func (tx *Tx) coll(name string, create bool) (*collState, error) {
	if tx.done {
		return nil, ErrTxDone
	}
	if create && tx.w == nil {
		return nil, ErrTxReadOnly
	}
	s, ok := tx.colls[name]
	if !ok {
		if !validName(name) {
			return nil, fmt.Errorf("%w: %q", ErrInvalidName, name)
		}
		s = &collState{name: name}
		if tx.catalog != nil {
			b, found, err := tx.catalog.Get([]byte(name))
			if err != nil {
				return nil, err
			}
			if found {
				if err := json.Unmarshal(b, &s.meta); err != nil {
					return nil, corruptf("bad catalog record for %q", name)
				}
				s.tree = btree.Open(tx.r, s.meta.Root)
				for _, im := range s.meta.Indexes {
					s.indexes = append(s.indexes, &index{field: im.Field, path: splitPath(im.Field), tree: btree.Open(tx.r, im.Root)})
				}
				s.exists = true
			}
		}
		tx.colls[name] = s
	}
	if !s.exists && create {
		root, err := btree.Create(tx.w)
		if err != nil {
			return nil, tx.fail(err)
		}
		s.meta = collMeta{Root: root}
		s.tree = btree.Open(tx.w, root)
		s.indexes = nil
		s.exists = true
		s.dirty = true
	}
	return s, nil
}

// drop removes collection name and its sub-collections.
func (tx *Tx) drop(name string) error {
	names, err := tx.Collections()
	if err != nil {
		return err
	}
	for _, n := range names {
		if n != name && !strings.HasPrefix(n, name+"/") {
			continue
		}
		s, err := tx.coll(n, false)
		if err != nil {
			return err
		}
		if err := tx.fail(s.tree.Drop()); err != nil {
			return err
		}
		for _, ix := range s.indexes {
			if err := tx.fail(ix.tree.Drop()); err != nil {
				return err
			}
		}
		*s = collState{name: n, dirty: true}
	}
	return nil
}

// check verifies every tree and that each page is used once.
func (tx *Tx) check() error {
	if tx.catalog == nil {
		return nil
	}
	seen := make(map[pager.PgNo]bool)
	visit := func(pg pager.PgNo) error {
		if pg == 0 || int(pg) >= tx.r.PageCount() {
			return corruptf("page %d out of range", pg)
		}
		if seen[pg] {
			return corruptf("page %d is used twice", pg)
		}
		seen[pg] = true
		return nil
	}
	if _, err := tx.catalog.Check(visit); err != nil {
		return err
	}
	names, err := tx.Collections()
	if err != nil {
		return err
	}
	for _, name := range names {
		s, err := tx.coll(name, false)
		if err != nil {
			return err
		}
		n, err := s.tree.Check(visit)
		if err != nil {
			return fmt.Errorf("collection %q: %w", name, err)
		}
		if int64(n) != s.meta.Count {
			return corruptf("collection %q has %d documents, catalog says %d", name, n, s.meta.Count)
		}
		if err := s.checkIndexes(visit); err != nil {
			return fmt.Errorf("collection %q: %w", name, err)
		}
	}
	free, err := tx.r.FreeList()
	if err != nil {
		return err
	}
	for _, pg := range free {
		if err := visit(pg); err != nil {
			return err
		}
	}
	if len(seen) != tx.r.PageCount()-1 {
		return corruptf("%d pages in use, file has %d", len(seen), tx.r.PageCount()-1)
	}
	return nil
}

func idKey(id int64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], uint64(id))
	return k[:]
}

func keyID(k []byte) int64 { return int64(binary.BigEndian.Uint64(k)) }

// encodeDoc returns doc as compact JSON. doc can be a value that
// encoding/json can encode, a json.RawMessage, or a []byte that holds
// JSON text.
func encodeDoc(doc any) ([]byte, error) {
	var raw []byte
	switch v := doc.(type) {
	case json.RawMessage:
		raw = v
	case []byte:
		raw = v
	default:
		b, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		raw = b
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return nil, err
	}
	if buf.Len() == 0 || buf.Bytes()[0] != '{' {
		return nil, ErrNotObject
	}
	return buf.Bytes(), nil
}

var errStop = errors.New("stop")
