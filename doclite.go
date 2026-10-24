// Package doclite is an embedded document database. A database is one
// file on disk. Documents are JSON objects in named collections.
//
// Writes are atomic and durable: each write transaction is written to a
// write-ahead log and synced before it returns. Many goroutines can use a
// DB at the same time. One write transaction runs at a time; read
// transactions run in parallel with it and see a fixed snapshot.
//
// A short example:
//
//	db, err := doclite.Open("app.db", nil)
//	if err != nil {
//		return err
//	}
//	defer db.Close()
//
//	users := db.Collection("users")
//	id, err := users.Insert(User{Name: "Ada", Age: 36})
//
//	var u User
//	err = users.FindOne(id, &u)
//
//	var adults []User
//	err = users.FindAll(doclite.M{"Age": doclite.M{"$gte": 18}}, &adults)
package doclite

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/haibeey/doclite/internal/btree"
	"github.com/haibeey/doclite/internal/pager"
)

// M is a short name for a filter or a document map.
type M = map[string]any

// Errors returned by doclite.
var (
	ErrNotFound      = errors.New("doclite: document not found")
	ErrNotObject     = errors.New("doclite: document must be a JSON object")
	ErrInvalidFilter = errors.New("doclite: invalid filter")
	ErrInvalidName   = errors.New("doclite: invalid collection name")
	ErrTxReadOnly    = errors.New("doclite: write in a read-only transaction")

	ErrCorrupt  = pager.ErrCorrupt
	ErrNotDB    = pager.ErrNotDB
	ErrVersion  = pager.ErrVersion
	ErrLocked   = pager.ErrLocked
	ErrReadOnly = pager.ErrReadOnly
	ErrClosed   = pager.ErrClosed
	ErrTxDone   = pager.ErrTxDone
)

// Options control how a database is opened. The zero value is good for
// most uses.
type Options struct {
	// ReadOnly opens the database for reading only. Many processes can
	// open a database read-only at the same time, but not while a
	// process has it open for writing.
	ReadOnly bool
	// NoSync turns off fsync. Commits are faster, but a crash of the OS
	// or a power loss can lose recent commits or corrupt the file. Use
	// it only for tests or bulk loads that you can repeat.
	NoSync bool
	// PageSize is the page size of a new database. The default is 4096.
	PageSize int
	// CacheSize is the number of pages to keep in memory. The default
	// is 2048.
	CacheSize int
	// CheckpointSize is the number of WAL pages after which doclite
	// copies the WAL into the database file. The default is 1000.
	CheckpointSize int
	// LockTimeout is how long Open waits when another process holds the
	// database. Zero means Open returns ErrLocked at once.
	LockTimeout time.Duration
}

// DB is an open database. It is safe for use by many goroutines.
type DB struct {
	p    *pager.Pager
	path string
}

// meta layout in the pager header.
const metaCatalogRoot = 0

// Open opens the database file at path. It creates the file if it does
// not exist. opts can be nil.
func Open(path string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = &Options{}
	}
	p, err := pager.Open(path, pager.Options{
		PageSize:       opts.PageSize,
		ReadOnly:       opts.ReadOnly,
		NoSync:         opts.NoSync,
		CacheSize:      opts.CacheSize,
		CheckpointSize: opts.CheckpointSize,
		LockTimeout:    opts.LockTimeout,
	})
	if err != nil {
		return nil, err
	}
	db := &DB{p: p, path: path}
	if !opts.ReadOnly {
		if err := db.initCatalog(); err != nil {
			p.Close()
			return nil, err
		}
	}
	return db, nil
}

func (db *DB) initCatalog() error {
	w, err := db.p.BeginWrite(context.Background())
	if err != nil {
		return err
	}
	if le.Uint32(w.Meta()[metaCatalogRoot:]) != 0 {
		w.Rollback()
		return nil
	}
	root, err := btree.Create(w)
	if err != nil {
		w.Rollback()
		return err
	}
	meta := make([]byte, pager.MetaSize)
	copy(meta, w.Meta())
	le.PutUint32(meta[metaCatalogRoot:], uint32(root))
	w.SetMeta(meta)
	return w.Commit()
}

// Path returns the path of the database file.
func (db *DB) Path() string { return db.path }

// Close waits for open transactions to end, writes all changes into the
// database file and closes it.
func (db *DB) Close() error { return db.p.Close() }

// Checkpoint copies the write-ahead log into the database file. doclite
// does this by itself; call it only to make the WAL small at once.
func (db *DB) Checkpoint() error { return db.p.Checkpoint() }

// Stats describes a database.
type Stats struct {
	PageSize  int
	PageCount int
	FreePages int
	WALPages  int
	TxID      uint64
}

// Stats returns the size and state of the database.
func (db *DB) Stats() Stats {
	s := db.p.Stats()
	return Stats{s.PageSize, s.PageCount, s.FreePages, s.WALFrames, s.TxID}
}

// Begin starts a transaction. A writable transaction waits for the
// running write transaction to end. Call Commit or Rollback when done.
func (db *DB) Begin(writable bool) (*Tx, error) {
	return db.BeginContext(context.Background(), writable)
}

// BeginContext is like Begin. It stops waiting for the write lock when
// ctx is done.
func (db *DB) BeginContext(ctx context.Context, writable bool) (*Tx, error) {
	if writable {
		w, err := db.p.BeginWrite(ctx)
		if err != nil {
			return nil, err
		}
		return newTx(db, w, w, w.Meta()), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s, err := db.p.BeginRead()
	if err != nil {
		return nil, err
	}
	return newTx(db, s, nil, s.Meta()), nil
}

// Update runs fn in a write transaction. If fn returns nil, the
// transaction commits; otherwise it rolls back and Update returns the
// error of fn.
//
// Inside fn, use tx.Collection, not db.Collection. A write through
// db.Collection waits for the write lock that fn holds, so it never
// returns.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.UpdateContext(context.Background(), fn)
}

// UpdateContext is like Update. It stops waiting for the write lock when
// ctx is done.
func (db *DB) UpdateContext(ctx context.Context, fn func(tx *Tx) error) error {
	tx, err := db.BeginContext(ctx, true)
	if err != nil {
		return err
	}
	return tx.run(fn, true)
}

// View runs fn in a read transaction.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.ViewContext(context.Background(), fn)
}

// ViewContext is like View.
func (db *DB) ViewContext(ctx context.Context, fn func(tx *Tx) error) error {
	tx, err := db.BeginContext(ctx, false)
	if err != nil {
		return err
	}
	return tx.run(fn, false)
}

// Collection returns a handle to a collection. Each call on the handle
// runs in its own transaction. The collection is made by the first
// write to it.
func (db *DB) Collection(name string) *Collection {
	return &Collection{db: db, name: name}
}

// Collections returns the names of all collections.
func (db *DB) Collections() ([]string, error) {
	var names []string
	err := db.View(func(tx *Tx) error {
		var err error
		names, err = tx.Collections()
		return err
	})
	return names, err
}

// Check reads the whole database and verifies its structure. It returns
// an error that wraps ErrCorrupt if it finds a problem.
func (db *DB) Check() error {
	return db.View(func(tx *Tx) error { return tx.check() })
}

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}
