package pager

import (
	"context"
	"crypto/rand"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

// Options control how a database file is opened.
type Options struct {
	// PageSize is the page size of a new file. It must be a power of two
	// from MinPageSize to MaxPageSize. It is ignored for an existing file.
	PageSize int
	// ReadOnly opens the file for reading only. Many read-only processes
	// can open the same file at the same time.
	ReadOnly bool
	// NoSync skips fsync. A commit is then not durable if the OS or the
	// machine stops. Use it only for tests and bulk loads.
	NoSync bool
	// CacheSize is the number of pages to keep in memory.
	CacheSize int
	// CheckpointSize is the number of WAL frames after which a commit
	// tries to copy the WAL into the main file.
	CheckpointSize int
	// LockTimeout is how long Open waits for the file lock. Zero means
	// do not wait.
	LockTimeout time.Duration
}

func (o *Options) setDefaults() {
	if o.PageSize == 0 {
		o.PageSize = DefaultPageSize
	}
	if o.CacheSize == 0 {
		o.CacheSize = 2048
	}
	if o.CheckpointSize == 0 {
		o.CheckpointSize = 1000
	}
}

type walEntry struct {
	frame uint64 // global frame number
	off   int64  // offset of the page data in the WAL file
}

// Pager is an open database file. It is safe for use by many goroutines.
type Pager struct {
	path     string
	file     *os.File
	wal      *os.File
	opts     Options
	pageSize int

	// writer is a semaphore: it holds one token while a write
	// transaction (or Close) runs.
	writer chan struct{}
	// ckpt is held in shared mode by every open transaction. A
	// checkpoint changes the main file, so it needs it in exclusive mode.
	ckpt sync.RWMutex

	// mu protects the fields below. Fields marked (w) are only used
	// while the writer token is held.
	mu        sync.RWMutex
	hdr       header
	index     map[PgNo][]walEntry
	frame     uint64 // number of the last committed frame
	closed    bool
	err       error  // a write error that makes the pager unsafe to write
	walOff    int64  // (w) end of the committed frames
	walFrames int    // (w) committed frames in the WAL
	walSalt   uint32 // (w)
	walCksum  uint32 // (w) checksum of the last committed frame

	cache *cache
}

// Open opens or creates the database file at path.
func Open(path string, opts Options) (_ *Pager, err error) {
	opts.setDefaults()
	if !validPageSize(opts.PageSize) {
		return nil, errors.New("doclite: page size must be a power of two from 1024 to 65536")
	}
	flag := os.O_RDWR | os.O_CREATE
	if opts.ReadOnly {
		flag = os.O_RDONLY
	}
	f, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		return nil, err
	}
	p := &Pager{
		path:   path,
		file:   f,
		opts:   opts,
		writer: make(chan struct{}, 1),
		index:  make(map[PgNo][]walEntry),
		cache:  newCache(opts.CacheSize),
	}
	if err = lockFile(f, !opts.ReadOnly, opts.LockTimeout); err != nil {
		f.Close()
		return nil, err
	}
	defer func() {
		if err != nil {
			p.closeFiles()
		}
	}()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() == 0 {
		if opts.ReadOnly {
			return nil, ErrNotDB
		}
		if err = p.create(); err != nil {
			return nil, err
		}
	}
	var b [16]byte
	if _, err = f.ReadAt(b[:], 0); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrNotDB
		}
		return nil, err
	}
	if _, err = decodeHeaderPrefix(b[:]); err != nil {
		return nil, err
	}
	p.pageSize = int(le.Uint32(b[12:]))
	if !validPageSize(p.pageSize) {
		return nil, corruptf("bad page size %d", p.pageSize)
	}
	if err = p.openWAL(); err != nil {
		return nil, err
	}
	if err = p.recover(); err != nil {
		return nil, err
	}
	if !opts.ReadOnly {
		if err = p.checkpoint(); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func decodeHeaderPrefix(b []byte) (int, error) {
	if [8]byte(b[0:8]) != magic {
		return 0, ErrNotDB
	}
	if v := le.Uint32(b[8:]); v != formatVersion {
		return 0, ErrVersion
	}
	return int(le.Uint32(b[12:])), nil
}

// create writes the header of a new, empty database.
func (p *Pager) create() error {
	if err := os.Remove(p.walPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	b := make([]byte, p.opts.PageSize)
	h := header{pageSize: uint32(p.opts.PageSize), pageCount: 1}
	h.encode(b)
	setChecksum(b)
	if _, err := p.file.WriteAt(b, 0); err != nil {
		return err
	}
	if err := fsync(p.file); err != nil {
		return err
	}
	return syncDir(p.path)
}

func (p *Pager) walPath() string { return p.path + "-wal" }

func (p *Pager) openWAL() error {
	flag := os.O_RDWR | os.O_CREATE
	if p.opts.ReadOnly {
		flag = os.O_RDONLY
	}
	w, err := os.OpenFile(p.walPath(), flag, 0o644)
	if err != nil {
		if p.opts.ReadOnly && errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	p.wal = w
	var b [walHeaderSize]byte
	if _, err := w.ReadAt(b[:], 0); err == nil &&
		[8]byte(b[0:8]) == walMagic &&
		int(le.Uint32(b[8:])) == p.pageSize &&
		le.Uint32(b[28:]) == crc32.Checksum(b[:28], crcT) {
		p.walSalt = le.Uint32(b[12:])
		p.walCksum = le.Uint32(b[28:])
		p.walOff = walHeaderSize
		return nil
	}
	// No valid header: the WAL holds no committed frames.
	p.walOff = 0
	if p.opts.ReadOnly {
		return nil
	}
	return p.resetWAL()
}

// resetWAL starts a new, empty WAL with a new salt. Frames from the old
// WAL do not match the new salt, so they can never be replayed.
func (p *Pager) resetWAL() error {
	var b [walHeaderSize]byte
	copy(b[0:8], walMagic[:])
	le.PutUint32(b[8:], uint32(p.pageSize))
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		return err
	}
	copy(b[12:16], salt[:])
	le.PutUint32(b[28:], crc32.Checksum(b[:28], crcT))
	if _, err := p.wal.WriteAt(b[:], 0); err != nil {
		return err
	}
	if err := p.wal.Truncate(walHeaderSize); err != nil {
		return err
	}
	if !p.opts.NoSync {
		if err := fsync(p.wal); err != nil {
			return err
		}
	}
	p.walSalt = le.Uint32(b[12:])
	p.walCksum = le.Uint32(b[28:])
	p.walOff = walHeaderSize
	p.walFrames = 0
	return nil
}

// recover reads the committed frames of the WAL into the index and loads
// the header.
func (p *Pager) recover() error {
	if p.wal != nil && p.walOff == walHeaderSize {
		fs := int64(frameHeaderSize + p.pageSize)
		buf := make([]byte, fs)
		off, prev := p.walOff, p.walCksum
		type pending struct {
			pg  PgNo
			off int64
		}
		var pend []pending
		for {
			if n, _ := p.wal.ReadAt(buf, off); int64(n) < fs {
				break
			}
			if le.Uint32(buf[8:]) != p.walSalt {
				break
			}
			ck := crc32.Update(prev, crcT, buf[:12])
			ck = crc32.Update(ck, crcT, buf[frameHeaderSize:])
			if ck != le.Uint32(buf[12:]) {
				break
			}
			pend = append(pend, pending{PgNo(le.Uint32(buf[0:])), off + frameHeaderSize})
			off += fs
			prev = ck
			if le.Uint32(buf[4:])&frameCommit != 0 {
				for _, e := range pend {
					p.frame++
					p.index[e.pg] = append(p.index[e.pg], walEntry{p.frame, e.off})
				}
				p.walFrames += len(pend)
				pend = pend[:0]
				p.walOff, p.walCksum = off, prev
			}
		}
		if !p.opts.ReadOnly {
			// Remove frames of a transaction that did not commit.
			if err := p.wal.Truncate(p.walOff); err != nil {
				return err
			}
		}
	}
	b, err := p.readPage(0, p.frame)
	if err != nil {
		return err
	}
	h, err := decodeHeader(b)
	if err != nil {
		return err
	}
	if int(h.pageSize) != p.pageSize {
		return corruptf("page size changed")
	}
	p.hdr = h
	return nil
}

// checkpoint copies the newest version of each page in the WAL to the
// main file, syncs it, and resets the WAL. The caller must make sure that
// no transaction is open.
func (p *Pager) checkpoint() error {
	if p.err != nil {
		return p.err
	}
	if len(p.index) > 0 {
		buf := make([]byte, p.pageSize)
		for pg, entries := range p.index {
			if _, err := p.wal.ReadAt(buf, entries[len(entries)-1].off); err != nil {
				return err
			}
			if _, err := p.file.WriteAt(buf, int64(pg)*int64(p.pageSize)); err != nil {
				return err
			}
		}
		if err := p.file.Truncate(int64(p.hdr.pageCount) * int64(p.pageSize)); err != nil {
			return err
		}
		if !p.opts.NoSync {
			if err := fsync(p.file); err != nil {
				return err
			}
		}
	}
	// The main file now holds every committed page, so the frames are
	// not needed. If the reset fails, later commits could be lost, so
	// stop all writes.
	if err := p.resetWAL(); err != nil {
		p.err = err
		return err
	}
	p.mu.Lock()
	clear(p.index)
	p.mu.Unlock()
	p.cache.clear()
	return nil
}

// Checkpoint copies the WAL into the main file. It waits for open
// transactions to end.
func (p *Pager) Checkpoint() error {
	if p.opts.ReadOnly {
		return ErrReadOnly
	}
	p.writer <- struct{}{}
	defer func() { <-p.writer }()
	p.ckpt.Lock()
	defer p.ckpt.Unlock()
	if p.isClosed() {
		return ErrClosed
	}
	return p.checkpoint()
}

func (p *Pager) isClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

// readPage returns the version of page pg that is visible at frame
// snap. The returned buffer is shared and must not be changed.
func (p *Pager) readPage(pg PgNo, snap uint64) ([]byte, error) {
	key := cacheKey{pg: pg}
	off := int64(-1)
	p.mu.RLock()
	entries := p.index[pg]
	if i := sort.Search(len(entries), func(i int) bool { return entries[i].frame > snap }) - 1; i >= 0 {
		key.frame, off = entries[i].frame, entries[i].off
	}
	p.mu.RUnlock()

	if b := p.cache.get(key); b != nil {
		return b, nil
	}
	b := make([]byte, p.pageSize)
	var err error
	if off >= 0 {
		_, err = p.wal.ReadAt(b, off)
	} else {
		_, err = p.file.ReadAt(b, int64(pg)*int64(p.pageSize))
	}
	if errors.Is(err, io.EOF) {
		return nil, corruptf("page %d is past the end of the file", pg)
	}
	if err != nil {
		return nil, err
	}
	if !checksumOK(b) {
		return nil, corruptf("checksum mismatch on page %d", pg)
	}
	p.cache.put(key, b)
	return b, nil
}

// PageSize returns the page size of the file.
func (p *Pager) PageSize() int { return p.pageSize }

// ReadOnly reports whether the file was opened read-only.
func (p *Pager) ReadOnly() bool { return p.opts.ReadOnly }

// Stats describes the state of a pager.
type Stats struct {
	PageSize  int
	PageCount int
	FreePages int
	WALFrames int
	TxID      uint64
}

// Stats returns the current state of the pager.
func (p *Pager) Stats() Stats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := 0
	for _, e := range p.index {
		n += len(e)
	}
	return Stats{
		PageSize:  p.pageSize,
		PageCount: int(p.hdr.pageCount),
		FreePages: int(p.hdr.freeCount),
		WALFrames: n,
		TxID:      p.hdr.txid,
	}
}

// Close waits for open transactions to end, copies the WAL into the main
// file and closes the files.
func (p *Pager) Close() error {
	p.writer <- struct{}{}
	defer func() { <-p.writer }()
	p.ckpt.Lock()
	defer p.ckpt.Unlock()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	var err error
	if !p.opts.ReadOnly {
		err = p.checkpoint()
	}
	removeWAL := err == nil && !p.opts.ReadOnly
	if cerr := p.closeFiles(); err == nil {
		err = cerr
	}
	if removeWAL {
		os.Remove(p.walPath())
	}
	return err
}

func (p *Pager) closeFiles() error {
	var err error
	if p.wal != nil {
		err = p.wal.Close()
	}
	unlockFile(p.file)
	if cerr := p.file.Close(); err == nil {
		err = cerr
	}
	return err
}

// Snapshot is a read transaction. It sees the database as it was when the
// snapshot began. It is safe for use by many goroutines, but Close must
// be called once.
type Snapshot struct {
	p     *Pager
	frame uint64
	hdr   header
	once  sync.Once
}

// BeginRead starts a read transaction.
func (p *Pager) BeginRead() (*Snapshot, error) {
	p.ckpt.RLock()
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		p.ckpt.RUnlock()
		return nil, ErrClosed
	}
	return &Snapshot{p: p, frame: p.frame, hdr: p.hdr}, nil
}

// Page returns page pg. The buffer must not be changed.
func (s *Snapshot) Page(pg PgNo) ([]byte, error) {
	if uint32(pg) >= s.hdr.pageCount {
		return nil, corruptf("page %d out of range", pg)
	}
	return s.p.readPage(pg, s.frame)
}

// PageSize returns the page size.
func (s *Snapshot) PageSize() int { return s.p.pageSize }

// Meta returns the user meta bytes of page 0.
func (s *Snapshot) Meta() []byte { return s.hdr.meta[:] }

// PageCount returns the number of pages in the file.
func (s *Snapshot) PageCount() int { return int(s.hdr.pageCount) }

// FreeList returns the pages on the free list.
func (s *Snapshot) FreeList() ([]PgNo, error) {
	return freeList(s.Page, s.hdr)
}

// Close ends the read transaction.
func (s *Snapshot) Close() {
	s.once.Do(s.p.ckpt.RUnlock)
}

func freeList(page func(PgNo) ([]byte, error), h header) ([]PgNo, error) {
	var out []PgNo
	for pg := h.freeHead; pg != 0; {
		if uint32(len(out)) >= h.freeCount {
			return nil, corruptf("free list is longer than %d pages", h.freeCount)
		}
		b, err := page(pg)
		if err != nil {
			return nil, err
		}
		if b[0] != TypeFree {
			return nil, corruptf("page %d on the free list is not free", pg)
		}
		out = append(out, pg)
		pg = PgNo(le.Uint32(b[4:]))
	}
	if uint32(len(out)) != h.freeCount {
		return nil, corruptf("free list has %d pages, header says %d", len(out), h.freeCount)
	}
	return out, nil
}

// WriteTxn is a write transaction. Only one runs at a time. It is not
// safe for use by many goroutines.
type WriteTxn struct {
	p     *Pager
	snap  *Snapshot
	hdr   header
	dirty map[PgNo][]byte
	done  bool
}

// BeginWrite starts a write transaction. It waits for the running write
// transaction to end, or for ctx to be done.
func (p *Pager) BeginWrite(ctx context.Context) (*WriteTxn, error) {
	if p.opts.ReadOnly {
		return nil, ErrReadOnly
	}
	select {
	case p.writer <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.RLock()
	err := p.err
	p.mu.RUnlock()
	if err != nil {
		<-p.writer
		return nil, err
	}
	snap, err := p.BeginRead()
	if err != nil {
		<-p.writer
		return nil, err
	}
	return &WriteTxn{p: p, snap: snap, hdr: snap.hdr, dirty: make(map[PgNo][]byte)}, nil
}

// PageSize returns the page size.
func (t *WriteTxn) PageSize() int { return t.p.pageSize }

// PageCount returns the number of pages in the file.
func (t *WriteTxn) PageCount() int { return int(t.hdr.pageCount) }

// Page returns page pg. The buffer must not be changed.
func (t *WriteTxn) Page(pg PgNo) ([]byte, error) {
	if t.done {
		return nil, ErrTxDone
	}
	if b, ok := t.dirty[pg]; ok {
		return b, nil
	}
	if uint32(pg) >= t.hdr.pageCount {
		return nil, corruptf("page %d out of range", pg)
	}
	return t.p.readPage(pg, t.snap.frame)
}

// PageForWrite returns page pg in a buffer that the caller can change.
// The change is saved when the transaction commits.
func (t *WriteTxn) PageForWrite(pg PgNo) ([]byte, error) {
	if pg == 0 {
		return nil, errors.New("doclite: page 0 is owned by the pager")
	}
	if b, ok := t.dirty[pg]; ok {
		return b, nil
	}
	src, err := t.Page(pg)
	if err != nil {
		return nil, err
	}
	b := make([]byte, len(src))
	copy(b, src)
	t.dirty[pg] = b
	return b, nil
}

// Allocate returns a new zeroed page that the caller can change.
func (t *WriteTxn) Allocate() (PgNo, []byte, error) {
	if t.done {
		return 0, nil, ErrTxDone
	}
	if pg := t.hdr.freeHead; pg != 0 {
		b, err := t.PageForWrite(pg)
		if err != nil {
			return 0, nil, err
		}
		if b[0] != TypeFree {
			return 0, nil, corruptf("page %d on the free list is not free", pg)
		}
		t.hdr.freeHead = PgNo(le.Uint32(b[4:]))
		t.hdr.freeCount--
		clear(b)
		return pg, b, nil
	}
	if t.hdr.pageCount == 1<<32-1 {
		return 0, nil, errors.New("doclite: database is full")
	}
	pg := PgNo(t.hdr.pageCount)
	t.hdr.pageCount++
	b := make([]byte, t.p.pageSize)
	t.dirty[pg] = b
	return pg, b, nil
}

// Free puts page pg on the free list. The caller must not use it again.
func (t *WriteTxn) Free(pg PgNo) error {
	if t.done {
		return ErrTxDone
	}
	if pg == 0 || uint32(pg) >= t.hdr.pageCount {
		return corruptf("free of bad page %d", pg)
	}
	b, ok := t.dirty[pg]
	if !ok {
		b = make([]byte, t.p.pageSize)
		t.dirty[pg] = b
	}
	clear(b)
	b[0] = TypeFree
	le.PutUint32(b[4:], uint32(t.hdr.freeHead))
	t.hdr.freeHead = pg
	t.hdr.freeCount++
	return nil
}

// Meta returns the user meta bytes of page 0.
func (t *WriteTxn) Meta() []byte { return t.hdr.meta[:] }

// SetMeta changes the user meta bytes of page 0.
func (t *WriteTxn) SetMeta(b []byte) { copy(t.hdr.meta[:], b) }

// FreeList returns the pages on the free list.
func (t *WriteTxn) FreeList() ([]PgNo, error) {
	return freeList(t.Page, t.hdr)
}

// Rollback ends the transaction and discards its changes.
func (t *WriteTxn) Rollback() {
	if t.done {
		return
	}
	t.done = true
	t.snap.Close()
	<-t.p.writer
}

// Commit makes the changes of the transaction durable and visible.
func (t *WriteTxn) Commit() error {
	if t.done {
		return ErrTxDone
	}
	p := t.p
	defer func() { <-p.writer }()
	t.done = true

	if len(t.dirty) == 0 && t.hdr == t.snap.hdr {
		t.snap.Close()
		return nil
	}
	t.hdr.txid++
	h0 := make([]byte, p.pageSize)
	t.hdr.encode(h0)
	t.dirty[0] = h0

	pgs := make([]PgNo, 0, len(t.dirty))
	for pg := range t.dirty {
		pgs = append(pgs, pg)
	}
	sort.Slice(pgs, func(i, j int) bool { return pgs[i] < pgs[j] })

	fs := frameHeaderSize + p.pageSize
	buf := make([]byte, len(pgs)*fs)
	prev := p.walCksum
	for i, pg := range pgs {
		fh := buf[i*fs : i*fs+frameHeaderSize]
		data := t.dirty[pg]
		setChecksum(data)
		le.PutUint32(fh[0:], uint32(pg))
		if i == len(pgs)-1 {
			le.PutUint32(fh[4:], frameCommit)
		}
		le.PutUint32(fh[8:], p.walSalt)
		ck := crc32.Update(prev, crcT, fh[:12])
		ck = crc32.Update(ck, crcT, data)
		le.PutUint32(fh[12:], ck)
		copy(buf[i*fs+frameHeaderSize:], data)
		prev = ck
	}

	_, err := p.wal.WriteAt(buf, p.walOff)
	if err == nil && !p.opts.NoSync {
		err = fsync(p.wal)
	}
	if err != nil {
		t.snap.Close()
		// Remove the partial frames so that they can not be replayed.
		if terr := p.wal.Truncate(p.walOff); terr != nil {
			p.mu.Lock()
			p.err = terr
			p.mu.Unlock()
		}
		return err
	}

	p.mu.Lock()
	for i, pg := range pgs {
		p.frame++
		p.index[pg] = append(p.index[pg], walEntry{p.frame, p.walOff + int64(i*fs+frameHeaderSize)})
		p.cache.put(cacheKey{pg, p.frame}, t.dirty[pg])
	}
	p.hdr = t.hdr
	p.mu.Unlock()
	p.walOff += int64(len(buf))
	p.walFrames += len(pgs)
	p.walCksum = prev
	t.snap.Close()

	// Copy the WAL into the main file when it is large and no reader
	// is open. If a reader is open, try again at a later commit.
	if p.walFrames >= p.opts.CheckpointSize && p.ckpt.TryLock() {
		p.checkpoint() // the commit is durable even if this fails
		p.ckpt.Unlock()
	}
	return nil
}
