package pager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func openTest(t *testing.T, path string, o Options) *Pager {
	t.Helper()
	p, err := Open(path, o)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func writePage(t *testing.T, w *WriteTxn, fill byte) PgNo {
	t.Helper()
	pg, b, err := w.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	for i := range b[:len(b)-trailerSize] {
		b[i] = fill
	}
	b[0] = 1
	return pg
}

func pageByte(t *testing.T, s interface {
	Page(PgNo) ([]byte, error)
}, pg PgNo) byte {
	t.Helper()
	b, err := s.Page(pg)
	if err != nil {
		t.Fatal(err)
	}
	return b[10]
}

func TestCommitReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	p := openTest(t, path, Options{})
	w, err := p.BeginWrite(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	pg := writePage(t, w, 7)
	w.SetMeta([]byte("hello"))
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + "-wal"); !os.IsNotExist(err) {
		t.Fatalf("WAL not removed on close: %v", err)
	}

	p = openTest(t, path, Options{})
	defer p.Close()
	s, _ := p.BeginRead()
	defer s.Close()
	if got := pageByte(t, s, pg); got != 7 {
		t.Fatalf("page content = %d, want 7", got)
	}
	if string(s.Meta()[:5]) != "hello" {
		t.Fatalf("meta = %q", s.Meta()[:5])
	}
}

func TestRollback(t *testing.T) {
	p := openTest(t, filepath.Join(t.TempDir(), "db"), Options{})
	defer p.Close()
	w, _ := p.BeginWrite(context.Background())
	writePage(t, w, 1)
	w.Rollback()
	if n := p.Stats().PageCount; n != 1 {
		t.Fatalf("page count = %d after rollback", n)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	p := openTest(t, filepath.Join(t.TempDir(), "db"), Options{})
	defer p.Close()
	w, _ := p.BeginWrite(context.Background())
	pg := writePage(t, w, 1)
	w.Commit()

	s, _ := p.BeginRead()
	w, _ = p.BeginWrite(context.Background())
	b, _ := w.PageForWrite(pg)
	b[10] = 2
	if got := pageByte(t, s, pg); got != 1 {
		t.Fatalf("reader sees uncommitted data: %d", got)
	}
	w.Commit()
	if got := pageByte(t, s, pg); got != 1 {
		t.Fatalf("old reader sees new commit: %d", got)
	}
	s.Close()
	s, _ = p.BeginRead()
	defer s.Close()
	if got := pageByte(t, s, pg); got != 2 {
		t.Fatalf("new reader = %d, want 2", got)
	}
}

// A reader that is open stops the checkpoint, so it keeps its snapshot.
func TestCheckpointWaitsForReaders(t *testing.T) {
	p := openTest(t, filepath.Join(t.TempDir(), "db"), Options{CheckpointSize: 2})
	defer p.Close()
	w, _ := p.BeginWrite(context.Background())
	pg := writePage(t, w, 1)
	w.Commit()
	s, _ := p.BeginRead()
	for i := 2; i < 10; i++ {
		w, _ = p.BeginWrite(context.Background())
		b, _ := w.PageForWrite(pg)
		b[10] = byte(i)
		if err := w.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	if got := pageByte(t, s, pg); got != 1 {
		t.Fatalf("reader snapshot changed: %d", got)
	}
	s.Close()
	w, _ = p.BeginWrite(context.Background())
	writePage(t, w, 0)
	w.Commit()
	if n := p.Stats().WALFrames; n != 0 {
		t.Fatalf("WAL frames = %d after checkpoint", n)
	}
	s, _ = p.BeginRead()
	defer s.Close()
	if got := pageByte(t, s, pg); got != 9 {
		t.Fatalf("after checkpoint = %d, want 9", got)
	}
}

func TestFreeListReuse(t *testing.T) {
	p := openTest(t, filepath.Join(t.TempDir(), "db"), Options{})
	defer p.Close()
	w, _ := p.BeginWrite(context.Background())
	a := writePage(t, w, 1)
	b := writePage(t, w, 1)
	w.Commit()
	w, _ = p.BeginWrite(context.Background())
	w.Free(a)
	w.Free(b)
	w.Commit()
	if st := p.Stats(); st.FreePages != 2 || st.PageCount != 3 {
		t.Fatalf("stats after free = %+v", st)
	}
	w, _ = p.BeginWrite(context.Background())
	c := writePage(t, w, 1)
	w.Commit()
	if c != b {
		t.Fatalf("allocate = %d, want free page %d", c, b)
	}
	s, _ := p.BeginRead()
	defer s.Close()
	fl, err := s.FreeList()
	if err != nil || len(fl) != 1 || fl[0] != a {
		t.Fatalf("free list = %v, %v", fl, err)
	}
}

// crashCopy copies the database and WAL as they are on disk now, as if
// the process stopped at this point.
func crashCopy(t *testing.T, path string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "crash")
	for _, sfx := range []string{"", "-wal"} {
		b, err := os.ReadFile(path + sfx)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst+sfx, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

func TestRecoverFromWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	p := openTest(t, path, Options{CheckpointSize: 1 << 30})
	defer p.Close()
	var pgs []PgNo
	for i := 1; i <= 5; i++ {
		w, _ := p.BeginWrite(context.Background())
		pgs = append(pgs, writePage(t, w, byte(i)))
		w.Commit()
	}
	walSize := func(path string) int64 {
		st, _ := os.Stat(path + "-wal")
		return st.Size()
	}
	full := crashCopy(t, path)
	q := openTest(t, full, Options{})
	s, _ := q.BeginRead()
	for i, pg := range pgs {
		if got := pageByte(t, s, pg); got != byte(i+1) {
			t.Fatalf("page %d = %d after recovery", pg, got)
		}
	}
	s.Close()
	q.Close()

	// Cut the WAL in the middle of the last transaction. Recovery must
	// keep the first four commits and drop the last one.
	torn := crashCopy(t, path)
	fs := int64(frameHeaderSize + p.PageSize())
	if err := os.Truncate(torn+"-wal", walSize(path)-fs/2); err != nil {
		t.Fatal(err)
	}
	q = openTest(t, torn, Options{})
	defer q.Close()
	if n := q.Stats().PageCount; n != 5 {
		t.Fatalf("page count = %d, want 5", n)
	}

	// Flip a byte in the WAL of a third copy. Recovery stops at the
	// damaged frame.
	bad := crashCopy(t, path)
	f, _ := os.OpenFile(bad+"-wal", os.O_RDWR, 0)
	f.WriteAt([]byte{0xAA}, walHeaderSize+2*fs+100)
	f.Close()
	r := openTest(t, bad, Options{})
	defer r.Close()
	if n := r.Stats().PageCount; n != 2 {
		t.Fatalf("page count = %d, want 2", n)
	}
}

func TestCorruptPageDetected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	p := openTest(t, path, Options{})
	w, _ := p.BeginWrite(context.Background())
	pg := writePage(t, w, 1)
	w.Commit()
	p.Close()

	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	f.WriteAt([]byte{0xAA}, int64(pg)*DefaultPageSize+50)
	f.Close()
	p = openTest(t, path, Options{})
	defer p.Close()
	s, _ := p.BeginRead()
	defer s.Close()
	if _, err := s.Page(pg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestNotDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	os.WriteFile(path, []byte("this is not a database file at all"), 0o644)
	if _, err := Open(path, Options{}); !errors.Is(err, ErrNotDB) {
		t.Fatalf("err = %v, want ErrNotDB", err)
	}
}

func TestLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	p := openTest(t, path, Options{})
	if _, err := Open(path, Options{}); !errors.Is(err, ErrLocked) {
		t.Fatalf("second open: err = %v, want ErrLocked", err)
	}
	if _, err := Open(path, Options{ReadOnly: true}); !errors.Is(err, ErrLocked) {
		t.Fatalf("read-only open: err = %v, want ErrLocked", err)
	}
	p.Close()
	r1 := openTest(t, path, Options{ReadOnly: true})
	r2 := openTest(t, path, Options{ReadOnly: true})
	if _, err := r1.BeginWrite(context.Background()); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("write on read-only: err = %v", err)
	}
	r1.Close()
	r2.Close()
}

func TestBeginWriteContext(t *testing.T) {
	p := openTest(t, filepath.Join(t.TempDir(), "db"), Options{})
	defer p.Close()
	w, _ := p.BeginWrite(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.BeginWrite(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	w.Rollback()
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	p := openTest(t, filepath.Join(t.TempDir(), "db"), Options{CheckpointSize: 8, NoSync: true})
	defer p.Close()
	w, _ := p.BeginWrite(context.Background())
	pa := writePage(t, w, 0)
	pb := writePage(t, w, 0)
	w.Commit()

	// The writer keeps both pages equal. A reader must never see them
	// differ.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s, err := p.BeginRead()
				if err != nil {
					t.Error(err)
					return
				}
				a, err1 := s.Page(pa)
				b, err2 := s.Page(pb)
				if err1 != nil || err2 != nil || a[10] != b[10] {
					t.Errorf("torn read: %v %v", err1, err2)
				}
				s.Close()
			}
		}()
	}
	for i := 1; i < 300; i++ {
		w, err := p.BeginWrite(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, pg := range []PgNo{pa, pb} {
			b, _ := w.PageForWrite(pg)
			b[10] = byte(i)
		}
		if err := w.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}
