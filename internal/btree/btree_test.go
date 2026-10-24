package btree

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"github.com/haibeey/doclite/internal/pager"
)

func newTxn(t testing.TB, pageSize int) (*pager.Pager, *pager.WriteTxn) {
	t.Helper()
	p, err := pager.Open(filepath.Join(t.TempDir(), "db"), pager.Options{PageSize: pageSize, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	w, err := p.BeginWrite(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Rollback)
	return p, w
}

// check verifies the tree and that every page is used once: by the
// tree or by the free list.
func check(t testing.TB, w *pager.WriteTxn, tr *Tree, model map[string][]byte) {
	t.Helper()
	seen := map[pager.PgNo]bool{}
	visit := func(pg pager.PgNo) error {
		if seen[pg] {
			return fmt.Errorf("page %d used twice", pg)
		}
		seen[pg] = true
		return nil
	}
	n, err := tr.Check(visit)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(model) {
		t.Fatalf("tree has %d keys, model has %d", n, len(model))
	}
	free, err := w.FreeList()
	if err != nil {
		t.Fatal(err)
	}
	for _, pg := range free {
		if err := visit(pg); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != w.PageCount()-1 {
		t.Fatalf("%d pages accounted for, file has %d", len(seen), w.PageCount()-1)
	}

	keys := make([]string, 0, len(model))
	for k := range model {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	c := tr.Cursor()
	i := 0
	for ok := c.First(); ok; ok = c.Next() {
		if i >= len(keys) || string(c.Key()) != keys[i] {
			t.Fatalf("cursor key %d = %q, want %q", i, c.Key(), keys[i])
		}
		v, err := c.Value()
		if err != nil || !bytes.Equal(v, model[keys[i]]) {
			t.Fatalf("cursor value for %q is wrong (%v)", keys[i], err)
		}
		i++
	}
	if c.Err() != nil || i != len(keys) {
		t.Fatalf("cursor saw %d keys, want %d (%v)", i, len(keys), c.Err())
	}
}

func randVal(r *rand.Rand, pageSize int) []byte {
	n := r.Intn(64)
	switch r.Intn(10) {
	case 0:
		n = r.Intn(pageSize * 3) // overflow
	case 1:
		n = r.Intn(pageSize / 3)
	}
	v := make([]byte, n)
	r.Read(v)
	return v
}

func TestRandomOps(t *testing.T) {
	for _, ps := range []int{1024, 4096} {
		t.Run(fmt.Sprint(ps), func(t *testing.T) {
			_, w := newTxn(t, ps)
			root, err := Create(w)
			if err != nil {
				t.Fatal(err)
			}
			tr := Open(w, root)
			r := rand.New(rand.NewSource(1))
			model := map[string][]byte{}
			key := func() []byte {
				k := make([]byte, 8+r.Intn(3)*r.Intn(tr.MaxKeySize()-8)/2)
				binary.BigEndian.PutUint64(k, uint64(r.Intn(3000)))
				return k
			}
			for i := 0; i < 20000; i++ {
				k := key()
				if r.Intn(3) == 0 {
					ok, err := tr.Delete(k)
					if err != nil {
						t.Fatal(err)
					}
					if _, want := model[string(k)]; ok != want {
						t.Fatalf("delete %x = %v, want %v", k, ok, want)
					}
					delete(model, string(k))
				} else {
					v := randVal(r, ps)
					if err := tr.Put(k, v); err != nil {
						t.Fatal(err)
					}
					model[string(k)] = v
				}
				if i%5000 == 0 {
					check(t, w, tr, model)
				}
			}
			check(t, w, tr, model)
			for k, v := range model {
				got, ok, err := tr.Get([]byte(k))
				if err != nil || !ok || !bytes.Equal(got, v) {
					t.Fatalf("get %x: ok=%v err=%v", k, ok, err)
				}
			}
			// Delete all: the tree must shrink back to one empty leaf.
			for k := range model {
				if _, err := tr.Delete([]byte(k)); err != nil {
					t.Fatal(err)
				}
				delete(model, k)
			}
			check(t, w, tr, model)
			if free, _ := w.FreeList(); len(free) != w.PageCount()-2 {
				t.Fatalf("after delete all: %d free of %d pages", len(free), w.PageCount())
			}
			if err := tr.Drop(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPersistAcrossCommit(t *testing.T) {
	p, w := newTxn(t, 4096)
	root, _ := Create(w)
	tr := Open(w, root)
	for i := 0; i < 5000; i++ {
		k := []byte(fmt.Sprintf("key%06d", i))
		tr.Put(k, bytes.Repeat(k, 1+i%50))
	}
	root = tr.Root()
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	s, _ := p.BeginRead()
	defer s.Close()
	ro := Open(s, root)
	if err := ro.Put([]byte("x"), nil); err == nil {
		t.Fatal("put on read-only tree worked")
	}
	c := ro.Cursor()
	n := 0
	for ok := c.Seek([]byte("key001000")); ok; ok = c.Next() {
		n++
	}
	if n != 4000 {
		t.Fatalf("seek scan saw %d keys, want 4000", n)
	}
}

func TestCursorWhileChanging(t *testing.T) {
	_, w := newTxn(t, 1024)
	root, _ := Create(w)
	tr := Open(w, root)
	for i := 0; i < 2000; i++ {
		tr.Put([]byte(fmt.Sprintf("%05d", i)), []byte("v"))
	}
	// Delete each key while the cursor is on it.
	c := tr.Cursor()
	n := 0
	for ok := c.First(); ok; ok = c.Next() {
		if _, err := tr.Delete(c.Key()); err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 2000 {
		t.Fatalf("visited %d keys, want 2000", n)
	}
}

func TestKeyTooLarge(t *testing.T) {
	_, w := newTxn(t, 4096)
	root, _ := Create(w)
	tr := Open(w, root)
	if err := tr.Put(make([]byte, tr.MaxKeySize()+1), nil); err != ErrKeyTooLarge {
		t.Fatalf("err = %v", err)
	}
	if err := tr.Put(make([]byte, tr.MaxKeySize()), make([]byte, 100000)); err != nil {
		t.Fatal(err)
	}
}

func FuzzTree(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0})
	f.Fuzz(func(t *testing.T, ops []byte) {
		_, w := newTxn(t, 1024)
		root, _ := Create(w)
		tr := Open(w, root)
		model := map[string][]byte{}
		for i := 0; i+1 < len(ops); i += 2 {
			k := []byte{ops[i] % 64}
			if ops[i+1]%4 == 0 {
				tr.Delete(k)
				delete(model, string(k))
				continue
			}
			v := bytes.Repeat([]byte{ops[i+1]}, int(ops[i+1])*13)
			if err := tr.Put(k, v); err != nil {
				t.Fatal(err)
			}
			model[string(k)] = v
		}
		check(t, w, tr, model)
	})
}

func BenchmarkPut(b *testing.B) {
	_, w := newTxn(b, 4096)
	root, _ := Create(w)
	tr := Open(w, root)
	k := make([]byte, 8)
	v := make([]byte, 100)
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(k, uint64(i))
		tr.Put(k, v)
	}
}
