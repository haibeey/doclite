package doclite

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

type Employee struct {
	Name    string   `json:"name"`
	Age     int      `json:"age"`
	City    string   `json:"city,omitempty"`
	Tags    []string `json:"tags,omitempty"`
	Bio     string   `json:"bio,omitempty"`
	Manager *Person  `json:"manager,omitempty"`
}

type Person struct {
	Name string `json:"name"`
}

func openDB(t testing.TB, path string, o *Options) *DB {
	t.Helper()
	if path == "" {
		path = filepath.Join(t.TempDir(), "test.db")
	}
	db, err := Open(path, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustCheck(t testing.TB, db *DB) {
	t.Helper()
	if err := db.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestCRUD(t *testing.T) {
	db := openDB(t, "", nil)
	c := db.Collection("employees")
	id, err := c.Insert(Employee{Name: "Ada", Age: 36})
	if err != nil || id != 1 {
		t.Fatalf("insert: id=%d err=%v", id, err)
	}
	var e Employee
	if err := c.FindOne(id, &e); err != nil || e.Name != "Ada" {
		t.Fatalf("find: %+v %v", e, err)
	}
	if err := c.UpdateOne(id, Employee{Name: "Ada L", Age: 37}); err != nil {
		t.Fatal(err)
	}
	c.FindOne(id, &e)
	if e.Name != "Ada L" || e.Age != 37 {
		t.Fatalf("after update: %+v", e)
	}
	if err := c.DeleteOne(id); err != nil {
		t.Fatal(err)
	}
	if err := c.FindOne(id, &e); !errors.Is(err, ErrNotFound) {
		t.Fatalf("find deleted: %v", err)
	}
	if err := c.DeleteOne(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete twice: %v", err)
	}
	if err := c.UpdateOne(99, e); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update missing: %v", err)
	}
	// Ids are not used again.
	id2, _ := c.Insert(Employee{Name: "Bob"})
	if id2 != 2 {
		t.Fatalf("id after delete = %d, want 2", id2)
	}
	if _, err := c.Insert([]int{1, 2}); !errors.Is(err, ErrNotObject) {
		t.Fatalf("insert array: %v", err)
	}
	if _, err := c.Insert(`not json`); !errors.Is(err, ErrNotObject) {
		t.Fatalf("insert string: %v", err)
	}
	mustCheck(t, db)
}

func TestMissingCollection(t *testing.T) {
	db := openDB(t, "", nil)
	c := db.Collection("nothing")
	var e Employee
	if err := c.FindOne(1, &e); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if n, err := c.Count(nil); n != 0 || err != nil {
		t.Fatal(n, err)
	}
	if err := c.DeleteOne(1); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	cur, err := c.Find(nil)
	if err != nil || cur.Next() {
		t.Fatal("cursor on missing collection")
	}
	if names, _ := db.Collections(); len(names) != 0 {
		t.Fatalf("reads made collections: %v", names)
	}
	if err := db.Collection("").Drop(); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("empty name: %v", err)
	}
	if _, err := db.Collection("a//b").Insert(M{}); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("bad name: %v", err)
	}
}

// This is the bug of the old engine: after a reopen, ids at page edges
// returned the wrong document.
func TestPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.db")
	db, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := db.Collection("e")
	for i := 1; i <= 500; i++ {
		e := Employee{Name: fmt.Sprint("e", i), Age: i}
		if i%50 == 0 {
			e.Bio = strings.Repeat("x", 20000) // overflow pages
		}
		if _, err := c.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	c.DeleteOne(5)
	db.Collection("other").Insert(M{"x": 1})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openDB(t, path, nil)
	c = db.Collection("e")
	for i := 1; i <= 500; i++ {
		var e Employee
		err := c.FindOne(int64(i), &e)
		if i == 5 {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted doc: %v", err)
			}
			continue
		}
		if err != nil || e.Age != i {
			t.Fatalf("doc %d: %+v %v", i, e, err)
		}
		if i%50 == 0 && len(e.Bio) != 20000 {
			t.Fatalf("doc %d: big field has %d bytes", i, len(e.Bio))
		}
	}
	if n, _ := c.Count(nil); n != 499 {
		t.Fatalf("count = %d", n)
	}
	if id, _ := c.Insert(Employee{}); id != 501 {
		t.Fatalf("next id = %d, want 501", id)
	}
	mustCheck(t, db)
}

func seed(t testing.TB, c *Collection) {
	t.Helper()
	docs := []any{
		Employee{Name: "Ada", Age: 36, City: "London", Tags: []string{"math", "code"}},
		Employee{Name: "Bola", Age: 28, City: "Lagos", Tags: []string{"code"}, Manager: &Person{"Ada"}},
		Employee{Name: "Chen", Age: 45, City: "Lagos"},
		Employee{Name: "Dee", Age: 19},
		M{"name": "Eve", "age": 28.0, "city": "Abuja", "score": 9.5},
	}
	if _, err := c.InsertMany(docs); err != nil {
		t.Fatal(err)
	}
}

func names(t testing.TB, c *Collection, filter any) []string {
	t.Helper()
	var out []Employee
	if err := c.FindAll(filter, &out); err != nil {
		t.Fatal(err)
	}
	var s []string
	for _, e := range out {
		s = append(s, e.Name)
	}
	return s
}

var filterCases = []struct {
	filter any
	want   []string
}{
	{nil, []string{"Ada", "Bola", "Chen", "Dee", "Eve"}},
	{M{"city": "Lagos"}, []string{"Bola", "Chen"}},
	{M{"age": 28}, []string{"Bola", "Eve"}}, // 28 matches 28.0
	{M{"age": M{"$gte": 28, "$lt": 40}}, []string{"Ada", "Bola", "Eve"}},
	{M{"age": M{"$gt": 28}}, []string{"Ada", "Chen"}},
	{M{"age": M{"$lte": 19}}, []string{"Dee"}},
	{M{"tags": "code"}, []string{"Ada", "Bola"}},
	{M{"tags": []string{"code"}}, []string{"Bola"}},
	{M{"manager.name": "Ada"}, []string{"Bola"}},
	{M{"city": M{"$in": []string{"London", "Abuja"}}}, []string{"Ada", "Eve"}},
	{M{"city": M{"$nin": []string{"Lagos"}}}, []string{"Ada", "Dee", "Eve"}},
	{M{"city": M{"$exists": false}}, []string{"Dee"}},
	{M{"city": nil}, []string{"Dee"}},
	{M{"city": M{"$ne": "Lagos"}}, []string{"Ada", "Dee", "Eve"}},
	{M{"name": M{"$regex": "^[AB]"}}, []string{"Ada", "Bola"}},
	{M{"age": M{"$not": M{"$gt": 30}}}, []string{"Bola", "Dee", "Eve"}},
	{M{"$or": []M{{"name": "Dee"}, {"city": "London"}}}, []string{"Ada", "Dee"}},
	{M{"$and": []M{{"city": "Lagos"}, {"age": M{"$gt": 30}}}}, []string{"Chen"}},
	{M{"$nor": []M{{"city": "Lagos"}, {"age": 19}}}, []string{"Ada", "Eve"}},
	{M{"score": M{"$gt": 9}}, []string{"Eve"}},
	{M{"city": "Lagos", "age": 28}, []string{"Bola"}},
	{M{"name": M{"$gte": "B", "$lt": "D"}}, []string{"Bola", "Chen"}},
	{struct {
		City string `json:"city"`
	}{"Lagos"}, []string{"Bola", "Chen"}},
}

func TestFilters(t *testing.T) {
	db := openDB(t, "", nil)
	c := db.Collection("e")
	seed(t, c)
	run := func(label string) {
		for _, tc := range filterCases {
			if got := names(t, c, tc.filter); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%s: filter %v = %v, want %v", label, tc.filter, got, tc.want)
			}
		}
	}
	run("scan")
	for _, f := range []string{"city", "age", "tags", "name", "manager.name", "score"} {
		if err := c.CreateIndex(f); err != nil {
			t.Fatal(err)
		}
	}
	run("index")
	mustCheck(t, db)

	for _, bad := range []any{M{"$foo": 1}, M{"a": M{"$in": 1}}, M{"a": M{"$regex": "("}}, []int{1}} {
		if _, err := c.Count(bad); !errors.Is(err, ErrInvalidFilter) {
			t.Errorf("filter %v: err = %v", bad, err)
		}
	}
}

func TestCursor(t *testing.T) {
	db := openDB(t, "", nil)
	c := db.Collection("e")
	seed(t, c)
	cur, err := c.Find(M{"city": "Lagos"})
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for cur.Next() {
		var e Employee
		if err := cur.Decode(&e); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, cur.ID())
	}
	if cur.Err() != nil || !reflect.DeepEqual(ids, []int64{2, 3}) {
		t.Fatalf("ids = %v err = %v", ids, cur.Err())
	}
	// A cursor that is closed early must end its read transaction, or
	// Close would wait for ever.
	cur, _ = c.Find(nil)
	cur.Next()
	cur.Close()

	var e Employee
	id, err := c.FindFirst(M{"age": 28}, &e)
	if err != nil || id != 2 || e.Name != "Bola" {
		t.Fatalf("FindFirst = %d %+v %v", id, e, err)
	}
	if _, err := c.FindFirst(M{"age": 99}, &e); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestDeleteManyAndCount(t *testing.T) {
	db := openDB(t, "", nil)
	c := db.Collection("e")
	seed(t, c)
	c.CreateIndex("city")
	n, err := c.DeleteMany(M{"city": "Lagos"})
	if err != nil || n != 2 {
		t.Fatalf("DeleteMany = %d %v", n, err)
	}
	if n, _ := c.Count(nil); n != 3 {
		t.Fatalf("count = %d", n)
	}
	if n, _ := c.Count(M{"age": 28}); n != 1 {
		t.Fatalf("count age 28 = %d", n)
	}
	mustCheck(t, db)
}

func TestTransactions(t *testing.T) {
	db := openDB(t, "", nil)
	errBoom := errors.New("boom")
	err := db.Update(func(tx *Tx) error {
		c := tx.Collection("e")
		c.Insert(M{"a": 1})
		c.Insert(M{"a": 2})
		return errBoom
	})
	if err != errBoom {
		t.Fatal(err)
	}
	if n, _ := db.Collection("e").Count(nil); n != 0 {
		t.Fatalf("rolled back insert is visible: %d", n)
	}
	err = db.Update(func(tx *Tx) error {
		c := tx.Collection("e")
		id, _ := c.Insert(M{"a": 1})
		var m M
		if err := c.FindOne(id, &m); err != nil {
			return err // a write tx sees its own changes
		}
		return c.CreateIndex("a")
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.View(func(tx *Tx) error {
		_, err := tx.Collection("e").Insert(M{})
		return err
	})
	if !errors.Is(err, ErrTxReadOnly) {
		t.Fatalf("insert in View: %v", err)
	}

	// A panic rolls back and does not keep the write lock.
	func() {
		defer func() { recover() }()
		db.Update(func(tx *Tx) error {
			tx.Collection("e").Insert(M{"a": 5})
			panic("x")
		})
	}()
	if n, _ := db.Collection("e").Count(nil); n != 1 {
		t.Fatalf("count after panic = %d", n)
	}

	// Snapshot: a read transaction does not see later commits.
	rtx, _ := db.Begin(false)
	db.Collection("e").Insert(M{"a": 9})
	if n, _ := rtx.Collection("e").Count(nil); n != 1 {
		t.Fatalf("snapshot count = %d", n)
	}
	rtx.Rollback()

	ctx, cancel := context.WithCancel(context.Background())
	wtx, _ := db.Begin(true)
	cancel()
	if _, err := db.BeginContext(ctx, true); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	wtx.Rollback()
	mustCheck(t, db)
}

func TestSubCollectionsAndDrop(t *testing.T) {
	db := openDB(t, "", nil)
	users := db.Collection("users")
	users.Insert(M{"n": 1})
	posts := users.Collection("posts")
	posts.CreateIndex("t")
	posts.Insert(M{"t": "hi"})
	if posts.Name() != "users/posts" {
		t.Fatal(posts.Name())
	}
	db.Collection("usersX").Insert(M{})
	got, _ := db.Collections()
	if !reflect.DeepEqual(got, []string{"users", "users/posts", "usersX"}) {
		t.Fatalf("collections = %v", got)
	}
	if err := users.Drop(); err != nil {
		t.Fatal(err)
	}
	got, _ = db.Collections()
	if !reflect.DeepEqual(got, []string{"usersX"}) {
		t.Fatalf("after drop = %v", got)
	}
	if n, _ := posts.Count(nil); n != 0 {
		t.Fatal("sub-collection not dropped")
	}
	mustCheck(t, db)
	if st := db.Stats(); st.FreePages == 0 {
		t.Fatalf("drop freed no pages: %+v", st)
	}
}

func TestIndexMaintenance(t *testing.T) {
	db := openDB(t, "", &Options{NoSync: true})
	c := db.Collection("e")
	c.CreateIndex("k")
	c.CreateIndex("tags")
	r := rand.New(rand.NewSource(3))
	model := map[int64]int{}
	for i := 0; i < 3000; i++ {
		switch op := r.Intn(4); {
		case op < 2 || len(model) == 0:
			k := r.Intn(50)
			id, err := c.Insert(M{"k": k, "tags": []int{k % 3, k % 5}, "pad": strings.Repeat("p", r.Intn(300))})
			if err != nil {
				t.Fatal(err)
			}
			model[id] = k
		case op == 2:
			for id := range model {
				k := r.Intn(50)
				if err := c.UpdateOne(id, M{"k": k, "tags": []int{k % 3}}); err != nil {
					t.Fatal(err)
				}
				model[id] = k
				break
			}
		default:
			for id := range model {
				c.DeleteOne(id)
				delete(model, id)
				break
			}
		}
	}
	mustCheck(t, db)
	for k := 0; k < 50; k++ {
		var want []int64
		for id, v := range model {
			if v == k {
				want = append(want, id)
			}
		}
		sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
		cur, _ := c.Find(M{"k": k})
		var got []int64
		for cur.Next() {
			got = append(got, cur.ID())
		}
		if len(want) == 0 {
			want = nil
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("k=%d: got %v want %v", k, got, want)
		}
	}
	if err := c.DropIndex("k"); err != nil {
		t.Fatal(err)
	}
	if ix, _ := c.Indexes(); !reflect.DeepEqual(ix, []string{"tags"}) {
		t.Fatalf("indexes = %v", ix)
	}
	mustCheck(t, db)
}

func TestConcurrency(t *testing.T) {
	db := openDB(t, "", &Options{NoSync: true, CheckpointSize: 50})
	c := db.Collection("e")
	c.CreateIndex("w")
	var wg sync.WaitGroup
	const writers, perWriter = 4, 200
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := c.Insert(M{"w": w, "i": i}); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	stop := make(chan struct{})
	var rg sync.WaitGroup
	for r := 0; r < 4; r++ {
		rg.Add(1)
		go func() {
			defer rg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Count and a scan in one snapshot must agree.
				err := db.View(func(tx *Tx) error {
					tc := tx.Collection("e")
					n, err := tc.Count(nil)
					if err != nil {
						return err
					}
					m, err := tc.Count(M{"i": M{"$gte": 0}})
					if err != nil {
						return err
					}
					if n != m {
						return fmt.Errorf("count %d != scan %d", n, m)
					}
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	rg.Wait()
	if n, _ := c.Count(nil); n != writers*perWriter {
		t.Fatalf("count = %d", n)
	}
	for w := 0; w < writers; w++ {
		if n, _ := c.Count(M{"w": w}); n != perWriter {
			t.Fatalf("writer %d: %d docs", w, n)
		}
	}
	mustCheck(t, db)
}

func TestReadOnlyAndLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ro.db")
	db, _ := Open(path, nil)
	db.Collection("e").Insert(M{"a": 1})
	if _, err := Open(path, nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("second open: %v", err)
	}
	db.Close()
	ro := openDB(t, path, &Options{ReadOnly: true})
	if n, _ := ro.Collection("e").Count(nil); n != 1 {
		t.Fatalf("read-only count = %d", n)
	}
	if _, err := ro.Collection("e").Insert(M{}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only insert: %v", err)
	}
}

func TestClosed(t *testing.T) {
	db, _ := Open(filepath.Join(t.TempDir(), "c.db"), nil)
	db.Close()
	if _, err := db.Collection("e").Insert(M{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("insert after close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func BenchmarkInsertNoSync(b *testing.B) {
	db := openDB(b, "", &Options{NoSync: true})
	c := db.Collection("e")
	doc := Employee{Name: "Ada", Age: 36, City: "London"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Insert(doc)
	}
}

func BenchmarkInsertBatch1000(b *testing.B) {
	db := openDB(b, "", nil)
	c := db.Collection("e")
	docs := make([]Employee, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.InsertMany(docs)
	}
}

func BenchmarkFindOne(b *testing.B) {
	db := openDB(b, "", &Options{NoSync: true})
	c := db.Collection("e")
	docs := make([]Employee, 10000)
	c.InsertMany(docs)
	b.ResetTimer()
	var e Employee
	for i := 0; i < b.N; i++ {
		c.FindOne(int64(i%10000)+1, &e)
	}
}

func TestPlanUsesIndex(t *testing.T) {
	db := openDB(t, "", nil)
	c := db.Collection("e")
	seed(t, c)
	c.CreateIndex("age")
	db.View(func(tx *Tx) error {
		s, _ := tx.coll("e", false)
		for _, tc := range []struct {
			f    M
			used bool
			ids  []int64
		}{
			{M{"age": 28}, true, []int64{2, 5}},
			{M{"age": M{"$in": []int{19, 45}}}, true, []int64{3, 4}},
			{M{"age": M{"$gt": 30}}, true, []int64{1, 3}},
			{M{"$and": []any{M{"age": 36}}}, true, []int64{1}},
			{M{"city": "Lagos"}, false, nil},
			{M{"age": M{"$ne": 28}}, false, nil},
			{M{"$or": []any{M{"age": 28}}}, false, nil},
		} {
			f, _ := normalize(tc.f)
			ids, used, err := s.plan(f.(map[string]any))
			if err != nil || used != tc.used || !reflect.DeepEqual(ids, tc.ids) {
				t.Errorf("plan %v = %v %v %v", tc.f, ids, used, err)
			}
		}
		return nil
	})
}
