// Package bench compares doclite with SQLite (modernc.org/sqlite, pure
// Go). Both use a WAL and sync each commit.
//
//	cd bench && go test -bench . -benchtime 2000x
package bench

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/haibeey/doclite"
	_ "modernc.org/sqlite"
)

type doc struct {
	Name  string `json:"name"`
	Age   int    `json:"age"`
	City  string `json:"city"`
	Email string `json:"email"`
}

func mkdoc(i int) doc {
	return doc{fmt.Sprint("user", i), i % 90, fmt.Sprint("city", i%100), fmt.Sprintf("u%d@example.com", i)}
}

const preload = 20000

func openDoclite(b *testing.B, load bool) (*doclite.DB, *doclite.Collection) {
	db, err := doclite.Open(filepath.Join(b.TempDir(), "d.db"), nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	c := db.Collection("users")
	if load {
		c.CreateIndex("city")
		docs := make([]doc, preload)
		for i := range docs {
			docs[i] = mkdoc(i)
		}
		if _, err := c.InsertMany(docs); err != nil {
			b.Fatal(err)
		}
	}
	return db, c
}

func openSQLite(b *testing.B, load bool) *sql.DB {
	db, err := sql.Open("sqlite", filepath.Join(b.TempDir(), "s.db")+"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, body TEXT NOT NULL)`); err != nil {
		b.Fatal(err)
	}
	if load {
		db.Exec(`CREATE INDEX users_city ON users (json_extract(body, '$.city'))`)
		tx, _ := db.Begin()
		for i := 0; i < preload; i++ {
			body, _ := json.Marshal(mkdoc(i))
			tx.Exec(`INSERT INTO users (body) VALUES (?)`, string(body))
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	return db
}

func BenchmarkInsertOneTx(b *testing.B) {
	b.Run("doclite", func(b *testing.B) {
		_, c := openDoclite(b, false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := c.Insert(mkdoc(i)); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sqlite", func(b *testing.B) {
		db := openSQLite(b, false)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			body, _ := json.Marshal(mkdoc(i))
			if _, err := db.Exec(`INSERT INTO users (body) VALUES (?)`, string(body)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkInsertBatch(b *testing.B) {
	b.Run("doclite", func(b *testing.B) {
		db, _ := openDoclite(b, false)
		b.ResetTimer()
		for i := 0; i < b.N; i += 1000 {
			db.Update(func(tx *doclite.Tx) error {
				c := tx.Collection("users")
				for j := i; j < i+1000 && j < b.N; j++ {
					if _, err := c.Insert(mkdoc(j)); err != nil {
						return err
					}
				}
				return nil
			})
		}
	})
	b.Run("sqlite", func(b *testing.B) {
		db := openSQLite(b, false)
		b.ResetTimer()
		for i := 0; i < b.N; i += 1000 {
			tx, _ := db.Begin()
			for j := i; j < i+1000 && j < b.N; j++ {
				body, _ := json.Marshal(mkdoc(j))
				tx.Exec(`INSERT INTO users (body) VALUES (?)`, string(body))
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGetByID(b *testing.B) {
	b.Run("doclite", func(b *testing.B) {
		_, c := openDoclite(b, true)
		b.ResetTimer()
		var d doc
		for i := 0; i < b.N; i++ {
			if err := c.FindOne(int64(i%preload)+1, &d); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sqlite", func(b *testing.B) {
		db := openSQLite(b, true)
		b.ResetTimer()
		var d doc
		for i := 0; i < b.N; i++ {
			var body string
			if err := db.QueryRow(`SELECT body FROM users WHERE id = ?`, i%preload+1).Scan(&body); err != nil {
				b.Fatal(err)
			}
			json.Unmarshal([]byte(body), &d)
		}
	})
}

// About 200 of 20000 documents match.
func BenchmarkIndexedQuery(b *testing.B) {
	b.Run("doclite", func(b *testing.B) {
		_, c := openDoclite(b, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out []doc
			if err := c.FindAll(doclite.M{"city": fmt.Sprint("city", i%100)}, &out); err != nil || len(out) != 200 {
				b.Fatal(len(out), err)
			}
		}
	})
	b.Run("sqlite", func(b *testing.B) {
		db := openSQLite(b, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`SELECT body FROM users WHERE json_extract(body, '$.city') = ?`, fmt.Sprint("city", i%100))
			if err != nil {
				b.Fatal(err)
			}
			var out []doc
			for rows.Next() {
				var body string
				rows.Scan(&body)
				var d doc
				json.Unmarshal([]byte(body), &d)
				out = append(out, d)
			}
			rows.Close()
			if len(out) != 200 {
				b.Fatal(len(out))
			}
		}
	})
}

// A filter on a field with no index reads all 20000 documents.
func BenchmarkFullScan(b *testing.B) {
	b.Run("doclite", func(b *testing.B) {
		_, c := openDoclite(b, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if n, err := c.Count(doclite.M{"age": 42}); err != nil || n == 0 {
				b.Fatal(n, err)
			}
		}
	})
	b.Run("sqlite", func(b *testing.B) {
		db := openSQLite(b, true)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var n int
			if err := db.QueryRow(`SELECT count(*) FROM users WHERE json_extract(body, '$.age') = 42`).Scan(&n); err != nil || n == 0 {
				b.Fatal(n, err)
			}
		}
	})
}
