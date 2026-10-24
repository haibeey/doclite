# doclite

doclite is an embedded document database for Go. It is to JSON documents
what SQLite is to SQL tables: one file, no server, no setup.

- **Crash-safe.** Each commit goes to a write-ahead log (WAL) and is
  synced before it returns. A crash or a power loss never leaves half a
  transaction. Every page has a checksum.
- **Safe with many goroutines.** One writer and many readers run at the
  same time. Readers see a fixed snapshot.
- **Safe with many processes.** A file lock stops two processes from
  writing the same file.
- **Transactions.** Change many documents and collections in one atomic
  step.
- **Queries and indexes.** Filters with `$gt`, `$in`, `$regex`, `$or`
  and more. Secondary indexes on any field path.
- **Fast.** About equal to SQLite for writes, faster for reads (see
  [Benchmarks](#benchmarks)).
- **Other languages.** A C shared library and a Python example.

## Install

```sh
go get github.com/haibeey/doclite
```

Go 1.22 or later.

## Use

```go
type User struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
	City string `json:"city"`
}

db, err := doclite.Open("app.db", nil) // makes the file if it is missing
if err != nil {
	log.Fatal(err)
}
defer db.Close()

users := db.Collection("users")

// Insert returns the id of the new document. Ids are never used again.
id, err := users.Insert(User{Name: "Ada", Age: 36, City: "London"})

// Get one document by id.
var u User
err = users.FindOne(id, &u) // doclite.ErrNotFound if it is missing

// Replace and delete.
err = users.UpdateOne(id, User{Name: "Ada L", Age: 37})
err = users.DeleteOne(id)

// Query. An index makes equality, $in and range queries fast.
err = users.CreateIndex("age")
var adults []User
err = users.FindAll(doclite.M{"age": doclite.M{"$gte": 18}}, &adults)

// A cursor reads the result one document at a time.
cur, err := users.Find(doclite.M{"city": "Lagos"})
defer cur.Close()
for cur.Next() {
	cur.Decode(&u)
	fmt.Println(cur.ID(), u)
}
err = cur.Err()

n, err := users.Count(nil)
n, err = users.DeleteMany(doclite.M{"age": doclite.M{"$lt": 18}})
```

### Transactions

`Update` commits if the function returns nil and rolls back if it
returns an error or panics. `View` is a read-only snapshot.

```go
err := db.Update(func(tx *doclite.Tx) error {
	accounts := tx.Collection("accounts")
	if err := accounts.UpdateOne(1, Account{Balance: 50}); err != nil {
		return err
	}
	return accounts.UpdateOne(2, Account{Balance: 150})
})
```

Inside the function, use `tx.Collection`. A write through
`db.Collection` waits for the write lock that the function holds, so it
never returns.

`db.Begin(writable)` gives a `*Tx` that you commit or roll back
yourself. `UpdateContext` and `BeginContext` stop waiting for the write
lock when the context is done.

### Filters

| Filter | Matches |
| --- | --- |
| `M{"name": "Ada"}` | equal |
| `M{"age": M{"$gt": 30, "$lte": 40}}` | range: `$gt` `$gte` `$lt` `$lte` |
| `M{"city": M{"$in": []string{"Lagos", "Abuja"}}}` | `$in`, `$nin` |
| `M{"city": M{"$ne": "Lagos"}}` | not equal |
| `M{"email": M{"$exists": false}}` | field is missing |
| `M{"name": M{"$regex": "^A"}}` | Go regular expression |
| `M{"age": M{"$not": M{"$gt": 30}}}` | negation |
| `M{"address.city": "Lagos"}` | field of a sub-document |
| `M{"tags": "go"}` | an array has the element |
| `M{"$or": []M{{"a": 1}, {"b": 2}}}` | `$and`, `$or`, `$nor` |

Numbers compare by value, so `1` matches `1.0`. A `nil` filter matches
all documents. A struct can be a filter, but encoding/json encodes its
zero fields too; use `omitempty` or a map.

### Sub-collections

```go
posts := db.Collection("users").Collection("posts") // name "users/posts"
```

`Drop` removes a collection with its indexes and sub-collections.

### Options

```go
db, err := doclite.Open("app.db", &doclite.Options{
	ReadOnly:    false,           // many read-only processes can share a file
	LockTimeout: 5 * time.Second, // wait for another process; default: fail at once
	CacheSize:   4096,            // pages in memory (default 2048, 8 MB)
	NoSync:      false,           // true is fast but not safe if the OS crashes
})
```

`db.Check()` reads the whole file and verifies it. `db.Stats()` gives
the size of the file and the WAL.

## How it works

| Layer | Package | What it does |
| --- | --- | --- |
| Pager | `internal/pager` | 4 KB pages with CRC-32C checksums, WAL with chained checksums, recovery, checkpoints, snapshots, free list, file lock |
| B+tree | `internal/btree` | byte-ordered keys, values of any size (overflow pages), split and merge |
| Documents | root package | catalog of collections, JSON documents keyed by id, secondary indexes, query planner, filters |

The database is two files: `app.db` and, while it is open, `app.db-wal`.
Do not copy only one of them while the database is open. On a clean
`Close`, doclite copies the WAL into the main file and removes it.

## Benchmarks

`cd bench && go test -bench .` compares doclite with SQLite
([modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)) in WAL mode
with `synchronous=FULL`. Documents are about 80 bytes of JSON; SQLite
stores them in a TEXT column with an index on `json_extract`. Results on
a 4-core Linux VM (lower is better):

| Benchmark | doclite | SQLite |
| --- | --- | --- |
| Insert, one transaction each (with fsync) | 450 µs | 414 µs |
| Insert, 1000 in a transaction (per document) | 5.5 µs | 5.9 µs |
| Get by id | 4.6 µs | 9.6 µs |
| Indexed query (200 of 20,000 match) | 624 µs | 651 µs |
| Full scan with a filter (20,000 documents) | 10.4 ms | 16.6 ms |

The insert time with fsync depends mostly on the disk.

## Tests

```sh
go test -race ./...                               # unit, concurrency and crash tests
go test -run XXX -fuzz FuzzTree ./internal/btree  # fuzz the B+tree
go test -run XXX -fuzz FuzzMatchRaw .             # fuzz the filter scanner
```

The crash test kills a writing process with SIGKILL at random times. Then
it checks that each commit that returned is in the file, that no
transaction is half applied, and that `Check` passes.

## Shared library (C, Python and others)

```sh
go build -buildmode=c-shared -o libdoclite.so ./sharedlib
```

This also writes `libdoclite.h`. Each function that can fail takes a
`char **err`; free every returned string and error with `doclite_free`.
See `examples/python/doclite.py` for a ctypes wrapper:

```python
from doclite import Doclite

with Doclite("app.db") as db:
    people = db.collection("people")
    people.create_index("age")
    pid = people.insert({"name": "Ada", "age": 36})
    print(people.find_one(pid))
    print(people.find({"age": {"$gte": 18}}))
```

## Upgrade from the 2020 version

The API and the file format changed. Old `.doclite` files can not be
opened. Main changes:

| Old | New |
| --- | --- |
| `doclite.Connect(file)` | `doclite.Open(file, nil)`, returns an error |
| `db.Base()` | `db.Collection(name)` |
| `Find(filter)` + `Next() interface{}` | `Find(filter)` + `Next() bool`, `Decode`, `Close` |
| `Delete(filter, doc)` | `DeleteMany(filter)` |
| `UpdateOneDoc(id, doc)` | `UpdateOne(id, doc)` |
| `Commit()` | each call commits; use `db.Update` for many changes |
| ids of deleted documents were used again | ids are never used again |

## License

See [LICENSE](LICENSE).
