// Command sharedlib builds doclite as a C shared library:
//
//	go build -buildmode=c-shared -o libdoclite.so ./sharedlib
//
// The build also writes libdoclite.h. Every function that can fail takes
// a char** err. On failure it sets *err to a message that the caller
// must free with doclite_free; on success it sets *err to NULL. Every
// returned string must also be freed with doclite_free.
//
// Collection names use "/" for sub-collections, for example
// "users/posts". Documents and filters are JSON text. A NULL or empty
// filter matches all documents.
package main

/*
#include <stdint.h>
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"errors"
	"sync"
	"unsafe"

	"github.com/haibeey/doclite"
)

var (
	mu      sync.Mutex
	handles = map[int64]*doclite.DB{}
	nextID  int64
)

func setErr(out **C.char, err error) {
	if out == nil {
		return
	}
	if err == nil {
		*out = nil
		return
	}
	*out = C.CString(err.Error())
}

func getDB(h C.int64_t) (*doclite.DB, error) {
	mu.Lock()
	defer mu.Unlock()
	db, ok := handles[int64(h)]
	if !ok {
		return nil, errors.New("doclite: bad database handle")
	}
	return db, nil
}

func coll(h C.int64_t, name *C.char) (*doclite.Collection, error) {
	db, err := getDB(h)
	if err != nil {
		return nil, err
	}
	if name == nil {
		return nil, doclite.ErrInvalidName
	}
	return db.Collection(C.GoString(name)), nil
}

func filterOf(f *C.char) (any, error) {
	if f == nil {
		return nil, nil
	}
	s := C.GoString(f)
	if s == "" {
		return nil, nil
	}
	return json.RawMessage(s), nil
}

func goBytes(s *C.char) []byte {
	if s == nil {
		return nil
	}
	return []byte(C.GoString(s))
}

//export doclite_open
func doclite_open(path *C.char, readOnly C.int, err **C.char) C.int64_t {
	db, e := doclite.Open(C.GoString(path), &doclite.Options{ReadOnly: readOnly != 0})
	setErr(err, e)
	if e != nil {
		return 0
	}
	mu.Lock()
	defer mu.Unlock()
	nextID++
	handles[nextID] = db
	return C.int64_t(nextID)
}

//export doclite_close
func doclite_close(h C.int64_t, err **C.char) C.int {
	db, e := getDB(h)
	if e == nil {
		mu.Lock()
		delete(handles, int64(h))
		mu.Unlock()
		e = db.Close()
	}
	setErr(err, e)
	if e != nil {
		return -1
	}
	return 0
}

//export doclite_insert
func doclite_insert(h C.int64_t, name, doc *C.char, err **C.char) C.int64_t {
	c, e := coll(h, name)
	var id int64
	if e == nil {
		id, e = c.Insert(goBytes(doc))
	}
	setErr(err, e)
	if e != nil {
		return -1
	}
	return C.int64_t(id)
}

// doclite_find_one returns the document as JSON, or NULL. If there is no
// such document, *err is NULL too.
//
//export doclite_find_one
func doclite_find_one(h C.int64_t, name *C.char, id C.int64_t, err **C.char) *C.char {
	c, e := coll(h, name)
	var raw json.RawMessage
	if e == nil {
		e = c.FindOne(int64(id), &raw)
	}
	if errors.Is(e, doclite.ErrNotFound) {
		setErr(err, nil)
		return nil
	}
	setErr(err, e)
	if e != nil {
		return nil
	}
	return C.CString(string(raw))
}

type result struct {
	ID  int64           `json:"id"`
	Doc json.RawMessage `json:"doc"`
}

// doclite_find returns a JSON array of {"id": ..., "doc": ...} objects.
//
//export doclite_find
func doclite_find(h C.int64_t, name, filter *C.char, err **C.char) *C.char {
	out, e := find(h, name, filter)
	setErr(err, e)
	if e != nil {
		return nil
	}
	return C.CString(string(out))
}

func find(h C.int64_t, name, filter *C.char) ([]byte, error) {
	c, err := coll(h, name)
	if err != nil {
		return nil, err
	}
	f, err := filterOf(filter)
	if err != nil {
		return nil, err
	}
	cur, err := c.Find(f)
	if err != nil {
		return nil, err
	}
	defer cur.Close()
	res := []result{}
	for cur.Next() {
		res = append(res, result{cur.ID(), append(json.RawMessage(nil), cur.Raw()...)})
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	return json.Marshal(res)
}

// notFound returns 1 for ErrNotFound, -1 for another error and 0 for
// none.
func status(err error) C.int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, doclite.ErrNotFound):
		return 1
	}
	return -1
}

// doclite_update_one returns 0 on success, 1 if there is no such
// document and -1 on an error.
//
//export doclite_update_one
func doclite_update_one(h C.int64_t, name *C.char, id C.int64_t, doc *C.char, err **C.char) C.int {
	c, e := coll(h, name)
	if e == nil {
		e = c.UpdateOne(int64(id), goBytes(doc))
	}
	if errors.Is(e, doclite.ErrNotFound) {
		setErr(err, nil)
	} else {
		setErr(err, e)
	}
	return status(e)
}

// doclite_delete_one returns 0 on success, 1 if there is no such
// document and -1 on an error.
//
//export doclite_delete_one
func doclite_delete_one(h C.int64_t, name *C.char, id C.int64_t, err **C.char) C.int {
	c, e := coll(h, name)
	if e == nil {
		e = c.DeleteOne(int64(id))
	}
	if errors.Is(e, doclite.ErrNotFound) {
		setErr(err, nil)
	} else {
		setErr(err, e)
	}
	return status(e)
}

//export doclite_delete_many
func doclite_delete_many(h C.int64_t, name, filter *C.char, err **C.char) C.int64_t {
	c, e := coll(h, name)
	var n int64
	if e == nil {
		var f any
		if f, e = filterOf(filter); e == nil {
			n, e = c.DeleteMany(f)
		}
	}
	setErr(err, e)
	if e != nil {
		return -1
	}
	return C.int64_t(n)
}

//export doclite_count
func doclite_count(h C.int64_t, name, filter *C.char, err **C.char) C.int64_t {
	c, e := coll(h, name)
	var n int64
	if e == nil {
		var f any
		if f, e = filterOf(filter); e == nil {
			n, e = c.Count(f)
		}
	}
	setErr(err, e)
	if e != nil {
		return -1
	}
	return C.int64_t(n)
}

//export doclite_create_index
func doclite_create_index(h C.int64_t, name, field *C.char, err **C.char) C.int {
	c, e := coll(h, name)
	if e == nil {
		e = c.CreateIndex(C.GoString(field))
	}
	setErr(err, e)
	return status(e)
}

//export doclite_free
func doclite_free(s *C.char) {
	C.free(unsafe.Pointer(s))
}

func main() {}
