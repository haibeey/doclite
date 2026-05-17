package doclite

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

// TestNextObjectAcrossCacheCursors verifies that NextObject propagates the
// object parameter across multiple cache cursor boundaries.
//
// Background: MinKeys = pageSize/dataSize = 32. Inserting 40 documents with a
// common filter produces two root/cache cursors. Each cache cursor pre-loads a
// single map[string]interface{} result from checkRootMatched. The fix ensures
// that when the current cache cursor is exhausted, NextObject recurses into
// NextObject (not Next), so that all subsequent results within the next cache
// cursor are unmarshaled into the caller's struct.
func TestNextObjectAcrossCacheCursors(t *testing.T) {
	db, err := Connect("test_nextobject_cursors.doclite")
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove("test_nextobject_cursors.doclite")
	}()

	baseCollection := db.Base()

	// Insert enough documents to span multiple cache cursors.
	// MinKeys = 32, so 40 documents will cross at least one boundary.
	totalDocs := 40
	for i := 0; i < totalDocs; i++ {
		e := &Employer{
			Name:    fmt.Sprintf("worker_%d", i),
			Address: "workplace",
		}
		_, err := baseCollection.Insert(e)
		if err != nil {
			t.Fatalf("failed to insert document %d: %v", i, err)
		}
	}

	e := &Employer{}
	filter := &Employer{Address: "workplace"}
	cur := baseCollection.Find(filter)

	expectedType := reflect.TypeOf(e)
	rawMapPositions := []int{}
	structCount := 0

	for i := 0; ; i++ {
		emp := cur.NextObject(e)
		if emp == nil {
			break
		}

		if _, isMap := emp.(map[string]interface{}); isMap {
			rawMapPositions = append(rawMapPositions, i)
		} else if reflect.TypeOf(emp) == expectedType {
			structCount++
		}
	}

	// Total count must be correct.
	if structCount+len(rawMapPositions) != totalDocs {
		t.Errorf("got %d total results (%d structs + %d maps), expected %d",
			structCount+len(rawMapPositions), structCount, len(rawMapPositions), totalDocs)
	}

	// With two cache cursors, checkRootMatched produces one map per cursor.
	// Without the fix (c.Next() instead of c.NextObject(object) on recursion),
	// ALL results from the second cursor onwards would be maps because Next()
	// delegates to cc.next() which uses Find() (map-based), not FindNodes().
	// With the fix, only the checkRootMatched results are maps.
	if len(rawMapPositions) != 2 {
		t.Errorf("expected exactly 2 raw maps (one per cache cursor from checkRootMatched), "+
			"got %d at positions %v — the recursive call may still delegate to Next()",
			len(rawMapPositions), rawMapPositions)
	}

	// The two expected map positions are at the start of each cache cursor.
	// Cursor 1 starts at index 0; cursor 2 starts at index 32 (MinKeys).
	// We check that the second map is near position 32 and not at a later
	// position (which would indicate all of cursor 2's results were maps).
	if len(rawMapPositions) >= 2 && rawMapPositions[1] > 33 {
		t.Errorf("second raw map at position %d is too far from expected boundary (~32); "+
			"this suggests multiple consecutive maps from cursor 2", rawMapPositions[1])
	}
}

// TestNextObjectStructsAtBoundary specifically asserts that the first struct
// returned AFTER each cache cursor boundary is a properly typed struct, not a
// raw map. This is the core acceptance criterion: the object parameter must be
// propagated through recursive cache cursor boundaries.
func TestNextObjectStructsAtBoundary(t *testing.T) {
	db, err := Connect("test_nextobject_boundary.doclite")
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}
	defer func() {
		db.Close()
		os.Remove("test_nextobject_boundary.doclite")
	}()

	baseCollection := db.Base()

	totalDocs := 40
	for i := 0; i < totalDocs; i++ {
		e := &Employer{
			Name:    fmt.Sprintf("boundary_%d", i),
			Address: "factory",
		}
		_, err := baseCollection.Insert(e)
		if err != nil {
			t.Fatalf("failed to insert document %d: %v", i, err)
		}
	}

	e := &Employer{}
	filter := &Employer{Address: "factory"}
	cur := baseCollection.Find(filter)

	expectedType := reflect.TypeOf(e)
	structCountAfterFirstMap := 0

	for {
		emp := cur.NextObject(e)
		if emp == nil {
			break
		}

		if reflect.TypeOf(emp) == expectedType {
			structCountAfterFirstMap++
		}
	}

	// With the fix, every non-checkRootMatched result must be a *Employer.
	// That is: totalDocs - numCacheCursors structs (40 - 2 = 38).
	// If the recursive call was c.Next() instead of c.NextObject(object),
	// most results from cursor 2 would be maps, yielding far fewer structs.
	expectedStructs := totalDocs - 2 // subtract one map per cache cursor
	if structCountAfterFirstMap < expectedStructs {
		t.Errorf("got %d typed structs, expected at least %d — "+
			"NextObject may not be propagating object across cache cursor boundaries",
			structCountAfterFirstMap, expectedStructs)
	}
}