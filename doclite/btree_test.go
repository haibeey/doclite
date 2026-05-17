package doclite

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func testBtree(bt *Btree, t *testing.T) *Btree {
	var (
		numOfInsert = 100
	)
	for i := 0; i < numOfInsert; i++ {
		_, err := bt.Insert([]byte(fmt.Sprintf("%d docklite", i)))
		if err != nil {
			t.Errorf("Insert failed: %v", err)
		}
	}

	if bt.NumDocuments != int64(numOfInsert) {
		t.Errorf("%d is not  equal to number of docs inserted", bt.NumDocuments)
	}
	add := 0
	if numOfInsert%MinKeys != 0 {
		add++
	}
	if bt.NumRoots != numOfInsert/MinKeys+add {
		t.Errorf("%d is not  equal to number of root docs", bt.NumRoots)
	}

	x := 0
	pageNo := bt.Pages[x]
	for i := 1; i <= numOfInsert; i++ {
		if i%MinKeys == 0 {
			x++
			pageNo = bt.Pages[x]
		}
		n, err := bt.Find(int64(i))
		if err != nil {
			t.Errorf("%v getting data failed", err)
		}
		if i%MinKeys != 0 && n.document.offset != pageNo*pageSize+int64(((i-1)%MinKeys)*dataSize) {
			t.Errorf("unmatching offset")
		}
	}

	bt.InsertSubCollection("newcollection")

	return bt.Get("newcollection")
}
func TestBtree(t *testing.T) {
	db := &DB{metadata: &Meta{}}
	bt := db.newBtree("")
	bt1 := testBtree(bt, t)

	if bt.Get("newcollection") == nil {
		t.Errorf("collection newcollection not found")
	}
	testBtree(bt1, t)

	data, err := json.Marshal(bt)
	if err != nil {
		t.Errorf("%v failed marshaling btree", err)
	}
	fmt.Println(string(data))
}

// TestBtreeDiskInitExactMultiple verifies that diskInitBtree sets the correct
// numChildren for the last root when NumDocuments is an exact multiple of MinKeys.
// This is a regression test for the bug where NumDocuments % MinKeys == 0 caused
// numChildren to be 0 (using the old buggy formula), making all documents in the
// last root invisible on reload.
func TestBtreeDiskInitExactMultiple(t *testing.T) {
	// Use exactly 2 * MinKeys documents so that NumDocuments % MinKeys == 0
	numDocs := 2 * MinKeys

	db := &DB{metadata: &Meta{}}
	bt := db.newBtree("")

	// Insert exactly 2 * MinKeys documents
	for i := 0; i < numDocs; i++ {
		_, err := bt.Insert([]byte(fmt.Sprintf("doc-%d", i)))
		if err != nil {
			t.Errorf("Insert(%d) failed: %v", i, err)
		}
	}

	// Verify all documents were inserted
	if bt.NumDocuments != int64(numDocs) {
		t.Fatalf("expected NumDocuments=%d, got %d", numDocs, bt.NumDocuments)
	}

	if bt.NumRoots != 2 {
		t.Fatalf("expected NumRoots=2, got %d", bt.NumRoots)
	}

	// Verify all documents are findable via Find() (structural check only;
	// data content verification requires real disk I/O which is unavailable
	// in this in-memory test setup)
	for i := int64(1); i <= int64(numDocs); i++ {
		n, err := bt.Find(i)
		if err != nil {
			t.Errorf("Find(%d) failed: %v", i, err)
			continue
		}
		if n == nil {
			t.Errorf("Find(%d) returned nil", i)
		}
	}

	// Verify the last root's numChildren is MinKeys-1 (the root doc itself is not a child)
	lastRoot := bt.roots[len(bt.roots)-1]
	if lastRoot.numChildren != MinKeys-1 {
		t.Errorf("last root numChildren = %d, want %d (MinKeys-1)", lastRoot.numChildren, MinKeys-1)
	}

	// Simulate a disk reload by clearing in-memory roots and re-initializing
	bt.roots = nil
	bt.initBtreeRoot = false
	bt.db = &DB{metadata: &Meta{}, file: nil} // nil file so read returns empty (ok for test)
	bt.findPool = make(map[int64]int64)
	bt.diskInitBtree()

	// After diskInitBtree, verify structural integrity.
	// Note: we cannot verify document data content after disk reload in this
	// in-memory test because db.file is nil (read returns empty bytes).
	for i := int64(1); i <= int64(numDocs); i++ {
		_, err := bt.Find(i)
		if err != nil {
			t.Errorf("after diskInitBtree, Find(%d) failed: %v", i, err)
		}
	}

	// Verify last root's numChildren is correct after disk re-init
	lastRootAfterReload := bt.roots[len(bt.roots)-1]
	if lastRootAfterReload.numChildren != MinKeys-1 {
		t.Errorf("after diskInitBtree, last root numChildren = %d, want %d (MinKeys-1)",
			lastRootAfterReload.numChildren, MinKeys-1)
	}

	// Verify first root's numChildren is MinKeys-1 (consistent with insert behavior)
	firstRootAfterReload := bt.roots[0]
	if firstRootAfterReload.numChildren != MinKeys-1 {
		t.Errorf("after diskInitBtree, first root numChildren = %d, want %d (MinKeys-1)",
			firstRootAfterReload.numChildren, MinKeys-1)
	}
}

// TestBtreePoolReuseRootBoundary is a regression test for the bug where Insert
// reuses a pool ID at a root boundary (where (id-1) % MinKeys == 0) but the
// original root no longer exists, causing silent data loss.
func TestBtreePoolReuseRootBoundary(t *testing.T) {
	db := &DB{metadata: &Meta{}}
	bt := db.newBtree("")

	// Insert enough documents to create at least 2 roots (2 * MinKeys)
	numDocs := 2 * MinKeys
	for i := 0; i < numDocs; i++ {
		id, err := bt.Insert([]byte(fmt.Sprintf("doc-%d", i)))
		if err != nil {
			t.Fatalf("Insert(%d) failed: %v", i, err)
		}
		if id == -1 {
			t.Fatalf("Insert(%d) returned -1 during initial insert", i)
		}
	}

	// Verify initial state
	if bt.NumDocuments != int64(numDocs) {
		t.Fatalf("expected NumDocuments=%d, got %d", numDocs, bt.NumDocuments)
	}
	if bt.NumRoots < 2 {
		t.Fatalf("expected at least 2 roots, got %d", bt.NumRoots)
	}

	// Save references to original roots for later comparison
	originalRootCount := bt.NumRoots

	// Delete ALL documents to put every ID into the pool
	for id := int64(1); id <= int64(numDocs); id++ {
		bt.Delete(id)
	}

	if len(bt.Pool) != numDocs {
		t.Fatalf("expected pool size=%d, got %d", numDocs, len(bt.Pool))
	}

	// Insert new documents that will reuse pool IDs, including root-boundary IDs.
	// The pool is LIFO, so the first reuse will be ID = numDocs, then numDocs-1, etc.
	// Root-boundary IDs are: 1, MinKeys+1, 2*MinKeys+1, ...
	// ID 1 is a root boundary ((1-1)%MinKeys == 0).
	newNumDocs := numDocs
	for i := 0; i < newNumDocs; i++ {
		data := []byte(fmt.Sprintf("reused-doc-%d", i))
		id, err := bt.Insert(data)
		if err != nil {
			t.Errorf("Insert(%d) failed during pool reuse: %v", i, err)
			continue
		}
		if id == -1 {
			t.Errorf("Insert(%d) returned -1 during pool reuse", i)
			continue
		}
	}

	// Verify that root-boundary IDs that were reused now have correct data.
	// We verify this by directly checking the roots' in-memory data, since Find
	// relies on disk I/O which is not available in this test setup.
	for _, root := range bt.roots {
		rootID := root.document.id
		if (rootID-1)%MinKeys == 0 {
			// This is a root-boundary node. Find which pool-reuse iteration
			// corresponds to this ID.
			// Pool after deletions: [1, 2, 3, ..., numDocs]
			// Reuse order: numDocs (i=0), numDocs-1 (i=1), ..., 1 (i=numDocs-1)
			if rootID >= 1 && rootID <= int64(numDocs) {
				iterIndex := int64(numDocs) - rootID
				expected := fmt.Sprintf("reused-doc-%d", iterIndex)
				actual := string(root.document.data)
				if actual != expected {
					t.Errorf("Root ID %d (root-boundary): expected data %q, got %q",
						rootID, expected, actual)
				}
			}
		}
	}

	// Verify that the number of roots is at least the original count.
	// New roots may have been created for pool IDs that originally had
	// root-boundary positions but whose roots were effectively "deleted".
	// The fix ensures that when a root no longer exists, addRoot is called
	// instead of the buggy Update that silently lost data.
	if bt.NumRoots < originalRootCount {
		t.Errorf("NumRoots decreased from %d to %d after pool reuse; data may have been lost",
			originalRootCount, bt.NumRoots)
	}
}

// TestBtreeMaxSizeEnforced verifies that Insert returns ErrTreeFull when
// the number of documents reaches BtreeMaxSize, and that insertions from the
// pool (reusing deleted IDs) are still allowed after the limit is hit.
//
// To avoid inserting millions of documents in the test, we directly set
// NumDocuments to simulate a full tree, and only verify the boundary checks.
func TestBtreeMaxSizeEnforced(t *testing.T) {
	db := &DB{metadata: &Meta{}}
	bt := db.newBtree("")

	// --- Part 1: Insert below the limit should succeed ---
	// Insert one document to verify normal operation works
	id, err := bt.Insert([]byte("doc-0"))
	if err != nil {
		t.Fatalf("normal Insert failed: %v", err)
	}
	if id != 1 {
		t.Fatalf("expected id=1, got %d", id)
	}

	// --- Part 2: Simulate a full tree by setting NumDocuments directly ---
	bt.NumDocuments = BtreeMaxSize

	// Insert should fail with ErrTreeFull
	id, err = bt.Insert([]byte("doc-overflow"))
	if err == nil {
		t.Fatal("Insert beyond BtreeMaxSize should return an error")
	}
	if !errors.Is(err, ErrTreeFull) {
		t.Fatalf("expected ErrTreeFull, got: %v", err)
	}
	if id != -1 {
		t.Fatalf("expected id=-1 on ErrTreeFull, got: %d", id)
	}

	// Verify NumDocuments was not incremented
	if bt.NumDocuments != BtreeMaxSize {
		t.Fatalf("NumDocuments should remain %d after failed insert, got %d",
			BtreeMaxSize, bt.NumDocuments)
	}

	// --- Part 3: Verify boundary at exactly BtreeMaxSize - 1 ---
	bt.NumDocuments = BtreeMaxSize - 1

	id, err = bt.Insert([]byte("doc-at-limit"))
	if err != nil {
		t.Fatalf("Insert at BtreeMaxSize-1 should succeed, got: %v", err)
	}
	if bt.NumDocuments != BtreeMaxSize {
		t.Fatalf("expected NumDocuments=%d after insert, got %d",
			BtreeMaxSize, bt.NumDocuments)
	}

	// --- Part 4: Verify boundary at exactly BtreeMaxSize ---
	id, err = bt.Insert([]byte("doc-over"))
	if err == nil {
		t.Fatal("Insert at BtreeMaxSize should return ErrTreeFull")
	}
	if !errors.Is(err, ErrTreeFull) {
		t.Fatalf("expected ErrTreeFull at BtreeMaxSize, got: %v", err)
	}
	if id != -1 {
		t.Fatalf("expected id=-1 on ErrTreeFull, got: %d", id)
	}

	// Verify NumDocuments was not incremented
	if bt.NumDocuments != BtreeMaxSize {
		t.Fatalf("NumDocuments should remain %d after failed insert, got %d",
			BtreeMaxSize, bt.NumDocuments)
	}

	// --- Part 5: Pool-based insertions should bypass the limit ---
	// Add an ID to the pool, then verify insert succeeds despite the tree being full
	bt.Pool = append(bt.Pool, int64(999))
	id, err = bt.Insert([]byte("doc-reused"))
	if err != nil {
		t.Fatalf("pool-based Insert after BtreeMaxSize should succeed, got: %v", err)
	}
	if id != 999 {
		t.Fatalf("expected reused id=999, got: %d", id)
	}

	// NumDocuments should still be BtreeMaxSize (pool reuse doesn't increment)
	if bt.NumDocuments != BtreeMaxSize {
		t.Fatalf("NumDocuments should still be %d after pool reuse, got %d",
			BtreeMaxSize, bt.NumDocuments)
	}

	// Pool should be empty now
	if len(bt.Pool) != 0 {
		t.Fatalf("expected empty pool after reuse, got %d items", len(bt.Pool))
	}

	// --- Part 6: With empty pool and full tree, insert should fail again ---
	id, err = bt.Insert([]byte("doc-still-full"))
	if err == nil {
		t.Fatal("Insert with empty pool and full tree should return ErrTreeFull")
	}
	if !errors.Is(err, ErrTreeFull) {
		t.Fatalf("expected ErrTreeFull, got: %v", err)
	}
}

func TestBinarySearch(t *testing.T) {
	nodes := []*Node{}
	ids := []int64{}
	for i := 0; i < 100; i++ {
		id := int64(i)
		ids = append(ids, id)
		doc := &Document{id: id, data: []byte{}}
		n := &Node{document: doc}
		nodes = append(nodes, n)
	}
	for i := 0; i < 100; i++ {
		if ids[i] != nodes[indexOfNodes(ids[i], nodes, 100)].document.id {
			t.Errorf(" nodes not sorted %d %d", i, indexOfNodes(ids[i], nodes, 100))
		}
	}

	list := []*Node{{document: &Document{id: 1, data: []byte{}}}, {document: &Document{id: 33, data: []byte{}}}, {document: &Document{id: 65, data: []byte{}}}, {document: &Document{id: 97, data: []byte{}}}}

	if indexOfNodes(int64(3), list, 4) != 0 {
		t.Errorf(" wrong node")
	}

	if indexOfNodes(int64(40), list, 4) != 1 {
		t.Errorf(" wrong node")
	}

	if indexOfNodes(int64(78), list, 4) != 2 {
		t.Errorf(" wrong node")
	}

	if indexOfNodes(int64(98), list, 4) != 3 {
		t.Errorf(" wrong node")
	}
}
func TestBinarySearchEmptySlice(t *testing.T) {
	result := indexOfNodes(int64(1), []*Node{}, 0)
	if result != -1 {
		t.Errorf("indexOfNodes with empty slice returned %d, want -1", result)
	}
}

func TestBinarySearchOfnEmptySlice(t *testing.T) {
	result := indexOfOfn(int64(1), []*overflowNode{}, 0)
	if result != -1 {
		t.Errorf("indexOfOfn with empty slice returned %d, want -1", result)
	}
}