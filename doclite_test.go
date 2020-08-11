package doclite

import (
	"fmt"
	"os"
	"testing"
)

type Employer struct {
	Name    string
	Address string
}

func TestMain(t *testing.T) {
	db := Connect("doclitetest.doclite")
	baseCollection := db.Base()
	testCollection(baseCollection, t)
	col := baseCollection.Collection("sub")
	testCollection(col, t)
	db.Close()
	os.Remove("doclitetest.doclite")

}

func findAll(col *Collection, t *testing.T, expectedCount int) {
	e := &Employer{}
	joe := &Employer{Address: "doe"}
	cur := col.Find(joe, e)
	count := 0
	for {
		emp := cur.Next()
		if emp == nil {
			break
		}
		count++
	}
	if count != expectedCount {
		t.Errorf("%d doesn't match expected count of %d", count, expectedCount)
	}
}

func testCollection(col *Collection, t *testing.T) {
	//insert 20 elements
	for i := 0; i < 20; i++ {
		e := &Employer{Name: fmt.Sprintf("%d docklite", i), Address: "doe"}
		col.Insert(e)
	}

	e := &Employer{}
	// find one with id 14 doc matching e
	col.FindOne(14, e)
	if e.Address != "doe" {
		t.Errorf("failed to find document with id %d", 14)
	}

	e = &Employer{}
	col.FindOne(17, e)
	// find one document with id 17 doc matching e
	if e.Address != "doe" {
		t.Errorf("failed to find document with id %d", 17)
	}
	//find all element in
	findAll(col, t, 20)
	e = &Employer{}
	//delete document with id 16
	col.DeleteOne(16)
	//find the just deleted element
	col.FindOne(16, e)
	if e.Address != "" {
		t.Errorf("document %v not deleted", e)
	}
	// find all document but exepecting number of document to be 16
	// since we deleted one element already
	findAll(col, t, 19)

	col.Delete(&Employer{Address: "doe"}, &Employer{})

	// find all document but exepecting number of document to be 0
	// since we deleted one element already
	findAll(col, t, 0)
}
