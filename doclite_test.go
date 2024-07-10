package doclite

import (
	"encoding/json"
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

func findAll(col *Collection, t *testing.T, expectedCount int, address string) {
	e := &Employer{}
	joe := &Employer{Address: address}
	cur := col.Find(joe)
	count := 0
	eeTest := &Employer{}
	for {
		emp := cur.Next()
		if emp == nil {
			break
		}

		b, err := json.Marshal(emp)
		if err != nil {
			t.Errorf("error while parsing json %s", err)
		}
		ee := &Employer{}
		err = json.Unmarshal(b, ee)

		if err != nil {
			t.Errorf("error while parsing json %s", err)
		}
		if eeTest.Name == ee.Name {
			t.Errorf("error: db is returing same values %v %v", e, ee)
		}

		eeTest.Name = ee.Name
		count++
	}
	if count != expectedCount {
		t.Errorf("%d doesn't match expected count of %d %s", count, expectedCount, address)
	}
}

func testCollection(col *Collection, t *testing.T) {
	//insert 20 elements
	for i := 0; i < 20; i++ {
		address := "doe"
		if i >= 10 {
			address = fmt.Sprintf("%d doe", i+1)
		}
		e := &Employer{Name: fmt.Sprintf("%d docklite", i), Address: address}
		col.Insert(e)
	}

	e := &Employer{}
	// find one with id 14 doc matching e
	col.FindOne(14, e)
	if e.Address != "14 doe" {
		t.Errorf("failed to find document with id %d", 14)
	}

	e = &Employer{}
	col.FindOne(17, e)
	// find one document with id 17 doc matching e
	if e.Address != "17 doe" {
		t.Errorf("failed to find document with id %d", 17)
	}
	//find all element in
	findAll(col, t, 10, "doe")
	e = &Employer{}
	//delete document with id 16
	col.DeleteOne(6)
	//find the just deleted element
	col.FindOne(6, e)
	if e.Address != "" {
		t.Errorf("document %v not deleted", e)
	}
	// find all document but exepecting number of document to be 16
	// since we deleted one element already
	findAll(col, t, 9, "doe")

	for i := 11; i <= 20; i++ {
		findAll(col, t, 1, fmt.Sprintf("%d doe", i))
	}

	col.Delete(&Employer{Address: "doe"}, &Employer{})

	// find all document but exepecting number of document to be 0
	// since we deleted one element already
	findAll(col, t, 0, "doe")
}
