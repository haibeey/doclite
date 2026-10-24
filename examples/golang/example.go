package main

import (
	"fmt"
	"log"
	"os"

	"github.com/haibeey/doclite"
)

type Employee struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	Age     int    `json:"age"`
}

func main() {
	path := "example.doclite"
	defer os.Remove(path)

	db, err := doclite.Open(path, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	employees := db.Collection("employees")
	if err := employees.CreateIndex("age"); err != nil {
		log.Fatal(err)
	}

	// Insert 20 documents in one transaction.
	var docs []Employee
	for i := 0; i < 20; i++ {
		docs = append(docs, Employee{Name: fmt.Sprintf("employee %d", i), Address: "20, Daniel's street, Abuja", Age: 20 + i})
	}
	if _, err := employees.InsertMany(docs); err != nil {
		log.Fatal(err)
	}

	var e Employee
	if err := employees.FindOne(14, &e); err != nil {
		log.Fatal(err)
	}
	fmt.Println("document 14:", e)

	if err := employees.DeleteOne(16); err != nil {
		log.Fatal(err)
	}
	if err := employees.FindOne(16, &e); err == doclite.ErrNotFound {
		fmt.Println("document 16 is deleted")
	}

	// Find with a cursor. The age index is used for the range.
	cur, err := employees.Find(doclite.M{"age": doclite.M{"$gte": 30, "$lt": 35}})
	if err != nil {
		log.Fatal(err)
	}
	defer cur.Close()
	for cur.Next() {
		if err := cur.Decode(&e); err != nil {
			log.Fatal(err)
		}
		fmt.Println(cur.ID(), e)
	}
	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

	// Several changes in one atomic transaction.
	err = db.Update(func(tx *doclite.Tx) error {
		c := tx.Collection("employees")
		if err := c.UpdateOne(1, Employee{Name: "renamed", Age: 99}); err != nil {
			return err
		}
		_, err := c.Collection("notes").Insert(doclite.M{"text": "a sub-collection"})
		return err
	})
	if err != nil {
		log.Fatal(err)
	}

	n, _ := employees.Count(nil)
	fmt.Println("employees:", n)
}
