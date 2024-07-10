package main

import (
	"fmt"
	"github.com/haibeey/doclite"
)

func main() {
	type Employer struct {
		Name    string
		Address string
	}

	//Add Connect to DB
	db := doclite.Connect("example.doclite")
	baseCollection := db.Base() // get base collection

	//Insert 20 new document
	for i := 0; i < 20; i++ {
		baseCollection.Insert(
			&Employer{
				Name:    fmt.Sprintf("%d docklite", i),
				Address: "20, daniel's street, Abuja",
			},
		)
	}

	e := &Employer{}
	baseCollection.FindOne(14, e)
	fmt.Println(e)
	baseCollection.DeleteOne(16)
	e = &Employer{}
	baseCollection.FindOne(16, e)
	fmt.Println(e)

	//find all document matching address==doe
	cur := baseCollection.Find(
		&Employer{Address: "20, daniel's street, Abuja"},
	)
	count := 0
	for {
		emp := cur.Next()
		if emp == nil {
			break
		}
		fmt.Println(emp)
		count++
	}
	fmt.Println("Found ", count, "documents")

	//sub collection
	type User struct {
		Name    string
		Address string
		Friends []User
	}

	//create a new collection from
	userCollection := baseCollection.Collection("user")
	//Insert 20 new document
	for i := 0; i < 20; i++ {
		userCollection.Insert(
			&User{
				Name:    fmt.Sprintf("%d docklite %s", i, "user"),
				Address: "20, daniel's street, Abuja",
				Friends: []User{
					{Name: fmt.Sprintf("%d testing %s", i, "user")},
				},
			},
		)
	}

	u := &User{}
	userCollection.FindOne(14, u)
	userCollection.DeleteOne(16)
	u = &User{}
	userCollection.FindOne(16, u)
	doe := &User{Address: "20, daniel's street, Abuja"}
	cur = userCollection.Find(doe)
	count = 0
	for {
		a := cur.NextObject(u)
		if a == nil {
			break
		}
		count++
		fmt.Println(a)
	}
	fmt.Println("Found ", count, "documents")

	db.Close()
}
