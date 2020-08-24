# Doclite
#### Doclite is a light-weight document base database libary much like how SQLite is for SQL so is how doclite is to NoSQL.

#### Doclite provide a nice and simple Api to persist document base data types/structures on disks.

#### Code Samples 
##### A Golang example below have a look at the example folder for other languages
  ```
   type Employer struct {
		Name    string
		Address string
	}

	db := doclite.Connect("example.doclite") // opens a new or existing database 

	baseCollection := db.Base() // get the root of the database
    // insert 20 documnets element
	for i := 0; i < 20; i++ {
	e := &Employer{Name: fmt.Sprintf("%d docklite", i), Address: "doe"}
	baseCollection.Insert(e)
	}

    // get the 14th document inserted
	e := &Employer{}
	baseCollection.FindOne(14, e)
	fmt.Println(e)

    // delete document with id 16
	baseCollection.DeleteOne(16)

	e = &Employer{}
    // get the 17 document inserted
	baseCollection.FindOne(17, e)
	fmt.Println(e)

	e=&Employer{}
	joe:=&Employer{Address:"doe"}
    // find all document matching the joe
	cur:=baseCollection.Find(joe,e)
	for {
		emp:=cur.Next()
		if emp==nil{
			break
		}
		fmt.Println(emp)
	}
	db.Close() // close the database
  ```

  #### Contributions are welcome 

  #### Shared library
  Doclite can be used with multiple programming langauges by building a shared library built on the platform specific machine using the go command ```go build -o docliteshared.so -buildmode=c-shared  docliteexport.go```.   
  docliteexport.go can be found in the sharedlib directory. An example usage for python can found in the examples directories