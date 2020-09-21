package doclite

import (
	"encoding/json"
	"errors"
	"github.com/haibeey/doclite/doclite"
	"log"
)

var (
	//ErrNotFound error for document not found
	//export ErrNotFound
	ErrNotFound = errors.New("Not Found")
)

// Doclite holds an intance of the Database object
type Doclite struct {
	db *doclite.DB
}

//GetDB returns the doclite databse
func (d *Doclite) GetDB() *doclite.DB {
	return d.db
}

//Commit saves all current changes on the database
func (d *Doclite) Commit() {
	d.db.Save()
}

// Collection holds a collection
type Collection struct {
	tree *doclite.Btree
}

//GetCol returns the collection tree
func (c *Collection) GetCol() *doclite.Btree {
	return c.tree
}

//Connect returns an instance of Doclite object database
func Connect(filename string) *Doclite {
	db := doclite.OpenDB(filename)
	return &Doclite{db: db}
}

/*Close closes the Database
Close must be called at exit of the process interacting with doclite.
*/
func (d *Doclite) Close() error {
	return d.db.Close()
}

//Base returns the root Collection of the database
func (d *Doclite) Base() *Collection {
	return &Collection{tree: d.db.Connect()}
}

//Collection returns a sub-collection in a collection
func (c *Collection) Collection(collectionName string) *Collection {
	return &Collection{tree: c.tree.Get(collectionName)}
}

//Name returns the name of the sub collection
func (c *Collection) Name() string {
	return c.tree.Name
}

/*Insert add a new document to the  collection and returns the id of the inserted document or an error.
The id return are not binding i.e when the document is deleted another new document would take up the id.
Doc is a go struct object holding some data example

	type Employer struct {
		Name    string
		Address string
	}
	e:=&Employer{name:"joe",address:"doe"}
	collection.Insert(e)

*/
func (c *Collection) Insert(doc interface{}) (int64, error) {
	buf, err := json.Marshal(doc)
	if err != nil {
		return -1, err
	}
	return c.tree.Insert(buf), nil
}

//DeleteOne deletes a document from the database
//When document is deleted a new document take up it space and id
func (c *Collection) DeleteOne(id int64) {
	c.tree.Delete(id)
}

//Delete remove all document matching filter from the database
func (c *Collection) Delete(filter, doc interface{}) {
	c.tree.DeleteAll(filter, doc)
}

/*FindOne find a document by id matching the doc struct. Example below.

type Employer struct {
	Name    string
	Address string
}
e:=&Employer{}
collection.FindOne(e)
fmt.Println(e.name,e.address)
*/
func (c *Collection) FindOne(id int64, doc interface{}) error {
	n, err := c.tree.Find(id)
	if err != nil {
		return err
	}
	if n == nil {
		return ErrNotFound
	}
	return json.Unmarshal(n.Doc().Data(), doc)
}

/*Find returns a cursor object containing all matching document of type doc.
filter is of type struct or map. it use to select matching argument of type docs.

	type Employer struct {
		Name    string
		Address string
	}
	e=&Employer{}
	joe:=&Employer{Address:"doe"}
	cur:=baseCollection.Find(joe,e)
	for {
		emp:=cur.Next()
		if emp==nil{
			fmt.Println(emp,"is nil at",i)
			break
		}
		fmt.Println(emp)
	}
*/
func (c *Collection) Find(filter, doc interface{}) *doclite.Cursor {
	return c.tree.FindAll(filter, doc)
}

//Commit saves all current changes
func (c *Collection) Commit() {
	c.tree.Save()
}

//UpdateOneDoc is used update an existing document  by id in the database with a new document.
func (c *Collection) UpdateOneDoc(id int64, doc interface{}) error {
	buf, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return c.tree.Update(id, buf)
}

func recoverFromFailure() {
	if err := recover(); err != nil {
		log.Println("Error occured", err)
	}
}
