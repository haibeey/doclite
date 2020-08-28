package main

import "C"

import (
	"github.com/haibeey/doclite"

	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

//variables holding shared libraries object
var (
	externalDB     *doclite.Doclite
	baseCollection *doclite.Collection
)

//ConvertToStruct converts a map to an struct passed as s
//export ConvertToStruct
func ConvertToStruct(m map[string]interface{}, s interface{}) error {
	structValue := reflect.ValueOf(s).Elem()
	for name, value := range m {
		structFieldValue := structValue.FieldByName(name)

		if !structFieldValue.IsValid() {
			return fmt.Errorf("No such field: %s in obj", name)
		}

		if !structFieldValue.CanSet() {
			return fmt.Errorf("Cannot set %s field value", name)
		}

		val := reflect.ValueOf(value)
		if structFieldValue.Type() != val.Type() {
			return fmt.Errorf("Provided value type didn't match obj field type")
		}

		structFieldValue.Set(val)
	}
	return nil
}

//ConnectDB is same as Connect only used for building shared library
//export ConnectDB
func ConnectDB(filename string) {
	externalDB = doclite.Connect(filename)
	Base()
}

//Close is same as Doclite.Close only used for building shared library
//export Close
func Close() {
	externalDB.GetDB().Close()
}

//Base is same as Doclite.Base only used for building shared library
//export Base
func Base() {
	baseCollection = externalDB.Base()
}

//Insert is same as Collection.Insert only used for building shared library
//export Insert
func Insert(doc, name string) {
	collection := getColFromName(name)
	document := make(map[string]interface{})
	err := json.Unmarshal([]byte(doc), &document)
	if err != nil {

	}
	collection.Insert(document)
}

//DeleteOne is same as Collection.DeleteOne only used for building shared library
//export DeleteOne
func DeleteOne(id int64, name string) {
	collection := getColFromName(name)
	collection.DeleteOne(id)
}

//Delete is same as Collection.Delete only used for building shared library
//export Delete
func Delete(name, filter string) {
	filterMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(filter), &filterMap)
	if err!=nil{
		return
	}
	
	doc := make(map[string]interface{})
	collection := getColFromName(name)
	collection.GetCol().DeleteAll(filterMap, doc)
}

//FindOne is same as Collection.FindOne only used for building shared library
//export FindOne
func FindOne(id int64, name string) *C.char {
	collection := getColFromName(name)
	n, err := collection.GetCol().Find(id)
	if err != nil {
		return C.CString("")
	}
	if n == nil {
		return C.CString("")
	}
	return C.CString(string(n.Doc().Data()))
}

//Find is same as Collection.Find only used for building shared library
//export Find
func Find(name, filter string) *C.char {
	filterMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(filter), &filterMap)
	if err != nil {
		return C.CString("")
	}
	doc := make(map[string]interface{})
	collection := getColFromName(name)
	cur := collection.GetCol().FindAll(filterMap, doc)
	result := make([]interface{}, 0)
	for {
		next := cur.Next()
		if next == nil {
			break
		}
		result = append(result, next)
	}
	res, err := json.Marshal(result)
	if err != nil {
		return C.CString("")
	}

	return C.CString(string(res))
}

//UpdateOneDoc is same as Collection.UpdateOneDoc only used for building shared library
//export UpdateOneDoc
func UpdateOneDoc(id int64, doc string, name string) {
	collection := getColFromName(name)
	collection.DeleteOne(id)
}

func getColFromName(name string) *doclite.Collection {
	collections := strings.Split(name, "|")
	collection := baseCollection
	if len(collections) > 1 {
		collection = baseCollection
		for _, collName := range collections[1:] {
			collection = collection.Collection(collName)
		}
	}

	return collection
}
func main() {

}
