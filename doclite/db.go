package doclite

import (
	"fmt"
	"os"
	"sync"

	"encoding/json"
	"gopkg.in/mgo.v2/bson"
)

const (
	metaDataLen    = 175
	magicString    = "646f636c697465"
	magicStringLen = 14
	// dataSize is maximum amount of byte to be stored on a Node.
	// Excess byte would be stored in overflow pages
	dataSize = 2048
	pageSize = 65536
)

// DB represents our database file
type DB struct {
	file         *os.File
	overflowfile *os.File
	metadata     *Meta
	rootTree     *Btree

	overflows map[string][]*overflowNode
}

/*Meta represent the database file metadata
  magic string is used to idenitify the database file
  number of collection in this database
  offset overflow bytes offset
  number of overflow doc
  number of overflow data
  offset of the root btree
*/
type Meta struct {
	MagicString        []byte
	NofCollections     int64 // add mutex
	nOCMutex           sync.Mutex
	OverflowSize       int64
	OverflowDataOffset int64
	RootTreeOffset     int64
	RootTreeSize       int64
	NumPages           int64 // add mutex
	nPMutex            sync.Mutex
}

func (m *Meta) incCollections() {
	m.nOCMutex.Lock()
	m.NofCollections++
	m.nOCMutex.Unlock()
}

func (m *Meta) String() string {
	return fmt.Sprintf("%d %d %d %d %d", m.NofCollections, m.OverflowSize, m.OverflowDataOffset, m.RootTreeOffset, m.NumPages)
}
func (m *Meta) incPages() {
	m.nPMutex.Lock()
	m.NumPages++
	m.nPMutex.Unlock()
}

func openFile(fileName string, flag int) *os.File {
	f, err := os.OpenFile(fileName, flag, 0755)
	if err != nil {
		panic(fmt.Sprintf("%v\n", err))
	}
	return f
}

// OpenDB instantiate our database
func OpenDB(fileName string) *DB {
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		f := openFile(fileName, os.O_RDWR|os.O_CREATE)
		db := &DB{file: f, overflows: make(map[string][]*overflowNode)}
		db.metadata = &Meta{
			MagicString:        []byte(magicString),
			NofCollections:     1,
			OverflowDataOffset: metaDataLen,
			RootTreeOffset:     metaDataLen,
		}

		db.rootTree = db.newBtree("")
		db.moveOverflow()
		return db
	}
	os.Remove(fmt.Sprintf("%s.overflow", fileName))
	db := &DB{file: openFile(fileName, os.O_RDWR), overflows: make(map[string][]*overflowNode)}
	db.getMeta()
	db.moveOverflow()
	t, err := db.initBtree()
	if err != nil {
		fmt.Println("database might have been corrupted")
	}
	db.rootTree = t
	return db
}

// Connect start a connection to this data base for insertion and deletion
func (db *DB) Connect() *Btree {
	return db.rootTree
}

//NewBtree returns a new Btree
func (db *DB) newBtree(name string) *Btree {
	return &Btree{
		db:             db,
		SubCollections: make(map[string]*Btree),
		findPool:       make(map[int64]int64),
		Pages:          []int64{},
		Name:           name,
		initBtreeRoot:  true,
	}
}

func (db *DB) initBtree() (*Btree, error) {

	buf := make([]byte, db.metadata.RootTreeSize)
	tree := &Btree{db: db, findPool: make(map[int64]int64)}
	db.file.Seek(db.metadata.RootTreeOffset, os.SEEK_SET)

	_, err := db.file.Read(buf)

	if err != nil {
		return tree, err
	}
	err = json.Unmarshal(buf, tree)
	if err != nil {
		fmt.Println("an error occured while initilizing the db",err)
	}

	tree.diskInitBtree()

	return tree, err
}

func (db *DB) getMeta() *Meta {
	buf := make([]byte, metaDataLen)
	db.file.Seek(0, os.SEEK_SET)
	db.file.Read(buf)
	db.metadata = &Meta{}
	bson.Unmarshal(buf[:], db.metadata)
	return db.metadata
}

func (db *DB) moveOverflow() error {
	db.overflowfile = openFile(fmt.Sprintf("%s.overflow", db.file.Name()), os.O_RDWR|os.O_CREATE)

	db.file.Seek(db.metadata.OverflowDataOffset, os.SEEK_SET)
	//TODO read in chucks
	buf := make([]byte, db.metadata.OverflowSize)
	_, err := db.file.Read(buf)
	if err != nil {
		return err
	}
	_, err = db.overflowfile.WriteAt(buf, 0)

	return err
}

func (db *DB) bringBackOverflow() error {
	db.overflowfile = openFile(fmt.Sprintf("%s.overflow", db.file.Name()), os.O_RDWR|os.O_CREATE)
	stat, err := db.overflowfile.Stat()
	if err != nil {
		return err
	}
	db.metadata.OverflowSize = stat.Size()

	buf := make([]byte, db.metadata.OverflowSize)
	//TODO read in chucks
	_, err = db.overflowfile.Read(buf)
	if err != nil {
		return err
	}
	_, err = db.file.WriteAt(buf, db.metadata.OverflowDataOffset)

	db.metadata.RootTreeOffset = db.metadata.OverflowDataOffset + db.metadata.OverflowSize
	os.Remove(fmt.Sprintf("%s.overflow", db.file.Name()))

	return err
}

func (db *DB) getOverflow(name string) []*overflowNode {
	overflow, ok := db.overflows[name]
	if !ok {
		db.overflows[name] = []*overflowNode{}
		return db.overflows[name]
	}
	return overflow
}

//Close closes the database must be called before at exit
func (db *DB) Close() error {
	db.rootTree.Save()

	err := db.bringBackOverflow()

	data, err := json.Marshal(db.rootTree)
	db.metadata.RootTreeSize = int64(len(data))

	if err != nil {
		return err
	}

	_, err = db.file.WriteAt(data, db.metadata.RootTreeOffset)
	if err != nil {
		return err
	}

	data, err = bson.Marshal(db.metadata)

	if err != nil {
		return err
	}
	_, err = db.file.WriteAt(data, 0)
	return err
}

//Save saves all current changes on the database 
func (db *DB)Save() error {
	db.rootTree.Save()

	data, err := json.Marshal(db.rootTree)
	db.metadata.RootTreeSize = int64(len(data))

	if err != nil {
		return err
	}

	_, err = db.file.WriteAt(data, db.metadata.RootTreeOffset)
	if err != nil {
		return err
	}

	data, err = bson.Marshal(db.metadata)

	if err != nil {
		return err
	}
	_, err = db.file.WriteAt(data, 0)
	return err
}
