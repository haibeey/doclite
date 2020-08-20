package doclite

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"sync"
)

const (
	demarcationbByteString = " "
	demarcationByte        = byte(32)
)

var readWriteMutex sync.Mutex

func (c *Cache) write(n *Node) error {
	if c.db.file == nil {
		//for testing db.file shouldn't be nil
		return nil
	}

	data := n.document.data
	if reflect.DeepEqual(data, []byte("deleted")) {
		ofn := c.getOverflowData(n)
		lenOfnData := len(ofn.Data)
		if lenOfnData > 0 {
			d, err := json.Marshal(ofn)
			if err == nil {
				c.cutOverflowfile(ofn.Offset, ofn.Offset+int64(len(d)+8))
			}
		}
	}
	if len(n.document.data) > dataSize {
		data = n.document.data[:dataSize]
		c.overflowDoc(n)
	} else {
		sizeFill := dataSize - len(n.document.data)
		if sizeFill > 0 {
			data = append(data, []byte(strings.Repeat(demarcationbByteString, sizeFill))...)
		}
	}
	if err := write(c.db.file, n.document.offset, data, true); err != nil {
		return err
	}
	return nil
}

func (c *Cache) read(n *Node) ([]byte, error) {
	if c.db.file == nil {
		return []byte{}, nil
	}
	buf := make([]byte, dataSize)
	_, err := read(c.db.file, n.document.offset, buf, true)
	lb := dataSize
	for lb >= 1 {

		if buf[lb-1] != demarcationByte {
			break
		}
		lb--
	}
	if lb == dataSize {
		buf = append(buf, c.getOverflowData(n).Data...)
	} else {
		buf = buf[:lb]
	}
	return buf, err
}

type overflowNode struct {
	ID     int64
	Data   []byte
	Offset int64
	BtName string
}

func (c *Cache) overflowDoc(n *Node) error {
	ofn := &overflowNode{
		ID:     n.document.id,
		Data:   n.document.data[dataSize:],
		Offset: c.db.metadata.OverflowSize,
	}
	buf := make([]byte, 8)
	data, err := json.Marshal(ofn)
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint64(buf, uint64(len(data)))
	buf = append(buf, data...)

	err = c.writeOverflowfile(buf)
	c.db.metadata.OverflowSize += int64(len(buf))
	if !c.db.isTesting {
		c.insertOfn(ofn)
	}
	return err
}

func (c *Cache) insertOfn(ofn *overflowNode) {
	nodes:=c.db.getOverflow(c.tree.Name)
	mid := indexOfOfn(ofn.ID, nodes, c.tree.lenOverflow)
	if mid < c.tree.lenOverflow {
		if nodes[mid].ID == ofn.ID {
			return
		}
	}
	nodes = append(nodes, nil)
	copy(nodes[mid+1:], nodes[mid:])
	nodes[mid] = ofn
	c.tree.lenOverflow++
	c.db.overflows[c.tree.Name]=nodes
}

func (c *Cache) getOverflowData(n *Node) *overflowNode {
	nodes:=c.db.getOverflow(c.tree.Name)
	mid := indexOfOfn(n.document.id, nodes, c.tree.lenOverflow)
	if mid < c.tree.lenOverflow {
		if nodes[mid].ID == n.document.id {
			return nodes[mid]
		}
	}
	var (
		sizeBuf [8]byte
	)
	x := int64(0)
	for {
		// read the first 8 byte to decode the size of the overflow data
		w, err := c.readOverflowfile(x, sizeBuf[:])
		if err != nil || w == 0 {
			break
		}

		x += int64(w)
		size := int64(binary.BigEndian.Uint64(sizeBuf[:])) // convert the byte to int
		buf := make([]byte, size)
		w, err = c.readOverflowfile(x, buf)
		if err != nil || w == 0 {
			break
		}
		x += size
		ofn := &overflowNode{}
		json.Unmarshal(buf, ofn)
		c.insertOfn(ofn)
		if n.document.id == ofn.ID {
			return ofn
		}
	}

	return &overflowNode{}
}
func (c *Cache) cutOverflowfile(start, end int64) {
	c.db.overflowfile.Seek(end, os.SEEK_SET)
	readWriteMutex.Lock()
	defer readWriteMutex.Unlock()
	buf := make([]byte, 1000)
	for {
		r, err := read(c.db.overflowfile, end, buf, false)
		write(c.db.overflowfile, start, buf[:r], false)
		if err != nil || r == 0 {
			c.db.overflowfile.Truncate(end)
			return
		}
		end += int64(r)
		start += int64(r)
	}

}

func (c *Cache) writeOverflowfile(data []byte) error {
	return write(c.db.overflowfile, c.db.metadata.OverflowSize, data, true)
}

func (c *Cache) readOverflowfile(offset int64, buf []byte) (int, error) {
	return read(c.db.overflowfile, offset, buf, true)
}
