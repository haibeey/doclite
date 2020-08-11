package doclite

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"strings"
)

const (
	demarcationbByteString = " "
	demarcationByte        = byte(32)
)

func (c *Cache) write(n *Node) error {
	if c.db.file == nil {
		//for testing db.file shouldn't be nil
		return nil
	}
	if n.document.id == 14 {
		//fmt.Println("write",n.document.offset)
	}
	data := n.document.data
	if len(n.document.data) > dataSize {
		data = n.document.data[:dataSize]
		c.overflowDoc(n)
	} else {
		sizeFill := dataSize - len(n.document.data)
		if sizeFill > 0 {
			data = append(data, []byte(strings.Repeat(demarcationbByteString, sizeFill))...)
		}
	}

	if _, err := c.db.file.WriteAt(data, n.document.offset); err != nil {
		return err
	}
	return nil
}

func (c *Cache) read(n *Node) ([]byte, error) {
	if c.db.file == nil {
		//for testing db.file shouldn't be nil
		return []byte{}, nil
	}
	buf := make([]byte, dataSize)
	_, err := c.db.file.ReadAt(buf, n.document.offset)

	lb := dataSize
	for lb >= 1 {
		if buf[lb-1] != demarcationByte {
			break
		}
		lb--
	}
	if lb >= dataSize {
		buf = append(buf, c.getOverflowData(n)...)
	} else {
		buf = buf[:lb]
	}
	return buf, err
}

func (c *Cache) readInPlace(buf []byte, n *Node) error {
	_, err := c.db.file.ReadAt(buf, n.document.offset)

	lb := dataSize
	for lb >= 1 {
		if buf[lb-1] != demarcationByte {
			break
		}
		lb--
	}
	if lb >= dataSize {
		buf = append(buf, c.getOverflowData(n)...)
	} else {
		buf = buf[:lb]
	}
	return err
}

type overflowNode struct {
	id   int64
	data []byte
}

func (c *Cache) overflowDoc(n *Node) error {

	ofn := &overflowNode{id: n.document.id, data: n.document.data[dataSize:]}
	buf := make([]byte, 8)
	data, err := json.Marshal(ofn)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint64(buf, uint64(len(data)))
	buf = append(buf, data...)
	c.db.overflows = append(c.db.overflows, *ofn)
	return c.writeOverflowfile(buf)
}

func (c *Cache) getOverflowData(n *Node) []byte {
	for _, ofn := range c.db.overflows {
		if ofn.id == n.document.id {
			return ofn.data
		}
	}
	var (
		sizeBuf [8]byte
	)
	x := int64(0)
	c.db.overflowfile.Seek(0, os.SEEK_SET)

	for {
		// read the first 8 byte to decode the size of the overflow data
		sizeBuf, w, err := c.readOverflowfile(x, sizeBuf[:])
		if err != nil || w == 0 {
			break
		}
		x += int64(w)
		size := int64(binary.BigEndian.Uint64(sizeBuf[:])) // convert the byte to int
		buf := make([]byte, size)
		buf, w, err = c.readOverflowfile(size, buf)
		if err != nil || w == 0 {
			break
		}

		x += size
		ofn := &overflowNode{}
		json.Unmarshal(buf, ofn)
		c.db.overflows = append(c.db.overflows, *ofn)
		if n.document.id == ofn.id {
			return ofn.data
		}

		c.db.overflows = append(c.db.overflows, *ofn)
	}

	return []byte{}
}

func (c *Cache) writeOverflowfile(data []byte) error {
	_, err := c.db.overflowfile.WriteAt(data, c.db.metadata.OverflowSize)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cache) readOverflowfile(offset int64, buf []byte) ([]byte, int, error) {
	w, err := c.db.overflowfile.ReadAt(buf, offset)
	return buf, w, err
}
