// Package pager implements the doclite page store.
//
// A database is one file made of fixed-size pages, plus a write-ahead log
// (WAL) file next to it. Every page ends with a CRC-32C checksum. Page 0
// holds the file header.
//
// Writes are atomic and durable: a write transaction appends its changed
// pages to the WAL, then syncs the WAL. A crash before the sync loses the
// whole transaction; a crash after it loses nothing. Pages are copied from
// the WAL into the main file at a checkpoint.
//
// One write transaction runs at a time. Read transactions run at the same
// time as the writer and see a fixed snapshot. A file lock stops other
// processes from opening the database for writing at the same time.
package pager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// PgNo is a page number. Page 0 is the file header.
type PgNo uint32

const (
	// DefaultPageSize is the page size of a new database.
	DefaultPageSize = 4096
	// MinPageSize is the smallest page size that is permitted.
	MinPageSize = 1024
	// MaxPageSize is the largest page size that is permitted.
	MaxPageSize = 65536

	// MetaSize is the number of bytes in page 0 that the layer above the
	// pager can use.
	MetaSize = 64

	// TypeFree is the type byte (byte 0) of a page on the free list.
	// Layers above the pager must not use it.
	TypeFree = 0xFF

	formatVersion = 1
	trailerSize   = 4
	metaOffset    = 40
)

var (
	magic = [8]byte{'d', 'o', 'c', 'l', 'i', 't', 'e', 0}
	le    = binary.LittleEndian
	crcT  = crc32.MakeTable(crc32.Castagnoli)
)

// Errors returned by the pager.
var (
	ErrCorrupt  = errors.New("doclite: database file is corrupt")
	ErrNotDB    = errors.New("doclite: file is not a doclite database")
	ErrVersion  = errors.New("doclite: unsupported file format version")
	ErrLocked   = errors.New("doclite: database is locked by another process")
	ErrReadOnly = errors.New("doclite: database is read-only")
	ErrClosed   = errors.New("doclite: database is closed")
	ErrTxDone   = errors.New("doclite: transaction is closed")
)

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

// header is the content of page 0.
//
//	0  magic      [8]byte
//	8  version    uint32
//	12 pageSize   uint32
//	16 pageCount  uint32
//	20 freeHead   uint32
//	24 freeCount  uint32
//	28 txid       uint64
//	36 reserved   uint32
//	40 meta       [MetaSize]byte
type header struct {
	pageSize  uint32
	pageCount uint32
	freeHead  PgNo
	freeCount uint32
	txid      uint64
	meta      [MetaSize]byte
}

func (h *header) encode(b []byte) {
	copy(b[0:8], magic[:])
	le.PutUint32(b[8:], formatVersion)
	le.PutUint32(b[12:], h.pageSize)
	le.PutUint32(b[16:], h.pageCount)
	le.PutUint32(b[20:], uint32(h.freeHead))
	le.PutUint32(b[24:], h.freeCount)
	le.PutUint64(b[28:], h.txid)
	le.PutUint32(b[36:], 0)
	copy(b[metaOffset:metaOffset+MetaSize], h.meta[:])
}

func decodeHeader(b []byte) (header, error) {
	var h header
	if len(b) < metaOffset+MetaSize || [8]byte(b[0:8]) != magic {
		return h, ErrNotDB
	}
	if v := le.Uint32(b[8:]); v != formatVersion {
		return h, fmt.Errorf("%w: %d", ErrVersion, v)
	}
	h.pageSize = le.Uint32(b[12:])
	h.pageCount = le.Uint32(b[16:])
	h.freeHead = PgNo(le.Uint32(b[20:]))
	h.freeCount = le.Uint32(b[24:])
	h.txid = le.Uint64(b[28:])
	copy(h.meta[:], b[metaOffset:metaOffset+MetaSize])
	if !validPageSize(int(h.pageSize)) || h.pageCount == 0 || uint32(h.freeHead) >= h.pageCount {
		return h, corruptf("bad file header")
	}
	return h, nil
}

func validPageSize(n int) bool {
	return n >= MinPageSize && n <= MaxPageSize && n&(n-1) == 0
}

// setChecksum writes the page checksum into the last 4 bytes of b.
func setChecksum(b []byte) {
	n := len(b) - trailerSize
	le.PutUint32(b[n:], crc32.Checksum(b[:n], crcT))
}

func checksumOK(b []byte) bool {
	n := len(b) - trailerSize
	return le.Uint32(b[n:]) == crc32.Checksum(b[:n], crcT)
}

// WAL file format.
//
// The WAL starts with a 32-byte header:
//
//	0  magic    [8]byte
//	8  pageSize uint32
//	12 salt     uint32
//	16 reserved [12]byte
//	28 crc      uint32 (CRC-32C of bytes 0..28)
//
// Then frames follow. A frame is a 16-byte frame header and one page:
//
//	0  pgno     uint32
//	4  flags    uint32 (frameCommit on the last frame of a transaction)
//	8  salt     uint32 (must match the WAL header)
//	12 checksum uint32
//
// The checksum is a CRC-32C chain: it covers bytes 0..12 of the frame
// header and the page, and it starts from the checksum of the frame
// before (or the WAL header crc for the first frame). Recovery stops at
// the first frame that does not match, and keeps only the frames up to
// the last commit frame before that point.
const (
	walHeaderSize   = 32
	frameHeaderSize = 16
	frameCommit     = 1
)

var walMagic = [8]byte{'d', 'c', 'l', 'w', 'a', 'l', '0', '1'}
