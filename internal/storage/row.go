package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Row tuples (docs/design/15-row-versioning.md section 2.1). Every heap
// tuple starts with a flags byte; the kinds are:
//
//	plain row:       u8 Flags=0, u8 reserved, u64 XID, u64 Undo, row
//	tombstone:       u8 Flags=Deleted, u8 reserved, u64 XID, u64 Undo
//	forward stub:    u8 Flags=Forward, u8 reserved, u64 page, u16 slot,
//	                 6 bytes reserved (a stub is as big as a tombstone, so
//	                 deleting a moved row never needs more room)
//	moved-in tuple:  u8 Flags=MovedIn, u8 reserved, u64 XID, u64 Undo,
//	                 u64 home page, u16 home slot, row
const (
	flagDeleted = 1 << 0
	flagForward = 1 << 1
	flagMovedIn = 1 << 2

	// RowHeaderSize is the size of a row's header: flags, reserved, XID
	// and undo pointer.
	RowHeaderSize = 18
	stubSize      = RowHeaderSize
	movedSize     = RowHeaderSize + 10 // header and home RID

	// MaxRowData is the largest encoded row a heap stores, so that every
	// version fits an undo record (section 2.6).
	MaxRowData = 8000
)

// Errors of row tuples.
var (
	// ErrRowDeleted means the row was deleted: its slot holds a tombstone.
	ErrRowDeleted = errors.New("storage: row deleted")
	// ErrInvalidVersion means a Stamper returned a header that cannot be
	// written (transaction ID 0).
	ErrInvalidVersion = errors.New("storage: invalid row version")
)

// RowHeader is a row version's header: the transaction that wrote it and
// the undo pointer to the version before it (0: none).
type RowHeader struct {
	XID  uint64
	Undo uint64
}

// Version is one version of a row: its header and its encoded data. A
// deleted row's version (a tombstone, from GetVersion or ScanVersions) has
// Deleted set, the deleter's header and no data.
type Version struct {
	RowHeader
	Data    []byte
	Deleted bool
}

// Stamper gives a write its header. It is called once per write, while the
// row's pages are latched and before anything changes, with the row's home
// RID and the version the write replaces (nil for an insert). An error
// leaves the heap unchanged and is returned by the write.
type Stamper func(rid RID, old *Version) (RowHeader, error)

type tupleKind uint8

const (
	tuplePlain tupleKind = iota
	tupleTombstone
	tupleStub
	tupleMoved
)

// rowTuple is a decoded heap tuple. data aliases the bytes it was decoded from.
type rowTuple struct {
	kind tupleKind
	hdr  RowHeader
	link RID // stub: the moved-in tuple; moved-in tuple: its home
	data []byte
}

func plainTuple(h RowHeader, data []byte) rowTuple {
	return rowTuple{kind: tuplePlain, hdr: h, data: data}
}

func (t rowTuple) version() Version {
	return Version{RowHeader: t.hdr, Data: t.data, Deleted: t.kind == tupleTombstone}
}

// encode returns the tuple's bytes.
func (t rowTuple) encode() []byte {
	var flags byte
	switch t.kind {
	case tupleTombstone:
		flags = flagDeleted
	case tupleStub:
		flags = flagForward
	case tupleMoved:
		flags = flagMovedIn
	}
	out := []byte{flags, 0}
	if t.kind == tupleStub {
		out = binary.LittleEndian.AppendUint64(out, t.link.Page)
		out = binary.LittleEndian.AppendUint16(out, t.link.Slot)
		return append(out, make([]byte, stubSize-12)...)
	}
	out = binary.LittleEndian.AppendUint64(out, t.hdr.XID)
	out = binary.LittleEndian.AppendUint64(out, t.hdr.Undo)
	if t.kind == tupleMoved {
		out = binary.LittleEndian.AppendUint64(out, t.link.Page)
		out = binary.LittleEndian.AppendUint16(out, t.link.Slot)
	}
	return append(out, t.data...)
}

// size returns the tuple's encoded size.
func (t rowTuple) size() int {
	switch t.kind {
	case tupleStub:
		return stubSize
	case tupleMoved:
		return movedSize + len(t.data)
	}
	return RowHeaderSize + len(t.data)
}

// decodeTuple decodes and validates a heap tuple. It never panics.
func decodeTuple(b []byte) (rowTuple, error) {
	bad := func(format string, args ...any) (rowTuple, error) {
		return rowTuple{}, fmt.Errorf("row tuple: %s: %w", fmt.Sprintf(format, args...), ErrCorruptHeap)
	}
	if len(b) < 2 {
		return bad("%d bytes", len(b))
	}
	if b[1] != 0 {
		return bad("reserved byte set")
	}
	var t rowTuple
	switch b[0] {
	case 0:
		t.kind = tuplePlain
	case flagDeleted:
		t.kind = tupleTombstone
	case flagForward:
		t.kind = tupleStub
	case flagMovedIn:
		t.kind = tupleMoved
	default:
		return bad("flags %#x", b[0])
	}
	if t.kind == tupleStub {
		if len(b) != stubSize {
			return bad("forward stub of %d bytes", len(b))
		}
		t.link = RID{Page: binary.LittleEndian.Uint64(b[2:]), Slot: binary.LittleEndian.Uint16(b[10:])}
		if t.link.Page < FirstDataPage {
			return bad("forward stub to page %d", t.link.Page)
		}
		for _, c := range b[12:] {
			if c != 0 {
				return bad("forward stub with reserved bytes set")
			}
		}
		return t, nil
	}
	if len(b) < RowHeaderSize {
		return bad("%d bytes, shorter than a header", len(b))
	}
	t.hdr = RowHeader{XID: binary.LittleEndian.Uint64(b[2:]), Undo: binary.LittleEndian.Uint64(b[10:])}
	if t.hdr.XID == 0 {
		return bad("transaction ID 0")
	}
	rest := b[RowHeaderSize:]
	switch t.kind {
	case tupleTombstone:
		if len(rest) != 0 {
			return bad("tombstone with %d bytes of data", len(rest))
		}
	case tupleMoved:
		if len(rest) < movedSize-RowHeaderSize {
			return bad("moved-in rowTuple of %d bytes", len(b))
		}
		t.link = RID{Page: binary.LittleEndian.Uint64(rest), Slot: binary.LittleEndian.Uint16(rest[8:])}
		if t.link.Page < FirstDataPage {
			return bad("moved-in rowTuple from page %d", t.link.Page)
		}
		rest = rest[movedSize-RowHeaderSize:]
	}
	if len(rest) > MaxRowData {
		return bad("row of %d bytes", len(rest))
	}
	t.data = rest[:len(rest):len(rest)]
	return t, nil
}

// checkRowData checks a row's size before it is written.
func checkRowData(data []byte) error {
	if len(data) > MaxRowData {
		return fmt.Errorf("row of %d bytes (max %d): %w", len(data), MaxRowData, ErrTupleTooLarge)
	}
	return nil
}

// checkStamp checks a header a Stamper returned.
func checkStamp(h RowHeader) error {
	if h.XID == 0 {
		return fmt.Errorf("writing a row with transaction ID 0: %w", ErrInvalidVersion)
	}
	return nil
}
