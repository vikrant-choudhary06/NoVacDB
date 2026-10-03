package undo

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
)

// Errors of the undo log.
var (
	// ErrCorrupt means an undo page, record, pointer or log record is
	// damaged or inconsistent.
	ErrCorrupt = errors.New("undo: corrupt undo log")
	// ErrInvalidRecord means Append was given a record that cannot be
	// written: a bad kind, image or pointer.
	ErrInvalidRecord = errors.New("undo: invalid record")
	// ErrImageTooLarge means a record's image is over MaxImage bytes.
	ErrImageTooLarge = errors.New("undo: row image too large")
	// ErrNoSegment means the transaction has no undo segment.
	ErrNoSegment = errors.New("undo: no undo segment for the transaction")
)

// Ptr is an undo pointer (docs/design/14-undo-log.md section 2.2): the byte
// address of a record in the data file, page ID × 8192 + offset. 0 means
// none.
type Ptr uint64

// MakePtr returns the pointer to the record at off in page.
func MakePtr(page uint64, off int) Ptr { return Ptr(page*storage.PageSize + uint64(off)) }

// Page returns the page the pointer names.
func (p Ptr) Page() uint64 { return uint64(p) / storage.PageSize }

// Offset returns the record's offset in its page.
func (p Ptr) Offset() int { return int(uint64(p) % storage.PageSize) }

// String formats the pointer like "(page,offset)".
func (p Ptr) String() string { return fmt.Sprintf("(%d,%d)", p.Page(), p.Offset()) }

// valid reports whether p is 0 or can name a record: a data page, at or
// after the first record's offset, with room for a record header.
func (p Ptr) valid() bool {
	return p == 0 || (p.Page() >= storage.FirstDataPage && p.Offset() >= pageHeaderSize && p.Offset() <= storage.PageSize-RecordHeaderSize)
}

// Kind is the operation an undo record undoes.
type Kind uint8

// Record kinds.
const (
	// Insert: the row did not exist before; the record has no image.
	Insert Kind = 1
	// Update: the row was changed; the image is the row before.
	Update Kind = 2
	// Delete: the row was deleted; the image is the row before.
	Delete Kind = 3
)

func (k Kind) String() string {
	switch k {
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

// Record is one undo record (section 2.5).
type Record struct {
	Kind Kind
	// XID is the transaction that made the change (a wal.XID).
	XID uint64
	// PrevForRow is the row's previous undo record, 0 if none.
	PrevForRow Ptr
	// PrevInTxn is the transaction's previous record, 0 if this is its
	// first. Append sets it.
	PrevInTxn Ptr
	// Table is the catalog table ID.
	Table uint64
	RID   storage.RID
	// Image is the row before the change; empty for Insert.
	Image []byte
}

// Undo page layout (section 3.1), little-endian, after the 24-byte page
// header:
//
//	offset  size  field
//	24      8     XID       the owning transaction
//	32      8     NextPage  next page of the segment, 0 = last
//	40      2     Used      end of the records; 48 when empty
//	42      6     reserved  zero
//	48      ...   records, back to back
const (
	offPageXID     = storage.HeaderSize
	offPageNext    = storage.HeaderSize + 8
	offPageUsed    = storage.HeaderSize + 16
	offPageReserve = storage.HeaderSize + 18
	pageHeaderSize = storage.HeaderSize + 24
)

// Undo record layout (section 3.2), little-endian:
//
//	offset  size  field
//	0       2     Length       of the whole record, header included
//	2       1     Kind
//	3       1     Flags        0
//	4       4     reserved     zero
//	8       8     XID
//	16      8     PrevForRow
//	24      8     PrevInTxn
//	32      8     Table
//	40      8     RID page
//	48      2     RID slot
//	50      4     ImageLength
//	54      n     Image
const (
	recOffLength   = 0
	recOffKind     = 2
	recOffFlags    = 3
	recOffReserved = 4
	recOffXID      = 8
	recOffPrevRow  = 16
	recOffPrevTxn  = 24
	recOffTable    = 32
	recOffRIDPage  = 40
	recOffRIDSlot  = 48
	recOffImageLen = 50
	// RecordHeaderSize is the size of an undo record without its image.
	RecordHeaderSize = 54
)

// MaxImage is the largest row image a record holds: the record must fit in
// one page (section 2.6).
const MaxImage = storage.PageSize - pageHeaderSize - RecordHeaderSize

// check validates rec's fields as Append and decoding require them,
// returning a description of the first problem.
func (rec *Record) check() (string, bool) {
	switch {
	case rec.Kind < Insert || rec.Kind > Delete:
		return fmt.Sprintf("kind %d", rec.Kind), false
	case rec.XID == 0:
		return "transaction ID 0", false
	case len(rec.Image) > MaxImage:
		return fmt.Sprintf("image of %d bytes", len(rec.Image)), false
	case rec.Kind == Insert && len(rec.Image) != 0:
		return "insert with an image", false
	case rec.Kind != Insert && len(rec.Image) == 0:
		return fmt.Sprintf("%s without an image", rec.Kind), false
	case !rec.PrevForRow.valid():
		return fmt.Sprintf("previous record of the row %s", rec.PrevForRow), false
	case !rec.PrevInTxn.valid():
		return fmt.Sprintf("previous record of the transaction %s", rec.PrevInTxn), false
	case rec.RID.Page < storage.FirstDataPage:
		return fmt.Sprintf("row ID %s", rec.RID), false
	}
	return "", true
}

// size returns the encoded size of rec.
func (rec *Record) size() int { return RecordHeaderSize + len(rec.Image) }

// encodeRecord appends rec's encoding to dst.
func encodeRecord(dst []byte, rec *Record) []byte {
	var h [RecordHeaderSize]byte
	binary.LittleEndian.PutUint16(h[recOffLength:], uint16(rec.size()))
	h[recOffKind] = byte(rec.Kind)
	binary.LittleEndian.PutUint64(h[recOffXID:], rec.XID)
	binary.LittleEndian.PutUint64(h[recOffPrevRow:], uint64(rec.PrevForRow))
	binary.LittleEndian.PutUint64(h[recOffPrevTxn:], uint64(rec.PrevInTxn))
	binary.LittleEndian.PutUint64(h[recOffTable:], rec.Table)
	binary.LittleEndian.PutUint64(h[recOffRIDPage:], rec.RID.Page)
	binary.LittleEndian.PutUint16(h[recOffRIDSlot:], rec.RID.Slot)
	binary.LittleEndian.PutUint32(h[recOffImageLen:], uint32(len(rec.Image)))
	dst = append(dst, h[:]...)
	return append(dst, rec.Image...)
}

// EncodeRecord returns rec's encoding. It does not validate rec.
func EncodeRecord(rec Record) []byte { return encodeRecord(nil, &rec) }

// DecodeRecord decodes and validates the record at the start of b, which
// may hold more bytes after it. It returns the record, whose Image aliases
// b, and its length. It never panics on any input.
func DecodeRecord(b []byte) (Record, int, error) {
	bad := func(format string, args ...any) (Record, int, error) {
		return Record{}, 0, fmt.Errorf("undo record: %s: %w", fmt.Sprintf(format, args...), ErrCorrupt)
	}
	if len(b) < RecordHeaderSize {
		return bad("%d bytes, shorter than a header", len(b))
	}
	n := int(binary.LittleEndian.Uint16(b[recOffLength:]))
	imageLen := binary.LittleEndian.Uint32(b[recOffImageLen:])
	// The image's size limit is checked with the other fields, below.
	if n != RecordHeaderSize+int(imageLen) {
		return bad("length %d with an image of %d bytes", n, imageLen)
	}
	if n > len(b) {
		return bad("length %d past the %d bytes available", n, len(b))
	}
	if b[recOffFlags] != 0 || binary.LittleEndian.Uint32(b[recOffReserved:]) != 0 {
		return bad("flags or reserved bytes set")
	}
	rec := Record{
		Kind:       Kind(b[recOffKind]),
		XID:        binary.LittleEndian.Uint64(b[recOffXID:]),
		PrevForRow: Ptr(binary.LittleEndian.Uint64(b[recOffPrevRow:])),
		PrevInTxn:  Ptr(binary.LittleEndian.Uint64(b[recOffPrevTxn:])),
		Table:      binary.LittleEndian.Uint64(b[recOffTable:]),
		RID: storage.RID{
			Page: binary.LittleEndian.Uint64(b[recOffRIDPage:]),
			Slot: binary.LittleEndian.Uint16(b[recOffRIDSlot:]),
		},
		Image: b[RecordHeaderSize:n:n],
	}
	if msg, ok := rec.check(); !ok {
		return bad("%s", msg)
	}
	return rec, n, nil
}

// pageHeader is the undo-specific part of an undo page's header.
type pageHeader struct {
	xid  uint64
	next uint64
	used int
}

// initPage makes buf, a page of the given ID, an empty undo page of xid.
func initPage(buf []byte, id, xid uint64) error {
	if err := storage.InitPage(buf, storage.Header{ID: id, Type: storage.PageTypeUndo}); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(buf[offPageXID:], xid)
	binary.LittleEndian.PutUint16(buf[offPageUsed:], pageHeaderSize)
	return nil
}

func setNext(buf []byte, next uint64) { binary.LittleEndian.PutUint64(buf[offPageNext:], next) }

func setUsed(buf []byte, used int) { binary.LittleEndian.PutUint16(buf[offPageUsed:], uint16(used)) }

// decodePageHeader reads and validates the header of the undo page id. It
// does not look at the records.
func decodePageHeader(buf []byte, id uint64) (pageHeader, error) {
	bad := func(format string, args ...any) (pageHeader, error) {
		return pageHeader{}, fmt.Errorf("undo page %d: %s: %w", id, fmt.Sprintf(format, args...), ErrCorrupt)
	}
	h, err := storage.DecodeHeader(buf)
	if err != nil {
		return bad("%v", err)
	}
	if h.Type != storage.PageTypeUndo || h.ID != id || h.Flags != 0 {
		return bad("page %d of type %d, flags %#x", h.ID, h.Type, h.Flags)
	}
	ph := pageHeader{
		xid:  binary.LittleEndian.Uint64(buf[offPageXID:]),
		next: binary.LittleEndian.Uint64(buf[offPageNext:]),
		used: int(binary.LittleEndian.Uint16(buf[offPageUsed:])),
	}
	for _, c := range buf[offPageReserve:pageHeaderSize] {
		if c != 0 {
			return bad("reserved header bytes set")
		}
	}
	switch {
	case ph.xid == 0:
		return bad("transaction ID 0")
	case ph.next != 0 && (ph.next < storage.FirstDataPage || ph.next == id):
		return bad("next page %d", ph.next)
	case ph.used < pageHeaderSize || ph.used > storage.PageSize:
		return bad("used %d", ph.used)
	}
	return ph, nil
}

// pageRecords validates the undo page id and calls fn, if not nil, with
// the offset of each record, in order. Every record must decode, belong to
// the page's transaction and end exactly at Used; a page holds at least
// one record (pages are created with their first record).
func pageRecords(buf []byte, id uint64, fn func(off int, rec Record)) (pageHeader, error) {
	ph, err := decodePageHeader(buf, id)
	if err != nil {
		return ph, err
	}
	if ph.used == pageHeaderSize {
		return ph, fmt.Errorf("undo page %d holds no records: %w", id, ErrCorrupt)
	}
	for off := pageHeaderSize; off < ph.used; {
		rec, n, err := DecodeRecord(buf[off:ph.used])
		if err != nil {
			return ph, fmt.Errorf("undo page %d, offset %d: %w", id, off, err)
		}
		if rec.XID != ph.xid {
			return ph, fmt.Errorf("undo page %d of transaction %d holds a record of %d at %d: %w", id, ph.xid, rec.XID, off, ErrCorrupt)
		}
		if fn != nil {
			fn(off, rec)
		}
		off += n
	}
	for _, c := range buf[ph.used:] {
		if c != 0 {
			return ph, fmt.Errorf("undo page %d: bytes after the records: %w", id, ErrCorrupt)
		}
	}
	return ph, nil
}
