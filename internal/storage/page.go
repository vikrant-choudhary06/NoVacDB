package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Page geometry. The page size is a locked project decision.
const (
	// PageSize is the size of every page on disk, in bytes.
	PageSize = 8192
	// HeaderSize is the size of the page header, in bytes.
	HeaderSize = 24
	// PayloadSize is the number of bytes available to the owner of a page.
	PayloadSize = PageSize - HeaderSize
)

// Header field offsets and sizes. See the layout table below.
const (
	offID    = 0
	offLSN   = 8
	offCRC   = 16
	offType  = 20
	offFlags = 22
	crcSize  = 4
)

// Page header layout (24 bytes, little-endian):
//
//	offset  size  field
//	0       8     PageID  (page N lives at file offset N * 8192)
//	8       8     LSN     (WAL position of the last change; 0 = none yet)
//	16      4     CRC-32C (over the whole page, computed with this field zeroed)
//	20      2     PageType
//	22      2     Flags   (meaning defined by each page type)
//
// The payload follows at offset 24, which is 8-byte aligned.

// PageType says what a page's payload contains.
type PageType uint16

// Page types. Adding one is a deliberate change to the on-disk format.
const (
	// PageTypeInvalid is never valid; an all-zero page decodes to it.
	PageTypeInvalid PageType = 0
	// PageTypeFileHeader is the first page of a data file (Step 1.2).
	PageTypeFileHeader PageType = 1
	// PageTypeFree is an unused page on the free list (Step 1.2).
	PageTypeFree PageType = 2
	// PageTypeHeap is a slotted heap page (Step 1.4).
	PageTypeHeap PageType = 3
	// PageTypeBTreeInternal is a B+Tree internal node (Step 3.1).
	PageTypeBTreeInternal PageType = 4
	// PageTypeBTreeLeaf is a B+Tree leaf node (Step 3.1).
	PageTypeBTreeLeaf PageType = 5
	// PageTypeUndo is a page of an undo segment (Step 6.2,
	// docs/design/14-undo-log.md).
	PageTypeUndo PageType = 6
)

// Valid reports whether t is a known, non-zero page type.
func (t PageType) Valid() bool {
	return t >= PageTypeFileHeader && t <= PageTypeUndo
}

// Sentinel errors returned (wrapped) by the functions in this file.
var (
	// ErrBadSize means a buffer is not exactly PageSize bytes.
	ErrBadSize = errors.New("storage: buffer is not one page")
	// ErrZeroPage means the page is entirely zero bytes: it was never
	// written, as opposed to written and damaged.
	ErrZeroPage = errors.New("storage: page is all zeros")
	// ErrChecksum means the stored CRC-32C does not match the page.
	ErrChecksum = errors.New("storage: page checksum mismatch")
	// ErrBadPageType means the page type is zero or unknown.
	ErrBadPageType = errors.New("storage: invalid page type")
	// ErrPageIDMismatch means the page claims a different ID than the one
	// it was read from, e.g. after a write to the wrong offset.
	ErrPageIDMismatch = errors.New("storage: page id mismatch")
)

// Header is the decoded page header. The CRC is not part of it: it is
// written only by Seal and checked only by Verify.
type Header struct {
	ID    uint64
	LSN   uint64
	Type  PageType
	Flags uint16
}

// castagnoli is read-only after init. CRC-32C is a locked project decision
// and has hardware support on amd64 and arm64.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func checkSize(buf []byte) error {
	if len(buf) != PageSize {
		return fmt.Errorf("buffer length %d, want %d: %w", len(buf), PageSize, ErrBadSize)
	}
	return nil
}

// InitPage zeroes buf and writes h into its header. The CRC stays zero until
// Seal. It fails if buf is not one page or h.Type is not valid.
func InitPage(buf []byte, h Header) error {
	if err := checkSize(buf); err != nil {
		return fmt.Errorf("initialising page %d: %w", h.ID, err)
	}
	if !h.Type.Valid() {
		return fmt.Errorf("initialising page %d with type %d: %w", h.ID, h.Type, ErrBadPageType)
	}
	clear(buf)
	binary.LittleEndian.PutUint64(buf[offID:], h.ID)
	binary.LittleEndian.PutUint64(buf[offLSN:], h.LSN)
	binary.LittleEndian.PutUint16(buf[offType:], uint16(h.Type))
	binary.LittleEndian.PutUint16(buf[offFlags:], h.Flags)
	return nil
}

// DecodeHeader reads the header of buf. It checks structure only (size and
// page type), not the checksum; use Verify for data read from disk.
func DecodeHeader(buf []byte) (Header, error) {
	if err := checkSize(buf); err != nil {
		return Header{}, fmt.Errorf("decoding page header: %w", err)
	}
	h := Header{
		ID:    binary.LittleEndian.Uint64(buf[offID:]),
		LSN:   binary.LittleEndian.Uint64(buf[offLSN:]),
		Type:  PageType(binary.LittleEndian.Uint16(buf[offType:])),
		Flags: binary.LittleEndian.Uint16(buf[offFlags:]),
	}
	if !h.Type.Valid() {
		return Header{}, fmt.Errorf("decoding header of page %d, type %d: %w", h.ID, h.Type, ErrBadPageType)
	}
	return h, nil
}

// pageCRC computes the checksum with the CRC field treated as zero, without
// copying the page.
func pageCRC(buf []byte) uint32 {
	var zero [crcSize]byte
	c := crc32.Update(0, castagnoli, buf[:offCRC])
	c = crc32.Update(c, castagnoli, zero[:])
	return crc32.Update(c, castagnoli, buf[offCRC+crcSize:])
}

// Seal computes the page checksum and stores it in the header. Call it right
// before the page is written to disk, after the last change to the page.
func Seal(buf []byte) error {
	if err := checkSize(buf); err != nil {
		return fmt.Errorf("sealing page: %w", err)
	}
	binary.LittleEndian.PutUint32(buf[offCRC:], pageCRC(buf))
	return nil
}

func isZero(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

// Verify checks a page read from disk at page ID wantID. It checks, in
// order: size, all-zero, checksum, page type, then that the stored page ID
// equals wantID (which catches writes to the wrong offset).
func Verify(buf []byte, wantID uint64) error {
	if err := checkSize(buf); err != nil {
		return fmt.Errorf("verifying page %d: %w", wantID, err)
	}
	if isZero(buf) {
		return fmt.Errorf("verifying page %d: %w", wantID, ErrZeroPage)
	}
	if got, want := binary.LittleEndian.Uint32(buf[offCRC:]), pageCRC(buf); got != want {
		return fmt.Errorf("verifying page %d: stored crc %#08x, computed %#08x: %w", wantID, got, want, ErrChecksum)
	}
	h, err := DecodeHeader(buf)
	if err != nil {
		return fmt.Errorf("verifying page %d: %w", wantID, err)
	}
	if h.ID != wantID {
		return fmt.Errorf("page read as %d claims to be %d: %w", wantID, h.ID, ErrPageIDMismatch)
	}
	return nil
}

// setPageLSN stamps a page with the LSN of the last record that changed it.
func setPageLSN(buf []byte, lsn uint64) { binary.LittleEndian.PutUint64(buf[offLSN:], lsn) }

// Payload returns the part of buf after the header, aliasing buf.
func Payload(buf []byte) ([]byte, error) {
	if err := checkSize(buf); err != nil {
		return nil, fmt.Errorf("getting page payload: %w", err)
	}
	return buf[HeaderSize:], nil
}

// PageLSN returns the LSN stored in a page's header. buf must be one page.
func PageLSN(buf []byte) uint64 { return pageLSN(buf) }

// SetPageLSN stamps a page with the LSN of the last record that changed it,
// for packages that log their own page formats. buf must be one page.
func SetPageLSN(buf []byte, lsn uint64) { setPageLSN(buf, lsn) }
