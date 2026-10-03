package undo

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
)

// Op says what a block of an Undo log record does to its page.
type Op uint8

// Block operations.
const (
	// OpImage holds the whole page after the change. A new page is always
	// logged as an image, with its first record in it.
	OpImage Op = 1
	// OpAppend appends one record at Offset, which must be the page's Used.
	OpAppend Op = 2
	// OpSetNext links the page to Next, the segment's new last page.
	OpSetNext Op = 3
)

// maxBlocks is the most pages one Undo log record changes: growing a
// segment changes its last page (the link) and the new page.
const maxBlocks = 2

// Block is the part of an Undo log record about one page.
type Block struct {
	Page   uint64
	Op     Op
	Offset uint16 // append
	Data   []byte // image (PageSize bytes) or the appended record
	Next   uint64 // set-next
}

// Undo log record payload (WAL type 10), little-endian:
//
//	offset  size  field
//	0       1     BlockCount (1 or 2)
//	1       ...   blocks, back to back
//
// Block:
//
//	offset  size  field
//	0       8     PageID
//	8       1     Op
//	9       ...   image:    8192 bytes
//	              append:   u16 offset, u16 length, the record
//	              set-next: u64 next page ID
const blockHeaderSize = 9

// EncodeBlocks encodes blocks as an Undo log record payload.
func EncodeBlocks(blocks []Block) []byte {
	out := []byte{byte(len(blocks))}
	for _, b := range blocks {
		out = binary.LittleEndian.AppendUint64(out, b.Page)
		out = append(out, byte(b.Op))
		switch b.Op {
		case OpImage:
			out = append(out, b.Data...)
		case OpAppend:
			out = binary.LittleEndian.AppendUint16(out, b.Offset)
			out = binary.LittleEndian.AppendUint16(out, uint16(len(b.Data)))
			out = append(out, b.Data...)
		case OpSetNext:
			out = binary.LittleEndian.AppendUint64(out, b.Next)
		}
	}
	return out
}

// DecodeBlocks decodes and validates an Undo log record payload: the
// structure, every image as a whole undo page, and every appended record.
// The blocks' Data alias p. It never panics on any input.
func DecodeBlocks(p []byte) ([]Block, error) {
	bad := func(format string, args ...any) ([]Block, error) {
		return nil, fmt.Errorf("undo log record: %s: %w", fmt.Sprintf(format, args...), ErrCorrupt)
	}
	if len(p) < 1 {
		return bad("empty payload")
	}
	n := int(p[0])
	if n < 1 || n > maxBlocks {
		return bad("%d blocks", n)
	}
	rest := p[1:]
	blocks := make([]Block, 0, n)
	for i := range n {
		if len(rest) < blockHeaderSize {
			return bad("block %d: truncated header", i)
		}
		b := Block{Page: binary.LittleEndian.Uint64(rest), Op: Op(rest[8])}
		rest = rest[blockHeaderSize:]
		if b.Page < storage.FirstDataPage {
			return bad("block %d: page %d is reserved", i, b.Page)
		}
		switch b.Op {
		case OpImage:
			if len(rest) < storage.PageSize {
				return bad("block %d: truncated image", i)
			}
			b.Data, rest = rest[:storage.PageSize:storage.PageSize], rest[storage.PageSize:]
			if _, err := pageRecords(b.Data, b.Page, nil); err != nil {
				return bad("block %d: image: %v", i, err)
			}
		case OpAppend:
			if len(rest) < 4 {
				return bad("block %d: truncated append", i)
			}
			b.Offset = binary.LittleEndian.Uint16(rest)
			size := int(binary.LittleEndian.Uint16(rest[2:]))
			rest = rest[4:]
			if size > len(rest) {
				return bad("block %d: record of %d bytes, %d left", i, size, len(rest))
			}
			b.Data, rest = rest[:size:size], rest[size:]
			if int(b.Offset) < pageHeaderSize || int(b.Offset)+size > storage.PageSize {
				return bad("block %d: record of %d bytes at offset %d", i, size, b.Offset)
			}
			if _, m, err := DecodeRecord(b.Data); err != nil || m != size {
				return bad("block %d: appended record: %v", i, err)
			}
		case OpSetNext:
			if len(rest) < 8 {
				return bad("block %d: truncated link", i)
			}
			b.Next, rest = binary.LittleEndian.Uint64(rest), rest[8:]
			if b.Next < storage.FirstDataPage || b.Next == b.Page {
				return bad("block %d: next page %d", i, b.Next)
			}
		default:
			return bad("block %d: op %d", i, b.Op)
		}
		for _, prev := range blocks {
			if prev.Page == b.Page {
				return bad("page %d appears twice", b.Page)
			}
		}
		blocks = append(blocks, b)
	}
	if len(rest) != 0 {
		return bad("%d bytes after the last block", len(rest))
	}
	return blocks, nil
}

// Redo replays the Undo log record at lsn onto the pages in bp. An image is
// installed whatever the page holds (its copy on disk may be torn or free);
// an append or a link is applied only if the page's LSN is below lsn, and
// must fit the page exactly. Every changed page is stamped with lsn.
// Anything that does not fit is ErrCorrupt.
func Redo(ctx context.Context, bp *storage.BufferPool, lsn uint64, payload []byte) error {
	blocks, err := DecodeBlocks(payload)
	if err != nil {
		return fmt.Errorf("redo at %d: %w", lsn, err)
	}
	for _, b := range blocks {
		if err := redoBlock(ctx, bp, lsn, b); err != nil {
			return fmt.Errorf("redo at %d, undo page %d: %w", lsn, b.Page, err)
		}
	}
	return nil
}

func redoBlock(ctx context.Context, bp *storage.BufferPool, lsn uint64, b Block) error {
	if b.Op == OpImage {
		// DecodeBlocks validated the image: an overwrite pin must be
		// followed by an overwrite.
		ref, err := bp.PinForOverwrite(ctx, b.Page)
		if err != nil {
			return err
		}
		ref.Lock()
		copy(ref.Data(), b.Data)
		storage.SetPageLSN(ref.Data(), lsn)
		ref.Unlock()
		return ref.Unpin(true)
	}
	ref, err := bp.FetchPage(ctx, b.Page)
	if err != nil {
		return err
	}
	ref.Lock()
	dirty, err := applyBlock(ref.Data(), b.Page, lsn, b)
	ref.Unlock()
	if uerr := ref.Unpin(dirty); err == nil {
		err = uerr
	}
	return err
}

// applyBlock applies an append or a link to the undo page id, unless the
// page already contains it. It changes the page only if the operation fits
// exactly.
func applyBlock(page []byte, id, lsn uint64, b Block) (bool, error) {
	if storage.PageLSN(page) >= lsn {
		return false, nil
	}
	ph, err := decodePageHeader(page, id)
	if err != nil {
		return false, err
	}
	switch b.Op {
	case OpAppend:
		rec, _, err := DecodeRecord(b.Data)
		if err != nil {
			return false, err
		}
		switch {
		case int(b.Offset) != ph.used:
			return false, fmt.Errorf("append at %d, page used to %d: %w", b.Offset, ph.used, ErrCorrupt)
		case rec.XID != ph.xid:
			return false, fmt.Errorf("record of transaction %d on a page of %d: %w", rec.XID, ph.xid, ErrCorrupt)
		}
		copy(page[ph.used:], b.Data)
		setUsed(page, ph.used+len(b.Data))
	case OpSetNext:
		if ph.next != 0 {
			return false, fmt.Errorf("link to %d, page already links to %d: %w", b.Next, ph.next, ErrCorrupt)
		}
		setNext(page, b.Next)
	default:
		return false, fmt.Errorf("op %d: %w", b.Op, ErrCorrupt)
	}
	storage.SetPageLSN(page, lsn)
	return true, nil
}

// clonePage returns a copy of a page, for rollback of an unlogged change.
func clonePage(buf []byte) []byte { return bytes.Clone(buf) }
