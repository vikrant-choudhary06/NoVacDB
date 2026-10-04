package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
)

// Logger is how a heap records its changes in the write-ahead log. The wal
// package's Logger implements it. See docs/design/07-checkpoints-recovery.md.
type Logger interface {
	// Log appends a record describing a change to pages the caller holds
	// exclusively latched, and returns the record's LSN. build is called
	// with the current redo point and returns the record payload; the redo
	// point cannot change between build and the append. If Log fails, the
	// record must be treated as possibly logged and the change undone in
	// memory.
	Log(ctx context.Context, build func(redoPoint uint64) []byte) (uint64, error)
}

// ErrBadHeapRecord means a heap WAL record is malformed, or cannot be
// replayed onto the page it names.
var ErrBadHeapRecord = errors.New("storage: bad heap log record")

// BlockKind says what a block of a heap record contains.
type BlockKind uint8

// Block kinds.
const (
	// BlockImage holds the whole page as it was after the change.
	BlockImage BlockKind = 1 + iota
	// BlockInsert inserts a tuple, which must land in Slot.
	BlockInsert
	// BlockUpdate replaces the tuple in Slot.
	BlockUpdate
	// BlockDelete deletes the tuple in Slot.
	BlockDelete
	// BlockSetNext sets the page's next-page link to Next.
	BlockSetNext
)

// maxHeapBlocks is the most pages one heap operation changes: moving a
// moved row again changes three (docs/design/15-row-versioning.md 2.2).
const maxHeapBlocks = 3

// HeapBlock is the part of a heap record about one page.
type HeapBlock struct {
	Page uint64
	Kind BlockKind
	Slot uint16 // insert, update, delete
	Data []byte // image (PageSize bytes) or tuple
	Next uint64 // set-next
}

// Heap record payload layout (little-endian):
//
//	offset  size  field
//	0       1     BlockCount (1 to 3)
//	1       ...   blocks, back to back
//
// Block:
//
//	offset  size  field
//	0       8     PageID
//	8       1     Kind
//	9       ...   image:    8192 bytes
//	              insert:   u16 slot, u32 length, tuple
//	              update:   u16 slot, u32 length, tuple
//	              delete:   u16 slot
//	              set-next: u64 next page ID
const blockHeaderSize = 9

// encodeHeapRecord encodes blocks as a heap record payload.
func encodeHeapRecord(blocks []HeapBlock) []byte {
	out := []byte{byte(len(blocks))}
	for _, b := range blocks {
		out = binary.LittleEndian.AppendUint64(out, b.Page)
		out = append(out, byte(b.Kind))
		switch b.Kind {
		case BlockImage:
			out = append(out, b.Data...)
		case BlockInsert, BlockUpdate:
			out = binary.LittleEndian.AppendUint16(out, b.Slot)
			out = binary.LittleEndian.AppendUint32(out, uint32(len(b.Data)))
			out = append(out, b.Data...)
		case BlockDelete:
			out = binary.LittleEndian.AppendUint16(out, b.Slot)
		case BlockSetNext:
			out = binary.LittleEndian.AppendUint64(out, b.Next)
		}
	}
	return out
}

// DecodeHeapRecord decodes and validates a heap record payload. The blocks'
// Data alias p. It never panics on any input.
func DecodeHeapRecord(p []byte) ([]HeapBlock, error) {
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), ErrBadHeapRecord)
	}
	if len(p) < 1 {
		return nil, bad("empty payload")
	}
	n := int(p[0])
	if n < 1 || n > maxHeapBlocks {
		return nil, bad("%d blocks", n)
	}
	rest := p[1:]
	blocks := make([]HeapBlock, 0, n)
	for i := range n {
		if len(rest) < blockHeaderSize {
			return nil, bad("block %d: truncated header", i)
		}
		b := HeapBlock{Page: binary.LittleEndian.Uint64(rest), Kind: BlockKind(rest[8])}
		rest = rest[blockHeaderSize:]
		if b.Page < FirstDataPage {
			return nil, bad("block %d: page %d is reserved", i, b.Page)
		}
		switch b.Kind {
		case BlockImage:
			if len(rest) < PageSize {
				return nil, bad("block %d: truncated image", i)
			}
			b.Data, rest = rest[:PageSize:PageSize], rest[PageSize:]
		case BlockInsert, BlockUpdate:
			if len(rest) < 6 {
				return nil, bad("block %d: truncated tuple header", i)
			}
			b.Slot = binary.LittleEndian.Uint16(rest)
			size := int(binary.LittleEndian.Uint32(rest[2:]))
			rest = rest[6:]
			if size < 1 || size > MaxTupleSize || size > len(rest) {
				return nil, bad("block %d: tuple length %d", i, size)
			}
			b.Data, rest = rest[:size:size], rest[size:]
		case BlockDelete:
			if len(rest) < 2 {
				return nil, bad("block %d: truncated slot", i)
			}
			b.Slot, rest = binary.LittleEndian.Uint16(rest), rest[2:]
		case BlockSetNext:
			if len(rest) < 8 {
				return nil, bad("block %d: truncated link", i)
			}
			b.Next, rest = binary.LittleEndian.Uint64(rest), rest[8:]
		default:
			return nil, bad("block %d: kind %d", i, b.Kind)
		}
		for _, prev := range blocks {
			if prev.Page == b.Page {
				return nil, bad("page %d appears twice", b.Page)
			}
		}
		blocks = append(blocks, b)
	}
	if len(rest) != 0 {
		return nil, bad("%d bytes after the last block", len(rest))
	}
	return blocks, nil
}

// RedoHeapRecord replays the heap record at lsn onto the pages in bp. An image
// block is installed whatever the page holds (its copy on disk may be torn);
// an operation is applied only if the page's LSN is below lsn, so replaying a
// record the page already contains changes nothing. Every changed page is
// stamped with lsn. Anything that does not fit is ErrBadHeapRecord.
func RedoHeapRecord(ctx context.Context, bp *BufferPool, lsn uint64, payload []byte) error {
	blocks, err := DecodeHeapRecord(payload)
	if err != nil {
		return fmt.Errorf("redo at %d: %w", lsn, err)
	}
	for _, b := range blocks {
		if err := redoBlock(ctx, bp, lsn, b); err != nil {
			return fmt.Errorf("redo at %d, page %d: %w", lsn, b.Page, err)
		}
	}
	return nil
}

func redoBlock(ctx context.Context, bp *BufferPool, lsn uint64, b HeapBlock) error {
	if b.Kind == BlockImage {
		// Validate before touching the pool: an overwrite pin must be
		// followed by an overwrite.
		if h, err := DecodeHeader(b.Data); err != nil || h.ID != b.Page || h.Type != PageTypeHeap {
			return fmt.Errorf("image is not heap page %d: %w", b.Page, ErrBadHeapRecord)
		}
		img := bytes.Clone(b.Data)
		sp, err := NewSlottedPage(img)
		if err == nil {
			err = sp.Validate()
		}
		if err != nil {
			return fmt.Errorf("image: %w: %w", ErrBadHeapRecord, err)
		}
		ref, err := bp.PinForOverwrite(ctx, b.Page)
		if err != nil {
			return err
		}
		ref.Lock()
		copy(ref.Data(), img)
		setPageLSN(ref.Data(), lsn)
		ref.Unlock()
		return ref.Unpin(true)
	}

	ref, err := bp.FetchPage(ctx, b.Page)
	if err != nil {
		return err
	}
	ref.Lock()
	dirty, err := applyBlock(ref.Data(), lsn, b)
	ref.Unlock()
	if uerr := ref.Unpin(dirty); err == nil {
		err = uerr
	}
	return err
}

// applyBlock applies an operation block to a page, unless the page already
// contains it.
func applyBlock(page []byte, lsn uint64, b HeapBlock) (bool, error) {
	if pageLSN(page) >= lsn {
		return false, nil
	}
	sp, err := NewSlottedPage(page)
	if err != nil {
		return false, fmt.Errorf("%w: %w", ErrBadHeapRecord, err)
	}
	switch b.Kind {
	case BlockInsert:
		slot, err := sp.Insert(b.Data)
		if err != nil {
			return false, fmt.Errorf("insert: %w: %w", ErrBadHeapRecord, err)
		}
		if slot != int(b.Slot) {
			// The page diverged from the one the record was made against.
			return true, fmt.Errorf("insert landed in slot %d, record says %d: %w", slot, b.Slot, ErrBadHeapRecord)
		}
	case BlockUpdate:
		if err := sp.Update(int(b.Slot), b.Data); err != nil {
			return false, fmt.Errorf("update: %w: %w", ErrBadHeapRecord, err)
		}
	case BlockDelete:
		if err := sp.Delete(int(b.Slot)); err != nil {
			return false, fmt.Errorf("delete: %w: %w", ErrBadHeapRecord, err)
		}
	case BlockSetNext:
		sp.SetNextPage(b.Next)
	default:
		return false, fmt.Errorf("kind %d: %w", b.Kind, ErrBadHeapRecord)
	}
	setPageLSN(page, lsn)
	return true, nil
}
