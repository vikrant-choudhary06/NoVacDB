package undo

import (
	"encoding/binary"
	"fmt"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
)

// SegmentOp is a change to the segment table.
type SegmentOp uint8

// Segment table operations.
const (
	// SegmentAdd records a transaction's segment and its first page.
	SegmentAdd SegmentOp = 1
	// SegmentDrop removes a transaction's segment.
	SegmentDrop SegmentOp = 2
)

// SegmentEntry is one change of an UndoSegment log record.
type SegmentEntry struct {
	Op    SegmentOp
	XID   uint64
	First uint64 // the segment's first page
}

// MaxSegmentEntries is the most entries one UndoSegment record holds; a
// checkpoint logs the table again in records of at most this many.
const MaxSegmentEntries = 400

// UndoSegment log record payload (WAL type 11), little-endian: u16 Count,
// then Count entries of u8 Op, u64 XID, u64 first page.
const segmentEntrySize = 17

// EncodeSegmentEntries encodes entries as an UndoSegment record payload.
func EncodeSegmentEntries(entries []SegmentEntry) []byte {
	out := binary.LittleEndian.AppendUint16(make([]byte, 0, 2+segmentEntrySize*len(entries)), uint16(len(entries)))
	for _, e := range entries {
		out = append(out, byte(e.Op))
		out = binary.LittleEndian.AppendUint64(out, e.XID)
		out = binary.LittleEndian.AppendUint64(out, e.First)
	}
	return out
}

// DecodeSegmentEntries decodes and validates an UndoSegment record payload.
// It never panics on any input.
func DecodeSegmentEntries(p []byte) ([]SegmentEntry, error) {
	if len(p) < 2 {
		return nil, fmt.Errorf("undo segment record of %d bytes: %w", len(p), ErrCorrupt)
	}
	n := int(binary.LittleEndian.Uint16(p))
	if n == 0 || n > MaxSegmentEntries || len(p) != 2+segmentEntrySize*n {
		return nil, fmt.Errorf("undo segment record: %d entries in %d bytes: %w", n, len(p), ErrCorrupt)
	}
	out := make([]SegmentEntry, n)
	for i := range out {
		e := p[2+segmentEntrySize*i:]
		out[i] = SegmentEntry{Op: SegmentOp(e[0]), XID: binary.LittleEndian.Uint64(e[1:]), First: binary.LittleEndian.Uint64(e[9:])}
		switch {
		case out[i].Op != SegmentAdd && out[i].Op != SegmentDrop:
			return nil, fmt.Errorf("undo segment record: entry %d has op %d: %w", i, out[i].Op, ErrCorrupt)
		case out[i].XID == 0:
			return nil, fmt.Errorf("undo segment record: entry %d names transaction 0: %w", i, ErrCorrupt)
		case out[i].First < storage.FirstDataPage:
			return nil, fmt.Errorf("undo segment record: entry %d names page %d: %w", i, out[i].First, ErrCorrupt)
		}
	}
	return out, nil
}
