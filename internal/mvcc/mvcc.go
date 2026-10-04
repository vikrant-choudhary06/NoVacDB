// Package mvcc writes and (from Step 6.4) reads versioned rows: every
// change to a heap row first saves the row's previous version in the undo
// log, then changes the row in place with a header naming the transaction
// and the undo record (docs/design/15-row-versioning.md).
package mvcc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/undo"
)

// ErrBadImage means an undo record's image is not a row version.
var ErrBadImage = errors.New("mvcc: bad version image")

// imageHeaderSize is the size of a version image's header: XID and undo
// pointer.
const imageHeaderSize = 16

// EncodeImage returns the undo image of a row version: u64 XID, u64 undo
// pointer, then the encoded row (section 2.3).
func EncodeImage(v storage.Version) []byte {
	b := make([]byte, imageHeaderSize, imageHeaderSize+len(v.Data))
	binary.LittleEndian.PutUint64(b, v.XID)
	binary.LittleEndian.PutUint64(b[8:], v.Undo)
	return append(b, v.Data...)
}

// DecodeImage decodes an undo image. The version's Data aliases b.
func DecodeImage(b []byte) (storage.Version, error) {
	if len(b) < imageHeaderSize || len(b)-imageHeaderSize > storage.MaxRowData {
		return storage.Version{}, fmt.Errorf("version image of %d bytes: %w", len(b), ErrBadImage)
	}
	v := storage.Version{
		RowHeader: storage.RowHeader{XID: binary.LittleEndian.Uint64(b), Undo: binary.LittleEndian.Uint64(b[8:])},
		Data:      b[imageHeaderSize:],
	}
	if v.XID == 0 {
		return storage.Version{}, fmt.Errorf("version image of transaction 0: %w", ErrBadImage)
	}
	return v, nil
}

// Writer writes row versions for one transaction into one table.
type Writer struct {
	Undo *undo.Log
	XID  uint64
	// Table is the catalog table ID; 0 for a system table.
	Table uint64
}

// record appends an undo record for a change of kind to the row at rid,
// whose version before the change is old (nil for an insert), and returns
// the header of the new version.
func (w Writer) record(ctx context.Context, kind undo.Kind, rid storage.RID, old *storage.Version) (storage.RowHeader, error) {
	rec := undo.Record{Kind: kind, XID: w.XID, Table: w.Table, RID: rid}
	if old != nil {
		rec.PrevForRow = undo.Ptr(old.Undo)
		rec.Image = EncodeImage(*old)
	}
	p, err := w.Undo.Append(ctx, rec)
	if err != nil {
		return storage.RowHeader{}, err
	}
	return storage.RowHeader{XID: w.XID, Undo: uint64(p)}, nil
}

// Insert stores a new row and its undo record.
func (w Writer) Insert(ctx context.Context, h *storage.Heap, data []byte) (storage.RID, error) {
	return h.Insert(ctx, data, func(rid storage.RID, _ *storage.Version) (storage.RowHeader, error) {
		return w.record(ctx, undo.Insert, rid, nil)
	})
}

// Update replaces the row at rid, saving its previous version.
func (w Writer) Update(ctx context.Context, h *storage.Heap, rid storage.RID, data []byte) error {
	return h.Update(ctx, rid, data, func(rid storage.RID, old *storage.Version) (storage.RowHeader, error) {
		return w.record(ctx, undo.Update, rid, old)
	})
}

// Delete deletes the row at rid, saving its last version.
func (w Writer) Delete(ctx context.Context, h *storage.Heap, rid storage.RID) error {
	return h.Delete(ctx, rid, func(rid storage.RID, old *storage.Version) (storage.RowHeader, error) {
		return w.record(ctx, undo.Delete, rid, old)
	})
}
