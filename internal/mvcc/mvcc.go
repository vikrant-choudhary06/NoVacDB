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

// Errors of reading and writing versions.
var (
	// ErrBadImage means an undo record's image is not a row version.
	ErrBadImage = errors.New("mvcc: bad version image")
	// ErrCorrupt means a row's undo chain does not lead where its versions
	// say: a record of another transaction or row, or a loop.
	ErrCorrupt = errors.New("mvcc: corrupt version chain")
	// ErrWriteConflict means a REPEATABLE READ transaction tried to change
	// a row whose newest version it does not see (docs/design/16-snapshots-
	// visibility.md section 2.3).
	ErrWriteConflict = errors.New("mvcc: row changed after the snapshot")
)

// maxChain bounds an undo chain, and past loopCheckAfter steps a chain is
// checked for loops: long chains are rare (one transaction changing a row
// many times), loops are corruption.
const (
	maxChain       = 1 << 24
	loopCheckAfter = 1024
)

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

// Snapshot says which transactions' changes a reader sees (a
// *wal.Snapshot).
type Snapshot interface {
	Sees(xid uint64) bool
}

// Reader reads the versions of rows a snapshot sees (section 2.2).
type Reader struct {
	Undo *undo.Log
	Snap Snapshot
	// Me is the reader's own transaction, 0 if it has not written: its
	// changes are always seen.
	Me uint64
}

func (r Reader) sees(xid uint64) bool { return xid == r.Me || r.Snap.Sees(xid) }

// Visible returns the data of the version of the row at rid that the
// snapshot sees, starting from v, the row's current version (a tombstone
// if v.Deleted); ok is false if the row does not exist for the snapshot.
func (r Reader) Visible(ctx context.Context, rid storage.RID, v storage.Version) (data []byte, ok bool, err error) {
	var seen map[uint64]bool // undo pointers followed, once the chain is long
	for step := range maxChain {
		if r.sees(v.XID) {
			if v.Deleted {
				return nil, false, nil
			}
			return v.Data, true, nil
		}
		if v.Undo == 0 {
			return nil, false, nil
		}
		if step >= loopCheckAfter {
			if seen == nil {
				seen = map[uint64]bool{}
			}
			if seen[v.Undo] {
				return nil, false, fmt.Errorf("row %s: undo chain loops at %s: %w", rid, undo.Ptr(v.Undo), ErrCorrupt)
			}
			seen[v.Undo] = true
		}
		rec, err := r.Undo.Read(ctx, undo.Ptr(v.Undo))
		if err != nil {
			return nil, false, fmt.Errorf("row %s: %w", rid, err)
		}
		if rec.XID != v.XID || rec.RID != rid {
			return nil, false, fmt.Errorf("row %s: version of transaction %d leads to a record of %d for row %s: %w", rid, v.XID, rec.XID, rec.RID, ErrCorrupt)
		}
		if rec.Kind == undo.Insert {
			return nil, false, nil // the row did not exist before
		}
		if v, err = DecodeImage(rec.Image); err != nil {
			return nil, false, fmt.Errorf("row %s: %w", rid, err)
		}
	}
	return nil, false, fmt.Errorf("row %s: undo chain longer than %d versions: %w", rid, maxChain, ErrCorrupt)
}

// Writer writes row versions for one transaction into one table.
type Writer struct {
	Undo *undo.Log
	XID  uint64
	// Table is the catalog table ID; 0 for a system table.
	Table uint64
	// Snap, if set (REPEATABLE READ), must see a row's newest version for
	// the row to be changed; otherwise the change fails with
	// ErrWriteConflict (section 2.3).
	Snap Snapshot
}

// record appends an undo record for a change of kind to the row at rid,
// whose version before the change is old (nil for an insert), and returns
// the header of the new version.
func (w Writer) record(ctx context.Context, kind undo.Kind, rid storage.RID, old *storage.Version) (storage.RowHeader, error) {
	if old != nil && w.Snap != nil && old.XID != w.XID && !w.Snap.Sees(old.XID) {
		return storage.RowHeader{}, fmt.Errorf("row %s was changed by transaction %d: %w", rid, old.XID, ErrWriteConflict)
	}
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
