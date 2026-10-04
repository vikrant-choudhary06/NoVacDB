package mvcc

import (
	"context"
	"fmt"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/undo"
)

// Rollback (docs/design/17-rollback.md): a transaction's undo records are
// reversed newest first, each putting back the row version its change
// replaced.

// Undo calls fn for each undo record of transaction xid from the record at
// from back to, but not including, the record at to (0: back to the
// first), newest first, following PrevInTxn. Each record is read with
// every check undo.Log.Read makes, and must belong to xid; the walk must
// reach to.
func Undo(ctx context.Context, log *undo.Log, xid uint64, from, to undo.Ptr, fn func(p undo.Ptr, rec undo.Record) error) error {
	var seen map[undo.Ptr]bool // pointers followed, once the walk is long
	p := from
	for step := 0; p != to; step++ {
		if p == 0 {
			return fmt.Errorf("transaction %d: undo ends before %s: %w", xid, to, ErrCorrupt)
		}
		if step >= maxChain {
			return fmt.Errorf("transaction %d: more than %d undo records: %w", xid, maxChain, ErrCorrupt)
		}
		if step >= loopCheckAfter {
			if seen == nil {
				seen = map[undo.Ptr]bool{}
			}
			if seen[p] {
				return fmt.Errorf("transaction %d: undo loops at %s: %w", xid, p, ErrCorrupt)
			}
			seen[p] = true
		}
		rec, err := log.Read(ctx, p)
		if err != nil {
			return fmt.Errorf("transaction %d: %w", xid, err)
		}
		if rec.XID != xid {
			return fmt.Errorf("transaction %d: undo at %s belongs to transaction %d: %w", xid, p, rec.XID, ErrCorrupt)
		}
		if err := fn(p, rec); err != nil {
			return err
		}
		p = rec.PrevInTxn
	}
	return nil
}

// Revert reverses the change of undo record rec, at p, to its row in h:
// the row's current version must be the one that change wrote (written by
// the record's transaction, naming the record). It returns that version,
// and the version put back (nil if the change was an insert, whose row is
// removed), so that the caller can change the indexes to match.
func Revert(ctx context.Context, h *storage.Heap, p undo.Ptr, rec undo.Record) (cur storage.Version, restored *storage.Version, err error) {
	cur, err = h.GetVersion(ctx, rec.RID)
	if err != nil {
		return storage.Version{}, nil, fmt.Errorf("reverting undo at %s: %w", p, err)
	}
	if cur.XID != rec.XID || cur.Undo != uint64(p) || cur.Deleted != (rec.Kind == undo.Delete) {
		return storage.Version{}, nil, fmt.Errorf("reverting undo at %s of transaction %d: row %s is a version of transaction %d naming undo %s (deleted %v): %w",
			p, rec.XID, rec.RID, cur.XID, undo.Ptr(cur.Undo), cur.Deleted, ErrCorrupt)
	}
	if rec.Kind == undo.Insert {
		if err := h.Remove(ctx, rec.RID); err != nil {
			return storage.Version{}, nil, err
		}
		return cur, nil, nil
	}
	old, err := DecodeImage(rec.Image)
	if err != nil {
		return storage.Version{}, nil, fmt.Errorf("reverting undo at %s: %w", p, err)
	}
	if err := h.Restore(ctx, rec.RID, old); err != nil {
		return storage.Version{}, nil, err
	}
	return cur, &old, nil
}
