package mvcc_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/mvcc"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/undo"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

// heapUndoer reverses records with mvcc.Revert on one heap (no indexes).
func (d *tdb) heapUndoer(xid uint64) wal.Undoer {
	return func(ctx context.Context, from, to undo.Ptr) error {
		return mvcc.Undo(ctx, d.e.Undo(), xid, from, to, func(p undo.Ptr, rec undo.Record) error {
			_, _, err := mvcc.Revert(ctx, d.h, p, rec)
			return err
		})
	}
}

// versions returns every row version of the heap, tombstones included.
func (d *tdb) versions() map[storage.RID]storage.Version {
	d.t.Helper()
	out := map[storage.RID]storage.Version{}
	s := d.h.ScanVersions()
	for {
		rid, v, ok, err := s.Next(bg)
		if err != nil {
			d.t.Fatal(err)
		}
		if !ok {
			return out
		}
		out[rid] = v
	}
}

func sameVersions(t *testing.T, got, want map[storage.RID]storage.Version, ctx string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d versions, want %d", ctx, len(got), len(want))
	}
	for rid, w := range want {
		g, ok := got[rid]
		if !ok || g.RowHeader != w.RowHeader || g.Deleted != w.Deleted || string(g.Data) != string(w.Data) {
			t.Fatalf("%s: row %s is %+v %q, want %+v %q", ctx, rid, g.RowHeader, g.Data, w.RowHeader, w.Data)
		}
	}
}

// TestRollbackRestoresVersions: inserts, updates (moving rows), deletes
// and repeated changes of one row are reversed exactly, both back to a
// savepoint and whole; the transaction's undo ends at the savepoint after
// a partial rollback, so later records chain from it.
func TestRollbackRestoresVersions(t *testing.T) {
	d := newTDB(t)
	var rids []storage.RID
	d.txn(func(w mvcc.Writer) error {
		for _, s := range []string{"a", "b", "c", "d"} {
			rid, err := w.Insert(bg, d.h, []byte(s))
			if err != nil {
				return err
			}
			rids = append(rids, rid)
		}
		return nil
	})
	before := d.versions()
	tx := d.e.Begin()
	xid := func() uint64 { return uint64(tx.XID()) }
	write := func(fn func(w mvcc.Writer) error) {
		t.Helper()
		if err := tx.Write(bg, func(context.Context) error {
			return fn(mvcc.Writer{Undo: d.e.Undo(), XID: xid(), Table: 1})
		}); err != nil {
			t.Fatal(err)
		}
	}
	write(func(w mvcc.Writer) error {
		if err := w.Update(bg, d.h, rids[0], []byte("a2")); err != nil {
			return err
		}
		return w.Delete(bg, d.h, rids[1])
	})
	sp := tx.Savepoint()
	afterFirst := d.versions()
	big := make([]byte, 7000) // moves the row
	write(func(w mvcc.Writer) error {
		if err := w.Update(bg, d.h, rids[0], big); err != nil {
			return err
		}
		if err := w.Update(bg, d.h, rids[0], []byte("a3")); err != nil {
			return err
		}
		if err := w.Update(bg, d.h, rids[2], big); err != nil {
			return err
		}
		if err := w.Delete(bg, d.h, rids[3]); err != nil {
			return err
		}
		_, err := w.Insert(bg, d.h, []byte("new"))
		return err
	})
	if err := tx.RollbackTo(bg, sp, d.heapUndoer(xid())); err != nil {
		t.Fatal(err)
	}
	sameVersions(t, d.versions(), afterFirst, "after rolling back to the savepoint")
	if tx.Savepoint() != sp {
		t.Fatalf("savepoint %s after rolling back to %s", tx.Savepoint(), sp)
	}
	// The transaction goes on; a new record follows the savepoint.
	var extra storage.RID
	write(func(w mvcc.Writer) (err error) { extra, err = w.Insert(bg, d.h, []byte("x")); return })
	rec, err := d.e.Undo().Read(bg, tx.Savepoint())
	if err != nil || rec.PrevInTxn != sp || rec.RID != extra {
		t.Fatalf("the next record: %+v, %v; want it to follow %s", rec, err, sp)
	}
	if err := tx.Rollback(bg, d.heapUndoer(xid())); err != nil {
		t.Fatal(err)
	}
	sameVersions(t, d.versions(), before, "after the rollback")
	if d.e.Status(tx.XID()) != wal.TxnAborted {
		t.Fatalf("status %v", d.e.Status(tx.XID()))
	}
	if segs := d.e.Undo().Segments(); len(segs) != 0 {
		t.Fatalf("undo left with no snapshot: %v", segs)
	}
	if err := tx.Write(bg, func(context.Context) error { return nil }); !errors.Is(err, wal.ErrTxnDone) {
		t.Fatalf("write after the rollback: %v", err)
	}
	// Replayed after a crash: the rollback's records too.
	if err := d.e.Flush(bg); err != nil {
		t.Fatal(err)
	}
	d.m.Crash(vfs.CrashOptions{})
	e := mustEngine(t, d.m, 256)
	defer func() { _ = e.Close(bg) }()
	d.e = e
	d.h, err = e.OpenHeap(bg, d.h.FirstPage())
	if err != nil {
		t.Fatal(err)
	}
	sameVersions(t, d.versions(), before, "after recovery")
	if e.Status(tx.XID()) != wal.TxnAborted || e.Recovery().DiscardedTransactions != 0 {
		t.Fatalf("after recovery: status %v, %d discarded", e.Status(tx.XID()), e.Recovery().DiscardedTransactions)
	}
}

// TestUndoWalkChecks: mvcc.Undo walks exactly the records between two
// pointers, and refuses a walk that never reaches its end or meets
// another transaction's record; Revert refuses a record that is not the
// row's latest change.
func TestUndoWalkChecks(t *testing.T) {
	d := newTDB(t)
	var rid storage.RID
	tx := d.e.Begin()
	var ptrs []undo.Ptr
	if err := tx.Write(bg, func(context.Context) error {
		w := mvcc.Writer{Undo: d.e.Undo(), XID: uint64(tx.XID()), Table: 1}
		var err error
		if rid, err = w.Insert(bg, d.h, []byte("v0")); err != nil {
			return err
		}
		ptrs = append(ptrs, d.e.Undo().Last(uint64(tx.XID())))
		for _, s := range []string{"v1", "v2", "v3"} {
			if err := w.Update(bg, d.h, rid, []byte(s)); err != nil {
				return err
			}
			ptrs = append(ptrs, d.e.Undo().Last(uint64(tx.XID())))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Abandon() }()
	xid := uint64(tx.XID())
	var got []undo.Ptr
	collect := func(p undo.Ptr, _ undo.Record) error { got = append(got, p); return nil }
	if err := mvcc.Undo(bg, d.e.Undo(), xid, ptrs[3], ptrs[1], collect); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []undo.Ptr{ptrs[3], ptrs[2]}) {
		t.Fatalf("walked %v, want %v and %v", got, ptrs[3], ptrs[2])
	}
	// to is after from: the walk passes 0 without meeting it.
	if err := mvcc.Undo(bg, d.e.Undo(), xid, ptrs[1], ptrs[3], collect); !errors.Is(err, mvcc.ErrCorrupt) {
		t.Fatalf("a walk that never reaches its end: %v", err)
	}
	if err := mvcc.Undo(bg, d.e.Undo(), xid+1, ptrs[3], 0, collect); !errors.Is(err, mvcc.ErrCorrupt) {
		t.Fatalf("a walk of another transaction's records: %v", err)
	}
	// The row's latest change is ptrs[3]: reverting ptrs[2] first is wrong.
	rec, err := d.e.Undo().Read(bg, ptrs[2])
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := mvcc.Revert(bg, d.h, ptrs[2], rec); !errors.Is(err, mvcc.ErrCorrupt) {
		t.Fatalf("reverting a record out of order: %v", err)
	}
	if v, _ := d.h.GetVersion(bg, rid); string(v.Data) != "v3" {
		t.Fatalf("a refused revert changed the row: %q", v.Data)
	}
}

// TestRolledBackUndoKeptForSnapshots: a rolled-back transaction's undo is
// kept while a snapshot taken during it lives (a reader may have copied
// its versions), and released once every snapshot was taken after it
// ended (docs/design/17-rollback.md section 2.8).
func TestRolledBackUndoKeptForSnapshots(t *testing.T) {
	d := newTDB(t)
	var rid storage.RID
	d.txn(func(w mvcc.Writer) (err error) { rid, err = w.Insert(bg, d.h, []byte("v1")); return })
	tx := d.e.Begin()
	if err := tx.Write(bg, func(context.Context) error {
		return mvcc.Writer{Undo: d.e.Undo(), XID: uint64(tx.XID()), Table: 1}.Update(bg, d.h, rid, []byte("v2"))
	}); err != nil {
		t.Fatal(err)
	}
	during := d.e.Snapshot()
	copied, err := d.h.GetVersion(bg, rid) // the uncommitted version
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(bg, d.heapUndoer(uint64(tx.XID()))); err != nil {
		t.Fatal(err)
	}
	after := d.e.Snapshot() // lives on: it does not need the undo
	defer after.Release()
	if !after.Ended(uint64(tx.XID())) || during.Ended(uint64(tx.XID())) || after.Sees(uint64(tx.XID())) {
		t.Fatal("Ended or Sees wrong for a rolled-back transaction")
	}
	if segs := d.e.Undo().Segments(); !slices.Equal(segs, []uint64{uint64(tx.XID())}) {
		t.Fatalf("segments %v while a snapshot taken during the transaction lives", segs)
	}
	// The copy taken before the rollback still leads to the old version.
	data, ok, err := d.reader(during, 0).Visible(bg, rid, copied)
	if err != nil || !ok || string(data) != "v1" {
		t.Fatalf("a copy of the rolled-back version: %q %v %v", data, ok, err)
	}
	during.Release()
	if _, err := d.e.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	if segs := d.e.Undo().Segments(); len(segs) != 0 {
		t.Fatalf("undo kept after the snapshot that needed it ended: %v", segs)
	}
	// Nothing of the transaction is open any more: the engine closes.
	if err := d.e.Close(bg); err != nil {
		t.Fatalf("closing after a rollback: %v", err)
	}
}

// TestRollbackToAfterAFailedWrite: a change that fails leaves the
// transaction able only to roll back; rolled back to its savepoint, it goes
// on and commits what came before.
func TestRollbackToAfterAFailedWrite(t *testing.T) {
	d := newTDB(t)
	tx := d.e.Begin()
	w := func() mvcc.Writer { return mvcc.Writer{Undo: d.e.Undo(), XID: uint64(tx.XID()), Table: 1} }
	var kept storage.RID
	if err := tx.Write(bg, func(context.Context) (err error) { kept, err = w().Insert(bg, d.h, []byte("kept")); return }); err != nil {
		t.Fatal(err)
	}
	sp := tx.Savepoint()
	failure := errors.New("failed partway")
	if err := tx.Write(bg, func(context.Context) error {
		if _, err := w().Insert(bg, d.h, []byte("undone")); err != nil {
			return err
		}
		return failure
	}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if _, err := tx.Commit(bg); !errors.Is(err, wal.ErrTxnAborting) {
		t.Fatalf("commit after a failed change: %v", err)
	}
	if err := tx.RollbackTo(bg, sp, d.heapUndoer(uint64(tx.XID()))); err != nil {
		t.Fatal(err)
	}
	var later storage.RID
	if err := tx.Write(bg, func(context.Context) (err error) { later, err = w().Insert(bg, d.h, []byte("later")); return }); err != nil {
		t.Fatalf("a change after rolling back to the savepoint: %v", err)
	}
	if _, err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, v := range d.versions() {
		got[string(v.Data)] = true
	}
	if len(got) != 2 || !got["kept"] || !got["later"] {
		t.Fatalf("committed rows %v (kept at %s, later at %s)", got, kept, later)
	}
}
