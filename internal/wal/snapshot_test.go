package wal

import (
	"context"
	"slices"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/undo"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

func TestSnapshotSees(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 16})
	h, tr := setupHeapAndTree(t, e)
	committed, err := runTxn(e, h, tr, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	tx := openTxn(t, e, func() error { return insertRows(h, tr, 2, 0, 1) })
	s := e.Snapshot()
	if !s.Sees(uint64(committed)) || s.Sees(uint64(tx.XID())) || s.Sees(uint64(s.XMax)) || s.Sees(0) {
		t.Fatalf("snapshot %+v", s)
	}
	if !slices.Equal(s.Active, []XID{tx.XID()}) || e.LastWriter() != tx.XID() || e.LiveSnapshots() != 1 {
		t.Fatalf("active %v, last writer %d, %d live", s.Active, e.LastWriter(), e.LiveSnapshots())
	}
	s.Release()
	s.Release()
	if e.LiveSnapshots() != 0 {
		t.Fatal("released snapshot still registered")
	}
	// After a crash the transaction in flight is aborted: never seen.
	flight := tx.XID()
	_ = e.w.Flush(bg) // its begin is durable, so recovery knows it aborted
	m.Crash(vfs.CrashOptions{})
	e = mustEngine(t, m, EngineOptions{Frames: 16})
	defer func() { _ = e.Close(bg) }()
	if e.Status(flight) != TxnAborted {
		t.Fatalf("setup: in-flight transaction is %v", e.Status(flight))
	}
	s = e.Snapshot()
	defer s.Release()
	if s.Sees(uint64(flight)) || !s.Sees(uint64(committed)) || flight >= s.XMax {
		t.Fatalf("after recovery: sees aborted %v, committed %v", s.Sees(uint64(flight)), s.Sees(uint64(committed)))
	}
}

// A transaction still writing never has its undo released, whatever the
// snapshots: only a committed one's may be, by its own commit.
func TestActiveUndoIsNeverReleased(t *testing.T) {
	e := mustEngine(t, newFS(t), EngineOptions{Frames: 16})
	defer func() { _ = e.Abandon() }()
	tx := e.Begin()
	if err := tx.Write(bg, func(ctx context.Context) error {
		_, err := e.Undo().Append(ctx, undo.Record{Kind: undo.Insert, XID: uint64(tx.XID()), RID: storage.RID{Page: 5}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.releaseUndo(bg); err != nil {
		t.Fatal(err)
	}
	if segs := e.Undo().Segments(); !slices.Equal(segs, []uint64{uint64(tx.XID())}) {
		t.Fatalf("an active transaction's undo was released: %v", segs)
	}
	if _, err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	if segs := e.Undo().Segments(); len(segs) != 0 {
		t.Fatalf("the committed transaction's undo was kept: %v", segs)
	}
}

// A transaction's ID is active from the moment it is allocated: a
// snapshot taken before its TxnBegin record is logged, here one that
// failed to be, does not see it.
func TestBeginningTransactionIsActive(t *testing.T) {
	e := mustEngine(t, newFS(t), EngineOptions{Frames: 16})
	defer func() { _ = e.Abandon() }()
	tx := e.Begin()
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if err := tx.Write(ctx, func(context.Context) error { return nil }); err == nil {
		t.Fatal("setup: logging TxnBegin with a canceled context succeeded")
	}
	s := e.Snapshot()
	defer s.Release()
	if tx.XID() == 0 || tx.XID() >= s.XMax || s.Sees(uint64(tx.XID())) || e.LastWriter() != tx.XID() {
		t.Fatalf("transaction %d, snapshot %+v, last writer %d", tx.XID(), s, e.LastWriter())
	}
}
