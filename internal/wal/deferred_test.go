package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

func TestDeferredFreeRecordCodec(t *testing.T) {
	b := EncodeDeferredFree([]uint64{2, 9, 1 << 40}, []uint64{0, 77, 5})
	got, err := DecodeDeferredFree(b, 1000)
	if err != nil {
		t.Fatal(err)
	}
	want := []deferredFree{{2, 1000}, {9, 77}, {1 << 40, 5}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("%v, want %v", got, want)
	}
	if got, _ := DecodeDeferredFree(EncodeDeferredFree([]uint64{3}, nil), 42); got[0] != (deferredFree{3, 42}) {
		t.Fatalf("own-LSN entry: %v", got)
	}
	bad := map[string][]byte{
		"empty":          nil,
		"no entries":     EncodeDeferredFree(nil, nil),
		"short":          b[:len(b)-1],
		"long":           append(append([]byte(nil), b...), 0),
		"count too high": binary.LittleEndian.AppendUint32(nil, MaxDeferredPerRecord+1),
		"header page":    EncodeDeferredFree([]uint64{1}, nil),
		"page zero":      EncodeDeferredFree([]uint64{0}, nil),
	}
	for name, p := range bad {
		if _, err := DecodeDeferredFree(p, 1); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// emptyTree builds a tree, fills it, checkpoints, and deletes everything,
// so that its merges leave pages waiting to be freed. It returns the
// tree and how many pages wait.
func emptyTree(t *testing.T, e *Engine, n, valueSize int) (*btree.Tree, int) {
	t.Helper()
	tr, err := e.CreateBTree(bg)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if err := tr.Insert(bg, btreeKey(i), make([]byte, valueSize)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if _, err := tr.Delete(bg, btreeKey(i)); err != nil {
			t.Fatal(err)
		}
	}
	waiting := e.Logger().PendingFrees()
	if waiting < 10 {
		t.Fatalf("only %d pages unlinked", waiting)
	}
	return tr, waiting
}

func TestDeferredFreesSurviveACrash(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 32})
	tr, waiting := emptyTree(t, e, 3000, 100)
	free := e.FreePageCount()
	if err := e.Flush(bg); err != nil {
		t.Fatal(err)
	}
	m.Crash(vfs.CrashOptions{TearLast: true})
	e2 := mustEngine(t, m, EngineOptions{Frames: 32})
	defer func() { _ = e2.Close(bg) }()
	// Recovery put them back on the list, and its own checkpoint freed them.
	if got := e2.Recovery().DeferredFrees; got != waiting {
		t.Fatalf("recovery found %d deferred frees, want %d", got, waiting)
	}
	if got := e2.FreePageCount() - free; got != uint64(waiting) || e2.Logger().PendingFrees() != 0 {
		t.Fatalf("%d pages freed, %d pending; %d were waiting", got, e2.Logger().PendingFrees(), waiting)
	}
	tr2, err := e2.OpenBTree(bg, tr.Root())
	if err != nil {
		t.Fatal(err)
	}
	checkTree(t, tr2, map[int][]byte{})
}

func TestCheckpointLogsWaitingPagesAgain(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 32})
	page, err := e.dm.Allocate(bg)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		// A threshold at the log's end, as for a page unlinked just
		// before a checkpoint starts: it is at the redo point, so the
		// checkpoint must keep the page and log it again.
		e.Logger().restore(page, uint64(e.w.EndLSN()))
		if _, err := e.Checkpoint(bg); err != nil {
			t.Fatal(err)
		}
		if e.Logger().PendingFrees() != 1 {
			t.Fatal("the page left the list early")
		}
	}
	free := e.FreePageCount()
	m.Crash(vfs.CrashOptions{})
	e2 := mustEngine(t, m, EngineOptions{Frames: 32})
	defer func() { _ = e2.Close(bg) }()
	if e2.Recovery().DeferredFrees != 1 || e2.FreePageCount() != free+1 {
		t.Fatalf("recovery: %+v, free pages %d -> %d", e2.Recovery(), free, e2.FreePageCount())
	}
}

func TestDiscardedTxnsDeferNothing(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 32})
	page, err := e.dm.Allocate(bg)
	if err != nil {
		t.Fatal(err)
	}
	openTxn(t, e, func() error { return e.Logger().DeferFree(bg, page) })
	if err := e.w.Flush(bg); err != nil {
		t.Fatal(err)
	}
	free := e.FreePageCount()
	m.Crash(vfs.CrashOptions{})
	e2 := mustEngine(t, m, EngineOptions{Frames: 32})
	defer func() { _ = e2.Close(bg) }()
	if e2.Recovery().DeferredFrees != 0 || e2.FreePageCount() != free {
		t.Fatalf("an uncommitted transaction's deferred free was applied: %+v", e2.Recovery())
	}
}

func TestReplayRejectsBadDeferredFrees(t *testing.T) {
	for name, payload := range map[string][]byte{
		"malformed":       {1, 2, 3},
		"past the end":    EncodeDeferredFree([]uint64{1 << 30}, nil),
		"the header page": EncodeDeferredFree([]uint64{1}, nil),
	} {
		t.Run(name, func(t *testing.T) {
			m := newFS(t)
			e := mustEngine(t, m, EngineOptions{Frames: 8})
			if _, err := e.w.Append(bg, RecordDeferredFree, payload); err != nil {
				t.Fatal(err)
			}
			if err := e.w.Flush(bg); err != nil {
				t.Fatal(err)
			}
			m.Crash(vfs.CrashOptions{})
			if _, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 8}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("open: %v", err)
			}
		})
	}
}

// reachable returns every page of the trees.
func reachable(t *testing.T, trees ...*btree.Tree) map[uint64]bool {
	t.Helper()
	out := map[uint64]bool{}
	for _, tr := range trees {
		ids, err := tr.Pages(bg)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range ids {
			out[id] = true
		}
	}
	return out
}

func TestCheckpointFaultsNeverFreeTwiceOrFreeLivePages(t *testing.T) {
	// Pages wait to be freed; a checkpoint fails at its n-th write, sync or
	// rename; the power goes; recovery and more work follow. No page is
	// freed twice (that fails recovery with ErrDoubleFree) and no page of
	// the live tree is ever on the free list (checked by allocating every
	// free page).
	for _, op := range []vfs.Op{vfs.OpWriteAt, vfs.OpSync, vfs.OpRename} {
		failures := 0
		for n := 0; ; n++ {
			if n > 2000 {
				t.Fatalf("%v: still failing after %d calls", op, n)
			}
			m := newFS(t)
			e := mustEngine(t, m, EngineOptions{Frames: 16})
			emptied, _ := emptyTree(t, e, 400, 400)
			live, err := e.CreateBTree(bg)
			if err != nil {
				t.Fatal(err)
			}
			for i := range 400 {
				if err := live.Insert(bg, btreeKey(i), make([]byte, 50)); err != nil {
					t.Fatal(err)
				}
			}
			// Some pages wait past the next redo point too.
			late, err := e.dm.Allocate(bg)
			if err != nil {
				t.Fatal(err)
			}
			e.Logger().restore(late, uint64(e.w.EndLSN())+1<<20)
			m.InjectError(vfs.Fault{Op: op, After: n})
			_, cerr := e.Checkpoint(bg)
			m.ClearFaults()
			if cerr == nil {
				_ = e.Close(bg)
				if failures == 0 {
					t.Fatalf("%v: no call ever failed", op)
				}
				break
			}
			failures++
			m.Crash(vfs.CrashOptions{TearLast: n%2 == 1})
			for round := range 2 { // recovery, then recovery after more work
				e2, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 16})
				if err != nil {
					t.Fatalf("%v at %d, round %d: recovery: %v", op, n, round, err)
				}
				tr, err := e2.OpenBTree(bg, live.Root())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := e2.OpenBTree(bg, emptied.Root()); err != nil {
					t.Fatal(err)
				}
				if _, err := tr.Check(bg); err != nil {
					t.Fatalf("%v at %d: %v", op, n, err)
				}
				used := reachable(t, tr)
				used[emptied.Root()] = true
				var allocated []uint64
				for range e2.FreePageCount() {
					p, err := e2.dm.Allocate(bg)
					if err != nil {
						t.Fatal(err)
					}
					if used[p] {
						t.Fatalf("%v at %d, round %d: page %d of a live tree was free", op, n, round, p)
					}
					allocated = append(allocated, p)
				}
				for _, p := range allocated {
					if err := e2.bp.DeletePage(bg, p); err != nil {
						t.Fatal(err)
					}
				}
				if round == 0 {
					for i := range 300 {
						if _, err := tr.Delete(bg, btreeKey(i)); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := e2.Flush(bg); err != nil {
					t.Fatal(err)
				}
				m.Crash(vfs.CrashOptions{})
			}
		}
		t.Logf("%v: %d failing checkpoints", op, failures)
	}
}
