package wal

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"path"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

func mustEngine(t testing.TB, fsys vfs.FS, opts EngineOptions) *Engine {
	t.Helper()
	e, err := OpenEngine(bg, fsys, dbDir, opts)
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	return e
}

func TestEngineCreateReopenClose(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 8})
	if r := e.Recovery(); r.Replayed != 0 {
		t.Fatalf("fresh database replayed %d records", r.Replayed)
	}
	h, err := e.CreateHeap(bg)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(1, 2))
	want := map[storage.RID][]byte{}
	for range 300 {
		d := payload(rng, 1+rng.IntN(3000))
		rid, err := heapInsert(h, d)
		if err != nil {
			t.Fatal(err)
		}
		want[rid] = d
	}
	if err := e.Close(bg); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(bg); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := e.CreateHeap(bg); !errors.Is(err, ErrClosed) {
		t.Fatalf("CreateHeap after Close: %v", err)
	}
	if err := e.Flush(bg); !errors.Is(err, ErrClosed) {
		t.Fatalf("Flush after Close: %v", err)
	}
	if _, err := e.Checkpoint(bg); !errors.Is(err, ErrClosed) {
		t.Fatalf("Checkpoint after Close: %v", err)
	}

	e2 := mustEngine(t, m, EngineOptions{Frames: 8})
	// A clean close ends with a checkpoint: nothing to replay.
	if r := e2.Recovery(); r.Replayed != 0 {
		t.Fatalf("replayed %d records after a clean close", r.Replayed)
	}
	h2, err := e2.OpenHeap(bg, h.FirstPage())
	if err != nil {
		t.Fatal(err)
	}
	for rid, d := range want {
		got, err := heapGet(h2, rid)
		if err != nil || !bytes.Equal(got, d) {
			t.Fatalf("row %s after reopen: %v", rid, err)
		}
	}
	if err := e2.Close(bg); err != nil {
		t.Fatal(err)
	}
}

func TestEngineRecoversAfterCrashAndReportsIt(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 4, WAL: Options{SegmentSize: 8192}})
	h, err := e.CreateHeap(bg)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(3, 4))
	var rids []storage.RID
	heapWork(t, h, rng, &rids, 200)
	if _, err := e.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	heapWork(t, h, rng, &rids, 100)
	rows := map[storage.RID][]byte{}
	for _, rid := range rids {
		d, err := heapGet(h, rid)
		if err != nil {
			t.Fatal(err)
		}
		rows[rid] = d
	}
	if err := e.Flush(bg); err != nil {
		t.Fatal(err)
	}
	m.Crash(vfs.CrashOptions{TearLast: true})

	e2 := mustEngine(t, m, EngineOptions{Frames: 4, WAL: Options{SegmentSize: 8192}})
	rec := e2.Recovery()
	if rec.Replayed == 0 || rec.RedoLSN <= SegmentHeaderSize {
		t.Fatalf("recovery %+v: expected to replay from the checkpoint", rec)
	}
	h2, err := e2.OpenHeap(bg, h.FirstPage())
	if err != nil {
		t.Fatal(err)
	}
	// Everything was acknowledged, so the heap is exactly what it was.
	// (Unacknowledged work is checked precisely by tests/crash.)
	got := scanEngineHeap(t, h2)
	if len(got) != len(rows) {
		t.Fatalf("%d rows after recovery, want %d", len(got), len(rows))
	}
	for rid, d := range rows {
		if !bytes.Equal(got[rid], d) {
			t.Fatalf("row %s differs after recovery", rid)
		}
	}
	if err := e2.Close(bg); err != nil {
		t.Fatal(err)
	}
}

func scanEngineHeap(t testing.TB, h *storage.Heap) map[storage.RID][]byte {
	t.Helper()
	out := map[storage.RID][]byte{}
	s := h.Scan()
	for {
		rid, d, ok, err := heapNext(s)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return out
		}
		out[rid] = d
	}
}

func TestEngineRejectsBadState(t *testing.T) {
	if _, err := OpenEngine(bg, newFS(t), dbDir, EngineOptions{Frames: 3}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("one frame: %v", err)
	}
	t.Run("data file lost", func(t *testing.T) {
		m := newFS(t)
		e := mustEngine(t, m, EngineOptions{Frames: 4})
		if _, err := e.CreateHeap(bg); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(bg); err != nil {
			t.Fatal(err)
		}
		if err := m.Remove(path.Join(dbDir, DataFileName)); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 4}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("err = %v, want ErrCorrupt", err)
		}
	})
	t.Run("unknown record type", func(t *testing.T) {
		m := newFS(t)
		e := mustEngine(t, m, EngineOptions{Frames: 4})
		if _, err := e.w.Append(bg, 99, []byte("from the future")); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(bg); err != nil {
			t.Fatal(err)
		}
		m.Crash(vfs.CrashOptions{})
		if _, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 4}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("err = %v, want ErrCorrupt", err)
		}
	})
	t.Run("malformed heap record", func(t *testing.T) {
		m := newFS(t)
		e := mustEngine(t, m, EngineOptions{Frames: 4})
		if _, err := e.w.Append(bg, RecordHeap, []byte{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(bg); err != nil {
			t.Fatal(err)
		}
		m.Crash(vfs.CrashOptions{})
		if _, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 4}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("err = %v, want ErrCorrupt", err)
		}
	})
}

func TestEngineOpenFailsCleanlyOnIOErrors(t *testing.T) {
	ops := []vfs.Op{vfs.OpOpenFile, vfs.OpWriteAt, vfs.OpSync, vfs.OpSyncDir, vfs.OpRename, vfs.OpList, vfs.OpMkdirAll, vfs.OpReadAt}
	for _, op := range ops {
		for after := range 5 {
			m := newFS(t)
			e := mustEngine(t, m, EngineOptions{Frames: 4})
			h, err := e.CreateHeap(bg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := heapInsert(h, []byte("acknowledged")); err != nil {
				t.Fatal(err)
			}
			if err := e.Flush(bg); err != nil {
				t.Fatal(err)
			}
			m.Crash(vfs.CrashOptions{})
			m.InjectError(vfs.Fault{Op: op, After: after})
			if e2, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 4}); err == nil {
				_ = e2.Close(bg)
			} else if !errors.Is(err, vfs.ErrInjected) {
				t.Fatalf("%v/%d: unexpected error %v", op, after, err)
			}
			m.ClearFaults()
			m.Crash(vfs.CrashOptions{TearLast: true})
			// Whatever the failed open did, the next one recovers the row.
			e3 := mustEngine(t, m, EngineOptions{Frames: 4})
			h3, err := e3.OpenHeap(bg, h.FirstPage())
			if err != nil {
				t.Fatalf("%v/%d: %v", op, after, err)
			}
			rows := scanEngineHeap(t, h3)
			if len(rows) != 1 {
				t.Fatalf("%v/%d: %d rows", op, after, len(rows))
			}
			if err := e3.Close(bg); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// Recovery ends with a checkpoint: crashing again right after it replays
// nothing, instead of the same log all over again.
func TestRecoveryEndsWithACheckpoint(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 4})
	h, err := e.CreateHeap(bg)
	if err != nil {
		t.Fatal(err)
	}
	var rids []storage.RID
	heapWork(t, h, rand.New(rand.NewPCG(5, 6)), &rids, 150)
	if err := e.Flush(bg); err != nil {
		t.Fatal(err)
	}
	m.Crash(vfs.CrashOptions{})
	e2 := mustEngine(t, m, EngineOptions{Frames: 4})
	if e2.Recovery().Replayed == 0 {
		t.Fatal("first recovery replayed nothing")
	}
	m.Crash(vfs.CrashOptions{}) // no new work at all
	e3 := mustEngine(t, m, EngineOptions{Frames: 4})
	if n := e3.Recovery().Replayed; n != 0 {
		t.Fatalf("second recovery replayed %d records; the first did not end with a checkpoint", n)
	}
	if err := e3.Close(bg); err != nil {
		t.Fatal(err)
	}
}
