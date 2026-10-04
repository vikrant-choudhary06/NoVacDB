package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/undo"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

// undoRecord returns a random record of xid.
func undoRecord(rng *rand.Rand, xid XID) undo.Record {
	rec := undo.Record{
		Kind:  undo.Kind(1 + rng.IntN(3)),
		XID:   uint64(xid),
		Table: rng.Uint64N(5),
		RID:   storage.RID{Page: storage.FirstDataPage + rng.Uint64N(100), Slot: uint16(rng.IntN(50))},
	}
	if rec.Kind != undo.Insert {
		n := 1 + rng.IntN(300)
		if rng.IntN(15) == 0 {
			n = 1 + rng.IntN(undo.MaxImage)
		}
		rec.Image = make([]byte, n)
		for i := range rec.Image {
			rec.Image[i] = byte(rng.Uint32())
		}
	}
	return rec
}

// writtenUndo is what a committed transaction wrote to the undo log.
type writtenUndo struct {
	ptrs []undo.Ptr
	recs []undo.Record
}

// appendUndo appends n random records in tx, in one or more writes.
func appendUndo(t *testing.T, e *Engine, tx *Txn, rng *rand.Rand, n int) writtenUndo {
	t.Helper()
	var w writtenUndo
	for done := 0; done < n; {
		k := min(n-done, 1+rng.IntN(5))
		err := tx.Write(bg, func(ctx context.Context) error {
			for range k {
				rec := undoRecord(rng, tx.XID())
				p, err := e.Undo().Append(ctx, rec)
				if err != nil {
					return err
				}
				w.ptrs, w.recs = append(w.ptrs, p), append(w.recs, rec)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		done += k
	}
	return w
}

// checkUndo checks that the undo log holds exactly the segments in want,
// with their records.
func checkUndo(t *testing.T, e *Engine, want map[XID]writtenUndo, what string) {
	t.Helper()
	l := e.Undo()
	var xids []uint64
	for xid := range want {
		xids = append(xids, uint64(xid))
	}
	slices.Sort(xids)
	if got := l.Segments(); !slices.Equal(got, xids) {
		t.Fatalf("%s: segments %v, want %v", what, got, xids)
	}
	for xid, w := range want {
		if l.Last(uint64(xid)) != w.ptrs[len(w.ptrs)-1] {
			t.Fatalf("%s: transaction %d's last record is %s, want %s", what, xid, l.Last(uint64(xid)), w.ptrs[len(w.ptrs)-1])
		}
		for i, p := range w.ptrs {
			rec, err := l.Read(bg, p)
			if err != nil {
				t.Fatalf("%s: record %d of transaction %d: %v", what, i, xid, err)
			}
			wantPrev := undo.Ptr(0)
			if i > 0 {
				wantPrev = w.ptrs[i-1]
			}
			r := w.recs[i]
			if rec.Kind != r.Kind || rec.XID != r.XID || rec.Table != r.Table || rec.RID != r.RID ||
				rec.PrevForRow != r.PrevForRow || rec.PrevInTxn != wantPrev || !bytes.Equal(rec.Image, r.Image) {
				t.Fatalf("%s: record %d of transaction %d differs: %+v", what, i, xid, rec)
			}
		}
	}
}

// mustUndoEngine opens an engine that keeps committed transactions' undo,
// as Step 6.4 will: these tests are about the undo log itself.
func mustUndoEngine(t *testing.T, fsys vfs.FS, opts EngineOptions) *Engine {
	t.Helper()
	e := mustEngine(t, fsys, opts)
	e.keepUndo = true
	return e
}

// Committed transactions' undo survives power cuts, torn writes and
// checkpoints at random points; an uncommitted transaction leaves no
// segment; released segments stay released.
func TestUndoSurvivesCrashes(t *testing.T) {
	base := testSeed(t)
	runs := 40
	if testing.Short() {
		runs = 8
	}
	var torn, discarded, released, grown int
	for run := range runs {
		seed := base + uint64(run)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 61))
			m := vfs.NewMemFS(seed)
			_ = m.MkdirAll("/db")
			_ = m.SyncDir("/")
			opts := EngineOptions{Frames: 8 + rng.IntN(24), WAL: Options{SegmentSize: []int64{8192, 1 << 20}[rng.IntN(2)]}}
			e := mustUndoEngine(t, m, opts)
			want := map[XID]writtenUndo{}
			for cycle := range 3 {
				for range 1 + rng.IntN(8) {
					if rng.IntN(4) == 0 {
						if _, err := e.Checkpoint(bg); err != nil {
							t.Fatal(err)
						}
					}
					if len(want) > 0 && rng.IntN(4) == 0 {
						// Release a segment, in a transaction of its own.
						var victim XID
						for xid := range want {
							victim = xid
							break
						}
						tx := e.Begin()
						if err := tx.Write(bg, func(ctx context.Context) error { return e.Undo().Release(ctx, uint64(victim)) }); err != nil {
							t.Fatal(err)
						}
						if _, err := tx.Commit(bg); err != nil {
							t.Fatal(err)
						}
						delete(want, victim)
						released++
						continue
					}
					tx := e.Begin()
					w := appendUndo(t, e, tx, rng, 1+rng.IntN(40))
					if _, err := tx.Commit(bg); err != nil {
						t.Fatal(err)
					}
					if len(e.Undo().Pages(uint64(tx.XID()))) > 1 {
						grown++
					}
					want[tx.XID()] = w
				}
				// A transaction in flight when the crash comes.
				tx := e.Begin()
				appendUndo(t, e, tx, rng, 1+rng.IntN(30))
				flight := tx.XID()
				if rng.IntN(2) == 0 {
					_ = e.w.Flush(bg) // its records durable, the commit not
				}
				opt := vfs.CrashOptions{TearLast: rng.IntN(2) == 0}
				if opt.TearLast {
					torn++
				}
				m.Crash(opt)
				e = mustUndoEngine(t, m, opts)
				if e.Recovery().DiscardedTransactions > 0 {
					discarded++
				}
				if e.Recovery().UndoSegments != len(want) {
					t.Fatalf("cycle %d: recovery found %d segments, want %d", cycle, e.Recovery().UndoSegments, len(want))
				}
				if e.Undo().Last(uint64(flight)) != 0 {
					t.Fatalf("cycle %d: uncommitted transaction %d has undo", cycle, flight)
				}
				checkUndo(t, e, want, fmt.Sprintf("cycle %d", cycle))
			}
			if err := e.Close(bg); err != nil {
				t.Fatal(err)
			}
			e = mustUndoEngine(t, m, opts)
			checkUndo(t, e, want, "after a clean restart")
			if err := e.Close(bg); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Logf("%d runs: %d torn crashes, %d discarded transactions, %d releases, %d multi-page segments", runs, torn, discarded, released, grown)
	if runs >= 40 && (torn < runs/3 || discarded < runs/3 || released < runs || grown < runs) {
		t.Fatalf("workload too tame")
	}
}

// The segment table survives once the log that created the segments has
// been removed: checkpoints log it again.
func TestUndoSegmentTableSurvivesLogTrimming(t *testing.T) {
	m := newFS(t)
	opts := EngineOptions{Frames: 16, WAL: Options{SegmentSize: 8192}}
	e := mustUndoEngine(t, m, opts)
	rng := rand.New(rand.NewPCG(testSeed(t), 62))
	want := map[XID]writtenUndo{}
	for range 5 {
		tx := e.Begin()
		w := appendUndo(t, e, tx, rng, 20)
		if _, err := tx.Commit(bg); err != nil {
			t.Fatal(err)
		}
		want[tx.XID()] = w
	}
	firstBefore, err := FirstLSN(m, "/db/wal")
	if err != nil {
		t.Fatal(err)
	}
	// Other traffic and checkpoints until the segments' records are gone.
	h, tr := setupHeapAndTree(t, e)
	for s := 1; s <= 30; s++ {
		if err := runStatement(e, h, tr, s, 5); err != nil {
			t.Fatal(err)
		}
		if _, err := e.Checkpoint(bg); err != nil {
			t.Fatal(err)
		}
	}
	first, err := FirstLSN(m, "/db/wal")
	if err != nil {
		t.Fatal(err)
	}
	recs, _ := readLog(t, m, "/db/wal")
	for _, r := range recs {
		if r.Type == RecordUndo {
			t.Fatalf("an undo record at %d is still in the log (first LSN %d, was %d)", r.LSN, first, firstBefore)
		}
	}
	m.Crash(vfs.CrashOptions{TearLast: true})
	e = mustUndoEngine(t, m, opts)
	checkUndo(t, e, want, "after trimming and a crash")
	if err := e.Close(bg); err != nil {
		t.Fatal(err)
	}
	e = mustUndoEngine(t, m, opts)
	defer func() { _ = e.Close(bg) }()
	checkUndo(t, e, want, "after a restart")
}

// A released segment's pages return to the free list, even when a crash
// comes right after the release.
func TestReleasedUndoPagesAreFreed(t *testing.T) {
	for _, crash := range []bool{false, true} {
		t.Run(fmt.Sprintf("crash=%v", crash), func(t *testing.T) {
			m := newFS(t)
			opts := EngineOptions{Frames: 16}
			e := mustUndoEngine(t, m, opts)
			tx := e.Begin()
			if err := tx.Write(bg, func(ctx context.Context) error {
				for range 20 {
					if _, err := e.Undo().Append(ctx, undo.Record{Kind: undo.Delete, XID: uint64(tx.XID()), RID: storage.RID{Page: 5}, Image: make([]byte, 4000)}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(bg); err != nil {
				t.Fatal(err)
			}
			xid := uint64(tx.XID())
			pages := e.Undo().Pages(xid)
			if len(pages) != 10 {
				t.Fatalf("%d pages", len(pages))
			}
			if _, err := e.Checkpoint(bg); err != nil {
				t.Fatal(err)
			}
			free := e.FreePageCount()
			tx = e.Begin()
			if err := tx.Write(bg, func(ctx context.Context) error { return e.Undo().Release(ctx, xid) }); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(bg); err != nil {
				t.Fatal(err)
			}
			if crash {
				m.Crash(vfs.CrashOptions{TearLast: true})
				e = mustUndoEngine(t, m, opts)
				if len(e.Undo().Segments()) != 0 {
					t.Fatalf("segments after the crash: %v", e.Undo().Segments())
				}
			}
			// Two checkpoints: the one whose redo point passes the release
			// frees the pages.
			for range 2 {
				if _, err := e.Checkpoint(bg); err != nil {
					t.Fatal(err)
				}
			}
			if got := e.FreePageCount(); got != free+uint64(len(pages)) {
				t.Fatalf("%d free pages, want %d", got, free+uint64(len(pages)))
			}
			if _, err := e.Undo().Read(bg, undo.MakePtr(pages[0], 48)); !errors.Is(err, undo.ErrCorrupt) {
				t.Fatalf("reading a freed page: %v", err)
			}
			if err := e.Close(bg); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A damaged Undo or UndoSegment record stops recovery as corruption.
func TestDamagedUndoRecordsAreCorrupt(t *testing.T) {
	for _, typ := range []RecordType{RecordUndo, RecordUndoSegment} {
		t.Run(fmt.Sprintf("type=%d", typ), func(t *testing.T) {
			m := newFS(t)
			e := mustEngine(t, m, EngineOptions{Frames: 8})
			w := e.w
			if _, err := w.Append(bg, typ, []byte{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(bg); err != nil {
				t.Fatal(err)
			}
			m.Crash(vfs.CrashOptions{})
			if _, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 8}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v", err)
			}
		})
	}
	// A segment whose first page is a heap page.
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 8})
	h, _ := setupHeapAndTree(t, e)
	entry := undo.SegmentEntry{Op: undo.SegmentAdd, XID: 1, First: h.FirstPage()}
	if _, err := e.w.Append(bg, RecordUndoSegment, undo.EncodeSegmentEntries([]undo.SegmentEntry{entry})); err != nil {
		t.Fatal(err)
	}
	if err := e.w.Flush(bg); err != nil {
		t.Fatal(err)
	}
	m.Crash(vfs.CrashOptions{})
	if _, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 8}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("segment at a heap page: err = %v", err)
	}
}
