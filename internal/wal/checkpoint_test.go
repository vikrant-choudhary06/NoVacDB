package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"path"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

const (
	dbDir    = "/db"
	dataFile = "/db/data"
)

// --- control file ------------------------------------------------------------------

func TestControlRoundTripAndLayout(t *testing.T) {
	for _, c := range []Control{{}, {CheckpointLSN: 100, RedoLSN: 32}, {CheckpointLSN: 1<<64 - 1, RedoLSN: 1<<64 - 1}} {
		got, err := DecodeControl(AppendControl(nil, c))
		if err != nil || got != c {
			t.Fatalf("%+v -> %+v, %v", c, got, err)
		}
	}
	enc := AppendControl(nil, Control{CheckpointLSN: 0x0102, RedoLSN: 0x0304})
	want := make([]byte, ControlSize)
	copy(want[4:], "NOVACTL\x00")
	binary.LittleEndian.PutUint32(want[12:], 4) // format version 4 (Step 6.3)
	binary.LittleEndian.PutUint64(want[16:], 0x0102)
	binary.LittleEndian.PutUint64(want[24:], 0x0304)
	binary.LittleEndian.PutUint32(want, crc32c(want[4:]))
	if !bytes.Equal(enc, want) {
		t.Fatalf("control = % x\nwant      % x", enc, want)
	}
	for i := range enc {
		b := bytes.Clone(enc)
		b[i] ^= 1
		if _, err := DecodeControl(b); err == nil {
			t.Fatalf("flip of byte %d undetected", i)
		}
	}
}

func TestDecodeControlRejects(t *testing.T) {
	reseal := func(f func(b []byte)) []byte {
		b := AppendControl(nil, Control{CheckpointLSN: 500, RedoLSN: 300})
		f(b)
		binary.LittleEndian.PutUint32(b, crc32c(b[4:]))
		return b
	}
	cases := []struct {
		name string
		buf  []byte
		want error
	}{
		{"empty", nil, ErrCorrupt},
		{"short", AppendControl(nil, Control{})[:31], ErrCorrupt},
		{"long", append(AppendControl(nil, Control{}), 0), ErrCorrupt},
		{"zeros", make([]byte, ControlSize), ErrCorrupt},
		{"magic", reseal(func(b []byte) { b[4] = 'X' }), ErrCorrupt},
		{"future version", reseal(func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 5) }), ErrUnsupportedVersion},
		{"version 3", reseal(func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 3) }), ErrUnsupportedVersion},
		{"version 2", reseal(func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 2) }), ErrUnsupportedVersion},
		{"version 1", reseal(func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 1) }), ErrUnsupportedVersion},
		{"redo after checkpoint", reseal(func(b []byte) { binary.LittleEndian.PutUint64(b[24:], 501) }), ErrCorrupt},
	}
	for _, tc := range cases {
		if _, err := DecodeControl(tc.buf); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
}

func FuzzDecodeControl(f *testing.F) {
	f.Add(AppendControl(nil, Control{CheckpointLSN: 100, RedoLSN: 50}))
	f.Add(make([]byte, ControlSize))
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, buf []byte) {
		c, err := DecodeControl(buf)
		if err != nil {
			return
		}
		if !bytes.Equal(AppendControl(nil, c), buf) {
			t.Fatal("accepted control file is not canonical")
		}
	})
}

func TestReadControlMissingAndDamaged(t *testing.T) {
	m := newFS(t)
	if _, ok, err := ReadControl(m, dbDir); ok || err != nil {
		t.Fatalf("missing: ok=%v err=%v", ok, err)
	}
	writeFile(t, m, path.Join(dbDir, ControlFileName), []byte("garbage"))
	if _, _, err := ReadControl(m, dbDir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("damaged: err = %v", err)
	}
}

// Replacing the control file must be atomic: after a crash at any point the
// file holds the old content or the new, never something else.
func TestWriteControlIsAtomicUnderCrashes(t *testing.T) {
	old := Control{CheckpointLSN: 1000, RedoLSN: 900}
	next := Control{CheckpointLSN: 5000, RedoLSN: 4000}
	for _, withOld := range []bool{true, false} {
		for _, op := range []vfs.Op{vfs.OpOpenFile, vfs.OpWriteAt, vfs.OpSync, vfs.OpRename, vfs.OpSyncDir} {
			for _, tear := range []bool{false, true} {
				name := fmt.Sprintf("old=%v/%v/tear=%v", withOld, op, tear)
				t.Run(name, func(t *testing.T) {
					m := newFS(t)
					if withOld {
						if err := WriteControl(m, dbDir, old); err != nil {
							t.Fatal(err)
						}
					}
					m.InjectError(vfs.Fault{Op: op})
					writeErr := WriteControl(m, dbDir, next)
					m.ClearFaults()
					m.Crash(vfs.CrashOptions{TearLast: tear})
					got, ok, err := ReadControl(m, dbDir)
					if err != nil {
						t.Fatalf("control file damaged by a crash (write err %v): %v", writeErr, err)
					}
					switch {
					case ok && got == next:
						if writeErr != nil && op != vfs.OpSyncDir {
							t.Fatalf("new content visible although the write failed before the rename (%v)", writeErr)
						}
					case ok && got == old && withOld:
					case !ok && !withOld:
					default:
						t.Fatalf("control file holds %+v (exists=%v)", got, ok)
					}
				})
			}
		}
	}
}

// --- removing old segments -------------------------------------------------------------

func TestRemoveSegmentsBefore(t *testing.T) {
	m, recs := buildLog(t, 60, 150)
	_, segs := readLog(t, m, testDir)
	if len(segs) < 6 {
		t.Fatalf("setup: %d segments", len(segs))
	}
	w := mustOpen(t, m, Options{SegmentSize: 150})

	// A cut inside segment 3 removes segments 0..2 only.
	cut := segs[3].Start + 40
	n, err := w.RemoveSegmentsBefore(bg, cut)
	if err != nil || n != 3 {
		t.Fatalf("removed %d, %v; want 3", n, err)
	}
	if first, _ := FirstLSN(m, testDir); first != segs[3].Start+SegmentHeaderSize {
		t.Fatalf("FirstLSN %d", first)
	}
	// Exactly at a segment boundary: that segment ends there, so it goes.
	if n, err := w.RemoveSegmentsBefore(bg, segs[4].Start); err != nil || n != 1 {
		t.Fatalf("boundary cut removed %d, %v", n, err)
	}
	// Never the segment being written, even if asked to remove everything.
	if _, err := w.RemoveSegmentsBefore(bg, 1<<62); err != nil {
		t.Fatal(err)
	}
	names, _ := m.List(testDir)
	if len(names) != 1 || names[0] != SegmentName(segs[len(segs)-1].Start) {
		t.Fatalf("left %v", names)
	}
	// The log keeps working and reopens.
	lsn := mustAppend(t, w, 1, []byte("after trimming"))
	mustClose(t, w)
	w2 := mustOpen(t, m, Options{SegmentSize: 150})
	got, _ := readAll(t, m, segs[len(segs)-1].Start)
	if got[len(got)-1].LSN != lsn {
		t.Fatalf("last record %v", got[len(got)-1])
	}
	mustClose(t, w2)
	_ = recs
}

func TestRemoveSegmentsSurvivesCrashesPartWay(t *testing.T) {
	for _, op := range []vfs.Op{vfs.OpRemove, vfs.OpSyncDir} {
		for after := range 4 {
			t.Run(fmt.Sprintf("%v/%d", op, after), func(t *testing.T) {
				m, recs := buildLog(t, 60, 150)
				_, segs := readLog(t, m, testDir)
				w := mustOpen(t, m, Options{SegmentSize: 150})
				m.InjectError(vfs.Fault{Op: op, After: after})
				_, rmErr := w.RemoveSegmentsBefore(bg, segs[len(segs)-2].Start)
				m.ClearFaults()
				m.Crash(vfs.CrashOptions{TearLast: true})
				w2, err := Open(m, testDir, Options{SegmentSize: 150})
				if err != nil {
					t.Fatalf("log unusable after a crash while trimming (%v): %v", rmErr, err)
				}
				first, err := FirstLSN(m, testDir)
				if err != nil {
					t.Fatal(err)
				}
				// What is left reads back as exactly the tail of the records.
				got, _ := readAll(t, m, first)
				var want []logRec
				for _, r := range recs {
					if r.LSN >= first {
						want = append(want, r)
					}
				}
				sameRecs(t, got, want, "remaining log")
				mustClose(t, w2)
			})
		}
	}
}

// --- checkpoints and replaying only what is needed -----------------------------------

// testDB is a database put together by hand from the parts (Step 2.5 wraps
// this in an Engine): open the log (repairing its tail), open the data file,
// find the redo start, replay from it, and set up the checkpointer.
type testDB struct {
	m        *vfs.MemFS
	w        *Writer
	dm       *storage.DiskManager
	bp       *storage.BufferPool
	lg       *Logger
	ck       *Checkpointer
	redo     LSN
	replayed int
}

func openTestDB(t testing.TB, m *vfs.MemFS, frames int, segSize int64) *testDB {
	t.Helper()
	w, err := Open(m, testDir, Options{SegmentSize: segSize})
	if err != nil {
		t.Fatalf("opening log: %v", err)
	}
	dm, err := storage.Open(m, dataFile)
	if errors.Is(err, vfs.ErrNotExist) {
		dm, err = storage.Create(m, dataFile)
	}
	if err != nil {
		t.Fatalf("opening data file: %v", err)
	}
	redo, err := RedoStart(m, dbDir, testDir)
	if err != nil {
		t.Fatalf("RedoStart: %v", err)
	}
	flushed, force := RuleHooks(w)
	bp, err := storage.NewBufferPool(dm, storage.Options{Frames: frames, FlushedLSN: flushed, FlushWAL: force})
	if err != nil {
		t.Fatal(err)
	}
	db := &testDB{m: m, w: w, dm: dm, bp: bp, redo: redo}
	r, err := NewReader(m, testDir, redo)
	if err != nil {
		t.Fatalf("reading from redo %d: %v", redo, err)
	}
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		switch rec.Type {
		case RecordHeap:
			if err := storage.RedoHeapRecord(bg, bp, uint64(rec.LSN), rec.Payload); err != nil {
				t.Fatalf("redo %d: %v", rec.LSN, err)
			}
			db.replayed++
		case RecordCheckpoint:
		default:
			t.Fatalf("record type %d", rec.Type)
		}
	}
	if r.End() != w.EndLSN() {
		t.Fatalf("replay ended at %d, log ends at %d", r.End(), w.EndLSN())
	}
	db.lg = NewLogger(w, redo)
	db.ck = NewCheckpointer(m, dbDir, w, db.lg, bp, dm)
	return db
}

// logicalPages returns every page as the database sees it (through the pool,
// with the checksum field cleared because in-memory pages are not sealed).
func logicalPages(t testing.TB, db *testDB) map[uint64][]byte {
	t.Helper()
	out := map[uint64][]byte{}
	for id := uint64(storage.FirstDataPage); id < db.dm.PageCount(); id++ {
		ref, err := db.bp.FetchPage(bg, id)
		if err != nil {
			t.Fatalf("page %d: %v", id, err)
		}
		ref.RLock()
		b := bytes.Clone(ref.Data())
		ref.RUnlock()
		_ = ref.Unpin(false)
		clear(b[16:20])
		out[id] = b
	}
	return out
}

func sameLogical(t testing.TB, got, want map[uint64][]byte, ctx string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d pages, want %d", ctx, len(got), len(want))
	}
	for id, w := range want {
		if !bytes.Equal(got[id], w) {
			t.Fatalf("%s: page %d differs", ctx, id)
		}
	}
}

// heapWork runs n random logged operations on h.
func heapWork(t testing.TB, h *storage.Heap, rng *rand.Rand, rids *[]storage.RID, n int) {
	t.Helper()
	for range n {
		switch op := rng.IntN(10); {
		case op < 5 || len(*rids) == 0:
			rid, err := heapInsert(h, payload(rng, 1+rng.IntN(2500)))
			if err != nil {
				t.Fatalf("insert: %v", err)
			}
			*rids = append(*rids, rid)
		case op < 8:
			j := rng.IntN(len(*rids))
			err := heapUpdate(h, (*rids)[j], payload(rng, 1+rng.IntN(5000)))
			if err != nil {
				t.Fatalf("update: %v", err)
			}
		default:
			j := rng.IntN(len(*rids))
			if err := heapDelete(h, (*rids)[j]); err != nil {
				t.Fatalf("delete: %v", err)
			}
			*rids = append((*rids)[:j], (*rids)[j+1:]...)
		}
	}
}

func countRecords(t testing.TB, fsys vfs.FS, from LSN) (heap int) {
	t.Helper()
	r, err := NewReader(fsys, testDir, from)
	if err != nil {
		t.Fatal(err)
	}
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			return heap
		}
		if err != nil {
			t.Fatal(err)
		}
		if rec.Type == RecordHeap {
			heap++
		}
	}
}

// The step's acceptance: after a checkpoint, recovery replays only the log
// written since its redo point (older segments are gone), and the database it
// rebuilds is exactly the one that crashed, torn pages included.
func TestRecoveryAfterCheckpointReplaysOnlyWhatIsNeeded(t *testing.T) {
	base := testSeed(t)
	runs := 40
	if testing.Short() {
		runs = 8
	}
	for i := range runs {
		seed := base + uint64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 31))
			m := newFS(t)
			segSize := []int64{4096, 16384, 1 << 20}[rng.IntN(3)]
			frames := 3 + rng.IntN(10)
			db := openTestDB(t, m, frames, segSize)
			cl := &countingLogger{Logger: db.lg}
			h, err := storage.CreateHeap(bg, db.bp, storage.WithLogger(cl))
			if err != nil {
				t.Fatal(err)
			}
			var rids []storage.RID
			var last Control
			for range 1 + rng.IntN(4) {
				heapWork(t, h, rng, &rids, 50+rng.IntN(150))
				if last, err = db.ck.Checkpoint(bg); err != nil {
					t.Fatal(err)
				}
			}
			after := rng.IntN(120) // work after the last checkpoint
			heapWork(t, h, rng, &rids, after)
			want := logicalPages(t, db)
			mustFlush(t, db.w) // everything acknowledged
			needed := countRecords(t, m, last.RedoLSN)
			m.Crash(vfs.CrashOptions{TearLast: rng.IntN(2) == 0})

			db2 := openTestDB(t, m, 2+rng.IntN(10), segSize)
			if db2.redo != last.RedoLSN {
				t.Fatalf("recovery started at %d, last checkpoint's redo point is %d", db2.redo, last.RedoLSN)
			}
			if db2.replayed != needed {
				t.Fatalf("replayed %d records, %d were logged since the redo point", db2.replayed, needed)
			}
			if total := int(cl.n.Load()); db2.replayed >= total {
				t.Fatalf("replayed %d records of the %d ever logged: the checkpoint saved nothing", db2.replayed, total)
			}
			if first, _ := FirstLSN(m, testDir); segSize < 1<<20 && first <= SegmentHeaderSize {
				t.Fatalf("no old segment was removed (log still starts at %d)", first)
			}
			sameLogical(t, logicalPages(t, db2), want, "recovered database")
			// And the recovered heap reads back.
			h2, err := storage.OpenHeap(bg, db2.bp, h.FirstPage(), storage.WithLogger(db2.lg))
			if err != nil {
				t.Fatal(err)
			}
			for _, rid := range rids {
				if _, err := heapGet(h2, rid); err != nil {
					t.Fatalf("row %s after recovery: %v", rid, err)
				}
			}
		})
	}
}

// A crash in the middle of a checkpoint leaves the previous checkpoint in
// force; recovery from it rebuilds the same database.
func TestCrashDuringCheckpoint(t *testing.T) {
	ops := []vfs.Op{vfs.OpWriteAt, vfs.OpSync, vfs.OpOpenFile, vfs.OpRename, vfs.OpSyncDir, vfs.OpRemove}
	for _, op := range ops {
		for after := range 6 {
			t.Run(fmt.Sprintf("%v/%d", op, after), func(t *testing.T) {
				rng := rand.New(rand.NewPCG(uint64(after), uint64(op)))
				m := newFS(t)
				db := openTestDB(t, m, 4, 4096)
				h, err := storage.CreateHeap(bg, db.bp, storage.WithLogger(db.lg))
				if err != nil {
					t.Fatal(err)
				}
				var rids []storage.RID
				heapWork(t, h, rng, &rids, 120)
				prev, err := db.ck.Checkpoint(bg)
				if err != nil {
					t.Fatal(err)
				}
				heapWork(t, h, rng, &rids, 120)
				want := logicalPages(t, db)
				mustFlush(t, db.w)

				m.InjectError(vfs.Fault{Op: op, After: after})
				ctl, ckErr := db.ck.Checkpoint(bg)
				m.ClearFaults()
				m.Crash(vfs.CrashOptions{TearLast: true})

				db2 := openTestDB(t, m, 4, 4096)
				if ckErr != nil && db2.redo != prev.RedoLSN && db2.redo != ctl.RedoLSN {
					t.Fatalf("after a failed checkpoint recovery started at %d (previous %d)", db2.redo, prev.RedoLSN)
				}
				sameLogical(t, logicalPages(t, db2), want, fmt.Sprintf("after a crash during a checkpoint (err %v)", ckErr))
			})
		}
	}
}

func TestRedoStartRefusesInconsistentState(t *testing.T) {
	setup := func(t *testing.T) (*vfs.MemFS, Control) {
		m := newFS(t)
		db := openTestDB(t, m, 4, 4096)
		h, err := storage.CreateHeap(bg, db.bp, storage.WithLogger(db.lg))
		if err != nil {
			t.Fatal(err)
		}
		var rids []storage.RID
		heapWork(t, h, rand.New(rand.NewPCG(1, 2)), &rids, 100)
		ctl, err := db.ck.Checkpoint(bg)
		if err != nil {
			t.Fatal(err)
		}
		heapWork(t, h, rand.New(rand.NewPCG(3, 4)), &rids, 20)
		mustClose(t, db.w)
		return m, ctl
	}
	t.Run("healthy", func(t *testing.T) {
		m, ctl := setup(t)
		if redo, err := RedoStart(m, dbDir, testDir); err != nil || redo != ctl.RedoLSN {
			t.Fatalf("redo %d, %v", redo, err)
		}
	})
	cases := map[string]func(t *testing.T, m *vfs.MemFS, ctl Control){
		"checkpoint record missing": func(t *testing.T, m *vfs.MemFS, ctl Control) {
			if err := WriteControl(m, dbDir, Control{CheckpointLSN: ctl.CheckpointLSN + 1, RedoLSN: ctl.RedoLSN}); err != nil {
				t.Fatal(err)
			}
		},
		"names a heap record": func(t *testing.T, m *vfs.MemFS, ctl Control) {
			first, _ := FirstLSN(m, testDir)
			r, _ := NewReader(m, testDir, first)
			rec, _ := r.Next()
			if err := WriteControl(m, dbDir, Control{CheckpointLSN: rec.LSN, RedoLSN: first}); err != nil {
				t.Fatal(err)
			}
		},
		"redo does not match the record": func(t *testing.T, m *vfs.MemFS, ctl Control) {
			if err := WriteControl(m, dbDir, Control{CheckpointLSN: ctl.CheckpointLSN, RedoLSN: ctl.RedoLSN - 1}); err != nil {
				t.Fatal(err)
			}
		},
		"checkpoint beyond the log": func(t *testing.T, m *vfs.MemFS, ctl Control) {
			if err := WriteControl(m, dbDir, Control{CheckpointLSN: 1 << 40, RedoLSN: ctl.RedoLSN}); err != nil {
				t.Fatal(err)
			}
		},
		"control file lost after trimming": func(t *testing.T, m *vfs.MemFS, ctl Control) {
			if err := m.Remove(path.Join(dbDir, ControlFileName)); err != nil {
				t.Fatal(err)
			}
		},
		"control file damaged": func(t *testing.T, m *vfs.MemFS, ctl Control) {
			writeFile(t, m, path.Join(dbDir, ControlFileName), bytes.Repeat([]byte{7}, ControlSize))
		},
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			m, ctl := setup(t)
			if first, _ := FirstLSN(m, testDir); first == SegmentHeaderSize && name == "control file lost after trimming" {
				t.Fatal("setup: the log was never trimmed")
			}
			edit(t, m, ctl)
			if _, err := RedoStart(m, dbDir, testDir); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestCheckpointWithNothingToDo(t *testing.T) {
	m := newFS(t)
	db := openTestDB(t, m, 4, 0)
	c1, err := db.ck.Checkpoint(bg)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := db.ck.Checkpoint(bg)
	if err != nil {
		t.Fatal(err)
	}
	if c2.RedoLSN <= c1.RedoLSN || c2.CheckpointLSN <= c1.CheckpointLSN {
		t.Fatalf("checkpoints did not advance: %+v then %+v", c1, c2)
	}
	m.Crash(vfs.CrashOptions{})
	db2 := openTestDB(t, m, 4, 0)
	if db2.redo != c2.RedoLSN || db2.replayed != 0 {
		t.Fatalf("redo %d replayed %d", db2.redo, db2.replayed)
	}
}

func TestCheckpointCancelled(t *testing.T) {
	m := newFS(t)
	db := openTestDB(t, m, 4, 0)
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if _, err := db.ck.Checkpoint(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if _, ok, _ := ReadControl(m, dbDir); ok {
		t.Fatal("a cancelled checkpoint wrote the control file")
	}
}

// countingLogger counts the heap records logged through it.
type countingLogger struct {
	*Logger
	n atomic.Int64
}

func (c *countingLogger) Log(ctx context.Context, build func(uint64) []byte) (uint64, error) {
	lsn, err := c.Logger.Log(ctx, build)
	if err == nil {
		c.n.Add(1)
	}
	return lsn, err
}

// A checkpoint must move the redo point the heaps see: the next change to a
// page that existed before it is logged as a full image.
func TestCheckpointMakesNextChangeLogAnImage(t *testing.T) {
	m := newFS(t)
	db := openTestDB(t, m, 8, 0)
	h, err := storage.CreateHeap(bg, db.bp, storage.WithLogger(db.lg))
	if err != nil {
		t.Fatal(err)
	}
	rid, err := heapInsert(h, []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	lastBlock := func() storage.HeapBlock {
		mustFlush(t, db.w)
		r, err := NewReader(m, testDir, db.redo)
		if err != nil {
			t.Fatal(err)
		}
		var last Record
		for {
			rec, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if rec.Type == RecordHeap {
				last = rec
			}
		}
		b, err := storage.DecodeHeapRecord(last.Payload)
		if err != nil {
			t.Fatal(err)
		}
		return b[0]
	}
	if err := heapUpdate(h, rid, []byte("BEFORE")); err != nil {
		t.Fatal(err)
	}
	if k := lastBlock().Kind; k != storage.BlockUpdate {
		t.Fatalf("change before any checkpoint logged %v", k)
	}
	if _, err := db.ck.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	if err := heapUpdate(h, rid, []byte("after")); err != nil {
		t.Fatal(err)
	}
	if k := lastBlock().Kind; k != storage.BlockImage {
		t.Fatalf("first change after a checkpoint logged %v, want an image", k)
	}
}

// Checkpoints running while several goroutines change the heap, then a crash:
// recovery rebuilds exactly the database that crashed.
func TestCheckpointsDuringConcurrentWorkThenCrash(t *testing.T) {
	for i := range 6 {
		t.Run(fmt.Sprintf("run=%d", i), func(t *testing.T) {
			m := newFS(t)
			db := openTestDB(t, m, 10, 8192)
			h, err := storage.CreateHeap(bg, db.bp, storage.WithLogger(db.lg))
			if err != nil {
				t.Fatal(err)
			}
			stop := make(chan struct{})
			ckDone := make(chan error, 1)
			go func() {
				var err error
				n := 0
				for err == nil {
					select {
					case <-stop:
						if n == 0 {
							err = errors.New("no checkpoint completed")
						}
						ckDone <- err
						return
					default:
					}
					_, err = db.ck.Checkpoint(bg)
					n++
				}
				ckDone <- err
			}()
			var wg sync.WaitGroup
			for g := range 5 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					rng := rand.New(rand.NewPCG(uint64(i), uint64(g)))
					var rids []storage.RID
					for range 120 {
						if len(rids) == 0 || rng.IntN(3) > 0 {
							rid, err := heapInsert(h, payload(rng, 1+rng.IntN(1500)))
							if err != nil {
								t.Errorf("insert: %v", err)
								return
							}
							rids = append(rids, rid)
						} else {
							j := rng.IntN(len(rids))
							err := heapUpdate(h, rids[j], payload(rng, 1+rng.IntN(3000)))
							if err != nil {
								t.Errorf("update: %v", err)
								return
							}
						}
					}
				}()
			}
			wg.Wait()
			close(stop)
			if err := <-ckDone; err != nil {
				t.Fatalf("checkpointer: %v", err)
			}
			if t.Failed() {
				return
			}
			want := logicalPages(t, db)
			mustFlush(t, db.w)
			m.Crash(vfs.CrashOptions{TearLast: true})
			db2 := openTestDB(t, m, 6, 8192)
			sameLogical(t, logicalPages(t, db2), want, "recovered database")
		})
	}
}
