package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

// walRuleStore checks, at every page write, that the page's LSN is covered by
// the durable log at that moment.
type walRuleStore struct {
	storage.PageStore
	w          *Writer
	writes     atomic.Int64
	violations atomic.Int64
}

func (s *walRuleStore) WritePage(ctx context.Context, id uint64, buf []byte) error {
	s.writes.Add(1)
	if lsn := binary.LittleEndian.Uint64(buf[pageLSNOffset:]); lsn > uint64(s.w.FlushedLSN()) {
		s.violations.Add(1)
	}
	return s.PageStore.WritePage(ctx, id, buf)
}

type loggedSetup struct {
	m     *vfs.MemFS
	w     *Writer
	dm    *storage.DiskManager
	store *walRuleStore
	bp    *storage.BufferPool
	lg    *Logger
	force atomic.Int64
}

func newLoggedSetup(t testing.TB, frames int, segSize int64) *loggedSetup {
	t.Helper()
	m := newFS(t)
	w := mustOpen(t, m, Options{SegmentSize: segSize})
	dm, err := storage.Create(m, "/db/data")
	if err != nil {
		t.Fatal(err)
	}
	s := &loggedSetup{m: m, w: w, dm: dm}
	s.store = &walRuleStore{PageStore: dm, w: w}
	flushed, force := RuleHooks(w)
	s.bp, err = storage.NewBufferPool(s.store, storage.Options{
		Frames:     frames,
		FlushedLSN: flushed,
		FlushWAL: func(ctx context.Context, lsn uint64) error {
			s.force.Add(1)
			return force(ctx, lsn)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := FirstLSN(m, testDir)
	if err != nil {
		t.Fatal(err)
	}
	s.lg = NewLogger(w, first)
	return s
}

// checkpointLite is the part of a checkpoint this step has: move the redo
// point, then write and sync everything dirty before it.
func (s *loggedSetup) checkpointLite(t testing.TB) LSN {
	t.Helper()
	redo := s.lg.BeginCheckpoint()
	if err := s.bp.FlushAll(bg); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	if err := s.dm.Sync(bg); err != nil {
		t.Fatal(err)
	}
	return redo
}

// pagesOf flushes the pool and returns every allocated page as stored.
func pagesOf(t testing.TB, bp *storage.BufferPool, dm *storage.DiskManager) map[uint64][]byte {
	t.Helper()
	if err := bp.FlushAll(bg); err != nil {
		t.Fatal(err)
	}
	out := map[uint64][]byte{}
	for id := uint64(storage.FirstDataPage); id < dm.PageCount(); id++ {
		buf := make([]byte, storage.PageSize)
		if err := dm.ReadPage(bg, id, buf); err != nil {
			t.Fatalf("page %d: %v", id, err)
		}
		out[id] = buf
	}
	return out
}

// replayLog replays the real log, read back with a Reader from from, onto a
// fresh data file with the same pages allocated.
func replayLog(t testing.TB, fsys vfs.FS, pageCount uint64, from LSN) map[uint64][]byte {
	t.Helper()
	m2 := newFS(t)
	dm2, err := storage.Create(m2, "/db/data")
	if err != nil {
		t.Fatal(err)
	}
	for dm2.PageCount() < pageCount {
		if _, err := dm2.Allocate(bg); err != nil {
			t.Fatal(err)
		}
	}
	bp2, err := storage.NewBufferPool(dm2, storage.Options{Frames: 6})
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(fsys, testDir, from)
	if err != nil {
		t.Fatal(err)
	}
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if rec.Type != RecordHeap {
			t.Fatalf("record type %d", rec.Type)
		}
		if err := storage.RedoHeapRecord(bg, bp2, uint64(rec.LSN), rec.Payload); err != nil {
			t.Fatalf("redo %d: %v", rec.LSN, err)
		}
	}
	return pagesOf(t, bp2, dm2)
}

func TestLoggedHeapWithRealLog(t *testing.T) {
	for _, frames := range []int{3, 4, 16} {
		t.Run(fmt.Sprintf("frames=%d", frames), func(t *testing.T) {
			s := newLoggedSetup(t, frames, 4096)
			rng := rand.New(rand.NewPCG(testSeed(t), uint64(frames)))
			h, err := storage.CreateHeap(bg, s.bp, storage.WithLogger(s.lg))
			if err != nil {
				t.Fatal(err)
			}
			var rids []storage.RID
			for i := range 800 {
				switch op := rng.IntN(10); {
				case op < 5 || len(rids) == 0:
					rid, err := heapInsert(h, payload(rng, 1+rng.IntN(3000)))
					if err != nil {
						t.Fatal(err)
					}
					rids = append(rids, rid)
				case op < 8:
					j := rng.IntN(len(rids))
					err := heapUpdate(h, rids[j], payload(rng, 1+rng.IntN(5000)))
					if err != nil {
						t.Fatal(err)
					}
				default:
					j := rng.IntN(len(rids))
					if err := heapDelete(h, rids[j]); err != nil {
						t.Fatal(err)
					}
					rids = append(rids[:j], rids[j+1:]...)
				}
				if i%97 == 0 {
					s.checkpointLite(t)
				}
				if i%31 == 0 {
					mustFlush(t, s.w) // an acknowledgement
				}
			}
			if v := s.store.violations.Load(); v != 0 {
				t.Fatalf("%d pages reached disk before their log record was durable", v)
			}
			if s.store.writes.Load() == 0 {
				t.Fatal("no page was ever written; the WAL rule was not exercised")
			}
			if frames <= 4 && s.force.Load() == 0 {
				t.Fatal("the pool never had to force the log")
			}
			want := pagesOf(t, s.bp, s.dm)
			mustFlush(t, s.w)
			got := replayLog(t, s.m, s.dm.PageCount(), SegmentHeaderSize)
			if len(got) != len(want) {
				t.Fatalf("%d pages replayed, want %d", len(got), len(want))
			}
			for id, w := range want {
				if !bytes.Equal(got[id], w) {
					t.Fatalf("page %d differs after replaying the real log", id)
				}
			}
		})
	}
}

// A checkpoint moving the redo point while writers log concurrently must not
// let any record slip through without the image it needs. Checked directly on
// the log: for every redo point R, the first record after R that touches a
// page last touched before R must carry an image of that page.
func TestImageRuleHoldsUnderConcurrentCheckpoints(t *testing.T) {
	s := newLoggedSetup(t, 12, 1<<20)
	h, err := storage.CreateHeap(bg, s.bp, storage.WithLogger(s.lg))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var redos []LSN
	stop := make(chan struct{})
	var ckWG sync.WaitGroup
	ckWG.Add(1)
	go func() {
		defer ckWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r := s.lg.BeginCheckpoint()
			mu.Lock()
			redos = append(redos, r)
			mu.Unlock()
			if err := s.bp.FlushAll(bg); err != nil {
				t.Errorf("FlushAll: %v", err)
				return
			}
		}
	}()
	var wg sync.WaitGroup
	for g := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 5))
			var mine []storage.RID
			for range 200 {
				if len(mine) == 0 || rng.IntN(3) > 0 {
					rid, err := heapInsert(h, payload(rng, 1+rng.IntN(600)))
					if err != nil {
						t.Errorf("insert: %v", err)
						return
					}
					mine = append(mine, rid)
				} else {
					j := rng.IntN(len(mine))
					err := heapUpdate(h, mine[j], payload(rng, 1+rng.IntN(1500)))
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
	ckWG.Wait()
	if t.Failed() {
		return
	}
	mustFlush(t, s.w)
	if len(redos) < 5 {
		t.Fatalf("only %d checkpoints ran", len(redos))
	}

	// Walk the log: per page, the LSN of the last record touching it.
	r, err := NewReader(s.m, testDir, SegmentHeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	last := map[uint64]LSN{}
	checked := 0
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		blocks, err := storage.DecodeHeapRecord(rec.Payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range blocks {
			prev, seen := last[b.Page]
			if seen && b.Kind != storage.BlockImage {
				for _, redo := range redos {
					if prev < redo && rec.LSN > redo {
						t.Fatalf("record %d changes page %d (last changed at %d) after redo point %d without an image",
							rec.LSN, b.Page, prev, redo)
					}
				}
				checked++
			}
			last[b.Page] = rec.LSN
		}
	}
	if checked == 0 {
		t.Fatal("no operation records were checked")
	}
	if v := s.store.violations.Load(); v != 0 {
		t.Fatalf("%d WAL-rule violations", v)
	}
}

func TestLoggerFailureUndoesChange(t *testing.T) {
	s := newLoggedSetup(t, 6, 0)
	h, err := storage.CreateHeap(bg, s.bp, storage.WithLogger(s.lg))
	if err != nil {
		t.Fatal(err)
	}
	rid, err := heapInsert(h, []byte("kept"))
	if err != nil {
		t.Fatal(err)
	}
	mustFlush(t, s.w)
	// Break the log: the next flush fails and poisons the writer.
	if _, err := s.w.Append(bg, RecordHeap, []byte{0}); err != nil {
		t.Fatal(err)
	}
	s.m.InjectError(vfs.Fault{Op: vfs.OpSync})
	if err := s.w.Flush(bg); err == nil {
		t.Fatal("flush unexpectedly succeeded")
	}
	if _, err := heapInsert(h, []byte("lost")); !errors.Is(err, ErrFailed) {
		t.Fatalf("insert on a failed log: %v", err)
	}
	if err := heapDelete(h, rid); !errors.Is(err, ErrFailed) {
		t.Fatalf("delete on a failed log: %v", err)
	}
	got, err := heapGet(h, rid)
	if err != nil || string(got) != "kept" {
		t.Fatalf("row after failed operations: %q, %v", got, err)
	}
}

func TestRuleHooks(t *testing.T) {
	m := newFS(t)
	w := mustOpen(t, m, Options{})
	flushed, force := RuleHooks(w)
	lsn := mustAppend(t, w, RecordHeap, []byte("x"))
	if flushed() >= uint64(lsn) {
		t.Fatal("unflushed record reported flushed")
	}
	if err := force(context.Background(), uint64(lsn)); err != nil {
		t.Fatal(err)
	}
	if flushed() < uint64(lsn) {
		t.Fatal("force did not make the record durable")
	}
}

// The redo point a record is built with and the record's append must be atomic
// with respect to a checkpoint starting: if a checkpoint set redo point R and
// a record then got an LSN at or after R, that record must have been built
// knowing R. Otherwise it could skip the image recovery needs.
func TestLoggerRedoPointIsAtomicWithAppend(t *testing.T) {
	m := newFS(t)
	w := mustOpen(t, m, Options{})
	lg := NewLogger(w, SegmentHeaderSize)
	type seen struct{ redo, lsn uint64 }
	var mu sync.Mutex
	var logged []seen
	var redos []LSN
	stop := make(chan struct{})
	var ck sync.WaitGroup
	ck.Add(1)
	go func() {
		defer ck.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r := lg.BeginCheckpoint()
			mu.Lock()
			redos = append(redos, r)
			mu.Unlock()
			runtime.Gosched()
		}
	}()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1500 {
				var redo uint64
				lsn, err := lg.Log(bg, func(r uint64) []byte {
					redo = r
					runtime.Gosched() // widen the gap between deciding and appending
					return []byte{1}
				})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				logged = append(logged, seen{redo, lsn})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(stop)
	ck.Wait()
	if t.Failed() {
		return
	}
	if len(redos) < 10 {
		t.Fatalf("only %d checkpoints began", len(redos))
	}
	for _, s := range logged {
		for _, r := range redos {
			if uint64(r) <= s.lsn && s.redo < uint64(r) {
				t.Fatalf("record %d was appended after redo point %d was set, but was built with redo point %d", s.lsn, r, s.redo)
			}
		}
	}
}
