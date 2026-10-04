package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

// fakeLogger is an in-memory log with the same LSN and durability semantics
// as the real WAL: LSNs grow by record size, FlushedLSN is "durable end - 1",
// and the redo point moves when a checkpoint begins.
type fakeLogger struct {
	mu       sync.Mutex
	next     uint64 // LSN of the next record
	durable  uint64 // every record starting below this is durable
	redo     uint64
	recs     []fakeRec
	fail     error // returned by the next Log calls while set
	forced   atomic.Int64
	imageCnt int
	opCnt    int
}

type fakeRec struct {
	lsn     uint64
	payload []byte
}

const fakeFirstLSN = 32 // as in the real log: a segment header comes first

func newFakeLogger() *fakeLogger {
	return &fakeLogger{next: fakeFirstLSN, durable: fakeFirstLSN, redo: fakeFirstLSN}
}

func (l *fakeLogger) Log(_ context.Context, build func(uint64) []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fail != nil {
		return 0, l.fail
	}
	p := build(l.redo)
	blocks, err := DecodeHeapRecord(p)
	if err != nil {
		panic(fmt.Sprintf("heap produced an undecodable record: %v", err)) // test invariant
	}
	for _, b := range blocks {
		if b.Kind == BlockImage {
			l.imageCnt++
		} else {
			l.opCnt++
		}
	}
	lsn := l.next
	l.recs = append(l.recs, fakeRec{lsn: lsn, payload: bytes.Clone(p)})
	l.next += 20 + uint64(len(p))
	return lsn, nil
}

func (l *fakeLogger) flushedLSN() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.durable - 1
}

func (l *fakeLogger) flushWAL(_ context.Context, _ uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.forced.Add(1)
	l.durable = l.next
	return nil
}

// beginCheckpoint moves the redo point to the end of the log, as a real
// checkpoint does, and returns it.
func (l *fakeLogger) beginCheckpoint() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.redo = l.next
	return l.redo
}

func (l *fakeLogger) records() []fakeRec {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.recs)
}

func (l *fakeLogger) setFail(err error) {
	l.mu.Lock()
	l.fail = err
	l.mu.Unlock()
}

// ruleStore checks the WAL rule at every page write: the page's LSN must be
// covered by the durable log at that moment.
type ruleStore struct {
	PageStore
	flushed    func() uint64
	writes     atomic.Int64
	violations atomic.Int64
	firstBad   atomic.Value
}

func (s *ruleStore) WritePage(ctx context.Context, id uint64, buf []byte) error {
	s.writes.Add(1)
	if lsn := pageLSN(buf); lsn > s.flushed() {
		if s.violations.Add(1) == 1 {
			s.firstBad.Store(fmt.Sprintf("page %d with lsn %d written while the log was durable to %d", id, lsn, s.flushed()))
		}
	}
	return s.PageStore.WritePage(ctx, id, buf)
}

type loggedEnv struct {
	m     *vfs.MemFS
	dm    *DiskManager
	store *ruleStore
	bp    *BufferPool
	lg    *fakeLogger
}

func newLoggedEnv(t testing.TB, frames int) *loggedEnv {
	t.Helper()
	m := newMem(t)
	dm := mustCreate(t, m, dbName)
	lg := newFakeLogger()
	st := &ruleStore{PageStore: dm, flushed: lg.flushedLSN}
	bp, err := NewBufferPool(st, Options{Frames: frames, FlushedLSN: lg.flushedLSN, FlushWAL: lg.flushWAL})
	if err != nil {
		t.Fatal(err)
	}
	return &loggedEnv{m: m, dm: dm, store: st, bp: bp, lg: lg}
}

func (e *loggedEnv) checkRule(t testing.TB) {
	t.Helper()
	if n := e.store.violations.Load(); n != 0 {
		t.Fatalf("%d WAL-rule violations; first: %v", n, e.store.firstBad.Load())
	}
}

// pageImages flushes the pool and returns every allocated page as stored.
func pageImages(t testing.TB, bp *BufferPool, dm *DiskManager) map[uint64][]byte {
	t.Helper()
	if err := bp.FlushAll(bg); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	out := map[uint64][]byte{}
	for id := uint64(FirstDataPage); id < dm.PageCount(); id++ {
		buf := make([]byte, PageSize)
		if err := dm.ReadPage(bg, id, buf); err != nil {
			t.Fatalf("reading page %d: %v", id, err)
		}
		out[id] = buf
	}
	return out
}

// replayOnto replays recs (those with lsn >= from) onto the data file of fsys
// with a fresh pool and returns the resulting pages.
func replayOnto(t testing.TB, m *vfs.MemFS, recs []fakeRec, from uint64, frames int) map[uint64][]byte {
	t.Helper()
	dm := mustOpenDM(t, m, dbName)
	bp, err := NewBufferPool(dm, Options{Frames: frames})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if r.lsn < from {
			continue
		}
		if err := RedoHeapRecord(bg, bp, r.lsn, r.payload); err != nil {
			t.Fatalf("redo of record at %d: %v", r.lsn, err)
		}
	}
	return pageImages(t, bp, dm)
}

func samePages(t testing.TB, got, want map[uint64][]byte, ctx string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d pages, want %d", ctx, len(got), len(want))
	}
	for id, w := range want {
		if g := got[id]; !bytes.Equal(g, w) {
			hg, _ := DecodeHeader(g)
			hw, _ := DecodeHeader(w)
			t.Fatalf("%s: page %d differs (lsn %d vs %d)", ctx, id, hg.LSN, hw.LSN)
		}
	}
}

// --- codec ----------------------------------------------------------------------

func TestHeapRecordRoundTrip(t *testing.T) {
	img := make([]byte, PageSize)
	for i := range img {
		img[i] = byte(i)
	}
	cases := [][]HeapBlock{
		{{Page: 2, Kind: BlockImage, Data: img}},
		{{Page: 9, Kind: BlockInsert, Slot: 3, Data: []byte("row")}},
		{{Page: 9, Kind: BlockUpdate, Slot: 65535, Data: bytes.Repeat([]byte{7}, MaxTupleSize)}},
		{{Page: 1 << 40, Kind: BlockDelete, Slot: 0}},
		{{Page: 4, Kind: BlockSetNext, Next: 77}},
		{{Page: 5, Kind: BlockDelete, Slot: 2}, {Page: 6, Kind: BlockInsert, Slot: 0, Data: []byte{1}}},
		{{Page: 6, Kind: BlockImage, Data: img}, {Page: 5, Kind: BlockSetNext, Next: 6}},
	}
	for i, blocks := range cases {
		enc := encodeHeapRecord(blocks)
		got, err := DecodeHeapRecord(enc)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if len(got) != len(blocks) {
			t.Fatalf("case %d: %d blocks", i, len(got))
		}
		for j := range blocks {
			w, g := blocks[j], got[j]
			if w.Page != g.Page || w.Kind != g.Kind || w.Slot != g.Slot || w.Next != g.Next || !bytes.Equal(w.Data, g.Data) {
				t.Fatalf("case %d block %d: %+v != %+v", i, j, g, w)
			}
		}
		if !bytes.Equal(encodeHeapRecord(got), enc) {
			t.Fatalf("case %d: not canonical", i)
		}
	}
}

func TestHeapRecordGoldenBytes(t *testing.T) {
	enc := encodeHeapRecord([]HeapBlock{
		{Page: 0x0102, Kind: BlockInsert, Slot: 0x0304, Data: []byte("ab")},
		{Page: 7, Kind: BlockSetNext, Next: 0x0A0B},
	})
	want := []byte{
		2,                            // BlockCount
		0x02, 0x01, 0, 0, 0, 0, 0, 0, // PageID
		2,          // Kind insert
		0x04, 0x03, // Slot
		2, 0, 0, 0, // length
		'a', 'b',
		7, 0, 0, 0, 0, 0, 0, 0, // PageID
		5,                            // Kind set-next
		0x0B, 0x0A, 0, 0, 0, 0, 0, 0, // Next
	}
	if !bytes.Equal(enc, want) {
		t.Fatalf("encoding = % x\nwant       % x", enc, want)
	}
}

func TestDecodeHeapRecordRejects(t *testing.T) {
	valid := encodeHeapRecord([]HeapBlock{{Page: 3, Kind: BlockInsert, Slot: 1, Data: []byte("xyz")}})
	for n := range len(valid) {
		if _, err := DecodeHeapRecord(valid[:n]); !errors.Is(err, ErrBadHeapRecord) {
			t.Fatalf("truncated to %d bytes: err = %v", n, err)
		}
	}
	mut := func(f func(b []byte) []byte) []byte { return f(bytes.Clone(valid)) }
	cases := map[string][]byte{
		"zero blocks":      mut(func(b []byte) []byte { b[0] = 0; return b }),
		"three blocks":     mut(func(b []byte) []byte { b[0] = 3; return b }),
		"reserved page 1":  mut(func(b []byte) []byte { b[1] = 1; return b }),
		"reserved page 0":  mut(func(b []byte) []byte { b[1] = 0; return b }),
		"unknown kind 0":   mut(func(b []byte) []byte { b[9] = 0; return b }),
		"unknown kind 6":   mut(func(b []byte) []byte { b[9] = 6; return b }),
		"zero length":      mut(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[12:], 0); return b }),
		"length too large": mut(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[12:], MaxTupleSize+1); return b }),
		"length past end":  mut(func(b []byte) []byte { binary.LittleEndian.PutUint32(b[12:], 4); return b }),
		"trailing byte":    append(bytes.Clone(valid), 0),
		"same page twice": encodeHeapRecord([]HeapBlock{
			{Page: 3, Kind: BlockDelete, Slot: 1}, {Page: 3, Kind: BlockDelete, Slot: 2}}),
		"empty": {},
	}
	for name, b := range cases {
		if _, err := DecodeHeapRecord(b); !errors.Is(err, ErrBadHeapRecord) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func FuzzDecodeHeapRecord(f *testing.F) {
	img := make([]byte, PageSize)
	f.Add(encodeHeapRecord([]HeapBlock{{Page: 3, Kind: BlockInsert, Slot: 1, Data: []byte("xyz")}}))
	f.Add(encodeHeapRecord([]HeapBlock{{Page: 5, Kind: BlockDelete, Slot: 2}, {Page: 6, Kind: BlockSetNext, Next: 9}}))
	f.Add(encodeHeapRecord([]HeapBlock{{Page: 6, Kind: BlockImage, Data: img}}))
	f.Add([]byte{})
	f.Add([]byte{1, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0})
	f.Fuzz(func(t *testing.T, p []byte) {
		blocks, err := DecodeHeapRecord(p)
		if err != nil {
			if !errors.Is(err, ErrBadHeapRecord) {
				t.Fatalf("unclassified error %v", err)
			}
			return
		}
		if !bytes.Equal(encodeHeapRecord(blocks), p) {
			t.Fatal("accepted record is not canonical")
		}
	})
}

// --- logging behaviour --------------------------------------------------------

func TestLoggedHeapImageRule(t *testing.T) {
	e := newLoggedEnv(t, 8)
	h, err := CreateHeap(bg, e.bp, WithLogger(e.lg))
	if err != nil {
		t.Fatal(err)
	}
	lastBlocks := func() []HeapBlock {
		recs := e.lg.records()
		b, err := DecodeHeapRecord(recs[len(recs)-1].payload)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	// Creating a heap logs the new page as an image.
	if b := lastBlocks(); len(b) != 1 || b[0].Kind != BlockImage || b[0].Page != h.FirstPage() {
		t.Fatalf("create logged %+v", b)
	}
	rid, err := h.put(bg, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	// The page's LSN (from creation) is not below the redo point: an operation.
	if b := lastBlocks(); b[0].Kind != BlockInsert || b[0].Slot != rid.Slot {
		t.Fatalf("insert logged %+v", b[0])
	} else if tu, err := decodeTuple(b[0].Data); err != nil || string(tu.data) != "first" || tu.hdr.XID != 1 {
		t.Fatalf("insert logged tuple %+v, %v", tu, err)
	}
	e.lg.beginCheckpoint()
	if _, err := h.put(bg, []byte("after checkpoint")); err != nil {
		t.Fatal(err)
	}
	// First change after the redo point moved: a full image.
	if b := lastBlocks(); b[0].Kind != BlockImage {
		t.Fatalf("first change after checkpoint logged %v, want image", b[0].Kind)
	}
	if err := h.del(bg, rid); err != nil {
		t.Fatal(err)
	}
	// A delete leaves a tombstone: an update of the slot.
	if b := lastBlocks(); b[0].Kind != BlockUpdate {
		t.Fatalf("second change after checkpoint logged %v, want update", b[0].Kind)
	} else if tu, err := decodeTuple(b[0].Data); err != nil || tu.kind != tupleTombstone {
		t.Fatalf("delete logged tuple %+v, %v", tu, err)
	}
	// Every page carries the LSN of the last record that changed it.
	recs := e.lg.records()
	ref, err := e.bp.FetchPage(bg, h.FirstPage())
	if err != nil {
		t.Fatal(err)
	}
	if got := pageLSN(ref.Data()); got != recs[len(recs)-1].lsn {
		t.Fatalf("page LSN %d, last record %d", got, recs[len(recs)-1].lsn)
	}
	_ = ref.Unpin(false)
}

func TestLoggedHeapGrowAndMoveAreSingleRecords(t *testing.T) {
	e := newLoggedEnv(t, 8)
	h, err := CreateHeap(bg, e.bp, WithLogger(e.lg))
	if err != nil {
		t.Fatal(err)
	}
	small, err := h.put(bg, []byte("small"))
	if err != nil {
		t.Fatal(err)
	}
	h.fillPage(t, 0, 1)
	blocksOf := func(r fakeRec) map[uint64]BlockKind {
		t.Helper()
		blocks, err := DecodeHeapRecord(r.payload)
		if err != nil {
			t.Fatal(err)
		}
		out := map[uint64]BlockKind{}
		for _, b := range blocks {
			out[b.Page] = b.Kind
		}
		return out
	}
	before := len(e.lg.records())
	if err := h.set(bg, small, bytes.Repeat([]byte{2}, 3000)); err != nil { // must grow, then move
		t.Fatal(err)
	}
	stub, err := h.readTuple(bg, small)
	if err != nil || stub.kind != tupleStub {
		t.Fatalf("row did not move: %+v %v", stub, err)
	}
	recs := e.lg.records()[before:]
	if len(recs) != 2 {
		t.Fatalf("grow+move wrote %d records, want 2 (one each)", len(recs))
	}
	if grow := blocksOf(recs[0]); len(grow) != 2 {
		t.Fatalf("grow has %d blocks, want 2", len(grow))
	}
	// Both pages were last changed after the redo point (no checkpoint has
	// run), so both are logged as operations, not images.
	if move := blocksOf(recs[1]); len(move) != 2 || move[small.Page] != BlockUpdate || move[stub.link.Page] != BlockInsert {
		t.Fatalf("move blocks %v", move)
	}

	// Moving a moved row again changes three pages in one record: the stub,
	// the old moved-in tuple and the new one.
	h.fillPage(t, 1, 3)
	before = len(e.lg.records())
	if err := h.set(bg, small, bytes.Repeat([]byte{4}, 4000)); err != nil {
		t.Fatal(err)
	}
	again, _ := h.readTuple(bg, small)
	recs = e.lg.records()[before:]
	move := blocksOf(recs[len(recs)-1])
	if len(move) != 3 || move[small.Page] != BlockUpdate || move[stub.link.Page] != BlockDelete || move[again.link.Page] != BlockInsert {
		t.Fatalf("second move blocks %v (%d records)", move, len(recs))
	}
	// Moving back home: the home slot and the old moved-in tuple, one record.
	before = len(e.lg.records())
	if err := h.set(bg, small, []byte("s")); err != nil {
		t.Fatal(err)
	}
	recs = e.lg.records()[before:]
	if len(recs) != 1 {
		t.Fatalf("moving home wrote %d records", len(recs))
	}
	if home := blocksOf(recs[0]); len(home) != 2 || home[small.Page] != BlockUpdate || home[again.link.Page] != BlockDelete {
		t.Fatalf("moving home blocks %v", home)
	}
	e.checkRule(t)
}

// A logged heap's three-page moves work with only three frames, whatever
// else the pool holds.
func TestLoggedHeapWorksWithThreeFrames(t *testing.T) {
	e := newLoggedEnv(t, 3)
	h, err := CreateHeap(bg, e.bp, WithLogger(e.lg))
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(7, 7))
	var rids []RID
	for range 20 { // many pages: constant eviction, grows and moves
		rid, err := h.put(bg, bytes.Repeat([]byte{3}, 2000))
		if err != nil {
			t.Fatal(err)
		}
		rids = append(rids, rid)
	}
	for range 60 {
		rid := rids[rng.IntN(len(rids))]
		if err := h.set(bg, rid, bytes.Repeat([]byte{4}, 1+rng.IntN(7000))); err != nil {
			t.Fatal(err)
		}
	}
	if h.checkTuples(t) == 0 {
		t.Fatal("no row moved")
	}
	h.checkFSM(t)
	e.checkRule(t)
	e.bp.checkInvariants(t, 0)
	if e.lg.forced.Load() == 0 {
		t.Fatal("the pool never had to force the log; the WAL rule was not exercised")
	}
}

// --- replay equals reality ----------------------------------------------------

// runLoggedWorkload runs random logged heap operations, with checkpoints that
// move the redo point and flush the pool, and returns the model of live rows
// and the redo point of the last checkpoint.
func runLoggedWorkload(t *testing.T, e *loggedEnv, rng *rand.Rand, steps int, heaps int) (map[RID][]byte, uint64, []*Heap) {
	t.Helper()
	var hs []*Heap
	for range heaps {
		h, err := CreateHeap(bg, e.bp, WithLogger(e.lg))
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
	}
	model := map[RID][]byte{}
	owner := map[RID]*Heap{}
	lastRedo := uint64(fakeFirstLSN)
	pick := func() (RID, bool) {
		if len(model) == 0 {
			return RID{}, false
		}
		rids := make([]RID, 0, len(model))
		for r := range model {
			rids = append(rids, r)
		}
		slices.SortFunc(rids, func(a, b RID) int {
			if a.Page != b.Page {
				return int(a.Page) - int(b.Page)
			}
			return int(a.Slot) - int(b.Slot)
		})
		return rids[rng.IntN(len(rids))], true
	}
	for step := range steps {
		switch op := rng.IntN(100); {
		case op < 40:
			h := hs[rng.IntN(len(hs))]
			d := rowBytes(rng, heapRowSize(rng))
			rid, err := h.put(bg, d)
			if err != nil {
				t.Fatalf("step %d insert: %v", step, err)
			}
			model[rid], owner[rid] = d, h
		case op < 65:
			if rid, ok := pick(); ok {
				d := rowBytes(rng, heapRowSize(rng))
				if err := owner[rid].set(bg, rid, d); err != nil {
					t.Fatalf("step %d update: %v", step, err)
				}
				model[rid] = d
			}
		case op < 85:
			if rid, ok := pick(); ok {
				if err := owner[rid].del(bg, rid); err != nil {
					t.Fatalf("step %d delete: %v", step, err)
				}
				delete(model, rid)
				delete(owner, rid)
			}
		case op < 95:
			if rid, ok := pick(); ok {
				got, err := owner[rid].get(bg, rid)
				if err != nil || !bytes.Equal(got, model[rid]) {
					t.Fatalf("step %d get: %v", step, err)
				}
			}
		default: // checkpoint: new redo point, everything dirty before it written
			lastRedo = e.lg.beginCheckpoint()
			if err := e.bp.FlushAll(bg); err != nil {
				t.Fatalf("step %d checkpoint flush: %v", step, err)
			}
			if err := e.dm.Sync(bg); err != nil {
				t.Fatal(err)
			}
		}
	}
	return model, lastRedo, hs
}

func TestReplayReproducesPagesExactly(t *testing.T) {
	base := testSeed(t)
	runs := 12
	if testing.Short() {
		runs = 3
	}
	for i := range runs {
		seed := base + uint64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 77))
			e := newLoggedEnv(t, 3+rng.IntN(6))
			model, _, hs := runLoggedWorkload(t, e, rng, 600, 1+rng.IntN(3))
			e.checkRule(t)
			if e.lg.imageCnt == 0 || e.lg.opCnt == 0 || e.store.writes.Load() == 0 {
				t.Fatalf("workload too tame: %d images, %d operations, %d page writes",
					e.lg.imageCnt, e.lg.opCnt, e.store.writes.Load())
			}
			t.Logf("%d records: %d image blocks, %d operation blocks; %d page writes, %d forced log flushes",
				len(e.lg.records()), e.lg.imageCnt, e.lg.opCnt, e.store.writes.Load(), e.lg.forced.Load())
			want := pageImages(t, e.bp, e.dm)

			// A fresh data file with the same pages allocated: replaying the
			// whole log must rebuild every page byte for byte.
			m2 := newMem(t)
			dm2 := mustCreate(t, m2, dbName)
			for dm2.PageCount() < e.dm.PageCount() {
				if _, err := dm2.Allocate(bg); err != nil {
					t.Fatal(err)
				}
			}
			_ = dm2.Close()
			recs := e.lg.records()
			got := replayOnto(t, m2, recs, 0, 3+rng.IntN(5))
			samePages(t, got, want, "full replay")

			// Replaying again on top (pages already newer) changes nothing.
			again := replayOnto(t, m2, recs, 0, 4)
			samePages(t, again, want, "second replay")

			// And the heaps read back the model.
			total := 0
			for _, h := range hs {
				for rid, d := range scanAll(t, h) {
					if !bytes.Equal(model[rid], d) {
						t.Fatalf("row %s differs from the model", rid)
					}
					total++
				}
			}
			if total != len(model) {
				t.Fatalf("%d rows, model has %d", total, len(model))
			}
		})
	}
}

// What recovery faces: the data file as a crash left it, where any page
// changed after the last checkpoint may be torn, newer, or older than the
// log. Replaying from the redo point must still rebuild everything exactly.
func TestReplayFromRedoPointRepairsTornPages(t *testing.T) {
	base := testSeed(t)
	runs := 12
	if testing.Short() {
		runs = 3
	}
	for i := range runs {
		seed := base + 1000 + uint64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 78))
			e := newLoggedEnv(t, 3+rng.IntN(6))
			_, redo, hs := runLoggedWorkload(t, e, rng, 500, 1+rng.IntN(2))
			// Make sure some pages changed after the last checkpoint, so
			// there is something for a crash to tear.
			for range 20 {
				if _, err := hs[0].put(bg, rowBytes(rng, heapRowSize(rng))); err != nil {
					t.Fatal(err)
				}
			}
			e.checkRule(t)

			// The crash image: whatever reached the data file (some pages
			// written by eviction after the checkpoint, some not)...
			if err := e.dm.Sync(bg); err != nil {
				t.Fatal(err)
			}
			crashed := vfs.NewMemFS(seed)
			_ = crashed.MkdirAll("/db")
			_ = crashed.SyncDir("/")
			raw := readFileBytes(t, e.m, dbName)
			// Pages allocated but never written are beyond the end of the
			// file; on disk they read as zeros.
			if need := int(e.dm.PageCount()) * PageSize; len(raw) < need {
				raw = append(raw, make([]byte, need-len(raw))...)
			}
			// ...with pages changed after the redo point torn.
			want := pageImages(t, e.bp, e.dm)
			var eligible []uint64
			for id, img := range want {
				if h, _ := DecodeHeader(img); h.LSN >= redo {
					eligible = append(eligible, id)
				}
			}
			slices.Sort(eligible)
			if len(eligible) == 0 {
				t.Fatal("no page changed after the last checkpoint")
			}
			torn := 0
			for i, id := range eligible {
				if i == 0 || rng.IntN(2) == 0 { // always at least one
					off := int(id) * PageSize
					for j := range PageSize / 2 {
						raw[off+PageSize/4+j] ^= 0xA5
					}
					torn++
				}
			}
			t.Logf("tore %d of %d pages; replaying from %d", torn, len(want), redo)
			writeFileBytes(t, crashed, dbName, raw)
			got := replayOnto(t, crashed, e.lg.records(), redo, 3+rng.IntN(5))
			samePages(t, got, want, fmt.Sprintf("replay from redo %d with %d torn pages", redo, torn))
		})
	}
}

func TestLoggedHeapConcurrentReplay(t *testing.T) {
	e := newLoggedEnv(t, 8)
	h, err := CreateHeap(bg, e.bp, WithLogger(e.lg))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for w := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 3))
			var mine []RID
			for i := range 150 {
				switch {
				case len(mine) == 0 || rng.IntN(3) == 0:
					rid, err := retryBusy(func() (RID, error) { return h.put(bg, concRow(byte(w), uint64(i), 9+rng.IntN(2500))) })
					if err != nil {
						t.Errorf("insert: %v", err)
						return
					}
					mine = append(mine, rid)
				case rng.IntN(2) == 0:
					j := rng.IntN(len(mine))
					if _, err := retryBusy(func() (struct{}, error) {
						return struct{}{}, h.set(bg, mine[j], concRow(byte(w), uint64(i), 9+rng.IntN(4000)))
					}); err != nil {
						t.Errorf("update: %v", err)
						return
					}
				default:
					j := rng.IntN(len(mine))
					if _, err := retryBusy(func() (struct{}, error) { return struct{}{}, h.del(bg, mine[j]) }); err != nil {
						t.Errorf("delete: %v", err)
						return
					}
					mine = append(mine[:j], mine[j+1:]...)
				}
				if rng.IntN(50) == 0 {
					e.lg.beginCheckpoint()
				}
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	e.checkRule(t)
	h.checkFSM(t)
	want := pageImages(t, e.bp, e.dm)
	m2 := newMem(t)
	dm2 := mustCreate(t, m2, dbName)
	for dm2.PageCount() < e.dm.PageCount() {
		if _, err := dm2.Allocate(bg); err != nil {
			t.Fatal(err)
		}
	}
	_ = dm2.Close()
	samePages(t, replayOnto(t, m2, e.lg.records(), 0, 5), want, "replay of a concurrent run")
}

// --- failure atomicity -------------------------------------------------------

func TestFailedLogLeavesPagesUntouched(t *testing.T) {
	setup := func(t *testing.T) (*loggedEnv, *Heap, RID) {
		e := newLoggedEnv(t, 8)
		h, err := CreateHeap(bg, e.bp, WithLogger(e.lg))
		if err != nil {
			t.Fatal(err)
		}
		a, err := h.put(bg, []byte("aaaa"))
		if err != nil {
			t.Fatal(err)
		}
		h.fillPage(t, 0, 9) // page full
		return e, h, a
	}
	snapshot := func(e *loggedEnv) map[uint64][]byte {
		out := map[uint64][]byte{}
		e.bp.mu.Lock()
		defer e.bp.mu.Unlock()
		for id, f := range e.bp.table {
			out[id] = bytes.Clone(f.data)
		}
		return out
	}
	ops := map[string]func(h *Heap, a RID) error{
		"insert":            func(h *Heap, a RID) error { _, err := h.put(bg, []byte("x")); return err },
		"insert that grows": func(h *Heap, a RID) error { _, err := h.put(bg, bytes.Repeat([]byte{1}, 6000)); return err },
		"update in place":   func(h *Heap, a RID) error { return h.set(bg, a, []byte("bb")) },
		"update that moves": func(h *Heap, a RID) error { return h.set(bg, a, bytes.Repeat([]byte{2}, 4000)) },
		"update that moves again": func(h *Heap, a RID) error {
			return h.set(bg, a, bytes.Repeat([]byte{2}, 7000))
		},
		"update that moves home": func(h *Heap, a RID) error { return h.set(bg, a, []byte("c")) },
		"delete":                 func(h *Heap, a RID) error { return h.del(bg, a) },
		"delete of a moved row":  func(h *Heap, a RID) error { return h.del(bg, a) },
		"create heap": func(h *Heap, a RID) error {
			_, err := CreateHeap(bg, h.bp, WithLogger(h.lg))
			return err
		},
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			e, h, a := setup(t)
			switch name {
			case "update that moves":
				// Make sure a second page exists so the move itself (not a
				// grow) is the logged step that fails.
				if _, err := h.put(bg, bytes.Repeat([]byte{3}, 100)); err != nil {
					t.Fatal(err)
				}
			case "update that moves again", "update that moves home", "delete of a moved row":
				// a has moved to a second page; a third has room for it.
				if err := h.set(bg, a, bytes.Repeat([]byte{2}, 4000)); err != nil {
					t.Fatal(err)
				}
				if err := h.growLogged(bg); err != nil {
					t.Fatal(err)
				}
				if stub, err := h.readTuple(bg, a); err != nil || stub.kind != tupleStub {
					t.Fatalf("setup: row did not move: %v", err)
				}
			}
			before := snapshot(e)
			beforeRows := scanAll(t, h)
			e.lg.setFail(errBoom)
			if err := op(h, a); !errors.Is(err, errBoom) {
				t.Fatalf("err = %v, want the logger's error", err)
			}
			e.lg.setFail(nil)
			after := snapshot(e)
			for id, img := range before {
				if !bytes.Equal(after[id], img) {
					t.Fatalf("page %d changed although its change was not logged", id)
				}
			}
			e.bp.checkInvariants(t, 0)
			h.checkFSM(t)
			sameRows(t, scanAll(t, h), beforeRows, "rows after the failed operation")
			// The heap keeps working once the log does.
			if _, err := h.put(bg, []byte("later")); err != nil {
				t.Fatalf("heap unusable after a failed log append: %v", err)
			}
		})
	}
}

func readFileBytes(t testing.TB, fsys vfs.FS, name string) []byte {
	t.Helper()
	f, err := fsys.OpenFile(name, vfs.ORead)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	size, _ := f.Size()
	b := make([]byte, size)
	if _, err := f.ReadAt(b, 0); err != nil {
		t.Fatal(err)
	}
	return b
}

func writeFileBytes(t testing.TB, fsys vfs.FS, name string, data []byte) {
	t.Helper()
	f, err := fsys.OpenFile(name, vfs.ORead|vfs.OWrite|vfs.OCreate|vfs.OTrunc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err := fsys.SyncDir("/db"); err != nil {
		t.Fatal(err)
	}
}

// A page that changes again while the pool is forcing the log for it must
// still be written safely: the pool writes the copy it took (whose LSN the
// log now covers), and the newer change keeps the page dirty. This is the
// interleaving the concurrent checkpoint test in the wal package found.
func TestFlushWhilePageChangesDuringLogForce(t *testing.T) {
	m := newMem(t)
	dm := mustCreate(t, m, dbName)
	lg := newFakeLogger()
	st := &ruleStore{PageStore: dm, flushed: lg.flushedLSN}
	var h *Heap
	changed := false
	bp, err := NewBufferPool(st, Options{
		Frames:     4,
		FlushedLSN: lg.flushedLSN,
		FlushWAL: func(ctx context.Context, lsn uint64) error {
			if err := lg.flushWAL(ctx, lsn); err != nil {
				return err
			}
			if !changed { // someone changes the page right after the log was forced
				changed = true
				if _, err := h.put(ctx, []byte("sneaked in")); err != nil {
					return err
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h, err = CreateHeap(bg, bp, WithLogger(lg))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.put(bg, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := bp.FlushPage(bg, h.FirstPage()); err != nil {
		t.Fatalf("FlushPage: %v", err)
	}
	if !changed {
		t.Fatal("the hook never ran")
	}
	if n := st.violations.Load(); n != 0 {
		t.Fatalf("WAL rule violated: %v", st.firstBad.Load())
	}
	// The newer change is still pending and reaches disk on the next flush.
	if err := bp.FlushAll(bg); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, PageSize)
	if err := dm.ReadPage(bg, h.FirstPage(), buf); err != nil {
		t.Fatal(err)
	}
	sp, _ := NewSlottedPage(buf)
	if sp.LiveCount() != 2 {
		t.Fatalf("page on disk has %d rows, want both", sp.LiveCount())
	}
}

// Redo must be idempotent record by record: applying an operation record to a
// page that already contains it (its LSN is not below the record's) changes
// nothing. Full replays never hit this because each page's first record after
// the redo point is an image, so it is tested directly.
func TestRedoSkipsRecordsThePageAlreadyHas(t *testing.T) {
	e := newLoggedEnv(t, 8)
	rng := rand.New(rand.NewPCG(testSeed(t), 12))
	runLoggedWorkload(t, e, rng, 300, 2)
	recs := e.lg.records()

	m2 := newMem(t)
	dm2 := mustCreate(t, m2, dbName)
	for dm2.PageCount() < e.dm.PageCount() {
		if _, err := dm2.Allocate(bg); err != nil {
			t.Fatal(err)
		}
	}
	bp2, err := NewBufferPool(dm2, Options{Frames: 64})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := RedoHeapRecord(bg, bp2, r.lsn, r.payload); err != nil {
			t.Fatal(err)
		}
	}
	want := pageImages(t, bp2, dm2)
	ops := 0
	for _, r := range recs {
		blocks, _ := DecodeHeapRecord(r.payload)
		image := false
		for _, b := range blocks {
			image = image || b.Kind == BlockImage
		}
		if image {
			continue // an image deliberately rewinds the page; covered elsewhere
		}
		if err := RedoHeapRecord(bg, bp2, r.lsn, r.payload); err != nil {
			t.Fatalf("re-applying record %d: %v", r.lsn, err)
		}
		ops++
	}
	if ops == 0 {
		t.Fatal("no operation records to re-apply")
	}
	samePages(t, pageImages(t, bp2, dm2), want, "after re-applying every operation record")
}

// Two-page operations must take their latches in a fixed order, or two of
// them on the same pages in opposite directions deadlock. With the wrong
// order this test hangs (and the test timeout reports it).
func TestTwoPageLatchesNeverDeadlock(t *testing.T) {
	e := newLoggedEnv(t, 8)
	h, err := CreateHeap(bg, e.bp, WithLogger(e.lg))
	if err != nil {
		t.Fatal(err)
	}
	a := h.FirstPage()
	if err := h.growLogged(bg); err != nil {
		t.Fatal(err)
	}
	b := h.pages[1].id
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x, y := a, b
			if g%2 == 1 {
				x, y = b, a
			}
			for range 2000 {
				err := h.withPages(bg, []uint64{x, y}, func(*edit) error {
					runtime.Gosched() // hold both latches while others queue
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// dirtyCheckLogger runs a check right after each record is appended, while the
// heap still holds the page latched: the moment a checkpoint could begin.
type dirtyCheckLogger struct {
	*fakeLogger
	after func()
}

func (l *dirtyCheckLogger) Log(ctx context.Context, build func(uint64) []byte) (uint64, error) {
	lsn, err := l.fakeLogger.Log(ctx, build)
	if err == nil && l.after != nil {
		l.after()
	}
	return lsn, err
}

// Every page a logged operation changes must already be marked dirty when its
// record is appended. Otherwise a checkpoint beginning right then would not
// flush the page, recovery would start after the record, and the change would
// be lost. (Found by TestCheckpointsDuringConcurrentWorkThenCrash in the wal
// package; this pins it down deterministically.)
func TestPagesAreDirtyBeforeTheirRecordIsAppended(t *testing.T) {
	e := newLoggedEnv(t, 8)
	lg := &dirtyCheckLogger{fakeLogger: e.lg}
	h, err := CreateHeap(bg, e.bp, WithLogger(lg))
	if err != nil {
		t.Fatal(err)
	}
	rid, err := h.put(bg, []byte("row"))
	if err != nil {
		t.Fatal(err)
	}
	// A filler leaving the page almost full, so the row's growth moves it.
	filler, err := h.put(bg, bytes.Repeat([]byte{1}, min(h.pages[0].free-RowHeaderSize-slotSize-10, MaxRowData)))
	if err != nil {
		t.Fatal(err)
	}
	h.fillPage(t, 0, 2)
	checks := 0
	lg.after = func() {
		recs := e.lg.records()
		blocks, _ := DecodeHeapRecord(recs[len(recs)-1].payload)
		e.bp.mu.Lock()
		defer e.bp.mu.Unlock()
		for _, b := range blocks {
			if f, ok := e.bp.table[b.Page]; !ok || !f.dirty.Load() {
				t.Errorf("page %d is not marked dirty when its record (kind %d) is appended", b.Page, b.Kind)
			}
		}
		checks++
	}
	ops := []func() error{
		func() error { _, err := h.put(bg, []byte("x")); return err },
		func() error { return h.set(bg, rid, []byte("same size!")) },
		func() error { return h.set(bg, rid, bytes.Repeat([]byte{2}, 3000)) }, // moves (grows first)
		func() error { return h.set(bg, rid, bytes.Repeat([]byte{2}, 7000)) }, // moves again
		func() error { return h.set(bg, rid, []byte("home")) },                // moves home
		func() error { return h.del(bg, filler) },
		func() error { _, err := CreateHeap(bg, e.bp, WithLogger(lg)); return err },
	}
	for i, op := range ops {
		// Start every operation from clean pages, as after a checkpoint.
		if err := e.bp.FlushAll(bg); err != nil {
			t.Fatal(err)
		}
		if err := op(); err != nil {
			t.Fatalf("op %d: %v", i, err)
		}
	}
	if checks < len(ops) {
		t.Fatalf("only %d records checked", checks)
	}
}
