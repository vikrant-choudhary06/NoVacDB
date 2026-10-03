package undo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

var bg = context.Background()

func testSeed(t testing.TB) uint64 {
	t.Helper()
	seed := uint64(1)
	if s := os.Getenv("NOVACDB_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("bad NOVACDB_SEED %q: %v", s, err)
		}
		seed = v
	}
	if tt, ok := t.(*testing.T); ok {
		tt.Logf("seed = %d (override with NOVACDB_SEED)", seed)
	}
	return seed
}

// Record types of the fake log.
const (
	recUndo = iota
	recSegment
	recFree
)

type fakeRec struct {
	typ     int
	lsn     uint64
	payload []byte
	pages   []uint64 // deferred frees
}

// fakeLog is an in-memory log with the real WAL's LSN and durability
// semantics: LSNs grow by record size, FlushedLSN is "durable end - 1", and
// the redo point moves when a checkpoint begins. It checks that every Undo
// record decodes.
type fakeLog struct {
	mu      sync.Mutex
	next    uint64
	durable uint64
	redo    uint64
	recs    []fakeRec
	// fail, if set, fails the next appends of the given type (-1 any).
	fail     error
	failType int
	images   int
	ops      int
}

const fakeFirstLSN = 32

func newFakeLog() *fakeLog {
	return &fakeLog{next: fakeFirstLSN, durable: fakeFirstLSN, redo: fakeFirstLSN, failType: -1}
}

func (l *fakeLog) append(typ int, p []byte, pages []uint64) (uint64, error) {
	if l.fail != nil && (l.failType == -1 || l.failType == typ) {
		return 0, l.fail
	}
	lsn := l.next
	l.recs = append(l.recs, fakeRec{typ: typ, lsn: lsn, payload: bytes.Clone(p), pages: slices.Clone(pages)})
	l.next += 20 + uint64(len(p)) + 8*uint64(len(pages))
	return lsn, nil
}

func (l *fakeLog) LogUndo(_ context.Context, build func(uint64) []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fail != nil && (l.failType == -1 || l.failType == recUndo) {
		return 0, l.fail
	}
	p := build(l.redo)
	blocks, err := DecodeBlocks(p)
	if err != nil {
		panic(fmt.Sprintf("undo log produced an undecodable record: %v", err)) // test invariant
	}
	for _, b := range blocks {
		if b.Op == OpImage {
			l.images++
		} else {
			l.ops++
		}
	}
	return l.append(recUndo, p, nil)
}

func (l *fakeLog) LogUndoSegment(_ context.Context, p []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := DecodeSegmentEntries(p); err != nil {
		panic(fmt.Sprintf("undo log produced an undecodable segment record: %v", err)) // test invariant
	}
	return l.append(recSegment, p, nil)
}

func (l *fakeLog) DeferFree(_ context.Context, pages ...uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.append(recFree, nil, pages)
	return err
}

func (l *fakeLog) flushedLSN() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.durable - 1
}

func (l *fakeLog) flushWAL(context.Context, uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.durable = l.next
	return nil
}

func (l *fakeLog) beginCheckpoint() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.redo = l.next
	return l.redo
}

func (l *fakeLog) records() []fakeRec {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.recs)
}

func (l *fakeLog) setFail(typ int, err error) {
	l.mu.Lock()
	l.fail, l.failType = err, typ
	l.mu.Unlock()
}

// ruleStore counts page writes that break the WAL rule.
type ruleStore struct {
	storage.PageStore
	flushed    func() uint64
	writes     int
	violations int
	mu         sync.Mutex
}

func (s *ruleStore) WritePage(ctx context.Context, id uint64, buf []byte) error {
	s.mu.Lock()
	s.writes++
	if storage.PageLSN(buf) > s.flushed() {
		s.violations++
	}
	s.mu.Unlock()
	return s.PageStore.WritePage(ctx, id, buf)
}

type env struct {
	m     *vfs.MemFS
	dm    *storage.DiskManager
	store *ruleStore
	bp    *storage.BufferPool
	lg    *fakeLog
	log   *Log
}

func newEnv(t testing.TB, frames int) *env {
	t.Helper()
	m := vfs.NewMemFS(1)
	dm, err := storage.Create(m, "/data")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dm.Close() })
	lg := newFakeLog()
	st := &ruleStore{PageStore: dm, flushed: lg.flushedLSN}
	bp, err := storage.NewBufferPool(st, storage.Options{Frames: frames, FlushedLSN: lg.flushedLSN, FlushWAL: lg.flushWAL})
	if err != nil {
		t.Fatal(err)
	}
	return &env{m: m, dm: dm, store: st, bp: bp, lg: lg, log: New(bp, lg)}
}

// pageImages flushes the pool and returns every written page as stored.
func pageImages(t testing.TB, bp *storage.BufferPool, dm *storage.DiskManager) map[uint64][]byte {
	t.Helper()
	if err := bp.FlushAll(bg); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}
	out := map[uint64][]byte{}
	for id := uint64(storage.FirstDataPage); id < dm.PageCount(); id++ {
		buf := make([]byte, storage.PageSize)
		if err := dm.ReadPage(bg, id, buf); err != nil {
			if errors.Is(err, storage.ErrZeroPage) {
				continue // allocated, never written
			}
			t.Fatalf("reading page %d: %v", id, err)
		}
		out[id] = buf
	}
	return out
}

// replayed is a data file rebuilt by replaying a fake log.
type replayed struct {
	dm  *storage.DiskManager
	bp  *storage.BufferPool
	log *Log
}

// replay replays recs from lsn from onto the data file in m with a fresh
// pool and undo log, as the engine's recovery does, and runs Recover.
func replay(t testing.TB, m *vfs.MemFS, recs []fakeRec, from uint64, frames int) (*replayed, error) {
	t.Helper()
	dm, err := storage.Open(m, "/data")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dm.Close() })
	bp, err := storage.NewBufferPool(dm, storage.Options{Frames: frames})
	if err != nil {
		t.Fatal(err)
	}
	r := &replayed{dm: dm, bp: bp, log: New(bp, newFakeLog())}
	for _, rec := range recs {
		if rec.lsn < from {
			continue
		}
		switch rec.typ {
		case recUndo:
			err = Redo(bg, bp, rec.lsn, rec.payload)
		case recSegment:
			err = r.log.RedoSegments(rec.payload)
		}
		if err != nil {
			return r, fmt.Errorf("redo of record at %d: %w", rec.lsn, err)
		}
	}
	return r, r.log.Recover(bg)
}

// sizedFile returns a MemFS holding a fresh data file with as many pages
// allocated as dm has.
func sizedFile(t testing.TB, dm *storage.DiskManager, seed uint64) *vfs.MemFS {
	t.Helper()
	m := vfs.NewMemFS(seed)
	dm2, err := storage.Create(m, "/data")
	if err != nil {
		t.Fatal(err)
	}
	for dm2.PageCount() < dm.PageCount() {
		if _, err := dm2.Allocate(bg); err != nil {
			t.Fatal(err)
		}
	}
	if err := dm2.Close(); err != nil {
		t.Fatal(err)
	}
	return m
}

func samePages(t testing.TB, got, want map[uint64][]byte, what string) {
	t.Helper()
	for id, w := range want {
		g, ok := got[id]
		if !ok || !bytes.Equal(g, w) {
			t.Fatalf("%s: page %d differs (lsn %d vs %d)", what, id, storage.PageLSN(g), storage.PageLSN(w))
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			t.Fatalf("%s: extra page %d", what, id)
		}
	}
}

// randomRecord returns a random valid record of xid. Images are mostly
// small, sometimes up to MaxImage.
func randomRecord(rng *rand.Rand, xid uint64) Record {
	rec := Record{
		Kind:  Kind(1 + rng.IntN(3)),
		XID:   xid,
		Table: rng.Uint64N(8),
		RID:   storage.RID{Page: storage.FirstDataPage + rng.Uint64N(1000), Slot: uint16(rng.IntN(300))},
	}
	if rng.IntN(3) == 0 {
		rec.PrevForRow = MakePtr(storage.FirstDataPage+rng.Uint64N(1000), pageHeaderSize+rng.IntN(storage.PageSize-pageHeaderSize-RecordHeaderSize+1))
	}
	if rec.Kind != Insert {
		n := 1 + rng.IntN(200)
		switch rng.IntN(20) {
		case 0:
			n = MaxImage
		case 1, 2:
			n = 1 + rng.IntN(MaxImage)
		}
		rec.Image = make([]byte, n)
		for i := range rec.Image {
			rec.Image[i] = byte(rng.Uint32())
		}
	}
	return rec
}

func sameRecord(a, b Record) bool {
	return a.Kind == b.Kind && a.XID == b.XID && a.PrevForRow == b.PrevForRow && a.PrevInTxn == b.PrevInTxn &&
		a.Table == b.Table && a.RID == b.RID && bytes.Equal(a.Image, b.Image)
}

func mustAppend(t testing.TB, l *Log, rec Record) Ptr {
	t.Helper()
	p, err := l.Append(bg, rec)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return p
}
