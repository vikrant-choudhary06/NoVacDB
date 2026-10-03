package undo

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

func TestAppendReadAcrossPages(t *testing.T) {
	rng := rand.New(rand.NewPCG(testSeed(t), 2))
	e := newEnv(t, 6)
	const xid = 7
	var ptrs []Ptr
	var recs []Record
	for i := range 600 {
		rec := randomRecord(rng, xid)
		rec.PrevInTxn = MakePtr(99, 99) // Append ignores it
		p := mustAppend(t, e.log, rec)
		rec.PrevInTxn = 0
		if i > 0 {
			rec.PrevInTxn = ptrs[i-1]
		}
		if e.log.Last(xid) != p {
			t.Fatalf("Last = %s, appended at %s", e.log.Last(xid), p)
		}
		ptrs, recs = append(ptrs, p), append(recs, rec)
	}
	pages := e.log.Pages(xid)
	if len(pages) < 20 {
		t.Fatalf("only %d pages", len(pages))
	}
	for i, p := range ptrs {
		got, err := e.log.Read(bg, p)
		if err != nil || !sameRecord(got, recs[i]) {
			t.Fatalf("record %d at %s: %+v, %v", i, p, got, err)
		}
		if !slices.Contains(pages, p.Page()) {
			t.Fatalf("record %d on page %d, not in the segment %v", i, p.Page(), pages)
		}
	}
	// The transaction's chain links every record, newest first.
	i := len(ptrs) - 1
	for p := e.log.Last(xid); p != 0; i-- {
		if i < 0 || p != ptrs[i] {
			t.Fatalf("chain reaches %s, want record %d", p, i)
		}
		rec, err := e.log.Read(bg, p)
		if err != nil {
			t.Fatal(err)
		}
		p = rec.PrevInTxn
	}
	if i != -1 {
		t.Fatalf("chain stopped before record %d", i)
	}
	// The pages link in order and end.
	for j, id := range pages {
		buf := make([]byte, storage.PageSize)
		if err := e.bp.FlushAll(bg); err != nil {
			t.Fatal(err)
		}
		if err := e.dm.ReadPage(bg, id, buf); err != nil {
			t.Fatal(err)
		}
		ph, err := pageRecords(buf, id, nil)
		if err != nil {
			t.Fatal(err)
		}
		if want := uint64(0); j+1 < len(pages) {
			want = pages[j+1]
			if ph.next != want {
				t.Fatalf("page %d links to %d, want %d", id, ph.next, want)
			}
		} else if ph.next != 0 {
			t.Fatalf("last page links to %d", ph.next)
		}
	}
	if e.store.violations != 0 {
		t.Fatalf("%d WAL-rule violations", e.store.violations)
	}
	if got := e.log.Segments(); !slices.Equal(got, []uint64{xid}) {
		t.Fatalf("segments %v", got)
	}
}

func TestPagesFillUp(t *testing.T) {
	// Records of a size that fits a known number per page: no page is
	// left with room for the next one.
	e := newEnv(t, 4)
	img := make([]byte, 1000-RecordHeaderSize)
	perPage := (storage.PageSize - pageHeaderSize) / 1000
	for range perPage * 3 {
		mustAppend(t, e.log, Record{Kind: Delete, XID: 1, RID: storage.RID{Page: 5}, Image: img})
	}
	if n := len(e.log.Pages(1)); n != 3 {
		t.Fatalf("%d records of 1000 bytes on %d pages, want 3", perPage*3, n)
	}
	// A record exactly filling a page goes on a new one, alone.
	p := mustAppend(t, e.log, Record{Kind: Update, XID: 1, RID: storage.RID{Page: 5}, Image: make([]byte, MaxImage)})
	if p.Offset() != pageHeaderSize || len(e.log.Pages(1)) != 4 {
		t.Fatalf("largest record at %s, %d pages", p, len(e.log.Pages(1)))
	}
	q := mustAppend(t, e.log, Record{Kind: Insert, XID: 1, RID: storage.RID{Page: 5}})
	if q.Page() == p.Page() {
		t.Fatal("a record after a full page went on the same page")
	}
}

func TestSegmentsAreSeparate(t *testing.T) {
	rng := rand.New(rand.NewPCG(testSeed(t), 3))
	e := newEnv(t, 8)
	want := map[uint64][]Ptr{}
	for range 1500 {
		xid := 1 + rng.Uint64N(5)
		want[xid] = append(want[xid], mustAppend(t, e.log, randomRecord(rng, xid)))
	}
	if got := e.log.Segments(); !slices.Equal(got, []uint64{1, 2, 3, 4, 5}) {
		t.Fatalf("segments %v", got)
	}
	owner := map[uint64]uint64{}
	for xid, ptrs := range want {
		for _, id := range e.log.Pages(xid) {
			if o, ok := owner[id]; ok {
				t.Fatalf("page %d in the segments of %d and %d", id, o, xid)
			}
			owner[id] = xid
		}
		for i := len(ptrs) - 1; i >= 0; i-- {
			rec, err := e.log.Read(bg, ptrs[i])
			if err != nil || rec.XID != xid {
				t.Fatalf("record %s: %+v %v", ptrs[i], rec, err)
			}
			if prev := Ptr(0); i > 0 {
				prev = ptrs[i-1]
				if rec.PrevInTxn != prev {
					t.Fatalf("record %d of %d links to %s, want %s", i, xid, rec.PrevInTxn, prev)
				}
			} else if rec.PrevInTxn != 0 {
				t.Fatalf("first record of %d links to %s", xid, rec.PrevInTxn)
			}
		}
	}
	if e.log.Last(99) != 0 || e.log.Pages(99) != nil {
		t.Fatal("a transaction without undo has some")
	}
}

func TestAppendRejectsInvalidRecords(t *testing.T) {
	e := newEnv(t, 4)
	mustAppend(t, e.log, Record{Kind: Insert, XID: 1, RID: storage.RID{Page: 5}})
	before := len(e.lg.records())
	rid := storage.RID{Page: 5}
	for name, c := range map[string]struct {
		rec  Record
		want error
	}{
		"kind 0":             {Record{XID: 1, RID: rid}, ErrInvalidRecord},
		"kind 4":             {Record{Kind: 4, XID: 1, RID: rid}, ErrInvalidRecord},
		"XID 0":              {Record{Kind: Insert, RID: rid}, ErrInvalidRecord},
		"insert with image":  {Record{Kind: Insert, XID: 1, RID: rid, Image: []byte{1}}, ErrInvalidRecord},
		"update, no image":   {Record{Kind: Update, XID: 1, RID: rid}, ErrInvalidRecord},
		"delete, no image":   {Record{Kind: Delete, XID: 1, RID: rid, Image: []byte{}}, ErrInvalidRecord},
		"bad row pointer":    {Record{Kind: Insert, XID: 1, RID: rid, PrevForRow: MakePtr(5, 3)}, ErrInvalidRecord},
		"RID in page 0":      {Record{Kind: Insert, XID: 1}, ErrInvalidRecord},
		"image too large":    {Record{Kind: Update, XID: 1, RID: rid, Image: make([]byte, MaxImage+1)}, ErrImageTooLarge},
		"new XID, too large": {Record{Kind: Delete, XID: 2, RID: rid, Image: make([]byte, MaxImage+1)}, ErrImageTooLarge},
	} {
		if _, err := e.log.Append(bg, c.rec); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
	}
	if n := len(e.lg.records()); n != before {
		t.Fatalf("invalid records logged %d records", n-before)
	}
	if got := e.log.Segments(); !slices.Equal(got, []uint64{1}) {
		t.Fatalf("segments %v", got)
	}
}

func TestReadRejectsBadPointers(t *testing.T) {
	e := newEnv(t, 4)
	img := make([]byte, 100)
	p1 := mustAppend(t, e.log, Record{Kind: Update, XID: 1, RID: storage.RID{Page: 5}, Image: img})
	p2 := mustAppend(t, e.log, Record{Kind: Insert, XID: 1, RID: storage.RID{Page: 5}})
	ref, err := e.bp.NewPage(bg, storage.PageTypeHeap)
	if err != nil {
		t.Fatal(err)
	}
	heap := ref.ID()
	if err := ref.Unpin(true); err != nil {
		t.Fatal(err)
	}
	for name, p := range map[string]Ptr{
		"none":                0,
		"page 0":              MakePtr(0, 100),
		"before the records":  MakePtr(p1.Page(), 40),
		"inside a record":     p1 + 1,
		"inside an image":     p1 + RecordHeaderSize + 10,
		"after the records":   p2 + RecordHeaderSize,
		"far past the record": MakePtr(p1.Page(), 4000),
		"a heap page":         MakePtr(heap, pageHeaderSize),
		"past the file":       MakePtr(e.dm.PageCount()+10, pageHeaderSize),
		"near the page end":   MakePtr(p1.Page(), storage.PageSize-1),
	} {
		if _, err := e.log.Read(bg, p); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s (%s): err = %v", name, p, err)
		}
	}
	for _, p := range []Ptr{p1, p2} {
		if _, err := e.log.Read(bg, p); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
	}
}

func TestReadReturnsACopy(t *testing.T) {
	e := newEnv(t, 4)
	p := mustAppend(t, e.log, Record{Kind: Update, XID: 1, RID: storage.RID{Page: 5}, Image: []byte("old row")})
	rec, err := e.log.Read(bg, p)
	if err != nil {
		t.Fatal(err)
	}
	rec.Image[0] = 'X'
	again, err := e.log.Read(bg, p)
	if err != nil || string(again.Image) != "old row" {
		t.Fatalf("image after changing a read copy: %q %v", again.Image, err)
	}
}

// rowModel is the history of every row: its records, newest last.
type rowModel map[[2]uint64][]Record

func TestChainModel(t *testing.T) {
	base := testSeed(t)
	for i := range 4 {
		seed := base + uint64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 4))
			e := newEnv(t, 8+rng.IntN(8))
			model := rowModel{}
			latest := map[[2]uint64]Ptr{}
			xid := uint64(0)
			for range 60 {
				xid++
				for range 1 + rng.IntN(60) {
					table := rng.Uint64N(4)
					rid := storage.RID{Page: storage.FirstDataPage + rng.Uint64N(6), Slot: uint16(rng.IntN(10))}
					key := [2]uint64{table, rid.Page<<16 | uint64(rid.Slot)}
					rec := randomRecord(rng, xid)
					rec.Table, rec.RID, rec.PrevForRow = table, rid, latest[key]
					p := mustAppend(t, e.log, rec)
					latest[key] = p
					model[key] = append(model[key], rec)
				}
			}
			check := func(l *Log, what string) {
				t.Helper()
				for key, hist := range model {
					n := len(hist) - 1
					for p := latest[key]; p != 0; n-- {
						rec, err := l.Read(bg, p)
						if err != nil {
							t.Fatalf("%s: %v", what, err)
						}
						want := hist[n]
						want.PrevInTxn = rec.PrevInTxn
						if n < 0 || !sameRecord(rec, want) {
							t.Fatalf("%s: row %v version %d differs", what, key, n)
						}
						p = rec.PrevForRow
					}
					if n != -1 {
						t.Fatalf("%s: row %v chain stopped with %d versions left", what, key, n+1)
					}
				}
			}
			check(e.log, "live")
			if len(e.log.Segments()) != 60 {
				t.Fatalf("%d segments", len(e.log.Segments()))
			}
			// The same chains after replaying the log onto a fresh file.
			want := pageImages(t, e.bp, e.dm)
			r, err := replay(t, sizedFile(t, e.dm, seed), e.lg.records(), 0, 6)
			if err != nil {
				t.Fatal(err)
			}
			samePages(t, pageImages(t, r.bp, r.dm), want, "replay")
			check(r.log, "replayed")
		})
	}
}

// workload appends random records of a few transactions, with checkpoints
// that move the redo point and flush, and releases some segments.
func workload(t *testing.T, e *env, rng *rand.Rand, steps int, xid *uint64) {
	t.Helper()
	for range steps {
		switch r := rng.IntN(100); {
		case r < 2:
			e.lg.beginCheckpoint()
			if err := e.log.Relog(bg); err != nil {
				t.Fatal(err)
			}
			if err := e.bp.FlushAll(bg); err != nil {
				t.Fatal(err)
			}
		case r < 4:
			if segs := e.log.Segments(); len(segs) > 0 {
				if err := e.log.Release(bg, segs[rng.IntN(len(segs))]); err != nil {
					t.Fatal(err)
				}
			}
		case r < 6:
			*xid++
		default:
			x := *xid - rng.Uint64N(min(*xid, 3))
			mustAppend(t, e.log, randomRecord(rng, x))
		}
	}
}

type segState struct {
	pages []uint64
	last  Ptr
}

func segments(l *Log) map[uint64]segState {
	out := map[uint64]segState{}
	for _, xid := range l.Segments() {
		out[xid] = segState{l.Pages(xid), l.Last(xid)}
	}
	return out
}

func sameSegments(t *testing.T, got, want map[uint64]segState, what string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d segments, want %d", what, len(got), len(want))
	}
	for xid, w := range want {
		g, ok := got[xid]
		if !ok || g.last != w.last || !slices.Equal(g.pages, w.pages) {
			t.Fatalf("%s: segment of %d is %+v, want %+v", what, xid, g, w)
		}
	}
}

func TestReplayReproducesPagesAndSegments(t *testing.T) {
	base := testSeed(t)
	for i := range 6 {
		seed := base + uint64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 5))
			e := newEnv(t, 4+rng.IntN(8))
			xid := uint64(1)
			workload(t, e, rng, 3000, &xid)
			if e.store.violations != 0 {
				t.Fatalf("%d WAL-rule violations", e.store.violations)
			}
			if e.lg.images == 0 || e.lg.ops == 0 || e.store.writes == 0 {
				t.Fatalf("workload too tame: %d images, %d operations, %d writes", e.lg.images, e.lg.ops, e.store.writes)
			}
			want, wantSegs := pageImages(t, e.bp, e.dm), segments(e.log)
			m := sizedFile(t, e.dm, seed)
			recs := e.lg.records()
			r, err := replay(t, m, recs, 0, 4+rng.IntN(8))
			if err != nil {
				t.Fatal(err)
			}
			samePages(t, pageImages(t, r.bp, r.dm), want, "full replay")
			sameSegments(t, segments(r.log), wantSegs, "full replay")
			_ = r.dm.Close()
			// Replaying again on top (pages already newer) changes nothing.
			r, err = replay(t, m, recs, 0, 5)
			if err != nil {
				t.Fatal(err)
			}
			samePages(t, pageImages(t, r.bp, r.dm), want, "second replay")
			sameSegments(t, segments(r.log), wantSegs, "second replay")
		})
	}
}

// What recovery faces: pages changed after the redo point may be torn on
// disk, and the log before the redo point is gone. Replaying from the redo
// point repairs every page and finds every segment, through the table the
// checkpoint logged again.
func TestReplayFromRedoPointRepairsTornPages(t *testing.T) {
	base := testSeed(t)
	for i := range 6 {
		seed := base + 100 + uint64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 6))
			e := newEnv(t, 4+rng.IntN(8))
			xid := uint64(1)
			workload(t, e, rng, 1500, &xid)
			redo := e.lg.beginCheckpoint()
			if err := e.log.Relog(bg); err != nil {
				t.Fatal(err)
			}
			if err := e.bp.FlushAll(bg); err != nil {
				t.Fatal(err)
			}
			workload(t, e, rng, 400, &xid)
			if err := e.dm.Sync(bg); err != nil {
				t.Fatal(err)
			}
			raw := readAll(t, e.m, "/data")
			if need := int(e.dm.PageCount()) * storage.PageSize; len(raw) < need {
				raw = append(raw, make([]byte, need-len(raw))...)
			}
			want, wantSegs := pageImages(t, e.bp, e.dm), segments(e.log)
			torn := 0
			for id, img := range want {
				if storage.PageLSN(img) >= redo && rng.IntN(2) == 0 {
					off := int(id) * storage.PageSize
					for k := range storage.PageSize / 2 {
						raw[off+storage.PageSize/4+k] ^= 0xA5
					}
					torn++
				}
			}
			crashed := vfs.NewMemFS(seed)
			writeAll(t, crashed, "/data", raw)
			r, err := replay(t, crashed, e.lg.records(), redo, 4+rng.IntN(8))
			if err != nil {
				t.Fatal(err)
			}
			samePages(t, pageImages(t, r.bp, r.dm), want, fmt.Sprintf("replay from %d with %d torn pages", redo, torn))
			sameSegments(t, segments(r.log), wantSegs, "replay from the redo point")
		})
	}
}

func readAll(t testing.TB, fsys vfs.FS, name string) []byte {
	t.Helper()
	f, err := fsys.OpenFile(name, vfs.ORead)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	return buf
}

func writeAll(t testing.TB, fsys vfs.FS, name string, data []byte) {
	t.Helper()
	f, err := fsys.OpenFile(name, vfs.ORead|vfs.OWrite|vfs.OCreate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestImageRule(t *testing.T) {
	e := newEnv(t, 4)
	img := make([]byte, 100)
	add := func() Ptr {
		return mustAppend(t, e.log, Record{Kind: Update, XID: 1, RID: storage.RID{Page: 5}, Image: img})
	}
	lastBlocks := func() []Block {
		recs := e.lg.records()
		for i := len(recs) - 1; i >= 0; i-- {
			if recs[i].typ == recUndo {
				b, err := DecodeBlocks(recs[i].payload)
				if err != nil {
					t.Fatal(err)
				}
				return b
			}
		}
		t.Fatal("no undo record")
		return nil
	}
	p := add()
	if b := lastBlocks(); len(b) != 1 || b[0].Op != OpImage || b[0].Page != p.Page() {
		t.Fatalf("first record: %+v", b)
	}
	if recs := e.lg.records(); recs[len(recs)-1].typ != recSegment {
		t.Fatal("the segment was not logged after its first page")
	}
	add()
	if b := lastBlocks(); len(b) != 1 || b[0].Op != OpAppend {
		t.Fatalf("second record: op %d", b[0].Op)
	}
	e.lg.beginCheckpoint()
	add()
	if b := lastBlocks(); len(b) != 1 || b[0].Op != OpImage {
		t.Fatalf("first change after the redo point: op %d", b[0].Op)
	}
	add()
	if b := lastBlocks(); b[0].Op != OpAppend {
		t.Fatalf("second change after the redo point: op %d", b[0].Op)
	}
	// Growing: a link and the new page's image, in one record.
	for len(e.log.Pages(1)) == 1 {
		add()
	}
	if b := lastBlocks(); len(b) != 2 || b[0].Op != OpSetNext || b[1].Op != OpImage || b[0].Next != b[1].Page {
		t.Fatalf("growing: %+v", b)
	}
	// Growing right after a redo point: the last page as an image.
	for {
		before := len(e.log.Pages(1))
		e.lg.beginCheckpoint()
		add()
		if len(e.log.Pages(1)) > before {
			break
		}
	}
	if b := lastBlocks(); len(b) != 2 || b[0].Op != OpImage || b[1].Op != OpImage {
		t.Fatalf("growing after a redo point: %+v", b)
	}
}

func TestFailedLogLeavesPagesUntouched(t *testing.T) {
	boom := errors.New("boom")
	img := make([]byte, 3000)
	rec := Record{Kind: Delete, XID: 1, RID: storage.RID{Page: 5}, Image: img}
	snapshot := func(e *env) map[uint64][]byte { return pageImages(t, e.bp, e.dm) }

	t.Run("append in place", func(t *testing.T) {
		e := newEnv(t, 4)
		mustAppend(t, e.log, rec)
		before, last := snapshot(e), e.log.Last(1)
		e.lg.setFail(recUndo, boom)
		if _, err := e.log.Append(bg, rec); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		e.lg.setFail(-1, nil)
		samePages(t, snapshot(e), before, "after a failed append")
		if e.log.Last(1) != last || len(e.log.Pages(1)) != 1 {
			t.Fatal("a failed append changed the segment")
		}
		// And the next append works.
		p := mustAppend(t, e.log, rec)
		if got, err := e.log.Read(bg, p); err != nil || got.PrevInTxn != last {
			t.Fatalf("after recovering: %+v %v", got, err)
		}
	})
	t.Run("grow", func(t *testing.T) {
		e := newEnv(t, 4)
		mustAppend(t, e.log, rec)
		mustAppend(t, e.log, rec)
		before, last := snapshot(e), e.log.Last(1)
		e.lg.setFail(recUndo, boom)
		if _, err := e.log.Append(bg, rec); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		e.lg.setFail(-1, nil)
		after := snapshot(e)
		for id, img := range before {
			if !bytes.Equal(after[id], img) {
				t.Fatalf("page %d changed by a failed grow", id)
			}
		}
		if e.log.Last(1) != last || len(e.log.Pages(1)) != 1 {
			t.Fatal("a failed grow changed the segment")
		}
		mustAppend(t, e.log, rec)
		if len(e.log.Pages(1)) != 2 {
			t.Fatal("no growth after recovering")
		}
	})
	t.Run("new segment", func(t *testing.T) {
		for _, typ := range []int{recUndo, recSegment} {
			e := newEnv(t, 4)
			e.lg.setFail(typ, boom)
			if _, err := e.log.Append(bg, rec); !errors.Is(err, boom) {
				t.Fatalf("err = %v", err)
			}
			if len(e.log.Segments()) != 0 || e.log.Last(1) != 0 {
				t.Fatalf("failing record type %d: a segment exists", typ)
			}
		}
	})
	t.Run("release", func(t *testing.T) {
		e := newEnv(t, 4)
		mustAppend(t, e.log, rec)
		e.lg.setFail(recSegment, boom)
		if err := e.log.Release(bg, 1); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if len(e.log.Segments()) != 1 {
			t.Fatal("a failed release dropped the segment")
		}
	})
}

func TestRelease(t *testing.T) {
	e := newEnv(t, 4)
	for range 10 {
		mustAppend(t, e.log, Record{Kind: Delete, XID: 3, RID: storage.RID{Page: 5}, Image: make([]byte, 4000)})
	}
	mustAppend(t, e.log, Record{Kind: Insert, XID: 4, RID: storage.RID{Page: 5}})
	pages := e.log.Pages(3)
	if err := e.log.Release(bg, 3); err != nil {
		t.Fatal(err)
	}
	recs := e.lg.records()
	drop, free := recs[len(recs)-2], recs[len(recs)-1]
	entries, err := DecodeSegmentEntries(drop.payload)
	if err != nil || drop.typ != recSegment || len(entries) != 1 || entries[0] != (SegmentEntry{SegmentDrop, 3, pages[0]}) {
		t.Fatalf("drop record %+v: %v %v", drop, entries, err)
	}
	if free.typ != recFree || !slices.Equal(free.pages, pages) {
		t.Fatalf("deferred free %+v, want pages %v", free, pages)
	}
	if got := e.log.Segments(); !slices.Equal(got, []uint64{4}) || e.log.Last(3) != 0 {
		t.Fatalf("segments %v", got)
	}
	if err := e.log.Release(bg, 3); !errors.Is(err, ErrNoSegment) {
		t.Fatalf("second release: %v", err)
	}
	// A new segment for the same ID starts afresh.
	p := mustAppend(t, e.log, Record{Kind: Insert, XID: 3, RID: storage.RID{Page: 5}})
	if rec, err := e.log.Read(bg, p); err != nil || rec.PrevInTxn != 0 {
		t.Fatalf("new segment's first record: %+v %v", rec, err)
	}
}

func TestRelogAndRedoSegments(t *testing.T) {
	e := newEnv(t, 4)
	for xid := uint64(1); xid <= MaxSegmentEntries+5; xid++ {
		mustAppend(t, e.log, Record{Kind: Insert, XID: xid, RID: storage.RID{Page: 5}})
	}
	n := len(e.lg.records())
	if err := e.log.Relog(bg); err != nil {
		t.Fatal(err)
	}
	recs := e.lg.records()[n:]
	if len(recs) != 2 {
		t.Fatalf("relog wrote %d records", len(recs))
	}
	l := New(e.bp, newFakeLog())
	total := 0
	for _, r := range recs {
		entries, err := DecodeSegmentEntries(r.payload)
		if err != nil {
			t.Fatal(err)
		}
		total += len(entries)
		if err := l.RedoSegments(r.payload); err != nil {
			t.Fatal(err)
		}
	}
	if total != MaxSegmentEntries+5 || !slices.Equal(l.Segments(), e.log.Segments()) {
		t.Fatalf("relogged %d entries", total)
	}
	if err := l.Recover(bg); err != nil {
		t.Fatal(err)
	}
	sameSegments(t, segments(l), segments(e.log), "relogged table")

	first := e.log.Pages(1)[0]
	other := e.log.Pages(2)[0]
	for name, c := range map[string]struct {
		entries []SegmentEntry
		ok      bool
	}{
		"add again, same page":   {[]SegmentEntry{{SegmentAdd, 1, first}}, true},
		"add again, other page":  {[]SegmentEntry{{SegmentAdd, 1, other}}, false},
		"drop of an unknown ID":  {[]SegmentEntry{{SegmentDrop, 9999, first}}, true},
		"drop at the wrong page": {[]SegmentEntry{{SegmentDrop, 1, other}}, false},
		"add, drop, add":         {[]SegmentEntry{{SegmentAdd, 5000, first}, {SegmentDrop, 5000, first}, {SegmentAdd, 5000, other}}, true},
	} {
		l := New(e.bp, newFakeLog())
		if err := l.RedoSegments(EncodeSegmentEntries([]SegmentEntry{{SegmentAdd, 1, first}})); err != nil {
			t.Fatal(err)
		}
		err := l.RedoSegments(EncodeSegmentEntries(c.entries))
		if c.ok != (err == nil) || (err != nil && !errors.Is(err, ErrCorrupt)) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if err := New(e.bp, newFakeLog()).RedoSegments([]byte{1}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("damaged record: %v", err)
	}
}

func TestRecoverRejectsBadSegments(t *testing.T) {
	e := newEnv(t, 6)
	for range 3 {
		mustAppend(t, e.log, Record{Kind: Delete, XID: 1, RID: storage.RID{Page: 5}, Image: make([]byte, 5000)})
	}
	mustAppend(t, e.log, Record{Kind: Insert, XID: 2, RID: storage.RID{Page: 5}})
	ref, err := e.bp.NewPage(bg, storage.PageTypeHeap)
	if err != nil {
		t.Fatal(err)
	}
	heap := ref.ID()
	if err := ref.Unpin(true); err != nil {
		t.Fatal(err)
	}
	p1, p2 := e.log.Pages(1), e.log.Pages(2)
	if len(p1) != 3 {
		t.Fatalf("segment of 1 has %d pages", len(p1))
	}
	setPageNext := func(id, next uint64) {
		ref, err := e.bp.FetchPage(bg, id)
		if err != nil {
			t.Fatal(err)
		}
		ref.Lock()
		setNext(ref.Data(), next)
		ref.Unlock()
		if err := ref.Unpin(true); err != nil {
			t.Fatal(err)
		}
	}
	try := func(name string, entries []SegmentEntry) {
		t.Helper()
		l := New(e.bp, newFakeLog())
		if err := l.RedoSegments(EncodeSegmentEntries(entries)); err != nil {
			t.Fatal(err)
		}
		if err := l.Recover(bg); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	try("first page is a heap page", []SegmentEntry{{SegmentAdd, 1, heap}})
	try("first page past the file", []SegmentEntry{{SegmentAdd, 1, e.dm.PageCount() + 5}})
	try("first page of another transaction", []SegmentEntry{{SegmentAdd, 1, p2[0]}})
	try("two segments share pages", []SegmentEntry{{SegmentAdd, 1, p1[0]}, {SegmentAdd, 3, p1[1]}})

	setPageNext(p1[2], p1[0])
	try("a cycle", []SegmentEntry{{SegmentAdd, 1, p1[0]}})
	setPageNext(p1[2], p2[0])
	try("a link into another transaction's pages", []SegmentEntry{{SegmentAdd, 1, p1[0]}})
	setPageNext(p1[2], heap)
	try("a link to a heap page", []SegmentEntry{{SegmentAdd, 1, p1[0]}})
	setPageNext(p1[2], 0)

	l := New(e.bp, newFakeLog())
	if err := l.RedoSegments(EncodeSegmentEntries([]SegmentEntry{{SegmentAdd, 1, p1[0]}, {SegmentAdd, 2, p2[0]}})); err != nil {
		t.Fatal(err)
	}
	if err := l.Recover(bg); err != nil {
		t.Fatalf("repaired: %v", err)
	}
	sameSegments(t, segments(l), segments(e.log), "recovered")
}

func TestRedoRejectsMismatches(t *testing.T) {
	e := newEnv(t, 4)
	p := mustAppend(t, e.log, Record{Kind: Insert, XID: 1, RID: storage.RID{Page: 5}})
	id := p.Page()
	if err := e.bp.FlushAll(bg); err != nil {
		t.Fatal(err)
	}
	lsn := storage.PageLSN(pageImages(t, e.bp, e.dm)[id]) + 100
	rec1 := EncodeRecord(Record{Kind: Insert, XID: 1, RID: storage.RID{Page: 5}})
	rec2 := EncodeRecord(Record{Kind: Insert, XID: 2, RID: storage.RID{Page: 5}})
	ref, err := e.bp.NewPage(bg, storage.PageTypeHeap)
	if err != nil {
		t.Fatal(err)
	}
	heap := ref.ID()
	if err := ref.Unpin(true); err != nil {
		t.Fatal(err)
	}
	for name, blocks := range map[string][]Block{
		"append at the wrong offset":    {{Page: id, Op: OpAppend, Offset: pageHeaderSize, Data: rec1}},
		"append of another transaction": {{Page: id, Op: OpAppend, Offset: pageHeaderSize + RecordHeaderSize, Data: rec2}},
		"append to a heap page":         {{Page: heap, Op: OpAppend, Offset: pageHeaderSize, Data: rec1}},
		"link on a heap page":           {{Page: heap, Op: OpSetNext, Next: id}},
	} {
		if err := Redo(bg, e.bp, lsn, EncodeBlocks(blocks)); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	// A link on a page that already has one.
	if err := Redo(bg, e.bp, lsn, EncodeBlocks([]Block{{Page: id, Op: OpSetNext, Next: 40}})); err != nil {
		t.Fatal(err)
	}
	if err := Redo(bg, e.bp, lsn+1, EncodeBlocks([]Block{{Page: id, Op: OpSetNext, Next: 41}})); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("second link: %v", err)
	}
	// An operation the page already has is skipped, whatever it says.
	if err := Redo(bg, e.bp, lsn, EncodeBlocks([]Block{{Page: id, Op: OpAppend, Offset: pageHeaderSize, Data: rec2}})); err != nil {
		t.Fatalf("an old append: %v", err)
	}
	// The right append applies once.
	good := EncodeBlocks([]Block{{Page: id, Op: OpAppend, Offset: pageHeaderSize + RecordHeaderSize, Data: rec1}})
	for range 2 {
		if err := Redo(bg, e.bp, lsn+2, good); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.log.Read(bg, MakePtr(id, pageHeaderSize+RecordHeaderSize)); err != nil {
		t.Fatalf("the replayed record: %v", err)
	}
	if _, err := e.log.Read(bg, MakePtr(id, pageHeaderSize+2*RecordHeaderSize)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("applied twice: %v", err)
	}
}
