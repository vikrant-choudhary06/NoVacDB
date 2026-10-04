package mvcc_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/mvcc"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

// tdb is an engine with one heap, for reading tests.
type tdb struct {
	t *testing.T
	m *vfs.MemFS
	e *wal.Engine
	h *storage.Heap
}

func newTDB(t *testing.T) *tdb {
	t.Helper()
	m := vfs.NewMemFS(testSeed(t))
	_ = m.MkdirAll("/db")
	_ = m.SyncDir("/")
	d := &tdb{t: t, m: m, e: mustEngine(t, m, 256)}
	tx := d.e.Begin()
	if err := tx.Write(bg, func(context.Context) error {
		var err error
		d.h, err = d.e.CreateHeap(bg)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	return d
}

// txn runs fn as one transaction's write and commits it, returning the XID.
func (d *tdb) txn(fn func(w mvcc.Writer) error) uint64 {
	d.t.Helper()
	tx := d.e.Begin()
	if err := tx.Write(bg, func(context.Context) error {
		return fn(mvcc.Writer{Undo: d.e.Undo(), XID: uint64(tx.XID()), Table: 1})
	}); err != nil {
		d.t.Fatal(err)
	}
	if _, err := tx.Commit(bg); err != nil {
		d.t.Fatal(err)
	}
	return uint64(tx.XID())
}

// read returns what a reader sees of the row at rid.
func (d *tdb) read(r mvcc.Reader, rid storage.RID) (string, bool) {
	d.t.Helper()
	v, err := d.h.GetVersion(bg, rid)
	if err != nil {
		d.t.Fatal(err)
	}
	data, ok, err := r.Visible(bg, rid, v)
	if err != nil {
		d.t.Fatal(err)
	}
	return string(data), ok
}

func (d *tdb) reader(s *wal.Snapshot, me uint64) mvcc.Reader {
	return mvcc.Reader{Undo: d.e.Undo(), Snap: s, Me: me}
}

func TestVisibilityRule(t *testing.T) {
	d := newTDB(t)
	var rid storage.RID
	s0 := d.e.Snapshot() // before the row exists
	d.txn(func(w mvcc.Writer) (err error) { rid, err = w.Insert(bg, d.h, []byte("v1")); return })
	s1 := d.e.Snapshot()
	d.txn(func(w mvcc.Writer) error { return w.Update(bg, d.h, rid, []byte("v2")) })
	s2 := d.e.Snapshot()
	// Several versions in one transaction, and a move (a long version).
	big := string(bytes.Repeat([]byte("B"), 7000))
	d.txn(func(w mvcc.Writer) error {
		if err := w.Update(bg, d.h, rid, []byte("v3a")); err != nil {
			return err
		}
		return w.Update(bg, d.h, rid, []byte(big))
	})
	s3 := d.e.Snapshot()
	d.txn(func(w mvcc.Writer) error { return w.Delete(bg, d.h, rid) })
	s4 := d.e.Snapshot()

	for i, c := range []struct {
		s    *wal.Snapshot
		want string
		ok   bool
	}{{s0, "", false}, {s1, "v1", true}, {s2, "v2", true}, {s3, big, true}, {s4, "", false}} {
		if got, ok := d.read(d.reader(c.s, 0), rid); ok != c.ok || got != c.want {
			t.Errorf("snapshot %d sees %.10q (%v), want %.10q (%v)", i, got, ok, c.want, c.ok)
		}
	}

	// A transaction in progress: others do not see it, it sees itself.
	tx := d.e.Begin()
	var rid2 storage.RID
	if err := tx.Write(bg, func(context.Context) (err error) {
		w := mvcc.Writer{Undo: d.e.Undo(), XID: uint64(tx.XID()), Table: 1}
		rid2, err = w.Insert(bg, d.h, []byte("mine"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	during := d.e.Snapshot()
	if slices.Index(during.Active, tx.XID()) < 0 || during.Sees(uint64(tx.XID())) {
		t.Fatalf("snapshot during a write: active %v", during.Active)
	}
	if _, ok := d.read(d.reader(during, 0), rid2); ok {
		t.Fatal("an uncommitted row is seen")
	}
	if got, ok := d.read(d.reader(during, uint64(tx.XID())), rid2); !ok || got != "mine" {
		t.Fatal("a transaction does not see its own row")
	}
	if _, err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	if _, ok := d.read(d.reader(during, 0), rid2); ok {
		t.Fatal("a snapshot taken before a commit sees it afterwards")
	}
	if got, ok := d.read(d.reader(d.e.Snapshot(), 0), rid2); !ok || got != "mine" {
		t.Fatal("a later snapshot does not see a committed row")
	}
	if s0.Sees(0) || s4.Sees(uint64(s4.XMax)) {
		t.Fatal("XID 0 or XMax seen")
	}
}

// The writer of a version must have written the undo record its pointer
// names, for that row: anything else is corruption, never a wrong row.
func TestVisibleRefusesMismatchedChains(t *testing.T) {
	d := newTDB(t)
	var a, b storage.RID
	d.txn(func(w mvcc.Writer) (err error) {
		if a, err = w.Insert(bg, d.h, []byte("a")); err != nil {
			return err
		}
		b, err = w.Insert(bg, d.h, []byte("b"))
		return err
	})
	s := d.e.Snapshot() // keeps the next transaction's undo
	defer s.Release()
	d.txn(func(w mvcc.Writer) error {
		if err := w.Update(bg, d.h, a, []byte("a2")); err != nil {
			return err
		}
		return w.Update(bg, d.h, b, []byte("b2"))
	})
	va, _ := d.h.GetVersion(bg, a)
	vb, _ := d.h.GetVersion(bg, b)
	r := d.reader(s, 0)
	for name, c := range map[string]struct {
		rid storage.RID
		v   storage.Version
	}{
		"another row's record":         {a, vb},
		"another transaction's record": {a, storage.Version{RowHeader: storage.RowHeader{XID: va.XID + 1000, Undo: va.Undo}}},
	} {
		if _, _, err := r.Visible(bg, c.rid, c.v); !errors.Is(err, mvcc.ErrCorrupt) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if got, ok := d.read(r, a); !ok || got != "a" {
		t.Fatalf("the real chain: %q %v", got, ok)
	}
}

// Snapshots taken at random points of a random workload each see exactly
// the committed state of their moment, by heap scan, however many
// transactions commit after them; undo they need is kept, and released
// once they end.
func TestSnapshotsModel(t *testing.T) {
	base := testSeed(t)
	runs := 8
	if testing.Short() {
		runs = 2
	}
	for run := range runs {
		seed := base + uint64(run)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 64))
			d := newTDB(t)
			state := map[storage.RID]string{} // committed
			type held struct {
				s    *wal.Snapshot
				want map[storage.RID]string
			}
			var snaps []held
			check := func(h held, what string) {
				t.Helper()
				got := map[storage.RID]string{}
				sc := d.h.ScanVersions()
				r := d.reader(h.s, 0)
				for {
					rid, v, ok, err := sc.Next(bg)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					data, vis, err := r.Visible(bg, rid, v)
					if err != nil {
						t.Fatal(err)
					}
					if vis {
						got[rid] = string(data)
					}
				}
				if len(got) != len(h.want) {
					t.Fatalf("%s: %d rows, want %d", what, len(got), len(h.want))
				}
				for rid, w := range h.want {
					if got[rid] != w {
						t.Fatalf("%s: row %s is %.12q, want %.12q", what, rid, got[rid], w)
					}
				}
			}
			for i := range 120 {
				if rng.IntN(3) == 0 {
					snaps = append(snaps, held{d.e.Snapshot(), maps(state)})
				}
				if len(snaps) > 0 && rng.IntN(4) == 0 {
					j := rng.IntN(len(snaps))
					check(snaps[j], fmt.Sprintf("step %d, snapshot %d", i, j))
					snaps[j].s.Release()
					snaps = append(snaps[:j], snaps[j+1:]...)
				}
				next := maps(state)
				d.txn(func(w mvcc.Writer) error {
					for range 1 + rng.IntN(6) {
						live := keys(next)
						switch op := rng.IntN(10); {
						case op < 4 || len(live) == 0:
							v := value(rng, i)
							rid, err := w.Insert(bg, d.h, []byte(v))
							if err != nil {
								return err
							}
							next[rid] = v
						case op < 8:
							rid := live[rng.IntN(len(live))]
							v := value(rng, i)
							if err := w.Update(bg, d.h, rid, []byte(v)); err != nil {
								return err
							}
							next[rid] = v
						default:
							rid := live[rng.IntN(len(live))]
							if err := w.Delete(bg, d.h, rid); err != nil {
								return err
							}
							delete(next, rid)
						}
					}
					return nil
				})
				state = next
			}
			for j, h := range snaps {
				check(h, fmt.Sprintf("end, snapshot %d", j))
				h.s.Release()
			}
			check(held{d.e.Snapshot(), state}, "a new snapshot")
			// Nothing holds undo back any more: a checkpoint releases it all.
			if _, err := d.e.Checkpoint(bg); err != nil {
				t.Fatal(err)
			}
			if segs := d.e.Undo().Segments(); len(segs) != 0 {
				t.Fatalf("undo kept with no snapshot: %v", segs)
			}
		})
	}
}

func maps(m map[storage.RID]string) map[storage.RID]string {
	out := make(map[storage.RID]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func keys(m map[storage.RID]string) []storage.RID {
	out := make([]storage.RID, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.SortFunc(out, func(a, b storage.RID) int {
		if a.Page != b.Page {
			return int(a.Page) - int(b.Page)
		}
		return int(a.Slot) - int(b.Slot)
	})
	return out
}

func value(rng *rand.Rand, step int) string {
	n := 5 + rng.IntN(60)
	if rng.IntN(8) == 0 {
		n = 2000 + rng.IntN(4000) // moves rows
	}
	return fmt.Sprintf("%d:%s", step, bytes.Repeat([]byte{byte('a' + rng.IntN(26))}, n))
}

// Undo retention: a commit keeps undo that a live snapshot misses, and the
// first commit or checkpoint after the snapshot ends releases it; recovery
// releases what is left.
func TestUndoKeptWhileASnapshotNeedsIt(t *testing.T) {
	d := newTDB(t)
	var rid storage.RID
	d.txn(func(w mvcc.Writer) (err error) { rid, err = w.Insert(bg, d.h, []byte("v1")); return })
	if segs := d.e.Undo().Segments(); len(segs) != 0 {
		t.Fatalf("undo kept with no snapshot: %v", segs)
	}
	old := d.e.Snapshot()
	x2 := d.txn(func(w mvcc.Writer) error { return w.Update(bg, d.h, rid, []byte("v2")) })
	x3 := d.txn(func(w mvcc.Writer) error { return w.Update(bg, d.h, rid, []byte("v3")) })
	if segs := d.e.Undo().Segments(); !slices.Equal(segs, []uint64{x2, x3}) {
		t.Fatalf("segments %v, want %d and %d", segs, x2, x3)
	}
	if got, _ := d.read(d.reader(old, 0), rid); got != "v1" {
		t.Fatalf("the old snapshot sees %q", got)
	}
	// A snapshot taken after them needs none of it, but the old one still
	// holds it.
	newer := d.e.Snapshot()
	newer.Release()
	if _, err := d.e.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	if n := len(d.e.Undo().Segments()); n != 2 {
		t.Fatalf("checkpoint released undo a snapshot needs: %d left", n)
	}
	if got, _ := d.read(d.reader(old, 0), rid); got != "v1" {
		t.Fatalf("after a checkpoint, the old snapshot sees %q", got)
	}
	old.Release()
	if d.e.LiveSnapshots() != 0 {
		t.Fatal("snapshots still registered")
	}
	d.txn(func(w mvcc.Writer) error { return w.Update(bg, d.h, rid, []byte("v4")) })
	if segs := d.e.Undo().Segments(); len(segs) != 0 {
		t.Fatalf("the next commit kept %v", segs)
	}
	// After a crash nothing is kept, whatever snapshots there were.
	held := d.e.Snapshot()
	d.txn(func(w mvcc.Writer) error { return w.Update(bg, d.h, rid, []byte("v5")) })
	_ = held
	if len(d.e.Undo().Segments()) != 1 {
		t.Fatal("setup: no undo held")
	}
	if err := d.e.Flush(bg); err != nil {
		t.Fatal(err)
	}
	d.m.Crash(vfs.CrashOptions{})
	e := mustEngine(t, d.m, 256)
	defer func() { _ = e.Close(bg) }()
	if segs := e.Undo().Segments(); len(segs) != 0 {
		t.Fatalf("undo left after recovery: %v", segs)
	}
}
