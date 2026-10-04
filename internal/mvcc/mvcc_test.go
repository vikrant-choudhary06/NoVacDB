package mvcc_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/mvcc"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/undo"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
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

func TestImageGoldenAndRoundTrip(t *testing.T) {
	v := storage.Version{RowHeader: storage.RowHeader{XID: 0x0102030405060708, Undo: 0x11}, Data: []byte("row")}
	b := mvcc.EncodeImage(v)
	if want := "0807060504030201" + "1100000000000000" + "726f77"; hex.EncodeToString(b) != want {
		t.Fatalf("image %x, want %s", b, want)
	}
	got, err := mvcc.DecodeImage(b)
	if err != nil || got.RowHeader != v.RowHeader || !bytes.Equal(got.Data, v.Data) {
		t.Fatalf("decode %+v, %v", got, err)
	}
	empty, err := mvcc.DecodeImage(mvcc.EncodeImage(storage.Version{RowHeader: storage.RowHeader{XID: 1}}))
	if err != nil || len(empty.Data) != 0 {
		t.Fatalf("empty row: %+v, %v", empty, err)
	}
	// The largest version fits an undo record.
	if big := mvcc.EncodeImage(storage.Version{RowHeader: storage.RowHeader{XID: 1}, Data: make([]byte, storage.MaxRowData)}); len(big) > undo.MaxImage {
		t.Fatalf("largest image %d bytes, undo holds %d", len(big), undo.MaxImage)
	}
	for n := range 16 {
		if _, err := mvcc.DecodeImage(b[:n]); !errors.Is(err, mvcc.ErrBadImage) {
			t.Fatalf("truncated to %d: %v", n, err)
		}
	}
	if _, err := mvcc.DecodeImage(make([]byte, 16)); !errors.Is(err, mvcc.ErrBadImage) {
		t.Fatalf("transaction 0: %v", err)
	}
	if _, err := mvcc.DecodeImage(make([]byte, 16+storage.MaxRowData+1)); !errors.Is(err, mvcc.ErrBadImage) {
		t.Fatalf("too long: %v", err)
	}
}

func mustEngine(t testing.TB, m *vfs.MemFS, frames int) *wal.Engine {
	t.Helper()
	e, err := wal.OpenEngine(bg, m, "/db", wal.EngineOptions{Frames: frames})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// version is a row version in the model: its data and the transaction
// that wrote it.
type version struct {
	data []byte
	xid  uint64
}

// rowState is a row of the model: its versions, oldest first; the last
// one has deleted set if the row is deleted.
type rowState struct {
	versions []version
	deleted  bool
}

func rowData(rng *rand.Rand, xid uint64) []byte {
	n := 1 + rng.IntN(200)
	switch rng.IntN(10) {
	case 0:
		n = 1 + rng.IntN(storage.MaxRowData)
	case 1, 2:
		n = 1000 + rng.IntN(3000)
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	copy(b, fmt.Sprintf("x%d.", xid))
	return b
}

// checkChain checks, inside the transaction that wrote them, the undo of
// every row it changed: the row's undo pointer leads, through PrevForRow,
// to an Update or Delete record per change with the previous version as its
// image, and to an Insert record for a row the transaction inserted.
func checkChain(t *testing.T, e *wal.Engine, h *storage.Heap, rid storage.RID, r *rowState, firstInTxn int, xid uint64, table uint64) {
	t.Helper()
	var p undo.Ptr
	if r.deleted {
		// A tombstone: Get refuses it; the heap header is checked through
		// the record chain only.
		if _, err := h.Get(bg, rid); !errors.Is(err, storage.ErrRowDeleted) {
			t.Fatalf("deleted row %s: %v", rid, err)
		}
		p = findLast(t, e, xid, rid)
	} else {
		v, err := h.Get(bg, rid)
		if err != nil {
			t.Fatal(err)
		}
		last := r.versions[len(r.versions)-1]
		if v.XID != xid || !bytes.Equal(v.Data, last.data) {
			t.Fatalf("row %s: written by %d, want %d", rid, v.XID, xid)
		}
		p = undo.Ptr(v.Undo)
	}
	// Changes in this transaction: versions[firstInTxn:] were written by
	// it; each has one record, newest first.
	n := len(r.versions) - firstInTxn
	if r.deleted {
		n++ // the delete itself
	}
	for i := 0; i < n; i++ {
		rec, err := e.Undo().Read(bg, p)
		if err != nil {
			t.Fatalf("row %s change %d: %v", rid, i, err)
		}
		if rec.XID != xid || rec.RID != rid || rec.Table != table {
			t.Fatalf("row %s: record of %d for %s, table %d", rid, rec.XID, rec.RID, rec.Table)
		}
		// The version this record undoes to.
		prev := len(r.versions) - 1 - i
		if r.deleted {
			prev++
		}
		switch {
		case prev == 0 && firstInTxn == 0:
			if rec.Kind != undo.Insert || rec.PrevForRow != 0 || len(rec.Image) != 0 {
				t.Fatalf("row %s: first record %+v, want an insert", rid, rec)
			}
		default:
			want := r.versions[prev-1]
			if r.deleted && i == 0 {
				want = r.versions[len(r.versions)-1]
				if rec.Kind != undo.Delete {
					t.Fatalf("row %s: newest record kind %v, want delete", rid, rec.Kind)
				}
			} else if rec.Kind != undo.Update {
				t.Fatalf("row %s: record kind %v, want update", rid, rec.Kind)
			}
			img, err := mvcc.DecodeImage(rec.Image)
			if err != nil || img.XID != want.xid || !bytes.Equal(img.Data, want.data) || undo.Ptr(img.Undo) != rec.PrevForRow {
				t.Fatalf("row %s change %d: image %+v (%v), want version of %d", rid, i, img.RowHeader, err, want.xid)
			}
		}
		p = rec.PrevForRow
	}
}

// findLast finds the newest undo record of xid for rid by walking the
// transaction's chain.
func findLast(t *testing.T, e *wal.Engine, xid uint64, rid storage.RID) undo.Ptr {
	t.Helper()
	for p := e.Undo().Last(xid); p != 0; {
		rec, err := e.Undo().Read(bg, p)
		if err != nil {
			t.Fatal(err)
		}
		if rec.RID == rid {
			return p
		}
		p = rec.PrevInTxn
	}
	t.Fatalf("no undo record of %d for %s", xid, rid)
	return 0
}

// Random transactions of inserts, updates and deletes, with row sizes that
// force moves, checked against a model: before each commit every changed
// row's undo chain is checked; after it the undo is released; after crashes
// the committed rows are all there with their headers.
func TestVersionedWritesModel(t *testing.T) {
	base := testSeed(t)
	runs := 12
	if testing.Short() {
		runs = 3
	}
	var moved, crashes int
	for run := range runs {
		seed := base + uint64(run)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 63))
			m := vfs.NewMemFS(seed)
			_ = m.MkdirAll("/db")
			_ = m.SyncDir("/")
			// No steal (13-transactions.md 2.6): a transaction's pages stay in
			// the pool until it commits.
			frames := 64 + rng.IntN(64)
			e := mustEngine(t, m, frames)
			var first uint64
			tx := e.Begin()
			if err := tx.Write(bg, func(context.Context) error {
				h, err := e.CreateHeap(bg)
				first = h.FirstPage()
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(bg); err != nil {
				t.Fatal(err)
			}
			const table = 7
			committed := map[storage.RID]*rowState{}
			for cycle := range 4 {
				h, err := e.OpenHeap(bg, first)
				if err != nil {
					t.Fatal(err)
				}
				for range 2 + rng.IntN(6) {
					rows := cloneRows(committed)
					tx := e.Begin()
					firsts := map[storage.RID]int{} // first version written by this transaction
					var xid uint64
					for range 1 + rng.IntN(30) {
						err := tx.Write(bg, func(ctx context.Context) error {
							xid = uint64(tx.XID())
							w := mvcc.Writer{Undo: e.Undo(), XID: xid, Table: table}
							live := liveRIDs(rows)
							switch op := rng.IntN(10); {
							case op < 4 || len(live) == 0:
								d := rowData(rng, xid)
								rid, err := w.Insert(ctx, h, d)
								if err != nil {
									return err
								}
								if _, dup := rows[rid]; dup {
									t.Fatalf("insert reused %s", rid)
								}
								rows[rid] = &rowState{versions: []version{{d, xid}}}
								firsts[rid] = 0
							case op < 8:
								rid := live[rng.IntN(len(live))]
								d := rowData(rng, xid)
								if err := w.Update(ctx, h, rid, d); err != nil {
									return err
								}
								r := rows[rid]
								if _, ok := firsts[rid]; !ok {
									firsts[rid] = len(r.versions)
								}
								r.versions = append(r.versions, version{d, xid})
							default:
								rid := live[rng.IntN(len(live))]
								if err := w.Delete(ctx, h, rid); err != nil {
									return err
								}
								r := rows[rid]
								if _, ok := firsts[rid]; !ok {
									firsts[rid] = len(r.versions)
								}
								r.deleted = true
							}
							return nil
						})
						if err != nil {
							t.Fatal(err)
						}
					}
					for rid, f := range firsts {
						checkChain(t, e, h, rid, rows[rid], f, xid, table)
					}
					if _, err := tx.Commit(bg); err != nil {
						t.Fatal(err)
					}
					if segs := e.Undo().Segments(); len(segs) != 0 {
						t.Fatalf("undo of %v kept after commit", segs)
					}
					committed = rows
					checkRows(t, h, committed)
				}
				moved = max(moved, countMoved(t, committed))
				// A transaction in flight when the power goes.
				tx := e.Begin()
				_ = tx.Write(bg, func(ctx context.Context) error {
					w := mvcc.Writer{Undo: e.Undo(), XID: uint64(tx.XID()), Table: table}
					for range 1 + rng.IntN(10) {
						if live := liveRIDs(committed); len(live) > 0 && rng.IntN(2) == 0 {
							if err := w.Update(ctx, h, live[rng.IntN(len(live))], rowData(rng, 999)); err != nil {
								return err
							}
						} else if _, err := w.Insert(ctx, h, rowData(rng, 999)); err != nil {
							return err
						}
					}
					return nil
				})
				m.Crash(vfs.CrashOptions{TearLast: rng.IntN(2) == 0})
				crashes++
				e = mustEngine(t, m, frames)
				if segs := e.Undo().Segments(); len(segs) != 0 {
					t.Fatalf("cycle %d: undo segments %v after a crash", cycle, segs)
				}
				h, err = e.OpenHeap(bg, first)
				if err != nil {
					t.Fatal(err)
				}
				checkRows(t, h, committed)
			}
			if err := e.Close(bg); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Logf("%d runs, %d crashes; up to %d moved rows at once", runs, crashes, moved)
	if moved == 0 {
		t.Fatal("no row ever moved")
	}
}

func cloneRows(rows map[storage.RID]*rowState) map[storage.RID]*rowState {
	out := map[storage.RID]*rowState{}
	for rid, r := range rows {
		out[rid] = &rowState{versions: append([]version(nil), r.versions...), deleted: r.deleted}
	}
	return out
}

func liveRIDs(rows map[storage.RID]*rowState) []storage.RID {
	var out []storage.RID
	for rid, r := range rows {
		if !r.deleted {
			out = append(out, rid)
		}
	}
	// Map order is random; sort for a deterministic choice.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && (out[j].Page < out[j-1].Page || (out[j].Page == out[j-1].Page && out[j].Slot < out[j-1].Slot)); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// checkRows checks that a scan returns exactly the live rows of the model,
// each with the header of the transaction that last wrote it, and that
// deleted rows are tombstones.
func checkRows(t *testing.T, h *storage.Heap, rows map[storage.RID]*rowState) {
	t.Helper()
	seen := map[storage.RID]bool{}
	s := h.Scan()
	for {
		rid, v, ok, err := s.Next(bg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		r, known := rows[rid]
		if !known || r.deleted || seen[rid] {
			t.Fatalf("scan returned %s (known %v)", rid, known)
		}
		seen[rid] = true
		last := r.versions[len(r.versions)-1]
		if v.XID != last.xid || !bytes.Equal(v.Data, last.data) {
			t.Fatalf("row %s: version of %d, want %d", rid, v.XID, last.xid)
		}
	}
	for rid, r := range rows {
		if !r.deleted && !seen[rid] {
			t.Fatalf("row %s missing", rid)
		}
		if r.deleted {
			if _, err := h.Get(bg, rid); !errors.Is(err, storage.ErrRowDeleted) {
				t.Fatalf("deleted row %s: %v", rid, err)
			}
		}
	}
}

// countMoved counts the pages whose live rows, by the model, could not all
// fit in one page: some of them must have moved away.
func countMoved(t *testing.T, rows map[storage.RID]*rowState) int {
	t.Helper()
	n := 0
	perPage := map[uint64]int{}
	for rid, r := range rows {
		if !r.deleted {
			perPage[rid.Page] += storage.RowHeaderSize + len(r.versions[len(r.versions)-1].data)
		}
	}
	for _, used := range perPage {
		if used > storage.MaxTupleSize {
			n++ // more than a page's worth of rows call it home: some moved
		}
	}
	return n
}
