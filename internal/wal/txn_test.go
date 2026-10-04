package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"path"
	"testing"
	"time"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

// stmtRow is a heap row and B+Tree key tagged with its transaction's
// number in the test.
func stmtRow(stmt, i int) []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(stmt))
	b = binary.BigEndian.AppendUint32(b, uint32(i))
	return append(b, bytes.Repeat([]byte{byte(stmt)}, 300)...)
}

// countByStatement reads a heap and a tree and counts rows and keys per
// statement number.
func countByStatement(t *testing.T, h *storage.Heap, tr *btree.Tree) (map[int]int, map[int]int) {
	t.Helper()
	rows, keys := map[int]int{}, map[int]int{}
	s := h.Scan()
	for {
		_, d, ok, err := heapNext(s)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		rows[int(binary.BigEndian.Uint32(d))]++
	}
	it := tr.Scan(btree.Bound{}, btree.Bound{})
	for {
		k, _, ok, err := it.Next(bg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		keys[int(binary.BigEndian.Uint32(k))]++
	}
	if _, err := tr.Check(bg); err != nil {
		t.Fatal(err)
	}
	return rows, keys
}

// insertRows inserts n tagged rows into the heap and the tree.
func insertRows(h *storage.Heap, tr *btree.Tree, stmt, from, n int) error {
	for i := from; i < from+n; i++ {
		r := stmtRow(stmt, i)
		if _, err := heapInsert(h, r); err != nil {
			return err
		}
		if err := tr.Insert(bg, r[:8+rand.IntN(200)], nil); err != nil {
			return err
		}
	}
	return nil
}

// runStatement inserts n tagged rows into the heap and the tree in one
// transaction, in up to three writes, and returns the commit error (or the
// first failure).
func runStatement(e *Engine, h *storage.Heap, tr *btree.Tree, stmt, n int) error {
	_, err := runTxn(e, h, tr, stmt, n)
	return err
}

// runTxn is runStatement, also returning the transaction's ID.
func runTxn(e *Engine, h *storage.Heap, tr *btree.Tree, stmt, n int) (XID, error) {
	tx := e.Begin()
	for done := 0; done < n; {
		k := min(n-done, 1+rand.IntN(max(1, n/2)))
		if err := tx.Write(bg, func(context.Context) error { return insertRows(h, tr, stmt, done, k) }); err != nil {
			return tx.XID(), err
		}
		done += k
	}
	_, err := tx.Commit(bg)
	return tx.XID(), err
}

// openTxn begins a transaction and runs fn as its first write, leaving it
// open.
func openTxn(t *testing.T, e *Engine, fn func() error) *Txn {
	t.Helper()
	tx := e.Begin()
	if err := tx.Write(bg, func(context.Context) error { return fn() }); err != nil {
		t.Fatal(err)
	}
	return tx
}

func setupHeapAndTree(t *testing.T, e *Engine) (*storage.Heap, *btree.Tree) {
	t.Helper()
	var h *storage.Heap
	var tr *btree.Tree
	tx := openTxn(t, e, func() error {
		var err error
		if h, err = e.CreateHeap(bg); err != nil {
			return err
		}
		tr, err = e.CreateBTree(bg)
		return err
	})
	if _, err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	return h, tr
}

func TestCommittedTxnSurvivesAndUncommittedIsDiscarded(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 64})
	h, tr := setupHeapAndTree(t, e)
	x1, err := runTxn(e, h, tr, 1, 30)
	if err != nil {
		t.Fatal(err)
	}
	// Transaction 2 is left open, in several writes, and its records are
	// made durable.
	tx := e.Begin()
	for part := range 3 {
		if err := tx.Write(bg, func(context.Context) error {
			for i := part * 10; i < part*10+10; i++ {
				if _, err := heapInsert(h, stmtRow(2, i)); err != nil {
					return err
				}
				if err := tr.Insert(bg, stmtRow(2, i), nil); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	x2 := tx.XID()
	if e.Status(x1) != TxnCommitted || e.Status(x2) != TxnActive || x2 <= x1 {
		t.Fatalf("before the crash: %d %v, %d %v", x1, e.Status(x1), x2, e.Status(x2))
	}
	if err := e.w.Flush(bg); err != nil {
		t.Fatal(err)
	}
	m.Crash(vfs.CrashOptions{TearLast: true})

	e2 := mustEngine(t, m, EngineOptions{Frames: 64})
	defer func() { _ = e2.Close(bg) }()
	rec := e2.Recovery()
	if rec.DiscardedTransactions != 1 || rec.Discarded < 60 {
		t.Fatalf("recovery %+v: want the open transaction's records discarded", rec)
	}
	// The table survived recovery: committed, aborted, and IDs move on.
	if e2.Status(x1) != TxnCommitted || e2.Status(x2) != TxnAborted || e2.NextXID() != x2+1 {
		t.Fatalf("after recovery: %v %v, next %d", e2.Status(x1), e2.Status(x2), e2.NextXID())
	}
	h2, err := e2.OpenHeap(bg, h.FirstPage())
	if err != nil {
		t.Fatal(err)
	}
	tr2, err := e2.OpenBTree(bg, tr.Root())
	if err != nil {
		t.Fatal(err)
	}
	rows, keys := countByStatement(t, h2, tr2)
	if rows[1] != 30 || keys[1] != 30 || rows[2] != 0 || keys[2] != 0 {
		t.Fatalf("rows %v keys %v", rows, keys)
	}
}

// diskLSNs returns the highest page LSN in the data file as stored.
func maxDiskLSN(t *testing.T, m *vfs.MemFS) uint64 {
	t.Helper()
	raw := readFile(t, m, path.Join(dbDir, DataFileName))
	var hi uint64
	for off := storage.FirstDataPage * storage.PageSize; off+storage.PageSize <= len(raw); off += storage.PageSize {
		hi = max(hi, storage.PageLSN(raw[off:off+storage.PageSize]))
	}
	return hi
}

func TestTxnPagesNeverReachDiskBeforeCommit(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 12})
	h, tr := setupHeapAndTree(t, e)
	for s := 1; s <= 3; s++ {
		if err := runStatement(e, h, tr, s, 8); err != nil {
			t.Fatal(err)
		}
	}
	// Committed work in another heap, which the transaction below does not
	// touch, leaves dirty pages that eviction may and must write.
	tx := openTxn(t, e, func() error {
		other, err := e.CreateHeap(bg)
		if err != nil {
			return err
		}
		for i := range 150 {
			if _, err := heapInsert(other, stmtRow(100, i)); err != nil {
				return err
			}
		}
		return nil
	})
	if _, err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	before := e.Pool().Stats().Writes
	tx = e.Begin()
	n := 0
	// The transaction writes a row per Write until the pool is full.
	var err error
	for ; err == nil && n < 1000; n++ {
		err = tx.Write(bg, func(context.Context) error {
			r := stmtRow(9, n)
			if _, err := heapInsert(h, r); err != nil {
				return err
			}
			return tr.Insert(bg, r, nil)
		})
		if hi := maxDiskLSN(t, m); hi > e.horizon.Load() {
			t.Fatalf("after %d inserts a page with lsn %d reached disk; the transaction began at %d", n, hi, e.horizon.Load())
		}
	}
	if !errors.Is(err, storage.ErrNoFreeFrames) {
		t.Fatalf("a transaction larger than the pool: %v after %d rows", err, n)
	}
	if e.Pool().Stats().Writes == before {
		t.Fatalf("no page was evicted during the transaction (%d rows, %+v): the test proves nothing", n, e.Pool().Stats())
	}
	// A change failed: the transaction can only roll back. Abandoning the
	// engine and reopening it discards it.
	if err := tx.Write(bg, func(context.Context) error { return nil }); !errors.Is(err, ErrTxnAborting) {
		t.Fatalf("write after a failure: %v", err)
	}
	if _, err := tx.Commit(bg); !errors.Is(err, ErrTxnAborting) {
		t.Fatalf("commit after a failure: %v", err)
	}
	if err := tx.Abandon(); err != nil {
		t.Fatal(err)
	}
	e2 := mustEngine(t, m, EngineOptions{Frames: 12})
	defer func() { _ = e2.Close(bg) }()
	h2, _ := e2.OpenHeap(bg, h.FirstPage())
	tr2, _ := e2.OpenBTree(bg, tr.Root())
	rows, keys := countByStatement(t, h2, tr2)
	if rows[9] != 0 || keys[9] != 0 || rows[3] != 8 || keys[3] != 8 {
		t.Fatalf("rows %v keys %v", rows, keys)
	}
}

func TestCheckpointWaitsForWritingTxn(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 32})
	defer func() { _ = e.Close(bg) }()
	h, tr := setupHeapAndTree(t, e)
	tx := openTxn(t, e, func() error {
		_, err := heapInsert(h, stmtRow(1, 0))
		return err
	})
	done := make(chan error, 1)
	go func() {
		_, err := e.Checkpoint(bg)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("checkpoint ran during a writing transaction: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := tx.Write(bg, func(context.Context) error { return tr.Insert(bg, stmtRow(1, 0), nil) }); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("checkpoint after the transaction: %v", err)
	}
	// A transaction that only reads never takes the slot: a checkpoint
	// runs while it is open.
	ro := e.Begin()
	if _, err := e.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	if _, err := ro.Commit(bg); err != nil || ro.XID() != 0 {
		t.Fatal(ro.XID(), err)
	}
}

func TestTxnMisuse(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 8})
	// A transaction that never wrote commits or rolls back without an ID.
	ro := e.Begin()
	if lsn, err := ro.Commit(bg); err != nil || lsn != 0 || ro.XID() != 0 {
		t.Fatalf("read-only commit: %d %v", lsn, err)
	}
	if _, err := ro.Commit(bg); !errors.Is(err, ErrTxnDone) {
		t.Fatalf("commit twice: %v", err)
	}
	if err := ro.Rollback(bg, nil); !errors.Is(err, ErrTxnDone) {
		t.Fatalf("rollback after commit: %v", err)
	}
	if err := ro.Write(bg, func(context.Context) error { return nil }); !errors.Is(err, ErrTxnDone) {
		t.Fatalf("write after commit: %v", err)
	}
	rb := e.Begin()
	if err := rb.Rollback(bg, nil); err != nil {
		t.Fatal(err)
	}
	if err := rb.Rollback(bg, nil); !errors.Is(err, ErrTxnDone) {
		t.Fatalf("rollback twice: %v", err)
	}
	if e.NextXID() != 1 {
		t.Fatalf("read-only transactions used IDs: next %d", e.NextXID())
	}
	// A function that fails having changed nothing leaves the transaction
	// usable; any other failure leaves it failed.
	tx := e.Begin()
	sentinel := errors.New("refused")
	var u *unchangedError
	if err := tx.Write(bg, func(context.Context) error { return Unchanged(sentinel) }); !errors.Is(err, sentinel) || errors.As(err, &u) {
		t.Fatalf("unchanged failure: %v (the error itself, unwrapped, is returned)", err)
	}
	if err := tx.Write(bg, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := tx.Write(bg, func(context.Context) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("failure: %v", err)
	}
	if err := tx.Write(bg, func(context.Context) error { return nil }); !errors.Is(err, ErrTxnAborting) {
		t.Fatalf("write after a failure: %v", err)
	}
	// Closing with a writing transaction open is refused, and closes the
	// engine as a crash would.
	if err := e.Close(bg); !errors.Is(err, ErrTxnOpen) {
		t.Fatalf("close with a transaction writing: %v", err)
	}
	if err := e.Begin().Write(bg, func(context.Context) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
	if err := e.Abandon(); !errors.Is(err, ErrClosed) {
		t.Fatalf("abandon after close: %v", err)
	}
	// An abandoned engine is closed too.
	ea := mustEngine(t, newFS(t), EngineOptions{Frames: 8})
	if err := ea.Abandon(); err != nil {
		t.Fatal(err)
	}
	if err := ea.Abandon(); !errors.Is(err, ErrClosed) {
		t.Fatalf("abandon twice: %v", err)
	}
	if err := ea.Close(bg); !errors.Is(err, ErrClosed) {
		t.Fatalf("close after abandon: %v", err)
	}
	if err := ea.Begin().Write(bg, func(context.Context) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after abandon: %v", err)
	}
	// The open transaction was never committed: reopening discards it.
	e2 := mustEngine(t, m, EngineOptions{Frames: 8})
	if e2.Recovery().DiscardedTransactions != 1 || e2.Status(tx.XID()) != TxnAborted {
		t.Fatalf("%+v %v", e2.Recovery(), e2.Status(tx.XID()))
	}
	if err := e2.Close(bg); err != nil {
		t.Fatal(err)
	}
}

func TestAbandonedTxnStaysDiscarded(t *testing.T) {
	// An abandoned transaction, then a reopen whose end-of-recovery
	// checkpoint appends its record but fails to write the control file:
	// the next recovery starts before the transaction again and must still
	// discard it, although a checkpoint record, not a begin, follows it.
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 32})
	h, tr := setupHeapAndTree(t, e)
	if err := runStatement(e, h, tr, 1, 5); err != nil {
		t.Fatal(err)
	}
	tx := openTxn(t, e, func() error {
		for i := range 5 {
			if _, err := heapInsert(h, stmtRow(2, i)); err != nil {
				return err
			}
		}
		return nil
	})
	if err := e.w.Flush(bg); err != nil {
		t.Fatal(err)
	}
	// Abandoning a transaction that wrote abandons the engine.
	if err := tx.Abandon(); err != nil {
		t.Fatal(err)
	}
	if err := e.Abandon(); !errors.Is(err, ErrClosed) {
		t.Fatalf("the engine is still open after a rollback: %v", err)
	}
	m.InjectError(vfs.Fault{Op: vfs.OpRename, Name: path.Join(dbDir, ControlFileName+".tmp")})
	if _, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 32}); err == nil {
		t.Fatal("open succeeded although the control file could not be written")
	}
	m.ClearFaults()
	e2 := mustEngine(t, m, EngineOptions{Frames: 32})
	defer func() { _ = e2.Close(bg) }()
	h2, _ := e2.OpenHeap(bg, h.FirstPage())
	tr2, _ := e2.OpenBTree(bg, tr.Root())
	rows, keys := countByStatement(t, h2, tr2)
	if rows[1] != 5 || keys[1] != 5 || rows[2] != 0 {
		t.Fatalf("rows %v keys %v (recovery %+v)", rows, keys, e2.Recovery())
	}
	if e2.Status(tx.XID()) != TxnAborted || e2.NextXID() <= tx.XID() {
		t.Fatalf("status %v, next %d", e2.Status(tx.XID()), e2.NextXID())
	}
}

func TestMismatchedCommitIsCorrupt(t *testing.T) {
	cases := map[string][][2]any{
		"commit without begin":          {{RecordTxnCommit, uint64(1)}},
		"commit of ID 0":                {{RecordTxnBegin, uint64(1)}, {RecordTxnCommit, uint64(0)}},
		"commit of another ID":          {{RecordTxnBegin, uint64(1)}, {RecordTxnCommit, uint64(2)}},
		"commit payload short":          {{RecordTxnBegin, uint64(1)}, {RecordTxnCommit, []byte{1}}},
		"commit twice":                  {{RecordTxnBegin, uint64(1)}, {RecordTxnCommit, uint64(1)}, {RecordTxnCommit, uint64(1)}},
		"begin of ID 0":                 {{RecordTxnBegin, uint64(0)}},
		"begin payload short":           {{RecordTxnBegin, []byte{1, 2}}},
		"begin of a used ID":            {{RecordTxnBegin, uint64(5)}, {RecordTxnCommit, uint64(5)}, {RecordTxnBegin, uint64(5)}},
		"IDs out of order":              {{RecordTxnBegin, uint64(5)}, {RecordTxnBegin, uint64(4)}},
		"checkpoint payload short":      {{RecordCheckpoint, uint64(32)}},
		"statement begin (version 1)":   {{RecordType(4), nil}},
		"statement commit (version 1)":  {{RecordType(5), uint64(32)}},
		"abort without begin":           {{RecordTxnAbort, uint64(1)}},
		"abort of another ID":           {{RecordTxnBegin, uint64(1)}, {RecordTxnAbort, uint64(2)}},
		"abort payload short":           {{RecordTxnBegin, uint64(1)}, {RecordTxnAbort, []byte{1}}},
		"abort after commit":            {{RecordTxnBegin, uint64(1)}, {RecordTxnCommit, uint64(1)}, {RecordTxnAbort, uint64(1)}},
		"commit after abort":            {{RecordTxnBegin, uint64(1)}, {RecordTxnAbort, uint64(1)}, {RecordTxnCommit, uint64(1)}},
		"type 12, after the undo types": {{RecordTxnBegin, uint64(1)}, {RecordType(12), nil}},
	}
	for name, recs := range cases {
		t.Run(name, func(t *testing.T) {
			m := newFS(t)
			e := mustEngine(t, m, EngineOptions{Frames: 8})
			for _, r := range recs {
				var p []byte
				switch v := r[1].(type) {
				case uint64:
					p = binary.LittleEndian.AppendUint64(nil, v)
				case []byte:
					p = v
				}
				if _, err := e.w.Append(bg, r[0].(RecordType), p); err != nil {
					t.Fatal(err)
				}
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

func TestRecoveryFindsUncommittedTxns(t *testing.T) {
	// Logs of transaction records alone (B: begin with the next ID, C:
	// commit of the latest begin, K: checkpoint record), appended after a
	// fresh engine's own records. Discarded counts every record of the
	// dropped transactions, their begin records included, up to the record
	// that ends each.
	cases := []struct {
		log               string
		groups, discarded int
		corrupt           bool // a commit with no open group
	}{
		{"", 0, 0, false},
		{"BC", 0, 0, false},
		{"BCBC", 0, 0, false},
		{"B", 1, 1, false},
		{"BCB", 1, 1, false},
		{"BB", 2, 2, false},
		{"BBC", 1, 1, false},
		{"BKBC", 1, 1, false},
		{"BBBC", 2, 2, false},
		{"BKBKB", 3, 3, false},
		{"BCBBCB", 2, 2, false},
		{"KBK", 1, 1, false},
		{"BCC", 0, 0, true},
		{"BKC", 0, 0, true},
	}
	for _, c := range cases {
		t.Run(c.log, func(t *testing.T) {
			m := newFS(t)
			e := mustEngine(t, m, EngineOptions{Frames: 8})
			var xid uint64
			for _, r := range c.log {
				var err error
				switch r {
				case 'B':
					xid++
					_, err = e.w.Append(bg, RecordTxnBegin, binary.LittleEndian.AppendUint64(nil, xid))
				case 'C':
					_, err = e.w.Append(bg, RecordTxnCommit, binary.LittleEndian.AppendUint64(nil, xid))
				case 'K':
					_, err = e.w.Append(bg, RecordCheckpoint, []byte("not a real check")) // 16 bytes, next ID huge
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := e.w.Flush(bg); err != nil {
				t.Fatal(err)
			}
			m.Crash(vfs.CrashOptions{})
			if c.corrupt {
				if _, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 8}); !errors.Is(err, ErrCorrupt) {
					t.Fatalf("open: %v", err)
				}
				return
			}
			e2 := mustEngine(t, m, EngineOptions{Frames: 8})
			defer func() { _ = e2.Close(bg) }()
			rec := e2.Recovery()
			if rec.DiscardedTransactions != c.groups || rec.Discarded != c.discarded {
				t.Fatalf("recovery %+v: want %d transactions and %d records discarded", rec, c.groups, c.discarded)
			}
		})
	}
}

func TestTxnsAreAllOrNothingAcrossCrashes(t *testing.T) {
	base := testSeed(t)
	runs := 60
	if testing.Short() {
		runs = 10
	}
	var discarded, torn, partialCommits int
	for run := range runs {
		seed := base + uint64(run)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 31))
			m := vfs.NewMemFS(seed)
			_ = m.MkdirAll("/db")
			_ = m.SyncDir("/")
			opts := EngineOptions{Frames: 24 + rng.IntN(40), WAL: Options{SegmentSize: []int64{8192, 1 << 20}[rng.IntN(2)]}}
			e := mustEngine(t, m, opts)
			h, tr := setupHeapAndTree(t, e)
			acked := map[int]int{} // transaction -> rows
			ackedXID := map[int]XID{}
			stmt := 0
			var lastXID XID
			for cycle := range 3 {
				for range 1 + rng.IntN(12) {
					stmt++
					n := 1 + rng.IntN(12)
					if rng.IntN(4) == 0 {
						if _, err := e.Checkpoint(bg); err != nil {
							t.Fatal(err)
						}
					}
					xid, err := runTxn(e, h, tr, stmt, n)
					if err != nil {
						t.Fatalf("transaction %d: %v", stmt, err)
					}
					if xid <= lastXID {
						t.Fatalf("transaction ID %d after %d", xid, lastXID)
					}
					lastXID = xid
					acked[stmt], ackedXID[stmt] = n, xid
				}
				// A transaction in flight when the crash comes.
				stmt++
				inFlight := stmt
				tx := openTxn(t, e, func() error { return insertRows(h, tr, inFlight, 0, rng.IntN(15)) })
				flightXID := tx.XID()
				if flightXID <= lastXID {
					t.Fatalf("transaction ID %d after %d", flightXID, lastXID)
				}
				if rng.IntN(2) == 0 {
					_ = e.w.Flush(bg) // its records durable, the commit not
				}
				opt := vfs.CrashOptions{TearLast: rng.IntN(2) == 0}
				if opt.TearLast {
					torn++
				}
				m.Crash(opt)
				e = mustEngine(t, m, opts)
				if e.Recovery().DiscardedTransactions > 0 {
					discarded++
				}
				// IDs never go back; acknowledged ones are committed, the
				// one in flight aborted (or, if its begin never reached the
				// disk, unallocated again).
				if e.NextXID() <= lastXID {
					t.Fatalf("cycle %d: next ID %d, but %d was acknowledged", cycle, e.NextXID(), lastXID)
				}
				for s, x := range ackedXID {
					if st := e.Status(x); st != TxnCommitted && st != TxnResolved {
						t.Fatalf("cycle %d: acknowledged transaction %d (ID %d) is %v", cycle, s, x, st)
					}
				}
				if st := e.Status(flightXID); st != TxnAborted && st != TxnUnknown {
					t.Fatalf("cycle %d: transaction in flight (ID %d) is %v", cycle, flightXID, st)
				}
				var err error
				if h, err = e.OpenHeap(bg, h.FirstPage()); err != nil {
					t.Fatal(err)
				}
				if tr, err = e.OpenBTree(bg, tr.Root()); err != nil {
					t.Fatal(err)
				}
				rows, keys := countByStatement(t, h, tr)
				for s, n := range acked {
					if rows[s] != n || keys[s] != n {
						t.Fatalf("cycle %d: acknowledged statement %d has %d rows and %d keys, want %d", cycle, s, rows[s], keys[s], n)
					}
				}
				if rows[inFlight] != 0 || keys[inFlight] != 0 {
					partialCommits++
					t.Fatalf("cycle %d: uncommitted transaction %d left %d rows and %d keys", cycle, inFlight, rows[inFlight], keys[inFlight])
				}
				for s := range rows {
					if _, ok := acked[s]; !ok {
						t.Fatalf("rows from transaction %d, which never committed", s)
					}
				}
			}
			if err := e.Close(bg); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Logf("%d runs: %d recoveries discarded a transaction, %d torn crashes", runs, discarded, torn)
	if runs >= 60 && (discarded < runs/2 || torn < runs/2) {
		t.Fatalf("too few discards (%d) or torn crashes (%d)", discarded, torn)
	}
}

func TestNextXIDSurvivesTrimmedLogs(t *testing.T) {
	// Once checkpoints have removed every segment holding a begin record,
	// only checkpoint records carry NextXID: a restart, and a crash, must
	// still not hand out an ID again.
	m := newFS(t)
	opts := EngineOptions{Frames: 32, WAL: Options{SegmentSize: 8192}}
	e := mustEngine(t, m, opts)
	h, tr := setupHeapAndTree(t, e)
	var last XID
	for s := 1; s <= 20; s++ {
		xid, err := runTxn(e, h, tr, s, 5)
		if err != nil {
			t.Fatal(err)
		}
		last = xid
	}
	for range 3 {
		if _, err := e.Checkpoint(bg); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Close(bg); err != nil {
		t.Fatal(err)
	}
	e = mustEngine(t, m, opts)
	if e.Recovery().NextXID != last+1 || e.NextXID() != last+1 {
		t.Fatalf("after a restart: next %d (recovery %d), last %d", e.NextXID(), e.Recovery().NextXID, last)
	}
	// Old IDs are resolved: committed or gone; the next is unknown.
	if e.Status(1) != TxnResolved || e.Status(last+1) != TxnUnknown || e.Status(0) != TxnUnknown {
		t.Fatalf("statuses %v %v %v", e.Status(1), e.Status(last+1), e.Status(0))
	}
	if _, err := e.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	m.Crash(vfs.CrashOptions{TearLast: true})
	e = mustEngine(t, m, opts)
	defer func() { _ = e.Close(bg) }()
	h, _ = e.OpenHeap(bg, h.FirstPage())
	tr, _ = e.OpenBTree(bg, tr.Root())
	xid, err := runTxn(e, h, tr, 99, 1)
	if err != nil || xid != last+1 {
		t.Fatalf("first ID after a crash: %d %v, want %d", xid, err, last+1)
	}
}

func TestXIDExhaustion(t *testing.T) {
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 16})
	h, tr := setupHeapAndTree(t, e)
	e.mu.Lock()
	e.nextXID = MaxXID - 1
	e.mu.Unlock()
	xid, err := runTxn(e, h, tr, 1, 1)
	if err != nil || xid != MaxXID-1 {
		t.Fatalf("the last ID: %d %v", xid, err)
	}
	tx := e.Begin()
	if err := tx.Write(bg, func(context.Context) error { return nil }); !errors.Is(err, ErrXIDExhausted) {
		t.Fatalf("past the last ID: %v", err)
	}
	// Nothing was taken: the engine still checkpoints and closes, and the
	// limit holds after a restart too.
	if err := e.Close(bg); err != nil {
		t.Fatal(err)
	}
	e = mustEngine(t, m, EngineOptions{Frames: 16})
	defer func() { _ = e.Close(bg) }()
	// A clean close checkpoints after the commit: its outcome is past the
	// table's window, so resolved.
	if e.NextXID() != MaxXID || e.Status(MaxXID-1) != TxnResolved {
		t.Fatalf("next %d, status %v", e.NextXID(), e.Status(MaxXID-1))
	}
	if err := e.Begin().Write(bg, func(context.Context) error { return nil }); !errors.Is(err, ErrXIDExhausted) {
		t.Fatalf("past the last ID after a restart: %v", err)
	}
}

func TestStatusTableStaysBounded(t *testing.T) {
	// Outcomes are kept for two checkpoint intervals: many transactions
	// over many checkpoints leave a small table, and an outcome is kept
	// at least until the checkpoint after the one following it.
	m := newFS(t)
	e := mustEngine(t, m, EngineOptions{Frames: 32})
	defer func() { _ = e.Close(bg) }()
	h, tr := setupHeapAndTree(t, e)
	var xids []XID
	for s := 1; s <= 300; s++ {
		xid, err := runTxn(e, h, tr, s, 1)
		if err != nil {
			t.Fatal(err)
		}
		xids = append(xids, xid)
		if s%50 == 0 {
			if e.Status(xid) != TxnCommitted {
				t.Fatalf("transaction %d: %v before a checkpoint", xid, e.Status(xid))
			}
			if _, err := e.Checkpoint(bg); err != nil {
				t.Fatal(err)
			}
			if e.Status(xid) != TxnCommitted {
				t.Fatalf("transaction %d: %v after one checkpoint", xid, e.Status(xid))
			}
		}
	}
	e.mu.Lock()
	n := len(e.outcomes)
	e.mu.Unlock()
	if n > 100 {
		t.Fatalf("%d outcomes kept after 300 transactions and 6 checkpoints", n)
	}
	if e.Status(xids[0]) != TxnResolved || e.Status(xids[len(xids)-1]) != TxnCommitted {
		t.Fatalf("oldest %v, newest %v", e.Status(xids[0]), e.Status(xids[len(xids)-1]))
	}
}
