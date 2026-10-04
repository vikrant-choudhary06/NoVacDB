package executor

import (
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/catalog"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

// Rollback with undo (docs/design/17-rollback.md).

// rawDump renders every table's row versions, read from the heap directly
// (uncommitted ones included): RID, XID, undo pointer, deleted, data; and
// every index's entries. A rollback must give back exactly the dump from
// before it (section 9, decision 1).
func rawDump(t testing.TB, db *DB) string {
	t.Helper()
	tables := db.cat.Tables()
	slices.SortFunc(tables, func(x, y *catalog.Table) int { return strings.Compare(x.Name, y.Name) })
	var b strings.Builder
	for _, tbl := range tables {
		fmt.Fprintf(&b, "table %s\n", tbl.Name)
		s := tbl.Heap.ScanVersions()
		for {
			rid, v, ok, err := s.Next(bg)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			fmt.Fprintf(&b, "  %s xid=%d undo=%d deleted=%v %s\n", rid, v.XID, v.Undo, v.Deleted, hex.EncodeToString(v.Data))
		}
		for _, ix := range tbl.Indexes {
			fmt.Fprintf(&b, " index %s\n", ix.Name)
			it := ix.Tree.Scan(btree.Bound{}, btree.Bound{})
			for {
				k, v, ok, err := it.Next(bg)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				fmt.Fprintf(&b, "  %s=%s\n", hex.EncodeToString(k), hex.EncodeToString(v))
			}
		}
	}
	return b.String()
}

// failAt makes the next statement fail with an SQL error at change i,
// stage (changeHook), and reports whether it did.
func failAt(db *DB, i int, stage changeStage) *bool {
	fired := false
	db.changeHook = func(n int, st changeStage) error {
		if n == i && st == stage {
			db.changeHook = nil
			fired = true
			return sqlerr.New(sqlerr.DataException, "injected failure")
		}
		return nil
	}
	return &fired
}

// rbStatement makes a random statement over tables a and b (id, v, tag,
// pad).
func rbStatement(rng *rand.Rand, next *int) string {
	tbl := []string{"a", "b"}[rng.IntN(2)]
	lo := rng.IntN(40)
	hi := lo + rng.IntN(12)
	switch rng.IntN(5) {
	case 0, 1:
		var vals []string
		for range 1 + rng.IntN(6) {
			*next++
			vals = append(vals, fmt.Sprintf("(%d, %d, 't%d', '%s')", *next, rng.IntN(50), *next, strings.Repeat("p", rng.IntN(200))))
		}
		return fmt.Sprintf("INSERT INTO %s VALUES %s", tbl, strings.Join(vals, ", "))
	case 2:
		// Grows or shrinks rows, which moves some.
		return fmt.Sprintf("UPDATE %s SET pad = '%s' WHERE id >= %d AND id <= %d", tbl, strings.Repeat("g", rng.IntN(3000)), lo, hi)
	case 3:
		return fmt.Sprintf("UPDATE %s SET v = v * 2 + 1, tag = tag || 'u' WHERE id >= %d AND id <= %d", tbl, lo, hi)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE id >= %d AND id <= %d", tbl, lo, lo+rng.IntN(3))
}

func rbSetup(t *testing.T, db *DB, rng *rand.Rand) (next int) {
	t.Helper()
	for _, tbl := range []string{"a", "b"} {
		mustExec(t, db, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v int, tag text, pad text); CREATE INDEX ON %s (v); CREATE UNIQUE INDEX ON %s (tag)", tbl, tbl, tbl))
	}
	for range 30 {
		s := rbStatement(rng, &next)
		if !strings.HasPrefix(s, "DELETE") {
			mustExec(t, db, s)
		}
	}
	return next
}

// TestRollbackRestoresEverything is the acceptance test of section 7:
// random transactions (inserts, updates that move rows, deletes, the same
// row changed many times, statements failing partway) rolled back leave
// every row version and every index entry exactly as before. Statements
// that fail are undone exactly, back to the state before them.
func TestRollbackRestoresEverything(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, 5))
	db := newDB(t)
	next := rbSetup(t, db, rng)
	txns := 120
	if testing.Short() {
		txns = 40
	}
	var failed, rolledBack, committed int
	for i := range txns {
		before := rawDump(t, db)
		tx := mustBegin(t, db)
		t.Cleanup(func() { _ = tx.Rollback(bg) }) // before Close, if the test fails
		ended := false
		for range 1 + rng.IntN(6) {
			s := rbStatement(rng, &next)
			if rng.IntN(5) == 0 {
				// Fail partway: after a row, or between its index changes.
				pre := rawDump(t, db)
				fired := failAt(db, rng.IntN(4), changeStage(rng.IntN(3)))
				_, err := tx.Exec(bg, s)
				db.changeHook = nil
				if !*fired {
					if err != nil && sqlerr.Code(err) != sqlerr.UniqueViolation {
						t.Fatalf("seed %d txn %d: %s: %v", seed, i, s, err)
					}
					if err != nil {
						break
					}
					continue // fewer changes than the failure point
				}
				if sqlerr.Code(err) != sqlerr.DataException {
					t.Fatalf("seed %d txn %d: an injected failure: %v", seed, i, err)
				}
				if got := rawDump(t, db); got != pre {
					t.Fatalf("seed %d txn %d: %s: the failed statement was not undone exactly:\n%s\nwant\n%s", seed, i, s, got, pre)
				}
				failed++
				break // the transaction is failed: only a rollback is left
			}
			if _, err := tx.Exec(bg, s); err != nil {
				if sqlerr.Code(err) == sqlerr.UniqueViolation {
					break // refused before any change; the transaction is failed
				}
				t.Fatalf("seed %d txn %d: %s: %v", seed, i, s, err)
			}
		}
		if rng.IntN(4) == 0 && !tx.failed {
			if err := tx.Commit(bg); err != nil {
				t.Fatal(err)
			}
			committed++
			ended = true
		}
		if !ended {
			if err := tx.Rollback(bg); err != nil {
				t.Fatal(err)
			}
			if got := rawDump(t, db); got != before {
				t.Fatalf("seed %d txn %d: after the rollback:\n%s\nwant\n%s", seed, i, got, before)
			}
			// No snapshot is left, the transaction's own included, so its
			// undo is gone.
			if segs := db.e.Undo().Segments(); len(segs) != 0 {
				t.Fatalf("seed %d txn %d: undo left after the rollback: %v", seed, i, segs)
			}
			rolledBack++
		}
	}
	if failed == 0 || rolledBack == 0 || committed == 0 || db.undoneStatements.Load() != int64(failed) || db.discards.Load() != 0 || db.restarts.Load() != 0 {
		t.Fatalf("%d failed statements, %d rollbacks, %d commits, %d reopens, %d restarts", failed, rolledBack, committed, db.discards.Load(), db.restarts.Load())
	}
	checkConsistency(t, db)
	t.Logf("%d rollbacks, %d failed statements undone, %d commits", rolledBack, failed, committed)
}

// TestStatementRollbackKeepsEarlierStatements: in a transaction, a failed
// statement is undone exactly; the statements before it stay until the
// transaction ends, which here is a commit, reported as a rollback
// (25P02), so nothing of it stays.
func TestStatementRollbackKeepsEarlierStatements(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, tag text); CREATE INDEX ON t (tag)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	before := rawDump(t, db)
	tx := mustBegin(t, db)
	t.Cleanup(func() { _ = tx.Rollback(bg) })
	txExec(t, tx, "INSERT INTO t VALUES (4, 'd'); UPDATE t SET tag = 'b2' WHERE id = 2")
	afterFirst := rawDump(t, db)
	failAt(db, 2, changeOldEntries) // rows 1 and 2 done, row 3 half done
	_, err := tx.Exec(bg, "UPDATE t SET tag = 'x'")
	if sqlerr.Code(err) != sqlerr.DataException {
		t.Fatalf("%v", err)
	}
	if got := rawDump(t, db); got != afterFirst {
		t.Fatalf("after the failed statement:\n%s\nwant\n%s", got, afterFirst)
	}
	if _, err := tx.Exec(bg, "SELECT 1"); sqlerr.Code(err) != sqlerr.InFailedSQLTransaction {
		t.Fatalf("after the failed statement: %v", err)
	}
	if err := tx.Commit(bg); sqlerr.Code(err) != sqlerr.InFailedSQLTransaction {
		t.Fatalf("commit of a failed transaction: %v", err)
	}
	if got := rawDump(t, db); got != before {
		t.Fatalf("after the transaction:\n%s\nwant\n%s", got, before)
	}
	if db.discards.Load() != 0 || db.restarts.Load() != 0 || db.undoneStatements.Load() != 1 || db.rollbacks.Load() != 1 {
		t.Fatalf("%d reopens, %d restarts, %d statements undone", db.discards.Load(), db.restarts.Load(), db.undoneStatements.Load())
	}
	// In autocommit, the failed statement is its whole transaction.
	failAt(db, 1, changeNewEntries)
	_, err = db.Exec(bg, "UPDATE t SET tag = 'y'")
	if sqlerr.Code(err) != sqlerr.DataException {
		t.Fatalf("%v", err)
	}
	if got := rawDump(t, db); got != before {
		t.Fatalf("after the failed autocommit statement:\n%s\nwant\n%s", got, before)
	}
	if segs := db.e.Undo().Segments(); len(segs) != 0 {
		t.Fatalf("undo left after the failed autocommit statement: %v", segs)
	}
	checkConsistency(t, db)
}

// TestWriteConflictUndoesOnlyTheStatement: a REPEATABLE READ write
// conflict partway through a statement undoes that statement in place,
// not the transaction's earlier ones; no reopen, so other snapshots go on.
func TestWriteConflictUndoesOnlyTheStatement(t *testing.T) {
	db := isoSetup(t)
	mustExec(t, db, "CREATE INDEX ON t (v)")
	other := beginTx(t, db, RepeatableRead)
	otherView := txRows(t, other, "SELECT id, v FROM t ORDER BY id")
	tx := beginTx(t, db, RepeatableRead)
	txRows(t, tx, "SELECT v FROM t") // its snapshot
	mustExec(t, db, "UPDATE t SET v = 'theirs' WHERE id = 3")
	txExec(t, tx, "UPDATE t SET v = 'mine' WHERE id = 1")
	afterFirst := rawDump(t, db)
	// Rows 1 and 2 change, then row 3 conflicts.
	_, err := tx.Exec(bg, "UPDATE t SET v = v || '!'")
	if sqlerr.Code(err) != sqlerr.SerializationFailure {
		t.Fatalf("%v", err)
	}
	if got := rawDump(t, db); got != afterFirst {
		t.Fatalf("after the conflict:\n%s\nwant\n%s", got, afterFirst)
	}
	if err := tx.Rollback(bg); err != nil {
		t.Fatal(err)
	}
	if got := rows(t, db, "SELECT id, v FROM t ORDER BY id"); got != "1|a\n2|b\n3|theirs" {
		t.Fatalf("after the rollback: %q", got)
	}
	if got := txRows(t, other, "SELECT id, v FROM t ORDER BY id"); got != otherView {
		t.Fatalf("another REPEATABLE READ transaction after the conflict: %q, want %q", got, otherView)
	}
	if db.discards.Load() != 0 || db.restarts.Load() != 0 || db.undoneStatements.Load() != 1 {
		t.Fatalf("%d reopens, %d restarts, %d undone", db.discards.Load(), db.restarts.Load(), db.undoneStatements.Load())
	}
	checkConsistency(t, db)
}

// TestRollbackKeepsIndexesUsable: right after a rollback, statements read
// through indexes again (section 2.8).
func TestRollbackKeepsIndexesUsable(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v int); CREATE INDEX ON t (v); INSERT INTO t VALUES (1, 10), (2, 20)")
	tx := mustBegin(t, db)
	t.Cleanup(func() { _ = tx.Rollback(bg) })
	txExec(t, tx, "UPDATE t SET v = 15 WHERE id = 1; INSERT INTO t VALUES (3, 30)")
	if err := tx.Rollback(bg); err != nil {
		t.Fatal(err)
	}
	scans, retries := db.indexScans.Load(), db.indexScanRetries.Load()
	if got := rows(t, db, "SELECT id FROM t WHERE v >= 10 ORDER BY id"); got != "1\n2" {
		t.Fatal(got)
	}
	if db.indexScans.Load() != scans+1 || db.indexScanRetries.Load() != retries {
		t.Fatalf("after a rollback: %d index scans, %d redone", db.indexScans.Load()-scans, db.indexScanRetries.Load()-retries)
	}
}

// TestFailedRollbackRestarts: an I/O failure while rolling back (at each
// write and sync in turn) restarts the database; recovery discards the
// transaction, so the result is the same, also after a crash.
func TestFailedRollbackRestarts(t *testing.T) {
	for _, op := range []vfs.Op{vfs.OpWriteAt, vfs.OpSync} {
		for after := 0; ; after++ {
			m := newFS(t)
			db := openDB(t, m, Options{})
			mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, pad text); CREATE INDEX ON t (pad)")
			mustExec(t, db, "INSERT INTO t VALUES (1, 'one'), (2, 'two')")
			want := rows(t, db, "SELECT id, pad FROM t ORDER BY id")
			tx := mustBegin(t, db)
			t.Cleanup(func() { _ = tx.Rollback(bg) })
			txExec(t, tx, fmt.Sprintf("INSERT INTO t VALUES (3, '%s'); UPDATE t SET pad = 'x' WHERE id = 1; DELETE FROM t WHERE id = 2", strings.Repeat("z", 900)))
			m.InjectError(vfs.Fault{Op: op, After: after})
			err := tx.Rollback(bg)
			m.ClearFaults()
			fired := db.discards.Load() == 1 // a failed rollback reopens
			if err != nil {
				t.Fatalf("%v after %d: rollback: %v", op, after, err)
			}
			if got := rows(t, db, "SELECT id, pad FROM t ORDER BY id"); got != want {
				t.Fatalf("%v after %d: %q", op, after, got)
			}
			checkConsistency(t, db)
			m.Crash(vfs.CrashOptions{TearLast: true})
			db2 := openDB(t, m, Options{})
			if got := rows(t, db2, "SELECT id, pad FROM t ORDER BY id"); got != want {
				t.Fatalf("%v after %d, after a crash: %q", op, after, got)
			}
			checkConsistency(t, db2)
			_ = db2.Close(bg)
			if !fired {
				if after == 0 {
					t.Fatalf("%v: a rollback does no such operation", op)
				}
				break
			}
		}
	}
}

// TestRolledBackTransactionsReplay: the records of rolled-back
// transactions, and their reversal, are replayed by recovery, and later
// transactions that depend on their page layout (index splits, reused
// slots) recover too (section 2.6).
func TestRolledBackTransactionsReplay(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, 9))
	m := newFS(t)
	db := openDB(t, m, Options{CheckpointBytes: 1 << 40})
	next := rbSetup(t, db, rng)
	for i := range 30 {
		tx := mustBegin(t, db)
		t.Cleanup(func() { _ = tx.Rollback(bg) })
		for range 1 + rng.IntN(5) {
			s := rbStatement(rng, &next)
			if _, err := tx.Exec(bg, s); err != nil {
				if sqlerr.Code(err) == sqlerr.UniqueViolation {
					break
				}
				t.Fatalf("txn %d: %s: %v", i, s, err)
			}
		}
		var err error
		if i%2 == 0 && !tx.failed {
			err = tx.Commit(bg)
		} else {
			err = tx.Rollback(bg)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	want := rawDump(t, db)
	aborted := 0
	for x := wal.XID(1); x < db.e.NextXID(); x++ {
		if db.e.Status(x) == wal.TxnAborted {
			aborted++
		}
	}
	if aborted == 0 {
		t.Fatal("setup: nothing rolled back")
	}
	m.Crash(vfs.CrashOptions{})
	db2 := openDB(t, m, Options{})
	defer func() { _ = db2.Close(bg) }()
	if got := rawDump(t, db2); got != want {
		t.Fatalf("seed %d: after recovery:\n%s\nwant\n%s", seed, got, want)
	}
	if db2.e.Recovery().DiscardedTransactions != 0 {
		t.Fatalf("recovery discarded %d transactions", db2.e.Recovery().DiscardedTransactions)
	}
	checkConsistency(t, db2)
}

// TestRollbackReleasesItsUndo: at either isolation level, a rolled-back
// transaction's own snapshot does not hold back its undo (section 2.8).
func TestRollbackReleasesItsUndo(t *testing.T) {
	for _, iso := range []IsolationLevel{ReadCommitted, RepeatableRead} {
		db := newDB(t)
		mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v int); INSERT INTO t VALUES (1, 1)")
		tx, err := db.BeginTx(bg, TxOptions{Isolation: iso})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Rollback(bg) })
		txRows(t, tx, "SELECT v FROM t")
		txExec(t, tx, "UPDATE t SET v = 2; INSERT INTO t VALUES (2, 2)")
		if err := tx.Rollback(bg); err != nil {
			t.Fatal(err)
		}
		if segs := db.e.Undo().Segments(); len(segs) != 0 {
			t.Fatalf("isolation %d: undo left after the rollback: %v", iso, segs)
		}
		if db.e.LiveSnapshots() != 0 {
			t.Fatalf("isolation %d: %d snapshots left", iso, db.e.LiveSnapshots())
		}
	}
}
