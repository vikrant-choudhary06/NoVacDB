package executor

import (
	"fmt"
	"strings"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
)

// txRows runs a query in a transaction and returns its rows, one per line.
func txRows(t *testing.T, tx *Tx, sql string) string {
	t.Helper()
	rs, err := tx.Exec(bg, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	var lines []string
	for _, r := range rs[len(rs)-1].Rows {
		var cells []string
		for _, v := range r {
			if v.Null {
				cells = append(cells, "NULL")
			} else {
				cells = append(cells, types.Format(v))
			}
		}
		lines = append(lines, strings.Join(cells, "|"))
	}
	return strings.Join(lines, "\n")
}

func isoSetup(t *testing.T) *DB {
	t.Helper()
	db := openDB(t, newFS(t), Options{})
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v text)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	// Cleanups run last-registered first: every transaction's rollback
	// (beginTx) before this Close, which a failed test's open
	// transaction would otherwise block.
	t.Cleanup(func() { _ = db.Close(bg) })
	return db
}

func beginTx(t *testing.T, db *DB, iso IsolationLevel) *Tx {
	t.Helper()
	tx, err := db.BeginTx(bg, TxOptions{Isolation: iso})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(bg) })
	return tx
}

func TestReadCommittedSeesEachCommit(t *testing.T) {
	db := isoSetup(t)
	tx := beginTx(t, db, ReadCommitted)
	if got := txRows(t, tx, "SELECT v FROM t WHERE id = 1"); got != "a" {
		t.Fatalf("first read: %q", got)
	}
	mustExec(t, db, "UPDATE t SET v = 'a2' WHERE id = 1; INSERT INTO t VALUES (4, 'd')")
	if got := txRows(t, tx, "SELECT v FROM t WHERE id = 1"); got != "a2" {
		t.Fatalf("a non-repeatable read is expected under READ COMMITTED: %q", got)
	}
	if got := txRows(t, tx, "SELECT id FROM t ORDER BY id"); got != "1\n2\n3\n4" {
		t.Fatalf("phantoms are expected under READ COMMITTED: %q", got)
	}
	if err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
}

func TestRepeatableReadKeepsItsSnapshot(t *testing.T) {
	db := isoSetup(t)
	tx := beginTx(t, db, RepeatableRead)
	before := txRows(t, tx, "SELECT id, v FROM t ORDER BY id")
	// Another session changes, moves, deletes, inserts and renumbers rows.
	mustExec(t, db, "UPDATE t SET v = 'a2' WHERE id = 1")
	mustExec(t, db, "UPDATE t SET v = '"+strings.Repeat("x", 6000)+"' WHERE id = 2")
	mustExec(t, db, "DELETE FROM t WHERE id = 3")
	mustExec(t, db, "INSERT INTO t VALUES (4, 'd')")
	mustExec(t, db, "UPDATE t SET id = 10 WHERE id = 1")
	for i := range 20 {
		mustExec(t, db, fmt.Sprintf("UPDATE t SET v = 'n%d' WHERE id = 10", i))
	}
	if got := txRows(t, tx, "SELECT id, v FROM t ORDER BY id"); got != before {
		t.Fatalf("REPEATABLE READ saw changes:\n%s\nwant\n%s", got, before)
	}
	// Lookups by key the index no longer describes for this snapshot.
	scans := db.indexScans.Load()
	if got := txRows(t, tx, "SELECT v FROM t WHERE id = 1"); got != "a" {
		t.Fatalf("by the old key: %q", got)
	}
	if got := txRows(t, tx, "SELECT v FROM t WHERE id = 10"); got != "" {
		t.Fatalf("by a key from after the snapshot: %q", got)
	}
	if db.indexScans.Load() != scans {
		t.Fatal("an index was used although it describes changes the snapshot does not see")
	}
	if err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	// A new statement sees everything, through the index again.
	if got := query(t, db, "SELECT v FROM t WHERE id = 10"); !strings.Contains(got, "n19") {
		t.Fatalf("after commit: %s", got)
	}
	if db.indexScans.Load() == scans {
		t.Fatal("the index is not used once every change is seen")
	}
	// The undo it needed is released by the next checkpoint.
	if _, err := db.e.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	if segs := db.e.Undo().Segments(); len(segs) != 0 {
		t.Fatalf("undo kept after the reader ended: %v", segs)
	}
}

func TestTransactionsSeeTheirOwnChanges(t *testing.T) {
	for _, iso := range []IsolationLevel{ReadCommitted, RepeatableRead} {
		t.Run(iso.String(), func(t *testing.T) {
			db := isoSetup(t)
			tx := beginTx(t, db, iso)
			txRows(t, tx, "SELECT id FROM t") // the snapshot, before any write
			if _, err := tx.Exec(bg, "INSERT INTO t VALUES (5, 'e'); UPDATE t SET v = 'own' WHERE id = 2; DELETE FROM t WHERE id = 3"); err != nil {
				t.Fatal(err)
			}
			if got := txRows(t, tx, "SELECT id, v FROM t ORDER BY id"); got != "1|a\n2|own\n5|e" {
				t.Fatalf("own changes: %q", got)
			}
			if got := txRows(t, tx, "SELECT v FROM t WHERE id = 5"); got != "e" {
				t.Fatalf("own insert by key: %q", got)
			}
			if err := tx.Commit(bg); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRepeatableReadWriteConflict(t *testing.T) {
	db := isoSetup(t)
	tx := beginTx(t, db, RepeatableRead)
	txRows(t, tx, "SELECT v FROM t WHERE id = 1")
	// A row the snapshot sees as current can be changed, and changed again:
	// its own version is no conflict.
	if _, err := tx.Exec(bg, "UPDATE t SET v = 'mine' WHERE id = 2; UPDATE t SET v = v || '2' WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	// Its snapshot does not hold back its own undo at commit.
	if segs := db.e.Undo().Segments(); len(segs) != 0 {
		t.Fatalf("undo kept after a REPEATABLE READ commit: %v", segs)
	}
	if got := query(t, db, "SELECT v FROM t WHERE id = 2"); !strings.Contains(got, "mine2") {
		t.Fatalf("after the double update: %s", got)
	}

	tx = beginTx(t, db, RepeatableRead)
	txRows(t, tx, "SELECT v FROM t WHERE id = 1")
	mustExec(t, db, "UPDATE t SET v = 'theirs' WHERE id = 1")
	for _, q := range []string{"UPDATE t SET v = v || '!' WHERE id = 1", "DELETE FROM t WHERE id = 1"} {
		tx2 := tx
		if q != "UPDATE t SET v = v || '!' WHERE id = 1" {
			tx2 = beginTx(t, db, RepeatableRead)
			txRows(t, tx2, "SELECT v FROM t WHERE id = 1")
			mustExec(t, db, "UPDATE t SET v = 'theirs again' WHERE id = 1")
		}
		_, err := tx2.Exec(bg, q)
		if sqlerr.Code(err) != sqlerr.SerializationFailure {
			t.Fatalf("%s: %v, want 40001", q, err)
		}
		if _, err := tx2.Exec(bg, "SELECT 1"); sqlerr.Code(err) != sqlerr.InFailedSQLTransaction {
			t.Fatalf("after the conflict: %v", err)
		}
		if err := tx2.Rollback(bg); err != nil {
			t.Fatal(err)
		}
	}
	// The other session's change stands; the conflicting ones are gone.
	if got := query(t, db, "SELECT v FROM t WHERE id = 1"); !strings.Contains(got, "theirs again") || strings.Contains(got, "!") {
		t.Fatalf("after the conflicts: %s", got)
	}
	checkConsistency(t, db)
}

// A rollback with undo leaves REPEATABLE READ snapshots alone
// (docs/design/17-rollback.md section 2.5); a reopen (here, the rollback of
// a transaction that changed the schema) ends them, with 40001.
func TestRepeatableReadAcrossAReopen(t *testing.T) {
	db := isoSetup(t)
	tx := beginTx(t, db, RepeatableRead)
	before := txRows(t, tx, "SELECT id, v FROM t ORDER BY id")
	w, err := db.Begin(bg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Exec(bg, "INSERT INTO t VALUES (9, 'z'); UPDATE t SET v = 'changed' WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if err := w.Rollback(bg); err != nil {
		t.Fatal(err)
	}
	if got := txRows(t, tx, "SELECT id, v FROM t ORDER BY id"); got != before {
		t.Fatalf("after another session's rollback: %q, want %q", got, before)
	}
	w, _ = db.Begin(bg)
	if _, err := w.Exec(bg, "CREATE TABLE gone (a int)"); err != nil {
		t.Fatal(err)
	}
	if err := w.Rollback(bg); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(bg, "SELECT v FROM t"); sqlerr.Code(err) != sqlerr.SerializationFailure {
		t.Fatalf("after a reopen: %v, want 40001", err)
	}
	// A READ COMMITTED transaction just goes on.
	rc := beginTx(t, db, ReadCommitted)
	txRows(t, rc, "SELECT v FROM t")
	w, _ = db.Begin(bg)
	if _, err := w.Exec(bg, "CREATE TABLE gone (a int)"); err != nil {
		t.Fatal(err)
	}
	_ = w.Rollback(bg)
	if got := txRows(t, rc, "SELECT id FROM t ORDER BY id"); got != "1\n2\n3" {
		t.Fatalf("READ COMMITTED after a reopen: %q", got)
	}
}
