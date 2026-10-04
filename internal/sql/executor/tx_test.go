package executor

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

func mustBegin(t testing.TB, db *DB) *Tx {
	t.Helper()
	tx, err := db.Begin(bg)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func txExec(t testing.TB, tx *Tx, sql string) []*Result {
	t.Helper()
	rs, err := tx.Exec(bg, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return rs
}

func txErr(t testing.TB, err error, code string) {
	t.Helper()
	var se *sqlerr.Error
	if !errors.As(err, &se) || se.Code != code {
		t.Fatalf("error %v, want code %s", err, code)
	}
}

// snapshot describes every table the tests use, for comparisons.
func snapshot(t testing.TB, db *DB) string {
	t.Helper()
	var b strings.Builder
	for _, name := range []string{"t", "u"} {
		if !db.cat.Exists(name) {
			fmt.Fprintf(&b, "%s: none\n", name)
			continue
		}
		fmt.Fprintf(&b, "%s:\n%s\n", name, rows(t, db, "SELECT * FROM "+name+" ORDER BY 1"))
	}
	return b.String()
}

func TestTxCommitsTogether(t *testing.T) {
	m := newFS(t)
	db := openDB(t, m, Options{})
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v text)")
	mustExec(t, db, "CREATE TABLE u (k bigint UNIQUE)")
	// Prepared before the transaction writes: once it has, the DB's own
	// methods wait for it (tx.go).
	p, err := db.Prepare(bg, "SELECT k FROM u WHERE k = $1", nil)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, "DROP TABLE u")
	next := db.e.NextXID()
	tx := mustBegin(t, db)
	txExec(t, tx, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
	txExec(t, tx, "UPDATE t SET v = v || '!' WHERE id = 1; DELETE FROM t WHERE id = 2")
	txExec(t, tx, "CREATE TABLE u (k bigint UNIQUE)")
	txExec(t, tx, "INSERT INTO u VALUES (10), (20)")
	// The transaction sees its own changes.
	if got := strings.Join(strings.Fields(rowsTx(t, tx, "SELECT * FROM t")), " "); got != "1|a!" {
		t.Fatalf("inside: %q", got)
	}
	if r, err := tx.ExecPrepared(bg, p, []types.Value{types.NewInt8(20)}); err != nil || len(r.Rows) != 1 {
		t.Fatal(r, err)
	}
	if err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	// One transaction ID for the whole transaction.
	if db.e.NextXID() != next+1 || db.e.Status(next) != wal.TxnCommitted {
		t.Fatalf("next %d, status %v", db.e.NextXID(), db.e.Status(next))
	}
	want := "t:\n1|a!\nu:\n10\n20\n"
	if got := snapshot(t, db); got != want {
		t.Fatalf("%q", got)
	}
	// Committed means durable: there after a crash.
	m.Crash(vfs.CrashOptions{TearLast: true})
	db = openDB(t, m, Options{})
	defer func() { _ = db.Close(bg) }()
	if got := snapshot(t, db); got != want {
		t.Fatalf("after a crash: %q", got)
	}
	// Ended: no more statements.
	_, err = tx.Exec(bg, "SELECT 1")
	txErr(t, err, sqlerr.InvalidTransactionState)
	txErr(t, tx.Commit(bg), sqlerr.InvalidTransactionState)
	txErr(t, tx.Rollback(bg), sqlerr.InvalidTransactionState)
}

// rowsTx runs a query in a transaction and formats its rows.
func rowsTx(t testing.TB, tx *Tx, sql string) string {
	t.Helper()
	rs := txExec(t, tx, sql)
	out := format(rs[0])
	if i := strings.IndexByte(out, '\n'); i >= 0 {
		return out[i+1:]
	}
	return ""
}

func TestTxRollback(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v text); INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	before := snapshot(t, db)
	next := db.e.NextXID()
	for _, stmts := range [][]string{
		{"INSERT INTO t VALUES (4, 'd')"},
		{"UPDATE t SET v = 'x'", "DELETE FROM t WHERE id = 1"},
		{"CREATE TABLE u (k int)", "INSERT INTO u VALUES (1)"},
		{"DROP TABLE t"},
		{"CREATE INDEX tv ON t (v)", "DELETE FROM t"},
	} {
		tx := mustBegin(t, db)
		schema := false
		for _, s := range stmts {
			txExec(t, tx, s)
			schema = schema || strings.HasPrefix(s, "CREATE") || strings.HasPrefix(s, "DROP")
		}
		rb, dc := db.rollbacks.Load(), db.discards.Load()
		if err := tx.Rollback(bg); err != nil {
			t.Fatal(err)
		}
		// With undo, unless the transaction changed the schema: then by
		// reopening (docs/design/17-rollback.md section 2.7).
		if schema && (db.rollbacks.Load() != rb || db.discards.Load() != dc+1) ||
			!schema && (db.rollbacks.Load() != rb+1 || db.discards.Load() != dc) {
			t.Fatalf("%v: %d rollbacks with undo, %d by reopening", stmts, db.rollbacks.Load()-rb, db.discards.Load()-dc)
		}
		if got := snapshot(t, db); got != before {
			t.Fatalf("after rolling back %v:\n%s\nwant\n%s", stmts, got, before)
		}
		if db.cat.Exists("tv") {
			t.Fatal("a rolled-back index exists")
		}
	}
	// Each rolled-back transaction used an ID. The last is aborted; older
	// ones, several reopens back, are past the table's window (resolved).
	if db.e.NextXID() != next+5 || db.e.Status(next+4) != wal.TxnAborted {
		t.Fatalf("next %d (was %d), status %v", db.e.NextXID(), next, db.e.Status(next+4))
	}
	if st := db.e.Status(next); st != wal.TxnAborted && st != wal.TxnResolved {
		t.Fatalf("the first rolled back: %v", st)
	}
	// A transaction that only read rolls back without reopening anything,
	// and uses no ID.
	tx := mustBegin(t, db)
	if got := rowsTx(t, tx, "SELECT v FROM t WHERE id = 2"); got != "b" {
		t.Fatal(got)
	}
	rb, dc := db.rollbacks.Load(), db.discards.Load()
	if err := tx.Rollback(bg); err != nil {
		t.Fatal(err)
	}
	if db.rollbacks.Load() != rb || db.discards.Load() != dc || db.e.NextXID() != next+5 {
		t.Fatal("a read-only rollback rolled back or reopened anything, or took an ID")
	}
	// Commit of a read-only transaction takes no ID either.
	tx = mustBegin(t, db)
	txExec(t, tx, "SELECT * FROM t")
	if err := tx.Commit(bg); err != nil || db.e.NextXID() != next+5 {
		t.Fatal(err, db.e.NextXID())
	}
}

func TestFailedTx(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v text)")
	before := snapshot(t, db)
	for _, bad := range []string{
		"INSERT INTO t VALUES (1, 'dup')", // unique violation, found before changing anything
		"SELECT nope FROM t",              // a binding error
		"CREATE TABLE t (a int)",          // DDL that changes nothing
		"SELEC 1",                         // a syntax error
	} {
		tx := mustBegin(t, db)
		txExec(t, tx, "INSERT INTO t VALUES (1, 'a'), (2, 'b')")
		if _, err := tx.Exec(bg, bad); err == nil {
			t.Fatalf("%s: no error", bad)
		}
		// Everything after the error is refused, as PostgreSQL does.
		_, err := tx.Exec(bg, "INSERT INTO t VALUES (3, 'c')")
		txErr(t, err, sqlerr.InFailedSQLTransaction)
		_, err = tx.ExecPrepared(bg, nil, nil)
		txErr(t, err, sqlerr.InFailedSQLTransaction)
		// Commit rolls back instead, and says so.
		txErr(t, tx.Commit(bg), sqlerr.InFailedSQLTransaction)
		if got := snapshot(t, db); got != before {
			t.Fatalf("after %s:\n%s", bad, got)
		}
		txErr(t, tx.Rollback(bg), sqlerr.InvalidTransactionState)
	}
	// A failed prepared statement fails the transaction as well.
	p, err := db.Prepare(bg, "INSERT INTO t VALUES ($1, $2)", nil)
	if err != nil {
		t.Fatal(err)
	}
	tx := mustBegin(t, db)
	if _, err := tx.ExecPrepared(bg, p, []types.Value{types.NewInt4(1), types.NewText("a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecPrepared(bg, p, []types.Value{types.NewInt4(1), types.NewText("dup")}); err == nil {
		t.Fatal("no error")
	}
	_, err = tx.Exec(bg, "SELECT 1")
	txErr(t, err, sqlerr.InFailedSQLTransaction)
	txErr(t, tx.Commit(bg), sqlerr.InFailedSQLTransaction)
	if got := snapshot(t, db); got != before {
		t.Fatal(got)
	}
	// Rollback works on a failed transaction too.
	tx = mustBegin(t, db)
	txExec(t, tx, "INSERT INTO t VALUES (5, 'e')")
	if _, err := tx.Exec(bg, "INSERT INTO t VALUES (5, 'again')"); err == nil {
		t.Fatal("no error")
	}
	if err := tx.Rollback(bg); err != nil {
		t.Fatal(err)
	}
	if got := snapshot(t, db); got != before {
		t.Fatal(got)
	}
}

func TestTxOutgrowsThePool(t *testing.T) {
	db := openDB(t, newFS(t), Options{Frames: 24})
	defer func() { _ = db.Close(bg) }()
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, pad text)")
	mustExec(t, db, "INSERT INTO t VALUES (0, 'committed')")
	tx := mustBegin(t, db)
	restarts, dc := db.restarts.Load(), db.discards.Load()
	var err error
	for i := 1; err == nil && i < 400; i++ {
		_, err = tx.Exec(bg, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", i, strings.Repeat("x", 2000)))
	}
	var se *sqlerr.Error
	if !errors.As(err, &se) || se.Code != sqlerr.ProgramLimitExceeded || !strings.Contains(se.Message, "transaction") {
		t.Fatalf("a transaction larger than the pool: %v", err)
	}
	// The failed statement was undone in place (docs/design/17-rollback.md
	// section 2.5); the transaction is failed, and rolls back with undo.
	if db.undoneStatements.Load() == 0 {
		t.Fatal("the failed statement was not undone")
	}
	if _, err := tx.Exec(bg, "SELECT 1"); sqlerr.Code(err) != sqlerr.InFailedSQLTransaction {
		t.Fatalf("after the failed statement: %v", err)
	}
	rb := db.rollbacks.Load()
	if err := tx.Rollback(bg); err != nil {
		t.Fatal(err)
	}
	if db.rollbacks.Load() != rb+1 || db.restarts.Load() != restarts || db.discards.Load() != dc {
		t.Fatalf("%d rollbacks with undo, %d restarts, %d reopens", db.rollbacks.Load()-rb, db.restarts.Load()-restarts, db.discards.Load()-dc)
	}
	if got := rows(t, db, "SELECT id, pad FROM t"); got != "0|committed" {
		t.Fatalf("%q", got)
	}
	mustExec(t, db, "INSERT INTO t VALUES (1, 'after')")
}

func TestWritingTxBlocksOnlyWriters(t *testing.T) {
	// Readers never wait for a writing transaction and never see its
	// uncommitted rows; another writer waits for it to end
	// (docs/design/16-snapshots-visibility.md section 2.4).
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY)")
	// A transaction that only read blocks nobody.
	ro := mustBegin(t, db)
	txExec(t, ro, "SELECT * FROM t")
	mustExec(t, db, "INSERT INTO t VALUES (1)")
	tx := mustBegin(t, db)
	committed := false
	t.Cleanup(func() {
		if !committed {
			_ = tx.Rollback(bg) // so that Close does not wait for it
		}
	})
	txExec(t, tx, "INSERT INTO t VALUES (2)")
	// Readers run at once and see only committed rows, through the heap
	// and through the index.
	for _, q := range []string{"SELECT id FROM t ORDER BY id", "SELECT id FROM t WHERE id >= 1 ORDER BY id"} {
		read := make(chan string, 1)
		go func() { read <- rows(t, db, q) }()
		select {
		case g := <-read:
			if g != "1" {
				t.Fatalf("%s during a writing transaction: %q", q, g)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s waited for a writing transaction", q)
		}
	}
	if g := txRows(t, ro, "SELECT id FROM t ORDER BY id"); g != "1" {
		t.Fatalf("another transaction during a writing transaction: %q", g)
	}
	// The writer itself sees its own row.
	if g := txRows(t, tx, "SELECT id FROM t ORDER BY id"); g != "1\n2" {
		t.Fatalf("the writing transaction: %q", g)
	}
	inserted := make(chan error, 1)
	go func() {
		_, err := db.Exec(bg, "INSERT INTO t VALUES (3)")
		inserted <- err
	}()
	select {
	case err := <-inserted:
		t.Fatalf("a second writer ran while a transaction was writing: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	// The read-only transaction can still end (it holds nothing).
	if err := ro.Commit(bg); err != nil {
		t.Fatal(err)
	}
	committed = true
	if err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	if err := <-inserted; err != nil {
		t.Fatal(err)
	}
	if r := rows(t, db, "SELECT id FROM t ORDER BY id"); r != "1\n2\n3" {
		t.Fatal(r)
	}
}

func TestTxAcrossCrashes(t *testing.T) {
	// A transaction open when the database crashes leaves nothing, even
	// with its records durable; committed ones are all there.
	m := newFS(t)
	db := openDB(t, m, Options{})
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v int)")
	tx := mustBegin(t, db)
	txExec(t, tx, "INSERT INTO t VALUES (1, 1), (2, 2)")
	if err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	open := mustBegin(t, db)
	txExec(t, open, "INSERT INTO t VALUES (3, 3)")
	txExec(t, open, "UPDATE t SET v = v * 100")
	txExec(t, open, "CREATE TABLE u (k int)")
	if err := db.e.Flush(bg); err != nil {
		t.Fatal(err)
	}
	m.Crash(vfs.CrashOptions{})
	db2 := openDB(t, m, Options{})
	defer func() { _ = db2.Close(bg) }()
	if got := snapshot(t, db2); got != "t:\n1|1\n2|2\nu: none\n" {
		t.Fatalf("%q", got)
	}
	if db2.e.Status(open.wtx.XID()) != wal.TxnAborted {
		t.Fatalf("status %v", db2.e.Status(open.wtx.XID()))
	}
}

// TestRandomTxsMatchAModel runs random transactions of random statements,
// committing, rolling back or failing them, against the database and a
// model that replays only what committed: after every transaction, and
// after crashes, the two agree.
func TestRandomTxsMatchAModel(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, 61))
	m := newFS(t)
	db := openDB(t, m, Options{Frames: 64})
	model := newDB(t)
	for _, d := range []*DB{db, model} {
		mustExec(t, d, "CREATE TABLE t (id int PRIMARY KEY, v int NOT NULL, w text UNIQUE)")
	}
	statement := func() string {
		id := rng.IntN(40)
		switch rng.IntN(6) {
		case 0, 1:
			return fmt.Sprintf("INSERT INTO t VALUES (%d, %d, 'w%d')", id, rng.IntN(100), rng.IntN(60))
		case 2:
			return fmt.Sprintf("UPDATE t SET v = v + %d WHERE id %% %d = %d", rng.IntN(5), 2+rng.IntN(3), rng.IntN(2))
		case 3:
			return fmt.Sprintf("UPDATE t SET w = 'w%d' WHERE id = %d", rng.IntN(60), id)
		case 4:
			return fmt.Sprintf("DELETE FROM t WHERE id = %d", id)
		}
		return fmt.Sprintf("SELECT * FROM t WHERE v > %d", rng.IntN(100))
	}
	var commits, rollbacks, failures, crashes int
	for round := range 300 {
		tx := mustBegin(t, db)
		var done []string
		failed := false
		for range 1 + rng.IntN(5) {
			s := statement()
			if _, err := tx.Exec(bg, s); err != nil {
				var se *sqlerr.Error
				if !errors.As(err, &se) || se.Code == sqlerr.InternalError || se.Code == sqlerr.DataCorrupted {
					t.Fatalf("round %d: %s: %v", round, s, err)
				}
				failed = true
				break
			}
			done = append(done, s)
		}
		switch {
		case rng.IntN(10) == 0:
			// A crash with the transaction open.
			if err := db.e.Flush(bg); err != nil {
				t.Fatal(err)
			}
			m.Crash(vfs.CrashOptions{TearLast: rng.IntN(2) == 0})
			db = openDB(t, m, Options{Frames: 64})
			crashes++
		case failed:
			txErr(t, tx.Commit(bg), sqlerr.InFailedSQLTransaction)
			failures++
		case rng.IntN(3) == 0:
			if err := tx.Rollback(bg); err != nil {
				t.Fatal(err)
			}
			rollbacks++
		default:
			if err := tx.Commit(bg); err != nil {
				t.Fatalf("round %d: commit: %v", round, err)
			}
			for _, s := range done {
				mustExec(t, model, s)
			}
			commits++
		}
		if got, want := rows(t, db, "SELECT * FROM t ORDER BY id"), rows(t, model, "SELECT * FROM t ORDER BY id"); got != want {
			t.Fatalf("round %d: database\n%s\nmodel\n%s", round, got, want)
		}
	}
	checkConsistency(t, db)
	_ = db.Close(bg)
	t.Logf("%d commits, %d rollbacks, %d failed, %d crashes", commits, rollbacks, failures, crashes)
	if commits < 50 || rollbacks < 20 || failures < 20 || crashes < 10 {
		t.Fatalf("too few of some outcome: %d %d %d %d", commits, rollbacks, failures, crashes)
	}
}
