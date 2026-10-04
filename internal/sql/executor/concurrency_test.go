package executor

import (
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
)

// Concurrent readers with one writer (docs/design/16-snapshots-
// visibility.md sections 2.4 and 7).

// kvRow is a row of the model checker's table.
type kvRow struct {
	v   int64
	pad string
}

// formatKV formats a model state as the checker's queries return it.
func formatKV(state map[int64]kvRow) string {
	keys := make([]int64, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	var lines []string
	for _, k := range keys {
		lines = append(lines, fmt.Sprintf("%d|%d|%s", k, state[k].v, state[k].pad))
	}
	return strings.Join(lines, "\n")
}

// resultRows formats a result's rows, one per line.
func resultRows(r *Result) string {
	var lines []string
	for _, row := range r.Rows {
		var cells []string
		for _, v := range row {
			cells = append(cells, types.Format(v))
		}
		lines = append(lines, strings.Join(cells, "|"))
	}
	return strings.Join(lines, "\n")
}

func isSerializationFailure(err error) bool {
	var se *sqlerr.Error
	return errors.As(err, &se) && se.Code == sqlerr.SerializationFailure
}

// TestConcurrentModel runs one writer, which commits and rolls back random
// transactions, against REPEATABLE READ readers. Every committed
// transaction bumps a counter, and the model's state after each is known,
// so a reader checks that each of its reads, through the table and
// through an index, is exactly the state after the last commit its
// snapshot sees, and stays so for the whole transaction.
func TestConcurrentModel(t *testing.T) {
	seed := testSeed(t)
	// Frequent checkpoints, which release undo, and a small pool, which
	// evicts pages, while readers follow undo chains.
	db := openDB(t, newFS(t), Options{Frames: 64, CheckpointBytes: 32 << 10})
	t.Cleanup(func() { _ = db.Close(bg) })
	mustExec(t, db, "CREATE TABLE kv (k int PRIMARY KEY, v int NOT NULL, pad text NOT NULL); CREATE INDEX ON kv (v)")
	mustExec(t, db, "CREATE TABLE meta (id int PRIMARY KEY, n int NOT NULL); INSERT INTO meta VALUES (1, 0)")
	var states sync.Map // n -> the state after the n-th commit
	states.Store(int64(0), "")

	txns := 400
	if testing.Short() {
		txns = 120
	}
	var done atomic.Bool
	errc := make(chan error, 8)
	var commits, rollbacks atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer done.Store(true)
		rng := rand.New(rand.NewPCG(seed, 1))
		model := map[int64]kvRow{}
		n := int64(0)
		for range txns {
			next := maps.Clone(model)
			tx, err := db.Begin(bg)
			if err != nil {
				errc <- err
				return
			}
			var stmts []string
			for range 1 + rng.IntN(4) {
				k := int64(rng.IntN(40))
				r, exists := next[k]
				switch {
				case !exists:
					r = kvRow{v: int64(rng.IntN(100)), pad: strings.Repeat("p", rng.IntN(50))}
					next[k] = r
					stmts = append(stmts, fmt.Sprintf("INSERT INTO kv VALUES (%d, %d, '%s')", k, r.v, r.pad))
				case rng.IntN(4) == 0:
					delete(next, k)
					stmts = append(stmts, fmt.Sprintf("DELETE FROM kv WHERE k = %d", k))
				case rng.IntN(3) == 0:
					// Grows or shrinks the row, which may move it.
					r.pad = strings.Repeat(string(rune('a'+rng.IntN(26))), rng.IntN(3000))
					next[k] = r
					stmts = append(stmts, fmt.Sprintf("UPDATE kv SET pad = '%s' WHERE k = %d", r.pad, k))
				default:
					r.v = int64(rng.IntN(100))
					next[k] = r
					stmts = append(stmts, fmt.Sprintf("UPDATE kv SET v = %d WHERE k = %d", r.v, k))
				}
			}
			stmts = append(stmts, "UPDATE meta SET n = n + 1")
			for _, s := range stmts {
				if _, err := tx.Exec(bg, s); err != nil {
					_ = tx.Rollback(bg)
					errc <- fmt.Errorf("%s: %w", s, err)
					return
				}
			}
			if rng.IntN(8) == 0 {
				if err := tx.Rollback(bg); err != nil {
					errc <- err
					return
				}
				rollbacks.Add(1)
				continue
			}
			// The state is known before the commit makes it visible.
			states.Store(n+1, formatKV(next))
			if err := tx.Commit(bg); err != nil {
				errc <- err
				return
			}
			n++
			model = next
			commits.Add(1)
		}
	}()

	var checks, conflicts atomic.Int64
	for r := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(2+r)))
			for !done.Load() {
				err := func() error {
					tx, err := db.BeginTx(bg, TxOptions{Isolation: RepeatableRead})
					if err != nil {
						return err
					}
					defer func() { _ = tx.Rollback(bg) }()
					var n int64 = -1
					for i := range 3 {
						rs, err := tx.Exec(bg, "SELECT n FROM meta")
						if err != nil {
							return err
						}
						got := rs[0].Rows[0][0].I
						if n >= 0 && got != n {
							return fmt.Errorf("a REPEATABLE READ transaction saw counter %d, then %d", n, got)
						}
						n = got
						want, ok := states.Load(n)
						if !ok {
							return fmt.Errorf("counter %d has no known state", n)
						}
						q := "SELECT k, v, pad FROM kv ORDER BY k"
						if i%2 == 1 {
							q = "SELECT k, v, pad FROM kv WHERE v >= 0 ORDER BY k"
						}
						rs, err = tx.Exec(bg, q)
						if err != nil {
							return err
						}
						if got := resultRows(rs[0]); got != want.(string) {
							return fmt.Errorf("after commit %d, %s returned\n%s\nwant\n%s", n, q, got, want)
						}
						checks.Add(1)
						time.Sleep(time.Duration(rng.IntN(500)) * time.Microsecond)
					}
					return tx.Commit(bg)
				}()
				switch {
				case isSerializationFailure(err):
					conflicts.Add(1) // the writer rolled back: the database reopened
				case err != nil:
					errc <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	if checks.Load() == 0 || commits.Load() == 0 || db.indexScans.Load() == 0 || db.lastCkpt == 0 {
		t.Fatalf("%d checks, %d commits, %d index scans", checks.Load(), commits.Load(), db.indexScans.Load())
	}
	checkConsistency(t, db)
	t.Logf("%d checks, %d commits, %d rollbacks, %d conflicts, %d index scans, %d redone as table scans",
		checks.Load(), commits.Load(), rollbacks.Load(), conflicts.Load(), db.indexScans.Load(), db.indexScanRetries.Load())
}

// TestDDLWithConcurrentWriters runs DDL, multi-statement writing
// transactions that also do DDL, and readers at once: the lock order
// (writer slot, then the statement lock) never deadlocks, and every
// statement succeeds.
func TestDDLWithConcurrentWriters(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE base (id int PRIMARY KEY, v int)")
	mustExec(t, db, "INSERT INTO base VALUES (1, 0)")
	const rounds = 30
	errc := make(chan error, 8)
	var wg sync.WaitGroup
	run := func(f func(i int) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range rounds {
				if err := f(i); err != nil {
					errc <- err
					return
				}
			}
		}()
	}
	run(func(i int) error { // DDL in autocommit
		_, err := db.Exec(bg, fmt.Sprintf("CREATE TABLE d%d (a int); CREATE INDEX ON d%d (a); DROP TABLE d%d", i, i, i))
		return err
	})
	var txCommits atomic.Int64
	run(func(i int) error { // DML, then DDL, in one transaction; some roll back
		tx, err := db.Begin(bg)
		if err != nil {
			return err
		}
		for _, s := range []string{
			"UPDATE base SET v = v + 1 WHERE id = 1",
			fmt.Sprintf("CREATE TABLE x%d (a int)", i),
			fmt.Sprintf("INSERT INTO x%d VALUES (%d)", i, i),
		} {
			if _, err := tx.Exec(bg, s); err != nil {
				_ = tx.Rollback(bg)
				return fmt.Errorf("%s: %w", s, err)
			}
		}
		if i%3 == 0 {
			return tx.Rollback(bg)
		}
		txCommits.Add(1)
		return tx.Commit(bg)
	})
	run(func(int) error { // autocommit DML
		_, err := db.Exec(bg, "UPDATE base SET v = v + 1 WHERE id = 1")
		return err
	})
	run(func(int) error { // readers
		_, err := db.Exec(bg, "SELECT v FROM base WHERE id = 1")
		return err
	})
	run(func(int) error { // readers of the tables being created and dropped
		for j := range rounds {
			_, err := db.Exec(bg, fmt.Sprintf("SELECT a FROM d%d WHERE a >= 0", j))
			if err != nil && sqlerr.Code(err) != sqlerr.UndefinedTable {
				return err
			}
		}
		return nil
	})
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(2 * time.Minute):
		t.Fatal("DDL and writers deadlocked")
	}
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	if got := rows(t, db, "SELECT v FROM base"); got != fmt.Sprint(rounds+txCommits.Load()) {
		t.Fatalf("base.v = %s", got)
	}
	checkConsistency(t, db)
}

// TestRollbackWhileReading rolls back writing transactions, which reopens
// the database, while autocommit readers run: they wait for the reopen
// and never fail, and never see the rolled-back rows.
func TestRollbackWhileReading(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v int); CREATE INDEX ON t (v)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 1), (2, 2)")
	var stop atomic.Bool
	errc := make(chan error, 4)
	var rg sync.WaitGroup
	for _, q := range []string{"SELECT id FROM t ORDER BY id", "SELECT id FROM t WHERE v >= 0 ORDER BY id"} {
		rg.Add(1)
		go func() {
			defer rg.Done()
			for !stop.Load() {
				r, err := db.Exec(bg, q)
				if err != nil {
					errc <- fmt.Errorf("%s: %w", q, err)
					return
				}
				if got := resultRows(r[0]); got != "1\n2" {
					errc <- fmt.Errorf("%s: %q", q, got)
					return
				}
			}
		}()
	}
	rb := db.rollbacks.Load()
	for i := range 20 {
		tx := mustBegin(t, db)
		txExec(t, tx, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", 10+i, i))
		txExec(t, tx, "UPDATE t SET v = v + 100 WHERE id = 1")
		if err := tx.Rollback(bg); err != nil {
			t.Fatal(err)
		}
	}
	stop.Store(true)
	rg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	if db.rollbacks.Load()-rb != 20 {
		t.Fatalf("%d rollbacks", db.rollbacks.Load()-rb)
	}
	if got := rows(t, db, "SELECT id, v FROM t ORDER BY id"); got != "1|1\n2|2" {
		t.Fatal(got)
	}
}

// TestOwnWritesKeepIndexes: a transaction's own changes do not stop it
// from reading through indexes (section 2.5).
func TestOwnWritesKeepIndexes(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v int); CREATE INDEX ON t (v)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 10), (2, 20)")
	for _, iso := range []IsolationLevel{ReadCommitted, RepeatableRead} {
		tx, err := db.BeginTx(bg, TxOptions{Isolation: iso})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Rollback(bg) }) // before Close, if the test fails
		txExec(t, tx, "UPDATE t SET v = v + 1 WHERE id = 1")
		before := db.indexScans.Load()
		if got := txRows(t, tx, "SELECT id, v FROM t WHERE v >= 11 ORDER BY id"); got != "1|11\n2|20" {
			t.Fatalf("isolation %d: %q", iso, got)
		}
		if db.indexScans.Load() == before {
			t.Fatalf("isolation %d: a transaction's own change stopped it from using an index", iso)
		}
		if err := tx.Rollback(bg); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCloseWaitsForWritingTx: Close waits for a writing transaction to
// end, which then commits.
func TestCloseWaitsForWritingTx(t *testing.T) {
	m := newFS(t)
	db := openDB(t, m, Options{})
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY)")
	tx := mustBegin(t, db)
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(bg) // lets Close end if the test fails
		}
	}()
	txExec(t, tx, "INSERT INTO t VALUES (1)")
	closed := make(chan error, 1)
	go func() { closed <- db.Close(bg) }()
	select {
	case err := <-closed:
		t.Fatalf("Close did not wait for a writing transaction: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	committed = true
	if err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	db = openDB(t, m, Options{})
	defer func() { _ = db.Close(bg) }()
	if got := rows(t, db, "SELECT id FROM t"); got != "1" {
		t.Fatalf("after reopening: %q", got)
	}
}

// TestIndexScanAfterWriterBegins: a writer that begins after a statement
// planned an index scan, and before the scan, changes the index under it;
// the scan sees that the snapshot misses the writer and reads the table
// instead (section 2.8).
func TestIndexScanAfterWriterBegins(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v int); CREATE INDEX ON t (v)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 10), (2, 20)")
	tx := mustBegin(t, db)
	t.Cleanup(func() { _ = tx.Rollback(bg) })
	fired := false
	db.indexScanHook = func() {
		if !fired {
			fired = true // the UPDATE's own index scan comes back here
			txExec(t, tx, "UPDATE t SET v = 11 WHERE id = 1")
		}
	}
	retries := db.indexScanRetries.Load()
	if got := rows(t, db, "SELECT id FROM t WHERE v = 10"); got != "1" {
		t.Fatalf("a row whose index entry an uncommitted change moved: %q", got)
	}
	db.indexScanHook = nil
	if !fired || db.indexScanRetries.Load() != retries+1 {
		t.Fatalf("setup: hook fired %v, %d retries", fired, db.indexScanRetries.Load()-retries)
	}
}

// TestRestartWhileReading: a statement too large for the pool restarts
// the database (reopening it) while autocommit readers run; they wait for
// the reopen and never fail.
func TestRestartWhileReading(t *testing.T) {
	db := openDB(t, newFS(t), Options{Frames: 24})
	t.Cleanup(func() { _ = db.Close(bg) })
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, pad text); INSERT INTO t VALUES (1, 'one'), (2, 'two')")
	big := "INSERT INTO t VALUES "
	for i := 3; i < 3000; i++ {
		if i > 3 {
			big += ", "
		}
		big += fmt.Sprintf("(%d, '%0200d')", i, i)
	}
	var stop atomic.Bool
	errc := make(chan error, 4)
	var rg sync.WaitGroup
	var reads atomic.Int64
	for _, q := range []string{"SELECT id FROM t ORDER BY id", "SELECT id FROM t WHERE id >= 0 ORDER BY id"} {
		rg.Add(1)
		go func() {
			defer rg.Done()
			for !stop.Load() {
				r, err := db.Exec(bg, q)
				if sqlerr.Code(err) == sqlerr.ProgramLimitExceeded {
					continue // the writer holds most of the pool
				}
				if err != nil {
					errc <- fmt.Errorf("%s: %w", q, err)
					return
				}
				if got := resultRows(r[0]); got != "1\n2" {
					errc <- fmt.Errorf("%s: %q", q, got)
					return
				}
				reads.Add(1)
			}
		}()
	}
	restarts := db.restarts.Load()
	for range 5 {
		expectErr(t, db, big, sqlerr.ProgramLimitExceeded)
	}
	stop.Store(true)
	rg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	if db.restarts.Load()-restarts != 5 || reads.Load() == 0 {
		t.Fatalf("%d restarts, %d reads", db.restarts.Load()-restarts, reads.Load())
	}
	checkConsistency(t, db)
}

// TestDDLTxBlocksReaders: a transaction that ran DDL holds the exclusive
// lock until it ends (the catalog is not versioned): no reader sees a
// table it created before it commits, or at all once it rolls back.
func TestDDLTxBlocksReaders(t *testing.T) {
	db := newDB(t)
	tx := mustBegin(t, db)
	ended := false
	defer func() {
		if !ended {
			_ = tx.Rollback(bg) // lets the reader and Close go on if the test fails
		}
	}()
	txExec(t, tx, "CREATE TABLE n (a int)")
	read := make(chan error, 1)
	go func() {
		_, err := db.Exec(bg, "SELECT a FROM n")
		read <- err
	}()
	select {
	case err := <-read:
		t.Fatalf("a reader ran beside a transaction that created a table: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	ended = true
	if err := tx.Rollback(bg); err != nil {
		t.Fatal(err)
	}
	if err := <-read; sqlerr.Code(err) != sqlerr.UndefinedTable {
		t.Fatalf("reading a table whose creation rolled back: %v", err)
	}
}
