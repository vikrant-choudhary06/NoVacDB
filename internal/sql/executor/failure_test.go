package executor

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

const failSetup = `CREATE TABLE other (x int, pad text);
	CREATE TABLE t (id int PRIMARY KEY, v text, n int);
	CREATE INDEX ON t (n, v)`

// failData fills the tables, in statements of 40 rows: the tests replay it
// for every injected fault, and what they examine is the statement after
// it. The pages it changes stay dirty in the small pool (failOpts), so the
// statement under test still has to evict them.
func failData(t *testing.T, db *DB) {
	t.Helper()
	insert := func(n int, row func(i int) string, table string) {
		for lo := 0; lo < n; lo += 40 {
			var vals []string
			for i := lo; i < min(lo+40, n); i++ {
				vals = append(vals, row(i))
			}
			mustExec(t, db, "INSERT INTO "+table+" VALUES "+strings.Join(vals, ", "))
		}
	}
	insert(200, func(i int) string { return fmt.Sprintf("(%d, '%0300d')", i, i) }, "other")
	insert(120, func(i int) string { return fmt.Sprintf("(%d, 'v%d', %d)", i, i, i%7) }, "t")
}

func TestSelfRestartAfterIOFailure(t *testing.T) {
	// A statement whose n-th write or sync fails: the client gets an I/O
	// error, the database restarts itself and keeps working, and the
	// statement is entirely absent (or, if only its commit failed, may be
	// entirely present), in memory and after a crash.
	statements := []string{
		"UPDATE t SET v = v || '!', n = n + 1 WHERE n < 5",
		"INSERT INTO t VALUES (1000, 'a', 1), (1001, 'b', 2), (1002, 'c', 3)",
		"DELETE FROM t WHERE id % 3 = 0",
		"CREATE INDEX ON t (v)",
		"DROP TABLE other",
		"UPDATE other SET pad = pad || pad WHERE x % 2 = 0",
	}
	for _, sql := range statements {
		for _, op := range []vfs.Op{vfs.OpWriteAt, vfs.OpSync} {
			t.Run(fmt.Sprintf("%s/%v", sql, op), func(t *testing.T) {
				// The state after the statement, from a run without faults.
				ref := openDB(t, newFS(t), failOpts(0))
				mustExec(t, ref, failSetup)
				failData(t, ref)
				before := dump(t, ref)
				mustExec(t, ref, sql)
				after := dump(t, ref)
				_ = ref.Close(bg)

				failures, kept := 0, 0
				for n := 0; ; n++ {
					if n > 400 {
						t.Fatal("the statement still fails after 400 calls")
					}
					m := newFS(t)
					db := openDB(t, m, failOpts(1<<40))
					mustExec(t, db, failSetup)
					failData(t, db)
					m.InjectError(vfs.Fault{Op: op, After: n})
					_, err := db.Exec(bg, sql)
					m.ClearFaults()
					if err == nil {
						if got := dump(t, db); got != after {
							t.Fatalf("n=%d: succeeded with\n%s\nwant\n%s", n, got, after)
						}
						_ = db.Close(bg)
						if failures == 0 {
							t.Fatal("no call ever failed")
						}
						t.Logf("%d failing runs, %d of them kept the statement", failures, kept)
						return
					}
					failures++
					var se *sqlerr.Error
					if !errors.As(err, &se) || se.Code != sqlerr.IOError || !errors.Is(err, vfs.ErrInjected) {
						t.Fatalf("n=%d: %v (want an I/O error wrapping the injected fault)", n, err)
					}
					got := dump(t, db) // the restarted database works
					if got != before && got != after {
						t.Fatalf("n=%d: after the failure:\n%s", n, got)
					}
					if got == after && before != after {
						kept++
					}
					checkConsistency(t, db)
					// A crash now keeps what the restarted database showed.
					m.Crash(vfs.CrashOptions{TearLast: n%2 == 0})
					db2 := openDB(t, m, failOpts(0))
					if again := dump(t, db2); again != got {
						t.Fatalf("n=%d: after a crash:\n%s\nwant\n%s", n, again, got)
					}
					checkConsistency(t, db2)
					mustExec(t, db2, "INSERT INTO t VALUES (5000, 'z', 0)")
					_ = db2.Close(bg)
				}
			})
		}
	}
}

// failOpts is a small pool, so that a statement evicts committed pages,
// and small WAL segments, so that it fills several: both make I/O happen in
// the middle of statements, not only at their commit.
func failOpts(checkpointBytes int64) Options {
	return Options{Frames: 20, CheckpointBytes: checkpointBytes, WAL: wal.Options{SegmentSize: 8192}}
}

// dump lists every table's rows, sorted.
func dump(t *testing.T, db *DB) string {
	t.Helper()
	out := ""
	for _, tbl := range db.cat.Tables() {
		out += tbl.Name + ":\n" + sortedRows(t, db, "SELECT * FROM "+tbl.Name) + "\n"
		for _, ix := range tbl.Indexes {
			out += " index " + ix.Name + "\n"
		}
	}
	return out
}

func TestStatementTooLarge(t *testing.T) {
	m := newFS(t)
	db := openDB(t, m, Options{Frames: 16})
	defer func() { _ = db.Close(bg) }()
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, pad text); INSERT INTO t VALUES (1, 'one')")
	sql := "INSERT INTO t VALUES "
	for i := 2; i < 3000; i++ {
		if i > 2 {
			sql += ", "
		}
		sql += fmt.Sprintf("(%d, '%0200d')", i, i)
	}
	e := expectErr(t, db, sql, sqlerr.ProgramLimitExceeded)
	if e.Message != "statement changes too much data" || e.Hint == "" {
		t.Fatalf("%+v", e)
	}
	if got := query(t, db, "SELECT * FROM t"); got != "SELECT 1\n1|one" {
		t.Fatalf("after the failed statement: %q", got)
	}
	mustExec(t, db, "INSERT INTO t VALUES (2, 'two')")
	checkConsistency(t, db)
}

func TestFailedRestartLeavesDatabaseUnavailable(t *testing.T) {
	m := newFS(t)
	db := openDB(t, m, Options{Frames: 32})
	mustExec(t, db, "CREATE TABLE t (a int); INSERT INTO t VALUES (1)")
	// The statement's commit fails, and so does reopening the data file.
	m.InjectError(vfs.Fault{Op: vfs.OpSync})
	m.InjectError(vfs.Fault{Op: vfs.OpOpenFile, Name: path.Join(dir, "data")})
	expectErr(t, db, "INSERT INTO t VALUES (2)", sqlerr.IOError)
	m.ClearFaults()
	e := expectErr(t, db, "SELECT * FROM t", sqlerr.IOError)
	if e.Message[:len("the database is unavailable")] != "the database is unavailable" {
		t.Fatalf("%v", e)
	}
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	expectErr(t, db, "SELECT 1", sqlerr.ObjectNotInPrerequisiteState)
	// The files are intact: opening again works.
	db = openDB(t, m, Options{})
	defer func() { _ = db.Close(bg) }()
	if got := rows(t, db, "SELECT a FROM t ORDER BY a"); got != "1" && got != "1\n2" {
		t.Fatalf("%q", got)
	}
}

// countingCtx is cancelled after its Err method has been called n times.
type countingCtx struct {
	context.Context
	n atomic.Int64
}

func (c *countingCtx) Err() error {
	if c.n.Add(-1) < 0 {
		return context.Canceled
	}
	return nil
}

func TestCancellation(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (a int, b text)")
	for i := range 20 {
		sql := "INSERT INTO t VALUES "
		for j := range 30 {
			if j > 0 {
				sql += ", "
			}
			sql += fmt.Sprintf("(%d, 'x%d')", i, j)
		}
		mustExec(t, db, sql)
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if _, err := db.Exec(ctx, "SELECT 1"); sqlerr.Code(err) != sqlerr.QueryCanceled {
		t.Fatalf("cancelled before: %v", err)
	}
	// Cancelled during the scan of a SELECT, and of an UPDATE's read phase
	// (the 600 rows outlast the first check, at 256): the UPDATE changes
	// nothing.
	for _, sql := range []string{"SELECT * FROM t ORDER BY a", "UPDATE t SET a = a + 1", "DELETE FROM t"} {
		c := &countingCtx{Context: bg}
		c.n.Store(1)
		if _, err := db.Exec(c, sql); sqlerr.Code(err) != sqlerr.QueryCanceled {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	if got := rows(t, db, "SELECT a FROM t ORDER BY a DESC LIMIT 1"); got != "19" {
		t.Fatalf("a cancelled UPDATE changed rows: max is %s", got)
	}
	if r := mustExec(t, db, "SELECT a FROM t")[0]; len(r.Rows) != 600 {
		t.Fatalf("a cancelled DELETE removed rows: %d left", len(r.Rows))
	}
}

func TestCheckpointsFollowWALGrowth(t *testing.T) {
	m := newFS(t)
	db := openDB(t, m, Options{Frames: 64, CheckpointBytes: 16 << 10})
	defer func() { _ = db.Close(bg) }()
	start := db.lastCkpt
	mustExec(t, db, "CREATE TABLE gone (a int, pad text); CREATE TABLE t (a int)")
	for i := range 50 {
		mustExec(t, db, fmt.Sprintf("INSERT INTO gone VALUES (%d, '%0300d')", i, i))
	}
	if db.lastCkpt == start {
		t.Fatal("no checkpoint after 15 KiB of rows")
	}
	gone, _ := db.cat.Table("gone")
	dropped := uint64(len(gone.Heap.Pages()))
	free := db.e.FreePageCount()
	mustExec(t, db, "DROP TABLE gone")
	// The pages are freed by the first checkpoint whose redo point is past
	// the drop: perhaps the one its own commit triggered, else this one.
	if _, err := db.e.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	if got := db.e.FreePageCount() - free; got != dropped || db.e.Logger().PendingFrees() != 0 {
		t.Fatalf("%d pages freed and %d pending; the dropped table had %d", got, db.e.Logger().PendingFrees(), dropped)
	}
	for i := range 100 {
		mustExec(t, db, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	if n := db.e.Logger().PendingFrees(); n != 0 {
		t.Fatalf("%d pages of the dropped table were never freed", n)
	}
	checkConsistency(t, db)
}

func TestConcurrentReadersSeeWholeStatements(t *testing.T) {
	// Writers move amounts between accounts in single UPDATE statements and
	// insert and delete rows; readers must always see the same total.
	db := newDB(t)
	const accounts, total = 20, 20000
	mustExec(t, db, "CREATE TABLE acct (id int PRIMARY KEY, bal bigint NOT NULL); CREATE INDEX ON acct (bal)")
	for i := range accounts {
		mustExec(t, db, fmt.Sprintf("INSERT INTO acct VALUES (%d, %d)", i, total/accounts))
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	errc := make(chan error, 8)
	for w := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 7))
			for i := 0; i < 300; i++ {
				from, to, amt := rng.IntN(accounts), rng.IntN(accounts), rng.IntN(50)
				sql := fmt.Sprintf("UPDATE acct SET bal = bal + CASE id WHEN %d THEN -%d ELSE %d END WHERE id IN (%d, %d) AND %d <> %d",
					from, amt, amt, from, to, from, to)
				if i%10 == 0 {
					id := 1000 + w*1000 + i
					sql = fmt.Sprintf("INSERT INTO acct VALUES (%d, 0); DELETE FROM acct WHERE id = %d", id, id)
				}
				if _, err := db.Exec(bg, sql); err != nil {
					errc <- err
					return
				}
			}
		}()
	}
	var reads atomic.Int64
	var rg sync.WaitGroup
	for range 3 {
		rg.Add(1)
		go func() {
			defer rg.Done()
			for !stop.Load() {
				r, err := db.Exec(bg, "SELECT bal FROM acct")
				if err != nil {
					errc <- err
					return
				}
				sum := int64(0)
				for _, row := range r[0].Rows {
					sum += row[0].I
				}
				if sum != total {
					errc <- fmt.Errorf("a reader saw a total of %d", sum)
					return
				}
				r2, err := db.Exec(bg, "SELECT id FROM acct WHERE bal >= -1000000 ORDER BY id")
				if err != nil {
					errc <- err
					return
				}
				if n := len(r2[0].Rows); n < accounts || n > accounts+2 {
					errc <- fmt.Errorf("an index scan saw %d accounts", n)
					return
				}
				reads.Add(1)
			}
		}()
	}
	wg.Wait() // the writers
	stop.Store(true)
	rg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	if reads.Load() == 0 {
		t.Fatal("the readers never ran")
	}
	checkConsistency(t, db)
	t.Logf("%d consistent reads", reads.Load())
}

func TestFailedCheckpointKeepsTheStatement(t *testing.T) {
	m := newFS(t)
	db := openDB(t, m, Options{Frames: 64, CheckpointBytes: 1})
	mustExec(t, db, "CREATE TABLE t (a int)")
	// The checkpoint after this INSERT cannot write the control file; the
	// INSERT has committed and succeeds.
	m.InjectError(vfs.Fault{Op: vfs.OpRename, Name: path.Join(dir, wal.ControlFileName+".tmp")})
	before := db.lastCkpt
	mustExec(t, db, "INSERT INTO t VALUES (1)")
	if db.lastCkpt != before {
		t.Fatal("a failed checkpoint was recorded")
	}
	mustExec(t, db, "INSERT INTO t VALUES (2)") // its checkpoint works
	if db.lastCkpt == before {
		t.Fatal("no checkpoint after the fault cleared")
	}
	m.Crash(vfs.CrashOptions{})
	db = openDB(t, m, Options{})
	defer func() { _ = db.Close(bg) }()
	if got := rows(t, db, "SELECT a FROM t ORDER BY a"); got != "1\n2" {
		t.Fatalf("%q", got)
	}
}

func TestOpenFailures(t *testing.T) {
	m := newFS(t)
	db := openDB(t, m, Options{})
	if err := db.Close(bg); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(bg); sqlerr.Code(err) != sqlerr.ObjectNotInPrerequisiteState {
		t.Fatalf("second close: %v", err)
	}
	// A damaged catalog file stops the database from opening.
	f, err := m.OpenFile(path.Join(dir, "catalog"), vfs.ORead|vfs.OWrite)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("X"), 0); err != nil {
		t.Fatal(err)
	}
	_ = f.Sync()
	_ = f.Close()
	if _, err := Open(bg, m, dir, Options{}); sqlerr.Code(err) != sqlerr.DataCorrupted {
		t.Fatalf("open with a damaged catalog: %v", err)
	}
}

func TestDroppedPagesAreFreedAfterACrash(t *testing.T) {
	m := newFS(t)
	db := openDB(t, m, Options{Frames: 64, CheckpointBytes: 1 << 40})
	mustExec(t, db, "CREATE TABLE gone (a int PRIMARY KEY, pad text)")
	for i := range 40 {
		mustExec(t, db, fmt.Sprintf("INSERT INTO gone VALUES (%d, '%0300d')", i, i))
	}
	if _, err := db.e.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	gone, _ := db.cat.Table("gone")
	pages, err := gone.PrimaryKey().Tree.Pages(bg)
	if err != nil {
		t.Fatal(err)
	}
	dropped := uint64(len(gone.Heap.Pages()) + len(pages))
	free := db.e.FreePageCount()
	mustExec(t, db, "DROP TABLE gone")
	// The DROP's undo records may take pages off the free list; nothing may
	// be freed before a checkpoint.
	if db.e.FreePageCount() > free {
		t.Fatal("pages were freed before a checkpoint")
	}
	// The power goes before any checkpoint: the request is in the log.
	m.Crash(vfs.CrashOptions{TearLast: true})
	db = openDB(t, m, Options{})
	defer func() { _ = db.Close(bg) }()
	if got := db.e.FreePageCount() - free; got != dropped {
		t.Fatalf("%d pages freed after recovery; the dropped table had %d", got, dropped)
	}
	expectErr(t, db, "SELECT * FROM gone", sqlerr.UndefinedTable)
}
