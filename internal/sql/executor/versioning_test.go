package executor

import (
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

// dataFileSize returns the size of the database's data file.
func dataFileSize(t *testing.T, m *vfs.MemFS) int64 {
	t.Helper()
	f, err := m.OpenFile(path.Join(dir, wal.DataFileName), vfs.ORead)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	n, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// The acceptance test of Step 6.3 (docs/design/15-row-versioning.md): the
// same row updated thousands of times changes in place, so the table keeps
// its pages, and the undo released at each commit is reused, so the data
// file stops growing. Every other update makes the row too big for its
// page, so it also moves away and back.
func TestRepeatedUpdatesDoNotGrowTheTable(t *testing.T) {
	m := newFS(t)
	db := openDB(t, m, Options{Frames: 64, CheckpointBytes: 256 << 10})
	defer func() { _ = db.Close(bg) }()
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v text)")
	for i := range 40 {
		mustExec(t, db, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", i, strings.Repeat("n", 180)))
	}
	tbl, _ := db.cat.Table("t")
	updates := 10000
	if testing.Short() {
		updates = 2000
	}
	var pages int
	var size int64
	for i := range updates {
		v := strings.Repeat(string(rune('a'+i%26)), 100)
		if i%2 == 1 {
			v = strings.Repeat("B", 3000) // too big for its page: moves
		}
		mustExec(t, db, fmt.Sprintf("UPDATE t SET v = '%s' WHERE id = 7", v))
		if i == 500 {
			pages, size = tbl.Heap.NumPages(), dataFileSize(t, m)
		}
	}
	if got := tbl.Heap.NumPages(); got != pages {
		t.Fatalf("table grew from %d to %d pages", pages, got)
	}
	// A checkpoint frees the last released undo pages; the file never
	// shrinks, but must not have grown after the first 500 updates by more
	// than a few pages (free-list order varies).
	if _, err := db.e.Checkpoint(bg); err != nil {
		t.Fatal(err)
	}
	if got := dataFileSize(t, m); got > size+8*storage.PageSize {
		t.Fatalf("data file grew from %d to %d bytes", size, got)
	}
	// The last update (an odd one) left the long value.
	if got := query(t, db, "SELECT length(v) FROM t WHERE id = 7"); !strings.Contains(got, "3000") {
		t.Fatalf("row after the updates: %s", got)
	}
	checkConsistency(t, db)
}

// Rows up to MaxRowData bytes are stored; one byte more is refused with
// 54000, whatever the statement.
func TestRowSizeLimit(t *testing.T) {
	db := openDB(t, newFS(t), Options{})
	defer func() { _ = db.Close(bg) }()
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, v text)")
	// The encoded row adds a few bytes to the text: find the longest text
	// that fits.
	fits := -1
	for n := storage.MaxRowData - 64; n <= storage.MaxRowData; n++ {
		_, err := db.Exec(bg, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", n, strings.Repeat("x", n)))
		if err != nil {
			if sqlerr.Code(err) != sqlerr.ProgramLimitExceeded {
				t.Fatalf("text of %d bytes: %v", n, err)
			}
			break
		}
		fits = n
	}
	if fits < 0 || fits == storage.MaxRowData {
		t.Fatalf("longest text that fits: %d", fits)
	}
	expectErr(t, db, fmt.Sprintf("INSERT INTO t VALUES (1, '%s')", strings.Repeat("x", fits+1)), sqlerr.ProgramLimitExceeded)
	mustExec(t, db, "INSERT INTO t VALUES (1, 'small')")
	expectErr(t, db, fmt.Sprintf("UPDATE t SET v = '%s' WHERE id = 1", strings.Repeat("x", fits+1)), sqlerr.ProgramLimitExceeded)
	mustExec(t, db, fmt.Sprintf("UPDATE t SET v = '%s' WHERE id = 1", strings.Repeat("y", fits)))
	if got := query(t, db, "SELECT length(v) FROM t WHERE id = 1"); !strings.Contains(got, fmt.Sprint(fits)) {
		t.Fatalf("length after update: %s", got)
	}
}

// Every row write in a transaction leaves an undo record naming the
// transaction, the table and the row, until the transaction commits.
func TestWritesLeaveUndoUntilCommit(t *testing.T) {
	db := openDB(t, newFS(t), Options{})
	defer func() { _ = db.Close(bg) }()
	mustExec(t, db, "CREATE TABLE a (id int PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE b (id int PRIMARY KEY, v text)")
	tbl, _ := db.cat.Table("b")
	tx, err := db.Begin(bg)
	if err != nil {
		t.Fatal(err)
	}
	// Runs before the deferred Close if the test fails mid-transaction:
	// the transaction holds the lock Close waits for.
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(bg)
		}
	}()
	for _, q := range []string{"INSERT INTO b VALUES (1, 'x')", "UPDATE b SET v = 'y' WHERE id = 1", "DELETE FROM b WHERE id = 1"} {
		if _, err := tx.Exec(bg, q); err != nil {
			t.Fatal(err)
		}
	}
	xid := uint64(tx.wtx.XID())
	var kinds []string
	for p := db.e.Undo().Last(xid); p != 0; {
		rec, err := db.e.Undo().Read(bg, p)
		if err != nil {
			t.Fatal(err)
		}
		if rec.XID != xid || rec.Table != uint64(tbl.ID) {
			t.Fatalf("undo record of transaction %d, table %d; want %d, %d", rec.XID, rec.Table, xid, tbl.ID)
		}
		kinds = append(kinds, rec.Kind.String())
		p = rec.PrevInTxn
	}
	if got := strings.Join(kinds, ","); got != "delete,update,insert" {
		t.Fatalf("undo records newest first: %s", got)
	}
	committed = true
	if err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	if segs := db.e.Undo().Segments(); len(segs) != 0 {
		t.Fatalf("undo kept after commit: %v", segs)
	}
}
