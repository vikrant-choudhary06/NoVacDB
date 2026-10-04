package executor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/catalog"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

var bg = context.Background()

const dir = "/db"

// fixedNow is the time now() returns in tests.
var fixedNow = time.Date(2024, 5, 6, 7, 8, 9, 500000000, time.UTC)

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
	return seed
}

func newFS(t testing.TB) *vfs.MemFS {
	t.Helper()
	m := vfs.NewMemFS(1)
	if err := m.MkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("/"); err != nil {
		t.Fatal(err)
	}
	return m
}

func openDB(t testing.TB, m *vfs.MemFS, opts Options) *DB {
	t.Helper()
	if opts.Now == nil {
		opts.Now = func() time.Time { return fixedNow }
	}
	if opts.Frames == 0 {
		opts.Frames = 256
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	db, err := Open(bg, m, dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

// newDB opens a fresh database that is closed when the test ends.
func newDB(t testing.TB) *DB {
	t.Helper()
	db := openDB(t, newFS(t), Options{})
	t.Cleanup(func() { _ = db.Close(bg) })
	return db
}

// mustExec runs sql and fails the test on error.
func mustExec(t testing.TB, db *DB, sql string) []*Result {
	t.Helper()
	rs, err := db.Exec(bg, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return rs
}

// query runs one statement and formats its result: the tag, then one line
// per row with values separated by "|" and NULL as "NULL".
func query(t testing.TB, db *DB, sql string) string {
	t.Helper()
	rs := mustExec(t, db, sql)
	if len(rs) != 1 {
		t.Fatalf("%s: %d results", sql, len(rs))
	}
	return format(rs[0])
}

func format(r *Result) string {
	var b strings.Builder
	b.WriteString(r.Tag)
	for _, row := range r.Rows {
		b.WriteString("\n")
		for i, v := range row {
			if i > 0 {
				b.WriteString("|")
			}
			b.WriteString(v.String())
		}
	}
	return b.String()
}

// rows runs a query and returns only its rows, formatted as in query.
func rows(t testing.TB, db *DB, sql string) string {
	t.Helper()
	out := query(t, db, sql)
	if i := strings.IndexByte(out, '\n'); i >= 0 {
		return out[i+1:]
	}
	return ""
}

// expectErr runs sql and requires an error with the code; it returns it.
func expectErr(t testing.TB, db *DB, sql, code string) *sqlerr.Error {
	t.Helper()
	_, err := db.Exec(bg, sql)
	if err == nil {
		t.Fatalf("%s: no error, want %s", sql, code)
	}
	var se *sqlerr.Error
	if !errors.As(err, &se) {
		t.Fatalf("%s: error %T %v is not a *sqlerr.Error", sql, err, err)
	}
	if se.Code != code {
		t.Fatalf("%s: %v, want code %s", sql, err, code)
	}
	return se
}

// checkConsistency checks every table's indexes against its rows: each
// tree is well formed, has one entry per row, and each entry's key is the
// key of the row it points at.
func checkConsistency(t testing.TB, db *DB) {
	t.Helper()
	for _, tbl := range db.cat.Tables() {
		live := map[storage.RID][]types.Value{}
		s := tbl.Heap.Scan()
		for {
			rid, v, ok, err := s.Next(bg)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			row, err := types.DecodeRow(v.Data, tbl.Types())
			if err != nil {
				t.Fatalf("table %s row %v: %v", tbl.Name, rid, err)
			}
			live[rid] = row
		}
		for _, ix := range tbl.Indexes {
			if _, err := ix.Tree.Check(bg); err != nil {
				t.Fatalf("index %s: %v", ix.Name, err)
			}
			n := 0
			it := ix.Tree.Scan(btree.Bound{}, btree.Bound{})
			for {
				k, v, ok, err := it.Next(bg)
				if err != nil {
					t.Fatal(err)
				}
				if !ok {
					break
				}
				n++
				rid, err := catalog.DecodeRID(v)
				if err != nil {
					t.Fatal(err)
				}
				row, ok := live[rid]
				if !ok {
					t.Fatalf("index %s points at %v, which holds no row", ix.Name, rid)
				}
				if want, _ := ix.Key(row, rid); string(want) != string(k) {
					t.Fatalf("index %s: entry %x for row %v, want %x", ix.Name, k, row, want)
				}
			}
			if n != len(live) {
				t.Fatalf("index %s has %d entries for %d rows", ix.Name, n, len(live))
			}
		}
	}
}
