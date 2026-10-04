package catalog

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

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

// rdb is a database under a random DDL workload, with the state its last
// committed statement left: the catalog's description and each table's rows.
type rdb struct {
	t     *testing.T
	rng   *rand.Rand
	m     *vfs.MemFS
	d     *db
	next  int // for unique names and values
	state string
	rows  map[string]int
}

var randomTypes = []types.Type{types.Int4, types.Int8, types.Float8, types.Text, types.Bool, types.TimestampTZ}

// value makes column values unique per row number n, except for booleans
// (two values, so unique indexes over them fail) and some NULLs.
func value(typ types.Type, n int, nullable bool) types.Value {
	if nullable && n%7 == 3 {
		return types.Null(typ)
	}
	switch typ {
	case types.Int4:
		return types.NewInt4(int32(n))
	case types.Int8:
		return types.NewInt8(int64(n) << 33)
	case types.Float8:
		return types.NewFloat8(float64(n) / 4)
	case types.Text:
		return types.NewText(strings.Repeat("v", n%40) + strconv.Itoa(n))
	case types.Bool:
		return types.NewBool(n%2 == 0)
	}
	return types.NewTimestampTZ(int64(n) * 1_000_003)
}

// insert adds up to n rows to a table and its indexes, as the executor
// will, skipping rows that would duplicate a unique key. It returns how many
// it added.
func (r *rdb) insert(xid wal.XID, tbl *Table, n int) (int, error) {
	added := 0
next:
	for range n {
		r.next++
		row := make([]types.Value, len(tbl.Columns))
		for i, c := range tbl.Columns {
			row[i] = value(c.Type, r.next, !c.NotNull)
		}
		for _, ix := range tbl.Indexes {
			if !ix.Unique {
				continue
			}
			key, ok := ix.UniquePrefix(row)
			if !ok {
				continue
			}
			if rids, err := ix.Lookup(bg, key); err != nil {
				return added, err
			} else if len(rids) > 0 {
				continue next
			}
		}
		data, err := types.EncodeRow(row, tbl.Types())
		if err != nil {
			return added, err
		}
		rid, err := r.d.c.writer(xid).Insert(bg, tbl.Heap, data)
		if err != nil {
			return added, err
		}
		for _, ix := range tbl.Indexes {
			key, err := ix.Key(row, rid)
			if err != nil {
				return added, err
			}
			if err := ix.Tree.Insert(bg, key, EncodeRID(rid)); err != nil {
				return added, err
			}
		}
		added++
	}
	return added, nil
}

func (r *rdb) randomTable() *Table {
	ts := r.d.c.Tables()
	if len(ts) == 0 {
		return nil
	}
	return ts[r.rng.IntN(len(ts))]
}

// op runs one random change and returns the pages to free at commit, and
// how the row counts change.
func (r *rdb) op(xid wal.XID, rows map[string]int) ([]uint64, error) {
	c := r.d.c
	switch k := r.rng.IntN(10); {
	case k < 3 || len(c.Tables()) == 0:
		r.next++
		def := TableDef{Name: "t" + strconv.Itoa(r.next)}
		for i := range 1 + r.rng.IntN(4) {
			def.Columns = append(def.Columns, ColumnDef{Name: "c" + strconv.Itoa(i), Type: randomTypes[r.rng.IntN(len(randomTypes))], NotNull: r.rng.IntN(4) == 0})
		}
		if r.rng.IntN(2) == 0 && def.Columns[0].Type != types.Bool {
			def.PrimaryKey = []string{"c0"}
		}
		if n := len(def.Columns); n > 1 && r.rng.IntN(3) == 0 {
			def.Unique = [][]string{{"c" + strconv.Itoa(n-1), "c0"}}
		}
		_, err := c.CreateTable(bg, xid, def)
		if err == nil {
			rows[def.Name] = 0
		}
		return nil, err
	case k < 6:
		tbl := r.randomTable()
		n, err := r.insert(xid, tbl, 1+r.rng.IntN(60))
		if err != nil {
			return nil, err
		}
		rows[tbl.Name] += n
		return nil, nil
	case k < 8:
		tbl := r.randomTable()
		col := tbl.Columns[r.rng.IntN(len(tbl.Columns))].Name
		_, err := c.CreateIndex(bg, xid, tbl, "", []string{col}, r.rng.IntN(2) == 0)
		return nil, err
	case k < 9:
		tbl := r.randomTable()
		if len(tbl.Indexes) == 0 {
			return nil, nil
		}
		return c.DropIndex(bg, xid, tbl.Indexes[r.rng.IntN(len(tbl.Indexes))])
	default:
		tbl := r.randomTable()
		pages, err := c.DropTable(bg, xid, tbl)
		if err == nil {
			delete(rows, tbl.Name)
		}
		return pages, err
	}
}

// statement runs a few random changes in a statement group. A SQL error
// leaves nothing changed and the statement goes on; commit frees the
// dropped pages after its LSN. If crash is set, the statement is left open
// and the database crashes instead.
func (r *rdb) statement(crash bool) (sqlErrors int) {
	t := r.t
	tx := r.d.e.Begin()
	rows := map[string]int{}
	for k, v := range r.rows {
		rows[k] = v
	}
	var free []uint64
	for range 1 + r.rng.IntN(4) {
		var pages []uint64
		err := tx.Write(bg, func(context.Context) error {
			var err error
			pages, err = r.op(tx.XID(), rows)
			var se *sqlerr.Error
			if errors.As(err, &se) {
				return wal.Unchanged(err) // the catalog's rule: nothing changed
			}
			return err
		})
		var se *sqlerr.Error
		switch {
		case errors.As(err, &se):
			sqlErrors++
		case err != nil:
			t.Fatalf("DDL failed: %v", err)
		}
		free = append(free, pages...)
	}
	if crash {
		if r.rng.IntN(2) == 0 {
			if err := r.d.e.Flush(bg); err != nil {
				t.Fatal(err)
			}
		}
		r.m.Crash(vfs.CrashOptions{TearLast: r.rng.IntN(2) == 0})
		r.reopen()
		return sqlErrors
	}
	// Dropped pages are freed later, as the executor does: the request is
	// logged inside the transaction.
	if err := tx.Write(bg, func(context.Context) error { return r.d.e.Logger().DeferFree(bg, free...) }); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	r.state, r.rows = describeAll(r.d.c), rows
	return sqlErrors
}

func (r *rdb) reopen() {
	t := r.t
	e, err := wal.OpenEngine(bg, r.m, dir, wal.EngineOptions{Frames: 32 + r.rng.IntN(64)})
	if err != nil {
		t.Fatal(err)
	}
	c, err := Open(bg, e, r.m, dir)
	if err != nil {
		t.Fatalf("opening the catalog: %v", err)
	}
	r.d = &db{e, c}
	r.verify()
}

// verify checks the catalog and every table and index against the last
// committed state.
func (r *rdb) verify() {
	t := r.t
	if got := describeAll(r.d.c); got != r.state {
		t.Fatalf("catalog\n%s\nwant\n%s", got, r.state)
	}
	for _, tbl := range r.d.c.Tables() {
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
		if len(live) != r.rows[tbl.Name] {
			t.Fatalf("table %s has %d rows, want %d", tbl.Name, len(live), r.rows[tbl.Name])
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
				rid, err := DecodeRID(v)
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

func TestRandomDDLAcrossCrashes(t *testing.T) {
	base := testSeed(t)
	runs := 40
	if testing.Short() {
		runs = 8
	}
	var crashes, sqlErrors, freed int
	for run := range runs {
		seed := base + uint64(run)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			m := vfs.NewMemFS(seed)
			if err := m.MkdirAll(dir); err != nil {
				t.Fatal(err)
			}
			_ = m.SyncDir("/")
			r := &rdb{t: t, rng: rand.New(rand.NewPCG(seed, 44)), m: m, rows: map[string]int{}}
			r.reopen()
			for range 40 {
				switch k := r.rng.IntN(20); {
				case k < 2:
					crashes++
					sqlErrors += r.statement(true)
				case k < 4:
					before := r.d.e.Logger().PendingFrees()
					if _, err := r.d.e.Checkpoint(bg); err != nil {
						t.Fatal(err)
					}
					freed += before - r.d.e.Logger().PendingFrees()
				case k < 5:
					r.d.close(t)
					r.reopen()
				default:
					sqlErrors += r.statement(false)
				}
			}
			r.verify()
			r.d.close(t)
		})
	}
	t.Logf("%d runs: %d crashes in a statement, %d SQL errors inside statements, %d pages freed", runs, crashes, sqlErrors, freed)
	if runs >= 40 && (crashes < runs || sqlErrors < runs/4 || freed == 0) {
		t.Fatal("the workload did not exercise crashes, SQL errors or page reuse")
	}
}
