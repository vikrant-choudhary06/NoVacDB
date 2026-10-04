package catalog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"path"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/ast"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/parser"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

var bg = context.Background()

const dir = "/db"

func newFS(t *testing.T) *vfs.MemFS {
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

// db is an engine and its catalog.
type db struct {
	e *wal.Engine
	c *Catalog
}

func open(t *testing.T, m *vfs.MemFS) *db {
	t.Helper()
	e, err := wal.OpenEngine(bg, m, dir, wal.EngineOptions{Frames: 64})
	if err != nil {
		t.Fatal(err)
	}
	c, err := Open(bg, e, m, dir)
	if err != nil {
		t.Fatalf("opening the catalog: %v", err)
	}
	return &db{e, c}
}

func (d *db) close(t *testing.T) {
	t.Helper()
	if err := d.e.Close(bg); err != nil {
		t.Fatal(err)
	}
}

// stmt runs fn in a statement group and commits it. If fn fails the
// engine is abandoned, as the executor does.
func (d *db) stmt(t *testing.T, fn func(xid wal.XID) error) error {
	t.Helper()
	tx := d.e.Begin()
	if err := tx.Write(bg, func(context.Context) error { return fn(tx.XID()) }); err != nil {
		if aerr := d.e.Abandon(); aerr != nil {
			t.Fatal(aerr)
		}
		return err
	}
	if _, err := tx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	return nil
}

func usersDef(t *testing.T) TableDef {
	t.Helper()
	def, _ := parser.ParseExpr("'anon' || 'ymous'")
	return TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.Int8},
			{Name: "name", Type: types.Text, NotNull: true, Default: def},
			{Name: "email", Type: types.Text},
			{Name: "age", Type: types.Int4},
		},
		PrimaryKey: []string{"id"},
		Unique:     [][]string{{"email"}, {"name", "age"}},
	}
}

func describe(t *Table) string {
	var b strings.Builder
	b.WriteString(t.Name + "(")
	for i, c := range t.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c.Name + " " + c.Type.String())
		if c.NotNull {
			b.WriteString(" NOT NULL")
		}
		if c.Default != nil {
			b.WriteString(" DEFAULT " + c.Default.String())
		}
	}
	b.WriteString(")")
	for _, ix := range t.Indexes {
		b.WriteString(" " + ix.Name + "[")
		for i, p := range ix.Columns {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(t.Columns[p].Name)
		}
		b.WriteString("]")
		if ix.Primary {
			b.WriteString("P")
		}
		if ix.Unique {
			b.WriteString("U")
		}
	}
	return b.String()
}

func TestCreateTableAndReload(t *testing.T) {
	m := newFS(t)
	d := open(t, m)
	if len(d.c.Tables()) != 0 {
		t.Fatal("new catalog is not empty")
	}
	var users *Table
	if err := d.stmt(t, func(xid wal.XID) (err error) {
		users, err = d.c.CreateTable(bg, xid, usersDef(t))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	want := "users(id bigint NOT NULL, name text NOT NULL DEFAULT 'anon' || 'ymous', email text, age integer) " +
		"users_pkey[id]PU users_email_key[email]U users_name_age_key[name,age]U"
	if got := describe(users); got != want {
		t.Fatalf("created\n got  %s\n want %s", got, want)
	}
	if err := d.stmt(t, func(xid wal.XID) error {
		_, err := d.c.CreateTable(bg, xid, TableDef{Name: "t2", Columns: []ColumnDef{{Name: "x", Type: types.Bool}, {Name: "ts", Type: types.TimestampTZ}, {Name: "f", Type: types.Float8}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	d.close(t)

	// After a clean reopen, and after a crash, the catalog reads back the same.
	for _, crash := range []bool{false, true} {
		if crash {
			m.Crash(vfs.CrashOptions{TearLast: true})
		}
		d = open(t, m)
		got, ok := d.c.Table("users")
		if !ok || describe(got) != want {
			t.Fatalf("reloaded (crash=%v): %v", crash, describe(got))
		}
		if t2, ok := d.c.Table("t2"); !ok || describe(t2) != "t2(x boolean, ts timestamp with time zone, f double precision)" {
			t.Fatalf("t2 reloaded as %v", describe(t2))
		}
		if ix, ok := d.c.Index("users_email_key"); !ok || ix.Table != got || got.PrimaryKey() == nil || got.PrimaryKey().Name != "users_pkey" {
			t.Fatal("indexes not linked after reload")
		}
		if names := len(d.c.Tables()); names != 2 {
			t.Fatalf("%d tables", names)
		}
		if !crash {
			d.close(t)
		}
	}
}

func TestDDLIsAtomicAcrossCrash(t *testing.T) {
	m := newFS(t)
	d := open(t, m)
	if err := d.stmt(t, func(xid wal.XID) error {
		_, err := d.c.CreateTable(bg, xid, TableDef{Name: "kept", Columns: []ColumnDef{{Name: "a", Type: types.Int4}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// A CREATE TABLE whose transaction never commits.
	tx := d.e.Begin()
	if err := tx.Write(bg, func(context.Context) error {
		_, err := d.c.CreateTable(bg, tx.XID(), usersDef(t))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	m.Crash(vfs.CrashOptions{})
	d = open(t, m)
	defer d.close(t)
	if _, ok := d.c.Table("users"); ok {
		t.Fatal("an uncommitted CREATE TABLE survived the crash")
	}
	for _, name := range []string{"users_pkey", "users_email_key"} {
		if d.c.Exists(name) {
			t.Fatalf("index %s of an uncommitted table survived", name)
		}
	}
	if _, ok := d.c.Table("kept"); !ok {
		t.Fatal("the committed table was lost")
	}
}

func TestNameRules(t *testing.T) {
	m := newFS(t)
	d := open(t, m)
	defer func() { d.close(t) }()
	if err := d.stmt(t, func(xid wal.XID) error {
		_, err := d.c.CreateTable(bg, xid, usersDef(t))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cols := []ColumnDef{{Name: "a", Type: types.Int4}}
	many := make([]ColumnDef, types.MaxColumns+1)
	for i := range many {
		many[i] = ColumnDef{Name: "c" + strings.Repeat("x", i%5) + string(rune('a'+i%26)) + strings.Repeat("y", i/130), Type: types.Int4}
	}
	for i := range many { // make the names unique
		many[i].Name = many[i].Name + "_" + string(rune('0'+i%10)) + string(rune('a'+i/10%26)) + string(rune('a'+i/260))
	}
	cases := []struct {
		def  TableDef
		code string
	}{
		{TableDef{Name: "users", Columns: cols}, sqlerr.DuplicateTable},
		{TableDef{Name: "users_pkey", Columns: cols}, sqlerr.DuplicateTable},
		{TableDef{Name: "novac_x", Columns: cols}, sqlerr.ReservedName},
		{TableDef{Name: "t", Columns: []ColumnDef{{Name: "a", Type: types.Int4}, {Name: "a", Type: types.Text}}}, sqlerr.DuplicateColumn},
		{TableDef{Name: "t", Columns: cols, PrimaryKey: []string{"b"}}, sqlerr.UndefinedColumn},
		{TableDef{Name: "t", Columns: cols, Unique: [][]string{{"a", "a"}}}, sqlerr.DuplicateColumn},
		{TableDef{Name: "t", Columns: many}, sqlerr.TooManyColumns},
		{TableDef{Name: strings.Repeat("n", 64), Columns: cols}, sqlerr.NameTooLong},
		{TableDef{Name: "", Columns: cols}, sqlerr.NameTooLong},
		{TableDef{Name: "t", Columns: []ColumnDef{{Name: strings.Repeat("c", 64), Type: types.Int4}}}, sqlerr.NameTooLong},
		{TableDef{Name: "t", Columns: []ColumnDef{{Name: "a"}}}, sqlerr.InternalError},
		{TableDef{Name: "t", Columns: cols, PrimaryKey: []string{}}, sqlerr.InvalidTableDefinition},
		{TableDef{Name: "t", Columns: []ColumnDef{{Name: "a", Type: types.Text, Default: longDefault}}}, sqlerr.ProgramLimitExceeded},
		{TableDef{Name: "t", Columns: many[:MaxIndexColumns+1], PrimaryKey: names(many[:MaxIndexColumns+1])}, sqlerr.TooManyColumns},
		{TableDef{Name: "t", Columns: many[:MaxIndexColumns+1], Unique: [][]string{names(many[:MaxIndexColumns+1])}}, sqlerr.TooManyColumns},
		// A later constraint's error still comes before any change.
		{TableDef{Name: "t", Columns: cols, PrimaryKey: []string{"a"}, Unique: [][]string{{"a"}, {"zz"}}}, sqlerr.UndefinedColumn},
	}
	before := describeAll(d.c)
	for _, c := range cases {
		// These fail before changing anything: no statement is needed, and
		// the error is a *sqlerr.Error, which promises it.
		_, err := d.c.CreateTable(bg, 0, c.def) // fails before writing
		var se *sqlerr.Error
		if sqlerr.Code(err) != c.code || !errors.As(err, &se) {
			t.Errorf("CreateTable(%.20s): %v, want %s", c.def.Name, err, c.code)
		}
	}
	users, _ := d.c.Table("users")
	for _, c := range []struct {
		name string
		cols []string
		code string
	}{
		{"users", []string{"age"}, sqlerr.DuplicateTable},
		{"i", []string{"nope"}, sqlerr.UndefinedColumn},
		{"i", nil, sqlerr.InvalidTableDefinition},
		{strings.Repeat("i", 64), []string{"age"}, sqlerr.NameTooLong},
		{"novac_i", []string{"age"}, sqlerr.ReservedName},
	} {
		if _, err := d.c.CreateIndex(bg, 0, users, c.name, c.cols, false); sqlerr.Code(err) != c.code { // fails before writing
			t.Errorf("CreateIndex(%.20s, %v): %v, want %s", c.name, c.cols, err, c.code)
		}
	}
	if after := describeAll(d.c); after != before {
		t.Fatalf("failed DDL changed the catalog:\n%s\nwant\n%s", after, before)
	}
	// Nor did anything reach the system tables: a reload sees the same.
	d.close(t)
	d2 := open(t, m)
	if after := describeAll(d2.c); after != before {
		t.Fatalf("after reopening:\n%s\nwant\n%s", after, before)
	}
	d = d2
}

// longDefault is a DEFAULT expression too long for a catalog row.
var longDefault = func() ast.Expr {
	e, err := parser.ParseExpr("'" + strings.Repeat("x", storage.MaxTupleSize) + "'")
	if err != nil {
		panic(err)
	}
	return e
}()

func names(cols []ColumnDef) []string {
	var out []string
	for _, c := range cols {
		out = append(out, c.Name)
	}
	return out
}

func describeAll(c *Catalog) string {
	var b strings.Builder
	for _, t := range c.Tables() {
		b.WriteString(describe(t) + "\n")
	}
	return b.String()
}

func TestIndexNames(t *testing.T) {
	m := newFS(t)
	d := open(t, m)
	defer d.close(t)
	long := strings.Repeat("x", 60)
	var tbl, ltbl *Table
	if err := d.stmt(t, func(xid wal.XID) (err error) {
		tbl, err = d.c.CreateTable(bg, xid, TableDef{Name: "t", Columns: []ColumnDef{{Name: "a", Type: types.Int4}, {Name: "b", Type: types.Int4}}})
		if err == nil {
			ltbl, err = d.c.CreateTable(bg, xid, TableDef{Name: long, Columns: []ColumnDef{{Name: "é" + long[:55], Type: types.Int4}}, PrimaryKey: []string{"é" + long[:55]}})
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var names []string
	for range 3 {
		if err := d.stmt(t, func(xid wal.XID) error {
			ix, err := d.c.CreateIndex(bg, xid, tbl, "", []string{"a", "b"}, false)
			if err == nil {
				names = append(names, ix.Name)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Join(names, " ") != "t_a_b_idx t_a_b_idx1 t_a_b_idx2" {
		t.Fatalf("generated names %v", names)
	}
	pk := ltbl.PrimaryKey().Name
	if len(pk) > 63 || !strings.HasSuffix(pk, "_pkey") || !strings.HasPrefix(pk, "xxx") {
		t.Fatalf("long primary key name %q", pk)
	}
	if err := d.stmt(t, func(xid wal.XID) error {
		ix, err := d.c.CreateIndex(bg, xid, ltbl, "", []string{"é" + long[:55]}, false)
		if err == nil && (len(ix.Name) > 63 || !strings.HasSuffix(ix.Name, "_idx")) {
			t.Errorf("long index name %q", ix.Name)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// insertRow puts a row straight into a table's heap, outside any index.
func insertRow(t *testing.T, tbl *Table, vals ...types.Value) storage.RID {
	t.Helper()
	data, err := types.EncodeRow(vals, tbl.Types())
	if err != nil {
		t.Fatal(err)
	}
	rid, err := tbl.Heap.Insert(bg, data, func(storage.RID, *storage.Version) (storage.RowHeader, error) {
		return storage.RowHeader{XID: 1}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return rid
}

func TestCreateIndexOverExistingRows(t *testing.T) {
	m := newFS(t)
	d := open(t, m)
	defer d.close(t)
	var tbl *Table
	rids := map[storage.RID]int32{}
	if err := d.stmt(t, func(xid wal.XID) (err error) {
		tbl, err = d.c.CreateTable(bg, xid, TableDef{Name: "t", Columns: []ColumnDef{{Name: "a", Type: types.Int4}, {Name: "b", Type: types.Text}}})
		if err != nil {
			return err
		}
		for i := range 200 {
			v := types.NewInt4(int32(i % 50))
			if i%7 == 0 {
				v = types.Null(types.Int4)
			}
			rids[insertRow(t, tbl, v, types.NewText("x"))] = int32(i)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A unique index over duplicates fails and changes nothing.
	pagesBefore := d.e.Pool().Stats()
	_, err := d.c.CreateIndex(bg, 0, tbl, "u", []string{"a"}, true) // fails before writing
	if sqlerr.Code(err) != sqlerr.UniqueViolation || !strings.Contains(sqlerr.From(err).Detail, "Key (a)=(") {
		t.Fatalf("unique index over duplicates: %v", err)
	}
	if d.c.Exists("u") || len(tbl.Indexes) != 0 || d.e.Pool().Stats().Misses != pagesBefore.Misses {
		t.Fatal("the failed CREATE UNIQUE INDEX changed something")
	}
	// A plain index has an entry for every row, NULLs included.
	var ix *Index
	if err := d.stmt(t, func(xid wal.XID) (err error) {
		ix, err = d.c.CreateIndex(bg, xid, tbl, "", []string{"a"}, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	st, err := ix.Tree.Check(bg)
	if err != nil || st.Keys != 200 {
		t.Fatalf("index has %+v, %v", st, err)
	}
	it := ix.Tree.Scan(btree.Bound{}, btree.Bound{})
	var prev []types.Value
	for {
		k, v, ok, err := it.Next(bg)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		rid, err := DecodeRID(v)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := rids[rid]; !ok {
			t.Fatalf("entry for unknown row %v", rid)
		}
		got, err := tbl.Heap.Get(bg, rid)
		if err != nil {
			t.Fatal(err)
		}
		row, _ := types.DecodeRow(got.Data, tbl.Types())
		want, _ := ix.Key(row, rid)
		if string(want) != string(k) {
			t.Fatalf("entry key does not match its row")
		}
		_ = prev
	}
}

func TestUniqueIndexAllowsManyNulls(t *testing.T) {
	m := newFS(t)
	d := open(t, m)
	defer d.close(t)
	if err := d.stmt(t, func(xid wal.XID) error {
		tbl, err := d.c.CreateTable(bg, xid, TableDef{Name: "t", Columns: []ColumnDef{{Name: "a", Type: types.Int4}}})
		if err != nil {
			return err
		}
		insertRow(t, tbl, types.Null(types.Int4))
		insertRow(t, tbl, types.Null(types.Int4))
		insertRow(t, tbl, types.NewInt4(1))
		_, err = d.c.CreateIndex(bg, xid, tbl, "u", []string{"a"}, true)
		return err
	}); err != nil {
		t.Fatalf("unique index over several NULLs: %v", err)
	}
	ix, _ := d.c.Index("u")
	// Every entry carries its row's RID, in unique indexes too (design doc
	// 08 section 2.11); uniqueness is on the columns, which NULLs escape.
	k1, _ := ix.Key([]types.Value{types.NewInt4(1)}, storage.RID{Page: 5, Slot: 1})
	k2, _ := ix.Key([]types.Value{types.NewInt4(1)}, storage.RID{Page: 6, Slot: 2})
	p1, ok1 := ix.UniquePrefix([]types.Value{types.NewInt4(1)})
	if string(k1) == string(k2) || !ok1 || !bytes.HasPrefix(k1, p1) || !bytes.HasPrefix(k2, p1) {
		t.Fatalf("keys %x and %x, unique prefix %x (%v)", k1, k2, p1, ok1)
	}
	if p, ok := ix.UniquePrefix([]types.Value{types.Null(types.Int4)}); ok || p != nil {
		t.Fatal("a NULL has a unique prefix")
	}
	// The index holds the three rows; a lookup by the prefix finds the one.
	if rids, err := ix.Lookup(bg, p1); err != nil || len(rids) != 1 {
		t.Fatalf("Lookup = %v, %v", rids, err)
	}
	if got := ix.DescribeKey(k1); got != "(a)=(1)" {
		t.Fatalf("DescribeKey = %s", got)
	}
}

func TestDropTableAndIndex(t *testing.T) {
	m := newFS(t)
	d := open(t, m)
	var users *Table
	if err := d.stmt(t, func(xid wal.XID) (err error) {
		users, err = d.c.CreateTable(bg, xid, usersDef(t))
		if err != nil {
			return err
		}
		for i := range 300 {
			insertRow(t, users, types.NewInt8(int64(i)), types.NewText(strings.Repeat("n", 100)), types.Null(types.Text), types.NewInt4(int32(i)))
		}
		_, err = d.c.CreateIndex(bg, xid, users, "users_age", []string{"age"}, false)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	pk := users.PrimaryKey()
	if _, err := d.c.DropIndex(bg, 0, pk); sqlerr.Code(err) != sqlerr.DependentObjectsStillExist { // fails before writing
		t.Fatalf("dropping the primary key's index: %v", err)
	}
	age, _ := d.c.Index("users_age")
	agePages, _ := age.Tree.Pages(bg)
	var dropped []uint64
	if err := d.stmt(t, func(xid wal.XID) (err error) {
		dropped, err = d.c.DropIndex(bg, xid, age)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(dropped) != len(agePages) || d.c.Exists("users_age") || len(users.Indexes) != 3 {
		t.Fatalf("DropIndex returned %d pages (tree has %d), indexes %d", len(dropped), len(agePages), len(users.Indexes))
	}
	heapPages := users.Heap.Pages()
	if err := d.stmt(t, func(xid wal.XID) (err error) {
		dropped, err = d.c.DropTable(bg, xid, users)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(dropped) <= len(heapPages) || d.c.Exists("users") || d.c.Exists("users_pkey") || d.c.Exists("users_email_key") {
		t.Fatalf("DropTable returned %d pages, heap had %d; names remain: %v", len(dropped), len(heapPages), d.c.Tables())
	}
	seen := map[uint64]bool{}
	for _, p := range dropped {
		if seen[p] {
			t.Fatalf("page %d returned twice", p)
		}
		seen[p] = true
	}
	d.close(t)
	d = open(t, m)
	defer d.close(t)
	if len(d.c.Tables()) != 0 || d.c.Exists("users_pkey") {
		t.Fatal("dropped objects came back after reopen")
	}
	// The name is free again.
	if err := d.stmt(t, func(xid wal.XID) error {
		_, err := d.c.CreateTable(bg, xid, usersDef(t))
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapInterrupted(t *testing.T) {
	m := newFS(t)
	m.InjectError(vfs.Fault{Op: vfs.OpRename, Name: path.Join(dir, FileName+".tmp")})
	e, err := wal.OpenEngine(bg, m, dir, wal.EngineOptions{Frames: 16})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(bg, e, m, dir); !errors.Is(err, vfs.ErrInjected) {
		t.Fatalf("bootstrap with a failing rename: %v", err)
	}
	m.ClearFaults()
	m.Crash(vfs.CrashOptions{})
	d := open(t, m)
	defer d.close(t)
	if err := d.stmt(t, func(xid wal.XID) error {
		_, err := d.c.CreateTable(bg, xid, TableDef{Name: "t", Columns: []ColumnDef{{Name: "a", Type: types.Int4}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDamagedCatalogFile(t *testing.T) {
	for name, mangle := range map[string]func([]byte) []byte{
		"bad crc":   func(b []byte) []byte { b[20]++; return b },
		"bad magic": func(b []byte) []byte { b[0] = 'X'; return b },
		"bad version, good crc": func(b []byte) []byte {
			b[8] = 2
			return binary.LittleEndian.AppendUint32(b[:36], crc32.Checksum(b[:36], castagnoli))
		},
		"short":               func(b []byte) []byte { return b[:30] },
		"long":                func(b []byte) []byte { return append(b, 0) },
		"bad version and crc": func(b []byte) []byte { b[8] = 2; return b },
	} {
		t.Run(name, func(t *testing.T) {
			m := newFS(t)
			open(t, m).close(t)
			p := path.Join(dir, FileName)
			f, _ := m.OpenFile(p, vfs.ORead)
			buf := make([]byte, 64)
			n, _ := f.ReadAt(buf, 0)
			_ = f.Close()
			b := mangle(buf[:n])
			f, _ = m.OpenFile(p, vfs.ORead|vfs.OWrite|vfs.OTrunc)
			_, _ = f.WriteAt(b, 0)
			_ = f.Sync()
			_ = f.Close()
			e, err := wal.OpenEngine(bg, m, dir, wal.EngineOptions{Frames: 16})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = e.Close(bg) }()
			if _, err := Open(bg, e, m, dir); sqlerr.Code(err) != sqlerr.DataCorrupted {
				t.Fatalf("open with a %s catalog file: %v", name, err)
			}
		})
	}
}

func TestDamagedCatalogRows(t *testing.T) {
	// Each case writes a bad row straight into a system table. Table t has
	// ID 1, one column and a primary key, whose tree root a bad index row
	// gets, so that only the row's own fault can fail the load.
	ixRow := func(id int64, name string, table int64, root uint64, unique, primary bool, cols string) []types.Value {
		return []types.Value{types.NewInt8(id), types.NewText(name), types.NewInt8(table), types.NewInt8(int64(root)), types.NewBool(unique), types.NewBool(primary), types.NewText(cols)}
	}
	cases := map[string]struct {
		which int
		row   func(root uint64) []types.Value
	}{
		"index on a missing table": {sysIndexes, func(r uint64) []types.Value { return ixRow(99, "i", 42, r, false, false, "0") }},
		"columns of a missing table": {sysColumns, func(uint64) []types.Value {
			return []types.Value{types.NewInt8(42), types.NewInt4(0), types.NewText("a"), types.NewInt4(1), types.NewBool(false), types.NewText("")}
		}},
		"bad column type": {sysColumns, func(uint64) []types.Value {
			return []types.Value{types.NewInt8(1), types.NewInt4(1), types.NewText("b"), types.NewInt4(77), types.NewBool(false), types.NewText("")}
		}},
		"gap in positions": {sysColumns, func(uint64) []types.Value {
			return []types.Value{types.NewInt8(1), types.NewInt4(5), types.NewText("b"), types.NewInt4(1), types.NewBool(false), types.NewText("")}
		}},
		"duplicate column": {sysColumns, func(uint64) []types.Value {
			return []types.Value{types.NewInt8(1), types.NewInt4(1), types.NewText("a"), types.NewInt4(1), types.NewBool(false), types.NewText("")}
		}},
		"bad default": {sysColumns, func(uint64) []types.Value {
			return []types.Value{types.NewInt8(1), types.NewInt4(1), types.NewText("b"), types.NewInt4(1), types.NewBool(false), types.NewText("1 +")}
		}},
		"duplicate table name": {sysTables, func(uint64) []types.Value {
			return []types.Value{types.NewInt8(7), types.NewText("t"), types.NewInt8(2)}
		}},
		"duplicate table id": {sysTables, func(uint64) []types.Value {
			return []types.Value{types.NewInt8(1), types.NewText("u"), types.NewInt8(2)}
		}},
		"NULL in a system row": {sysTables, func(uint64) []types.Value {
			return []types.Value{types.NewInt8(7), types.Null(types.Text), types.NewInt8(2)}
		}},
		"bad index column list":     {sysIndexes, func(r uint64) []types.Value { return ixRow(99, "i", 1, r, false, false, "0,x") }},
		"index column out of range": {sysIndexes, func(r uint64) []types.Value { return ixRow(99, "i", 1, r, false, false, "1") }},
		"negative index column":     {sysIndexes, func(r uint64) []types.Value { return ixRow(99, "i", 1, r, false, false, "-1") }},
		"two primary keys":          {sysIndexes, func(r uint64) []types.Value { return ixRow(99, "i", 1, r, true, true, "0") }},
		"duplicate index name":      {sysIndexes, func(r uint64) []types.Value { return ixRow(99, "t_pkey", 1, r, false, false, "0") }},
		"index named like a table":  {sysIndexes, func(r uint64) []types.Value { return ixRow(99, "t", 1, r, false, false, "0") }},
		"index on a heap page":      {sysIndexes, func(uint64) []types.Value { return ixRow(99, "i", 1, 2, false, false, "0") }},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			m := newFS(t)
			d := open(t, m)
			if err := d.stmt(t, func(xid wal.XID) error {
				tbl, err := d.c.CreateTable(bg, xid, TableDef{Name: "t", Columns: []ColumnDef{{Name: "a", Type: types.Int4}}, PrimaryKey: []string{"a"}})
				if err != nil {
					return err
				}
				if tbl.ID != 1 {
					t.Fatalf("table ID %d", tbl.ID)
				}
				_, err = d.c.insertSys(bg, d.c.writer(xid), c.which, c.row(tbl.PrimaryKey().Tree.Root()))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			d.close(t)
			e, err := wal.OpenEngine(bg, m, dir, wal.EngineOptions{Frames: 16})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = e.Close(bg) }()
			if _, err := Open(bg, e, m, dir); sqlerr.Code(err) != sqlerr.DataCorrupted {
				t.Fatalf("open: %v", err)
			}
		})
	}
}

func TestRIDEncoding(t *testing.T) {
	rid := storage.RID{Page: 1<<40 + 3, Slot: 513}
	got, err := DecodeRID(EncodeRID(rid))
	if err != nil || got != rid {
		t.Fatalf("%v %v", got, err)
	}
	for _, b := range [][]byte{{1, 2}, nil, append(EncodeRID(rid), 0)} {
		if _, err := DecodeRID(b); sqlerr.Code(err) != sqlerr.DataCorrupted {
			t.Fatalf("DecodeRID(%x): %v", b, err)
		}
	}
}

func TestDDLUnderIOFailures(t *testing.T) {
	// A statement of several DDL changes, with the n-th write or sync
	// failing. Errors after a change are never *sqlerr.Error (so the caller
	// knows to abandon), and after abandoning and reopening the catalog is
	// exactly as before the statement or exactly as after it.
	want := "" // the catalog after the statement, from a run without faults
	for _, op := range []vfs.Op{vfs.OpWriteAt, vfs.OpSync} {
		failures := 0
		for n := 0; ; n++ {
			if n > 500 {
				t.Fatalf("%v: the statement still fails after %d calls", op, n)
			}
			m := newFS(t)
			d := open(t, m)
			var base *Table
			if err := d.stmt(t, func(xid wal.XID) (err error) {
				if base, err = d.c.CreateTable(bg, xid, TableDef{Name: "base", Columns: []ColumnDef{{Name: "a", Type: types.Int4}, {Name: "b", Type: types.Text}}}); err != nil {
					return err
				}
				for i := range 300 {
					insertRow(t, base, types.NewInt4(int32(i)), types.NewText(strings.Repeat("b", i%50)))
				}
				_, err = d.c.CreateTable(bg, xid, TableDef{Name: "gone", Columns: []ColumnDef{{Name: "x", Type: types.Int8}}, PrimaryKey: []string{"x"}})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			before := describeAll(d.c)
			if want == "" {
				n-- // the reference run
			} else {
				m.InjectError(vfs.Fault{Op: op, After: n})
			}
			atCommit := false
			tx := d.e.Begin()
			err := tx.Write(bg, func(context.Context) error {
				xid := tx.XID()
				if _, err := d.c.CreateTable(bg, xid, usersDef(t)); err != nil {
					return err
				}
				if _, err := d.c.CreateIndex(bg, xid, base, "", []string{"b", "a"}, true); err != nil {
					return err
				}
				gone, _ := d.c.Table("gone")
				_, err := d.c.DropTable(bg, xid, gone)
				return err
			})
			if err == nil {
				atCommit = true
				_, err = tx.Commit(bg)
			}
			if want == "" {
				if err != nil {
					t.Fatal(err)
				}
				want = describeAll(d.c)
				d.close(t)
				continue
			}
			if err == nil {
				if got := describeAll(d.c); got != want {
					t.Fatalf("catalog\n%s\nwant\n%s", got, want)
				}
				m.ClearFaults()
				d.close(t)
				if failures == 0 {
					t.Fatalf("%v: no call ever failed", op)
				}
				t.Logf("%v: %d failing runs", op, failures)
				break
			}
			failures++
			var se *sqlerr.Error
			if errors.As(err, &se) || !errors.Is(err, vfs.ErrInjected) {
				t.Fatalf("%v after %d: error %v (want an injected I/O error, not a SQL error)", op, n, err)
			}
			_ = d.e.Abandon()
			m.ClearFaults()
			m.Crash(vfs.CrashOptions{TearLast: n%2 == 0})
			d = open(t, m)
			got := describeAll(d.c)
			// Only a failure in the commit itself may leave the statement
			// committed.
			if got != before && (got != want || !atCommit) {
				t.Fatalf("%v after %d (commit reached: %v): catalog\n%s\nis neither the state before\n%s\nnor after\n%s", op, n, atCommit, got, before, want)
			}
			for _, tbl := range d.c.Tables() {
				for _, ix := range tbl.Indexes {
					if _, err := ix.Tree.Check(bg); err != nil {
						t.Fatalf("index %s: %v", ix.Name, err)
					}
				}
			}
			d.close(t)
		}
	}
}

func TestIDsStayUniqueAcrossReloads(t *testing.T) {
	m := newFS(t)
	ids := map[int64]string{}
	record := func(c *Catalog) {
		for _, tbl := range c.Tables() {
			for _, x := range append([]struct {
				id   int64
				name string
			}{{tbl.ID, tbl.Name}}, indexIDs(tbl)...) {
				if other, ok := ids[x.id]; ok && other != x.name {
					t.Fatalf("ID %d is used by %s and %s", x.id, other, x.name)
				}
				ids[x.id] = x.name
			}
		}
	}
	for round := range 4 {
		d := open(t, m)
		record(d.c)
		maxID := int64(0)
		for id := range ids {
			maxID = max(maxID, id)
		}
		var tbl *Table
		if err := d.stmt(t, func(xid wal.XID) (err error) {
			// Alternate which kind of object gets the highest ID.
			name := "t" + strconv.Itoa(round)
			if tbl, err = d.c.CreateTable(bg, xid, TableDef{Name: name, Columns: []ColumnDef{{Name: "a", Type: types.Int4}}}); err != nil {
				return err
			}
			if round%2 == 1 {
				_, err = d.c.CreateIndex(bg, xid, tbl, "", []string{"a"}, false)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if tbl.ID <= maxID {
			t.Fatalf("round %d: new table got ID %d, not above the existing %d", round, tbl.ID, maxID)
		}
		record(d.c)
		d.close(t)
	}
	d := open(t, m)
	defer d.close(t)
	record(d.c)
	if len(ids) != 6 {
		t.Fatalf("IDs %v", ids)
	}
}

func indexIDs(tbl *Table) []struct {
	id   int64
	name string
} {
	var out []struct {
		id   int64
		name string
	}
	for _, ix := range tbl.Indexes {
		out = append(out, struct {
			id   int64
			name string
		}{ix.ID, ix.Name})
	}
	return out
}

func TestLoadDoesNotDependOnRowOrder(t *testing.T) {
	// Rows of the system tables can come back in any order once slots are
	// reused: here a table's column and index rows are rewritten in
	// reverse, and the catalog must still load columns by position and
	// indexes in creation order.
	m := newFS(t)
	d := open(t, m)
	var users *Table
	if err := d.stmt(t, func(xid wal.XID) (err error) {
		users, err = d.c.CreateTable(bg, xid, usersDef(t))
		if err == nil {
			_, err = d.c.CreateIndex(bg, xid, users, "", []string{"age"}, false)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	want := describe(users)
	if err := d.stmt(t, func(xid wal.XID) error {
		for _, which := range []int{sysColumns, sysIndexes} {
			var rows [][]types.Value
			if err := d.c.scanSys(bg, which, func(rid storage.RID, row []types.Value) error {
				rows = append(rows, row)
				return d.c.writer(xid).Delete(bg, d.c.sys[which], rid)
			}); err != nil {
				return err
			}
			if len(rows) < 4 {
				t.Fatalf("only %d rows", len(rows))
			}
			for i := len(rows) - 1; i >= 0; i-- {
				if _, err := d.c.insertSys(bg, d.c.writer(xid), which, rows[i]); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	d.close(t)
	d = open(t, m)
	defer d.close(t)
	got, _ := d.c.Table("users")
	if describe(got) != want {
		t.Fatalf("reloaded as\n%s\nwant\n%s", describe(got), want)
	}
}

func TestTablesAreSortedByName(t *testing.T) {
	m := newFS(t)
	d := open(t, m)
	defer d.close(t)
	for _, name := range []string{"b", "c", "a", "ab"} {
		if err := d.stmt(t, func(xid wal.XID) error {
			_, err := d.c.CreateTable(bg, xid, TableDef{Name: name, Columns: []ColumnDef{{Name: "x", Type: types.Int4}}})
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	var names []string
	for _, tbl := range d.c.Tables() {
		names = append(names, tbl.Name)
	}
	if strings.Join(names, " ") != "a ab b c" {
		t.Fatalf("tables %v", names)
	}
}

func TestChosenNamesFitExactly(t *testing.T) {
	m := newFS(t)
	d := open(t, m)
	defer d.close(t)
	pkName := func(table string) string {
		t.Helper()
		var tbl *Table
		if err := d.stmt(t, func(xid wal.XID) (err error) {
			tbl, err = d.c.CreateTable(bg, xid, TableDef{Name: table, Columns: []ColumnDef{{Name: "a", Type: types.Int4}}, PrimaryKey: []string{"a"}})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return tbl.PrimaryKey().Name
	}
	x := strings.Repeat("x", 57)
	for _, c := range []struct{ table, want string }{
		{strings.Repeat("b", 58), strings.Repeat("b", 58) + "_pkey"}, // fits exactly
		{strings.Repeat("a", 63), strings.Repeat("a", 58) + "_pkey"}, // cut to fit 63
		{x + "é", x + "_pkey"},             // the cut would split é
		{x + "éz", x + "_pkey1"},           // the same, and that name is taken
		{"y" + x + "é", "y" + x + "_pkey"}, // é ends exactly at the cut
	} {
		got := pkName(c.table)
		if got != c.want || !utf8.ValidString(got) || len(got) > maxNameLen {
			t.Errorf("primary key of %q: %q, want %q", c.table, got, c.want)
		}
	}
}

func TestKeySizeLimit(t *testing.T) {
	tbl := &Table{Name: "t", Columns: []*Column{{Name: "s", Type: types.Text}}}
	unique := &Index{Name: "u", Table: tbl, Columns: []int{0}, Unique: true}
	plain := &Index{Name: "p", Table: tbl, Columns: []int{0}}
	rid := storage.RID{Page: 3, Slot: 4}
	// A text key takes its length plus 3 bytes, and the RID's 18, in a
	// unique index as in any other.
	for _, c := range []struct {
		ix  *Index
		n   int
		err bool
	}{
		{unique, btree.MaxKeySize - 21, false},
		{unique, btree.MaxKeySize - 20, true},
		{plain, btree.MaxKeySize - 21, false},
		{plain, btree.MaxKeySize - 20, true},
	} {
		key, err := c.ix.Key([]types.Value{types.NewText(strings.Repeat("s", c.n))}, rid)
		if c.err {
			if sqlerr.Code(err) != sqlerr.ProgramLimitExceeded {
				t.Errorf("%s, %d bytes: %v", c.ix.Name, c.n, err)
			}
		} else if err != nil || len(key) > btree.MaxKeySize {
			t.Errorf("%s, %d bytes: %d-byte key, %v", c.ix.Name, c.n, len(key), err)
		}
	}
	if len(btree.AppendInt64(btree.AppendInt64(nil, 1), 2)) != 18 {
		t.Fatal("RID size assumption is wrong")
	}
}

func TestDescribeKey(t *testing.T) {
	cols := []*Column{
		{Name: "i", Type: types.Int4}, {Name: "b", Type: types.Bool}, {Name: "f", Type: types.Float8},
		{Name: "s", Type: types.Text}, {Name: "ts", Type: types.TimestampTZ}, {Name: "n", Type: types.Int8},
	}
	tbl := &Table{Name: "t", Columns: cols}
	ts, err := types.Parse("2001-02-03 04:05:06.5+00", types.TimestampTZ)
	if err != nil {
		t.Fatal(err)
	}
	row := []types.Value{types.NewInt4(-5), types.NewBool(true), types.NewFloat8(1.5), types.NewText("it's"), ts, types.Null(types.Int8)}
	for _, c := range []struct {
		cols []int
		want string
	}{
		{[]int{0, 1, 2, 3, 4, 5}, "(i, b, f, s, ts, n)=(-5, t, 1.5, it's, 2001-02-03 04:05:06.5+00, NULL)"},
		{[]int{4, 3}, "(ts, s)=(2001-02-03 04:05:06.5+00, it's)"},
		{[]int{1}, "(b)=(t)"},
	} {
		ix := &Index{Name: "x", Table: tbl, Columns: c.cols, Unique: true}
		key, err := ix.Key(row, storage.RID{Page: 9, Slot: 9})
		if err != nil {
			t.Fatal(err)
		}
		if got := ix.DescribeKey(key); got != c.want {
			t.Errorf("DescribeKey = %s, want %s", got, c.want)
		}
	}
	ix := &Index{Name: "x", Table: tbl, Columns: []int{0, 1}}
	if got := ix.DescribeKey([]byte{0xff}); got != "(i, b)=(?, ?)" {
		t.Errorf("DescribeKey of a bad key = %s", got)
	}
}
