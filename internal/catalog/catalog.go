// Package catalog keeps NoVacDB's tables, columns and indexes in system
// tables inside the database, and loads them at startup. Every change runs
// inside the caller's WAL statement group, so it is atomic across a crash.
// See docs/design/10-executor.md, section 2.5.
package catalog

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/ast"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/parser"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

// ReservedPrefix starts the names of system tables; user tables and
// indexes may not use it.
const ReservedPrefix = "novac_"

// System tables and their row formats.
const (
	sysTables = iota
	sysColumns
	sysIndexes
	numSys
)

var sysNames = [numSys]string{"novac_tables", "novac_columns", "novac_indexes"}

var sysTypes = [numSys][]types.Type{
	// id, name, heap
	{types.Int8, types.Text, types.Int8},
	// table_id, position, name, type, not_null, default_expr
	{types.Int8, types.Int4, types.Text, types.Int4, types.Bool, types.Text},
	// id, name, table_id, root, is_unique, is_primary, columns
	{types.Int8, types.Text, types.Int8, types.Int8, types.Bool, types.Bool, types.Text},
}

// Column is a table column.
type Column struct {
	Name    string
	Type    types.Type
	NotNull bool
	Default ast.Expr // nil means NULL
	rid     storage.RID
}

// Table is a user table.
type Table struct {
	ID      int64
	Name    string
	Heap    *storage.Heap
	Columns []*Column
	Indexes []*Index // in creation order
	rid     storage.RID
}

// ColumnIndex returns the position of the named column, or -1.
func (t *Table) ColumnIndex(name string) int {
	for i, c := range t.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// Types returns the column types in order.
func (t *Table) Types() []types.Type {
	ts := make([]types.Type, len(t.Columns))
	for i, c := range t.Columns {
		ts[i] = c.Type
	}
	return ts
}

// Index is a B+Tree index on a table.
type Index struct {
	ID      int64
	Name    string
	Table   *Table
	Columns []int // positions in the table
	Unique  bool
	Primary bool
	Tree    *btree.Tree
	rid     storage.RID
}

// Catalog is the set of tables and indexes. It is not safe for concurrent
// changes: the executor changes it only under its exclusive lock, and
// reads it under the shared one.
type Catalog struct {
	e       *wal.Engine
	sys     [numSys]*storage.Heap
	tables  map[string]*Table
	indexes map[string]*Index
	nextID  int64
}

// Open loads the catalog of the database in dir, creating it if the
// database is new. The engine must have finished recovery.
func Open(ctx context.Context, e *wal.Engine, fsys vfs.FS, dir string) (*Catalog, error) {
	c := &Catalog{e: e, tables: map[string]*Table{}, indexes: map[string]*Index{}, nextID: 1}
	firsts, ok, err := readFile(fsys, dir)
	if err != nil {
		return nil, err
	}
	if !ok {
		if err := c.bootstrap(ctx, fsys, dir); err != nil {
			return nil, fmt.Errorf("creating the catalog: %w", err)
		}
		return c, nil
	}
	for i, first := range firsts {
		if c.sys[i], err = e.OpenHeap(ctx, first); err != nil {
			return nil, corruptCatalog("opening %s: %v", sysNames[i], err)
		}
	}
	if err := c.load(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

// bootstrap creates the system tables in one transaction, then records
// them in the catalog file.
func (c *Catalog) bootstrap(ctx context.Context, fsys vfs.FS, dir string) error {
	var firsts [numSys]uint64
	tx := c.e.Begin()
	err := tx.Write(ctx, func(ctx context.Context) error {
		for i := range c.sys {
			h, err := c.e.CreateHeap(ctx)
			if err != nil {
				return err
			}
			c.sys[i], firsts[i] = h, h.FirstPage()
		}
		return nil
	})
	if err != nil {
		return err // the caller abandons the engine
	}
	if _, err := tx.Commit(ctx); err != nil {
		return err
	}
	return writeFile(fsys, dir, firsts)
}

func corruptCatalog(format string, args ...any) *sqlerr.Error {
	return sqlerr.New(sqlerr.DataCorrupted, "corrupt catalog: %s", fmt.Sprintf(format, args...))
}

// scanSys calls fn with every row of a system table.
func (c *Catalog) scanSys(ctx context.Context, which int, fn func(rid storage.RID, row []types.Value) error) error {
	s := c.sys[which].Scan()
	for {
		rid, v, ok, err := s.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		row, err := types.DecodeRow(v.Data, sysTypes[which])
		if err != nil {
			return corruptCatalog("%s row %s: %v", sysNames[which], rid, err)
		}
		for i, v := range row {
			if v.Null {
				return corruptCatalog("%s row %s: column %d is NULL", sysNames[which], rid, i)
			}
		}
		if err := fn(rid, row); err != nil {
			return err
		}
	}
}

func (c *Catalog) load(ctx context.Context) error {
	byID := map[int64]*Table{}
	err := c.scanSys(ctx, sysTables, func(rid storage.RID, r []types.Value) error {
		t := &Table{ID: r[0].I, Name: r[1].S, rid: rid}
		if _, dup := byID[t.ID]; dup {
			return corruptCatalog("table id %d appears twice", t.ID)
		}
		h, err := c.e.OpenHeap(ctx, uint64(r[2].I))
		if err != nil {
			return corruptCatalog("table %q: opening its heap: %v", t.Name, err)
		}
		t.Heap = h
		byID[t.ID] = t
		if err := c.register(t.Name, t, nil); err != nil {
			return err
		}
		c.bumpID(t.ID)
		return nil
	})
	if err != nil {
		return err
	}
	type colRow struct {
		pos int
		col *Column
	}
	cols := map[int64][]colRow{}
	err = c.scanSys(ctx, sysColumns, func(rid storage.RID, r []types.Value) error {
		typ := types.Type(r[3].I)
		if !typ.Valid() {
			return corruptCatalog("column %q has type %d", r[2].S, r[3].I)
		}
		col := &Column{Name: r[2].S, Type: typ, NotNull: r[4].Bool(), rid: rid}
		if src := r[5].S; src != "" {
			e, err := parser.ParseExpr(src)
			if err != nil {
				return corruptCatalog("default of column %q: %v", col.Name, err)
			}
			col.Default = e
		}
		cols[r[0].I] = append(cols[r[0].I], colRow{int(r[1].I), col})
		return nil
	})
	if err != nil {
		return err
	}
	for id, cs := range cols {
		t, ok := byID[id]
		if !ok {
			return corruptCatalog("columns of missing table %d", id)
		}
		sort.Slice(cs, func(i, j int) bool { return cs[i].pos < cs[j].pos })
		for i, cr := range cs {
			if cr.pos != i {
				return corruptCatalog("table %q: column positions are not 0..%d", t.Name, len(cs)-1)
			}
			if t.ColumnIndex(cr.col.Name) >= 0 {
				return corruptCatalog("table %q: column %q appears twice", t.Name, cr.col.Name)
			}
			t.Columns = append(t.Columns, cr.col)
		}
	}
	var idxs []*Index
	err = c.scanSys(ctx, sysIndexes, func(rid storage.RID, r []types.Value) error {
		t, ok := byID[r[2].I]
		if !ok {
			return corruptCatalog("index %q on missing table %d", r[1].S, r[2].I)
		}
		ix := &Index{ID: r[0].I, Name: r[1].S, Table: t, Unique: r[4].Bool(), Primary: r[5].Bool(), rid: rid}
		for _, f := range strings.Split(r[6].S, ",") {
			p, err := strconv.Atoi(f)
			if err != nil || p < 0 || p >= len(t.Columns) {
				return corruptCatalog("index %q has column list %q", ix.Name, r[6].S)
			}
			ix.Columns = append(ix.Columns, p)
		}
		tr, err := c.e.OpenBTree(ctx, uint64(r[3].I))
		if err != nil {
			return corruptCatalog("index %q: opening its tree: %v", ix.Name, err)
		}
		ix.Tree = tr
		if err := c.register(ix.Name, nil, ix); err != nil {
			return err
		}
		c.bumpID(ix.ID)
		idxs = append(idxs, ix)
		return nil
	})
	if err != nil {
		return err
	}
	// Indexes in creation (ID) order on each table.
	sort.Slice(idxs, func(i, j int) bool { return idxs[i].ID < idxs[j].ID })
	for _, ix := range idxs {
		if ix.Primary && primaryKey(ix.Table) != nil {
			return corruptCatalog("table %q has two primary keys", ix.Table.Name)
		}
		ix.Table.Indexes = append(ix.Table.Indexes, ix)
	}
	return nil
}

// register adds a relation name, which must be new.
func (c *Catalog) register(name string, t *Table, ix *Index) error {
	if c.Exists(name) {
		return corruptCatalog("relation %q appears twice", name)
	}
	if t != nil {
		c.tables[name] = t
	} else {
		c.indexes[name] = ix
	}
	return nil
}

func (c *Catalog) bumpID(id int64) {
	if id >= c.nextID {
		c.nextID = id + 1
	}
}

func (c *Catalog) newID() int64 {
	id := c.nextID
	c.nextID++
	return id
}

// Exists reports whether a table or index has the name.
func (c *Catalog) Exists(name string) bool {
	_, t := c.tables[name]
	_, i := c.indexes[name]
	return t || i
}

// Table returns the named table.
func (c *Catalog) Table(name string) (*Table, bool) {
	t, ok := c.tables[name]
	return t, ok
}

// Index returns the named index.
func (c *Catalog) Index(name string) (*Index, bool) {
	ix, ok := c.indexes[name]
	return ix, ok
}

// Tables returns every table, sorted by name.
func (c *Catalog) Tables() []*Table {
	ts := make([]*Table, 0, len(c.tables))
	for _, t := range c.tables {
		ts = append(ts, t)
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i].Name < ts[j].Name })
	return ts
}

func primaryKey(t *Table) *Index {
	for _, ix := range t.Indexes {
		if ix.Primary {
			return ix
		}
	}
	return nil
}

// PrimaryKey returns the table's primary key index, or nil.
func (t *Table) PrimaryKey() *Index { return primaryKey(t) }

// checkNewName checks that a new table or index may take name.
func (c *Catalog) checkNewName(name string) error {
	if err := checkNameLen(name); err != nil {
		return err
	}
	if strings.HasPrefix(name, ReservedPrefix) {
		return sqlerr.New(sqlerr.ReservedName, "the name %q is reserved", name).
			WithHint("Names beginning with %q are reserved for system tables.", ReservedPrefix)
	}
	if c.Exists(name) {
		return sqlerr.New(sqlerr.DuplicateTable, "relation %q already exists", name)
	}
	return nil
}

// checkNameLen rejects names the lexer would not produce: empty, or longer
// than PostgreSQL's limit.
func checkNameLen(name string) error {
	if name == "" || len(name) > maxNameLen {
		return sqlerr.New(sqlerr.NameTooLong, "the name %q is empty or longer than %d bytes", name, maxNameLen)
	}
	return nil
}
