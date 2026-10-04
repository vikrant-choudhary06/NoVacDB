package catalog

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/mvcc"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/ast"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/parser"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

// maxNameLen is PostgreSQL's NAMEDATALEN-1, as the lexer enforces.
const maxNameLen = parser.MaxIdentifierLength

// MaxIndexColumns is PostgreSQL's INDEX_MAX_KEYS.
const MaxIndexColumns = 32

// Errors from the DDL methods (CreateTable, CreateIndex, DropTable,
// DropIndex) follow one rule: a *sqlerr.Error means nothing was changed,
// so the caller's statement may still commit; any other error may leave
// some changes made, and the caller must abandon the statement. Everything
// that can fail with a SQL error is checked before the first change.

// ColumnDef describes a new column.
type ColumnDef struct {
	Name    string
	Type    types.Type
	NotNull bool
	Default ast.Expr // nil means NULL; the executor has checked its type
}

// TableDef describes a new table. Constraints name their columns.
type TableDef struct {
	Name       string
	Columns    []ColumnDef
	PrimaryKey []string   // nil if none
	Unique     [][]string // UNIQUE constraints
}

// resolve turns column names into positions.
func resolve(t *Table, names []string) ([]int, error) {
	if len(names) == 0 {
		return nil, sqlerr.New(sqlerr.InvalidTableDefinition, "a key needs at least one column")
	}
	if len(names) > MaxIndexColumns {
		return nil, sqlerr.New(sqlerr.TooManyColumns, "cannot use more than %d columns in an index", MaxIndexColumns)
	}
	pos := make([]int, len(names))
	for i, n := range names {
		p := t.ColumnIndex(n)
		if p < 0 {
			return nil, sqlerr.New(sqlerr.UndefinedColumn, "column %q named in key does not exist", n)
		}
		for _, q := range pos[:i] {
			if q == p {
				return nil, sqlerr.New(sqlerr.DuplicateColumn, "column %q appears twice in the key", n)
			}
		}
		pos[i] = p
	}
	return pos, nil
}

// CreateTable creates a table and the indexes for its primary key and
// unique constraints, in transaction xid. If this fails after changing
// anything, the caller must abandon the transaction.
func (c *Catalog) CreateTable(ctx context.Context, xid wal.XID, def TableDef) (*Table, error) {
	if err := c.checkNewName(def.Name); err != nil {
		return nil, err
	}
	if len(def.Columns) > types.MaxColumns {
		return nil, sqlerr.New(sqlerr.TooManyColumns, "tables can have at most %d columns", types.MaxColumns)
	}
	t := &Table{Name: def.Name}
	for i, cd := range def.Columns {
		if err := checkNameLen(cd.Name); err != nil {
			return nil, err
		}
		if t.ColumnIndex(cd.Name) >= 0 {
			return nil, sqlerr.New(sqlerr.DuplicateColumn, "column %q specified more than once", cd.Name)
		}
		if !cd.Type.Valid() {
			return nil, sqlerr.New(sqlerr.InternalError, "column %q has no type", cd.Name)
		}
		col := &Column{Name: cd.Name, Type: cd.Type, NotNull: cd.NotNull, Default: cd.Default}
		// The column's catalog row must fit; only a long default can make
		// it too large. The ID is fixed-size, so 0 stands in for it.
		if _, err := types.EncodeRow(columnRow(0, i, col), sysTypes[sysColumns]); err != nil {
			return nil, sqlerr.New(sqlerr.ProgramLimitExceeded, "the default of column %q is too long", cd.Name)
		}
		t.Columns = append(t.Columns, col)
	}
	// Resolve every constraint before changing anything.
	var pk []int
	var err error
	if def.PrimaryKey != nil {
		if pk, err = resolve(t, def.PrimaryKey); err != nil {
			return nil, err
		}
		for _, p := range pk {
			t.Columns[p].NotNull = true // primary key columns are NOT NULL
		}
	}
	uniques := make([][]int, len(def.Unique))
	for i, u := range def.Unique {
		if uniques[i], err = resolve(t, u); err != nil {
			return nil, err
		}
	}

	if t.Heap, err = c.e.CreateHeap(ctx); err != nil {
		return nil, err
	}
	t.ID = c.newID()
	w := c.writer(xid)
	if t.rid, err = c.insertSys(ctx, w, sysTables, []types.Value{
		types.NewInt8(t.ID), types.NewText(t.Name), types.NewInt8(int64(t.Heap.FirstPage())),
	}); err != nil {
		return nil, err
	}
	for i, col := range t.Columns {
		if col.rid, err = c.insertSys(ctx, w, sysColumns, columnRow(t.ID, i, col)); err != nil {
			return nil, err
		}
	}
	c.tables[t.Name] = t
	if pk != nil {
		if _, err := c.createIndex(ctx, w, t, c.chooseName(t.Name, nil, "pkey"), pk, true, true); err != nil {
			return nil, err
		}
	}
	for _, u := range uniques {
		names := make([]string, len(u))
		for i, p := range u {
			names[i] = t.Columns[p].Name
		}
		if _, err := c.createIndex(ctx, w, t, c.chooseName(t.Name, names, "key"), u, true, false); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// columnRow is a column's row in novac_columns.
func columnRow(tableID int64, pos int, col *Column) []types.Value {
	def := ""
	if col.Default != nil {
		def = col.Default.String()
	}
	return []types.Value{
		types.NewInt8(tableID), types.NewInt4(int32(pos)), types.NewText(col.Name),
		types.NewInt4(int32(col.Type)), types.NewBool(col.NotNull), types.NewText(def),
	}
}

// insertSys adds a row to a system table. It runs after changes have been
// made, so its errors are never *sqlerr.Error (see the rule above).
func (c *Catalog) insertSys(ctx context.Context, w mvcc.Writer, which int, row []types.Value) (storage.RID, error) {
	data, err := types.EncodeRow(row, sysTypes[which])
	if err != nil {
		// The callers check the rows' sizes first; this is a bug.
		return storage.RID{}, fmt.Errorf("catalog: encoding a %s row: %s", sysNames[which], err.Error())
	}
	return w.Insert(ctx, c.sys[which], data)
}

// writer writes system table rows in transaction xid. System tables have
// no catalog ID; their undo records carry table 0.
func (c *Catalog) writer(xid wal.XID) mvcc.Writer {
	return mvcc.Writer{Undo: c.e.Undo(), XID: uint64(xid)}
}

// chooseName picks a name for an index as PostgreSQL does: table, columns
// and a label joined by underscores (t_pkey, t_a_b_key, t_a_idx), shortened
// to fit, with a number appended until it is unused.
func (c *Catalog) chooseName(table string, cols []string, label string) string {
	base := strings.Join(append([]string{table}, cols...), "_")
	for n := 0; ; n++ {
		suffix := "_" + label
		if n > 0 {
			suffix += strconv.Itoa(n)
		}
		b := base
		if len(b)+len(suffix) > maxNameLen {
			b = truncateUTF8(b, maxNameLen-len(suffix))
		}
		if name := b + suffix; !c.Exists(name) {
			return name
		}
	}
}

// truncateUTF8 cuts s to at most n bytes without splitting a character.
func truncateUTF8(s string, n int) string {
	for n > 0 && n < len(s) && s[n]&0xC0 == 0x80 {
		n--
	}
	return s[:n]
}

// CreateIndex creates an index on table columns. An empty name gets one
// chosen as PostgreSQL does. The table is read first, so a unique index
// over duplicate values or an oversized key fails before anything changes.
// It runs in transaction xid.
func (c *Catalog) CreateIndex(ctx context.Context, xid wal.XID, t *Table, name string, cols []string, unique bool) (*Index, error) {
	pos, err := resolve(t, cols)
	if err != nil {
		return nil, err
	}
	if name == "" {
		name = c.chooseName(t.Name, cols, "idx")
	} else if err := c.checkNewName(name); err != nil {
		return nil, err
	}
	return c.createIndex(ctx, c.writer(xid), t, name, pos, unique, false)
}

// indexEntry is a row's entry for an index, and for a unique index the
// prefix that must be unique (nil if the row has a NULL in it).
type indexEntry struct {
	key, value, unique []byte
}

// rowEntries reads every row of t and returns each one's entry for ix.
func (c *Catalog) rowEntries(ctx context.Context, ix *Index) ([]indexEntry, error) {
	var out []indexEntry
	s := ix.Table.Heap.Scan()
	colTypes := ix.Table.Types()
	for {
		rid, v, ok, err := s.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		row, err := types.DecodeRow(v.Data, colTypes)
		if err != nil {
			return nil, err
		}
		key, err := ix.Key(row, rid)
		if err != nil {
			return nil, err
		}
		e := indexEntry{key: key, value: EncodeRID(rid)}
		if ix.Unique {
			e.unique, _ = ix.UniquePrefix(row)
		}
		out = append(out, e)
	}
}

func (c *Catalog) createIndex(ctx context.Context, w mvcc.Writer, t *Table, name string, cols []int, unique, primary bool) (*Index, error) {
	ix := &Index{Name: name, Table: t, Columns: cols, Unique: unique, Primary: primary}
	entries, err := c.rowEntries(ctx, ix)
	if err != nil {
		return nil, err
	}
	if unique {
		seen := map[string]bool{}
		for _, e := range entries {
			if e.unique == nil {
				continue // a NULL never conflicts
			}
			if seen[string(e.unique)] {
				return nil, sqlerr.New(sqlerr.UniqueViolation, "could not create unique index %q", name).
					WithDetail("Key %s is duplicated.", ix.DescribeKey(e.unique))
			}
			seen[string(e.unique)] = true
		}
	}
	if ix.Tree, err = c.e.CreateBTree(ctx); err != nil {
		return nil, err
	}
	for _, e := range entries {
		if err := ix.Tree.Insert(ctx, e.key, e.value); err != nil {
			return nil, err
		}
	}
	ix.ID = c.newID()
	colList := make([]string, len(cols))
	for i, p := range cols {
		colList[i] = strconv.Itoa(p)
	}
	if ix.rid, err = c.insertSys(ctx, w, sysIndexes, []types.Value{
		types.NewInt8(ix.ID), types.NewText(name), types.NewInt8(t.ID), types.NewInt8(int64(ix.Tree.Root())),
		types.NewBool(unique), types.NewBool(primary), types.NewText(strings.Join(colList, ",")),
	}); err != nil {
		return nil, err
	}
	c.indexes[name] = ix
	t.Indexes = append(t.Indexes, ix)
	return ix, nil
}

// DropTable removes a table and its indexes from the catalog and returns
// their pages, which the caller frees once the statement has committed. It
// runs in transaction xid.
func (c *Catalog) DropTable(ctx context.Context, xid wal.XID, t *Table) ([]uint64, error) {
	// Everything that can fail on a sound database happens before the
	// first change.
	pages := t.Heap.Pages()
	for _, ix := range t.Indexes {
		ps, err := ix.Tree.Pages(ctx)
		if err != nil {
			return nil, err
		}
		pages = append(pages, ps...)
	}
	w := c.writer(xid)
	for _, ix := range t.Indexes {
		if err := w.Delete(ctx, c.sys[sysIndexes], ix.rid); err != nil {
			return nil, err
		}
	}
	for _, col := range t.Columns {
		if err := w.Delete(ctx, c.sys[sysColumns], col.rid); err != nil {
			return nil, err
		}
	}
	if err := w.Delete(ctx, c.sys[sysTables], t.rid); err != nil {
		return nil, err
	}
	for _, ix := range t.Indexes {
		delete(c.indexes, ix.Name)
	}
	delete(c.tables, t.Name)
	return pages, nil
}

// DropIndex removes an index and returns its pages, which the caller frees
// once the statement has committed. A primary key's index cannot be dropped
// alone. It runs in transaction xid.
func (c *Catalog) DropIndex(ctx context.Context, xid wal.XID, ix *Index) ([]uint64, error) {
	if ix.Primary {
		return nil, sqlerr.New(sqlerr.DependentObjectsStillExist, "cannot drop index %q because it is the primary key of table %q", ix.Name, ix.Table.Name).
			WithHint("Drop the table instead; dropping a primary key alone is not supported yet.")
	}
	pages, err := ix.Tree.Pages(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.writer(xid).Delete(ctx, c.sys[sysIndexes], ix.rid); err != nil {
		return nil, err
	}
	delete(c.indexes, ix.Name)
	t := ix.Table
	for i, x := range t.Indexes {
		if x == ix {
			t.Indexes = append(t.Indexes[:i], t.Indexes[i+1:]...)
			break
		}
	}
	return pages, nil
}

// --- index entries --------------------------------------------------------

// ridSize is the size of an encoded RID: u64 page, u16 slot.
const ridSize = 10

// EncodeRID encodes a RID as an index entry's value.
func EncodeRID(rid storage.RID) []byte {
	b := binary.LittleEndian.AppendUint64(make([]byte, 0, ridSize), rid.Page)
	return binary.LittleEndian.AppendUint16(b, rid.Slot)
}

// DecodeRID decodes an index entry's value.
func DecodeRID(b []byte) (storage.RID, error) {
	if len(b) != ridSize {
		return storage.RID{}, sqlerr.New(sqlerr.DataCorrupted, "index entry value of %d bytes", len(b))
	}
	return storage.RID{Page: binary.LittleEndian.Uint64(b), Slot: binary.LittleEndian.Uint16(b[8:])}, nil
}

// KeyPrefix returns the encoding of the index's columns for row values
// given in index column order; scans use it to build ranges.
func KeyPrefix(vals []types.Value) []byte {
	var key []byte
	for _, v := range vals {
		key = types.AppendKey(key, v)
	}
	return key
}

// Key returns the index key of a table row stored at rid: the indexed
// columns followed by the RID, in every index, unique or not, so that two
// entries with equal columns can coexist (as Phase 6 needs for row versions;
// docs/design/08-btree.md section 2.11). A unique index enforces uniqueness
// on the columns with a prefix probe (UniquePrefix, Lookup). A key too large
// for the B+Tree is a 54000 error.
func (ix *Index) Key(row []types.Value, rid storage.RID) ([]byte, error) {
	var key []byte
	for _, p := range ix.Columns {
		key = types.AppendKey(key, row[p])
	}
	key = btree.AppendInt64(key, int64(rid.Page))
	key = btree.AppendInt64(key, int64(rid.Slot))
	if len(key) > btree.MaxKeySize {
		return nil, sqlerr.New(sqlerr.ProgramLimitExceeded, "index row size %d exceeds the maximum of %d for index %q", len(key), btree.MaxKeySize, ix.Name).
			WithHint("Values in indexed columns must be short; index a shorter column or a prefix of it.")
	}
	return key, nil
}

// UniquePrefix returns the part of a row's key that a unique index keeps
// unique: the encoded columns. ok is false if any of them is NULL, as NULLs
// are never equal to each other and never conflict.
func (ix *Index) UniquePrefix(row []types.Value) (prefix []byte, ok bool) {
	for _, p := range ix.Columns {
		if row[p].Null {
			return nil, false
		}
		prefix = types.AppendKey(prefix, row[p])
	}
	return prefix, true
}

// Lookup returns the RIDs of the entries whose columns encode to prefix (a
// UniquePrefix or KeyPrefix of all the index's columns).
func (ix *Index) Lookup(ctx context.Context, prefix []byte) ([]storage.RID, error) {
	end := btree.Bound{}
	if s := KeySuccessor(prefix); s != nil {
		end = btree.Excl(s)
	}
	it := ix.Tree.Scan(btree.Incl(prefix), end)
	var out []storage.RID
	for {
		_, v, ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		rid, err := DecodeRID(v)
		if err != nil {
			return nil, err
		}
		out = append(out, rid)
	}
}

// KeySuccessor returns the smallest key greater than every key that starts
// with p, or nil if there is none (p is empty or all 0xff).
func KeySuccessor(p []byte) []byte {
	s := bytes.Clone(p)
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] < 0xff {
			s[i]++
			return s[:i+1]
		}
	}
	return nil
}

// DescribeKey renders an index key as PostgreSQL does in messages:
// (a, b)=(1, x).
func (ix *Index) DescribeKey(key []byte) string {
	names := make([]string, len(ix.Columns))
	for i, p := range ix.Columns {
		names[i] = ix.Table.Columns[p].Name
	}
	vals, err := btree.DecodeKey(key)
	parts := make([]string, len(ix.Columns))
	for i := range parts {
		switch {
		case err != nil || i >= len(vals):
			parts[i] = "?"
		case vals[i].Kind == btree.KindNull:
			parts[i] = "NULL"
		default:
			parts[i] = formatKeyValue(vals[i], ix.Table.Columns[ix.Columns[i]].Type)
		}
	}
	return fmt.Sprintf("(%s)=(%s)", strings.Join(names, ", "), strings.Join(parts, ", "))
}

func formatKeyValue(v btree.Value, t types.Type) string {
	switch v.Kind {
	case btree.KindBool:
		return types.Format(types.NewBool(v.Bool))
	case btree.KindFloat64:
		return types.Format(types.NewFloat8(v.Float64))
	case btree.KindBytes:
		return string(bytes.Clone(v.Bytes))
	}
	return types.Format(types.Value{T: t, I: v.Int64})
}
