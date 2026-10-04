package executor

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/catalog"
	"github.com/vikrant-choudhary06/NoVacDB/internal/mvcc"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/ast"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

// change is one row write, computed and checked before anything changes.
type change struct {
	rid  storage.RID   // the existing row (UPDATE, DELETE)
	old  []types.Value // nil for INSERT
	new  []types.Value // nil for DELETE
	data []byte        // the encoded new row
}

// defaultNode binds a column's default for an INSERT or UPDATE: its stored
// expression, converted to the column's type, or NULL.
func defaultNode(col *catalog.Column) (node, error) {
	if col.Default == nil {
		return &constNode{types.Null(col.Type)}, nil
	}
	b := &binder{} // a stored default: no positions, no columns
	n, err := b.bind(col.Default)
	if err != nil {
		return nil, err
	}
	return b.assign(n, 0, col)
}

// value binds an expression assigned to col; DEFAULT is the column's
// default.
func (b *binder) value(e ast.Expr, col *catalog.Column) (node, error) {
	if _, ok := e.(*ast.Default); ok {
		return defaultNode(col)
	}
	n, err := b.bind(e)
	if err != nil {
		return nil, err
	}
	return b.assign(n, e.Pos(), col)
}

// checkRow checks a new row's NOT NULL constraints and sizes, and encodes
// it.
func checkRow(tbl *catalog.Table, row []types.Value) ([]byte, error) {
	for i, c := range tbl.Columns {
		if c.NotNull && row[i].Null {
			return nil, sqlerr.New(sqlerr.NotNullViolation, "null value in column %q of relation %q violates not-null constraint", c.Name, tbl.Name).
				WithDetail("Failing row contains %s.", describeRow(row))
		}
	}
	data, err := types.EncodeRow(row, tbl.Types())
	if err != nil {
		return nil, err
	}
	for _, ix := range tbl.Indexes {
		// The RID's encoding has a fixed size, so any RID gives the size.
		if _, err := ix.Key(row, storage.RID{}); err != nil {
			return nil, err
		}
	}
	return data, nil
}

// describeRow formats a row as PostgreSQL's error details do: (1, null, x).
func describeRow(row []types.Value) string {
	parts := make([]string, len(row))
	for i, v := range row {
		if v.Null {
			parts[i] = "null"
		} else {
			parts[i] = types.Format(v)
		}
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// checkUnique checks the unique indexes against the table as it will be
// after the statement: an existing entry conflicts unless its row is one
// the statement updates (and so loses its old key), and new rows must not
// conflict with each other. Keys with a NULL never conflict.
func (st *stmt) checkUnique(tbl *catalog.Table, changes []change) error {
	updated := map[storage.RID]bool{}
	for _, c := range changes {
		if c.old != nil {
			updated[c.rid] = true
		}
	}
	for _, ix := range tbl.Indexes {
		if !ix.Unique {
			continue
		}
		keys := map[string]bool{}
		for _, c := range changes {
			if c.new == nil {
				continue
			}
			key, ok := ix.UniquePrefix(c.new)
			if !ok {
				continue // a NULL never conflicts
			}
			dup := keys[string(key)]
			keys[string(key)] = true
			if !dup {
				// Existing entries with these columns conflict unless their
				// row is one this statement changes (and so loses them).
				rids, err := ix.Lookup(st.ctx, key)
				if err != nil {
					return err
				}
				for _, rid := range rids {
					dup = dup || !updated[rid]
				}
			}
			if dup {
				return sqlerr.New(sqlerr.UniqueViolation, "duplicate key value violates unique constraint %q", ix.Name).
					WithDetail("Key %s already exists.", ix.DescribeKey(key))
			}
		}
	}
	return nil
}

// applyChanges writes checked changes: first every old index entry goes,
// then each row is written and its new entries added, so that rows may
// swap unique keys within one statement.
func applyChanges(ctx context.Context, w mvcc.Writer, tbl *catalog.Table, changes []change) error {
	for _, c := range changes {
		if c.old == nil {
			continue
		}
		for _, ix := range tbl.Indexes {
			key, err := ix.Key(c.old, c.rid)
			if err != nil {
				return err
			}
			found, err := ix.Tree.Delete(ctx, key)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("index %q has no entry for row %v: %w", ix.Name, c.rid, btree.ErrCorruptNode)
			}
		}
	}
	for _, c := range changes {
		rid := c.rid
		var err error
		switch {
		case c.new == nil:
			err = w.Delete(ctx, tbl.Heap, rid)
		case c.old == nil:
			rid, err = w.Insert(ctx, tbl.Heap, c.data)
		default:
			// The row keeps its RID, even if it moves (section 2.2).
			err = w.Update(ctx, tbl.Heap, rid, c.data)
		}
		if err != nil {
			return err
		}
		if c.new == nil {
			continue
		}
		for _, ix := range tbl.Indexes {
			key, err := ix.Key(c.new, rid)
			if err != nil {
				return err
			}
			if err := ix.Tree.Insert(ctx, key, catalog.EncodeRID(rid)); err != nil {
				return fmt.Errorf("index %q: %w", ix.Name, err)
			}
		}
	}
	return nil
}

// write checks changes and applies them as one statement group.
func (st *stmt) write(tbl *catalog.Table, changes []change) error {
	if err := st.checkUnique(tbl, changes); err != nil {
		return err
	}
	if len(changes) == 0 {
		return nil
	}
	if err := st.ctx.Err(); err != nil {
		return err
	}
	err := st.apply(false, func(ctx context.Context, xid wal.XID) error {
		w := mvcc.Writer{Undo: st.db.e.Undo(), XID: uint64(xid), Table: uint64(tbl.ID)}
		return applyChanges(ctx, w, tbl, changes)
	})
	return err
}

func (st *stmt) insert(s *ast.Insert) (*Result, error) {
	tbl, err := st.table(s.Table)
	if err != nil {
		return nil, err
	}
	// Target columns. Without a column list the values go to the first
	// columns in order, and any others take their defaults, as in
	// PostgreSQL.
	var targets []int
	if s.Columns == nil && !s.DefaultValues {
		for i := range min(len(s.Rows[0]), len(tbl.Columns)) {
			targets = append(targets, i)
		}
	}
	for _, c := range s.Columns {
		i := tbl.ColumnIndex(c.Name)
		if i < 0 {
			return nil, sqlerr.New(sqlerr.UndefinedColumn, "column %q of relation %q does not exist", c.Name, tbl.Name).At(st.sql, c.P)
		}
		for _, t := range targets {
			if t == i {
				return nil, sqlerr.New(sqlerr.DuplicateColumn, "column %q specified more than once", c.Name).At(st.sql, c.P)
			}
		}
		targets = append(targets, i)
	}
	rows := s.Rows
	if s.DefaultValues {
		rows = [][]ast.Expr{nil}
		targets = nil
	}
	for _, r := range rows[1:] {
		if len(r) != len(rows[0]) {
			return nil, sqlerr.New(sqlerr.SyntaxError, "VALUES lists must all be the same length").At(st.sql, r[0].Pos())
		}
	}
	if n := len(rows[0]); n > len(targets) {
		return nil, sqlerr.New(sqlerr.SyntaxError, "INSERT has more expressions than target columns").At(st.sql, rows[0][len(targets)].Pos())
	} else if n < len(targets) {
		pos := s.P
		if s.Columns != nil {
			pos = s.Columns[n].P
		}
		return nil, sqlerr.New(sqlerr.SyntaxError, "INSERT has more target columns than expressions").At(st.sql, pos)
	}

	// Bind every value and the defaults of the other columns.
	b := st.binder()
	targeted := make([]bool, len(tbl.Columns))
	for _, t := range targets {
		targeted[t] = true
	}
	defaults := make([]node, len(tbl.Columns))
	for i, c := range tbl.Columns {
		if !targeted[i] {
			if defaults[i], err = defaultNode(c); err != nil {
				return nil, err
			}
		}
	}
	bound := make([][]node, len(rows))
	for r, row := range rows {
		bound[r] = make([]node, len(row))
		for i, e := range row {
			if bound[r][i], err = b.value(e, tbl.Columns[targets[i]]); err != nil {
				return nil, err
			}
		}
	}

	if st.describe {
		return &Result{}, nil
	}

	// Compute and check.
	changes := make([]change, 0, len(rows))
	for r := range rows {
		if r%256 == 255 {
			if err := st.ctx.Err(); err != nil {
				return nil, err
			}
		}
		vals := make([]types.Value, len(tbl.Columns))
		for i, d := range defaults {
			if d != nil {
				if vals[i], err = d.eval(st.ec, nil); err != nil {
					return nil, err
				}
			}
		}
		for i, n := range bound[r] {
			if vals[targets[i]], err = n.eval(st.ec, nil); err != nil {
				return nil, err
			}
		}
		data, err := checkRow(tbl, vals)
		if err != nil {
			return nil, err
		}
		changes = append(changes, change{new: vals, data: data})
	}
	if err := st.write(tbl, changes); err != nil {
		return nil, err
	}
	return &Result{Tag: "INSERT 0 " + strconv.Itoa(len(changes))}, nil
}

func (st *stmt) update(s *ast.Update) (*Result, error) {
	tbl, err := st.table(s.Table.Name)
	if err != nil {
		return nil, err
	}
	b := st.rowBinder(tbl, s.Table)
	sets := map[int]node{}
	for _, a := range s.Sets {
		i := tbl.ColumnIndex(a.Column.Name)
		if i < 0 {
			return nil, sqlerr.New(sqlerr.UndefinedColumn, "column %q of relation %q does not exist", a.Column.Name, tbl.Name).At(st.sql, a.Column.P)
		}
		if _, dup := sets[i]; dup {
			return nil, sqlerr.New(sqlerr.SyntaxError, "multiple assignments to same column %q", a.Column.Name).At(st.sql, a.Column.P)
		}
		if sets[i], err = b.value(a.Value, tbl.Columns[i]); err != nil {
			return nil, err
		}
	}
	where, err := b.where(s.Where)
	if err != nil {
		return nil, err
	}
	if st.describe {
		return &Result{}, nil
	}
	var changes []change
	err = st.scan(tbl, st.access(tbl, where), where, func(rid storage.RID, old []types.Value) (bool, error) {
		row := append([]types.Value(nil), old...)
		for i, n := range sets {
			v, err := n.eval(st.ec, old)
			if err != nil {
				return false, err
			}
			row[i] = v
		}
		data, err := checkRow(tbl, row)
		if err != nil {
			return false, err
		}
		changes = append(changes, change{rid: rid, old: old, new: row, data: data})
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if err := st.write(tbl, changes); err != nil {
		return nil, err
	}
	return &Result{Tag: "UPDATE " + strconv.Itoa(len(changes))}, nil
}

func (st *stmt) delete(s *ast.Delete) (*Result, error) {
	tbl, err := st.table(s.Table.Name)
	if err != nil {
		return nil, err
	}
	where, err := st.rowBinder(tbl, s.Table).where(s.Where)
	if err != nil {
		return nil, err
	}
	if st.describe {
		return &Result{}, nil
	}
	var changes []change
	err = st.scan(tbl, st.access(tbl, where), where, func(rid storage.RID, old []types.Value) (bool, error) {
		changes = append(changes, change{rid: rid, old: old})
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if err := st.write(tbl, changes); err != nil {
		return nil, err
	}
	return &Result{Tag: "DELETE " + strconv.Itoa(len(changes))}, nil
}

// access plans how to read tbl for a WHERE clause.
func (st *stmt) access(tbl *catalog.Table, where node) access {
	if st.db.noIndexScans {
		return access{}
	}
	return plan(tbl, where)
}
