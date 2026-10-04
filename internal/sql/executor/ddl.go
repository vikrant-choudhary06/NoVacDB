package executor

import (
	"context"
	"fmt"

	"github.com/vikrant-choudhary06/NoVacDB/internal/catalog"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/ast"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

func (st *stmt) createTable(s *ast.CreateTable) (*Result, error) {
	res := &Result{Tag: "CREATE TABLE"}
	if st.db.cat.Exists(s.Name.Name) && s.IfNotExists {
		res.Notices = append(res.Notices, Notice{sqlerr.DuplicateTable, "relation \"" + s.Name.Name + "\" already exists, skipping"})
		return res, nil
	}
	def := catalog.TableDef{Name: s.Name.Name}
	setPK := func(cols []string, pos int) error {
		if def.PrimaryKey != nil {
			return sqlerr.New(sqlerr.InvalidTableDefinition, "multiple primary keys for table %q are not allowed", s.Name.Name).At(st.sql, pos)
		}
		def.PrimaryKey = cols
		return nil
	}
	for _, c := range s.Columns {
		if c.NotNull && c.Null || c.Null && c.PrimaryKey {
			return nil, sqlerr.New(sqlerr.SyntaxError, "conflicting NULL/NOT NULL declarations for column %q of table %q", c.Name.Name, s.Name.Name).At(st.sql, c.Name.P)
		}
		cd := catalog.ColumnDef{Name: c.Name.Name, Type: types.FromAST(c.Type), NotNull: c.NotNull, Default: c.Default}
		if c.Default != nil {
			// The default must be a constant expression of a type that
			// converts to the column's.
			b := &binder{sql: st.sql, noVars: func() *sqlerr.Error {
				return sqlerr.New(sqlerr.FeatureNotSupported, "cannot use column reference in DEFAULT expression")
			}}
			n, err := b.bind(c.Default)
			if err != nil {
				return nil, err
			}
			if _, err := b.assign(n, c.Default.Pos(), &catalog.Column{Name: cd.Name, Type: cd.Type}); err != nil {
				return nil, err
			}
		}
		def.Columns = append(def.Columns, cd)
		if c.PrimaryKey {
			if err := setPK([]string{c.Name.Name}, c.Name.P); err != nil {
				return nil, err
			}
		}
		if c.Unique {
			def.Unique = append(def.Unique, []string{c.Name.Name})
		}
	}
	for _, tc := range s.Constraints {
		cols := make([]string, len(tc.Columns))
		for i, n := range tc.Columns {
			cols[i] = n.Name
		}
		if tc.PrimaryKey {
			if err := setPK(cols, tc.P); err != nil {
				return nil, err
			}
		} else {
			def.Unique = append(def.Unique, cols)
		}
	}
	err := st.apply(true, func(ctx context.Context, xid wal.XID) error {
		_, err := st.db.cat.CreateTable(ctx, xid, def)
		return err
	})
	if err != nil {
		return nil, positioned(err, st.sql, s.Name.P)
	}
	return res, nil
}

// positioned gives a catalog error the position of the statement's object
// name if it has none.
func positioned(err error, sql string, pos int) error {
	se := sqlerr.From(err)
	if se.Position == 0 && se.Code != sqlerr.IOError && se.Code != sqlerr.DataCorrupted && se.Code != sqlerr.InternalError {
		se = se.At(sql, pos)
	}
	return se
}

func (st *stmt) dropTable(s *ast.DropTable) (*Result, error) {
	res := &Result{Tag: "DROP TABLE"}
	tbl, ok := st.db.cat.Table(s.Name.Name)
	switch {
	case !ok && st.isIndex(s.Name.Name):
		return nil, sqlerr.New(sqlerr.WrongObjectType, "%q is not a table", s.Name.Name).
			WithHint("Use DROP INDEX to remove an index.").At(st.sql, s.Name.P)
	case !ok && s.IfExists:
		res.Notices = append(res.Notices, Notice{sqlerr.SuccessfulCompletion, "table \"" + s.Name.Name + "\" does not exist, skipping"})
		return res, nil
	case !ok:
		return nil, sqlerr.New(sqlerr.UndefinedTable, "table %q does not exist", s.Name.Name).At(st.sql, s.Name.P)
	}
	err := st.apply(true, func(ctx context.Context, xid wal.XID) error {
		pages, err := st.db.cat.DropTable(ctx, xid, tbl)
		if err != nil {
			return err
		}
		return st.freeLater(ctx, pages)
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (st *stmt) isIndex(name string) bool {
	_, ok := st.db.cat.Index(name)
	return ok
}

// freeLater hands a dropped object's pages to the checkpointer, inside the
// statement group: the request is logged, so it is discarded with the
// statement or survives a crash with it, and the pages are freed once a
// checkpoint's redo point is past it (08-btree.md section 2.7). A failure is
// never a *sqlerr.Error, so the statement is abandoned.
func (st *stmt) freeLater(ctx context.Context, pages []uint64) error {
	if err := st.db.e.Logger().DeferFree(ctx, pages...); err != nil {
		return fmt.Errorf("dropping: %w", err)
	}
	return nil
}

func (st *stmt) createIndex(s *ast.CreateIndex) (*Result, error) {
	res := &Result{Tag: "CREATE INDEX"}
	tbl, err := st.table(s.Table)
	if err != nil {
		return nil, err
	}
	if s.Name.Name != "" && st.db.cat.Exists(s.Name.Name) && s.IfNotExists {
		res.Notices = append(res.Notices, Notice{sqlerr.DuplicateTable, "relation \"" + s.Name.Name + "\" already exists, skipping"})
		return res, nil
	}
	cols := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		if tbl.ColumnIndex(c.Name) < 0 {
			return nil, sqlerr.New(sqlerr.UndefinedColumn, "column %q does not exist", c.Name).At(st.sql, c.P)
		}
		cols[i] = c.Name
	}
	err = st.apply(true, func(ctx context.Context, xid wal.XID) error {
		_, err := st.db.cat.CreateIndex(ctx, xid, tbl, s.Name.Name, cols, s.Unique)
		return err
	})
	if err != nil {
		pos := s.Name.P
		if s.Name.Name == "" {
			pos = s.P
		}
		return nil, positioned(err, st.sql, pos)
	}
	return res, nil
}

func (st *stmt) dropIndex(s *ast.DropIndex) (*Result, error) {
	res := &Result{Tag: "DROP INDEX"}
	ix, ok := st.db.cat.Index(s.Name.Name)
	switch {
	case !ok && st.db.cat.Exists(s.Name.Name):
		return nil, sqlerr.New(sqlerr.WrongObjectType, "%q is not an index", s.Name.Name).
			WithHint("Use DROP TABLE to remove a table.").At(st.sql, s.Name.P)
	case !ok && s.IfExists:
		res.Notices = append(res.Notices, Notice{sqlerr.SuccessfulCompletion, "index \"" + s.Name.Name + "\" does not exist, skipping"})
		return res, nil
	case !ok:
		return nil, sqlerr.New(sqlerr.UndefinedObject, "index %q does not exist", s.Name.Name).At(st.sql, s.Name.P)
	}
	err := st.apply(true, func(ctx context.Context, xid wal.XID) error {
		pages, err := st.db.cat.DropIndex(ctx, xid, ix)
		if err != nil {
			return err
		}
		return st.freeLater(ctx, pages)
	})
	if err != nil {
		return nil, positioned(err, st.sql, s.Name.P)
	}
	return res, nil
}
