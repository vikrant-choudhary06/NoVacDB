package executor

import (
	"context"
	"errors"
	"fmt"

	"github.com/vikrant-choudhary06/NoVacDB/internal/catalog"
	"github.com/vikrant-choudhary06/NoVacDB/internal/mvcc"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/undo"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

// Rollback with undo (docs/design/17-rollback.md).

// errNotUserTable means an undo record names a table that is not a user
// table: a system table's row, changed by DDL, whose rollback reopens the
// database instead (section 2.7).
var errNotUserTable = errors.New("executor: undo of a table that is not a user table")

// undoer returns the reversal of transaction xid's undo records (section
// 2.1): each row gets back the version the change replaced, and its index
// entries follow.
func (db *DB) undoer(xid wal.XID) wal.Undoer {
	return func(ctx context.Context, from, to undo.Ptr) error {
		tables := map[uint64]*catalog.Table{}
		for _, t := range db.cat.Tables() {
			tables[uint64(t.ID)] = t
		}
		return mvcc.Undo(ctx, db.e.Undo(), uint64(xid), from, to, func(p undo.Ptr, rec undo.Record) error {
			tbl := tables[rec.Table]
			if tbl == nil {
				return fmt.Errorf("undo at %s names table %d: %w", p, rec.Table, errNotUserTable)
			}
			cur, old, err := mvcc.Revert(ctx, tbl.Heap, p, rec)
			if err != nil {
				return err
			}
			colTypes := tbl.Types()
			if !cur.Deleted {
				row, err := types.DecodeRow(cur.Data, colTypes)
				if err != nil {
					return err
				}
				if err := deleteEntries(ctx, tbl, row, rec.RID, true); err != nil {
					return err
				}
			}
			if old != nil {
				row, err := types.DecodeRow(old.Data, colTypes)
				if err != nil {
					return err
				}
				if err := addEntries(ctx, tbl, row, rec.RID, true); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

// pagesConsistent reports whether a statement that failed with err left
// every page consistent, so that its changes can be undone in place
// (section 2.5): an SQL error, a REPEATABLE READ write conflict, or a full
// buffer pool. Every heap and B+Tree operation either completes or changes
// nothing. Any other error (I/O, logging, corruption) restarts the
// database.
func pagesConsistent(err error) bool {
	var se *sqlerr.Error
	return errors.As(err, &se) || errors.Is(err, mvcc.ErrWriteConflict) || errors.Is(err, storage.ErrNoFreeFrames)
}

// undoStatement undoes the changes of a statement that failed with err
// after it began changing data, back to sp, where the transaction's undo
// ended before it (section 2.5). In autocommit the statement is the
// transaction, which rolls back; in a transaction, the transaction goes on,
// failed. It returns the error for the client.
func (st *stmt) undoStatement(ctx context.Context, wtx *wal.Txn, sp undo.Ptr, err error) error {
	db := st.db
	var rerr error
	if st.tx == nil {
		// The statement's snapshot must not hold back the undo it is about
		// to release (section 2.8).
		st.snap.Release()
		rerr = wtx.Rollback(ctx, db.undoer(wtx.XID()))
	} else {
		rerr = wtx.RollbackTo(ctx, sp, db.undoer(wtx.XID()))
	}
	if rerr != nil {
		db.log.Warn("undoing a failed statement failed; restarting the database", "dir", db.dir, "err", rerr, "statement_error", err)
		if st.tx != nil {
			st.tx.discarded = true
		}
		return db.restart(ctx, err, st.tx != nil, st.held)
	}
	db.undoneStatements.Add(1)
	switch {
	case errors.Is(err, mvcc.ErrWriteConflict):
		return sqlerr.Wrap(err, sqlerr.SerializationFailure, "could not serialize access due to concurrent update").
			WithHint("Retry the transaction.")
	case errors.Is(err, storage.ErrNoFreeFrames):
		return db.tooMuch(err, st.tx != nil)
	}
	return publicError(err)
}

// rollback ends a transaction that changed something without its changes:
// with its undo, or by reopening the database if it changed the schema
// (section 2.7) or its undo cannot be applied.
func (db *DB) rollback(ctx context.Context, wtx *wal.Txn, h held, schema bool) error {
	if !schema {
		err := wtx.Rollback(ctx, db.undoer(wtx.XID()))
		if err == nil {
			db.rollbacks.Add(1)
			return nil
		}
		db.log.Warn("rolling back with undo failed; reopening the database", "dir", db.dir, "xid", wtx.XID(), "err", err)
	} else {
		db.log.Info("rolling back a transaction that changed the schema: reopening the database", "dir", db.dir, "xid", wtx.XID())
	}
	db.discards.Add(1)
	db.exclusively(h, func() {
		_ = db.e.Abandon()
		db.reopen(ctx)
	})
	if db.broken != nil {
		return sqlerr.Wrap(db.broken, sqlerr.IOError, "the database is unavailable: reopening it after a rollback failed: %v", db.broken)
	}
	return nil
}
