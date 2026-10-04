package executor

import (
	"context"

	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/parser"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

// Tx is an explicit transaction (docs/design/13-transactions.md section
// 2.5): its statements commit or roll back together. It is not safe for
// concurrent use.
//
// A transaction that has changed something holds the writer slot until it
// ends (docs/design/16-snapshots-visibility.md section 2.4): other
// sessions read meanwhile, each through its own snapshot, so nobody sees
// its uncommitted changes, but another change waits for it. One that ran
// DDL also holds the exclusive lock, and nothing else runs. Calling the
// DB's own methods to change data from the goroutine of such a
// transaction would wait for the transaction itself; use the Tx's methods.
type Tx struct {
	db        *DB
	isolation IsolationLevel
	// snap is a REPEATABLE READ transaction's snapshot, taken at its first
	// statement (docs/design/16-snapshots-visibility.md section 2.3).
	snap *wal.Snapshot
	wtx  *wal.Txn // nil until the first change
	// writing: the transaction holds the writer slot (db.writer), from
	// its first change to its end.
	writing bool
	// locked: the transaction holds db.mu exclusively (it ran DDL).
	locked bool
	// failed: a statement failed; only Rollback (or Commit, which rolls
	// back) is left.
	failed bool
	// discarded: a restart after a failure already removed its changes.
	discarded bool
	done      bool
}

// IsolationLevel is a transaction's isolation level
// (docs/design/16-snapshots-visibility.md section 2.3).
type IsolationLevel int

// Isolation levels.
const (
	// ReadCommitted: each statement sees what was committed when it
	// started (PostgreSQL's default).
	ReadCommitted IsolationLevel = iota
	// RepeatableRead: every statement sees what was committed when the
	// transaction's first statement started. Changing a row changed since
	// fails with 40001.
	RepeatableRead
)

func (l IsolationLevel) String() string {
	if l == RepeatableRead {
		return "REPEATABLE READ"
	}
	return "READ COMMITTED"
}

// TxOptions configures a transaction.
type TxOptions struct {
	Isolation IsolationLevel
}

// Begin starts a READ COMMITTED transaction. It takes no lock and no
// transaction ID until its first change.
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	return db.BeginTx(ctx, TxOptions{})
}

// BeginTx starts a transaction with the given options.
func (db *DB) BeginTx(ctx context.Context, opts TxOptions) (*Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, canceled(err)
	}
	if opts.Isolation != ReadCommitted && opts.Isolation != RepeatableRead {
		return nil, sqlerr.New(sqlerr.InvalidParameterValue, "unknown isolation level %d", opts.Isolation)
	}
	return &Tx{db: db, isolation: opts.Isolation}, nil
}

func (tx *Tx) usable() error {
	switch {
	case tx.done:
		return sqlerr.New(sqlerr.InvalidTransactionState, "the transaction has already ended")
	case tx.failed:
		return sqlerr.New(sqlerr.InFailedSQLTransaction, "current transaction is aborted, commands ignored until end of transaction block")
	}
	return nil
}

// Exec runs the statements in sql in the transaction. The first error
// leaves the transaction failed: later statements return 25P02 until it
// ends.
func (tx *Tx) Exec(ctx context.Context, sql string) ([]*Result, error) {
	if err := tx.usable(); err != nil {
		return nil, err
	}
	stmts, err := parser.Parse(sql)
	if err != nil {
		tx.failed = true
		return nil, sqlerr.From(err)
	}
	var results []*Result
	for _, s := range stmts {
		r, err := tx.db.run(ctx, tx, sql, s, nil, false)
		if err != nil {
			tx.failed = true
			return results, err
		}
		results = append(results, r)
	}
	return results, nil
}

// ExecPrepared runs a prepared statement in the transaction, as
// DB.ExecPrepared does outside one.
func (tx *Tx) ExecPrepared(ctx context.Context, p *Prepared, values []types.Value) (*Result, error) {
	if err := tx.usable(); err != nil {
		return nil, err
	}
	r, err := tx.db.execPrepared(ctx, tx, p, values)
	if err != nil {
		tx.failed = true
	}
	return r, err
}

// Commit commits the transaction. A failed transaction is rolled back
// instead, and Commit reports 25P02.
func (tx *Tx) Commit(ctx context.Context) error {
	if tx.done {
		return sqlerr.New(sqlerr.InvalidTransactionState, "the transaction has already ended")
	}
	if tx.failed {
		if err := tx.Rollback(ctx); err != nil {
			return err
		}
		return sqlerr.New(sqlerr.InFailedSQLTransaction, "current transaction is aborted: it was rolled back, not committed")
	}
	db := tx.db
	defer tx.end()
	if tx.wtx == nil {
		return nil
	}
	ctx = context.WithoutCancel(ctx)
	// The snapshot must not hold back the transaction's own undo at commit
	// (docs/design/16-snapshots-visibility.md section 2.6).
	if tx.snap != nil {
		tx.snap.Release()
		tx.snap = nil
	}
	lsn, err := tx.wtx.Commit(ctx)
	if err != nil {
		// Outcome unknown until recovery decides it.
		return db.restart(ctx, err, true, tx.held())
	}
	db.maybeCheckpoint(ctx, lsn)
	return nil
}

// Rollback ends the transaction without its changes. A transaction that
// changed something is discarded the way Step 6.1 can: the database
// reopens, and recovery leaves the transaction out (section 2.6).
func (tx *Tx) Rollback(ctx context.Context) error {
	if tx.done {
		return sqlerr.New(sqlerr.InvalidTransactionState, "the transaction has already ended")
	}
	defer tx.end()
	if tx.wtx == nil || tx.discarded {
		return nil
	}
	return tx.db.discard(context.WithoutCancel(ctx), tx.wtx, tx.held())
}

// held returns how the transaction holds db.mu between statements.
func (tx *Tx) held() held {
	if tx.locked {
		return heldExclusiveByTx
	}
	return heldNone
}

// end marks the transaction over and releases its snapshot and the locks
// it holds.
func (tx *Tx) end() {
	tx.done = true
	if tx.snap != nil {
		tx.snap.Release()
		tx.snap = nil
	}
	if tx.locked {
		tx.locked = false
		tx.db.mu.Unlock()
	}
	if tx.writing {
		tx.writing = false
		tx.db.writer.Unlock()
	}
}
