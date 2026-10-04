// Package executor runs SQL statements against a NoVacDB database: it
// binds them against the catalog, plans scans, evaluates expressions, and
// applies changes as atomic WAL statement groups. See
// docs/design/10-executor.md, section 2.6.
package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/catalog"
	"github.com/vikrant-choudhary06/NoVacDB/internal/mvcc"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/ast"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/parser"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/undo"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

// Defaults for Options.
const (
	DefaultFrames          = 4096     // 32 MiB of pages
	DefaultCheckpointBytes = 64 << 20 // of WAL between checkpoints
)

// Options configure a database.
type Options struct {
	// Frames is the buffer pool size in pages; it bounds how many pages
	// one statement can change. Zero means DefaultFrames.
	Frames int
	// CheckpointBytes is how much WAL may grow before a write statement
	// triggers a checkpoint. Zero means DefaultCheckpointBytes.
	CheckpointBytes int64
	// WAL configures the log; zero values take the log's defaults.
	WAL wal.Options
	// Now gives the time for now(); nil means time.Now.
	Now func() time.Time
	// Logger receives operational messages; nil means slog.Default().
	Logger *slog.Logger
}

// Column describes an output column.
type Column struct {
	Name string
	Type types.Type
}

// Result is one statement's outcome.
type Result struct {
	// Tag is PostgreSQL's command tag: "SELECT 2", "INSERT 0 1",
	// "CREATE TABLE", ...
	Tag     string
	Columns []Column        // SELECT only, and non-nil for every SELECT
	Rows    [][]types.Value // SELECT only
	Notices []Notice
}

// Notice is a message about a statement that succeeded, such as "relation
// "t" already exists, skipping", with its SQLSTATE.
type Notice struct {
	Code    string
	Message string
}

// DB is an open database. It is safe for concurrent use: SELECTs run
// concurrently, and every statement that changes something runs alone.
type DB struct {
	fsys vfs.FS
	dir  string
	opts Options
	log  *slog.Logger

	mu       sync.RWMutex // shared for SELECT, exclusive for changes
	e        *wal.Engine
	cat      *catalog.Catalog
	closed   bool
	broken   error   // set when a self-restart failed
	lastCkpt wal.LSN // the WAL position of the last checkpoint

	// noIndexScans makes every scan sequential (tests compare the two).
	noIndexScans bool
	indexScans   atomic.Int64 // scans that used an index
	rowsRead     atomic.Int64 // rows scans have read, before WHERE
	restarts     atomic.Int64 // self-restarts after failed statements
	rollbacks    atomic.Int64 // rollbacks of transactions that wrote
}

// Open opens the database in dir, creating it if it does not exist, and
// recovers it after a crash.
func Open(ctx context.Context, fsys vfs.FS, dir string, opts Options) (*DB, error) {
	if opts.Frames == 0 {
		opts.Frames = DefaultFrames
	}
	if opts.CheckpointBytes == 0 {
		opts.CheckpointBytes = DefaultCheckpointBytes
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	db := &DB{fsys: fsys, dir: dir, opts: opts, log: opts.Logger}
	if db.log == nil {
		db.log = slog.Default()
	}
	if err := db.open(ctx); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) open(ctx context.Context) error {
	e, err := wal.OpenEngine(ctx, db.fsys, db.dir, wal.EngineOptions{Frames: db.opts.Frames, WAL: db.opts.WAL})
	if err != nil {
		return fmt.Errorf("opening the database: %w", err)
	}
	cat, err := catalog.Open(ctx, e, db.fsys, db.dir)
	if err != nil {
		// Creating the catalog may have left a statement open: drop
		// everything as a crash would.
		_ = e.Abandon()
		return fmt.Errorf("opening the catalog: %w", err)
	}
	db.e, db.cat = e, cat
	// Opening ends with a checkpoint at the end of the recovered log.
	db.lastCkpt = e.Recovery().EndLSN
	if rec := e.Recovery(); rec.Replayed > 0 || rec.DiscardedTransactions > 0 {
		db.log.Info("recovered the database", "dir", db.dir, "replayed", rec.Replayed, "discarded_transactions", rec.DiscardedTransactions, "next_xid", rec.NextXID)
	}
	return nil
}

// Close checkpoints and closes the database. Statements after Close fail.
func (db *DB) Close(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return sqlerr.New(sqlerr.ObjectNotInPrerequisiteState, "the database is closed")
	}
	db.closed = true
	if db.broken != nil {
		return nil // nothing is open
	}
	return db.e.Close(ctx)
}

// Exec runs the statements in sql in order, each in its own transaction
// (autocommit), and returns one result per statement. It stops at the
// first error, returning the results of the statements before it; each
// statement that succeeded stays committed. Errors are *sqlerr.Error.
func (db *DB) Exec(ctx context.Context, sql string) ([]*Result, error) {
	stmts, err := parser.Parse(sql)
	if err != nil {
		return nil, sqlerr.From(err)
	}
	var results []*Result
	for _, s := range stmts {
		r, err := db.run(ctx, nil, sql, s, nil, false)
		if err != nil {
			return results, err
		}
		results = append(results, r)
	}
	return results, nil
}

func canceled(err error) *sqlerr.Error {
	return sqlerr.Wrap(err, sqlerr.QueryCanceled, "canceling statement due to user request")
}

// run runs one statement under the database lock, in the transaction tx
// or, if tx is nil, in its own (autocommit), with its parameters if it has
// any. With describe set it only binds the statement, to learn its
// parameters' types and result columns: nothing is read or changed, and a
// statement that is not a query or a row change is not looked at.
//
// A statement that may change data takes the lock exclusively; in a
// transaction it keeps it until the transaction ends (tx.go).
func (db *DB) run(ctx context.Context, tx *Tx, sql string, s ast.Stmt, ps *params, describe bool) (*Result, error) {
	_, isSelect := s.(*ast.Select)
	switch {
	case tx != nil && tx.locked:
	case isSelect || describe:
		db.mu.RLock()
		defer db.mu.RUnlock()
	case tx != nil:
		db.mu.Lock()
		tx.locked = true
	default:
		db.mu.Lock()
		defer db.mu.Unlock()
	}
	switch {
	case db.closed:
		return nil, sqlerr.New(sqlerr.ObjectNotInPrerequisiteState, "the database is closed")
	case db.broken != nil:
		return nil, sqlerr.Wrap(db.broken, sqlerr.IOError, "the database is unavailable: restarting it after a failure failed: %v", db.broken)
	}
	if err := ctx.Err(); err != nil {
		return nil, canceled(err)
	}
	snap, err := db.snapshot(tx)
	if err != nil {
		return nil, err
	}
	if tx == nil || tx.isolation != RepeatableRead {
		defer snap.Release()
	}
	st := &stmt{db: db, tx: tx, ctx: ctx, sql: sql, ec: &evalCtx{now: types.TimestampFromTime(db.opts.Now())}, params: ps, describe: describe, snap: snap}
	var r *Result
	switch s.(type) {
	case *ast.CreateTable, *ast.DropTable, *ast.CreateIndex, *ast.DropIndex:
		if describe {
			return &Result{}, nil
		}
	}
	switch s := s.(type) {
	case *ast.Select:
		r, err = st.selectStmt(s)
	case *ast.Insert:
		r, err = st.insert(s)
	case *ast.Update:
		r, err = st.update(s)
	case *ast.Delete:
		r, err = st.delete(s)
	case *ast.CreateTable:
		r, err = st.createTable(s)
	case *ast.DropTable:
		r, err = st.dropTable(s)
	case *ast.CreateIndex:
		r, err = st.createIndex(s)
	case *ast.DropIndex:
		r, err = st.dropIndex(s)
	default:
		err = sqlerr.New(sqlerr.FeatureNotSupported, "statement not supported: %s", s)
	}
	if err != nil {
		return nil, publicError(err)
	}
	return r, nil
}

// publicError turns any error into a *sqlerr.Error.
func publicError(err error) *sqlerr.Error {
	var se *sqlerr.Error
	switch {
	case errors.As(err, &se):
		return se
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return canceled(err)
	case isCorruption(err):
		return sqlerr.Wrap(err, sqlerr.DataCorrupted, "data corrupted: %v", err)
	}
	return sqlerr.Wrap(err, sqlerr.IOError, "could not access the database: %v", err)
}

func isCorruption(err error) bool {
	for _, c := range []error{storage.ErrCorrupt, storage.ErrCorruptHeap, storage.ErrChecksum, storage.ErrBadPageType,
		storage.ErrZeroPage, storage.ErrInvalidRID, btree.ErrCorruptNode, btree.ErrBadKey, wal.ErrCorrupt,
		undo.ErrCorrupt, mvcc.ErrCorrupt, mvcc.ErrBadImage} {
		if errors.Is(err, c) {
			return true
		}
	}
	return false
}

// stmt is one statement being run.
type stmt struct {
	db       *DB
	tx       *Tx // nil for autocommit
	ctx      context.Context
	sql      string
	ec       *evalCtx
	params   *params // nil for a statement without parameters
	describe bool    // bind only: see run
	// snap is what the statement sees: its own snapshot, or its
	// REPEATABLE READ transaction's (docs/design/16-snapshots-
	// visibility.md).
	snap *wal.Snapshot
}

// snapshot returns the snapshot a statement of tx (nil: autocommit) reads
// with: the transaction's for REPEATABLE READ, taken at its first
// statement, else a new one. A REPEATABLE READ transaction whose snapshot
// belongs to an engine since abandoned (a rollback or a restart reopened
// the database) can no longer read consistently: 40001.
func (db *DB) snapshot(tx *Tx) (*wal.Snapshot, error) {
	if tx == nil || tx.isolation != RepeatableRead {
		return db.e.Snapshot(), nil
	}
	if tx.snap == nil {
		tx.snap = db.e.Snapshot()
	} else if tx.snap.Engine() != db.e {
		return nil, sqlerr.New(sqlerr.SerializationFailure, "could not serialize access: the database restarted during the transaction").
			WithHint("Retry the transaction.")
	}
	return tx.snap, nil
}

// me returns the statement's transaction ID, 0 if it has not written.
func (st *stmt) me() uint64 {
	if st.tx != nil && st.tx.wtx != nil {
		return uint64(st.tx.wtx.XID())
	}
	return 0
}

// reader reads row versions as the statement sees them.
func (st *stmt) reader() mvcc.Reader {
	return mvcc.Reader{Undo: st.db.e.Undo(), Snap: st.snap, Me: st.me()}
}

// writer writes rows of tbl in transaction xid. Under REPEATABLE READ it
// checks that the rows it changes have not changed since the snapshot.
func (st *stmt) writer(xid wal.XID, tableID uint64) mvcc.Writer {
	w := mvcc.Writer{Undo: st.db.e.Undo(), XID: uint64(xid), Table: tableID}
	if st.tx != nil && st.tx.isolation == RepeatableRead {
		w.Snap = st.snap
	}
	return w
}

// indexesUsable reports whether the statement may read through indexes:
// its snapshot sees every transaction that has written, so the indexes,
// which describe the newest versions, describe what it sees (section 2.5).
func (st *stmt) indexesUsable() bool {
	last := uint64(st.db.e.LastWriter())
	return last == 0 || last == st.me() || st.snap.Sees(last)
}

// binder returns a binder for the statement's expressions that see no
// table.
func (st *stmt) binder() *binder { return &binder{sql: st.sql, params: st.params} }

// tooMuch is the error for a statement, or a transaction, that changes
// more pages than the buffer pool holds.
func (db *DB) tooMuch(err error, inTx bool) *sqlerr.Error {
	if inTx {
		return sqlerr.Wrap(err, sqlerr.ProgramLimitExceeded, "transaction changes too much data").
			WithHint("A transaction can change at most %d pages (the buffer pool's size) until the undo log arrives; split it into smaller transactions. It was rolled back.", db.opts.Frames)
	}
	return sqlerr.Wrap(err, sqlerr.ProgramLimitExceeded, "statement changes too much data").
		WithHint("A statement can change at most %d pages (the buffer pool's size) until the undo log arrives; split it into smaller statements.", db.opts.Frames)
}

// apply runs fn, the change phase of a statement, as a write of its
// transaction: in autocommit it commits it at once, in an explicit
// transaction the transaction commits later. The caller holds the
// exclusive lock and has already checked everything that can fail for SQL
// reasons.
//
// If anything fails once the transaction has begun writing, its changes
// cannot be undone in memory: the database restarts itself (closing as a
// crash would, then reopening, which discards the transaction) and returns
// the error. With ddl set, a *sqlerr.Error from fn means fn changed
// nothing (the catalog's rule), so the transaction goes on and the error
// is returned.
//
// Cancellation is not honoured from here on: a statement that has begun
// changing data runs to its end.
func (st *stmt) apply(ddl bool, fn func(ctx context.Context, xid wal.XID) error) error {
	db := st.db
	ctx := context.WithoutCancel(st.ctx)
	var wtx *wal.Txn
	if st.tx != nil {
		if st.tx.wtx == nil {
			st.tx.wtx = db.e.Begin()
		}
		wtx = st.tx.wtx
	} else {
		wtx = db.e.Begin()
	}
	var se *sqlerr.Error
	err := wtx.Write(ctx, func(ctx context.Context) error {
		ferr := fn(ctx, wtx.XID())
		if ddl && errors.As(ferr, &se) {
			return wal.Unchanged(ferr)
		}
		return ferr
	})
	switch {
	case err == nil:
	case ddl && errors.As(err, &se):
		if st.tx == nil {
			if _, cerr := wtx.Commit(ctx); cerr != nil {
				return db.restart(ctx, cerr, false)
			}
		}
		return se
	case errors.Is(err, mvcc.ErrWriteConflict):
		// REPEATABLE READ: the transaction cannot go on; its changes go
		// as a rollback's do.
		st.tx.discarded = true
		if derr := db.discard(ctx, wtx); derr != nil {
			return derr
		}
		return sqlerr.Wrap(err, sqlerr.SerializationFailure, "could not serialize access due to concurrent update").
			WithHint("Retry the transaction.")
	default:
		if st.tx != nil {
			st.tx.discarded = true
		}
		return db.restart(ctx, err, st.tx != nil)
	}
	if st.tx != nil {
		return nil
	}
	// The statement has finished reading: its snapshot must not hold back
	// its own undo at commit (section 2.6).
	st.snap.Release()
	lsn, err := wtx.Commit(ctx)
	if err != nil {
		return db.restart(ctx, err, false)
	}
	db.maybeCheckpoint(ctx, lsn)
	return nil
}

// restart abandons the engine and reopens the database after a failed
// statement or commit, and returns the error for the client. The writing
// transaction, if any, is discarded by recovery.
func (db *DB) restart(ctx context.Context, cause error, inTx bool) error {
	db.log.Warn("a change failed; restarting the database", "dir", db.dir, "err", cause)
	db.restarts.Add(1)
	_ = db.e.Abandon()
	db.reopen(ctx)
	if errors.Is(cause, storage.ErrNoFreeFrames) {
		return db.tooMuch(cause, inTx)
	}
	return publicError(cause)
}

// discard rolls back a transaction that changed something: the engine is
// abandoned and the database reopens, and recovery leaves it out (Step 6.1
// has no undo: docs/design/13-transactions.md section 2.6).
func (db *DB) discard(ctx context.Context, wtx *wal.Txn) error {
	db.log.Info("rolling back a transaction: reopening the database", "dir", db.dir, "xid", wtx.XID())
	db.rollbacks.Add(1)
	_ = wtx.Rollback(ctx)
	db.reopen(ctx)
	if db.broken != nil {
		return sqlerr.Wrap(db.broken, sqlerr.IOError, "the database is unavailable: reopening it after a rollback failed: %v", db.broken)
	}
	return nil
}

// reopen opens the database again after the engine was abandoned.
func (db *DB) reopen(ctx context.Context) {
	db.e, db.cat = nil, nil
	if err := db.open(ctx); err != nil {
		db.broken = err
		db.log.Error("reopening the database failed", "dir", db.dir, "err", err)
	}
}

// maybeCheckpoint takes a checkpoint once the WAL has grown enough since
// the last one. The statement has committed, so a failure is only logged;
// the next checkpoint tries again.
func (db *DB) maybeCheckpoint(ctx context.Context, lsn wal.LSN) {
	if int64(lsn-db.lastCkpt) < db.opts.CheckpointBytes {
		return
	}
	c, err := db.e.Checkpoint(ctx)
	if err != nil {
		db.log.Warn("checkpoint failed", "dir", db.dir, "err", err)
		return
	}
	db.lastCkpt = c.CheckpointLSN
}
