package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/vikrant-choudhary06/NoVacDB/internal/undo"
)

// XID is a transaction ID (docs/design/13-transactions.md section 2.1):
// 64 bits, allocated in increasing order on a transaction's first write,
// never reused. 0 means "no transaction".
type XID uint64

// MaxXID is the largest transaction ID; allocation never passes it.
const MaxXID = XID(math.MaxUint64)

// TxnStatus is what the status table knows of a transaction ID (section
// 2.3).
type TxnStatus int

// Transaction statuses.
const (
	// TxnUnknown: 0, or an ID not allocated yet.
	TxnUnknown TxnStatus = iota
	// TxnActive: begun, not yet committed or rolled back.
	TxnActive
	// TxnCommitted: its commit record is durable.
	TxnCommitted
	// TxnAborted: rolled back, or found by recovery without a commit.
	TxnAborted
	// TxnResolved: older than the outcomes the table keeps; committed, or
	// aborted with every change removed.
	TxnResolved
)

func (s TxnStatus) String() string {
	switch s {
	case TxnActive:
		return "active"
	case TxnCommitted:
		return "committed"
	case TxnAborted:
		return "aborted"
	case TxnResolved:
		return "resolved"
	}
	return "unknown"
}

// Errors of transactions.
var (
	// ErrTxnDone means the transaction already committed or rolled back.
	ErrTxnDone = errors.New("wal: transaction already ended")
	// ErrTxnFailed means logging for the transaction, or its rollback,
	// failed: it can only be abandoned (Abandon).
	ErrTxnFailed = errors.New("wal: transaction failed; it can only be abandoned")
	// ErrTxnAborting means a change of the transaction failed: it can only
	// roll back (RollbackTo or Rollback).
	ErrTxnAborting = errors.New("wal: a change of the transaction failed; it can only roll back")
	// ErrXIDExhausted means every transaction ID has been used.
	ErrXIDExhausted = errors.New("wal: transaction IDs exhausted")
	// ErrTxnOpen means the engine was closed while a transaction was
	// writing; its changes are discarded, as by a crash.
	ErrTxnOpen = errors.New("wal: a writing transaction is open")
)

// outcome is how a transaction ended, and where in the log.
type outcome struct {
	status TxnStatus
	end    LSN
}

type txnState int

const (
	txnOpen txnState = iota
	// txnAborting: a change failed, leaving every page consistent; only
	// a rollback may follow (docs/design/17-rollback.md section 2.4).
	txnAborting
	// txnFailed: logging, or a rollback, failed; only Abandon is left.
	txnFailed
	txnCommitted
	txnRolledBack
)

// Txn is a transaction (section 2.4). It is not safe for concurrent use.
type Txn struct {
	e     *Engine
	xid   XID
	state txnState
}

// Begin starts a transaction. It takes no ID and logs nothing until its
// first Write.
func (e *Engine) Begin() *Txn { return &Txn{e: e} }

// XID returns the transaction's ID, 0 until its first Write.
func (t *Txn) XID() XID { return t.xid }

// unchangedError marks an error after which nothing was changed.
type unchangedError struct{ err error }

func (u *unchangedError) Error() string { return u.err.Error() }
func (u *unchangedError) Unwrap() error { return u.err }

// Unchanged wraps an error returned by a Write function that changed
// nothing before failing: the transaction stays usable, and Write returns
// err itself.
func Unchanged(err error) error { return &unchangedError{err} }

// Write runs fn, one change, as part of the transaction. On the first
// Write the transaction waits for the engine's writer slot, takes its ID
// and logs TxnBegin; the pages it changes cannot reach disk until it
// commits. If fn fails (unless with Unchanged), the transaction can only
// roll back; if logging TxnBegin fails, it can only be abandoned.
func (t *Txn) Write(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := t.usable(); err != nil {
		return err
	}
	if t.xid == 0 {
		if err := t.begin(ctx); err != nil {
			return err
		}
	}
	err := fn(ctx)
	var u *unchangedError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &u):
		return u.err
	}
	t.state = txnAborting
	return err
}

// usable checks that the transaction may change data or commit.
func (t *Txn) usable() error {
	switch t.state {
	case txnAborting:
		return ErrTxnAborting
	case txnFailed:
		return ErrTxnFailed
	case txnCommitted, txnRolledBack:
		return ErrTxnDone
	}
	return nil
}

// begin takes the writer slot, allocates the ID and logs TxnBegin.
func (t *Txn) begin(ctx context.Context) error {
	e := t.e
	if err := e.check(); err != nil {
		return err
	}
	e.stmtMu.Lock()
	e.mu.Lock()
	xid := e.nextXID
	if xid == MaxXID {
		e.mu.Unlock()
		e.stmtMu.Unlock()
		return fmt.Errorf("beginning a transaction: %w", ErrXIDExhausted)
	}
	// The ID is active as soon as it is allocated: a snapshot taken
	// between the two would see it as committed.
	e.nextXID++
	e.active[xid] = struct{}{}
	e.lastWriter = xid
	e.mu.Unlock()
	lsn, err := e.w.Append(ctx, RecordTxnBegin, binary.LittleEndian.AppendUint64(nil, uint64(xid)))
	if err != nil {
		// The slot stays taken and the ID active: the engine must be
		// abandoned (Rollback).
		t.state, t.xid = txnFailed, xid
		return fmt.Errorf("beginning a transaction: %w", err)
	}
	t.xid = xid
	e.horizon.Store(uint64(lsn))
	return nil
}

// Commit commits the transaction and returns its commit record's LSN (0 for
// a transaction that never wrote): it logs TxnCommit and makes the log
// durable through it, then releases the undo no snapshot needs, this
// transaction's included. If logging fails, the outcome is unknown until
// recovery; if releasing fails, the transaction has committed (the LSN is
// returned with the error). Either way the caller must abandon the engine.
func (t *Txn) Commit(ctx context.Context) (LSN, error) {
	if err := t.usable(); err != nil {
		return 0, err
	}
	if t.xid == 0 {
		t.state = txnCommitted
		return 0, nil
	}
	e := t.e
	lsn, err := e.w.Append(ctx, RecordTxnCommit, binary.LittleEndian.AppendUint64(nil, uint64(t.xid)))
	if err == nil {
		err = e.w.FlushTo(ctx, lsn)
	}
	if err != nil {
		t.state = txnFailed
		return 0, fmt.Errorf("committing transaction %d: %w", t.xid, err)
	}
	e.mu.Lock()
	delete(e.active, t.xid)
	e.outcomes[t.xid] = outcome{TxnCommitted, lsn}
	e.mu.Unlock()
	t.state = txnCommitted
	e.horizon.Store(0)
	// Undo is released only now, still in the writer slot
	// (docs/design/16-snapshots-visibility.md section 2.6): every snapshot
	// taken from here on sees this transaction, so only the live ones can
	// need its undo. Released before the commit record, the segment could
	// be gone for a snapshot taken before the transaction left the active
	// set. A crash in between loses nothing: recovery releases all undo.
	if err := e.releaseUndo(ctx); err != nil {
		return lsn, fmt.Errorf("transaction %d committed; releasing undo: %w", t.xid, err)
	}
	e.stmtMu.Unlock()
	return lsn, nil
}

// Savepoint returns where the transaction's undo ends now: its latest
// undo record, 0 if none (docs/design/17-rollback.md section 2.4).
func (t *Txn) Savepoint() undo.Ptr {
	if t.xid == 0 {
		return 0
	}
	return t.e.undo.Last(uint64(t.xid))
}

// Undoer reverses a transaction's undo records from from, its latest, back
// to, but not including, to (mvcc.Undo walks them).
type Undoer func(ctx context.Context, from, to undo.Ptr) error

// RollbackTo undoes the transaction's changes after savepoint sp with fn,
// as a write of the transaction, after which its undo ends at sp again and
// it may go on. If fn fails, the transaction can only be abandoned
// (section 2.5).
func (t *Txn) RollbackTo(ctx context.Context, sp undo.Ptr, fn Undoer) error {
	switch t.state {
	case txnFailed:
		return ErrTxnFailed
	case txnCommitted, txnRolledBack:
		return ErrTxnDone
	}
	if err := t.undo(ctx, sp, fn); err != nil {
		return err
	}
	t.state = txnOpen
	return nil
}

// undo runs fn back to sp and rewinds the undo to it; on failure the
// transaction is failed.
func (t *Txn) undo(ctx context.Context, sp undo.Ptr, fn Undoer) error {
	from := t.Savepoint()
	if from == sp {
		return nil
	}
	err := fn(ctx, from, sp)
	if err == nil {
		err = t.e.undo.Rewind(ctx, uint64(t.xid), sp)
	}
	if err != nil {
		t.state = txnFailed
		return fmt.Errorf("rolling back transaction %d: %w", t.xid, err)
	}
	return nil
}

// Rollback ends the transaction without its changes (section 2.6). A
// transaction that never wrote just ends. Otherwise fn undoes all its
// changes, as a write of the transaction; then TxnAbort is logged and made
// durable, the transaction ends as aborted, and its undo is released once
// no snapshot can need it. If any of that fails, the transaction can only
// be abandoned (Abandon), and recovery discards it.
func (t *Txn) Rollback(ctx context.Context, fn Undoer) error {
	switch t.state {
	case txnFailed:
		return ErrTxnFailed
	case txnCommitted, txnRolledBack:
		return ErrTxnDone
	}
	if t.xid == 0 {
		t.state = txnRolledBack
		return nil
	}
	if err := t.undo(ctx, 0, fn); err != nil {
		return err
	}
	e := t.e
	lsn, err := e.w.Append(ctx, RecordTxnAbort, binary.LittleEndian.AppendUint64(nil, uint64(t.xid)))
	if err == nil {
		// Durable before any page of the transaction may reach disk.
		err = e.w.FlushTo(ctx, lsn)
	}
	if err != nil {
		t.state = txnFailed
		return fmt.Errorf("rolling back transaction %d: %w", t.xid, err)
	}
	e.mu.Lock()
	delete(e.active, t.xid)
	e.outcomes[t.xid] = outcome{TxnAborted, lsn}
	e.mu.Unlock()
	t.state = txnRolledBack
	e.horizon.Store(0)
	if err := e.releaseUndo(ctx); err != nil {
		return fmt.Errorf("transaction %d rolled back; releasing undo: %w", t.xid, err)
	}
	e.stmtMu.Unlock()
	return nil
}

// Abandon ends the transaction the way Step 6.1 rolled back: a transaction
// that wrote leaves the engine abandoned, as a crash would, and the caller
// must reopen it; recovery discards the transaction. It is the way out of
// a failed transaction, or one whose rollback cannot be done (a schema
// change, docs/design/17-rollback.md section 2.7).
func (t *Txn) Abandon() error {
	switch t.state {
	case txnCommitted, txnRolledBack:
		return ErrTxnDone
	}
	t.state = txnRolledBack
	if t.xid == 0 {
		return nil
	}
	return t.e.Abandon()
}

// Status returns what the status table knows of xid.
func (e *Engine) Status(xid XID) TxnStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	if xid == 0 || xid >= e.nextXID {
		return TxnUnknown
	}
	if _, ok := e.active[xid]; ok {
		return TxnActive
	}
	if o, ok := e.outcomes[xid]; ok {
		return o.status
	}
	return TxnResolved
}

// NextXID returns the ID the next writing transaction will get.
func (e *Engine) NextXID() XID {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.nextXID
}

// pruneOutcomes forgets the outcomes of transactions that ended before
// the previous checkpoint's redo point, and remembers redo for the next
// call: the table keeps two checkpoint intervals, so outcomes recovery
// found survive its own end-of-recovery checkpoint (implementation notes,
// section 2.8).
func (e *Engine) pruneOutcomes(redo LSN) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for xid, o := range e.outcomes {
		if o.end < e.keepFrom {
			delete(e.outcomes, xid)
		}
	}
	e.keepFrom = redo
}
