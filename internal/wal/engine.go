package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"
	"sync/atomic"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/undo"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

// Names inside a database directory.
const (
	DataFileName = "data"
	WALDirName   = "wal"
)

// EngineOptions configures an Engine.
type EngineOptions struct {
	// Frames is the buffer pool size, at least 2 (logged heaps change up
	// to two pages at once). Zero means 256.
	Frames int
	// WAL configures the log.
	WAL Options
}

const defaultFrames = 256

// RecoveryStats describes what OpenEngine's recovery did.
type RecoveryStats struct {
	RedoLSN  LSN // where replay started
	EndLSN   LSN // where the log ended
	Replayed int // heap, B+Tree and undo page records replayed
	// Transactions that began but never committed, and their records,
	// which recovery left out.
	DiscardedTransactions int
	Discarded             int
	// NextXID is the transaction ID recovery resumed from.
	NextXID XID
	// Entries of deferred-free records replayed (pages put back on the
	// list to free).
	DeferredFrees int
	// UndoSegments is how many undo segments the segment table held after
	// recovery.
	UndoSegments int
}

// Engine is a database directory with crash recovery: a data file, its log,
// a buffer pool that obeys the WAL rule, and checkpoints. See
// docs/design/07-checkpoints-recovery.md. It does not know which heaps and
// trees exist; callers keep their first and root page IDs (until the
// catalog, Step 4.4).
type Engine struct {
	fsys vfs.FS
	dir  string
	w    *Writer
	dm   *storage.DiskManager
	bp   *storage.BufferPool
	lg   *Logger
	ck   *Checkpointer
	undo *undo.Log
	rec  RecoveryStats

	mu     sync.Mutex
	closed bool
	// The transaction table (docs/design/13-transactions.md section 2.3),
	// under mu.
	nextXID  XID
	active   map[XID]struct{}
	outcomes map[XID]outcome
	keepFrom LSN // outcomes that ended before this are forgotten next

	// stmtMu is the writer slot: held by a writing transaction from its
	// first write to its commit or rollback, so there is one at a time and
	// checkpoints wait for it.
	stmtMu sync.Mutex
	// horizon is the writing transaction's begin LSN, 0 if none: pages
	// with a higher LSN were changed by it and must not reach disk yet.
	horizon atomic.Uint64
}

// OpenEngine opens the database in dir, creating it if it does not exist,
// and recovers it: the log's torn tail is cut off, every record since the
// last checkpoint's redo point is replayed, and an end-of-recovery checkpoint
// is taken. Every change whose record was durable before a crash is then
// present, and no operation is partly visible.
func OpenEngine(ctx context.Context, fsys vfs.FS, dir string, opts EngineOptions) (*Engine, error) {
	if opts.Frames == 0 {
		opts.Frames = defaultFrames
	}
	if opts.Frames < 2 {
		return nil, fmt.Errorf("opening engine: %d frames, need at least 2: %w", opts.Frames, ErrInvalidOptions)
	}
	e := &Engine{fsys: fsys, dir: dir, nextXID: 1, active: map[XID]struct{}{}, outcomes: map[XID]outcome{}}
	if err := e.open(ctx, opts); err != nil {
		// Report failures to close what was opened too, after the cause.
		return nil, fmt.Errorf("opening engine %s: %w", dir, errors.Join(err, e.closeAll()))
	}
	return e, nil
}

func (e *Engine) open(ctx context.Context, opts EngineOptions) error {
	if err := e.fsys.MkdirAll(e.dir); err != nil {
		return err
	}
	if err := e.fsys.SyncDir(path.Dir(e.dir)); err != nil {
		return err
	}
	walDir := path.Join(e.dir, WALDirName)
	dataPath := path.Join(e.dir, DataFileName)

	// The data file is created before the log, so a log without a data
	// file means the data file was lost.
	dm, err := storage.Open(e.fsys, dataPath)
	if errors.Is(err, vfs.ErrNotExist) {
		if names, lerr := e.fsys.List(walDir); lerr == nil && len(names) > 0 {
			return fmt.Errorf("log exists but the data file is missing: %w", ErrCorrupt)
		}
		dm, err = storage.Create(e.fsys, dataPath)
	}
	if err != nil {
		return err
	}
	e.dm = dm

	if e.w, err = Open(e.fsys, walDir, opts.WAL); err != nil {
		return err
	}
	redo, err := RedoStart(e.fsys, e.dir, walDir)
	if err != nil {
		return err
	}
	flushed, force := RuleHooks(e.w)
	// No page changed by an unfinished transaction may reach disk: while
	// one is writing, the log counts as durable only up to its begin
	// record (docs/design/13-transactions.md section 2.6).
	clamped := func() uint64 {
		f := flushed()
		if h := e.horizon.Load(); h != 0 && h < f {
			return h
		}
		return f
	}
	if e.bp, err = storage.NewBufferPool(e.dm, storage.Options{Frames: opts.Frames, FlushedLSN: clamped, FlushWAL: force}); err != nil {
		return err
	}
	// The logger exists before replay so that replayed deferred-free
	// records can put their pages back on its list.
	e.lg = NewLogger(e.w, redo)
	// So does the undo log, for replayed segment table records.
	e.undo = undo.New(e.bp, e.lg)
	if err := e.replay(ctx, walDir, redo); err != nil {
		return err
	}
	// The undo pages are up to date: find where each segment ends.
	if err := e.undo.Recover(ctx); err != nil {
		return fmt.Errorf("recovery: %w: %w", ErrCorrupt, err)
	}
	e.rec.UndoSegments = len(e.undo.Segments())
	e.ck = NewCheckpointer(e.fsys, e.dir, e.w, e.lg, e.bp, e.dm)
	e.ck.nextXID = e.NextXID
	e.ck.relogUndo = e.undo.Relog
	// End-of-recovery checkpoint: the replayed pages reach disk and the
	// next recovery starts here.
	ctl, err := e.ck.Checkpoint(ctx)
	if err != nil {
		return fmt.Errorf("end-of-recovery checkpoint: %w", err)
	}
	e.pruneOutcomes(ctl.RedoLSN)
	return nil
}

// span is a range of LSNs [from, to).
type span struct{ from, to LSN }

// uncommitted reads the log from redo and finds the transactions that
// began and never committed: a TxnBegin followed by another TxnBegin, a
// checkpoint record or the end of the log (with one writer at a time, a
// transaction's records are contiguous). Their records must not be
// replayed. It also rebuilds the transaction table: NextXID past every ID
// in the log and the checkpoints, and the outcomes of the transactions in
// it (docs/design/13-transactions.md section 2.7).
func (e *Engine) uncommitted(walDir string, redo LSN) ([]span, error) {
	r, err := NewReader(e.fsys, walDir, redo)
	if err != nil {
		return nil, err
	}
	var out []span
	var open LSN // TxnBegin LSN of the open transaction, 0 if none
	var openXID, last XID
	next := XID(1)
	abandon := func(to LSN) {
		out = append(out, span{open, to})
		e.outcomes[openXID] = outcome{TxnAborted, to}
		open, openXID = 0, 0
	}
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("finding transactions: %w", err)
		}
		switch rec.Type {
		case RecordTxnBegin:
			xid, ok := decodeXID(rec.Payload)
			// IDs are allocated in increasing order and logged once.
			if !ok || xid <= last {
				return nil, fmt.Errorf("transaction begin at %d with ID %d after ID %d: %w", rec.LSN, xid, last, ErrCorrupt)
			}
			if open != 0 {
				abandon(rec.LSN)
			}
			open, openXID, last = rec.LSN, xid, xid
			next = max(next, xid+1)
		case RecordTxnCommit:
			xid, ok := decodeXID(rec.Payload)
			if !ok || open == 0 || xid != openXID {
				return nil, fmt.Errorf("transaction commit at %d does not match the open transaction: %w", rec.LSN, ErrCorrupt)
			}
			e.outcomes[xid] = outcome{TxnCommitted, rec.LSN}
			open, openXID = 0, 0
		case RecordCheckpoint:
			// A checkpoint never runs inside a writing transaction: one
			// that follows an open transaction means it was abandoned.
			if open != 0 {
				abandon(rec.LSN)
			}
			if len(rec.Payload) != checkpointPayloadSize {
				return nil, fmt.Errorf("checkpoint record %d: %w", rec.LSN, ErrCorrupt)
			}
			next = max(next, XID(binary.LittleEndian.Uint64(rec.Payload[8:])))
		case RecordHeap, RecordBTree, RecordDeferredFree, RecordUndo, RecordUndoSegment:
		default:
			// Checked here, not only in replay, so that a record of an
			// unknown type (or of format version 1, or TxnAbort, reserved)
			// is refused even inside a transaction that replay skips.
			return nil, fmt.Errorf("record %d has unknown type %d: %w", rec.LSN, rec.Type, ErrCorrupt)
		}
	}
	if open != 0 {
		abandon(r.End())
	}
	e.nextXID = next
	e.rec.NextXID = next
	return out, nil
}

// decodeXID decodes a transaction record's payload. (ID 0 needs no check
// of its own: a begin must name an ID above every earlier one, all above
// 0, and a commit the open transaction's.)
func decodeXID(p []byte) (XID, bool) {
	if len(p) != 8 {
		return 0, false
	}
	return XID(binary.LittleEndian.Uint64(p)), true
}

// replay redoes every record from redo to the end of the log, except those
// of transactions that never committed.
func (e *Engine) replay(ctx context.Context, walDir string, redo LSN) error {
	skip, err := e.uncommitted(walDir, redo)
	if err != nil {
		return err
	}
	e.rec.DiscardedTransactions = len(skip)
	r, err := NewReader(e.fsys, walDir, redo)
	if err != nil {
		return err
	}
	e.rec.RedoLSN = redo
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("replay: %w", err)
		}
		for len(skip) > 0 && rec.LSN >= skip[0].to {
			skip = skip[1:]
		}
		if len(skip) > 0 && rec.LSN >= skip[0].from {
			e.rec.Discarded++
			continue
		}
		switch rec.Type {
		case RecordHeap:
			if err := storage.RedoHeapRecord(ctx, e.bp, uint64(rec.LSN), rec.Payload); err != nil {
				return fmt.Errorf("replay: %w: %w", ErrCorrupt, err)
			}
			e.rec.Replayed++
		case RecordBTree:
			if err := btree.Redo(ctx, e.bp, uint64(rec.LSN), rec.Payload); err != nil {
				return fmt.Errorf("replay: %w: %w", ErrCorrupt, err)
			}
			e.rec.Replayed++
		case RecordUndo:
			if err := undo.Redo(ctx, e.bp, uint64(rec.LSN), rec.Payload); err != nil {
				return fmt.Errorf("replay: %w: %w", ErrCorrupt, err)
			}
			e.rec.Replayed++
		case RecordUndoSegment:
			if err := e.undo.RedoSegments(rec.Payload); err != nil {
				return fmt.Errorf("replay: record %d: %w: %w", rec.LSN, ErrCorrupt, err)
			}
		case RecordDeferredFree:
			// Pages waiting to be freed: back on the list.
			frees, err := DecodeDeferredFree(rec.Payload, rec.LSN)
			if err != nil {
				return fmt.Errorf("replay: record %d: %w", rec.LSN, err)
			}
			for _, d := range frees {
				// The page was allocated before whatever unlinked it.
				if d.page >= e.dm.PageCount() {
					return fmt.Errorf("replay: record %d defers page %d, past the end of the data file: %w", rec.LSN, d.page, ErrCorrupt)
				}
				e.lg.restore(d.page, d.lsn)
			}
			e.rec.DeferredFrees += len(frees)
		case RecordCheckpoint, RecordTxnBegin, RecordTxnCommit:
			// Nothing to redo.
		default:
			return fmt.Errorf("replay: record %d has unknown type %d: %w", rec.LSN, rec.Type, ErrCorrupt)
		}
	}
	// The writer cut the tail at the same place the reader stopped; any
	// difference means the log changed under us or the two disagree.
	if r.End() != e.w.EndLSN() {
		return fmt.Errorf("replay ended at %d, log ends at %d: %w", r.End(), e.w.EndLSN(), ErrCorrupt)
	}
	e.rec.EndLSN = r.End()
	return nil
}

// Abandon closes the engine as a crash would: the buffer pool is dropped
// without being written, so an unfinished transaction's changes are lost
// and recovery at the next OpenEngine discards its records. Use it when a
// writing transaction fails or rolls back (Txn.Rollback calls it).
func (e *Engine) Abandon() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	e.closed = true
	e.mu.Unlock()
	return e.closeAll()
}

// Recovery returns what recovery did when the engine was opened.
func (e *Engine) Recovery() RecoveryStats { return e.rec }

// Pool returns the buffer pool.
func (e *Engine) Pool() *storage.BufferPool { return e.bp }

// CreateHeap creates a new, logged heap. Its first page ID identifies it; it
// survives a crash once a Flush has returned after CreateHeap.
func (e *Engine) CreateHeap(ctx context.Context) (*storage.Heap, error) {
	if err := e.check(); err != nil {
		return nil, err
	}
	return storage.CreateHeap(ctx, e.bp, storage.WithLogger(e.lg))
}

// OpenHeap opens the logged heap whose first page is first.
func (e *Engine) OpenHeap(ctx context.Context, first uint64) (*storage.Heap, error) {
	if err := e.check(); err != nil {
		return nil, err
	}
	return storage.OpenHeap(ctx, e.bp, first, storage.WithLogger(e.lg))
}

// CreateBTree creates a new, logged B+Tree. Its root page ID identifies it;
// it survives a crash once a Flush has returned after CreateBTree.
func (e *Engine) CreateBTree(ctx context.Context) (*btree.Tree, error) {
	if err := e.check(); err != nil {
		return nil, err
	}
	return btree.Create(ctx, e.bp, btree.WithLogger(e.lg))
}

// OpenBTree opens the logged B+Tree whose root page is root.
func (e *Engine) OpenBTree(ctx context.Context, root uint64) (*btree.Tree, error) {
	if err := e.check(); err != nil {
		return nil, err
	}
	return btree.Open(ctx, e.bp, root, btree.WithLogger(e.lg))
}

// Undo returns the engine's undo log. Its changes are logged like any
// other; write it inside a transaction, so that an uncommitted
// transaction's undo is discarded by recovery with its other changes.
func (e *Engine) Undo() *undo.Log { return e.undo }

// Logger returns the engine's logger.
func (e *Engine) Logger() *Logger { return e.lg }

// FreePageCount returns how many pages of the data file are free.
func (e *Engine) FreePageCount() uint64 { return e.dm.FreePageCount() }

// Flush makes every change made so far durable; it is the point at which
// changes are acknowledged (later, COMMIT).
func (e *Engine) Flush(ctx context.Context) error {
	if err := e.check(); err != nil {
		return err
	}
	return e.w.Flush(ctx)
}

// Checkpoint takes a checkpoint. It waits for a writing transaction to
// end: a checkpoint must not write that transaction's pages.
func (e *Engine) Checkpoint(ctx context.Context) (Control, error) {
	if err := e.check(); err != nil {
		return Control{}, err
	}
	e.stmtMu.Lock()
	defer e.stmtMu.Unlock()
	ctl, err := e.ck.Checkpoint(ctx)
	if err == nil {
		e.pruneOutcomes(ctl.RedoLSN)
	}
	return ctl, err
}

func (e *Engine) check() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}
	return nil
}

// Close takes a final checkpoint and closes everything. No heap may be in
// use. Even if the checkpoint fails, the files are closed; nothing
// acknowledged is lost, because the log has it.
func (e *Engine) Close(ctx context.Context) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	e.closed = true
	e.mu.Unlock()
	if e.horizon.Load() != 0 {
		// A transaction is writing: closing normally would write its pages.
		return errors.Join(fmt.Errorf("closing: %w", ErrTxnOpen), e.closeAll())
	}
	_, err := e.ck.Checkpoint(ctx)
	if err == nil {
		err = e.bp.Close(ctx)
	}
	return errors.Join(err, e.closeAll())
}

// closeAll closes whatever was opened, ignoring what was not.
func (e *Engine) closeAll() error {
	var errs []error
	if e.w != nil {
		if err := e.w.Close(context.Background()); err != nil && !errors.Is(err, ErrClosed) {
			errs = append(errs, err)
		}
	}
	if e.dm != nil {
		if err := e.dm.Close(); err != nil && !errors.Is(err, storage.ErrDiskClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
