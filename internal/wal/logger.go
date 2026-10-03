package wal

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Record types written by NoVacDB itself. See
// docs/design/07-checkpoints-recovery.md.
const (
	// RecordHeap is a heap operation (storage.DecodeHeapRecord).
	RecordHeap RecordType = 1
	// RecordCheckpoint marks a completed checkpoint; its payload is the
	// redo LSN and the next transaction ID, u64 each.
	RecordCheckpoint RecordType = 2
	// RecordBTree is a B+Tree change (btree.DecodeRecord).
	RecordBTree RecordType = 3
	// Types 4 and 5 were statement groups (format version 1), replaced by
	// transactions.

	// RecordDeferredFree lists pages to free once a checkpoint's redo point
	// is past a given LSN (EncodeDeferredFree).
	RecordDeferredFree RecordType = 6
	// RecordTxnBegin starts a writing transaction; its payload is the XID
	// (u64).
	RecordTxnBegin RecordType = 7
	// RecordTxnCommit commits a transaction; its payload is the XID.
	RecordTxnCommit RecordType = 8
	// RecordTxnAbort is reserved for Step 6.5's rollback; nothing writes
	// it yet, and recovery refuses it as an unknown type.
	RecordTxnAbort RecordType = 9
	// RecordUndo is a change to undo pages (undo.DecodeBlocks; format
	// version 3, docs/design/14-undo-log.md).
	RecordUndo RecordType = 10
	// RecordUndoSegment changes the undo segment table
	// (undo.DecodeSegmentEntries).
	RecordUndoSegment RecordType = 11
)

// Logger connects heaps, B+Trees and the undo log to the log (it
// implements storage.Logger, btree.Logger and undo.Logger) and holds
// the redo point: the redo LSN of the latest checkpoint that has started. A
// heap logs a full page image for the first change to a page after the redo
// point, so recovery never needs a page's possibly torn copy on disk.
type Logger struct {
	w *Writer

	// mu makes "read the redo point, build the record, append it" atomic
	// with respect to a checkpoint moving the redo point: Log holds it
	// shared, BeginCheckpoint exclusively (and only briefly).
	mu   sync.RWMutex
	redo LSN

	freeMu  sync.Mutex
	pending map[uint64]uint64 // page -> LSN the redo point must pass before it is freed
}

// deferredFree is a page to free once the redo point is past lsn.
type deferredFree struct{ page, lsn uint64 }

// NewLogger returns a Logger appending to w, with the given redo point (the
// redo LSN recovery started from, or of the last completed checkpoint).
func NewLogger(w *Writer, redo LSN) *Logger { return &Logger{w: w, redo: redo} }

// Log appends a heap record built by build, which receives the current redo
// point. It returns the record's LSN. An error means the record may or may
// not reach the log; the caller undoes its change in memory.
func (l *Logger) Log(ctx context.Context, build func(redoPoint uint64) []byte) (uint64, error) {
	return l.log(ctx, RecordHeap, build)
}

// LogBTree is Log for a B+Tree record.
func (l *Logger) LogBTree(ctx context.Context, build func(redoPoint uint64) []byte) (uint64, error) {
	return l.log(ctx, RecordBTree, build)
}

// LogUndo is Log for an undo page record.
func (l *Logger) LogUndo(ctx context.Context, build func(redoPoint uint64) []byte) (uint64, error) {
	return l.log(ctx, RecordUndo, build)
}

// LogUndoSegment appends an undo segment table record. An error means the
// record may or may not reach the log.
func (l *Logger) LogUndoSegment(ctx context.Context, payload []byte) (uint64, error) {
	lsn, err := l.w.Append(ctx, RecordUndoSegment, payload)
	return uint64(lsn), err
}

func (l *Logger) log(ctx context.Context, t RecordType, build func(redoPoint uint64) []byte) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	lsn, err := l.w.Append(ctx, t, build(uint64(l.redo)))
	return uint64(lsn), err
}

// DeferFree asks for pages to be freed once no record that refers to them
// can be replayed. It logs a deferred-free record naming them, so the list
// survives a crash (docs/design/08-btree.md section 2.7), and the pages wait
// for a checkpoint whose redo point is past that record. The record comes
// after whatever unlinked the pages, so its LSN is a safe threshold. An
// error means the record may or may not reach the log; the pages stay on
// the list in memory either way.
func (l *Logger) DeferFree(ctx context.Context, pages ...uint64) error {
	for len(pages) > 0 {
		n := min(len(pages), MaxDeferredPerRecord)
		lsn, err := l.w.Append(ctx, RecordDeferredFree, EncodeDeferredFree(pages[:n], nil))
		l.freeMu.Lock()
		for _, p := range pages[:n] {
			l.addPending(p, uint64(lsn))
		}
		l.freeMu.Unlock()
		if err != nil {
			return fmt.Errorf("logging deferred frees: %w", err)
		}
		pages = pages[n:]
	}
	return nil
}

// restore puts a page on the list without logging it: replayed from the
// log, or put back by a checkpoint that could not free it yet.
func (l *Logger) restore(page, lsn uint64) {
	l.freeMu.Lock()
	defer l.freeMu.Unlock()
	l.addPending(page, lsn)
}

// addPending adds or updates an entry, keeping the later threshold. The
// caller holds freeMu.
func (l *Logger) addPending(page, lsn uint64) {
	if l.pending == nil {
		l.pending = map[uint64]uint64{}
	}
	if old, ok := l.pending[page]; !ok || lsn > old {
		l.pending[page] = lsn
	}
}

// takeFreeable removes and returns the deferred pages whose threshold lies
// before redo, in page order.
func (l *Logger) takeFreeable(redo LSN) []deferredFree {
	l.freeMu.Lock()
	defer l.freeMu.Unlock()
	var out []deferredFree
	for p, lsn := range l.pending {
		if LSN(lsn) < redo {
			out = append(out, deferredFree{p, lsn})
			delete(l.pending, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].page < out[j].page })
	return out
}

// waiting returns the deferred pages whose threshold is at or after redo,
// in page order: those a checkpoint at redo will not free.
func (l *Logger) waiting(redo LSN) []deferredFree {
	l.freeMu.Lock()
	defer l.freeMu.Unlock()
	var out []deferredFree
	for p, lsn := range l.pending {
		if LSN(lsn) >= redo {
			out = append(out, deferredFree{p, lsn})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].page < out[j].page })
	return out
}

// PendingFrees returns how many unlinked pages wait to be freed.
func (l *Logger) PendingFrees() int {
	l.freeMu.Lock()
	defer l.freeMu.Unlock()
	return len(l.pending)
}

// RedoPoint returns the current redo point.
func (l *Logger) RedoPoint() LSN {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.redo
}

// BeginCheckpoint moves the redo point to the current end of the log and
// returns it. Every record already appended has a lower LSN; every record
// appended afterwards was built knowing the new redo point.
func (l *Logger) BeginCheckpoint() LSN {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.redo = l.w.EndLSN()
	return l.redo
}

// RuleHooks returns the two buffer pool hooks (storage.Options.FlushedLSN
// and FlushWAL) that enforce the WAL rule against w.
func RuleHooks(w *Writer) (flushed func() uint64, force func(context.Context, uint64) error) {
	flushed = func() uint64 { return uint64(w.FlushedLSN()) }
	force = func(ctx context.Context, lsn uint64) error { return w.FlushTo(ctx, LSN(lsn)) }
	return flushed, force
}
