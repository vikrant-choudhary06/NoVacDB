package wal

import (
	"context"
	"slices"
)

// Snapshot says which transactions a reader sees (docs/design/16-snapshots-
// visibility.md section 2.1): those committed before it was taken. A
// reader also sees its own transaction's changes; that is the caller's
// rule (mvcc.Reader), since a transaction's ID is allocated after its first
// snapshot may have been taken.
type Snapshot struct {
	// XMax is the next transaction ID when the snapshot was taken: every
	// ID at or above it is unseen.
	XMax XID
	// Active are the transactions writing when it was taken: unseen.
	Active []XID

	e       *Engine
	aborted map[XID]bool // aborted transactions below XMax
}

// Snapshot takes a snapshot and registers it until Release: undo it may
// need is kept (section 2.6).
func (e *Engine) Snapshot() *Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := &Snapshot{XMax: e.nextXID, e: e}
	for xid := range e.active {
		s.Active = append(s.Active, xid)
	}
	slices.Sort(s.Active)
	for xid, o := range e.outcomes {
		if o.status == TxnAborted {
			if s.aborted == nil {
				s.aborted = map[XID]bool{}
			}
			s.aborted[xid] = true
		}
	}
	if e.snaps == nil {
		e.snaps = map[*Snapshot]struct{}{}
	}
	e.snaps[s] = struct{}{}
	return s
}

// Release ends the snapshot. It may be called more than once.
func (s *Snapshot) Release() {
	s.e.mu.Lock()
	defer s.e.mu.Unlock()
	delete(s.e.snaps, s)
}

// Engine returns the engine the snapshot was taken from.
func (s *Snapshot) Engine() *Engine { return s.e }

// Sees reports whether the snapshot sees the changes of transaction xid:
// it committed before the snapshot was taken (section 2.2).
func (s *Snapshot) Sees(xid uint64) bool {
	x := XID(xid)
	if x == 0 || x >= s.XMax || s.aborted[x] {
		return false
	}
	_, active := slices.BinarySearch(s.Active, x)
	return !active
}

// Ended reports whether transaction xid had ended, committed or rolled
// back, when the snapshot was taken: it is below XMax and was not active
// (docs/design/17-rollback.md section 2.8). A committed one the snapshot
// sees; a rolled-back one has no changes left.
func (s *Snapshot) Ended(xid uint64) bool {
	x := XID(xid)
	if x == 0 || x >= s.XMax {
		return false
	}
	_, active := slices.BinarySearch(s.Active, x)
	return !active
}

// LastWriter returns the most recent transaction that has started
// writing, 0 if none since the engine opened (section 2.5).
func (e *Engine) LastWriter() XID {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastWriter
}

// LiveSnapshots returns how many snapshots are registered.
func (e *Engine) LiveSnapshots() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.snaps)
}

// releaseUndo releases the undo segments that no live snapshot needs:
// those of transactions that ended, committed or rolled back, before every
// live snapshot was taken (section 2.6; docs/design/17-rollback.md section
// 2.8). The caller holds the writer slot, or no transaction can be
// writing, so the records it logs never fall inside another transaction's.
func (e *Engine) releaseUndo(ctx context.Context) error {
	if e.keepUndo {
		return nil
	}
	for _, x := range e.undo.Segments() {
		if !e.undoReleasable(XID(x)) {
			continue
		}
		if err := e.undo.Release(ctx, x); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) undoReleasable(xid XID) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, active := e.active[xid]; active {
		return false
	}
	for s := range e.snaps {
		if !s.Ended(uint64(xid)) {
			return false
		}
	}
	return true
}
