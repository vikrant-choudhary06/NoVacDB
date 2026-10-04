package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
)

// Row operations on versioned tuples (docs/design/15-row-versioning.md).
// A row is named by its home RID for its whole life. A row that outgrows
// its home page moves to another page as a moved-in tuple, and its home
// slot keeps a forward stub; there is never more than one hop.

// errRetry means a write found the pages different from what it planned
// for (a stale free-space hint, or a row that changed shape) and must plan
// again.
var errRetry = errors.New("storage: retry")

// edit collects the changes of one row operation to pages the caller holds
// exclusively latched, so that they are logged as one record, or undone
// together if anything fails.
type edit struct {
	h       *Heap
	sps     map[uint64]*SlottedPage
	changes []change
}

// touch returns the change for page id, recording the page's image
// before its first change.
func (e *edit) touch(id uint64) (*change, error) {
	for i := range e.changes {
		if e.changes[i].id == id {
			return nil, fmt.Errorf("page %d changed twice in one operation: %w", id, ErrCorruptHeap)
		}
	}
	sp := e.sps[id]
	e.changes = append(e.changes, change{sp: sp, id: id, before: bytes.Clone(sp.buf)})
	return &e.changes[len(e.changes)-1], nil
}

func (e *edit) insert(id uint64, t rowTuple, want int) error {
	c, err := e.touch(id)
	if err != nil {
		return err
	}
	data := t.encode()
	slot, err := c.sp.Insert(data)
	if err != nil {
		return err
	}
	if slot != want {
		return fmt.Errorf("insert on page %d landed in slot %d, not %d: %w", id, slot, want, ErrCorruptHeap)
	}
	c.op = HeapBlock{Page: id, Kind: BlockInsert, Slot: uint16(slot), Data: data}
	return nil
}

func (e *edit) update(rid RID, t rowTuple) error {
	c, err := e.touch(rid.Page)
	if err != nil {
		return err
	}
	data := t.encode()
	if err := c.sp.Update(int(rid.Slot), data); err != nil {
		return err
	}
	c.op = HeapBlock{Page: rid.Page, Kind: BlockUpdate, Slot: rid.Slot, Data: data}
	return nil
}

func (e *edit) delete(rid RID) error {
	c, err := e.touch(rid.Page)
	if err != nil {
		return err
	}
	if err := c.sp.Delete(int(rid.Slot)); err != nil {
		return err
	}
	c.op = HeapBlock{Page: rid.Page, Kind: BlockDelete, Slot: rid.Slot}
	return nil
}

// undo restores every page changed so far.
func (e *edit) undo() {
	for _, c := range e.changes {
		copy(c.sp.buf, c.before)
	}
	e.changes = nil
}

// finish logs the changes as one record (restoring the pages if that
// fails) and refreshes the free-space hints of every latched page.
func (e *edit) finish(ctx context.Context, err error) error {
	if err == nil && len(e.changes) > 0 {
		err = e.h.logChanges(ctx, e.changes...) // restores the pages itself
		if err != nil {
			e.changes = nil
		}
	}
	if err != nil {
		e.undo()
	}
	for id, sp := range e.sps {
		e.h.setFree(id, sp.FreeSpace())
	}
	return err
}

// withPages pins and exclusively latches distinct pages in increasing page
// order, so that two operations can never deadlock, and runs fn on them as
// an edit. The pages are marked dirty before fn can log anything (see
// PageRef.MarkDirty).
func (h *Heap) withPages(ctx context.Context, ids []uint64, fn func(e *edit) error) error {
	ids = slices.Clone(ids)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	refs := make([]*PageRef, 0, len(ids))
	release := func(dirty bool) error {
		var errs []error
		for i := len(refs) - 1; i >= 0; i-- {
			refs[i].Unlock()
		}
		for _, r := range refs {
			errs = append(errs, r.Unpin(dirty))
		}
		return errors.Join(errs...)
	}
	e := &edit{h: h, sps: map[uint64]*SlottedPage{}}
	for _, id := range ids {
		ref, err := h.bp.FetchPage(ctx, id)
		if err != nil {
			return errors.Join(err, release(false))
		}
		ref.Lock()
		ref.MarkDirty()
		refs = append(refs, ref)
		sp, err := NewSlottedPage(ref.Data())
		if err != nil {
			return errors.Join(err, release(false))
		}
		e.sps[id] = sp
	}
	err := fn(e)
	changed := err == nil && len(e.changes) > 0
	return errors.Join(err, release(changed))
}

// tupleAt decodes the tuple in rid's slot of a latched page.
func tupleAt(sp *SlottedPage, rid RID) (rowTuple, error) {
	b, err := sp.Get(int(rid.Slot))
	if err != nil {
		return rowTuple{}, err
	}
	t, err := decodeTuple(b)
	if err != nil {
		return rowTuple{}, fmt.Errorf("row %s: %w", rid, err)
	}
	return t, nil
}

// readTuple returns a copy of the tuple at rid.
func (h *Heap) readTuple(ctx context.Context, rid RID) (rowTuple, error) {
	var t rowTuple
	err := withPage(ctx, h.bp, rid.Page, false, false, func(sp *SlottedPage) (bool, error) {
		b, err := sp.Get(int(rid.Slot))
		if err != nil {
			return false, err
		}
		t, err = decodeTuple(bytes.Clone(b))
		if err != nil {
			return false, fmt.Errorf("row %s: %w", rid, err)
		}
		return false, nil
	})
	return t, err
}

// homeKind checks what a home slot holds: a row (plain, or a stub to a
// moved-in tuple), or nothing a caller may name.
func homeKind(t rowTuple, rid RID) error {
	switch t.kind {
	case tupleTombstone:
		return fmt.Errorf("row %s: %w", rid, ErrRowDeleted)
	case tupleMoved:
		// A moved-in tuple is reached through its home RID only.
		return fmt.Errorf("row %s is a moved row's new place, not a row ID: %w", rid, ErrSlotNotFound)
	}
	return nil
}

// checkMoved checks that the moved-in tuple m, reached from the stub at
// home, points back to it.
func checkMoved(m rowTuple, home, at RID) error {
	if m.kind != tupleMoved || m.link != home {
		return fmt.Errorf("row %s forwards to %s, which does not point back: %w", home, at, ErrCorruptHeap)
	}
	return nil
}

// Insert stores a new row. stamp gives its header once its RID is known.
func (h *Heap) Insert(ctx context.Context, data []byte, stamp Stamper) (RID, error) {
	if err := checkRowData(data); err != nil {
		return RID{}, err
	}
	size := RowHeaderSize + len(data)
	for {
		id, ok := h.pickPage(size)
		if !ok {
			if err := h.grow(ctx, size); err != nil {
				return RID{}, err
			}
			continue
		}
		var rid RID
		err := h.withPages(ctx, []uint64{id}, func(e *edit) error {
			slot, err := e.sps[id].insertSlot(size)
			if err != nil {
				return err
			}
			rid = RID{Page: id, Slot: uint16(slot)}
			hdr, err := stamp(rid, nil)
			if err == nil {
				err = checkStamp(hdr)
			}
			if err == nil {
				err = e.insert(id, plainTuple(hdr, data), slot)
			}
			return e.finish(ctx, err)
		})
		switch {
		case err == nil:
			return rid, nil
		case errors.Is(err, ErrNoSpace):
			continue // the hint was stale; it is fixed now
		default:
			return RID{}, fmt.Errorf("inserting row: %w", err)
		}
	}
}

// Get returns a copy of the current version of the row at rid, following
// a forward stub.
func (h *Heap) Get(ctx context.Context, rid RID) (Version, error) {
	if err := h.checkRID(rid); err != nil {
		return Version{}, err
	}
	for {
		t, err := h.readTuple(ctx, rid)
		if err == nil {
			err = homeKind(t, rid)
		}
		if err != nil {
			return Version{}, fmt.Errorf("getting row %s: %w", rid, err)
		}
		if t.kind == tuplePlain {
			return t.version(), nil
		}
		at := t.link
		if err := h.checkRID(at); err != nil {
			return Version{}, fmt.Errorf("getting row %s: forwarded: %w: %w", rid, ErrCorruptHeap, err)
		}
		m, err := h.readTuple(ctx, at)
		if err == nil {
			err = checkMoved(m, rid, at)
		}
		if err == nil {
			return m.version(), nil
		}
		// The row may have moved meanwhile: if the stub is unchanged, the
		// damage is real.
		again, aerr := h.readTuple(ctx, rid)
		if aerr == nil && again.kind == tupleStub && again.link == at {
			return Version{}, fmt.Errorf("getting row %s: %w", rid, err)
		}
	}
}

// stamped remembers the header a Stamper gave, so that a write that must
// latch more pages and try again does not call it twice.
type stamped struct {
	stamp Stamper
	old   *RowHeader // the version stamped against
	hdr   RowHeader
	done  bool
}

func (s *stamped) get(rid RID, old Version) (RowHeader, error) {
	if s.done {
		if *s.old != old.RowHeader {
			return RowHeader{}, fmt.Errorf("row %s changed while it was being written: %w", rid, ErrCorruptHeap)
		}
		return s.hdr, nil
	}
	hdr, err := s.stamp(rid, &old)
	if err == nil {
		err = checkStamp(hdr)
	}
	if err != nil {
		return RowHeader{}, err
	}
	s.old, s.hdr, s.done = &old.RowHeader, hdr, true
	return hdr, nil
}

// locate reads the home slot and, for a moved row, where it is now.
func (h *Heap) locate(ctx context.Context, rid RID) (rowTuple, error) {
	if err := h.checkRID(rid); err != nil {
		return rowTuple{}, err
	}
	t, err := h.readTuple(ctx, rid)
	if err == nil {
		err = homeKind(t, rid)
	}
	if err == nil && t.kind == tupleStub {
		if cerr := h.checkRID(t.link); cerr != nil {
			err = fmt.Errorf("forwarded: %w: %w", ErrCorruptHeap, cerr)
		}
	}
	return t, err
}

// current returns, from latched pages, the row's current version. It fails
// with errRetry if the home slot no longer forwards to want (0: not
// forwarded).
func current(e *edit, rid RID, want RID) (Version, error) {
	home, err := tupleAt(e.sps[rid.Page], rid)
	if err == nil {
		err = homeKind(home, rid)
	}
	if err != nil {
		return Version{}, err
	}
	switch {
	case home.kind == tuplePlain && want == (RID{}):
		return home.version(), nil
	case home.kind == tupleStub && home.link == want:
		sp, ok := e.sps[want.Page]
		if !ok {
			return Version{}, errRetry
		}
		m, err := tupleAt(sp, want)
		if err == nil {
			err = checkMoved(m, rid, want)
		}
		return m.version(), err
	}
	return Version{}, errRetry
}

// Update replaces the row at rid with a new version of data, stamped by
// stamp. The row keeps its RID; if it no longer fits where it is, it moves
// (section 2.2).
func (h *Heap) Update(ctx context.Context, rid RID, data []byte, stamp Stamper) error {
	if err := checkRowData(data); err != nil {
		return err
	}
	s := &stamped{stamp: stamp}
	for {
		err := h.update(ctx, rid, data, s)
		if errors.Is(err, errRetry) {
			continue
		}
		if err != nil {
			return fmt.Errorf("updating row %s: %w", rid, err)
		}
		return nil
	}
}

func (h *Heap) update(ctx context.Context, rid RID, data []byte, s *stamped) error {
	home, err := h.locate(ctx, rid)
	if err != nil {
		return err
	}
	var at RID // where a moved row is now
	pages := []uint64{rid.Page}
	if home.kind == tupleStub {
		at = home.link
		pages = append(pages, at.Page)
	}
	// First, without a new page: in place, or back home, or in place where
	// it moved to.
	moved := false
	err = h.withPages(ctx, pages, func(e *edit) error {
		old, err := current(e, rid, at)
		if err != nil {
			return err
		}
		hdr, err := s.get(rid, old)
		if err != nil {
			return err
		}
		err = e.update(rid, plainTuple(hdr, data)) // in place, or home again
		if err == nil && at != (RID{}) {
			err = e.delete(at)
		}
		if errors.Is(err, ErrNoSpace) && at != (RID{}) {
			e.undo()
			err = e.update(at, rowTuple{kind: tupleMoved, hdr: hdr, link: rid, data: data})
		}
		if errors.Is(err, ErrNoSpace) {
			e.undo()
			moved = true
			return e.finish(ctx, nil)
		}
		return e.finish(ctx, err)
	})
	if err != nil || !moved {
		return err
	}
	return h.moveRow(ctx, rid, at, data, s)
}

// moveRow moves the row at rid (now at its home, or at at) to a page with
// room: the moved-in tuple, the home stub and the removal of the old
// moved-in tuple are one record.
func (h *Heap) moveRow(ctx context.Context, rid, at RID, data []byte, s *stamped) error {
	t := rowTuple{kind: tupleMoved, link: rid, data: data}
	skip := []uint64{rid.Page}
	if at != (RID{}) {
		skip = append(skip, at.Page)
	}
	target, ok := h.pickPageExcept(t.size(), skip...)
	if !ok {
		if err := h.growFor(ctx, t.size(), skip...); err != nil {
			return err
		}
		return errRetry
	}
	return h.withPages(ctx, append(slices.Clone(skip), target), func(e *edit) error {
		old, err := current(e, rid, at)
		if err != nil {
			return err
		}
		hdr, err := s.get(rid, old)
		if err != nil {
			return err
		}
		t.hdr = hdr
		slot, err := e.sps[target].insertSlot(t.size())
		if err != nil {
			if errors.Is(err, ErrNoSpace) {
				err = errRetry // the hint was stale; finish fixes it
			}
			return e.finish(ctx, err)
		}
		to := RID{Page: target, Slot: uint16(slot)}
		err = e.insert(target, t, slot)
		if err == nil {
			err = e.update(rid, rowTuple{kind: tupleStub, link: to})
		}
		if err == nil && at != (RID{}) {
			err = e.delete(at)
		}
		return e.finish(ctx, err)
	})
}

// Delete deletes the row at rid: its home slot becomes a tombstone stamped
// by stamp, and a moved-in tuple is removed.
func (h *Heap) Delete(ctx context.Context, rid RID, stamp Stamper) error {
	s := &stamped{stamp: stamp}
	for {
		err := h.delete(ctx, rid, s)
		if errors.Is(err, errRetry) {
			continue
		}
		if err != nil {
			return fmt.Errorf("deleting row %s: %w", rid, err)
		}
		return nil
	}
}

func (h *Heap) delete(ctx context.Context, rid RID, s *stamped) error {
	home, err := h.locate(ctx, rid)
	if err != nil {
		return err
	}
	var at RID
	pages := []uint64{rid.Page}
	if home.kind == tupleStub {
		at = home.link
		pages = append(pages, at.Page)
	}
	return h.withPages(ctx, pages, func(e *edit) error {
		old, err := current(e, rid, at)
		if err != nil {
			return err
		}
		hdr, err := s.get(rid, old)
		if err != nil {
			return err
		}
		err = e.update(rid, rowTuple{kind: tupleTombstone, hdr: hdr})
		if err == nil && at != (RID{}) {
			err = e.delete(at)
		}
		return e.finish(ctx, err)
	})
}

// Scanner iterates over the rows of a heap in page-chain order, each at its
// home RID: tombstones and moved-in tuples are skipped, and a forward stub
// is followed. It holds no pin or latch between calls, and sees each page
// as of the moment it visits it.
type Scanner struct {
	h    *Heap
	page uint64 // current page, 0 when finished
	slot int    // next slot to look at on that page
}

// Scan starts a scan at the first page.
func (h *Heap) Scan() *Scanner { return &Scanner{h: h, page: h.first} }

// Next returns the next row's RID and current version (a copy). ok is
// false at the end.
func (s *Scanner) Next(ctx context.Context) (rid RID, v Version, ok bool, err error) {
	for s.page != 0 {
		var found, forwarded bool
		var next uint64
		err = withPage(ctx, s.h.bp, s.page, false, false, func(sp *SlottedPage) (bool, error) {
			for {
				slot := sp.NextLive(s.slot)
				if slot < 0 {
					next = sp.NextPage()
					return false, nil
				}
				s.slot = slot + 1
				rid = RID{Page: s.page, Slot: uint16(slot)}
				t, err := tupleAt(sp, rid)
				if err != nil {
					return false, err
				}
				switch t.kind {
				case tuplePlain:
					v = Version{RowHeader: t.hdr, Data: bytes.Clone(t.data)}
					found = true
					return false, nil
				case tupleStub:
					forwarded = true
					return false, nil
				}
			}
		})
		if err != nil {
			return RID{}, Version{}, false, fmt.Errorf("scanning page %d: %w", s.page, err)
		}
		if forwarded {
			// Read outside the page's latch, following the stub.
			v, err = s.h.Get(ctx, rid)
			if errors.Is(err, ErrRowDeleted) {
				continue // deleted since
			}
			if err != nil {
				return RID{}, Version{}, false, err
			}
			return rid, v, true, nil
		}
		if found {
			return rid, v, true, nil
		}
		s.page, s.slot = next, 0
	}
	return RID{}, Version{}, false, nil
}

// pickPageExcept is pickPage that never returns the pages in skip.
func (h *Heap) pickPageExcept(size int, skip ...uint64) (uint64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := len(h.pages)
	for i := range n {
		j := (h.cursor + i) % n
		if !slices.Contains(skip, h.pages[j].id) && h.pages[j].free >= size {
			h.cursor = j
			return h.pages[j].id, true
		}
	}
	return 0, false
}

// growFor grows the heap unless some page not in skip already has room.
func (h *Heap) growFor(ctx context.Context, size int, skip ...uint64) error {
	h.growMu.Lock()
	defer h.growMu.Unlock()
	if _, ok := h.pickPageExcept(size, skip...); ok {
		return nil
	}
	if h.lg != nil {
		return h.growLogged(ctx)
	}
	return h.growUnlogged(ctx)
}
