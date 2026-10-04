package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

// change is one page modified by a logged heap operation.
type change struct {
	sp         *SlottedPage
	id         uint64
	before     []byte    // the page before the change: for rollback and the image rule
	op         HeapBlock // logged unless an image is needed
	forceImage bool      // a newly created page: only an image can describe it
}

// logChanges logs changes already made to pages the caller holds exclusively
// latched, as one record, and stamps them with its LSN. A page whose LSN was
// below the redo point is logged as a full image (its copy on disk may be torn
// when recovery needs it). If logging fails every page is restored, so the
// buffer pool never holds a change the log does not have. For an unlogged
// heap it does nothing.
func (h *Heap) logChanges(ctx context.Context, changes ...change) error {
	if h.lg == nil {
		return nil
	}
	lsn, err := h.lg.Log(ctx, func(redo uint64) []byte {
		blocks := make([]HeapBlock, len(changes))
		for i, c := range changes {
			if c.forceImage || pageLSN(c.before) < redo {
				blocks[i] = HeapBlock{Page: c.id, Kind: BlockImage, Data: c.sp.buf}
			} else {
				blocks[i] = c.op
			}
		}
		return encodeHeapRecord(blocks)
	})
	if err != nil {
		for _, c := range changes {
			copy(c.sp.buf, c.before)
		}
		return fmt.Errorf("logging heap change: %w", err)
	}
	for _, c := range changes {
		setPageLSN(c.sp.buf, lsn)
	}
	return nil
}

// newLoggedHeapPage allocates a page, makes it an empty heap page and logs
// its image. If logging fails the page is not freed: the record may still
// reach the log, and recovery would then use the page. It stays allocated
// and unreachable (a leak) instead.
func (h *Heap) newLoggedHeapPage(ctx context.Context) (uint64, error) {
	ref, err := h.bp.NewPage(ctx, PageTypeHeap)
	if err != nil {
		return 0, err
	}
	id := ref.ID()
	ref.Lock()
	ref.MarkDirty() // NewPage already did; kept explicit for the logging rule
	before := bytes.Clone(ref.Data())
	err = InitSlottedPage(ref.Data())
	var sp *SlottedPage
	if err == nil {
		sp, err = NewSlottedPage(ref.Data())
	}
	if err == nil {
		err = h.logChanges(ctx, change{sp: sp, id: id, before: before, forceImage: true})
	}
	ref.Unlock()
	if uerr := ref.Unpin(err == nil); err == nil {
		err = uerr
	}
	return id, err
}

// growLogged appends a page to a logged heap: the new page's image and the
// tail's link to it are one record, so after a crash the chain either has the
// complete new page or does not reach it. The caller holds growMu.
func (h *Heap) growLogged(ctx context.Context) error {
	ref, err := h.bp.NewPage(ctx, PageTypeHeap)
	if err != nil {
		return fmt.Errorf("growing heap: %w", err)
	}
	newID := ref.ID()
	h.mu.Lock()
	tail := h.pages[len(h.pages)-1].id
	h.mu.Unlock()
	tref, err := h.bp.FetchPage(ctx, tail)
	if err != nil {
		// The page stays allocated but unreachable; its record was never made.
		_ = ref.Unpin(false)
		return fmt.Errorf("growing heap: %w", err)
	}
	first, second := ref, tref
	if tail < newID {
		first, second = tref, ref
	}
	first.Lock()
	second.Lock()
	ref.MarkDirty() // before the grow record exists (see PageRef.MarkDirty)
	tref.MarkDirty()
	err = func() error {
		newBefore := bytes.Clone(ref.Data())
		tailBefore := bytes.Clone(tref.Data())
		if err := InitSlottedPage(ref.Data()); err != nil {
			return err
		}
		spNew, err := NewSlottedPage(ref.Data())
		if err != nil {
			return err
		}
		spTail, err := NewSlottedPage(tref.Data())
		if err != nil {
			copy(ref.Data(), newBefore)
			return err
		}
		spTail.SetNextPage(newID)
		return h.logChanges(ctx,
			change{sp: spNew, id: newID, before: newBefore, forceImage: true},
			change{sp: spTail, id: tail, before: tailBefore,
				op: HeapBlock{Page: tail, Kind: BlockSetNext, Next: newID}})
	}()
	second.Unlock()
	first.Unlock()
	uerr := errors.Join(ref.Unpin(err == nil), tref.Unpin(err == nil))
	if err != nil {
		return fmt.Errorf("growing heap: %w", err)
	}
	if uerr != nil {
		return uerr
	}
	h.mu.Lock()
	h.index[newID] = len(h.pages)
	h.pages = append(h.pages, heapPage{id: newID, free: MaxTupleSize})
	h.mu.Unlock()
	return nil
}
