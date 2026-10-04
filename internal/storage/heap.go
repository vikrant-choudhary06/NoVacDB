package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Heap errors, in addition to those of slotted pages.
var (
	// ErrCorruptHeap means the page chain is damaged: a cycle, or a page
	// that is not a heap page.
	ErrCorruptHeap = errors.New("storage: corrupt heap")
	// ErrInvalidRID means a record ID does not belong to this heap.
	ErrInvalidRID = errors.New("storage: invalid record id")
)

// RID names one row: the page it lives on and its slot in that page.
type RID struct {
	Page uint64
	Slot uint16
}

// String formats the RID like "(page,slot)".
func (r RID) String() string { return fmt.Sprintf("(%d,%d)", r.Page, r.Slot) }

// heapPage is one entry of the free-space map.
type heapPage struct {
	id   uint64
	free int // largest tuple the page could take, as last observed (a hint)
}

// Heap is a table: a chain of slotted pages reached from a first page. See
// docs/design/05-heap-storage.md. It is safe for concurrent use. Only one
// Heap value may exist per heap file at a time.
type Heap struct {
	bp    *BufferPool
	first uint64
	lg    Logger // nil: changes are not logged (Step 1.4 behaviour)

	// growMu serialises appending pages so there is only ever one tail.
	// Lock order: growMu -> page latch -> mu.
	growMu sync.Mutex

	// mu guards pages, index and cursor. It is a leaf lock: never wait for
	// a page latch or the buffer pool while holding it.
	mu     sync.Mutex
	pages  []heapPage     // chain order; the last entry is the tail
	index  map[uint64]int // page ID -> position in pages
	cursor int            // where the free-space search starts
}

// withPage pins page id, latches it (exclusively if write), and runs fn on it
// as a slotted page. fn reports whether it changed the page. Latch and pin
// are always released, in that order.
//
// If markDirty is true (write must be too), the page is marked dirty as soon
// as it is latched, before fn can log a change to it; see PageRef.MarkDirty.
func withPage(ctx context.Context, bp *BufferPool, id uint64, write, markDirty bool, fn func(*SlottedPage) (bool, error)) error {
	ref, err := bp.FetchPage(ctx, id)
	if err != nil {
		return err
	}
	if write {
		ref.Lock()
		if markDirty {
			ref.MarkDirty()
		}
	} else {
		ref.RLock()
	}
	dirty := false
	sp, err := NewSlottedPage(ref.Data())
	if err == nil {
		dirty, err = fn(sp)
	}
	if write {
		ref.Unlock()
	} else {
		ref.RUnlock()
	}
	if uerr := ref.Unpin(dirty && write); err == nil {
		err = uerr
	}
	return err
}

// HeapOption configures a heap.
type HeapOption func(*Heap)

// WithLogger makes every change to the heap go through the write-ahead log:
// each operation becomes one log record, pages carry the LSN of their last
// change, and operations on two pages are atomic. A logged heap needs a
// buffer pool of at least two frames. See docs/design/07-checkpoints-recovery.md.
func WithLogger(lg Logger) HeapOption { return func(h *Heap) { h.lg = lg } }

// CreateHeap allocates the first page of a new, empty heap.
func CreateHeap(ctx context.Context, bp *BufferPool, opts ...HeapOption) (*Heap, error) {
	h := &Heap{bp: bp, index: map[uint64]int{}}
	for _, o := range opts {
		o(h)
	}
	var id uint64
	var err error
	if h.lg != nil {
		id, err = h.newLoggedHeapPage(ctx)
	} else {
		id, err = newHeapPage(ctx, bp)
	}
	if err != nil {
		return nil, fmt.Errorf("creating heap: %w", err)
	}
	h.first = id
	h.pages = []heapPage{{id: id, free: MaxTupleSize}}
	h.index[id] = 0
	return h, nil
}

// newHeapPage allocates and initialises an empty heap page, leaving it dirty
// in the pool.
func newHeapPage(ctx context.Context, bp *BufferPool) (uint64, error) {
	ref, err := bp.NewPage(ctx, PageTypeHeap)
	if err != nil {
		return 0, err
	}
	id := ref.ID()
	ref.Lock()
	err = InitSlottedPage(ref.Data())
	ref.Unlock()
	if uerr := ref.Unpin(true); err == nil {
		err = uerr
	}
	if err != nil {
		// Give the page back; cleanup must not depend on the caller's ctx.
		_ = bp.DeletePage(context.WithoutCancel(ctx), id)
		return 0, err
	}
	return id, nil
}

// OpenHeap opens the heap whose first page is first. It reads the whole
// chain, fully validating every page, and rebuilds the free-space map.
func OpenHeap(ctx context.Context, bp *BufferPool, first uint64, opts ...HeapOption) (*Heap, error) {
	h := &Heap{bp: bp, first: first, index: map[uint64]int{}}
	for _, o := range opts {
		o(h)
	}
	for id := first; id != 0; {
		if _, seen := h.index[id]; seen {
			return nil, fmt.Errorf("opening heap at page %d: chain loops back to page %d: %w", first, id, ErrCorruptHeap)
		}
		var next uint64
		var free int
		err := withPage(ctx, bp, id, false, false, func(sp *SlottedPage) (bool, error) {
			if err := sp.Validate(); err != nil {
				return false, err
			}
			for slot := sp.NextLive(0); slot >= 0; slot = sp.NextLive(slot + 1) {
				if _, err := tupleAt(sp, RID{Page: id, Slot: uint16(slot)}); err != nil {
					return false, err
				}
			}
			next, free = sp.NextPage(), sp.FreeSpace()
			return false, nil
		})
		if err != nil {
			return nil, fmt.Errorf("opening heap at page %d: page %d: %w", first, id, err)
		}
		h.index[id] = len(h.pages)
		h.pages = append(h.pages, heapPage{id: id, free: free})
		id = next
	}
	if len(h.pages) == 0 {
		return nil, fmt.Errorf("opening heap: no first page: %w", ErrCorruptHeap)
	}
	return h, nil
}

// FirstPage returns the ID that identifies this heap; OpenHeap takes it.
func (h *Heap) FirstPage() uint64 { return h.first }

// Pages returns the IDs of the heap's pages in chain order, for freeing
// them when the heap is dropped.
func (h *Heap) Pages() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	ids := make([]uint64, len(h.pages))
	for i, p := range h.pages {
		ids[i] = p.id
	}
	return ids
}

// NumPages returns the number of pages in the heap.
func (h *Heap) NumPages() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.pages)
}

func (h *Heap) setFree(id uint64, free int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if i, ok := h.index[id]; ok {
		h.pages[i].free = free
	}
}

// pickPage finds a page whose hint says a tuple of size fits.
func (h *Heap) pickPage(size int) (uint64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := len(h.pages)
	for i := range n {
		j := (h.cursor + i) % n
		if h.pages[j].free >= size {
			h.cursor = j
			return h.pages[j].id, true
		}
	}
	return 0, false
}

func (h *Heap) checkRID(rid RID) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.index[rid.Page]; !ok {
		return fmt.Errorf("record %s: page is not part of this heap: %w", rid, ErrInvalidRID)
	}
	return nil
}

// grow appends a page unless someone else has made room for size meanwhile.
func (h *Heap) grow(ctx context.Context, size int) error {
	h.growMu.Lock()
	defer h.growMu.Unlock()
	if _, ok := h.pickPage(size); ok {
		return nil
	}
	if h.lg != nil {
		return h.growLogged(ctx)
	}
	return h.growUnlogged(ctx)
}

// growUnlogged appends a page to an unlogged heap. The caller holds growMu.
func (h *Heap) growUnlogged(ctx context.Context) error {
	newID, err := newHeapPage(ctx, h.bp)
	if err != nil {
		return fmt.Errorf("growing heap: %w", err)
	}
	h.mu.Lock()
	tail := h.pages[len(h.pages)-1].id
	h.mu.Unlock()
	// Link only after the new page exists, so a chain never points at
	// nothing (crash atomicity proper needs the WAL).
	err = withPage(ctx, h.bp, tail, true, false, func(sp *SlottedPage) (bool, error) {
		sp.SetNextPage(newID)
		return true, nil
	})
	if err != nil {
		_ = h.bp.DeletePage(context.WithoutCancel(ctx), newID)
		return fmt.Errorf("growing heap: linking page %d: %w", newID, err)
	}
	h.mu.Lock()
	h.index[newID] = len(h.pages)
	h.pages = append(h.pages, heapPage{id: newID, free: MaxTupleSize})
	h.mu.Unlock()
	return nil
}
