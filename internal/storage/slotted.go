package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

// Heap page geometry. See docs/design/05-heap-storage.md.
const (
	heapOffNext     = HeaderSize     // NextPage, uint64
	heapOffNumSlots = HeaderSize + 8 // NumSlots, uint16
	heapOffUpper    = HeaderSize + 10
	heapOffReserved = HeaderSize + 12 // 4 bytes, must be zero
	slotsOffset     = HeaderSize + 16 // start of the slot directory
	slotSize        = 4

	// slotAreaSize is the space shared by the slot directory and tuples.
	slotAreaSize = PageSize - slotsOffset
	// MaxTupleSize is the largest tuple a heap page can hold: the whole
	// area minus the one slot entry it needs.
	MaxTupleSize = slotAreaSize - slotSize
)

// Heap page layout (8192 bytes, little-endian):
//
//	offset  size        field
//	0       24          page header (PageType = Heap)
//	24      8           NextPage   next heap page in the chain, 0 = last
//	32      2           NumSlots   entries in the slot directory
//	34      2           Upper      offset of the lowest tuple byte (8192 if none)
//	36      4           reserved   must be zero
//	40      4*NumSlots  slot directory
//	...                 free space
//	Upper   ...         tuple area (tuples and garbage), up to 8192
//
// Slot entry (4 bytes at offset 40 + 4*slot):
//
//	offset  size  field
//	0       2     Offset  page offset of the tuple; 0 = dead slot
//	2       2     Length  tuple length; 0 only for a dead slot

// Errors returned by slotted pages and the heap.
var (
	// ErrNoSpace means the page cannot hold the tuple, even after
	// compaction. The page is unchanged.
	ErrNoSpace = errors.New("storage: not enough space in page")
	// ErrTupleTooLarge means a tuple exceeds MaxTupleSize.
	ErrTupleTooLarge = errors.New("storage: tuple too large")
	// ErrEmptyTuple means a zero-length tuple; tuples must have a byte.
	ErrEmptyTuple = errors.New("storage: empty tuple")
	// ErrSlotNotFound means a slot is out of range or dead.
	ErrSlotNotFound = errors.New("storage: slot not found")
	// ErrCorruptPage means a heap page is structurally invalid.
	ErrCorruptPage = errors.New("storage: corrupt heap page")
)

// SlottedPage is a view of a heap page buffer. It does no locking: the caller
// holds the page's latch. It never panics or reads outside the buffer, even
// if the page content is corrupt; it returns ErrCorruptPage instead.
type SlottedPage struct {
	buf []byte
}

// NewSlottedPage wraps buf, which must be one page of type Heap whose heap
// header is sane. Only O(1) checks are made here; use Validate for the rest.
func NewSlottedPage(buf []byte) (*SlottedPage, error) {
	if err := checkSize(buf); err != nil {
		return nil, err
	}
	h, err := DecodeHeader(buf)
	if err != nil {
		return nil, fmt.Errorf("heap page: %w: %w", ErrCorruptPage, err)
	}
	if h.Type != PageTypeHeap {
		return nil, fmt.Errorf("page %d has type %d: %w", h.ID, h.Type, ErrCorruptPage)
	}
	p := &SlottedPage{buf: buf}
	if err := p.checkHeader(); err != nil {
		return nil, fmt.Errorf("page %d: %w", h.ID, err)
	}
	return p, nil
}

// InitSlottedPage turns a freshly initialised Heap page into an empty slotted
// page: it zeroes the payload and writes an empty heap header.
func InitSlottedPage(buf []byte) error {
	if err := checkSize(buf); err != nil {
		return err
	}
	h, err := DecodeHeader(buf)
	if err != nil {
		return fmt.Errorf("initialising heap page: %w", err)
	}
	if h.Type != PageTypeHeap {
		return fmt.Errorf("initialising page %d of type %d as heap: %w", h.ID, h.Type, ErrCorruptPage)
	}
	clear(buf[HeaderSize:])
	binary.LittleEndian.PutUint16(buf[heapOffUpper:], PageSize)
	return nil
}

func (p *SlottedPage) numSlots() int { return int(binary.LittleEndian.Uint16(p.buf[heapOffNumSlots:])) }
func (p *SlottedPage) upper() int    { return int(binary.LittleEndian.Uint16(p.buf[heapOffUpper:])) }
func (p *SlottedPage) lower() int    { return slotsOffset + slotSize*p.numSlots() }

func (p *SlottedPage) setNumSlots(n int) {
	binary.LittleEndian.PutUint16(p.buf[heapOffNumSlots:], uint16(n))
}
func (p *SlottedPage) setUpper(u int) { binary.LittleEndian.PutUint16(p.buf[heapOffUpper:], uint16(u)) }

func (p *SlottedPage) slot(i int) (off, length int) {
	b := p.buf[slotsOffset+slotSize*i:]
	return int(binary.LittleEndian.Uint16(b)), int(binary.LittleEndian.Uint16(b[2:]))
}

func (p *SlottedPage) setSlot(i, off, length int) {
	b := p.buf[slotsOffset+slotSize*i:]
	binary.LittleEndian.PutUint16(b, uint16(off))
	binary.LittleEndian.PutUint16(b[2:], uint16(length))
}

// checkHeader is the O(1) structural check.
func (p *SlottedPage) checkHeader() error {
	if !bytes.Equal(p.buf[heapOffReserved:slotsOffset], make([]byte, slotsOffset-heapOffReserved)) {
		return fmt.Errorf("reserved bytes not zero: %w", ErrCorruptPage)
	}
	lower, upper := p.lower(), p.upper()
	if lower > PageSize || upper > PageSize || upper < lower {
		return fmt.Errorf("slots=%d upper=%d: %w", p.numSlots(), upper, ErrCorruptPage)
	}
	return nil
}

// NextPage returns the next page in the heap chain, 0 if this is the last.
func (p *SlottedPage) NextPage() uint64 { return binary.LittleEndian.Uint64(p.buf[heapOffNext:]) }

// SetNextPage links the next heap page.
func (p *SlottedPage) SetNextPage(id uint64) { binary.LittleEndian.PutUint64(p.buf[heapOffNext:], id) }

// NumSlots returns the number of entries in the slot directory (live or dead).
func (p *SlottedPage) NumSlots() int { return p.numSlots() }

// isLive reports whether slot i (in range) holds a tuple.
func (p *SlottedPage) isLive(i int) bool {
	off, _ := p.slot(i)
	return off != 0
}

// slotBounds validates a live slot's entry against the page layout.
func (p *SlottedPage) slotBounds(i int) (off, length int, err error) {
	off, length = p.slot(i)
	if length < 1 || off < p.upper() || off+length > PageSize {
		return 0, 0, fmt.Errorf("slot %d (offset %d, length %d, upper %d): %w", i, off, length, p.upper(), ErrCorruptPage)
	}
	return off, length, nil
}

// pageStats summarises the slot directory.
type pageStats struct {
	liveBytes int
	firstDead int // lowest dead slot, or -1
	totalFree int // bytes free for tuples and new slot entries, after compaction
}

// stats walks the slot directory, rejecting malformed entries.
func (p *SlottedPage) stats() (pageStats, error) {
	st := pageStats{firstDead: -1}
	n := p.numSlots()
	for i := range n {
		off, length := p.slot(i)
		if off == 0 {
			if length != 0 {
				return st, fmt.Errorf("slot %d is half dead: %w", i, ErrCorruptPage)
			}
			if st.firstDead < 0 {
				st.firstDead = i
			}
			continue
		}
		_, l, err := p.slotBounds(i)
		if err != nil {
			return st, err
		}
		st.liveBytes += l
	}
	st.totalFree = slotAreaSize - slotSize*n - st.liveBytes
	if st.totalFree < 0 {
		return st, fmt.Errorf("tuples (%d bytes) do not fit: %w", st.liveBytes, ErrCorruptPage)
	}
	return st, nil
}

// FreeSpace returns the size of the largest tuple Insert could place now
// (0 if the page is full or corrupt).
func (p *SlottedPage) FreeSpace() int {
	st, err := p.stats()
	if err != nil {
		return 0
	}
	return insertCapacity(st)
}

func insertCapacity(st pageStats) int {
	free := st.totalFree
	if st.firstDead < 0 {
		free -= slotSize // a new slot entry is needed
	}
	return max(free, 0)
}

// NextLive returns the lowest live slot >= from, or -1.
func (p *SlottedPage) NextLive(from int) int {
	for i := max(from, 0); i < p.numSlots(); i++ {
		if p.isLive(i) {
			return i
		}
	}
	return -1
}

// LiveCount returns the number of live tuples.
func (p *SlottedPage) LiveCount() int {
	n := 0
	for i := range p.numSlots() {
		if p.isLive(i) {
			n++
		}
	}
	return n
}

// Get returns the tuple in slot. The slice aliases the page and is valid only
// while the caller keeps the page pinned and latched.
func (p *SlottedPage) Get(slot int) ([]byte, error) {
	if slot < 0 || slot >= p.numSlots() || !p.isLive(slot) {
		return nil, fmt.Errorf("slot %d: %w", slot, ErrSlotNotFound)
	}
	off, length, err := p.slotBounds(slot)
	if err != nil {
		return nil, err
	}
	return p.buf[off : off+length : off+length], nil
}

func checkTupleSize(n int) error {
	switch {
	case n == 0:
		return ErrEmptyTuple
	case n > MaxTupleSize:
		return fmt.Errorf("%d bytes (max %d): %w", n, MaxTupleSize, ErrTupleTooLarge)
	}
	return nil
}

// insertSlot returns the slot Insert would give a tuple of n bytes, without
// changing the page, or ErrNoSpace.
func (p *SlottedPage) insertSlot(n int) (int, error) {
	if err := checkTupleSize(n); err != nil {
		return 0, err
	}
	st, err := p.stats()
	if err != nil {
		return 0, err
	}
	if n > insertCapacity(st) {
		return 0, ErrNoSpace
	}
	if st.firstDead >= 0 {
		return st.firstDead, nil
	}
	return p.numSlots(), nil
}

// Insert stores data in the lowest dead slot, or a new slot, and returns the
// slot number. data must not alias the page. If the page has no room it
// returns ErrNoSpace and is unchanged.
func (p *SlottedPage) Insert(data []byte) (int, error) {
	if err := checkTupleSize(len(data)); err != nil {
		return 0, err
	}
	st, err := p.stats()
	if err != nil {
		return 0, err
	}
	slot := st.firstDead
	need := len(data)
	if slot < 0 {
		slot = p.numSlots()
		need += slotSize
	}
	if need > st.totalFree {
		return 0, ErrNoSpace
	}
	if need > p.upper()-p.lower() {
		if err := p.compact(); err != nil {
			return 0, err
		}
	}
	newUpper := p.upper() - len(data)
	copy(p.buf[newUpper:], data)
	if slot == p.numSlots() {
		p.setNumSlots(slot + 1)
	}
	p.setSlot(slot, newUpper, len(data))
	p.setUpper(newUpper)
	return slot, nil
}

// Update replaces the tuple in slot, keeping the slot number. If the page
// cannot hold the new version it returns ErrNoSpace and is unchanged.
func (p *SlottedPage) Update(slot int, data []byte) error {
	if err := checkTupleSize(len(data)); err != nil {
		return err
	}
	if slot < 0 || slot >= p.numSlots() || !p.isLive(slot) {
		return fmt.Errorf("slot %d: %w", slot, ErrSlotNotFound)
	}
	off, oldLen, err := p.slotBounds(slot)
	if err != nil {
		return err
	}
	if len(data) <= oldLen {
		copy(p.buf[off:], data)
		clear(p.buf[off+len(data) : off+oldLen])
		p.setSlot(slot, off, len(data))
		return nil
	}
	st, err := p.stats()
	if err != nil {
		return err
	}
	if len(data) > st.totalFree+oldLen {
		return ErrNoSpace
	}
	if len(data) <= p.upper()-p.lower() {
		// Room in the gap: write the new version first, then retire the old.
		newOff := p.upper() - len(data)
		copy(p.buf[newOff:], data)
		clear(p.buf[off : off+oldLen])
		p.setSlot(slot, newOff, len(data))
		p.setUpper(newOff)
		return nil
	}
	// Needs compaction. data may alias the page, which compaction moves.
	data = bytes.Clone(data)
	clear(p.buf[off : off+oldLen])
	p.setSlot(slot, 0, 0) // temporarily dead so its old bytes are reclaimed
	if err := p.compact(); err != nil {
		return err
	}
	newOff := p.upper() - len(data)
	copy(p.buf[newOff:], data)
	p.setSlot(slot, newOff, len(data))
	p.setUpper(newOff)
	return nil
}

// Delete removes the tuple in slot, zeroing its bytes. Trailing dead slots
// are trimmed from the directory.
func (p *SlottedPage) Delete(slot int) error {
	if slot < 0 || slot >= p.numSlots() || !p.isLive(slot) {
		return fmt.Errorf("slot %d: %w", slot, ErrSlotNotFound)
	}
	off, length, err := p.slotBounds(slot)
	if err != nil {
		return err
	}
	clear(p.buf[off : off+length])
	p.setSlot(slot, 0, 0)

	n := p.numSlots()
	trimmed := n
	for trimmed > 0 && !p.isLive(trimmed-1) {
		trimmed--
	}
	if trimmed != n {
		clear(p.buf[slotsOffset+slotSize*trimmed : slotsOffset+slotSize*n])
		p.setNumSlots(trimmed)
	}
	if trimmed == 0 {
		p.setUpper(PageSize) // nothing live: all tuple space is free again
	}
	return nil
}

// Compact moves all live tuples to the end of the page, removing garbage
// between them. Slot numbers do not change.
func (p *SlottedPage) Compact() error { return p.compact() }

func (p *SlottedPage) compact() error {
	if _, err := p.stats(); err != nil { // guarantees the live bytes fit in the page
		return err
	}
	live := make([]int, 0, p.numSlots())
	for i := range p.numSlots() {
		if p.isLive(i) {
			live = append(live, i)
		}
	}
	// Highest address first: a tuple only ever moves up, into space that is
	// either free or already vacated, never over a tuple not yet moved.
	slices.SortFunc(live, func(a, b int) int {
		oa, _ := p.slot(a)
		ob, _ := p.slot(b)
		return ob - oa
	})
	top := PageSize
	for _, i := range live {
		off, length := p.slot(i)
		top -= length
		if top != off {
			copy(p.buf[top:top+length], p.buf[off:off+length])
			p.setSlot(i, top, length)
		}
	}
	clear(p.buf[p.lower():top])
	p.setUpper(top)
	return nil
}

// Validate checks the whole page structure, including that no two tuples
// overlap. It is O(slots log slots).
func (p *SlottedPage) Validate() error {
	if err := p.checkHeader(); err != nil {
		return err
	}
	type span struct{ off, end int }
	var spans []span
	for i := range p.numSlots() {
		off, length := p.slot(i)
		if off == 0 {
			if length != 0 {
				return fmt.Errorf("slot %d is half dead: %w", i, ErrCorruptPage)
			}
			continue
		}
		if _, _, err := p.slotBounds(i); err != nil {
			return err
		}
		spans = append(spans, span{off, off + length})
	}
	slices.SortFunc(spans, func(a, b span) int { return a.off - b.off })
	for i := 1; i < len(spans); i++ {
		if spans[i].off < spans[i-1].end {
			return fmt.Errorf("tuples at %d and %d overlap: %w", spans[i-1].off, spans[i].off, ErrCorruptPage)
		}
	}
	return nil
}
