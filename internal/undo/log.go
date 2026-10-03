package undo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
)

// Logger is how the undo log records its changes in the write-ahead log.
// The wal package's Logger implements it, with record types 10 (Undo) and
// 11 (UndoSegment).
type Logger interface {
	// LogUndo appends an Undo record describing a change to undo pages the
	// caller holds exclusively latched, and returns its LSN. build gets the
	// current redo point, which cannot change before the append. If it
	// fails, the record must be treated as possibly logged.
	LogUndo(ctx context.Context, build func(redoPoint uint64) []byte) (uint64, error)
	// LogUndoSegment appends an UndoSegment record with the given payload.
	LogUndoSegment(ctx context.Context, payload []byte) (uint64, error)
	// DeferFree asks for pages to be freed once no record that refers to
	// them can be replayed (docs/design/08-btree.md section 2.7).
	DeferFree(ctx context.Context, pages ...uint64) error
}

// segment is one transaction's undo segment.
type segment struct {
	pages []uint64 // in chain order; the last is the tail
	last  Ptr      // the latest record
}

// Log is the undo log: one segment of undo pages per transaction, and the
// segment table (docs/design/14-undo-log.md). Its methods are safe for
// concurrent use; Append, Release and Relog run one at a time.
type Log struct {
	bp *storage.BufferPool
	lg Logger

	mu   sync.Mutex
	segs map[uint64]*segment
}

// New returns an empty undo log over bp, logging to lg. After crash
// recovery has replayed the log into it (RedoSegments and Redo), Recover
// must run before anything else.
func New(bp *storage.BufferPool, lg Logger) *Log {
	return &Log{bp: bp, lg: lg, segs: map[uint64]*segment{}}
}

// Append writes rec at the end of rec.XID's segment, creating the segment
// at the transaction's first record, and returns its pointer. It sets
// rec.PrevInTxn to the transaction's previous record. An error after
// something may have been logged leaves the change in memory undone; the
// caller must not use the transaction further (its writes fail it).
func (l *Log) Append(ctx context.Context, rec Record) (Ptr, error) {
	if len(rec.Image) > MaxImage {
		return 0, fmt.Errorf("appending undo: image of %d bytes, at most %d: %w", len(rec.Image), MaxImage, ErrImageTooLarge)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	seg := l.segs[rec.XID]
	rec.PrevInTxn = 0
	if seg != nil {
		rec.PrevInTxn = seg.last
	}
	if msg, ok := rec.check(); !ok {
		return 0, fmt.Errorf("appending undo: %s: %w", msg, ErrInvalidRecord)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if seg != nil {
		p, done, err := l.appendInPlace(ctx, seg, &rec)
		if done || err != nil {
			return p, err
		}
	}
	return l.appendNewPage(ctx, seg, &rec)
}

// appendInPlace appends rec to the segment's last page if it fits there.
// done is false if it does not fit.
func (l *Log) appendInPlace(ctx context.Context, seg *segment, rec *Record) (p Ptr, done bool, err error) {
	tail := seg.pages[len(seg.pages)-1]
	ref, err := l.bp.FetchPage(ctx, tail)
	if err != nil {
		return 0, false, fmt.Errorf("appending undo: %w", err)
	}
	ref.Lock()
	defer func() {
		ref.Unlock()
		if uerr := ref.Unpin(done && err == nil); err == nil {
			err = uerr
		}
	}()
	buf := ref.Data()
	ph, err := decodePageHeader(buf, tail)
	if err != nil {
		return 0, false, fmt.Errorf("appending undo: %w", err)
	}
	if ph.xid != rec.XID || ph.next != 0 {
		return 0, false, fmt.Errorf("appending undo: last page %d of transaction %d's segment belongs to %d, links to %d: %w", tail, rec.XID, ph.xid, ph.next, ErrCorrupt)
	}
	if ph.used+rec.size() > storage.PageSize {
		return 0, false, nil
	}
	ref.MarkDirty()
	before := clonePage(buf)
	enc := encodeRecord(buf[ph.used:ph.used], rec)
	setUsed(buf, ph.used+len(enc))
	op := Block{Page: tail, Op: OpAppend, Offset: uint16(ph.used), Data: enc}
	lsn, err := l.lg.LogUndo(ctx, func(redo uint64) []byte {
		if storage.PageLSN(before) < redo {
			return EncodeBlocks([]Block{{Page: tail, Op: OpImage, Data: buf}})
		}
		return EncodeBlocks([]Block{op})
	})
	if err != nil {
		copy(buf, before)
		return 0, true, fmt.Errorf("logging undo: %w", err)
	}
	storage.SetPageLSN(buf, lsn)
	p = MakePtr(tail, ph.used)
	seg.last = p
	return p, true, nil
}

// appendNewPage allocates a page holding rec as its first record and logs
// its image. If seg is nil the page starts a new segment, which is then
// logged in the segment table; otherwise it is linked after the segment's
// last page, in the same log record. If logging fails the page is not
// freed: the record may still reach the log. It stays allocated and
// unreachable (a leak) instead.
func (l *Log) appendNewPage(ctx context.Context, seg *segment, rec *Record) (p Ptr, err error) {
	var tail *storage.PageRef
	if seg != nil {
		id := seg.pages[len(seg.pages)-1]
		if tail, err = l.bp.FetchPage(ctx, id); err != nil {
			return 0, fmt.Errorf("appending undo: %w", err)
		}
		tail.Lock()
		defer func() {
			tail.Unlock()
			if uerr := tail.Unpin(err == nil); err == nil {
				err = uerr
			}
		}()
		ph, perr := decodePageHeader(tail.Data(), id)
		if perr != nil {
			return 0, fmt.Errorf("appending undo: %w", perr)
		}
		if ph.xid != rec.XID || ph.next != 0 {
			return 0, fmt.Errorf("appending undo: last page %d of transaction %d's segment belongs to %d, links to %d: %w", id, rec.XID, ph.xid, ph.next, ErrCorrupt)
		}
	}
	ref, err := l.bp.NewPage(ctx, storage.PageTypeUndo)
	if err != nil {
		return 0, fmt.Errorf("appending undo: %w", err)
	}
	id := ref.ID()
	ref.Lock()
	ref.MarkDirty()
	buf := ref.Data()
	err = initPage(buf, id, rec.XID)
	if err == nil {
		enc := encodeRecord(buf[pageHeaderSize:pageHeaderSize], rec)
		setUsed(buf, pageHeaderSize+len(enc))
		err = l.logNewPage(ctx, buf, id, tail)
	}
	ref.Unlock()
	if uerr := ref.Unpin(err == nil); err == nil {
		err = uerr
	}
	if err != nil {
		return 0, err
	}
	p = MakePtr(id, pageHeaderSize)
	if seg == nil {
		// The page's image is logged first: a segment entry always names
		// a page whose image recovery has.
		if _, err := l.lg.LogUndoSegment(ctx, EncodeSegmentEntries([]SegmentEntry{{SegmentAdd, rec.XID, id}})); err != nil {
			return 0, fmt.Errorf("logging undo segment: %w", err)
		}
		l.segs[rec.XID] = &segment{pages: []uint64{id}, last: p}
		return p, nil
	}
	seg.pages = append(seg.pages, id)
	seg.last = p
	return p, nil
}

// logNewPage logs the image of the new page id (buf) and, if tail is not
// nil, the link to it from the segment's last page, as one record. On
// failure the last page is restored.
func (l *Log) logNewPage(ctx context.Context, buf []byte, id uint64, tail *storage.PageRef) error {
	var tailBuf, before []byte
	if tail != nil {
		tail.MarkDirty()
		tailBuf = tail.Data()
		before = clonePage(tailBuf)
		setNext(tailBuf, id)
	}
	lsn, err := l.lg.LogUndo(ctx, func(redo uint64) []byte {
		var blocks []Block
		if tail != nil {
			if storage.PageLSN(before) < redo {
				blocks = append(blocks, Block{Page: tail.ID(), Op: OpImage, Data: tailBuf})
			} else {
				blocks = append(blocks, Block{Page: tail.ID(), Op: OpSetNext, Next: id})
			}
		}
		return EncodeBlocks(append(blocks, Block{Page: id, Op: OpImage, Data: buf}))
	})
	if err != nil {
		if tail != nil {
			copy(tailBuf, before)
		}
		return fmt.Errorf("logging undo: %w", err)
	}
	storage.SetPageLSN(buf, lsn)
	if tail != nil {
		storage.SetPageLSN(tailBuf, lsn)
	}
	return nil
}

// Read returns the record p points to. Anything that is not a record of an
// undo page, at a record boundary, is ErrCorrupt.
func (l *Log) Read(ctx context.Context, p Ptr) (Record, error) {
	if p == 0 || !p.valid() {
		return Record{}, fmt.Errorf("reading undo record %s: bad pointer: %w", p, ErrCorrupt)
	}
	ref, err := l.bp.FetchPage(ctx, p.Page())
	if err != nil {
		if errors.Is(err, storage.ErrInvalidPageID) || errors.Is(err, storage.ErrZeroPage) {
			err = fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		return Record{}, fmt.Errorf("reading undo record %s: %w", p, err)
	}
	var out Record
	found := false
	ref.RLock()
	_, err = pageRecords(ref.Data(), p.Page(), func(off int, rec Record) {
		if off == p.Offset() {
			// The image aliases the page, which may change once unlatched.
			rec.Image = slices.Clone(rec.Image)
			out, found = rec, true
		}
	})
	ref.RUnlock()
	if uerr := ref.Unpin(false); err == nil {
		err = uerr
	}
	if err != nil {
		return Record{}, fmt.Errorf("reading undo record %s: %w", p, err)
	}
	if !found {
		return Record{}, fmt.Errorf("reading undo record %s: no record starts there: %w", p, ErrCorrupt)
	}
	return out, nil
}

// Last returns the latest record of xid's segment, 0 if it has none.
func (l *Log) Last(xid uint64) Ptr {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seg := l.segs[xid]; seg != nil {
		return seg.last
	}
	return 0
}

// Segments returns the transactions that have an undo segment, in
// increasing order.
func (l *Log) Segments() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]uint64, 0, len(l.segs))
	for xid := range l.segs {
		out = append(out, xid)
	}
	slices.Sort(out)
	return out
}

// Pages returns the pages of xid's segment, in chain order.
func (l *Log) Pages(xid uint64) []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seg := l.segs[xid]; seg != nil {
		return slices.Clone(seg.pages)
	}
	return nil
}

// Release ends xid's segment: it logs the segment's removal from the table,
// then the deferred free of its pages. A crash between the two leaks the
// pages instead of freeing pages the table still names.
func (l *Log) Release(ctx context.Context, xid uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	seg := l.segs[xid]
	if seg == nil {
		return fmt.Errorf("releasing undo of transaction %d: %w", xid, ErrNoSegment)
	}
	if _, err := l.lg.LogUndoSegment(ctx, EncodeSegmentEntries([]SegmentEntry{{SegmentDrop, xid, seg.pages[0]}})); err != nil {
		return fmt.Errorf("releasing undo of transaction %d: %w", xid, err)
	}
	delete(l.segs, xid)
	if err := l.lg.DeferFree(ctx, seg.pages...); err != nil {
		return fmt.Errorf("releasing undo of transaction %d: %w", xid, err)
	}
	return nil
}

// Relog logs the whole segment table again, as add entries. A checkpoint
// calls it after moving its redo point, so recovery from that point knows
// every segment without the log before it (section 2.4).
func (l *Log) Relog(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	xids := make([]uint64, 0, len(l.segs))
	for xid := range l.segs {
		xids = append(xids, xid)
	}
	slices.Sort(xids)
	for len(xids) > 0 {
		n := min(len(xids), MaxSegmentEntries)
		entries := make([]SegmentEntry, n)
		for i, xid := range xids[:n] {
			entries[i] = SegmentEntry{SegmentAdd, xid, l.segs[xid].pages[0]}
		}
		if _, err := l.lg.LogUndoSegment(ctx, EncodeSegmentEntries(entries)); err != nil {
			return fmt.Errorf("logging the undo segment table: %w", err)
		}
		xids = xids[n:]
	}
	return nil
}

// RedoSegments applies a replayed UndoSegment record to the table. An add
// of a segment already there must name the same first page (the table was
// logged again by a checkpoint). A drop of a segment not there is ignored:
// a drop logged after a checkpoint's redo point, before the checkpoint
// logged the table again, follows no add in the log replayed from it.
func (l *Log) RedoSegments(payload []byte) error {
	entries, err := DecodeSegmentEntries(payload)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range entries {
		seg := l.segs[e.XID]
		switch e.Op {
		case SegmentAdd:
			if seg == nil {
				l.segs[e.XID] = &segment{pages: []uint64{e.First}}
			} else if seg.pages[0] != e.First {
				return fmt.Errorf("undo segment of transaction %d added at page %d, and before at %d: %w", e.XID, e.First, seg.pages[0], ErrCorrupt)
			}
		case SegmentDrop:
			if seg != nil && seg.pages[0] != e.First {
				return fmt.Errorf("undo segment of transaction %d dropped at page %d, but starts at %d: %w", e.XID, e.First, seg.pages[0], ErrCorrupt)
			}
			delete(l.segs, e.XID)
		}
	}
	return nil
}

// Recover finds the end of every segment after replay, by following its
// pages from the first, and checks them: each must be a valid undo page of
// the segment's transaction, and no page may appear twice, in one segment
// or two. Anything else is ErrCorrupt.
func (l *Log) Recover(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	seen := map[uint64]uint64{} // page -> transaction
	for _, xid := range l.sortedXIDs() {
		seg := l.segs[xid]
		pages := []uint64{}
		var last Ptr
		for id := seg.pages[0]; id != 0; {
			if other, ok := seen[id]; ok {
				return fmt.Errorf("recovering undo: page %d is in the segments of transactions %d and %d: %w", id, other, xid, ErrCorrupt)
			}
			seen[id] = xid
			ph, err := l.walkPage(ctx, id, func(off int, _ Record) { last = MakePtr(id, off) })
			if err != nil {
				return fmt.Errorf("recovering undo of transaction %d: %w", xid, err)
			}
			if ph.xid != xid {
				return fmt.Errorf("recovering undo: page %d of transaction %d's segment belongs to %d: %w", id, xid, ph.xid, ErrCorrupt)
			}
			pages = append(pages, id)
			id = ph.next
		}
		seg.pages, seg.last = pages, last
	}
	return nil
}

// walkPage validates the undo page id and calls fn for each record.
func (l *Log) walkPage(ctx context.Context, id uint64, fn func(off int, rec Record)) (pageHeader, error) {
	ref, err := l.bp.FetchPage(ctx, id)
	if err != nil {
		if errors.Is(err, storage.ErrInvalidPageID) || errors.Is(err, storage.ErrZeroPage) {
			err = fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		return pageHeader{}, err
	}
	ref.RLock()
	ph, err := pageRecords(ref.Data(), id, fn)
	ref.RUnlock()
	if uerr := ref.Unpin(false); err == nil {
		err = uerr
	}
	return ph, err
}

func (l *Log) sortedXIDs() []uint64 {
	out := make([]uint64, 0, len(l.segs))
	for xid := range l.segs {
		out = append(out, xid)
	}
	slices.Sort(out)
	return out
}
