package storage

import (
	"bytes"
	"context"
	"testing"
)

// Byte-level helpers for tests that are about pages and heaps, not row
// versions: every write is stamped with transaction 1 and no undo.

func stampXID(xid uint64) Stamper {
	return func(RID, *Version) (RowHeader, error) { return RowHeader{XID: xid}, nil }
}

var stamp1 = stampXID(1)

func (h *Heap) put(ctx context.Context, data []byte) (RID, error) {
	return h.Insert(ctx, data, stamp1)
}

func (h *Heap) get(ctx context.Context, rid RID) ([]byte, error) {
	v, err := h.Get(ctx, rid)
	return v.Data, err
}

func (h *Heap) set(ctx context.Context, rid RID, data []byte) error {
	return h.Update(ctx, rid, data, stamp1)
}

func (h *Heap) del(ctx context.Context, rid RID) error {
	return h.Delete(ctx, rid, stamp1)
}

func (s *Scanner) nextData(ctx context.Context) (RID, []byte, bool, error) {
	rid, v, ok, err := s.Next(ctx)
	return rid, v.Data, ok, err
}

// fillPage inserts rows until page i of the heap cannot take another one,
// and returns how many it inserted. Every other page must be full.
func (h *Heap) fillPage(t testing.TB, i int, b byte) int {
	t.Helper()
	n := 0
	for {
		h.mu.Lock()
		id, free := h.pages[i].id, h.pages[i].free
		h.mu.Unlock()
		if free < RowHeaderSize+1 {
			return n
		}
		rid, err := h.put(context.Background(), bytes.Repeat([]byte{b}, min(free-RowHeaderSize, MaxRowData)))
		if err != nil {
			t.Fatal(err)
		}
		if rid.Page != id {
			t.Fatalf("filling page %d: row went to page %d", id, rid.Page)
		}
		n++
	}
}
