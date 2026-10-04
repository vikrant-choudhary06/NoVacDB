package storage

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

func goldenTuples() map[string]struct {
	t   rowTuple
	hex string
} {
	hdr := RowHeader{XID: 0x0102030405060708, Undo: 0x1112131415161718}
	return map[string]struct {
		t   rowTuple
		hex string
	}{
		"plain": {plainTuple(hdr, []byte("ab")),
			"00" + "00" + "0807060504030201" + "1817161514131211" + "6162"},
		"empty plain": {plainTuple(RowHeader{XID: 1}, nil),
			"00" + "00" + "0100000000000000" + "0000000000000000"},
		"tombstone": {rowTuple{kind: tupleTombstone, hdr: hdr},
			"01" + "00" + "0807060504030201" + "1817161514131211"},
		"forward stub": {rowTuple{kind: tupleStub, link: RID{Page: 9, Slot: 3}},
			"02" + "00" + "0900000000000000" + "0300" + "000000000000"},
		"moved-in": {rowTuple{kind: tupleMoved, hdr: hdr, link: RID{Page: 7, Slot: 0x102}, data: []byte("z")},
			"04" + "00" + "0807060504030201" + "1817161514131211" + "0700000000000000" + "0201" + "7a"},
	}
}

func TestRowTupleGoldenBytes(t *testing.T) {
	for name, c := range goldenTuples() {
		got := c.t.encode()
		if hex.EncodeToString(got) != c.hex {
			t.Errorf("%s: encoding\n got %x\nwant %s", name, got, c.hex)
		}
		if len(got) != c.t.size() {
			t.Errorf("%s: %d bytes, size() says %d", name, len(got), c.t.size())
		}
		back, err := decodeTuple(got)
		if err != nil || back.kind != c.t.kind || back.hdr != c.t.hdr || back.link != c.t.link || !bytes.Equal(back.data, c.t.data) {
			t.Errorf("%s: decode %+v, %v", name, back, err)
		}
	}
	if RowHeaderSize != 18 || stubSize != RowHeaderSize || movedSize != 28 || MaxRowData != 8000 {
		t.Fatal("row layout constants moved")
	}
	// Every version fits an undo record (54-byte header, 8090-byte image),
	// and a moved-in tuple fits a heap page.
	if 16+MaxRowData > 8090 || movedSize+MaxRowData > MaxTupleSize {
		t.Fatal("MaxRowData too large")
	}
}

func TestDecodeTupleRejects(t *testing.T) {
	for name, c := range goldenTuples() {
		b := c.t.encode()
		for n := range len(b) {
			if (c.t.kind == tuplePlain && n >= RowHeaderSize) || (c.t.kind == tupleMoved && n >= movedSize) {
				continue // a shorter row is still a row
			}
			if _, err := decodeTuple(b[:n]); !errors.Is(err, ErrCorruptHeap) {
				t.Errorf("%s truncated to %d: %v", name, n, err)
			}
		}
	}
	g := goldenTuples()
	mod := func(base string, f func(b []byte)) []byte {
		b := g[base].t.encode()
		f(b)
		return b
	}
	for name, b := range map[string][]byte{
		"unknown flag":          mod("plain", func(b []byte) { b[0] = 8 }),
		"two flags":             mod("plain", func(b []byte) { b[0] = flagDeleted | flagForward }),
		"all flags":             mod("plain", func(b []byte) { b[0] = 0xFF }),
		"reserved byte":         mod("plain", func(b []byte) { b[1] = 1 }),
		"transaction 0":         mod("plain", func(b []byte) { clear(b[2:10]) }),
		"tombstone with data":   append(g["tombstone"].t.encode(), 1),
		"tombstone of xid 0":    mod("tombstone", func(b []byte) { clear(b[2:10]) }),
		"stub too long":         append(g["forward stub"].t.encode(), 0),
		"stub to page 0":        mod("forward stub", func(b []byte) { clear(b[2:10]) }),
		"stub reserved set":     mod("forward stub", func(b []byte) { b[17] = 1 }),
		"moved-in from page 1":  mod("moved-in", func(b []byte) { b[18] = 1; clear(b[19:26]) }),
		"moved-in of xid 0":     mod("moved-in", func(b []byte) { clear(b[2:10]) }),
		"row over the limit":    plainTuple(RowHeader{XID: 1}, make([]byte, MaxRowData+1)).encode(),
		"moved row over limit":  rowTuple{kind: tupleMoved, hdr: RowHeader{XID: 1}, link: RID{Page: 5}, data: make([]byte, MaxRowData+1)}.encode(),
		"moved-in without home": mod("moved-in", func(b []byte) {})[:27],
	} {
		if _, err := decodeTuple(b); !errors.Is(err, ErrCorruptHeap) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := decodeTuple(plainTuple(RowHeader{XID: 1}, make([]byte, MaxRowData)).encode()); err != nil {
		t.Fatalf("largest row: %v", err)
	}
}

func FuzzRowHeader(f *testing.F) {
	for _, c := range goldenTuples() {
		f.Add(c.t.encode())
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		tu, err := decodeTuple(b)
		if err != nil {
			if !errors.Is(err, ErrCorruptHeap) {
				t.Fatalf("error not ErrCorruptHeap: %v", err)
			}
			return
		}
		if enc := tu.encode(); !bytes.Equal(enc, b) {
			t.Fatalf("re-encoding differs\n got %x\nwant %x", enc, b)
		}
	})
}

// A heap page holding a tuple that does not decode is refused when the heap
// is opened.
func TestOpenHeapRejectsBadTuples(t *testing.T) {
	for name, bad := range map[string][]byte{
		"unknown flag":  {8, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		"transaction 0": make([]byte, RowHeaderSize),
		"short":         {0},
	} {
		t.Run(name, func(t *testing.T) {
			e, h := newHeapEnv(t, 4)
			if _, err := h.put(bg, []byte("good")); err != nil {
				t.Fatal(err)
			}
			err := withPage(bg, e.bp, h.FirstPage(), true, true, func(sp *SlottedPage) (bool, error) {
				_, err := sp.Insert(bad)
				return err == nil, err
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := e.bp.FlushAll(bg); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenHeap(bg, e.bp, h.FirstPage()); !errors.Is(err, ErrCorruptHeap) {
				t.Fatalf("OpenHeap: %v", err)
			}
			if _, _, _, err := h.Scan().Next(bg); err != nil {
				t.Fatalf("the good row comes first: %v", err)
			}
		})
	}
}
