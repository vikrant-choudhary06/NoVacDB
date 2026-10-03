package undo

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
)

func TestLayoutConstants(t *testing.T) {
	if pageHeaderSize != 48 || RecordHeaderSize != 54 || MaxImage != 8090 {
		t.Fatalf("page header %d, record header %d, max image %d", pageHeaderSize, RecordHeaderSize, MaxImage)
	}
	if offPageXID != 24 || offPageNext != 32 || offPageUsed != 40 || offPageReserve != 42 {
		t.Fatal("undo page header offsets moved")
	}
	if MaxSegmentEntries*segmentEntrySize+2 > 1<<16 {
		t.Fatal("segment records too large")
	}
}

func TestPtr(t *testing.T) {
	p := MakePtr(7, 300)
	if p != 7*8192+300 || p.Page() != 7 || p.Offset() != 300 || p.String() != "(7,300)" {
		t.Fatalf("pointer %d = %s", p, p)
	}
	for _, c := range []struct {
		p    Ptr
		want bool
	}{
		{0, true},
		{MakePtr(storage.FirstDataPage, pageHeaderSize), true},
		{MakePtr(storage.FirstDataPage, storage.PageSize-RecordHeaderSize), true},
		{MakePtr(storage.FirstDataPage, storage.PageSize-RecordHeaderSize+1), false},
		{MakePtr(storage.FirstDataPage, pageHeaderSize-1), false},
		{MakePtr(storage.FirstDataPage-1, pageHeaderSize), false},
		{MakePtr(0, 100), false},
		{1, false},
	} {
		if got := c.p.valid(); got != c.want {
			t.Errorf("%s valid = %v", c.p, got)
		}
	}
}

func goldenRecord() Record {
	return Record{
		Kind:       Update,
		XID:        0x0102030405060708,
		PrevForRow: MakePtr(5, 48),
		PrevInTxn:  MakePtr(9, 102),
		Table:      42,
		RID:        storage.RID{Page: 17, Slot: 3},
		Image:      []byte("abc"),
	}
}

func TestRecordGoldenBytes(t *testing.T) {
	exp, err := hex.DecodeString("3900" + "02" + "00" + "00000000" +
		"0807060504030201" + // XID
		"30a0000000000000" + // PrevForRow: 5*8192+48 = 0xA030
		"6620010000000000" + // PrevInTxn: 9*8192+102 = 0x12066
		"2a00000000000000" + // Table
		"1100000000000000" + "0300" + // RID
		"03000000" + "616263") // image
	if err != nil {
		t.Fatal(err)
	}
	got := EncodeRecord(goldenRecord())
	if !bytes.Equal(got, exp) {
		t.Fatalf("encoding\n got %x\nwant %x", got, exp)
	}
	if len(got) != 57 || got[0] != 57 {
		t.Fatalf("length %d", len(got))
	}
	rec, n, err := DecodeRecord(append(got, 0xFF, 0xFF))
	if err != nil || n != 57 || !sameRecord(rec, goldenRecord()) {
		t.Fatalf("decode: %+v %d %v", rec, n, err)
	}
}

func TestRecordRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(testSeed(t), 1))
	sizes := map[Kind]int{}
	for range 2000 {
		rec := randomRecord(rng, 1+rng.Uint64N(1<<40))
		if rng.IntN(2) == 0 {
			rec.PrevInTxn = MakePtr(storage.FirstDataPage+rng.Uint64N(50), pageHeaderSize+rng.IntN(1000))
		}
		b := EncodeRecord(rec)
		if len(b) != rec.size() {
			t.Fatalf("encoded %d bytes, size %d", len(b), rec.size())
		}
		got, n, err := DecodeRecord(b)
		if err != nil || n != len(b) || !sameRecord(got, rec) {
			t.Fatalf("round trip of %+v: %+v %d %v", rec, got, n, err)
		}
		sizes[rec.Kind] = max(sizes[rec.Kind], len(rec.Image))
	}
	if sizes[Update] != MaxImage && sizes[Delete] != MaxImage {
		t.Fatalf("no record with the largest image: %v", sizes)
	}
	// The largest record fills a page exactly.
	if pageHeaderSize+RecordHeaderSize+MaxImage != storage.PageSize {
		t.Fatal("MaxImage does not fill a page")
	}
}

func TestDecodeRecordRejects(t *testing.T) {
	good := EncodeRecord(goldenRecord())
	for n := range len(good) {
		if _, _, err := DecodeRecord(good[:n]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncated to %d: %v", n, err)
		}
	}
	insert := EncodeRecord(Record{Kind: Insert, XID: 1, RID: storage.RID{Page: storage.FirstDataPage}})
	mod := func(base []byte, f func(b []byte)) []byte {
		b := bytes.Clone(base)
		f(b)
		return b
	}
	for name, b := range map[string][]byte{
		"length too long":              mod(good, func(b []byte) { b[0]++ }),
		"length too short":             mod(good, func(b []byte) { b[0]-- }),
		"length too long, bytes after": append(mod(good, func(b []byte) { b[0]++ }), 0),
		"image length too long":        mod(good, func(b []byte) { b[50]++ }),
		"image length huge":            mod(good, func(b []byte) { binary.LittleEndian.PutUint32(b[50:], 1<<31) }),
		"kind 0":                       mod(good, func(b []byte) { b[2] = 0 }),
		"kind 4":                       mod(good, func(b []byte) { b[2] = 4 }),
		"flags":                        mod(good, func(b []byte) { b[3] = 1 }),
		"reserved":                     mod(good, func(b []byte) { b[7] = 1 }),
		"XID 0":                        mod(good, func(b []byte) { clear(b[8:16]) }),
		"bad row pointer":              mod(good, func(b []byte) { binary.LittleEndian.PutUint64(b[16:], uint64(MakePtr(5, 10))) }),
		"row pointer in page 0":        mod(good, func(b []byte) { binary.LittleEndian.PutUint64(b[16:], uint64(MakePtr(0, 100))) }),
		"bad txn pointer":              mod(good, func(b []byte) { binary.LittleEndian.PutUint64(b[24:], uint64(MakePtr(5, 8190))) }),
		"RID in page 0":                mod(good, func(b []byte) { clear(b[40:48]) }),
		"update without image":         mod(insert, func(b []byte) { b[2] = byte(Update) }),
		"delete without image":         mod(insert, func(b []byte) { b[2] = byte(Delete) }),
		"insert with image":            mod(good, func(b []byte) { b[2] = byte(Insert) }),
		"image over MaxImage": func() []byte {
			b := EncodeRecord(Record{Kind: Update, XID: 1, RID: storage.RID{Page: storage.FirstDataPage}, Image: make([]byte, MaxImage+1)})
			return b
		}(),
	} {
		if _, _, err := DecodeRecord(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, _, err := DecodeRecord(insert); err != nil {
		t.Fatalf("a valid insert: %v", err)
	}
}

func FuzzUndoRecord(f *testing.F) {
	f.Add(EncodeRecord(goldenRecord()))
	f.Add(EncodeRecord(Record{Kind: Insert, XID: 1, RID: storage.RID{Page: storage.FirstDataPage}}))
	f.Add(append(EncodeRecord(goldenRecord()), 1, 2, 3))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		rec, n, err := DecodeRecord(b)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error not ErrCorrupt: %v", err)
			}
			return
		}
		if n > len(b) {
			t.Fatalf("length %d past %d bytes", n, len(b))
		}
		if enc := EncodeRecord(rec); !bytes.Equal(enc, b[:n]) {
			t.Fatalf("re-encoding differs\n got %x\nwant %x", enc, b[:n])
		}
		if msg, ok := rec.check(); !ok {
			t.Fatalf("decoded an invalid record: %s", msg)
		}
	})
}

// goldenPage returns undo page 9 of transaction 5 with the golden record.
func goldenPage(t testing.TB) []byte {
	const id = 9
	t.Helper()
	buf := make([]byte, storage.PageSize)
	if err := initPage(buf, id, 5); err != nil {
		t.Fatal(err)
	}
	rec := goldenRecord()
	rec.XID = 5
	enc := encodeRecord(buf[pageHeaderSize:pageHeaderSize], &rec)
	setUsed(buf, pageHeaderSize+len(enc))
	return buf
}

func TestPageGoldenHeader(t *testing.T) {
	buf := goldenPage(t)
	setNext(buf, 12)
	want := make([]byte, pageHeaderSize)
	binary.LittleEndian.PutUint64(want[0:], 9)                             // page ID
	binary.LittleEndian.PutUint16(want[20:], uint16(storage.PageTypeUndo)) // type 6
	binary.LittleEndian.PutUint64(want[24:], 5)                            // XID
	binary.LittleEndian.PutUint64(want[32:], 12)                           // next
	binary.LittleEndian.PutUint16(want[40:], 48+57)                        // used
	if !bytes.Equal(buf[:pageHeaderSize], want) {
		t.Fatalf("header\n got %x\nwant %x", buf[:pageHeaderSize], want)
	}
	var offs []int
	ph, err := pageRecords(buf, 9, func(off int, _ Record) { offs = append(offs, off) })
	if err != nil || ph != (pageHeader{xid: 5, next: 12, used: 105}) || len(offs) != 1 || offs[0] != 48 {
		t.Fatalf("pageRecords: %+v %v %v", ph, offs, err)
	}
}

func TestPageRecordsRejects(t *testing.T) {
	good := goldenPage(t)
	mod := func(f func(b []byte)) []byte {
		b := bytes.Clone(good)
		f(b)
		return b
	}
	heap := make([]byte, storage.PageSize)
	if err := storage.InitPage(heap, storage.Header{ID: 9, Type: storage.PageTypeHeap}); err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string][]byte{
		"heap page":            heap,
		"labelled a heap page": mod(func(b []byte) { b[20] = byte(storage.PageTypeHeap) }),
		"labelled a leaf":      mod(func(b []byte) { b[20] = byte(storage.PageTypeBTreeLeaf) }),
		"zero page":            make([]byte, storage.PageSize),
		"wrong ID":             mod(func(b []byte) { b[0] = 8 }),
		"flags":                mod(func(b []byte) { b[22] = 1 }),
		"XID 0":                mod(func(b []byte) { clear(b[24:32]) }),
		"next in page 0":       mod(func(b []byte) { b[32] = 1 }),
		"next is itself":       mod(func(b []byte) { b[32] = 9 }),
		"reserved":             mod(func(b []byte) { b[45] = 1 }),
		"used below header":    mod(func(b []byte) { setUsed(b, 47) }),
		"used past page":       mod(func(b []byte) { setUsed(b, storage.PageSize+1) }),
		"empty":                mod(func(b []byte) { setUsed(b, 48); clear(b[48:]) }),
		"used inside record":   mod(func(b []byte) { setUsed(b, 100) }),
		"used past record":     mod(func(b []byte) { setUsed(b, 106) }),
		"bytes after records":  mod(func(b []byte) { b[storage.PageSize-1] = 1 }),
		"other transaction's":  mod(func(b []byte) { b[48+8] = 6 }),
		"damaged record":       mod(func(b []byte) { b[48+2] = 9 }),
	} {
		if _, err := pageRecords(b, 9, nil); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestBlocksRoundTripAndGolden(t *testing.T) {
	page := goldenPage(t)
	rec := goldenRecord()
	enc := EncodeRecord(rec)
	blocks := []Block{
		{Page: 7, Op: OpSetNext, Next: 9},
		{Page: 9, Op: OpImage, Data: page},
	}
	p := EncodeBlocks(blocks)
	if p[0] != 2 || binary.LittleEndian.Uint64(p[1:]) != 7 || p[9] != byte(OpSetNext) || binary.LittleEndian.Uint64(p[10:]) != 9 ||
		binary.LittleEndian.Uint64(p[18:]) != 9 || p[26] != byte(OpImage) || !bytes.Equal(p[27:], page) {
		t.Fatalf("encoding %x...", p[:40])
	}
	got, err := DecodeBlocks(p)
	if err != nil || len(got) != 2 || got[0].Next != 9 || !bytes.Equal(got[1].Data, page) {
		t.Fatalf("decode: %+v %v", got, err)
	}
	app := EncodeBlocks([]Block{{Page: 9, Op: OpAppend, Offset: 105, Data: enc}})
	want := []byte{1, 9, 0, 0, 0, 0, 0, 0, 0, byte(OpAppend), 105, 0, byte(len(enc)), 0}
	if !bytes.Equal(app, append(want, enc...)) {
		t.Fatalf("append block %x", app)
	}
	got, err = DecodeBlocks(app)
	if err != nil || got[0].Offset != 105 || !bytes.Equal(got[0].Data, enc) {
		t.Fatalf("decode append: %+v %v", got, err)
	}
}

func TestDecodeBlocksRejects(t *testing.T) {
	page := goldenPage(t)
	enc := EncodeRecord(goldenRecord())
	good := EncodeBlocks([]Block{{Page: 7, Op: OpSetNext, Next: 9}, {Page: 9, Op: OpImage, Data: page}})
	for n := range len(good) {
		if _, err := DecodeBlocks(good[:n]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncated to %d: %v", n, err)
		}
	}
	badPage := bytes.Clone(page)
	badPage[48+2] = 9
	for name, p := range map[string][]byte{
		"no blocks":          {0},
		"three blocks":       {3},
		"three valid blocks": EncodeBlocks([]Block{{Page: 7, Op: OpSetNext, Next: 9}, {Page: 8, Op: OpSetNext, Next: 9}, {Page: 10, Op: OpSetNext, Next: 9}}),
		"trailing byte":      append(bytes.Clone(good), 0),
		"page 0":             EncodeBlocks([]Block{{Page: 0, Op: OpSetNext, Next: 9}}),
		"op 0":               EncodeBlocks([]Block{{Page: 7, Op: 0}}),
		"op 4":               append(EncodeBlocks([]Block{{Page: 7, Op: 4}}), make([]byte, 8)...),
		"page twice":         EncodeBlocks([]Block{{Page: 9, Op: OpSetNext, Next: 10}, {Page: 9, Op: OpImage, Data: page}}),
		"link to itself":     EncodeBlocks([]Block{{Page: 7, Op: OpSetNext, Next: 7}}),
		"link to page 0":     EncodeBlocks([]Block{{Page: 7, Op: OpSetNext, Next: 0}}),
		"image of page 8":    EncodeBlocks([]Block{{Page: 8, Op: OpImage, Data: page}}),
		"bad image":          EncodeBlocks([]Block{{Page: 9, Op: OpImage, Data: badPage}}),
		"append at 47":       EncodeBlocks([]Block{{Page: 9, Op: OpAppend, Offset: 47, Data: enc}}),
		"append past end":    EncodeBlocks([]Block{{Page: 9, Op: OpAppend, Offset: storage.PageSize - 56, Data: enc}}),
		"append garbage":     EncodeBlocks([]Block{{Page: 9, Op: OpAppend, Offset: 100, Data: enc[:20]}}),
		"append two":         EncodeBlocks([]Block{{Page: 9, Op: OpAppend, Offset: 100, Data: append(bytes.Clone(enc), enc...)}}),
	} {
		if _, err := DecodeBlocks(p); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func FuzzDecodeBlocks(f *testing.F) {
	f.Add(EncodeBlocks([]Block{{Page: 9, Op: OpAppend, Offset: 105, Data: EncodeRecord(goldenRecord())}}))
	f.Add(EncodeBlocks([]Block{{Page: 7, Op: OpSetNext, Next: 9}}))
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, p []byte) {
		blocks, err := DecodeBlocks(p)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error not ErrCorrupt: %v", err)
			}
			return
		}
		if enc := EncodeBlocks(blocks); !bytes.Equal(enc, p) {
			t.Fatalf("re-encoding differs")
		}
	})
}

func TestSegmentEntries(t *testing.T) {
	entries := []SegmentEntry{{SegmentAdd, 3, 10}, {SegmentDrop, 0x0102, 11}}
	p := EncodeSegmentEntries(entries)
	want := "0200" + "01" + "0300000000000000" + "0a00000000000000" + "02" + "0201000000000000" + "0b00000000000000"
	if hex.EncodeToString(p) != want {
		t.Fatalf("encoding %x", p)
	}
	got, err := DecodeSegmentEntries(p)
	if err != nil || len(got) != 2 || got[0] != entries[0] || got[1] != entries[1] {
		t.Fatalf("decode: %v %v", got, err)
	}
	for n := range len(p) {
		if _, err := DecodeSegmentEntries(p[:n]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncated to %d: %v", n, err)
		}
	}
	mod := func(f func(b []byte)) []byte {
		b := bytes.Clone(p)
		f(b)
		return b
	}
	many := make([]SegmentEntry, MaxSegmentEntries+1)
	for i := range many {
		many[i] = SegmentEntry{SegmentAdd, uint64(i + 1), 10}
	}
	for name, b := range map[string][]byte{
		"no entries":    {0, 0},
		"too many":      EncodeSegmentEntries(many),
		"trailing byte": append(bytes.Clone(p), 0),
		"op 0":          mod(func(b []byte) { b[2] = 0 }),
		"op 3":          mod(func(b []byte) { b[2] = 3 }),
		"XID 0":         mod(func(b []byte) { clear(b[3:11]) }),
		"page 0":        mod(func(b []byte) { clear(b[11:19]) }),
		"count wrong":   mod(func(b []byte) { b[0] = 1 }),
	} {
		if _, err := DecodeSegmentEntries(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := DecodeSegmentEntries(EncodeSegmentEntries(many[:MaxSegmentEntries])); err != nil {
		t.Fatalf("%d entries: %v", MaxSegmentEntries, err)
	}
}
