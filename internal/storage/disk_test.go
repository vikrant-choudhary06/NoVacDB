package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

const dbName = "/db/data"

var bg = context.Background()

// backend gives a test an FS plus the path of a (not yet created) data file.
type backend struct {
	name string
	new  func(t *testing.T) (vfs.FS, string)
}

func backends() []backend {
	return []backend{
		{"MemFS", func(t *testing.T) (vfs.FS, string) { return newMem(t), dbName }},
		{"OSFS", func(t *testing.T) (vfs.FS, string) { return vfs.OSFS{}, t.TempDir() + "/data" }},
	}
}

// newMem returns a MemFS with a durable /db directory.
func newMem(t testing.TB) *vfs.MemFS {
	t.Helper()
	seed := uint64(1)
	if tt, ok := t.(*testing.T); ok {
		seed = testSeed(tt)
	}
	m := vfs.NewMemFS(seed)
	if err := m.MkdirAll("/db"); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("/"); err != nil {
		t.Fatal(err)
	}
	return m
}

func mustCreate(t testing.TB, fsys vfs.FS, name string) *DiskManager {
	t.Helper()
	dm, err := Create(fsys, name)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return dm
}

func mustOpenDM(t testing.TB, fsys vfs.FS, name string) *DiskManager {
	t.Helper()
	dm, err := Open(fsys, name)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return dm
}

// dataPage builds a heap page for id with pseudo-random payload from rng.
func dataPage(t testing.TB, id uint64, rng *rand.Rand) []byte {
	t.Helper()
	buf := make([]byte, PageSize)
	if err := InitPage(buf, Header{ID: id, LSN: rng.Uint64(), Type: PageTypeHeap}); err != nil {
		t.Fatal(err)
	}
	for i := HeaderSize; i < PageSize; i++ {
		buf[i] = byte(rng.UintN(256))
	}
	return buf
}

func mustAlloc(t testing.TB, dm *DiskManager) uint64 {
	t.Helper()
	id, err := dm.Allocate(bg)
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	return id
}

func mustRead(t testing.TB, dm *DiskManager, id uint64) []byte {
	t.Helper()
	buf := make([]byte, PageSize)
	if err := dm.ReadPage(bg, id, buf); err != nil {
		t.Fatalf("ReadPage(%d): %v", id, err)
	}
	return buf
}

func readRawFile(t testing.TB, fsys vfs.FS, id uint64) []byte {
	t.Helper()
	f, err := fsys.OpenFile(dbName, vfs.ORead)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, PageSize)
	if err := readRaw(f, id, buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

func writeRawFile(t testing.TB, fsys vfs.FS, id uint64, buf []byte) {
	t.Helper()
	f, err := fsys.OpenFile(dbName, vfs.ORead|vfs.OWrite)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := writeRaw(f, id, buf); err != nil {
		t.Fatal(err)
	}
}

// freeListIDs walks the free list from the in-memory header and fails the
// test on any inconsistency (cycle, bad page, wrong length).
func freeListIDs(t testing.TB, dm *DiskManager) []uint64 {
	t.Helper()
	var ids []uint64
	seen := map[uint64]bool{}
	for id := dm.hdr.freeHead; id != 0; {
		if seen[id] {
			t.Fatalf("free list cycle at page %d", id)
		}
		if id < FirstDataPage || id >= dm.hdr.pageCount {
			t.Fatalf("free list contains out-of-range page %d", id)
		}
		seen[id] = true
		ids = append(ids, id)
		buf := mustRead(t, dm, id)
		next, err := parseFreePage(buf, id, dm.hdr.pageCount)
		if err != nil {
			t.Fatalf("free list page %d: %v", id, err)
		}
		id = next
	}
	if uint64(len(ids)) != dm.hdr.freeCount {
		t.Fatalf("free list has %d pages, header says %d", len(ids), dm.hdr.freeCount)
	}
	return ids
}

func TestCreateOpen(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			fsys, name := b.new(t)
			if _, err := Open(fsys, name); !errors.Is(err, vfs.ErrNotExist) {
				t.Fatalf("Open missing err = %v", err)
			}
			dm := mustCreate(t, fsys, name)
			if dm.PageCount() != FirstDataPage || dm.FreePageCount() != 0 {
				t.Fatalf("fresh counts = %d, %d", dm.PageCount(), dm.FreePageCount())
			}
			if err := dm.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Create(fsys, name); !errors.Is(err, vfs.ErrExist) {
				t.Fatalf("second Create err = %v", err)
			}
			dm = mustOpenDM(t, fsys, name)
			if dm.PageCount() != FirstDataPage {
				t.Fatalf("PageCount = %d", dm.PageCount())
			}
			if err := dm.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPagesSurviveReopen(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			fsys, name := b.new(t)
			rng := rand.New(rand.NewPCG(testSeed(t), 7))
			dm := mustCreate(t, fsys, name)
			want := map[uint64][]byte{}
			for range 20 {
				id := mustAlloc(t, dm)
				page := dataPage(t, id, rng)
				if err := dm.WritePage(bg, id, page); err != nil {
					t.Fatal(err)
				}
				want[id] = bytes.Clone(page)
			}
			if err := dm.Close(); err != nil {
				t.Fatal(err)
			}
			dm = mustOpenDM(t, fsys, name)
			defer func() { _ = dm.Close() }()
			if dm.PageCount() != 22 {
				t.Fatalf("PageCount = %d", dm.PageCount())
			}
			for id, page := range want {
				if got := mustRead(t, dm, id); !bytes.Equal(got, page) {
					t.Fatalf("page %d differs after reopen", id)
				}
			}
		})
	}
}

func TestAllocateAndFree(t *testing.T) {
	for _, b := range backends() {
		t.Run(b.name, func(t *testing.T) {
			fsys, name := b.new(t)
			dm := mustCreate(t, fsys, name)
			defer func() { _ = dm.Close() }()
			for want := uint64(2); want < 7; want++ {
				if id := mustAlloc(t, dm); id != want {
					t.Fatalf("Allocate = %d, want %d", id, want)
				}
			}
			// A fresh page reads as a zero page, not corruption.
			buf := make([]byte, PageSize)
			if err := dm.ReadPage(bg, 3, buf); !errors.Is(err, ErrZeroPage) {
				t.Fatalf("fresh page err = %v", err)
			}
			rng := rand.New(rand.NewPCG(1, 1))
			if err := dm.WritePage(bg, 4, dataPage(t, 4, rng)); err != nil {
				t.Fatal(err)
			}
			for _, id := range []uint64{4, 2, 6} { // written, unwritten, unwritten
				if err := dm.Free(bg, id); err != nil {
					t.Fatalf("Free(%d): %v", id, err)
				}
			}
			if dm.FreePageCount() != 3 {
				t.Fatalf("FreePageCount = %d", dm.FreePageCount())
			}
			if got := freeListIDs(t, dm); fmt.Sprint(got) != "[6 2 4]" {
				t.Fatalf("free list = %v, want [6 2 4]", got)
			}
			// LIFO reuse, and reused pages read as zeros again.
			for _, want := range []uint64{6, 2, 4} {
				id := mustAlloc(t, dm)
				if id != want {
					t.Fatalf("reused %d, want %d", id, want)
				}
				if err := dm.ReadPage(bg, id, buf); !errors.Is(err, ErrZeroPage) {
					t.Fatalf("reused page %d err = %v", id, err)
				}
			}
			if dm.FreePageCount() != 0 || dm.PageCount() != 7 {
				t.Fatalf("counts = %d free, %d pages", dm.FreePageCount(), dm.PageCount())
			}
			if id := mustAlloc(t, dm); id != 7 {
				t.Fatalf("extension Allocate = %d", id)
			}
		})
	}
}

func TestFreeListSurvivesReopen(t *testing.T) {
	fsys := newMem(t)
	dm := mustCreate(t, fsys, dbName)
	for range 6 {
		mustAlloc(t, dm)
	}
	for _, id := range []uint64{3, 5, 2} {
		if err := dm.Free(bg, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := dm.Close(); err != nil {
		t.Fatal(err)
	}
	dm = mustOpenDM(t, fsys, dbName)
	if got := freeListIDs(t, dm); fmt.Sprint(got) != "[2 5 3]" {
		t.Fatalf("free list = %v", got)
	}
	if id := mustAlloc(t, dm); id != 2 {
		t.Fatalf("Allocate = %d, want 2", id)
	}
}

func TestInvalidArguments(t *testing.T) {
	fsys := newMem(t)
	dm := mustCreate(t, fsys, dbName)
	id := mustAlloc(t, dm)
	rng := rand.New(rand.NewPCG(1, 1))
	good := dataPage(t, id, rng)
	buf := make([]byte, PageSize)

	for _, bad := range []uint64{0, 1, 3, 1 << 40} {
		if err := dm.ReadPage(bg, bad, buf); !errors.Is(err, ErrInvalidPageID) {
			t.Errorf("ReadPage(%d) err = %v", bad, err)
		}
		if err := dm.WritePage(bg, bad, good); !errors.Is(err, ErrInvalidPageID) {
			t.Errorf("WritePage(%d) err = %v", bad, err)
		}
		if err := dm.Free(bg, bad); !errors.Is(err, ErrInvalidPageID) {
			t.Errorf("Free(%d) err = %v", bad, err)
		}
	}
	if err := dm.ReadPage(bg, id, make([]byte, 10)); !errors.Is(err, ErrBadSize) {
		t.Errorf("short read buffer err = %v", err)
	}
	if err := dm.WritePage(bg, id, make([]byte, 10)); !errors.Is(err, ErrBadSize) {
		t.Errorf("short write buffer err = %v", err)
	}
	if err := dm.WritePage(bg, id, make([]byte, PageSize)); !errors.Is(err, ErrBadPageType) {
		t.Errorf("zero page write err = %v", err)
	}
	wrongID := dataPage(t, id+1, rng)
	if err := dm.WritePage(bg, id, wrongID); !errors.Is(err, ErrPageIDMismatch) {
		t.Errorf("wrong header id err = %v", err)
	}
	for _, typ := range []PageType{PageTypeFileHeader, PageTypeFree} {
		p := dataPage(t, id, rng)
		if err := InitPage(p, Header{ID: id, Type: typ}); err != nil {
			t.Fatal(err)
		}
		if err := dm.WritePage(bg, id, p); !errors.Is(err, ErrBadPageType) {
			t.Errorf("write of type %d err = %v", typ, err)
		}
	}
}

func TestDoubleFree(t *testing.T) {
	dm := mustCreate(t, newMem(t), dbName)
	id := mustAlloc(t, dm)
	if err := dm.Free(bg, id); err != nil {
		t.Fatal(err)
	}
	if err := dm.Free(bg, id); !errors.Is(err, ErrDoubleFree) {
		t.Fatalf("err = %v, want ErrDoubleFree", err)
	}
	if dm.FreePageCount() != 1 {
		t.Fatalf("FreePageCount = %d", dm.FreePageCount())
	}
}

func TestFreeRefusesDamagedPage(t *testing.T) {
	fsys := newMem(t)
	dm := mustCreate(t, fsys, dbName)
	id := mustAlloc(t, dm)
	if err := dm.WritePage(bg, id, dataPage(t, id, rand.New(rand.NewPCG(1, 1)))); err != nil {
		t.Fatal(err)
	}
	damaged := readRawFile(t, fsys, id)
	damaged[100] ^= 0xFF
	writeRawFile(t, fsys, id, damaged)
	if err := dm.Free(bg, id); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
	buf := make([]byte, PageSize)
	if err := dm.ReadPage(bg, id, buf); !errors.Is(err, ErrChecksum) {
		t.Fatalf("ReadPage err = %v", err)
	}
}

func TestClosedAndCancelled(t *testing.T) {
	dm := mustCreate(t, newMem(t), dbName)
	id := mustAlloc(t, dm)
	buf := make([]byte, PageSize)

	cancelled, cancel := context.WithCancel(bg)
	cancel()
	if _, err := dm.Allocate(cancelled); !errors.Is(err, context.Canceled) {
		t.Errorf("Allocate cancelled err = %v", err)
	}
	if err := dm.ReadPage(cancelled, id, buf); !errors.Is(err, context.Canceled) {
		t.Errorf("ReadPage cancelled err = %v", err)
	}
	if err := dm.Sync(cancelled); !errors.Is(err, context.Canceled) {
		t.Errorf("Sync cancelled err = %v", err)
	}
	if dm.PageCount() != 3 {
		t.Fatal("cancelled Allocate changed state")
	}

	if err := dm.Close(); err != nil {
		t.Fatal(err)
	}
	if err := dm.Close(); !errors.Is(err, ErrDiskClosed) {
		t.Errorf("second Close err = %v", err)
	}
	if _, err := dm.Allocate(bg); !errors.Is(err, ErrDiskClosed) {
		t.Errorf("Allocate after Close err = %v", err)
	}
	if err := dm.ReadPage(bg, id, buf); !errors.Is(err, ErrDiskClosed) {
		t.Errorf("ReadPage after Close err = %v", err)
	}
	if err := dm.Free(bg, id); !errors.Is(err, ErrDiskClosed) {
		t.Errorf("Free after Close err = %v", err)
	}
}

func TestFull(t *testing.T) {
	dm := mustCreate(t, newMem(t), dbName)
	dm.maxPages = 4 // header slots + 2 data pages
	a, b := mustAlloc(t, dm), mustAlloc(t, dm)
	if _, err := dm.Allocate(bg); !errors.Is(err, ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
	if err := dm.Free(bg, b); err != nil {
		t.Fatal(err)
	}
	if got := mustAlloc(t, dm); got != b {
		t.Fatalf("reuse after full = %d, want %d", got, b)
	}
	_ = a
}

// --- corrupt and foreign files ---------------------------------------------

func headerPage(t *testing.T, slot uint64, h fileHeader) []byte {
	t.Helper()
	buf := make([]byte, PageSize)
	if err := encodeHeader(buf, slot, h); err != nil {
		t.Fatal(err)
	}
	return buf
}

// resealWith modifies a header page's payload and recomputes the checksum, to
// build files that are well-formed except for one field.
func resealWith(t *testing.T, page []byte, mutate func(payload []byte)) []byte {
	t.Helper()
	out := bytes.Clone(page)
	mutate(out[HeaderSize:])
	if err := Seal(out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestOpenRejectsBadFiles(t *testing.T) {
	good0 := headerPage(t, 0, fileHeader{pageCount: 2})
	tests := []struct {
		name  string
		pages [][]byte
		want  error
	}{
		{"empty file", nil, ErrCorrupt},
		{"blank slots", [][]byte{make([]byte, PageSize), make([]byte, PageSize)}, ErrCorrupt},
		{"garbage", [][]byte{bytes.Repeat([]byte{0xAB}, PageSize), bytes.Repeat([]byte{0xCD}, PageSize)}, ErrCorrupt},
		{"wrong magic", [][]byte{resealWith(t, good0, func(p []byte) { p[0] = 'X' })}, ErrBadMagic},
		{"future version", [][]byte{resealWith(t, good0, func(p []byte) { binary.LittleEndian.PutUint32(p[hdrOffVersion:], FormatVersion+1) })}, ErrUnsupportedVersion},
		{"version 1, before leaf flags", [][]byte{resealWith(t, good0, func(p []byte) { binary.LittleEndian.PutUint32(p[hdrOffVersion:], 1) })}, ErrUnsupportedVersion},
		{"version 2, before undo pages", [][]byte{resealWith(t, good0, func(p []byte) { binary.LittleEndian.PutUint32(p[hdrOffVersion:], 2) })}, ErrUnsupportedVersion},
		{"page size 4096", [][]byte{resealWith(t, good0, func(p []byte) { binary.LittleEndian.PutUint32(p[hdrOffPageSize:], 4096) })}, ErrPageSizeMismatch},
		{"page count 1", [][]byte{resealWith(t, good0, func(p []byte) { binary.LittleEndian.PutUint64(p[hdrOffPageCount:], 1) })}, ErrCorrupt},
		{"free head out of range", [][]byte{resealWith(t, good0, func(p []byte) {
			binary.LittleEndian.PutUint64(p[hdrOffFreeHead:], 99)
			binary.LittleEndian.PutUint64(p[hdrOffFreeCount:], 1)
		})}, ErrCorrupt},
		{"free head without count", [][]byte{resealWith(t, good0, func(p []byte) { binary.LittleEndian.PutUint64(p[hdrOffFreeHead:], 2) })}, ErrCorrupt},
		{"free count without head", [][]byte{resealWith(t, good0, func(p []byte) { binary.LittleEndian.PutUint64(p[hdrOffFreeCount:], 1) })}, ErrCorrupt},
		{"free count too large", [][]byte{resealWith(t, good0, func(p []byte) {
			binary.LittleEndian.PutUint64(p[hdrOffPageCount:], 4)
			binary.LittleEndian.PutUint64(p[hdrOffFreeHead:], 2)
			binary.LittleEndian.PutUint64(p[hdrOffFreeCount:], 3)
		})}, ErrCorrupt},
		{"generation in wrong slot", [][]byte{resealWith(t, good0, func(p []byte) { binary.LittleEndian.PutUint64(p[hdrOffGeneration:], 1) })}, ErrCorrupt},
		{"header page claims other slot", [][]byte{headerPage(t, 1, fileHeader{generation: 1, pageCount: 2})}, ErrCorrupt},
		{"data page in slot 0", [][]byte{sealedPage(t, 0)}, ErrCorrupt},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fsys := newMem(t)
			f, err := fsys.OpenFile(dbName, vfs.ORead|vfs.OWrite|vfs.OCreate)
			if err != nil {
				t.Fatal(err)
			}
			for i, p := range tc.pages {
				if err := writeRaw(f, uint64(i), p); err != nil {
					t.Fatal(err)
				}
			}
			_ = f.Close()
			if _, err := Open(fsys, dbName); !errors.Is(err, tc.want) {
				t.Fatalf("Open err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestOpenFallsBackToOtherSlot(t *testing.T) {
	// Build a file whose current header (gen 1, slot 1) is damaged: Open must
	// use gen 0 in slot 0. Then the reverse.
	fsys := newMem(t)
	dm := mustCreate(t, fsys, dbName)
	mustAlloc(t, dm) // gen 1 -> slot 1, pageCount 3
	_ = dm.Close()

	slot1 := readRawFile(t, fsys, 1)
	damaged := bytes.Clone(slot1)
	damaged[HeaderSize+hdrOffPageCount] ^= 0x01
	writeRawFile(t, fsys, 1, damaged)
	dm = mustOpenDM(t, fsys, dbName)
	if dm.PageCount() != 2 {
		t.Fatalf("fell back to PageCount %d, want 2", dm.PageCount())
	}
	_ = dm.Close()

	writeRawFile(t, fsys, 1, slot1) // restore
	dm = mustOpenDM(t, fsys, dbName)
	if dm.PageCount() != 3 {
		t.Fatalf("higher generation not chosen: PageCount %d", dm.PageCount())
	}
	_ = dm.Close()

	slot0 := readRawFile(t, fsys, 0)
	slot0[5] ^= 0xFF
	writeRawFile(t, fsys, 0, slot0) // damage the older slot only
	dm = mustOpenDM(t, fsys, dbName)
	if dm.PageCount() != 3 {
		t.Fatalf("PageCount %d with damaged old slot", dm.PageCount())
	}
	_ = dm.Close()

	slot1[7] ^= 0xFF
	writeRawFile(t, fsys, 1, slot1) // now both damaged
	if _, err := Open(fsys, dbName); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("both damaged: err = %v", err)
	}
}

func TestEveryHeaderByteFlipIsSurvivable(t *testing.T) {
	// Flipping any single byte of either slot must never make Open panic or
	// return a wrong header: with the other slot intact it must succeed with
	// that slot's data, or (only if the flip hits the winning slot's valid
	// bytes in a way that stays valid -- impossible with a CRC) fail cleanly.
	fsys := newMem(t)
	dm := mustCreate(t, fsys, dbName)
	mustAlloc(t, dm)
	_ = dm.Close()
	slots := [2][]byte{readRawFile(t, fsys, 0), readRawFile(t, fsys, 1)}
	wantCount := [2]uint64{2, 3} // slot 0 holds gen 0, slot 1 holds gen 1
	for slot := range 2 {
		for i := 0; i < PageSize; i += 7 { // stride keeps the test fast
			dmg := bytes.Clone(slots[slot])
			dmg[i] ^= 0x80
			writeRawFile(t, fsys, uint64(slot), dmg)
			d, err := Open(fsys, dbName)
			if err != nil {
				t.Fatalf("slot %d byte %d: %v", slot, i, err)
			}
			if want := wantCount[1-slot]; d.PageCount() != want {
				t.Fatalf("slot %d byte %d: PageCount %d, want %d", slot, i, d.PageCount(), want)
			}
			_ = d.Close()
		}
		writeRawFile(t, fsys, uint64(slot), slots[slot])
	}
}

func TestCorruptFreeListDetectedOnAllocate(t *testing.T) {
	fsys := newMem(t)
	dm := mustCreate(t, fsys, dbName)
	mustAlloc(t, dm)
	if err := dm.Free(bg, 2); err != nil {
		t.Fatal(err)
	}
	_ = dm.Close()
	page := readRawFile(t, fsys, 2)
	page[HeaderSize] = 0x77 // break the next pointer; checksum now wrong
	writeRawFile(t, fsys, 2, page)
	dm = mustOpenDM(t, fsys, dbName)
	if _, err := dm.Allocate(bg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	// Valid checksum but a next pointer outside the file.
	fixed := resealWith(t, page, func(p []byte) { binary.LittleEndian.PutUint64(p[freeOffNext:], 999) })
	writeRawFile(t, fsys, 2, fixed)
	if _, err := dm.Allocate(bg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("out of range next: err = %v", err)
	}
	// A page of the wrong type at the head of the list.
	heap := dataPage(t, 2, rand.New(rand.NewPCG(1, 1)))
	_ = Seal(heap)
	writeRawFile(t, fsys, 2, heap)
	if _, err := dm.Allocate(bg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("wrong type: err = %v", err)
	}
}

// --- Create atomicity and injected I/O errors -------------------------------

func TestCreateSurvivesFaultsAtEveryStep(t *testing.T) {
	ops := []vfs.Op{vfs.OpOpenFile, vfs.OpWriteAt, vfs.OpSync, vfs.OpRename, vfs.OpSyncDir, vfs.OpRemove}
	for _, op := range ops {
		for after := range 4 {
			t.Run(fmt.Sprintf("%v/after%d", op, after), func(t *testing.T) {
				for _, crash := range []bool{false, true} {
					m := newMem(t)
					m.InjectError(vfs.Fault{Op: op, After: after})
					_, createErr := Create(m, dbName)
					m.ClearFaults()
					if crash {
						m.Crash(vfs.CrashOptions{TearLast: true})
					}
					dm, err := Open(m, dbName)
					switch {
					case err == nil:
						if dm.PageCount() != 2 {
							t.Fatalf("PageCount = %d", dm.PageCount())
						}
						_ = dm.Close()
					case errors.Is(err, vfs.ErrNotExist):
						// absent: a retry must work
					default:
						t.Fatalf("crash=%v: half-made file: Open err = %v (Create err = %v)", crash, err, createErr)
					}
					if err != nil {
						dm2, err := Create(m, dbName)
						if err != nil {
							t.Fatalf("retry Create: %v", err)
						}
						_ = dm2.Close()
					}
				}
			})
		}
	}
}

func TestIOErrorsPoisonManager(t *testing.T) {
	for _, op := range []vfs.Op{vfs.OpWriteAt, vfs.OpSync} {
		for after := range 4 {
			for _, doFree := range []bool{false, true} {
				name := fmt.Sprintf("%v/after%d/free=%v", op, after, doFree)
				t.Run(name, func(t *testing.T) {
					m := newMem(t)
					dm := mustCreate(t, m, dbName)
					for range 4 {
						mustAlloc(t, dm)
					}
					if err := dm.Free(bg, 3); err != nil {
						t.Fatal(err)
					}
					m.InjectError(vfs.Fault{Op: op, After: after})
					var err error
					if doFree {
						err = dm.Free(bg, 4)
					} else {
						_, err = dm.Allocate(bg)
					}
					m.ClearFaults()
					if err == nil {
						return // fault landed after the operation finished
					}
					if !errors.Is(err, vfs.ErrInjected) {
						t.Fatalf("err = %v", err)
					}
					if _, err := dm.Allocate(bg); !errors.Is(err, ErrFailed) {
						t.Fatalf("after failure err = %v, want ErrFailed", err)
					}
					if err := dm.Sync(bg); !errors.Is(err, ErrFailed) {
						t.Fatalf("Sync after failure err = %v", err)
					}
					_ = dm.Close()
					m.Crash(vfs.CrashOptions{TearLast: true})
					re := mustOpenDM(t, m, dbName)
					freeListIDs(t, re)
				})
			}
		}
	}
}

func TestSyncFailurePoisons(t *testing.T) {
	m := newMem(t)
	dm := mustCreate(t, m, dbName)
	m.InjectError(vfs.Fault{Op: vfs.OpSync})
	if err := dm.Sync(bg); !errors.Is(err, vfs.ErrInjected) {
		t.Fatalf("err = %v", err)
	}
	if err := dm.Sync(bg); !errors.Is(err, ErrFailed) {
		t.Fatalf("retry err = %v, want ErrFailed", err)
	}
	if err := dm.Close(); err != nil {
		t.Fatalf("Close of failed manager: %v", err)
	}
}

func TestWritePageIOErrorDoesNotPoison(t *testing.T) {
	m := newMem(t)
	dm := mustCreate(t, m, dbName)
	id := mustAlloc(t, dm)
	page := dataPage(t, id, rand.New(rand.NewPCG(1, 1)))
	m.InjectError(vfs.Fault{Op: vfs.OpWriteAt})
	if err := dm.WritePage(bg, id, page); !errors.Is(err, vfs.ErrInjected) {
		t.Fatalf("err = %v", err)
	}
	if err := dm.WritePage(bg, id, page); err != nil {
		t.Fatalf("retry: %v", err)
	}
}

// --- concurrency -------------------------------------------------------------

func TestConcurrentUse(t *testing.T) {
	m := newMem(t)
	dm := mustCreate(t, m, dbName)
	const workers, rounds = 8, 40
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 99))
			var mine []uint64
			content := map[uint64][]byte{}
			for range rounds {
				switch rng.IntN(4) {
				case 0, 1:
					id, err := dm.Allocate(bg)
					if err != nil {
						t.Error(err)
						return
					}
					p := dataPage(t, id, rng)
					if err := dm.WritePage(bg, id, p); err != nil {
						t.Error(err)
						return
					}
					mine = append(mine, id)
					content[id] = bytes.Clone(p)
				case 2:
					if len(mine) == 0 {
						continue
					}
					id := mine[rng.IntN(len(mine))]
					got := make([]byte, PageSize)
					if err := dm.ReadPage(bg, id, got); err != nil || !bytes.Equal(got, content[id]) {
						t.Errorf("page %d read back wrong: %v", id, err)
						return
					}
				case 3:
					if len(mine) == 0 {
						continue
					}
					i := rng.IntN(len(mine))
					id := mine[i]
					mine = append(mine[:i], mine[i+1:]...)
					delete(content, id)
					if err := dm.Free(bg, id); err != nil {
						t.Error(err)
						return
					}
				}
				if rng.IntN(10) == 0 {
					if err := dm.Sync(bg); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	freeListIDs(t, dm)
}

// --- crash recovery ---------------------------------------------------------

// crashModel records what a caller was promised.
type crashModel struct {
	written   map[uint64][]byte // content written, durable or not
	durable   map[uint64][]byte // content synced and untouched since
	live      map[uint64]bool   // allocated, not (even partially) freed
	ambiguous map[uint64]bool   // a Free on it failed: state unknown
}

func newCrashModel() *crashModel {
	return &crashModel{map[uint64][]byte{}, map[uint64][]byte{}, map[uint64]bool{}, map[uint64]bool{}}
}

func (c *crashModel) livePages() []uint64 {
	var ids []uint64
	for id := range c.live {
		ids = append(ids, id)
	}
	// Deterministic order so a seed reproduces the run.
	for i := range ids {
		for j := i + 1; j < len(ids); j++ {
			if ids[j] < ids[i] {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	return ids
}

func runCrashScenario(t *testing.T, seed uint64) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, seed+1))
	m := vfs.NewMemFS(seed)
	if err := m.MkdirAll("/db"); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("/"); err != nil {
		t.Fatal(err)
	}
	dm, err := Create(m, dbName)
	if err != nil {
		t.Fatalf("seed %d: Create: %v", seed, err)
	}
	model := newCrashModel()

	if rng.IntN(10) < 7 { // most runs get an I/O error injected mid-way
		op := vfs.OpWriteAt
		if rng.IntN(2) == 0 {
			op = vfs.OpSync
		}
		m.InjectError(vfs.Fault{Op: op, After: rng.IntN(25)})
	}

ops:
	for range 5 + rng.IntN(60) {
		live := model.livePages()
		switch rng.IntN(10) {
		case 0, 1, 2: // allocate
			id, err := dm.Allocate(bg)
			if err != nil {
				break ops
			}
			model.live[id] = true
			delete(model.written, id)
			delete(model.durable, id)
			delete(model.ambiguous, id)
		case 3, 4, 5, 6: // write
			if len(live) == 0 {
				continue
			}
			id := live[rng.IntN(len(live))]
			p := dataPage(t, id, rng)
			delete(model.durable, id) // touched: no longer promised until the next Sync
			if err := dm.WritePage(bg, id, p); err != nil {
				delete(model.written, id)
				continue
			}
			model.written[id] = bytes.Clone(p)
		case 7, 8: // free
			if len(live) == 0 {
				continue
			}
			id := live[rng.IntN(len(live))]
			delete(model.live, id)
			delete(model.written, id)
			delete(model.durable, id)
			if err := dm.Free(bg, id); err != nil {
				model.ambiguous[id] = true
				break ops
			}
		case 9: // sync
			if err := dm.Sync(bg); err != nil {
				break ops
			}
			for id, p := range model.written {
				model.durable[id] = p
			}
		}
	}

	m.ClearFaults()
	m.Crash(vfs.CrashOptions{TearLast: rng.IntN(2) == 0})

	re, err := Open(m, dbName)
	if err != nil {
		t.Fatalf("seed %d: Open after crash: %v", seed, err)
	}
	for id, want := range model.durable {
		got := make([]byte, PageSize)
		if err := re.ReadPage(bg, id, got); err != nil {
			t.Fatalf("seed %d: synced page %d unreadable: %v", seed, id, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("seed %d: synced page %d changed", seed, id)
		}
	}
	inList := map[uint64]bool{}
	for _, id := range freeListIDs(t, re) {
		inList[id] = true
		if model.live[id] && !model.ambiguous[id] {
			t.Fatalf("seed %d: live page %d is on the free list", seed, id)
		}
	}
	// Draining the free list and extending must never return a live page.
	returned := map[uint64]bool{}
	for range len(inList) + 3 {
		id, err := re.Allocate(bg)
		if err != nil {
			t.Fatalf("seed %d: Allocate after recovery: %v", seed, err)
		}
		if returned[id] || (model.live[id] && !model.ambiguous[id]) {
			t.Fatalf("seed %d: Allocate returned in-use page %d", seed, id)
		}
		returned[id] = true
	}
	if err := re.Close(); err != nil {
		t.Fatalf("seed %d: Close: %v", seed, err)
	}
}

func TestCrashRecovery(t *testing.T) {
	base := testSeed(t)
	runs := 1500
	if testing.Short() {
		runs = 150
	}
	for i := range runs {
		runCrashScenario(t, base+uint64(i))
	}
}

// --- fuzzing ---------------------------------------------------------------

func FuzzOpen(f *testing.F) {
	m := vfs.NewMemFS(1)
	_ = m.MkdirAll("/db")
	_ = m.SyncDir("/")
	dm, err := Create(m, dbName)
	if err != nil {
		f.Fatal(err)
	}
	fresh := append(readRawFile(f, m, 0), readRawFile(f, m, 1)...)
	for range 3 {
		if _, err := dm.Allocate(bg); err != nil {
			f.Fatal(err)
		}
	}
	if err := dm.Free(bg, 3); err != nil {
		f.Fatal(err)
	}
	used := append(readRawFile(f, m, 0), readRawFile(f, m, 1)...)
	f.Add(fresh)
	f.Add(used)
	f.Add([]byte{})
	f.Add(make([]byte, 2*PageSize))
	f.Add(fresh[:PageSize])

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 2*PageSize {
			data = data[:2*PageSize]
		}
		fsys := newMem(t)
		file, err := fsys.OpenFile(dbName, vfs.ORead|vfs.OWrite|vfs.OCreate)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteAt(data, 0); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
		d, err := Open(fsys, dbName)
		if err != nil {
			return
		}
		if !d.hdr.valid() {
			t.Fatalf("Open accepted invalid header %+v", d.hdr)
		}
		// Operations on an arbitrary accepted file must return, not panic.
		_, _ = d.Allocate(bg)
		_ = d.Close()
	})
}
