package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
)

func testSeed(t *testing.T) uint64 {
	t.Helper()
	seed := uint64(1)
	if s := os.Getenv("NOVACDB_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("bad NOVACDB_SEED %q: %v", s, err)
		}
		seed = v
	}
	t.Logf("seed = %d (override with NOVACDB_SEED)", seed)
	return seed
}

// sealedPage builds a valid sealed heap page with a patterned payload.
func sealedPage(t testing.TB, id uint64) []byte {
	t.Helper()
	buf := make([]byte, PageSize)
	if err := InitPage(buf, Header{ID: id, LSN: 42, Type: PageTypeHeap, Flags: 3}); err != nil {
		t.Fatal(err)
	}
	for i := HeaderSize; i < PageSize; i++ {
		buf[i] = byte(i % 251)
	}
	if err := Seal(buf); err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestConstants(t *testing.T) {
	if PageSize != 8192 || HeaderSize != 24 || PayloadSize != 8168 {
		t.Fatalf("geometry changed: %d %d %d", PageSize, HeaderSize, PayloadSize)
	}
}

func TestHeaderRoundTrip(t *testing.T) {
	types := []PageType{PageTypeFileHeader, PageTypeFree, PageTypeHeap, PageTypeBTreeInternal, PageTypeBTreeLeaf}
	values := []uint64{0, 1, math.MaxUint64}
	for _, typ := range types {
		for _, v := range values {
			for _, flags := range []uint16{0, 0xFFFF} {
				h := Header{ID: v, LSN: v, Type: typ, Flags: flags}
				buf := make([]byte, PageSize)
				if err := InitPage(buf, h); err != nil {
					t.Fatalf("%+v: InitPage: %v", h, err)
				}
				got, err := DecodeHeader(buf)
				if err != nil || got != h {
					t.Fatalf("round trip %+v -> %+v, %v", h, got, err)
				}
			}
		}
	}
}

func TestHeaderByteLayout(t *testing.T) {
	buf := make([]byte, PageSize)
	h := Header{ID: 0x0102030405060708, LSN: 0x1112131415161718, Type: PageTypeBTreeLeaf, Flags: 0xA1B2}
	if err := InitPage(buf, h); err != nil {
		t.Fatal(err)
	}
	want := []byte{
		0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // PageID, little-endian
		0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // LSN
		0, 0, 0, 0, // CRC not yet set
		0x05, 0x00, // PageType
		0xB2, 0xA1, // Flags
	}
	if !bytes.Equal(buf[:HeaderSize], want) {
		t.Fatalf("header bytes = % x\nwant           % x", buf[:HeaderSize], want)
	}
	if !bytes.Equal(buf[HeaderSize:], make([]byte, PayloadSize)) {
		t.Fatal("payload not zeroed")
	}
	if err := Seal(buf); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(buf[16:]) == 0 {
		t.Fatal("CRC not stored at offset 16")
	}
	if !bytes.Equal(buf[:16], want[:16]) || !bytes.Equal(buf[20:HeaderSize], want[20:]) {
		t.Fatal("Seal modified bytes outside the CRC field")
	}
}

func TestInitPageZeroesOldContents(t *testing.T) {
	buf := bytes.Repeat([]byte{0xFF}, PageSize)
	if err := InitPage(buf, Header{ID: 1, Type: PageTypeHeap}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[HeaderSize:], make([]byte, PayloadSize)) {
		t.Fatal("old payload survived InitPage")
	}
}

func TestSealVerify(t *testing.T) {
	buf := sealedPage(t, 7)
	if err := Verify(buf, 7); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	before := bytes.Clone(buf)
	if err := Seal(buf); err != nil || !bytes.Equal(buf, before) {
		t.Fatalf("Seal not idempotent: %v", err)
	}
}

// TestChecksumIsCRC32COfZeroedPage pins the checksum scope independently of
// pageCRC, and a constant guards against accidental changes to the format.
func TestChecksumIsCRC32COfZeroedPage(t *testing.T) {
	buf := sealedPage(t, 7)
	stored := binary.LittleEndian.Uint32(buf[16:])
	tmp := bytes.Clone(buf)
	clear(tmp[16:20])
	if want := crc32.Checksum(tmp, crc32.MakeTable(crc32.Castagnoli)); stored != want {
		t.Fatalf("stored %#x, independent computation %#x", stored, want)
	}
	const golden = 0x62dc4a91 // verified above against an independent computation
	if stored != golden {
		t.Fatalf("golden CRC changed: got %#08x", stored)
	}
}

func TestVerifyDetectsEverySingleByteCorruption(t *testing.T) {
	orig := sealedPage(t, 9)
	for i := range orig {
		buf := bytes.Clone(orig)
		buf[i] ^= 0x01
		if err := Verify(buf, 9); err == nil {
			t.Fatalf("flipping bit 0 of byte %d went undetected", i)
		}
	}
}

func TestVerifyDetectsHeaderBitFlips(t *testing.T) {
	orig := sealedPage(t, 9)
	for i := 0; i < HeaderSize*8; i++ {
		buf := bytes.Clone(orig)
		buf[i/8] ^= 1 << (i % 8)
		if err := Verify(buf, 9); err == nil {
			t.Fatalf("header bit %d flip went undetected", i)
		}
	}
}

func TestVerifyDetectsRandomCorruption(t *testing.T) {
	rng := rand.New(rand.NewPCG(testSeed(t), 1))
	orig := sealedPage(t, 3)
	for range 2000 {
		buf := bytes.Clone(orig)
		for range 1 + rng.IntN(8) {
			buf[rng.IntN(PageSize)] ^= byte(1 + rng.IntN(255))
		}
		if bytes.Equal(buf, orig) {
			continue // XORs cancelled out
		}
		if err := Verify(buf, 3); err == nil {
			t.Fatal("random corruption went undetected")
		}
	}
}

func TestVerifyErrors(t *testing.T) {
	zeroID := make([]byte, PageSize)
	badType := make([]byte, PageSize)
	if err := InitPage(badType, Header{ID: 5, Type: PageTypeHeap}); err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(badType[20:], 99)
	if err := Seal(badType); err != nil {
		t.Fatal(err)
	}
	typeZero := bytes.Clone(badType)
	binary.LittleEndian.PutUint16(typeZero[20:], 0)
	if err := Seal(typeZero); err != nil {
		t.Fatal(err)
	}
	unsealed := make([]byte, PageSize)
	if err := InitPage(unsealed, Header{ID: 5, Type: PageTypeHeap}); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		buf  []byte
		id   uint64
		want error
	}{
		{"nil buffer", nil, 0, ErrBadSize},
		{"empty buffer", []byte{}, 0, ErrBadSize},
		{"one byte short", make([]byte, PageSize-1), 0, ErrBadSize},
		{"one byte long", make([]byte, PageSize+1), 0, ErrBadSize},
		{"two pages", make([]byte, 2*PageSize), 0, ErrBadSize},
		{"all zeros", zeroID, 0, ErrZeroPage},
		{"never sealed", unsealed, 5, ErrChecksum},
		{"unknown type with valid crc", badType, 5, ErrBadPageType},
		{"type zero with valid crc", typeZero, 5, ErrBadPageType},
		{"wrong page id", sealedPage(t, 8), 9, ErrPageIDMismatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := Verify(tc.buf, tc.id); !errors.Is(err, tc.want) {
				t.Fatalf("Verify err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestWrongSizeBuffersRejectedEverywhere(t *testing.T) {
	for _, n := range []int{0, 1, PageSize - 1, PageSize + 1, 2 * PageSize} {
		buf := make([]byte, n)
		if err := InitPage(buf, Header{Type: PageTypeHeap}); !errors.Is(err, ErrBadSize) {
			t.Errorf("InitPage(%d) err = %v", n, err)
		}
		if _, err := DecodeHeader(buf); !errors.Is(err, ErrBadSize) {
			t.Errorf("DecodeHeader(%d) err = %v", n, err)
		}
		if err := Seal(buf); !errors.Is(err, ErrBadSize) {
			t.Errorf("Seal(%d) err = %v", n, err)
		}
		if _, err := Payload(buf); !errors.Is(err, ErrBadSize) {
			t.Errorf("Payload(%d) err = %v", n, err)
		}
	}
}

func TestInitPageRejectsInvalidType(t *testing.T) {
	for _, typ := range []PageType{PageTypeInvalid, 7, 0xFFFF} {
		if err := InitPage(make([]byte, PageSize), Header{Type: typ}); !errors.Is(err, ErrBadPageType) {
			t.Errorf("type %d err = %v", typ, err)
		}
	}
}

func TestDecodeHeaderRejectsZeroPage(t *testing.T) {
	if _, err := DecodeHeader(make([]byte, PageSize)); !errors.Is(err, ErrBadPageType) {
		t.Fatalf("err = %v", err)
	}
}

func TestPayloadAliasesPage(t *testing.T) {
	buf := sealedPage(t, 1)
	p, err := Payload(buf)
	if err != nil || len(p) != PayloadSize {
		t.Fatalf("Payload len = %d, %v", len(p), err)
	}
	p[0] = 0xEE
	if buf[HeaderSize] != 0xEE {
		t.Fatal("Payload does not alias the page")
	}
	if err := Verify(buf, 1); !errors.Is(err, ErrChecksum) {
		t.Fatalf("modified payload must fail until re-sealed, got %v", err)
	}
	if err := Seal(buf); err != nil {
		t.Fatal(err)
	}
	if err := Verify(buf, 1); err != nil {
		t.Fatal(err)
	}
}

func TestPageTypeValid(t *testing.T) {
	for typ, want := range map[PageType]bool{0: false, 1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: false, 0xFFFF: false} {
		if got := typ.Valid(); got != want {
			t.Errorf("PageType(%d).Valid() = %v", typ, got)
		}
	}
}
