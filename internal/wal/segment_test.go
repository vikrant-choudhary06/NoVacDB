package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestSegmentHeaderRoundTrip(t *testing.T) {
	for _, start := range []LSN{0, 1, 4096, 1<<64 - 1} {
		enc := AppendSegmentHeader(nil, start)
		if len(enc) != SegmentHeaderSize {
			t.Fatalf("header is %d bytes", len(enc))
		}
		got, err := DecodeSegmentHeader(enc)
		if err != nil || got != start {
			t.Fatalf("start %d: got %d, %v", start, got, err)
		}
	}
}

func TestSegmentHeaderGoldenBytes(t *testing.T) {
	enc := AppendSegmentHeader(nil, 0x1122334455667788)
	want := make([]byte, SegmentHeaderSize)
	copy(want[4:], "NOVAWAL\x00")
	binary.LittleEndian.PutUint32(want[12:], 3) // format version 3 (Step 6.2)
	binary.LittleEndian.PutUint64(want[16:], 0x1122334455667788)
	crc := crc32.Checksum(want[4:], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(want, crc)
	if !bytes.Equal(enc, want) {
		t.Fatalf("header = % x\nwant     % x", enc, want)
	}
}

func TestSegmentHeaderEveryByteFlipDetected(t *testing.T) {
	enc := AppendSegmentHeader(nil, 4096)
	for i := range enc {
		b := bytes.Clone(enc)
		b[i] ^= 0x01
		if _, err := DecodeSegmentHeader(b); err == nil {
			t.Fatalf("flip of byte %d undetected", i)
		}
	}
}

func TestSegmentHeaderErrors(t *testing.T) {
	reseal := func(f func(b []byte)) []byte {
		b := AppendSegmentHeader(nil, 64)
		f(b)
		binary.LittleEndian.PutUint32(b, crc32.Checksum(b[4:], crc32.MakeTable(crc32.Castagnoli)))
		return b
	}
	tests := []struct {
		name string
		buf  []byte
		want error
	}{
		{"nil", nil, ErrCorrupt},
		{"short", AppendSegmentHeader(nil, 0)[:31], ErrCorrupt},
		{"zeros", make([]byte, SegmentHeaderSize), ErrCorrupt},
		{"wrong magic", reseal(func(b []byte) { b[4] = 'X' }), ErrBadMagic},
		{"version 4", reseal(func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 4) }), ErrUnsupportedVersion},
		{"version 2", reseal(func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 2) }), ErrUnsupportedVersion},
		{"version 1", reseal(func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 1) }), ErrUnsupportedVersion},
		{"version 0", reseal(func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 0) }), ErrUnsupportedVersion},
		{"reserved bytes set", reseal(func(b []byte) { b[24] = 1 }), ErrCorrupt},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeSegmentHeader(tc.buf); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
	// Extra bytes after the header are fine (it is followed by records).
	if _, err := DecodeSegmentHeader(append(AppendSegmentHeader(nil, 5), 1, 2, 3)); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentNames(t *testing.T) {
	for _, start := range []LSN{0, 32, 0xABCDEF, 1<<64 - 1} {
		name := SegmentName(start)
		got, ok := ParseSegmentName(name)
		if !ok || got != start {
			t.Fatalf("%q -> %d, %v", name, got, ok)
		}
	}
	if SegmentName(255) != "wal-00000000000000ff.seg" {
		t.Fatalf("name format changed: %s", SegmentName(255))
	}
	for _, bad := range []string{
		"", "wal-.seg", "wal-ff.seg", "wal-00000000000000FF.seg", "wal-0000000000000000.segx",
		"xwal-0000000000000000.seg", "wal-+000000000000000.seg", "wal-00000000000000000.seg",
		"wal-0000000000000000.seg.tmp", "wal-000000000000000g.seg", "wal--000000000000000.seg",
	} {
		if _, ok := ParseSegmentName(bad); ok {
			t.Errorf("ParseSegmentName(%q) accepted", bad)
		}
	}
}
