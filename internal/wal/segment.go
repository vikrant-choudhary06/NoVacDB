package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
)

// SegmentHeaderSize is the size of the header at the start of every segment.
const SegmentHeaderSize = 32

// FormatVersion is the log format version written by this code.
const FormatVersion = 3

// segMagic identifies a NoVacDB WAL segment.
var segMagic = [8]byte{'N', 'O', 'V', 'A', 'W', 'A', 'L', 0}

// Segment header layout (32 bytes, little-endian):
//
//	offset  size  field
//	0       4     CRC-32C of bytes 4 .. 32
//	4       8     Magic "NOVAWAL\0"
//	12      4     FormatVersion
//	16      8     StartLSN (equals the number in the file name)
//	24      8     reserved (zero)
const (
	segOffCRC      = 0
	segOffMagic    = 4
	segOffVersion  = 12
	segOffStart    = 16
	segOffReserved = 24
)

// Errors about segment files and the log as a whole.
var (
	// ErrCorrupt means the log's files are damaged or inconsistent.
	ErrCorrupt = errors.New("wal: corrupt log")
	// ErrBadMagic means a segment header passes its checksum but is not a
	// NoVacDB WAL header.
	ErrBadMagic = errors.New("wal: not a NoVacDB WAL segment")
	// ErrUnsupportedVersion means the segment has a format version this
	// code does not understand.
	ErrUnsupportedVersion = errors.New("wal: unsupported format version")
)

// AppendSegmentHeader appends the header of a segment starting at start.
func AppendSegmentHeader(dst []byte, start LSN) []byte {
	b := len(dst)
	dst = append(dst, make([]byte, SegmentHeaderSize)...)
	h := dst[b:]
	copy(h[segOffMagic:], segMagic[:])
	binary.LittleEndian.PutUint32(h[segOffVersion:], FormatVersion)
	binary.LittleEndian.PutUint64(h[segOffStart:], uint64(start))
	binary.LittleEndian.PutUint32(h[segOffCRC:], crc32.Checksum(h[segOffMagic:], castagnoli))
	return dst
}

// DecodeSegmentHeader validates the header at the start of buf and returns
// the segment's start LSN. A header that is short or fails its checksum
// yields ErrCorrupt; one that is intact but foreign yields ErrBadMagic or
// ErrUnsupportedVersion.
func DecodeSegmentHeader(buf []byte) (LSN, error) {
	if len(buf) < SegmentHeaderSize {
		return 0, fmt.Errorf("segment header: %d bytes, need %d: %w", len(buf), SegmentHeaderSize, ErrCorrupt)
	}
	h := buf[:SegmentHeaderSize]
	if got, want := binary.LittleEndian.Uint32(h[segOffCRC:]), crc32.Checksum(h[segOffMagic:], castagnoli); got != want {
		return 0, fmt.Errorf("segment header checksum: stored %#08x, computed %#08x: %w", got, want, ErrCorrupt)
	}
	if [8]byte(h[segOffMagic:segOffMagic+8]) != segMagic {
		return 0, ErrBadMagic
	}
	if v := binary.LittleEndian.Uint32(h[segOffVersion:]); v != FormatVersion {
		return 0, fmt.Errorf("version %d: %w", v, ErrUnsupportedVersion)
	}
	if binary.LittleEndian.Uint64(h[segOffReserved:]) != 0 {
		return 0, fmt.Errorf("reserved bytes are not zero: %w", ErrCorrupt)
	}
	return LSN(binary.LittleEndian.Uint64(h[segOffStart:])), nil
}

const (
	segNamePrefix = "wal-"
	segNameSuffix = ".seg"
	segNameDigits = 16
)

// SegmentName returns the file name of the segment starting at start.
func SegmentName(start LSN) string {
	return fmt.Sprintf("%s%0*x%s", segNamePrefix, segNameDigits, uint64(start), segNameSuffix)
}

// ParseSegmentName extracts the start LSN from a segment file name. It
// accepts exactly the names SegmentName produces.
func ParseSegmentName(name string) (LSN, bool) {
	if !strings.HasPrefix(name, segNamePrefix) || !strings.HasSuffix(name, segNameSuffix) {
		return 0, false
	}
	digits := name[len(segNamePrefix) : len(name)-len(segNameSuffix)]
	if len(digits) != segNameDigits {
		return 0, false
	}
	v, err := strconv.ParseUint(digits, 16, 64)
	if err != nil || SegmentName(LSN(v)) != name { // rejects uppercase and signs
		return 0, false
	}
	return LSN(v), true
}
