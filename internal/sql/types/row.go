package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
)

// Row format (docs/design/10-executor.md section 2.2), little-endian:
//
//	offset  size      field
//	0       1         format version (rowVersion)
//	1       2         column count n
//	3       ceil(n/8) null bitmap, bit i (LSB first) set = column i is NULL
//	...               non-NULL columns in order: integer 4 bytes, bigint,
//	                  double precision and timestamptz 8, boolean 1 (0 or 1),
//	                  text u32 length + bytes
const (
	rowVersion    = 1
	rowHeaderSize = 3
	// MaxColumns is the most columns a table may have, as in PostgreSQL.
	MaxColumns = 1600
)

// MaxRowSize is the largest encoded row (docs/design/15-row-versioning.md
// section 2.6).
const MaxRowSize = storage.MaxRowData

// EncodeRow encodes a row of values of the given column types. Each value
// must be NULL or have its column's type. A row larger than MaxRowSize is
// a 54000 error.
func EncodeRow(vals []Value, cols []Type) ([]byte, error) {
	if len(vals) != len(cols) || len(cols) > MaxColumns {
		return nil, sqlerr.New(sqlerr.InternalError, "encoding a row of %d values for %d columns", len(vals), len(cols))
	}
	n := len(cols)
	out := make([]byte, rowHeaderSize+(n+7)/8, max(64, rowHeaderSize+(n+7)/8))
	out[0] = rowVersion
	binary.LittleEndian.PutUint16(out[1:], uint16(n))
	for i, v := range vals {
		if v.Null {
			out[rowHeaderSize+i/8] |= 1 << (i % 8)
			continue
		}
		if v.T != cols[i] {
			return nil, sqlerr.New(sqlerr.InternalError, "encoding a %s value into a %s column", v.T, cols[i])
		}
		switch cols[i] {
		case Int4:
			out = binary.LittleEndian.AppendUint32(out, uint32(int32(v.I)))
		case Int8, TimestampTZ:
			out = binary.LittleEndian.AppendUint64(out, uint64(v.I))
		case Float8:
			out = binary.LittleEndian.AppendUint64(out, math.Float64bits(v.F))
		case Bool:
			out = append(out, byte(v.I&1))
		case Text:
			out = binary.LittleEndian.AppendUint32(out, uint32(len(v.S)))
			out = append(out, v.S...)
		default:
			return nil, sqlerr.New(sqlerr.InternalError, "encoding a column of %s", cols[i])
		}
		if len(out) > MaxRowSize {
			break // reported below; stop copying large values
		}
	}
	if len(out) > MaxRowSize {
		return nil, sqlerr.New(sqlerr.ProgramLimitExceeded, "row is too big: more than the maximum of %d bytes", MaxRowSize).
			WithHint("NoVacDB does not store large values outside the row yet; keep each row's values under about 8 KB in total.")
	}
	return out, nil
}

func corruptRow(format string, args ...any) *sqlerr.Error {
	return sqlerr.New(sqlerr.DataCorrupted, "invalid stored row: %s", fmt.Sprintf(format, args...))
}

// DecodeRow decodes a row of the given column types. A row with fewer
// columns than the table reads the missing trailing ones as NULL. Every
// length and value is checked: damage is a XX001 error, never a panic.
func DecodeRow(b []byte, cols []Type) ([]Value, error) {
	if len(b) < rowHeaderSize || b[0] != rowVersion {
		return nil, corruptRow("bad header")
	}
	n := int(binary.LittleEndian.Uint16(b[1:]))
	if n > len(cols) {
		return nil, corruptRow("%d columns, table has %d", n, len(cols))
	}
	bitmap := rowHeaderSize + (n+7)/8
	if len(b) < bitmap {
		return nil, corruptRow("truncated null bitmap")
	}
	vals := make([]Value, len(cols))
	pos := bitmap
	need := func(k int) bool { return pos+k <= len(b) }
	for i, t := range cols {
		if i >= n || b[rowHeaderSize+i/8]&(1<<(i%8)) != 0 {
			vals[i] = Null(t)
			continue
		}
		switch t {
		case Int4:
			if !need(4) {
				return nil, corruptRow("truncated column %d", i)
			}
			vals[i] = NewInt4(int32(binary.LittleEndian.Uint32(b[pos:])))
			pos += 4
		case Int8, TimestampTZ:
			if !need(8) {
				return nil, corruptRow("truncated column %d", i)
			}
			vals[i] = Value{T: t, I: int64(binary.LittleEndian.Uint64(b[pos:]))}
			pos += 8
		case Float8:
			if !need(8) {
				return nil, corruptRow("truncated column %d", i)
			}
			vals[i] = NewFloat8(math.Float64frombits(binary.LittleEndian.Uint64(b[pos:])))
			pos += 8
		case Bool:
			if !need(1) || b[pos] > 1 {
				return nil, corruptRow("bad boolean in column %d", i)
			}
			vals[i] = NewBool(b[pos] == 1)
			pos++
		case Text:
			if !need(4) {
				return nil, corruptRow("truncated column %d", i)
			}
			l := int(binary.LittleEndian.Uint32(b[pos:]))
			pos += 4
			if l < 0 || l > len(b)-pos {
				return nil, corruptRow("text length %d in column %d", l, i)
			}
			s := string(b[pos : pos+l])
			if !utf8.ValidString(s) {
				return nil, corruptRow("invalid UTF-8 in column %d", i)
			}
			vals[i] = NewText(s)
			pos += l
		default:
			return nil, corruptRow("column %d has type %s", i, t)
		}
	}
	if pos != len(b) {
		return nil, corruptRow("%d bytes after the last column", len(b)-pos)
	}
	return vals, nil
}

// AppendKey appends v's order-preserving index key encoding to key. Its
// byte order equals Compare's order, with NULL after every value.
func AppendKey(key []byte, v Value) []byte {
	if v.Null {
		return btree.AppendNull(key)
	}
	switch v.T {
	case Float8:
		return btree.AppendFloat64(key, v.F)
	case Text, Unknown:
		return btree.AppendString(key, v.S)
	case Bool:
		return btree.AppendBool(key, v.Bool())
	default:
		return btree.AppendInt64(key, v.I)
	}
}
