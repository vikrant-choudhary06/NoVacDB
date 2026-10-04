package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"slices"
	"strconv"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

var bg = context.Background()

const testDir = "/db/wal"

func testSeed(t testing.TB) uint64 {
	t.Helper()
	seed := uint64(1)
	if s := os.Getenv("NOVACDB_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("bad NOVACDB_SEED %q: %v", s, err)
		}
		seed = v
	}
	if tt, ok := t.(*testing.T); ok {
		tt.Logf("seed = %d (override with NOVACDB_SEED)", seed)
	}
	return seed
}

// newFS returns a MemFS with a durable /db directory.
func newFS(t testing.TB) *vfs.MemFS {
	t.Helper()
	m := vfs.NewMemFS(testSeed(t))
	if err := m.MkdirAll("/db"); err != nil {
		t.Fatal(err)
	}
	if err := m.SyncDir("/"); err != nil {
		t.Fatal(err)
	}
	return m
}

func mustOpen(t testing.TB, fsys vfs.FS, opts Options) *Writer {
	t.Helper()
	w, err := Open(fsys, testDir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return w
}

func mustAppend(t testing.TB, w *Writer, typ RecordType, payload []byte) LSN {
	t.Helper()
	lsn, err := w.Append(bg, typ, payload)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	return lsn
}

func mustFlush(t testing.TB, w *Writer) {
	t.Helper()
	if err := w.Flush(bg); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func mustClose(t testing.TB, w *Writer) {
	t.Helper()
	if err := w.Close(bg); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func payload(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.UintN(256))
	}
	return b
}

// logRec is a record as the oracle reads it back.
type logRec struct {
	LSN     LSN
	Type    RecordType
	Payload []byte
}

func (r logRec) String() string {
	return fmt.Sprintf("{%d type %d len %d}", r.LSN, r.Type, len(r.Payload))
}

type segFile struct {
	Start LSN
	Size  int64
	Recs  int
}

func readFile(t testing.TB, fsys vfs.FS, name string) []byte {
	t.Helper()
	f, err := fsys.OpenFile(name, vfs.ORead)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	defer func() { _ = f.Close() }()
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, size)
	if size > 0 {
		if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	}
	return buf
}

// writeFile replaces name with data and makes it durable.
func writeFile(t testing.TB, fsys vfs.FS, name string, data []byte) {
	t.Helper()
	dir := path.Dir(name)
	if err := fsys.MkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := fsys.SyncDir(path.Dir(dir)); err != nil {
		t.Fatal(err)
	}
	f, err := fsys.OpenFile(name, vfs.ORead|vfs.OWrite|vfs.OCreate|vfs.OTrunc)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 0 {
		if _, err := f.WriteAt(data, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if err := fsys.SyncDir(path.Dir(name)); err != nil {
		t.Fatal(err)
	}
}

// readLog is the test oracle: an independent reader of the segment files. It
// is strict: every byte of every segment must be accounted for, which is what
// a healthy, recovered log looks like.
func readLog(t testing.TB, fsys vfs.FS, dir string) ([]logRec, []segFile) {
	t.Helper()
	names, err := fsys.List(dir)
	if err != nil {
		t.Fatalf("list %s: %v", dir, err)
	}
	var starts []LSN
	for _, n := range names {
		if s, ok := ParseSegmentName(n); ok {
			starts = append(starts, s)
		}
	}
	slices.Sort(starts)
	var recs []logRec
	var segs []segFile
	for i, s := range starts {
		data := readFile(t, fsys, path.Join(dir, SegmentName(s)))
		hs, err := DecodeSegmentHeader(data)
		if err != nil || hs != s {
			t.Fatalf("segment %s: header: start %d, %v", SegmentName(s), hs, err)
		}
		if i > 0 {
			prev := segs[i-1]
			if uint64(prev.Start)+uint64(prev.Size) != uint64(s) {
				t.Fatalf("segment %s does not continue the previous one (%d+%d)", SegmentName(s), prev.Start, prev.Size)
			}
		}
		off, n := SegmentHeaderSize, 0
		for off < len(data) {
			rec, size, err := DecodeRecord(data[off:])
			if err != nil || uint64(rec.LSN) != uint64(s)+uint64(off) {
				break
			}
			recs = append(recs, logRec{rec.LSN, rec.Type, bytes.Clone(rec.Payload)})
			off += size
			n++
		}
		if off != len(data) {
			t.Fatalf("segment %s: %d trailing bytes after the last record", SegmentName(s), len(data)-off)
		}
		segs = append(segs, segFile{Start: s, Size: int64(len(data)), Recs: n})
	}
	return recs, segs
}

func sameRecs(t testing.TB, got, want []logRec, ctx string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d records, want %d", ctx, len(got), len(want))
	}
	for i := range want {
		if got[i].LSN != want[i].LSN || got[i].Type != want[i].Type || !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Fatalf("%s: record %d is %v, want %v", ctx, i, got[i], want[i])
		}
	}
}

func crc32c(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// Heap helpers for tests about logging and recovery, not row versions:
// every write is stamped with transaction 1 and no undo.

var stamp1 storage.Stamper = func(storage.RID, *storage.Version) (storage.RowHeader, error) {
	return storage.RowHeader{XID: 1}, nil
}

func heapInsert(h *storage.Heap, data []byte) (storage.RID, error) {
	return h.Insert(bg, data, stamp1)
}

func heapUpdate(h *storage.Heap, rid storage.RID, data []byte) error {
	return h.Update(bg, rid, data, stamp1)
}

func heapDelete(h *storage.Heap, rid storage.RID) error { return h.Delete(bg, rid, stamp1) }

func heapGet(h *storage.Heap, rid storage.RID) ([]byte, error) {
	v, err := h.Get(bg, rid)
	return v.Data, err
}

func heapNext(s *storage.Scanner) (storage.RID, []byte, bool, error) {
	rid, v, ok, err := s.Next(bg)
	return rid, v.Data, ok, err
}
