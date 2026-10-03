package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

// FuzzDecodeRecord: arbitrary bytes never panic; anything that decodes is
// canonical (it re-encodes to the same bytes) and its prefixes are short.
func FuzzDecodeRecord(f *testing.F) {
	f.Add(AppendRecord(nil, 32, 1, []byte("hello")))
	f.Add(AppendRecord(nil, 1<<40, 0xFFFF, nil))
	f.Add(AppendRecord(nil, 7, 2, bytes.Repeat([]byte{9}, 300)))
	f.Add(make([]byte, 64))
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xFF}, 40))
	f.Fuzz(func(t *testing.T, buf []byte) {
		rec, n, err := DecodeRecord(buf)
		if err != nil {
			if !errors.Is(err, ErrShortRecord) && !errors.Is(err, ErrBadRecord) && !errors.Is(err, ErrChecksum) {
				t.Fatalf("unclassified error %v", err)
			}
			return
		}
		if n < RecordHeaderSize || n > len(buf) || len(rec.Payload) != n-RecordHeaderSize || len(rec.Payload) > MaxPayload {
			t.Fatalf("inconsistent decode: n=%d payload=%d len=%d", n, len(rec.Payload), len(buf))
		}
		if rec.Type == 0 {
			t.Fatal("decoded type 0")
		}
		if again := AppendRecord(nil, rec.LSN, rec.Type, rec.Payload); !bytes.Equal(again, buf[:n]) {
			t.Fatal("record is not canonical: re-encoding differs")
		}
		if _, _, err := DecodeRecord(buf[:n-1]); !errors.Is(err, ErrShortRecord) {
			t.Fatalf("one byte short: err = %v", err)
		}
	})
}

// FuzzDecodeSegmentHeader: arbitrary bytes never panic; an accepted header is
// canonical.
func FuzzDecodeSegmentHeader(f *testing.F) {
	f.Add(AppendSegmentHeader(nil, 0))
	f.Add(AppendSegmentHeader(nil, 123456))
	f.Add(make([]byte, SegmentHeaderSize))
	f.Add([]byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, buf []byte) {
		start, err := DecodeSegmentHeader(buf)
		if err != nil {
			return
		}
		if !bytes.Equal(AppendSegmentHeader(nil, start), buf[:SegmentHeaderSize]) {
			t.Fatal("accepted header is not canonical")
		}
	})
}

// FuzzOpen: an arbitrary file as the only segment. Open must not panic. If it
// accepts the file, the log must be fully usable: appending works, flushing
// works, and reopening shows exactly what was appended on top of what Open
// reported.
func FuzzOpen(f *testing.F) {
	m := vfs.NewMemFS(1)
	_ = m.MkdirAll("/db")
	_ = m.SyncDir("/")
	w, err := Open(m, testDir, Options{})
	if err != nil {
		f.Fatal(err)
	}
	for i := range 4 {
		if _, err := w.Append(bg, RecordType(i+1), bytes.Repeat([]byte{byte(i)}, 10*i)); err != nil {
			f.Fatal(err)
		}
	}
	if err := w.Close(bg); err != nil {
		f.Fatal(err)
	}
	valid := readFile(f, m, testDir+"/"+SegmentName(0))
	f.Add(valid)
	f.Add(valid[:len(valid)-3])
	f.Add(append(bytes.Clone(valid), 1, 2, 3, 4))
	f.Add(AppendSegmentHeader(nil, 0))
	f.Add(AppendSegmentHeader(nil, 0)[:20])
	f.Add([]byte{})
	f.Add(make([]byte, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<16 {
			data = data[:1<<16]
		}
		fsys := newFS(t)
		writeFile(t, fsys, testDir+"/"+SegmentName(0), data)
		w, err := Open(fsys, testDir, Options{SegmentSize: 300})
		if err != nil {
			return
		}
		end := w.EndLSN()
		if end < SegmentHeaderSize || w.DurableEnd() != end {
			t.Fatalf("EndLSN %d DurableEnd %d", end, w.DurableEnd())
		}
		lsn, err := w.Append(bg, 9, []byte("fuzz"))
		if err != nil || lsn != end {
			t.Fatalf("Append = %d, %v; want LSN %d", lsn, err, end)
		}
		if err := w.Close(bg); err != nil {
			t.Fatal(err)
		}
		w2, err := Open(fsys, testDir, Options{SegmentSize: 300})
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		if want := end + LSN(RecordSize(4)); w2.EndLSN() != want {
			t.Fatalf("reopened EndLSN %d, want %d", w2.EndLSN(), want)
		}
		recs, _ := readLog(t, fsys, testDir)
		if len(recs) == 0 || recs[len(recs)-1].LSN != lsn || string(recs[len(recs)-1].Payload) != "fuzz" {
			t.Fatalf("appended record missing from the log: %v", recs)
		}
	})
}

// FuzzReader: arbitrary bytes as the content of two consecutive segments.
// The reader must never panic, must only return records that sit exactly at
// their own LSN, and must agree with what the writer's recovery keeps.
func FuzzReader(f *testing.F) {
	m := vfs.NewMemFS(1)
	_ = m.MkdirAll("/db")
	_ = m.SyncDir("/")
	w, err := Open(m, testDir, Options{SegmentSize: 120})
	if err != nil {
		f.Fatal(err)
	}
	for i := range 6 {
		if _, err := w.Append(bg, RecordType(i+1), bytes.Repeat([]byte{byte(i)}, 5+7*i)); err != nil {
			f.Fatal(err)
		}
	}
	if err := w.Close(bg); err != nil {
		f.Fatal(err)
	}
	names, _ := m.List(testDir)
	seg0 := readFile(f, m, testDir+"/"+names[0])
	seg1 := readFile(f, m, testDir+"/"+names[1])
	f.Add(seg0, seg1)
	f.Add(seg0, []byte{})
	f.Add(seg0[:len(seg0)-1], seg1)
	f.Add(seg0, seg1[:40])
	f.Add([]byte{}, []byte{})

	f.Fuzz(func(t *testing.T, a, b []byte) {
		fsys := newFS(t)
		writeFile(t, fsys, testDir+"/"+SegmentName(0), a)
		if len(b) > 0 {
			writeFile(t, fsys, testDir+"/"+SegmentName(LSN(len(a))), b)
		}
		r, err := NewReader(fsys, testDir, 0)
		if err != nil {
			return
		}
		var got []Record
		var prevEnd LSN
		for {
			rec, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				if !errors.Is(err, ErrCorrupt) {
					t.Fatalf("unclassified error %v", err)
				}
				return
			}
			if prevEnd != 0 && rec.LSN < prevEnd {
				t.Fatalf("record %d overlaps the previous one ending at %d", rec.LSN, prevEnd)
			}
			prevEnd = rec.LSN + LSN(RecordSize(len(rec.Payload)))
			got = append(got, rec)
		}
		// Recovery must keep exactly what the reader returned.
		w, err := Open(fsys, testDir, Options{})
		if err != nil {
			t.Fatalf("reader accepted a log that Open rejects: %v", err)
		}
		if w.EndLSN() != r.End() && len(got) > 0 {
			t.Fatalf("reader ended at %d, recovery at %d", r.End(), w.EndLSN())
		}
		_ = w.Close(bg)
		kept, _ := readLog(t, fsys, testDir)
		if len(kept) != len(got) {
			t.Fatalf("reader returned %d records, recovery kept %d", len(got), len(kept))
		}
	})
}

// FuzzTxnRecovery appends arbitrary transaction and checkpoint records
// (types and payloads from the input) to a fresh engine's log and
// recovers it. Recovery either refuses the log as corrupt or succeeds; on
// success NextXID is past every ID a begin record named, and the status
// table agrees with the log: a begin followed by its commit is committed,
// any other begin aborted.
func FuzzTxnRecovery(f *testing.F) {
	u64 := func(v uint64) []byte { return binary.LittleEndian.AppendUint64(nil, v) }
	enc := func(recs ...[]byte) []byte { return bytes.Join(recs, nil) }
	rec := func(typ byte, payload []byte) []byte { return append([]byte{typ, byte(len(payload))}, payload...) }
	f.Add(enc(rec(7, u64(1)), rec(8, u64(1)), rec(7, u64(2))))
	f.Add(enc(rec(7, u64(3)), rec(2, append(u64(0), u64(10)...)), rec(7, u64(4)), rec(8, u64(4))))
	f.Add(enc(rec(8, u64(1))))
	f.Add(enc(rec(7, u64(5)), rec(7, u64(5))))
	f.Add(enc(rec(9, u64(1)), rec(4, nil)))
	f.Fuzz(func(t *testing.T, in []byte) {
		m := vfs.NewMemFS(1)
		_ = m.MkdirAll("/db")
		_ = m.SyncDir("/")
		e, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 8})
		if err != nil {
			t.Fatal(err)
		}
		begins := map[XID]bool{} // ID -> committed by the next commit record
		var openXID XID
		var maxBegin XID
		for len(in) >= 2 {
			typ, n := RecordType(in[0]%10), int(in[1])
			in = in[2:]
			if n > len(in) {
				n = len(in)
			}
			payload := in[:n]
			in = in[n:]
			if typ == 0 || typ == RecordHeap || typ == RecordBTree || typ == RecordDeferredFree {
				continue // 0 is never a type; the others' payloads are fuzzed elsewhere
			}
			if _, err := e.w.Append(bg, typ, payload); err != nil {
				t.Fatal(err)
			}
			if len(payload) == 8 {
				xid := XID(binary.LittleEndian.Uint64(payload))
				switch typ {
				case RecordTxnBegin:
					begins[xid], openXID = false, xid
					maxBegin = max(maxBegin, xid)
				case RecordTxnCommit:
					if xid == openXID {
						begins[xid] = true
					}
				}
			}
		}
		if err := e.w.Flush(bg); err != nil {
			t.Fatal(err)
		}
		m.Crash(vfs.CrashOptions{})
		e2, err := OpenEngine(bg, m, dbDir, EngineOptions{Frames: 8})
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("recovery failed with %v, not corruption", err)
			}
			return
		}
		defer func() { _ = e2.Close(bg) }()
		if maxBegin != 0 && e2.NextXID() <= maxBegin {
			t.Fatalf("next ID %d, but the log began %d", e2.NextXID(), maxBegin)
		}
		for xid, committed := range begins {
			want := TxnAborted
			if committed {
				want = TxnCommitted
			}
			if got := e2.Status(xid); got != want {
				t.Fatalf("transaction %d: %v, want %v", xid, got, want)
			}
		}
	})
}
