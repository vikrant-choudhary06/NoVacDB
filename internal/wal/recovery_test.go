package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"path"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

func TestReopenContinuesLog(t *testing.T) {
	for _, segSize := range []int64{0, 300} {
		t.Run(fmt.Sprintf("segment=%d", segSize), func(t *testing.T) {
			m := newFS(t)
			rng := rand.New(rand.NewPCG(testSeed(t), 3))
			var recs []logRec
			for cycle := range 30 {
				w := mustOpen(t, m, Options{SegmentSize: segSize})
				if len(recs) > 0 {
					last := recs[len(recs)-1]
					if want := last.LSN + LSN(RecordSize(len(last.Payload))); w.EndLSN() != want {
						t.Fatalf("cycle %d: EndLSN %d, want the end of the last record %d", cycle, w.EndLSN(), want)
					}
				}
				for range 1 + rng.IntN(12) {
					typ := RecordType(1 + rng.IntN(9))
					p := payload(rng, rng.IntN(120))
					recs = append(recs, logRec{mustAppend(t, w, typ, p), typ, p})
				}
				if cycle%2 == 0 {
					mustClose(t, w)
				} else { // acknowledged, then power loss
					mustFlush(t, w)
					m.Crash(vfs.CrashOptions{TearLast: rng.IntN(2) == 0})
				}
			}
			mustOpen(t, m, Options{SegmentSize: segSize})
			got, _ := readLog(t, m, testDir)
			sameRecs(t, got, recs, "final log")
		})
	}
}

// buildLog writes n records of varying size, closes the log, and returns the
// filesystem and the records.
func buildLog(t *testing.T, n int, segSize int64) (*vfs.MemFS, []logRec) {
	t.Helper()
	m := newFS(t)
	w := mustOpen(t, m, Options{SegmentSize: segSize})
	rng := rand.New(rand.NewPCG(testSeed(t), 4))
	var recs []logRec
	for range n {
		typ := RecordType(1 + rng.IntN(5))
		p := payload(rng, 1+rng.IntN(60))
		recs = append(recs, logRec{mustAppend(t, w, typ, p), typ, p})
	}
	mustClose(t, w)
	return m, recs
}

// Cutting the last segment at any byte must recover exactly the records that
// are complete, and appending afterwards must work.
func TestTornTailAtEveryByte(t *testing.T) {
	m, recs := buildLog(t, 6, 0)
	name := path.Join(testDir, SegmentName(0))
	full := readFile(t, m, name)
	for cut := SegmentHeaderSize; cut <= len(full); cut++ {
		var want []logRec
		wantEnd := LSN(SegmentHeaderSize)
		for _, r := range recs {
			if end := r.LSN + LSN(RecordSize(len(r.Payload))); int(end) <= cut {
				want = append(want, r)
				wantEnd = end
			}
		}
		m2 := newFS(t)
		writeFile(t, m2, name, full[:cut])
		w := mustOpen(t, m2, Options{})
		if w.EndLSN() != wantEnd {
			t.Fatalf("cut at %d: EndLSN %d, want %d", cut, w.EndLSN(), wantEnd)
		}
		if got := readFile(t, m2, name); len(got) != int(wantEnd) {
			t.Fatalf("cut at %d: file is %d bytes, tail not truncated to %d", cut, len(got), wantEnd)
		}
		lsn := mustAppend(t, w, 9, []byte("after"))
		if lsn != wantEnd {
			t.Fatalf("cut at %d: next LSN %d, want %d", cut, lsn, wantEnd)
		}
		mustClose(t, w)
		got, _ := readLog(t, m2, testDir)
		sameRecs(t, got, append(want, logRec{lsn, 9, []byte("after")}), fmt.Sprintf("cut at %d", cut))
	}
}

func TestGarbageAfterLastRecordIsCutOff(t *testing.T) {
	m, recs := buildLog(t, 4, 0)
	name := path.Join(testDir, SegmentName(0))
	clean := readFile(t, m, name)
	rng := rand.New(rand.NewPCG(testSeed(t), 5))
	for n := 1; n <= 200; n += 7 {
		garbage := append(bytes.Clone(clean), payload(rng, n)...)
		m2 := newFS(t)
		writeFile(t, m2, name, garbage)
		w := mustOpen(t, m2, Options{})
		if int(w.EndLSN()) != len(clean) {
			t.Fatalf("%d garbage bytes: EndLSN %d, want %d", n, w.EndLSN(), len(clean))
		}
		mustClose(t, w)
		got, _ := readLog(t, m2, testDir)
		sameRecs(t, got, recs, "log")
	}
	// A run of zeros (what a file extended by the OS looks like).
	zeros := append(bytes.Clone(clean), make([]byte, 4096)...)
	m3 := newFS(t)
	writeFile(t, m3, name, zeros)
	if w := mustOpen(t, m3, Options{}); int(w.EndLSN()) != len(clean) {
		t.Fatalf("zero tail: EndLSN %d", w.EndLSN())
	}
}

func TestStaleRecordAtTailIsCutOff(t *testing.T) {
	// A well-formed record with a good checksum but the wrong LSN (an old
	// record copied or left behind) must not be accepted at this position.
	m, recs := buildLog(t, 4, 0)
	name := path.Join(testDir, SegmentName(0))
	clean := readFile(t, m, name)
	stale := AppendRecord(nil, recs[0].LSN, recs[0].Type, recs[0].Payload)
	m2 := newFS(t)
	writeFile(t, m2, name, append(bytes.Clone(clean), stale...))
	w := mustOpen(t, m2, Options{})
	if int(w.EndLSN()) != len(clean) {
		t.Fatalf("EndLSN %d, want %d", w.EndLSN(), len(clean))
	}
}

func TestCorruptRecordEndsTheLog(t *testing.T) {
	// Documented behaviour: the log ends at the first bad record, so damage
	// in the middle of the last segment drops what follows it.
	m, recs := buildLog(t, 6, 0)
	name := path.Join(testDir, SegmentName(0))
	data := readFile(t, m, name)
	victim := recs[3]
	data[int(victim.LSN)+RecordHeaderSize] ^= 0xFF // inside record 3's payload
	m2 := newFS(t)
	writeFile(t, m2, name, data)
	w := mustOpen(t, m2, Options{})
	if w.EndLSN() != victim.LSN {
		t.Fatalf("EndLSN %d, want the start of the damaged record %d", w.EndLSN(), victim.LSN)
	}
	mustClose(t, w)
	got, _ := readLog(t, m2, testDir)
	sameRecs(t, got, recs[:3], "log")
}

func TestOnlyTheLastSegmentIsTrimmed(t *testing.T) {
	m, recs := buildLog(t, 30, 150)
	_, segs := readLog(t, m, testDir)
	if len(segs) < 5 {
		t.Fatalf("setup: %d segments", len(segs))
	}
	last := path.Join(testDir, SegmentName(segs[len(segs)-1].Start))
	data := readFile(t, m, last)
	writeFile(t, m, last, append(data, 0xAB, 0xCD, 0xEF))
	w := mustOpen(t, m, Options{SegmentSize: 150})
	mustClose(t, w)
	got, _ := readLog(t, m, testDir)
	sameRecs(t, got, recs, "log")
}

// --- leftovers, corruption, and bad directories ----------------------------------

func TestLeftoverSegmentFromCrashedRollover(t *testing.T) {
	// A crash while creating the next segment leaves a file that cannot hold
	// records. Open must delete it and carry on from the previous segment.
	for _, size := range []int{0, 1, 17, 31, 32} {
		t.Run(fmt.Sprintf("invalid-%dB", size), func(t *testing.T) {
			m, recs := buildLog(t, 5, 0)
			_, segs := readLog(t, m, testDir)
			end := segs[0].Start + LSN(segs[0].Size)
			var junk []byte
			if size > 0 {
				junk = bytes.Repeat([]byte{0xAA}, size)
			}
			leftover := path.Join(testDir, SegmentName(end))
			writeFile(t, m, leftover, junk)
			w := mustOpen(t, m, Options{})
			if w.EndLSN() != end {
				t.Fatalf("EndLSN %d, want %d", w.EndLSN(), end)
			}
			if _, err := m.OpenFile(leftover, vfs.ORead); !errors.Is(err, vfs.ErrNotExist) {
				t.Fatalf("leftover file not removed: %v", err)
			}
			lsn := mustAppend(t, w, 1, []byte("next"))
			mustClose(t, w)
			got, _ := readLog(t, m, testDir)
			sameRecs(t, got, append(recs, logRec{lsn, 1, []byte("next")}), "log")
		})
	}
	t.Run("only segment", func(t *testing.T) {
		m := newFS(t)
		writeFile(t, m, path.Join(testDir, SegmentName(0)), []byte{1, 2, 3})
		w := mustOpen(t, m, Options{})
		if w.EndLSN() != SegmentHeaderSize {
			t.Fatalf("EndLSN %d", w.EndLSN())
		}
	})
	t.Run("only segment but not the first", func(t *testing.T) {
		m := newFS(t)
		writeFile(t, m, path.Join(testDir, SegmentName(4096)), []byte{1, 2, 3})
		if _, err := Open(m, testDir, Options{}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestEmptySegmentWithValidHeaderIsKept(t *testing.T) {
	m, recs := buildLog(t, 5, 0)
	_, segs := readLog(t, m, testDir)
	end := segs[0].Start + LSN(segs[0].Size)
	writeFile(t, m, path.Join(testDir, SegmentName(end)), AppendSegmentHeader(nil, end))
	w := mustOpen(t, m, Options{})
	if w.EndLSN() != end+SegmentHeaderSize {
		t.Fatalf("EndLSN %d, want %d", w.EndLSN(), end+SegmentHeaderSize)
	}
	lsn := mustAppend(t, w, 1, []byte("x"))
	if lsn != end+SegmentHeaderSize {
		t.Fatalf("first record of the empty segment got LSN %d", lsn)
	}
	mustClose(t, w)
	got, _ := readLog(t, m, testDir)
	sameRecs(t, got, append(recs, logRec{lsn, 1, []byte("x")}), "log")
}

func TestOpenRejectsDamagedLogs(t *testing.T) {
	reseal := func(hdr []byte, f func(b []byte)) []byte {
		b := bytes.Clone(hdr)
		f(b)
		binary.LittleEndian.PutUint32(b, crc32c(b[4:]))
		return b
	}
	good := func(t *testing.T) (*vfs.MemFS, []segFile) {
		m, _ := buildLog(t, 20, 150)
		_, segs := readLog(t, m, testDir)
		if len(segs) < 4 {
			t.Fatalf("setup: %d segments", len(segs))
		}
		return m, segs
	}
	name := func(s segFile) string { return path.Join(testDir, SegmentName(s.Start)) }

	tests := []struct {
		name string
		edit func(t *testing.T, m *vfs.MemFS, segs []segFile)
		want error
	}{
		{"wrong magic", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			d := readFile(t, m, name(s[1]))
			copy(d, reseal(d[:SegmentHeaderSize], func(b []byte) { b[4] = 'X' }))
			writeFile(t, m, name(s[1]), d)
		}, ErrBadMagic},
		{"future version", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			d := readFile(t, m, name(s[1]))
			copy(d, reseal(d[:SegmentHeaderSize], func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 5) }))
			writeFile(t, m, name(s[1]), d)
		}, ErrUnsupportedVersion},
		{"version 3 log", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			d := readFile(t, m, name(s[1]))
			copy(d, reseal(d[:SegmentHeaderSize], func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 3) }))
			writeFile(t, m, name(s[1]), d)
		}, ErrUnsupportedVersion},
		{"version 2 log", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			d := readFile(t, m, name(s[1]))
			copy(d, reseal(d[:SegmentHeaderSize], func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 2) }))
			writeFile(t, m, name(s[1]), d)
		}, ErrUnsupportedVersion},
		{"version 1 log", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			d := readFile(t, m, name(s[1]))
			copy(d, reseal(d[:SegmentHeaderSize], func(b []byte) { binary.LittleEndian.PutUint32(b[12:], 1) }))
			writeFile(t, m, name(s[1]), d)
		}, ErrUnsupportedVersion},
		{"damaged header in a middle segment", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			d := readFile(t, m, name(s[1]))
			d[20] ^= 1
			writeFile(t, m, name(s[1]), d)
		}, ErrCorrupt},
		{"damaged header in the last segment, records present", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			last := s[len(s)-1]
			d := readFile(t, m, name(last))
			d[20] ^= 1
			writeFile(t, m, name(last), d)
		}, ErrCorrupt},
		{"header start differs from file name", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			d := readFile(t, m, name(s[2]))
			copy(d, reseal(d[:SegmentHeaderSize], func(b []byte) { binary.LittleEndian.PutUint64(b[16:], uint64(s[2].Start)+1) }))
			writeFile(t, m, name(s[2]), d)
		}, ErrCorrupt},
		{"missing middle segment", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			if err := m.Remove(name(s[2])); err != nil {
				t.Fatal(err)
			}
			if err := m.SyncDir(testDir); err != nil {
				t.Fatal(err)
			}
		}, ErrCorrupt},
		{"middle segment shortened", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			d := readFile(t, m, name(s[1]))
			writeFile(t, m, name(s[1]), d[:len(d)-10])
		}, ErrCorrupt},
		{"middle segment extended", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			d := readFile(t, m, name(s[1]))
			writeFile(t, m, name(s[1]), append(d, 1, 2, 3))
		}, ErrCorrupt},
		{"short file in the middle", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			writeFile(t, m, name(s[1]), []byte{1, 2, 3})
		}, ErrCorrupt},
		{"gap before the last segment", func(t *testing.T, m *vfs.MemFS, s []segFile) {
			last := s[len(s)-1]
			bogus := last.Start + LSN(last.Size) + 1000
			writeFile(t, m, path.Join(testDir, SegmentName(bogus)), AppendSegmentHeader(nil, bogus))
		}, ErrCorrupt},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, segs := good(t)
			tc.edit(t, m, segs)
			before := map[string][]byte{}
			names, _ := m.List(testDir)
			for _, n := range names {
				before[n] = readFile(t, m, path.Join(testDir, n))
			}
			if _, err := Open(m, testDir, Options{SegmentSize: 150}); !errors.Is(err, tc.want) {
				t.Fatalf("Open err = %v, want %v", err, tc.want)
			}
			// A refused Open must not have modified or deleted anything.
			after, _ := m.List(testDir)
			if len(after) != len(before) {
				t.Fatalf("Open changed the directory although it failed: %v -> %v", len(before), len(after))
			}
			for n, want := range before {
				if got := readFile(t, m, path.Join(testDir, n)); !bytes.Equal(got, want) {
					t.Fatalf("Open changed %s although it failed", n)
				}
			}
		})
	}
}

func TestOpenIgnoresUnrelatedFiles(t *testing.T) {
	m, recs := buildLog(t, 3, 0)
	for _, n := range []string{"README", "wal-zz.seg", SegmentName(0) + ".tmp", "wal-0000000000000000.SEG"} {
		writeFile(t, m, path.Join(testDir, n), []byte("not a segment"))
	}
	w := mustOpen(t, m, Options{})
	mustClose(t, w)
	got, _ := readLog(t, m, testDir)
	sameRecs(t, got, recs, "log")
}

func TestOpenFailsCleanlyOnIOErrors(t *testing.T) {
	for _, op := range []vfs.Op{vfs.OpMkdirAll, vfs.OpSyncDir, vfs.OpList, vfs.OpOpenFile, vfs.OpWriteAt, vfs.OpSync} {
		for after := range 3 {
			t.Run(fmt.Sprintf("%v/%d", op, after), func(t *testing.T) {
				m := newFS(t)
				m.InjectError(vfs.Fault{Op: op, After: after})
				w, err := Open(m, testDir, Options{})
				m.ClearFaults()
				if err == nil { // the fault landed after Open finished
					mustClose(t, w)
				} else if !errors.Is(err, vfs.ErrInjected) {
					t.Fatalf("Open err = %v", err)
				}
				// Whatever happened, a second Open must work.
				m.Crash(vfs.CrashOptions{TearLast: true})
				w2 := mustOpen(t, m, Options{})
				mustClose(t, w2)
			})
		}
	}
}

// --- misuse ----------------------------------------------------------------------

func TestOptionsAndArgumentValidation(t *testing.T) {
	m := newFS(t)
	for _, o := range []Options{{SegmentSize: -1}, {MaxPendingBytes: -1}} {
		if _, err := Open(m, testDir, o); !errors.Is(err, ErrInvalidOptions) {
			t.Errorf("Open(%+v) err = %v", o, err)
		}
	}
	w := mustOpen(t, m, Options{})
	if _, err := w.Append(bg, 0, []byte("x")); !errors.Is(err, ErrInvalidType) {
		t.Errorf("type 0 err = %v", err)
	}
	if _, err := w.Append(bg, 1, make([]byte, MaxPayload+1)); !errors.Is(err, ErrPayloadTooLarge) {
		t.Errorf("oversize err = %v", err)
	}
	if _, err := w.Append(bg, 1, make([]byte, MaxPayload)); err != nil {
		t.Errorf("max payload rejected: %v", err)
	}
	if _, err := w.Append(bg, 1, nil); err != nil {
		t.Errorf("nil payload rejected: %v", err)
	}
	if w.EndLSN() != SegmentHeaderSize+LSN(RecordSize(MaxPayload))+LSN(RecordSize(0)) {
		t.Errorf("rejected appends changed EndLSN: %d", w.EndLSN())
	}
	cancelled, cancel := context.WithCancel(bg)
	cancel()
	if _, err := w.Append(cancelled, 1, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("Append cancelled err = %v", err)
	}
	if err := w.Flush(cancelled); !errors.Is(err, context.Canceled) {
		t.Errorf("Flush cancelled err = %v", err)
	}
	if err := w.FlushTo(cancelled, 1); !errors.Is(err, context.Canceled) {
		t.Errorf("FlushTo cancelled err = %v", err)
	}
	mustFlush(t, w) // a cancelled flush must not have damaged the writer
}

func TestUseAfterClose(t *testing.T) {
	m := newFS(t)
	w := mustOpen(t, m, Options{})
	mustAppend(t, w, 1, []byte("x"))
	mustClose(t, w)
	if _, err := w.Append(bg, 1, nil); !errors.Is(err, ErrClosed) {
		t.Errorf("Append err = %v", err)
	}
	if err := w.Flush(bg); !errors.Is(err, ErrClosed) {
		t.Errorf("Flush err = %v", err)
	}
	if err := w.FlushTo(bg, 40); !errors.Is(err, ErrClosed) {
		t.Errorf("FlushTo err = %v", err)
	}
	if err := w.Close(bg); !errors.Is(err, ErrClosed) {
		t.Errorf("second Close err = %v", err)
	}
	// The record was flushed by Close.
	mustOpen(t, m, Options{})
	if recs, _ := readLog(t, m, testDir); len(recs) != 1 {
		t.Fatalf("%d records", len(recs))
	}
}

func TestOSFSWorks(t *testing.T) {
	dir := path.Join(t.TempDir(), "wal")
	var fsys vfs.FS = vfs.OSFS{}
	w, err := Open(fsys, dir, Options{SegmentSize: 200})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(1, 1))
	var recs []logRec
	for range 40 {
		p := payload(rng, rng.IntN(60))
		lsn, err := w.Append(bg, 3, p)
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, logRec{lsn, 3, p})
	}
	if err := w.Close(bg); err != nil {
		t.Fatal(err)
	}
	w2, err := Open(fsys, dir, Options{SegmentSize: 200})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close(bg) }()
	got, segs := readLog(t, fsys, dir)
	sameRecs(t, got, recs, "log on disk")
	if len(segs) < 3 {
		t.Fatalf("%d segments", len(segs))
	}
}
