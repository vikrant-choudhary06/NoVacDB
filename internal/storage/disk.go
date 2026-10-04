package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"sync"
	"sync/atomic"

	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

// File layout constants. See docs/design/03-disk-manager.md.
const (
	// headerSlots is the number of file header copies at the start of the
	// file (pages 0 and 1). Header writes alternate between them.
	headerSlots = 2
	// FirstDataPage is the ID of the first page usable for data. Page ID 0
	// is therefore never data, so 0 can mean "none" in the free list.
	FirstDataPage = headerSlots
	// FormatVersion is the on-disk format version written by this code.
	// Version 2 added a Flags byte to B+Tree leaf cells
	// (docs/design/08-btree.md, revision 2); version 3 added undo pages
	// (docs/design/14-undo-log.md); version 4 a header on every heap row
	// (docs/design/15-row-versioning.md). Other versions are refused.
	FormatVersion = 4
	// MaxPages is the largest page count for which every page offset fits
	// in an int64.
	MaxPages = uint64(math.MaxInt64 / PageSize)

	tmpSuffix = ".tmp"
)

// fileMagic identifies a NoVacDB data file.
var fileMagic = [8]byte{'N', 'O', 'V', 'A', 'C', 'D', 'B', 0}

// File header slot payload layout (offsets relative to the payload, which
// starts at page offset 24; little-endian):
//
//	offset  size  field
//	0       8     Magic          "NOVACDB\0"
//	8       4     FormatVersion  uint32, currently 2
//	12      4     PageSize       uint32, must be 8192
//	16      8     Generation     uint64; slot used = Generation % 2; higher wins
//	24      8     PageCount      uint64; all pages including the 2 header pages
//	32      8     FreeListHead   uint64; first free page ID, 0 = empty
//	40      8     FreePageCount  uint64; length of the free list
//	48      ...   reserved (zero)
const (
	hdrOffMagic      = 0
	hdrOffVersion    = 8
	hdrOffPageSize   = 12
	hdrOffGeneration = 16
	hdrOffPageCount  = 24
	hdrOffFreeHead   = 32
	hdrOffFreeCount  = 40
)

// Free page payload layout (offsets relative to the payload):
//
//	offset  size  field
//	0       8     NextFree  uint64; next page in the free list, 0 = end
//	8       ...   reserved (zero)
const freeOffNext = 0

// Errors returned by the disk manager, in addition to those in page.go and
// the vfs sentinels (which are wrapped, e.g. vfs.ErrNotExist).
var (
	// ErrInvalidPageID means an ID is reserved or not below PageCount.
	ErrInvalidPageID = errors.New("storage: invalid page id")
	// ErrCorrupt means the file has no valid header or a structure is
	// inconsistent.
	ErrCorrupt = errors.New("storage: corrupt data file")
	// ErrBadMagic means a header passes its checksum but is not a NoVacDB
	// file header.
	ErrBadMagic = errors.New("storage: not a NoVacDB data file")
	// ErrUnsupportedVersion means the file has a format version this code
	// does not understand.
	ErrUnsupportedVersion = errors.New("storage: unsupported format version")
	// ErrPageSizeMismatch means the file was created with another page size.
	ErrPageSizeMismatch = errors.New("storage: page size mismatch")
	// ErrDoubleFree means the page being freed is already on the free list.
	ErrDoubleFree = errors.New("storage: page already free")
	// ErrDiskClosed means the disk manager was closed.
	ErrDiskClosed = errors.New("storage: disk manager closed")
	// ErrFailed means an earlier I/O error left memory and disk possibly
	// out of step. Reopen the file to continue.
	ErrFailed = errors.New("storage: disk manager failed, reopen required")
	// ErrFull means the file cannot grow any further.
	ErrFull = errors.New("storage: data file full")
)

// fileHeader is the decoded content of a header slot.
type fileHeader struct {
	generation uint64
	pageCount  uint64
	freeHead   uint64
	freeCount  uint64
}

// valid checks the invariants a header must satisfy.
func (h fileHeader) valid() bool {
	if h.pageCount < FirstDataPage || h.pageCount > MaxPages {
		return false
	}
	if (h.freeHead == 0) != (h.freeCount == 0) {
		return false
	}
	if h.freeHead != 0 && (h.freeHead < FirstDataPage || h.freeHead >= h.pageCount) {
		return false
	}
	return h.freeCount <= h.pageCount-FirstDataPage
}

// slotState classifies a header slot read from disk.
type slotState int

const (
	slotBlank   slotState = iota // all zeros: never written
	slotDamaged                  // torn, corrupt or inconsistent
	slotValid
)

// encodeHeader fills buf with the sealed header page for the given slot.
func encodeHeader(buf []byte, slot uint64, h fileHeader) error {
	if err := InitPage(buf, Header{ID: slot, Type: PageTypeFileHeader}); err != nil {
		return err
	}
	p := buf[HeaderSize:]
	copy(p[hdrOffMagic:], fileMagic[:])
	binary.LittleEndian.PutUint32(p[hdrOffVersion:], FormatVersion)
	binary.LittleEndian.PutUint32(p[hdrOffPageSize:], PageSize)
	binary.LittleEndian.PutUint64(p[hdrOffGeneration:], h.generation)
	binary.LittleEndian.PutUint64(p[hdrOffPageCount:], h.pageCount)
	binary.LittleEndian.PutUint64(p[hdrOffFreeHead:], h.freeHead)
	binary.LittleEndian.PutUint64(p[hdrOffFreeCount:], h.freeCount)
	return Seal(buf)
}

// decodeHeader classifies buf as the content of the given slot. A non-nil
// error is fatal ("this is not our file") and must not be papered over by
// falling back to the other slot; ordinary damage is reported via the state.
func decodeHeader(buf []byte, slot uint64) (fileHeader, slotState, error) {
	if err := Verify(buf, slot); err != nil {
		if errors.Is(err, ErrZeroPage) {
			return fileHeader{}, slotBlank, nil
		}
		return fileHeader{}, slotDamaged, nil
	}
	// Verify already established the type is valid, so the error is nil.
	if h, _ := DecodeHeader(buf); h.Type != PageTypeFileHeader {
		return fileHeader{}, slotDamaged, nil
	}
	p := buf[HeaderSize:]
	if [8]byte(p[hdrOffMagic:hdrOffMagic+8]) != fileMagic {
		return fileHeader{}, slotDamaged, fmt.Errorf("slot %d: %w", slot, ErrBadMagic)
	}
	if v := binary.LittleEndian.Uint32(p[hdrOffVersion:]); v != FormatVersion {
		return fileHeader{}, slotDamaged, fmt.Errorf("slot %d: version %d: %w", slot, v, ErrUnsupportedVersion)
	}
	if s := binary.LittleEndian.Uint32(p[hdrOffPageSize:]); s != PageSize {
		return fileHeader{}, slotDamaged, fmt.Errorf("slot %d: page size %d: %w", slot, s, ErrPageSizeMismatch)
	}
	h := fileHeader{
		generation: binary.LittleEndian.Uint64(p[hdrOffGeneration:]),
		pageCount:  binary.LittleEndian.Uint64(p[hdrOffPageCount:]),
		freeHead:   binary.LittleEndian.Uint64(p[hdrOffFreeHead:]),
		freeCount:  binary.LittleEndian.Uint64(p[hdrOffFreeCount:]),
	}
	if !h.valid() || h.generation%headerSlots != slot {
		return fileHeader{}, slotDamaged, nil
	}
	return h, slotValid, nil
}

// readRaw reads page id into buf. Bytes beyond the end of the file read as
// zeros, so allocated-but-never-written pages look like zero pages.
func readRaw(f vfs.File, id uint64, buf []byte) error {
	n, err := f.ReadAt(buf, int64(id)*PageSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("reading page %d: %w", id, err)
	}
	clear(buf[n:])
	return nil
}

func writeRaw(f vfs.File, id uint64, buf []byte) error {
	if _, err := f.WriteAt(buf, int64(id)*PageSize); err != nil {
		return fmt.Errorf("writing page %d: %w", id, err)
	}
	return nil
}

// DiskManager reads, writes, allocates and frees pages in one data file.
// It is safe for concurrent use.
type DiskManager struct {
	mu       sync.RWMutex // write lock for header changes, read lock for page I/O
	f        vfs.File
	hdr      fileHeader
	closed   bool
	failed   atomic.Bool
	maxPages uint64 // MaxPages; a field so tests can make the file "full" quickly
}

// Create makes a new data file at name. It fails with an error wrapping
// vfs.ErrExist if the file exists. Creation is atomic: the file is built
// under a temporary name and renamed into place, so a crash leaves either
// no file or a complete one.
func Create(fsys vfs.FS, name string) (*DiskManager, error) {
	if f, err := fsys.OpenFile(name, vfs.ORead); err == nil {
		_ = f.Close()
		return nil, fmt.Errorf("creating %s: %w", name, vfs.ErrExist)
	} else if !errors.Is(err, vfs.ErrNotExist) {
		return nil, fmt.Errorf("creating %s: %w", name, err)
	}
	tmp := name + tmpSuffix
	if err := fsys.Remove(tmp); err != nil && !errors.Is(err, vfs.ErrNotExist) {
		return nil, fmt.Errorf("removing stale %s: %w", tmp, err)
	}
	if err := writeNewFile(fsys, tmp); err != nil {
		_ = fsys.Remove(tmp)
		return nil, fmt.Errorf("creating %s: %w", name, err)
	}
	if err := fsys.Rename(tmp, name); err != nil {
		return nil, fmt.Errorf("creating %s: renaming: %w", name, err)
	}
	if err := fsys.SyncDir(path.Dir(name)); err != nil {
		return nil, fmt.Errorf("creating %s: syncing directory: %w", name, err)
	}
	return Open(fsys, name)
}

// writeNewFile writes and syncs the two initial header slots.
func writeNewFile(fsys vfs.FS, name string) error {
	f, err := fsys.OpenFile(name, vfs.ORead|vfs.OWrite|vfs.OCreate|vfs.OExcl)
	if err != nil {
		return err
	}
	buf := make([]byte, PageSize)
	err = encodeHeader(buf, 0, fileHeader{pageCount: FirstDataPage})
	if err == nil {
		err = writeRaw(f, 0, buf)
	}
	if err == nil {
		clear(buf) // slot 1 starts blank
		err = writeRaw(f, 1, buf)
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// Open opens an existing data file and recovers its header: the valid slot
// with the highest generation wins.
func Open(fsys vfs.FS, name string) (*DiskManager, error) {
	f, err := fsys.OpenFile(name, vfs.ORead|vfs.OWrite)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", name, err)
	}
	hdr, err := readHeader(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("opening %s: %w", name, err)
	}
	return &DiskManager{f: f, hdr: hdr, maxPages: MaxPages}, nil
}

// readHeader picks the current header from the two slots.
func readHeader(f vfs.File) (fileHeader, error) {
	var (
		best  fileHeader
		found bool
	)
	buf := make([]byte, PageSize)
	for slot := range uint64(headerSlots) {
		if err := readRaw(f, slot, buf); err != nil {
			return fileHeader{}, err
		}
		h, state, err := decodeHeader(buf, slot)
		if err != nil {
			return fileHeader{}, err
		}
		if state == slotValid && (!found || h.generation > best.generation) {
			best, found = h, true
		}
	}
	if !found {
		return fileHeader{}, fmt.Errorf("no valid file header: %w", ErrCorrupt)
	}
	return best, nil
}

// usable returns an error if the manager cannot be used. Callers hold d.mu.
func (d *DiskManager) usable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed {
		return ErrDiskClosed
	}
	if d.failed.Load() {
		return ErrFailed
	}
	return nil
}

// checkID rejects reserved and out-of-range IDs. Callers hold d.mu.
func (d *DiskManager) checkID(id uint64) error {
	if id < FirstDataPage || id >= d.hdr.pageCount {
		return fmt.Errorf("page %d (valid %d..%d): %w", id, FirstDataPage, d.hdr.pageCount-1, ErrInvalidPageID)
	}
	return nil
}

// fail marks the manager failed after an I/O error. A failed fsync may have
// dropped data, so nothing is retried; the file must be reopened.
func (d *DiskManager) fail(err error) error {
	d.failed.Store(true)
	return err
}

// ReadPage reads page id into buf (which must be PageSize bytes) and verifies
// its checksum and ID. A page that was allocated but never written returns
// ErrZeroPage.
func (d *DiskManager) ReadPage(ctx context.Context, id uint64, buf []byte) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(ctx); err != nil {
		return err
	}
	if err := d.checkID(id); err != nil {
		return err
	}
	if err := checkSize(buf); err != nil {
		return fmt.Errorf("reading page %d: %w", id, err)
	}
	if err := readRaw(d.f, id, buf); err != nil {
		return err
	}
	return Verify(buf, id)
}

// WritePage seals buf (setting its checksum) and writes it as page id. The
// header in buf must carry the same ID and a data page type. The write is not
// durable until Sync.
func (d *DiskManager) WritePage(ctx context.Context, id uint64, buf []byte) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(ctx); err != nil {
		return err
	}
	if err := d.checkID(id); err != nil {
		return err
	}
	h, err := DecodeHeader(buf)
	if err != nil {
		return fmt.Errorf("writing page %d: %w", id, err)
	}
	if h.ID != id {
		return fmt.Errorf("writing page %d whose header says %d: %w", id, h.ID, ErrPageIDMismatch)
	}
	if h.Type == PageTypeFileHeader || h.Type == PageTypeFree {
		// Those types are managed by the disk manager itself.
		return fmt.Errorf("writing page %d of type %d: %w", id, h.Type, ErrBadPageType)
	}
	if err := Seal(buf); err != nil {
		return err
	}
	return writeRaw(d.f, id, buf)
}

// commitHeader durably replaces the header with next (generation bumped),
// writing the slot that does not hold the current header. On success d.hdr
// is updated. Callers hold the write lock.
func (d *DiskManager) commitHeader(next fileHeader) error {
	next.generation = d.hdr.generation + 1
	slot := next.generation % headerSlots
	buf := make([]byte, PageSize)
	if err := encodeHeader(buf, slot, next); err != nil {
		return d.fail(err)
	}
	if err := writeRaw(d.f, slot, buf); err != nil {
		return d.fail(err)
	}
	if err := d.f.Sync(); err != nil {
		return d.fail(fmt.Errorf("syncing header: %w", err))
	}
	d.hdr = next
	return nil
}

// Allocate returns the ID of a fresh page: the head of the free list if there
// is one, otherwise a new page at the end of the file. The page reads as all
// zeros until first written. The allocation is durable when Allocate returns,
// so a page is never handed out twice, even across crashes.
func (d *DiskManager) Allocate(ctx context.Context) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(ctx); err != nil {
		return 0, err
	}
	next := d.hdr
	if d.hdr.freeHead == 0 {
		if d.hdr.pageCount >= d.maxPages {
			return 0, ErrFull
		}
		id := d.hdr.pageCount
		next.pageCount++
		if err := d.commitHeader(next); err != nil {
			return 0, err
		}
		return id, nil
	}

	id := d.hdr.freeHead
	buf := make([]byte, PageSize)
	if err := readRaw(d.f, id, buf); err != nil {
		return 0, err
	}
	following, err := parseFreePage(buf, id, d.hdr.pageCount)
	if err != nil {
		return 0, err
	}
	next.freeHead = following
	next.freeCount--
	if err := d.commitHeader(next); err != nil {
		return 0, err
	}
	// Header first, zeroing second and unsynced: if the zero write is lost
	// the page keeps its old Free image, which is harmless because the
	// header no longer lists it.
	clear(buf)
	if err := writeRaw(d.f, id, buf); err != nil {
		return 0, d.fail(err)
	}
	return id, nil
}

// parseFreePage validates a Free page and returns its next pointer.
func parseFreePage(buf []byte, id, pageCount uint64) (uint64, error) {
	if err := Verify(buf, id); err != nil {
		return 0, fmt.Errorf("free list page %d: %w: %w", id, ErrCorrupt, err)
	}
	if h, _ := DecodeHeader(buf); h.Type != PageTypeFree {
		return 0, fmt.Errorf("free list page %d has type %d: %w", id, h.Type, ErrCorrupt)
	}
	following := binary.LittleEndian.Uint64(buf[HeaderSize+freeOffNext:])
	if following != 0 && (following < FirstDataPage || following >= pageCount) {
		return 0, fmt.Errorf("free list page %d points to %d: %w", id, following, ErrCorrupt)
	}
	return following, nil
}

// Free puts page id on the free list for reuse. It is durable when it
// returns. Free overwrites the page, so callers must only free pages whose
// contents are no longer needed (see the design doc, section 5).
func (d *DiskManager) Free(ctx context.Context, id uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(ctx); err != nil {
		return err
	}
	if err := d.checkID(id); err != nil {
		return err
	}
	buf := make([]byte, PageSize)
	if err := readRaw(d.f, id, buf); err != nil {
		return err
	}
	switch err := Verify(buf, id); {
	case err == nil:
		if h, _ := DecodeHeader(buf); h.Type == PageTypeFree {
			return fmt.Errorf("freeing page %d: %w", id, ErrDoubleFree)
		}
	case errors.Is(err, ErrZeroPage):
		// allocated but never written: fine to free
	default:
		return fmt.Errorf("freeing page %d: %w", id, err)
	}

	// The Free page must be durable before the header points at it.
	if err := InitPage(buf, Header{ID: id, Type: PageTypeFree}); err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(buf[HeaderSize+freeOffNext:], d.hdr.freeHead)
	if err := Seal(buf); err != nil {
		return err
	}
	if err := writeRaw(d.f, id, buf); err != nil {
		return d.fail(err)
	}
	if err := d.f.Sync(); err != nil {
		return d.fail(fmt.Errorf("syncing freed page %d: %w", id, err))
	}
	next := d.hdr
	next.freeHead = id
	next.freeCount++
	return d.commitHeader(next)
}

// Sync makes all completed WritePage calls durable. If it fails, the manager
// is marked failed: the data may or may not be on disk, and retrying fsync
// cannot be trusted.
func (d *DiskManager) Sync(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(ctx); err != nil {
		return err
	}
	if err := d.f.Sync(); err != nil {
		return d.fail(fmt.Errorf("syncing data file: %w", err))
	}
	return nil
}

// PageCount returns the number of pages in the file, including the header
// pages.
func (d *DiskManager) PageCount() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.hdr.pageCount
}

// FreePageCount returns the number of pages on the free list.
func (d *DiskManager) FreePageCount() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.hdr.freeCount
}

// Close syncs (unless the manager has failed) and closes the file.
func (d *DiskManager) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDiskClosed
	}
	d.closed = true
	var syncErr error
	if !d.failed.Load() {
		if syncErr = d.f.Sync(); syncErr != nil {
			syncErr = fmt.Errorf("syncing on close: %w", syncErr)
		}
	}
	return errors.Join(syncErr, d.f.Close())
}
