package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

// pageFlusher writes every dirty page and frees pages (storage.BufferPool).
type pageFlusher interface {
	FlushAll(ctx context.Context) error
	DeletePage(ctx context.Context, id uint64) error
}

// dataSyncer makes written pages durable (storage.DiskManager).
type dataSyncer interface {
	Sync(ctx context.Context) error
}

// checkpointPayloadSize is the size of a checkpoint record's payload: the
// redo LSN, then the next transaction ID, uint64 little-endian each.
const checkpointPayloadSize = 16

// Checkpointer takes checkpoints: it bounds how much log recovery must
// replay and lets old segments be deleted. See
// docs/design/07-checkpoints-recovery.md, section 2.4.
type Checkpointer struct {
	fsys  vfs.FS
	dir   string // where the control file lives
	w     *Writer
	lg    *Logger
	pages pageFlusher
	data  dataSyncer

	mu sync.Mutex // one checkpoint at a time
	// nextXID, if set (by the Engine), gives the next transaction ID the
	// checkpoint record carries; without it the record carries 0.
	nextXID func() XID
	// relogUndo, if set (by the Engine), logs the undo segment table again
	// after the redo point (docs/design/14-undo-log.md section 2.4).
	relogUndo func(ctx context.Context) error
}

// NewCheckpointer returns a Checkpointer for the log w (with its Logger lg),
// the buffer pool pages and the data file data. The control file is kept in
// dir.
func NewCheckpointer(fsys vfs.FS, dir string, w *Writer, lg *Logger, pages pageFlusher, data dataSyncer) *Checkpointer {
	return &Checkpointer{fsys: fsys, dir: dir, w: w, lg: lg, pages: pages, data: data}
}

// Checkpoint takes a checkpoint and returns what it recorded. Normal work
// may continue while it runs. If it fails, the previous checkpoint remains
// the one recovery uses; moving the redo point forward early only makes more
// changes log full page images, which is harmless.
func (c *Checkpointer) Checkpoint(ctx context.Context) (Control, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 1. From here on, the first change to any page logs an image.
	redo := c.lg.BeginCheckpoint()
	// Pages waiting to be freed that this checkpoint will not free are
	// logged again after the redo point, so that recovery from it still
	// knows them (docs/design/08-btree.md section 2.7). Those it will free
	// are not: once its control file exists, nothing may name them again.
	if err := c.relogWaiting(ctx, redo); err != nil {
		return Control{}, fmt.Errorf("checkpoint: %w", err)
	}
	// Likewise the undo segment table, so that recovery from the redo
	// point finds every segment.
	if c.relogUndo != nil {
		if err := c.relogUndo(ctx); err != nil {
			return Control{}, fmt.Errorf("checkpoint: %w", err)
		}
	}
	// 2. Write every page that was dirty before the redo point (forcing the
	// log as needed), and 3. make the writes durable.
	if err := c.pages.FlushAll(ctx); err != nil {
		return Control{}, fmt.Errorf("checkpoint: flushing pages: %w", err)
	}
	if err := c.data.Sync(ctx); err != nil {
		return Control{}, fmt.Errorf("checkpoint: syncing data: %w", err)
	}
	// 4. The checkpoint record, durable.
	var next XID
	if c.nextXID != nil {
		next = c.nextXID()
	}
	payload := binary.LittleEndian.AppendUint64(nil, uint64(redo))
	payload = binary.LittleEndian.AppendUint64(payload, uint64(next))
	ckpt, err := c.w.Append(ctx, RecordCheckpoint, payload)
	if err != nil {
		return Control{}, fmt.Errorf("checkpoint: logging: %w", err)
	}
	if err := c.w.FlushTo(ctx, ckpt); err != nil {
		return Control{}, fmt.Errorf("checkpoint: flushing the log: %w", err)
	}
	// 5. Publish it. Only now does recovery start from the new redo point.
	ctl := Control{CheckpointLSN: ckpt, RedoLSN: redo}
	if err := WriteControl(c.fsys, c.dir, ctl); err != nil {
		return Control{}, fmt.Errorf("checkpoint: %w", err)
	}
	// 6. Pages unlinked by records before the redo point can be freed: no
	// record that refers to them will ever be replayed.
	if err := c.freeDeferred(ctx, redo); err != nil {
		return ctl, fmt.Errorf("checkpoint: %w", err)
	}
	// 7. Log before the redo point is no longer needed.
	if _, err := c.w.RemoveSegmentsBefore(ctx, redo); err != nil {
		return ctl, fmt.Errorf("checkpoint: removing old log: %w", err)
	}
	return ctl, nil
}

// freeDeferred frees the deferred pages whose record lies before redo. A
// page still pinned (a reader that had just let go of it) and the pages after
// a failure wait for the next checkpoint.
func (c *Checkpointer) freeDeferred(ctx context.Context, redo LSN) error {
	pending := c.lg.takeFreeable(redo)
	for i, d := range pending {
		err := c.pages.DeletePage(ctx, d.page)
		if errors.Is(err, storage.ErrPagePinned) {
			c.lg.restore(d.page, d.lsn)
			continue
		}
		if err != nil {
			for _, rest := range pending[i:] {
				c.lg.restore(rest.page, rest.lsn)
			}
			return fmt.Errorf("freeing page %d: %w", d.page, err)
		}
	}
	return nil
}

// relogWaiting logs again, with their thresholds, the deferred pages that
// a checkpoint at redo will not free.
func (c *Checkpointer) relogWaiting(ctx context.Context, redo LSN) error {
	waiting := c.lg.waiting(redo)
	for len(waiting) > 0 {
		n := min(len(waiting), MaxDeferredPerRecord)
		pages, lsns := make([]uint64, n), make([]uint64, n)
		for i, d := range waiting[:n] {
			pages[i], lsns[i] = d.page, d.lsn
		}
		if _, err := c.w.Append(ctx, RecordDeferredFree, EncodeDeferredFree(pages, lsns)); err != nil {
			return fmt.Errorf("logging deferred frees again: %w", err)
		}
		waiting = waiting[n:]
	}
	return nil
}

// RedoStart returns the LSN recovery must replay from, using the control file
// in dir and the log in walDir (which must already have been opened, so its
// tail is repaired). With a control file, the checkpoint record it names must
// exist and carry the same redo LSN. Without one, replay starts at the first
// record, which is only allowed if the log has never been trimmed. Anything
// else is ErrCorrupt.
func RedoStart(fsys vfs.FS, dir, walDir string) (LSN, error) {
	ctl, ok, err := ReadControl(fsys, dir)
	if err != nil {
		return 0, err
	}
	if !ok {
		first, err := FirstLSN(fsys, walDir)
		if err != nil {
			return 0, fmt.Errorf("finding redo start: %w", err)
		}
		if first != SegmentHeaderSize {
			return 0, fmt.Errorf("log starts at %d but there is no control file: %w", first, ErrCorrupt)
		}
		return first, nil
	}
	r, err := NewReader(fsys, walDir, ctl.CheckpointLSN)
	if err != nil {
		if errors.Is(err, ErrLSNNotFound) {
			return 0, fmt.Errorf("control file names checkpoint %d, which is not in the log: %w: %w", ctl.CheckpointLSN, ErrCorrupt, err)
		}
		return 0, err
	}
	rec, err := r.Next()
	if errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("control file names checkpoint %d at the end of the log: %w", ctl.CheckpointLSN, ErrCorrupt)
	}
	if err != nil {
		return 0, err
	}
	if rec.Type != RecordCheckpoint || len(rec.Payload) != checkpointPayloadSize ||
		LSN(binary.LittleEndian.Uint64(rec.Payload)) != ctl.RedoLSN {
		return 0, fmt.Errorf("record %d is not the checkpoint the control file describes: %w", rec.LSN, ErrCorrupt)
	}
	return ctl.RedoLSN, nil
}
