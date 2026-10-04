# 07 — Logging, Checkpoints and Crash Recovery

Status: **Implemented: logging in Step 2.3, checkpoints in Step 2.4, recovery, engine and crash harness in Step 2.5.** Written and approved under the standing autonomous-mode instruction. 
This document covers the rest of Phase 2, because the three parts depend on each other: what a log record contains (2.3) is decided by what recovery needs (2.5), and what recovery needs is decided by what a checkpoint guarantees (2.4).

## 1. Problem

Heap pages change in the buffer pool and reach the data file later, in any order, possibly torn by a crash. Without help, a crash can leave the data file with half an operation (a row moved off one page but not yet onto another), with changes that were acknowledged but never written, or with a page that is half old and half new.

The goals for Phase 2 (there are no transactions until Phase 6, so **each heap operation is the unit of atomicity**):

1. Every heap change is described by a WAL record before any page containing it can reach disk (the WAL rule).
2. After a crash, every change whose record was durable is present, and no change is partially visible: each heap operation is either entirely there or entirely absent.
3. A torn data page is repaired, not just detected.
4. Recovery replays only the log written since the last checkpoint, and older log segments are deleted.

## 2. Design

### 2.1 Physiological records, one per operation (Step 2.3)

A heap WAL record describes one heap operation as a list of **page blocks**, one for each page the operation changed (one or two). Each block names a page and says either:

- **image**: the full 8 KiB content of the page after the change; or
- an **operation** on that page: insert tuple at slot *s*, update slot *s*, delete slot *s*, or set the next-page link.

Operations are applied to the page's slot directory, not to byte offsets ("physiological" logging). Replaying them reproduces the page exactly, because the slotted-page code is deterministic: the same page plus the same operation gives the same bytes. Redo checks that an insert lands in the slot the record names and fails with `ErrCorrupt` otherwise.

Operations that touch two pages are **one record**:

- **growing the heap**: the new, empty page (an image, see 2.2) and the tail's next-page link;
- **an update that moves a row**: delete at the old page and insert at the new page.

A single record is atomic under the WAL rule. If it is durable, recovery replays every block. If it is not, no page carrying its effects can have reached disk, because each changed page carries the record's LSN. (Step 1.4 implemented the move as insert-then-delete. Without a log that could duplicate a row after a crash; with the log it cannot.)

Logged heaps therefore need at least three buffer frames: a multi-page operation (up to three pages since Step 6.3, 15-row-versioning.md section 2.2) pins and exclusively latches all its pages, always in ascending page-ID order so two operations can never deadlock. Unlogged heaps (no logger, the Step 1.4 behaviour kept for tests) are unchanged.

**Order of work for one operation**, done while holding the exclusive latch of every page involved:

1. Copy each page (for rollback).
2. Apply the change in memory. If it is impossible (a full page, a dead slot), nothing has changed and nothing is logged.
3. Append the record. If the append fails, copy the pages back and fail the operation. The pool must never hold an unlogged change, because the old LSN would let it be written.
4. Stamp every changed page with the record's LSN, then mark it dirty.

The LSN is the record's **start** LSN, exactly what `wal.Writer.FlushedLSN` and the buffer pool's `pageLSN <= FlushedLSN()` rule expect (06-wal.md).

### 2.2 Torn data pages: full-page images after a checkpoint starts

Redoing an operation needs a correct page to apply it to, but the on-disk copy may be torn. As PostgreSQL does, **the first change to a page after a checkpoint starts logs an image of the whole page** instead of the operation:

> A block is logged as an image when the page's LSN before the change is lower than the current **redo point** (the redo LSN of the latest checkpoint that has *started*).

This is sufficient. Recovery replays from the redo point *R* of the last *completed* checkpoint. Take any page *P* changed after *R*, and its first change *m* after *R*:
- When *m* happened, the redo point was at least *R*, because the pointer only moves forward and was set to *R* before *m*.
- *P*'s LSN before *m* was below *R*, so *m* logged an image.

So during recovery the first record touching *P* is an image: *P* is rebuilt without ever reading its possibly torn disk copy.

A page not changed after *R* was flushed and synced by the checkpoint if it was dirty, and nothing wrote it afterwards (only dirty pages are written), so its disk copy is intact.

A new page has LSN 0, which is always below the redo point, so creating a page always logs its image. No separate "init" operation is needed.

**Race with a starting checkpoint.** Deciding "image or operation" and appending must see the same redo point. The `Logger` holds a read lock across both (`Log` calls a builder with the redo point, then appends). Starting a checkpoint takes the write lock just long enough to set the redo point to the log's current end. So every record appended earlier has a lower LSN, and every later record saw the new redo point.

### 2.3 Buffer pool support

- **Forcing the WAL.** With logging, most dirty pages carry LSNs beyond what is durable, and Step 1.3's pool would refuse to evict them (`ErrNoFreeFrames`). A new optional hook `Options.FlushWAL(ctx, lsn)` lets the pool force the log. When every candidate for eviction is blocked only by the WAL rule, it calls the hook and sweeps again. `FlushPage` and `FlushAll` call it instead of returning `ErrWALRule`. Without the hook the Step 1.3 behaviour is unchanged.
- **`PinForOverwrite(ctx, id)`** pins a page *without reading it*, a zeroed frame if the page is not in memory. Redo uses it to install an image over a page whose disk copy may be torn.

### 2.4 Checkpoints (Step 2.4)

A **control file** `control` (section 3) in the database directory records the last completed checkpoint: the LSN of its checkpoint record, and its redo LSN. It is replaced atomically: write `control.tmp`, fsync it, rename it over `control`, fsync the directory.

`Checkpoint(ctx)`:

1. Under the `Logger`'s write lock: redo point ← `Writer.EndLSN()`. Then (since 08-btree.md revision 2) every page still waiting to be freed whose threshold is at or after the redo point is logged again in deferred-free records (type 6), so the log from the redo point names all of them.
2. `BufferPool.FlushAll`: every page dirty before the redo point is written, forcing the log as needed through the hook. Pages dirtied meanwhile may be written too; that is harmless.
3. `DiskManager.Sync`: those writes are durable.
4. Append a checkpoint record (payload: the redo LSN) and flush the log up to it.
5. Write the control file.
6. Free the pages waiting with a threshold before the redo LSN (pages B+Trees unlinked, and dropped tables' and indexes'; 08-btree.md section 2.7): no record that refers to them can be replayed any more.
7. Delete log segments that end at or before the redo LSN (never the current segment), and fsync the directory.

A crash at any point leaves the previous control file in force. Its redo point is still in the log, because segments are deleted only after step 5. Only one checkpoint runs at a time.

**Implementation (Step 2.4):** `wal.Checkpointer` (steps 1–6 above), `WriteControl`/`ReadControl` (temp file, fsync, rename, fsync directory), `Writer.RemoveSegmentsBefore` (oldest first, directory fsynced after each removal, never the segment being written), and `RedoStart`. `RedoStart` finds the replay start: the control file's redo LSN after checking that the checkpoint record it names exists, is a checkpoint record, and carries that redo LSN; or the first record of an untrimmed log when there is no control file. The `Engine` that strings these together with replay comes in Step 2.5; until then, tests assemble the parts by hand.

### 2.5 Recovery (Step 2.5)

`wal.OpenEngine(ctx, fsys, dir, opts)` opens or creates a database in `dir` (`dir/data`, `dir/wal/`, `dir/control`):

1. Open (or create) the data file and the log. Opening the log already cuts off a torn tail (06-wal.md).
2. Find the redo LSN:
   - If the control file exists, read the checkpoint record it names and require it to be a checkpoint record carrying the same redo LSN; otherwise `ErrCorrupt`.
   - If there is no control file, replay from the first record. That is only allowed if the log still starts at LSN 0; a trimmed log without a control file is `ErrCorrupt`.
3. Create the buffer pool with both WAL hooks, and a `Logger` whose redo point is that redo LSN.
4. **Redo:** read every record from the redo LSN to the end (`wal.Reader`). Heap records go to `storage.RedoHeapRecord`, checkpoint records are skipped, and anything else is `ErrCorrupt`. (Since Step 4.4 a first pass finds statement groups that never committed, and their records are skipped too; see 10-executor.md section 2.4. Deferred-free records put their pages back on the list of pages to free, which the end-of-recovery checkpoint then frees; a record naming a page past the end of the data file is `ErrCorrupt`. See 08-btree.md section 2.7.) Each block is redone:
   - An image is installed unconditionally (via `PinForOverwrite`) and stamped with the record's LSN.
   - An operation is applied only if the page's LSN is lower than the record's (otherwise the page already contains it), then stamped.
   - Replay is sequential and in LSN order, so replaying again after a crash during recovery gives the same result.
5. The reader must end exactly where the writer's end is; anything else is `ErrCorrupt`.
6. An **end-of-recovery checkpoint** flushes the redone pages and moves the redo point forward.

There is no undo phase: without transactions every logged operation is complete. Phase 6 adds undo.

Allocations in the data file are made durable by the disk manager itself before the record that uses the page is logged. A crash between the two leaks the page (allocated, never linked) but never double-uses it. A logged operation whose append fails never frees a page it allocated: the record's bytes might still reach the log, and recovery would then use the page.

### 2.6 Engine

`wal.Engine` ties the pieces together for the crash harness and for later phases. Its methods:

- `CreateHeap` and `OpenHeap` return logged heaps.
- `Flush(ctx)` makes everything done so far durable. This is the acknowledgement point; later, `COMMIT`.
- `Checkpoint(ctx)`.
- `Close(ctx)`: a checkpoint, then close.

It does not remember which heaps exist; that is the catalog (Step 4.4). Its callers keep heap first-page IDs.

### 2.7 Crash harness (Step 2.5)

`tests/crash/` contains:

- **`MemFS` crash tests.** Seeded random heap operations on several heaps, acknowledgements with `Flush`, checkpoints, injected I/O errors, and crashes with or without a torn last write, over several crash cycles. After each recovery the heap contents must equal the model state after *some* number of operations between the last acknowledgement and the crash. That number must be at least the acknowledged count, so the result is a *prefix* of operations: atomic and in order. A failing seed is printed and reproducible with `NOVACDB_SEED`.
- **An OS-level kill test.** The test binary re-executes itself as a worker on a real directory and prints a line after each acknowledged flush. The parent kills it with SIGKILL after a random number of acknowledgements (synchronised on that output, not on time), then recovers and checks the same prefix rule.

`make crashtest` runs both with more iterations.

### 2.8 Implementation notes (Step 2.3)

- **A page is marked dirty *before* its record is appended (fixed in Step 2.4).** Step 2.3 first marked a changed page dirty only when it was unpinned, after the append. A checkpoint that began in between moved the redo point past the record, but its flush pass did not see the page as dirty and skipped it. Recovery then started after the record, so the change was lost if nothing touched the page again. The concurrent checkpoint-and-crash test found it, failing about once in 50 race-detector runs. Logged operations now call `PageRef.MarkDirty` while holding the exclusive latch, before the append, as PostgreSQL does. Any record below a checkpoint's redo point therefore belongs to a page that is already dirty when the checkpoint snapshots the dirty set. `TestPagesAreDirtyBeforeTheirRecordIsAppended` pins it, and the concurrent test then passed 100 race runs.

- **Bug found and fixed in the pool's flush path.** As first written, `FlushPage` forced the log for the page's LSN, then took a *fresh* copy of the page and checked again. Under concurrent writers the page had changed again by then, and the flush failed with `ErrWALRule`; the concurrent-checkpoint test caught it. The pool now writes the copy it already took, after forcing the log up to *that copy's* LSN. Any later change re-dirties the page, the same copy-time rule as before. There is no retry loop. `TestFlushWhilePageChangesDuringLogForce` reproduces the interleaving deterministically.
- **The LSN check on operations** (skip if the page's LSN is not below the record's) is never needed in a correct full or from-redo-point replay, because each page's first record after the redo point is an image. It stays as the standard safety net, and is tested directly.
- Logged heaps are opt-in (`WithLogger`); the unlogged Step 1.4 paths are unchanged, so their tests (including one-frame pools) still apply.

### 2.9 Implementation notes (Step 2.5)

- `wal.Engine` and `wal.OpenEngine` implement section 2.5 as designed, with `Recovery()` reporting where replay started and how many records it replayed. The data file is created before the log, so a log with no data file is reported as `ErrCorrupt`, not silently recreated.
- The `Logger`'s initial redo point does not matter in practice: the end-of-recovery checkpoint resets it before any heap is handed out. A deliberate bug that started it at 0 was therefore indistinguishable from correct code.
- The harness compares rows by an 8-byte key stored in each tuple, never by RID, and reads the RIDs back after each recovery, so the oracle does not depend on where recovery places rows. (Redo reproduces slots exactly anyway, and redo checks this.)

## 3. Formats

All integers little-endian.

**WAL record types** (`wal.RecordType`): `1` heap operation, `2` checkpoint, `3` B+Tree change (format in 08-btree.md), `4` statement begin and `5` statement commit (format and meaning in 10-executor.md, sections 2.4 and 3), `6` deferred free (format in 08-btree.md section 3).

**Heap record payload** (the WAL record's payload, type 1):

| offset | size | field | notes |
|---|---|---|---|
| 0 | 1 | BlockCount | 1 to 3 (1 or 2 before WAL format 4) |
| 1 | … | blocks | back to back |

Block:

| offset | size | field | notes |
|---|---|---|---|
| 0 | 8 | PageID | |
| 8 | 1 | Kind | 1 image, 2 insert, 3 update, 4 delete, 5 set-next |
| 9 | … | data | depends on Kind |

| Kind | data |
|---|---|
| image (1) | 8192 bytes: the page after the change (its LSN and CRC fields are ignored; redo stamps the LSN) |
| insert (2) | u16 slot, u32 length, tuple bytes |
| update (3) | u16 slot, u32 length, tuple bytes |
| delete (4) | u16 slot |
| set-next (5) | u64 next page ID |

A block's page ID must be at least 2, blocks must name different pages, the tuple length must be 1..`MaxTupleSize`, and nothing may follow the last block.

**Checkpoint record payload** (type 2): u64 redo LSN.

**Control file** `control` (32 bytes):

| offset | size | field | notes |
|---|---|---|---|
| 0 | 4 | CRC-32C | over bytes 4..32 |
| 4 | 8 | Magic | `"NOVACTL\0"` |
| 12 | 4 | FormatVersion | 1 |
| 16 | 8 | CheckpointLSN | LSN of the checkpoint record |
| 24 | 8 | RedoLSN | where recovery starts |

## 4. Concurrency

- Page latches as before (05-heap-storage.md). Two-page operations take both exclusive latches in ascending page-ID order. Growing the heap holds `growMu`, then the two latches.
- `Logger`: a read/write lock. `Log` holds the read lock across building the record and appending it. Starting a checkpoint holds the write lock only to set the redo point, never while waiting for a latch or the buffer pool.
- The pool calls `FlushWAL` while holding its own mutex. That is safe, because the log never waits for the pool.
- One checkpoint at a time (`ckMu`). Normal operations continue during a checkpoint's `FlushAll`.
- Recovery runs before the engine is handed to anyone, so it is single-threaded.

## 5. Failure and crash behaviour

| Event | Result |
|---|---|
| Crash before an operation's record is durable | The operation is absent after recovery: no page with its LSN can be on disk. |
| Crash after it is durable | Present after recovery. |
| Two-page operation | Both halves or neither. |
| Torn data page | Rebuilt from the image logged after the redo point. |
| Crash during a checkpoint | The previous checkpoint is used; its log is still there. |
| Crash during recovery | Recovery runs again from the same redo point, with the same result. |
| Log append fails | The operation fails and its pages are restored in memory. The writer is now failed, so the engine stops accepting work until it is reopened. |
| Control file damaged or naming a missing or wrong record | `ErrCorrupt`; recovery refuses rather than guessing. |
| Unknown record type, malformed heap record, insert landing in the wrong slot | `ErrCorrupt`. |

## 6. Alternatives considered

- **Logical logging** ("insert this row into the table"). Redo would depend on the free-space map and page choice, which are not deterministic across a crash. Rejected.
- **Pure physical logging** (byte ranges). Larger records for compaction and fragile against layout changes. Physiological records are compact and checked.
- **Double-write buffer instead of full-page images** (InnoDB). Fixes torn pages without putting images in the log, which is what WORKFLOW problem #8 asks for, but adds a second write path and file. Full-page images are simpler and proven, so problem #8 stays open: a double-write area is the candidate fix once there are benchmarks.
- **Sharp checkpoints that stop all writes.** Simpler reasoning, but they pause the database; the redo-point rule makes a fuzzy checkpoint safe.
- **Moving a row as two records** (insert, then delete). Duplicates rows after a crash between them.
- **Logging the redo point in each record instead of a control file.** Recovery would have to search the log for the last checkpoint. A tiny control file replaced atomically is the standard approach.
- **Freeing a page when its operation's log append fails.** Unsafe, because the record may still become durable after a killed process; leaking is safe.

## 7. Testing plan

**Step 2.3 (logging):**
- Heap record codec: round trip for every block kind and combination; byte layout against golden bytes; every truncation and every invalid field rejected. `FuzzDecodeHeapRecord`.
- Image rule: with a fake logger, a change to a page whose LSN is below the redo point logs an image, and otherwise logs the operation. A new page always logs an image.
- **Replay equals reality:** random logged heap operations (with and without checkpoints moving the redo point) on a small pool. The records are then replayed with `RedoHeapRecord` onto a fresh data file with the same pages allocated. Every page must be byte-identical to the original, LSNs included. Replaying twice changes nothing. The same holds for concurrent writers.
- **WAL rule:** a store wrapper checks, at every page write, that the page's LSN is covered by the durable log. Random workloads with tiny pools (constant eviction) must produce zero violations and must actually exercise the `FlushWAL` hook.
- **Failure atomicity:** a failing logger (and a failing real writer) makes single- and two-page operations fail with the pages byte-for-byte unchanged and no pins held.
- Existing unlogged heap and pool tests unchanged.

**Step 2.4 (checkpoints):**
- Control file codec: round trip, golden bytes, every byte flip detected, invalid fields (size, magic, version, redo after checkpoint), `FuzzDecodeControl`.
- `WriteControl` atomicity: a fault at each step (create, write, sync, rename, directory sync), with and without an old file, crash with and without a torn write. The control file afterwards always holds exactly the old content or the new.
- Segment removal: exact boundaries, never the current segment, the log keeps working and reopens; faults on remove and directory sync part way, then a crash. What is left is always a contiguous tail that reads back exactly.
- **Acceptance:** random logged work across 1 to 4 checkpoints and random segment sizes and pool sizes, then a crash (torn or not). Recovery must start at the last checkpoint's redo point, replay exactly the heap records logged since then, and replay fewer than were ever logged. Old segments must be gone, and every page must equal the pre-crash database.
- A crash in the middle of a checkpoint (a fault at each of its I/O kinds, at several points, then a torn crash) recovers the same database from the previous checkpoint.
- A checkpoint makes the next change to an existing page log an image.
- Checkpoints running concurrently with several writers, then a crash and recovery: identical database (100 race-detector runs).
- `RedoStart` refuses: a missing checkpoint record, a control file naming a heap record, a redo mismatch, a checkpoint beyond the log, a lost control file after trimming, a damaged control file.

**Step 2.5 (recovery, engine, harness):**
- Engine: create, reopen after a clean close (nothing replayed), use after close; recovery after a crash with checkpoints rebuilds exactly the acknowledged rows; recovery ends with a checkpoint (a second crash with no new work replays nothing); refuses a lost data file, an unknown record type and a malformed heap record; an injected I/O fault at each point of `OpenEngine` followed by a torn crash never stops the next open from recovering.
- **`tests/crash` MemFS harness.** Seeded scenarios with random pool and segment sizes and several crash cycles each. Each cycle runs random inserts, updates, deletes, flushes and checkpoints on two heaps, sometimes with an injected I/O fault (write, sync, create, rename, remove, read, truncate). It ends with a torn power cut, a plain power cut or a killed process. After recovery the rows must equal the model **after some number of operations since the last acknowledged flush**, which proves atomicity, order and durability at once. The test also asserts that faults, lost unacknowledged work, checkpoints, torn crashes and process kills all actually happen.
- **`tests/crash` OS kill test.** A worker process (the test binary re-run) works on a real directory and prints an acknowledgement after each durable flush. The parent kills it with SIGKILL after a random number of acknowledgements (synchronised on that output), recovers, regenerates the same operation stream from the seed, and requires the recovered rows to equal its state after at least the acknowledged number of operations. A second reopen must see the same.
- `make crashtest` runs 3000 MemFS scenarios and 30 SIGKILL runs. A failure prints the seed and the environment variables that reproduce it.

## 8. Limitations

- No transactions or undo: the unit of atomicity is one heap operation (Phase 6).
- Full-page images make the log larger after each checkpoint (WORKFLOW problem #8 is not fixed).
- Pages leaked by a crash between allocating and logging are never reclaimed (needs a consistency checker).
- The engine does not know which heaps exist (catalog, Step 4.4).
- Logged heaps need at least three buffer frames, and the engine at least four (a row write also pins up to two undo pages).
