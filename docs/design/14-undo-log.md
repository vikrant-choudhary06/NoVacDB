# 14 — Undo Log (`internal/undo`, `internal/wal`)

Status: **Step 6.2 (undo log) designed, approved and implemented** (notes in section 2.8).

## 1. Problem

NoVacDB's answer to PostgreSQL's table bloat is to change rows **in place** and keep their old versions somewhere else, the undo log, until no reader needs them (PROGRESS.md, Phase 6). Step 6.2 builds that "somewhere else" on its own, before anything uses it:

- **Undo pages and undo segments**, one segment per transaction.
- **An undo record format.** Each record holds the transaction ID, the operation (insert, update, delete), the table and row ID, the row's previous image, and pointers to the previous record for the same row and for the same transaction.
- **Every undo write is WAL-logged**, and undo pages survive crashes like any other page.

Out of scope, for later steps:
- writing undo for heap changes, and the row header that points to it (Step 6.3);
- reading old versions for snapshots (Step 6.4);
- applying undo to roll back (Steps 6.5 and 6.9);
- freeing undo nobody needs (Step 6.8).

Step 6.2 gives them an API to build on, and its tests use only that API.

## 2. Design

### 2.1 Where undo lives

Undo pages live **in the data file**, as a new page type, `PageTypeUndo` (6). They go through the buffer pool and the WAL like heap and B+Tree pages, and they inherit everything those already have:

- checksums;
- crash recovery, including full page images after each checkpoint;
- "no steal" while their transaction is open (13-transactions.md section 2.6);
- page allocation from the free list, and deferred freeing.

A separate undo file would need its own allocation, checkpoints and recovery, which is all duplicated machinery for no gain at this size.

### 2.2 Undo pointers

An **undo pointer** (`undo.Ptr`, u64) is the byte address of a record in the data file: `page ID × 8192 + offset in the page`.

- `0` means "none". Page 0 is the file header, so it never holds undo.
- A pointer names its page and offset with no lookup.
- It fits in the 8 bytes that Step 6.3's row header will give it.

### 2.3 Segments

A transaction gets an **undo segment** at its first undo record. A segment is a chain of undo pages linked forward (each page names the next). Records are appended to the last page; a new page is linked on when the last one is full.

- **Within a transaction, records chain backwards** through `PrevInTxn`. Rollback (Step 6.5) starts from the transaction's last record and follows that chain.
- **Records of the same row chain backwards** through `PrevForRow` (the row's undo pointer before this change), whichever transactions wrote them. Readers (Step 6.4) follow that chain to the version their snapshot sees.
- **A segment ends when its pages are released:** all its pages are freed through the existing deferred-free mechanism (08-btree.md section 2.7), so a page a reader still holds is not reused under it.
  - **Who releases them:** purge (Step 6.8) for committed transactions, rollback (Step 6.5) for aborted ones.
  - **Until those steps exist:** nothing writes undo, so Step 6.2 only provides `Release` and tests it.

### 2.4 Finding segments after a crash: the segment table

Rollback (Step 6.9) and purge (Step 6.8) must find every segment that still exists after a crash. The engine keeps a **segment table** in memory: transaction ID → first page. It is made durable with the same pattern that already makes deferred frees durable (08-btree.md section 2.7):

- **Creating a segment** logs an `UndoSegment` record naming the transaction and its first page, right after the log record holding that page's image (section 2.8), so the table never names a page recovery has no image of.
- **Releasing a segment** logs an `UndoSegment` record that drops it, then the deferred free of its pages. A crash between the two leaks the pages; the other order could free pages the table still names.
- **Every checkpoint logs the whole table again** after its redo point, so recovery from that redo point sees every live segment without the log before it.
- **Recovery rebuilds the table** from these records.

The end of a segment (its last page and the next free offset there) is not logged in the table. It is found by following the segment's page chain and reading the last page's header, which recovery has already brought up to date.

### 2.5 Writing and reading records

```go
package undo

type Ptr uint64
type Kind uint8 // Insert, Update, Delete

type Record struct {
	Kind       Kind
	XID        uint64 // a wal.XID (section 2.8)
	PrevForRow Ptr    // the row's previous undo record, 0 if none
	PrevInTxn  Ptr    // the transaction's previous record, 0 if first
	Table      uint64 // catalog table ID
	RID        storage.RID
	Image      []byte // the row before the change; empty for Insert
}

func (l *Log) Append(ctx context.Context, rec Record) (Ptr, error) // in rec.XID's segment, created if needed
func (l *Log) Read(ctx context.Context, p Ptr) (Record, error)
func (l *Log) Last(xid uint64) Ptr                              // the transaction's latest record
func (l *Log) Pages(xid uint64) []uint64                        // the segment's pages, in chain order
func (l *Log) Release(ctx context.Context, xid uint64) error
func (l *Log) Segments() []uint64
```

`wal.Engine.Undo()` returns the engine's log. Its writes belong inside a transaction's `Write`, so that an uncommitted transaction's undo is discarded with its other changes.

- **`Append`** sets `PrevInTxn` itself, from the segment's last record, so callers cannot get that chain wrong.
- **`Read`** validates everything it decodes. A bad length, kind or pointer is `ErrCorrupt`, never a panic.
- **Logging:** each write logs one WAL record, `Undo` (section 3.3). The record carries the bytes appended; or, when a new page is needed, the new page's image (holding the record) and the link to it from the segment's last page. As with heap pages, the first change to an undo page after a checkpoint logs its full image instead.

### 2.6 Records never span pages; the maximum row size

A record holds the whole previous row image, which must fit in one undo page with its header (section 3). The largest row is one heap tuple, 8148 bytes, and that does not fit.

**Decision:** Step 6.3, which starts writing undo for rows, lowers the maximum encoded row to **8000 bytes** (from 8148). Every undo record then fits in one page. Step 6.3 also adds a row header, which makes rows a little smaller still. Rows between 8001 and 8148 bytes become `54000` "row is too big". Until then, `Append` refuses an image over `MaxImage` (8090 bytes) with an error.

The alternative, records that continue across pages, costs a continuation format, partial records during recovery, and two page latches to read one record. The 148 bytes it would save are not worth that.

### 2.7 Full images, not changed columns

A record holds the row's whole previous image. A delta of only the changed columns would write less undo for wide rows with small updates, but it would need a column-delta format and decoding against the current row. The record's `Flags` byte is reserved for such a delta. Step 6.11 measures write amplification, and that measurement decides whether a delta is worth adding.

### 2.8 Implementation notes (Step 6.2)

- **A new page is logged as an image holding its first record.** The design's separate "init" operation is gone.
  - Growing a segment is one `Undo` record with two blocks: the link from the last page (or that page's image, if it is its first change since the redo point) and the new page's image. The two changes are atomic in the log.
  - No undo page is ever empty, and recovery refuses one that is.
  - The operations are numbered 1 image, 2 append, 3 set-next. An append carries its length, because a record may hold two blocks.
- **The undo package takes a transaction ID as `uint64`, not `wal.XID`.** `wal` imports `undo` for replay, so `undo` cannot import `wal`. It has its own `Logger` interface (`LogUndo`, `LogUndoSegment`, `DeferFree`), which `wal.Logger` implements, as `storage` and `btree` already do.
- **A segment's `add` is logged after its first page's image.** The other order, add first, could leave after a crash a table entry naming a page whose image never reached the log. Now a crash between the two leaks one page.
- **A `drop` of a segment not in the table is ignored in replay, not refused.** A checkpoint's redo point is set first, and the table is logged again afterwards. A release logged in between is after the redo point, but the segment is already gone from the table the checkpoint logs. Replay from that redo point then sees the drop and no add before it. Inside the engine this cannot happen, since checkpoints wait for the writing transaction, but the undo log does not rely on that. A `drop` of a segment that *is* in the table must name its first page.
- **Validation is complete on every read.** `Read` checks the whole page, not only the record it is asked for: every record must decode and belong to the page's transaction, the records must end exactly at `Used`, and the bytes after them must be zero. A pointer must land on a record boundary, so one into the middle of a record or an image is `ErrCorrupt`, even if the bytes there happen to look like a record.
- **`ErrImageTooLarge`** is the error for an image over `MaxImage`. Other invalid records are `ErrInvalidRecord`. Neither logs anything.
- **Deliberate bugs, as planned (section 7):** 33, run four at a time on copies of the code, each against its package's tests with a 60-second limit; the whole run took about 1.5 minutes.
  - The first run caught 29.
  - Three of the 4 survivors were test gaps: a too-long record length with bytes after it, an undo page labelled as another type, and a well-formed three-block record. New test cases now catch them.
  - The fourth was a duplicate check. The image limit was checked both in decoding and in the shared field validation; decoding now relies on the shared one, and the bug is caught.

## 3. Formats (approved)

These are on-disk format changes. The phase's rules approve one only for Step 6.3; the maintainer approved this one, as written here with section 2.8's changes to `Undo` records.

- **Data file format version 3:** page type 6 (undo). Files of version 2 are refused with the existing "unsupported format version" error. There is no production data to migrate.
- **WAL format version 3:** record types 10 and 11 (section 3.2). Version-2 logs are refused, as before.

### 3.1 Undo page (8192 bytes, little-endian)

```
offset  size   field
0       24     page header (PageType = Undo, 6)
24      8      XID          the owning transaction
32      8      NextPage     next page of the segment, 0 = last
40      2      Used         end of the records, from offset 48; 48 when empty
42      6      reserved     zero
48      ...    records, back to back
```

The payload after the header is 8192 − 48 = 8144 bytes.

### 3.2 Undo record

```
offset  size   field
0       2      Length       of the whole record, header included
2       1      Kind         1 insert, 2 update, 3 delete
3       1      Flags        0 (reserved for a column delta)
4       4      reserved     zero
8       8      XID
16      8      PrevForRow   undo pointer, 0 = none
24      8      PrevInTxn    undo pointer, 0 = first in the transaction
32      8      Table        catalog table ID
40      8      RID page
48      2      RID slot
50      4      ImageLength
54      n      Image        n = ImageLength; 0 for insert
```

Header 54 bytes; the largest image that fits is 8144 − 54 = **8090** (`MaxImage`).

**Validation on read:**
- the length must equal 54 plus the image length, and stay inside the page's used area;
- the kind must be 1–3, and the flags and reserved bytes zero;
- an insert's image must be empty, and an update's or delete's must not be;
- the XID must match the page's;
- a pointer must name an undo page and an offset at or after 48.

### 3.3 WAL records

**Type 10, `Undo`:** one change to one or two undo pages (section 2.8), in the heap record's block layout.

```
offset  size   field
0       1      BlockCount   1 or 2
1       ...    blocks, back to back

block:
0       8      PageID
8       1      Op           1 image, 2 append, 3 set-next
9       ...    image:     8192 bytes
               append:    u16 offset, u16 length, the record
               set-next:  u64 next page ID
```

Redo installs an image whatever the page holds. It applies an append or a link only if the page's LSN is older than the record's, as heap redo does, and only if it fits exactly: an append must land at the page's `Used` and belong to the page's transaction, and a link must be the page's first.

**Type 11, `UndoSegment`:** changes to the segment table.

```
offset  size   field
0       2      Count
2       ...    Count entries of 17 bytes: u8 op (1 add, 2 drop), u64 XID, u64 first page
```

A checkpoint logs the table again with `add` entries, at most 400 to a record. Replay applies the entries in order. An `add` of a segment already in the table must name the same first page. A `drop` of a segment not in the table is ignored (section 2.8). After replay, recovery follows each segment's pages to find its end, and refuses a page that is not an undo page of the segment's transaction, or one that appears twice.

## 4. Concurrency

- **Writing undo:** in Step 6.1's interim rule, one transaction writes at a time, so its writes to its own segment are serial. The segment table has its own mutex.
- **Reading undo:** readers latch undo pages shared through the buffer pool, like any page.
- **Freeing pages:** deferred freeing keeps a page from being reused while a reader may still follow a pointer into it.

## 5. Failure behaviour

| Event | Result |
|---|---|
| Crash with a transaction's undo written | Recovery replays committed transactions' undo pages; an uncommitted transaction's undo is discarded with its other records (no steal), and its `UndoSegment` add is discarded too, so no segment is left behind. |
| Crash after a segment's release record | The segment is gone from the table; its pages are freed by the deferred-free records, as for dropped tables. |
| A damaged undo page or record | `Read` returns `ErrCorrupt`; a bad `Undo` WAL record stops recovery with `ErrCorrupt`. |
| An image too large | `Append` refuses it (section 2.6). |
| A pointer to a page that is not an undo page | `ErrCorrupt`. |

## 6. Alternatives considered

- **A separate undo file.** Its own allocator, checkpoints and recovery would duplicate the data file's (section 2.1).
- **A persistent segment directory page.** It needs its own page format and crash rules. Re-logging the table at checkpoints reuses a pattern that is already crash-tested.
- **Records that span pages.** Section 2.6.
- **Column deltas.** Section 2.7.
- **One shared undo log instead of per-transaction segments.** Freeing then has to work out which ranges are still needed. A segment per transaction is freed whole, and rollback walks only its own pages.

## 7. Testing plan

- **Format:** golden bytes of an undo page and a record; every truncation and every invalid field refused; `FuzzUndoRecord`, in which any decoded record re-encodes to the same bytes and nothing panics.
- **Round trips:** records of every kind and size, up to `MaxImage`, across many pages of one segment. `PrevInTxn` chains link every record of a transaction in reverse order.
- **Model test for undo chains:** many transactions, each changing random rows of several tables, with a model of every row's version history. Walking `PrevForRow` from each row's latest record yields exactly its history, newest first.
- **Crash tests:** segments and records written by committed transactions are identical after MemFS power cuts, torn writes and checkpoints at random points. An uncommitted transaction leaves no segment and no records. A released segment's pages return to the free list after a crash. The segment table survives log trimming, through the re-logging at checkpoints.
- **Deliberate bugs, faster than in Step 6.1.** About 25–30 bugs, placed in recovery, chain and crash-path logic. Each runs only its own package's relevant tests, with a 60-second timeout, on a copy of the code, four at a time.

## 8. Limitations (Step 6.2)

- Nothing writes undo yet (Step 6.3), reads it for snapshots (6.4), or applies it (6.5, 6.9).
- Committed transactions' segments are released only by purge (Step 6.8). Until then, the steps that write undo say how long it is kept.
- Full images only (section 2.7).
