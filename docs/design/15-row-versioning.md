# 15 — Row Versioning (`internal/storage`, `internal/mvcc`, `internal/sql/executor`, `internal/catalog`)

Status: **Step 6.3 (row versioning) designed, approved and implemented** (notes in section 2.7).

## 1. Problem

This is the step where NoVacDB stops working like PostgreSQL. PostgreSQL writes every `UPDATE` as a new row version elsewhere in the table and leaves the old one behind for VACUUM. NoVacDB changes the row **where it is** and moves the old version to the undo log (14-undo-log.md), where it is cleaned up automatically.

Step 6.3 makes every write keep that history:

- **A row header** on every heap row: the transaction that last changed it, and an undo pointer to its previous version.
- **`UPDATE`** saves the old version to the undo log, then changes the row in place. A row that no longer fits on its page moves, and its old place keeps a **forwarding pointer**, so its row ID never changes.
- **`DELETE`** marks the row deleted and saves it to the undo log. The space is reclaimed by purge (Step 6.8).
- **`INSERT`** writes an undo record, so that rollback (Step 6.5) can remove the row.

Out of scope, for later steps:
- reading old versions for snapshots (6.4);
- applying undo to roll back (6.5 and 6.9);
- locks (6.6);
- keeping old index entries (6.7);
- purge (6.8).

Until then, nobody reads the history. Step 6.3 writes it correctly and proves it with tests.

## 2. Design

### 2.1 The row header

Every heap tuple starts with a **row header** of 18 bytes, then the encoded row (types.EncodeRow) as today.

```
offset  size  field
0       1     Flags     bit 0 Deleted, bit 1 Forward, bit 2 MovedIn; others zero
1       1     reserved  zero
2       8     XID       the transaction that last inserted, updated or deleted the row
10      8     Undo      undo pointer to the row's previous version (undo.Ptr), 0 = none
18      ...   the encoded row (absent in a forward stub or a tombstone)
```

- **XID and undo pointer** are what Step 6.4's readers need. A reader compares the XID with its snapshot and, if the version is too new, follows the undo pointer to an older one.
- **Only the last writer** is kept, not a creator and a deleter as in PostgreSQL. The history is in the undo log, so the row itself needs only its newest version. This is the InnoDB layout (`DB_TRX_ID`, `DB_ROLL_PTR`).

The three kinds of tuple that are not a plain row:

| Kind | Flags | Content after the flags |
|---|---|---|
| **Tombstone**: a deleted row | Deleted | XID and Undo only (18 bytes); the row is in the undo log |
| **Forward stub**: a row that moved | Forward | reserved, then u64 page and u16 slot of the moved tuple, then 6 reserved bytes (18 bytes, section 2.7) |
| **Moved-in tuple**: the moved row itself | MovedIn | XID, Undo, then u64 page and u16 slot of its home (the stub), then the row |

Every other combination of flags is corrupt.

### 2.2 Row identity and forwarding

A row's **home RID** is where it was inserted. It never changes while the row exists: indexes, undo records and (in 6.6) locks name it.

When an update makes a row too big for its home page:

1. The new version goes to another page as a **moved-in tuple**, carrying a back pointer to its home.
2. The home slot becomes a **forward stub** pointing at it.

There is **at most one hop**. If a moved row moves again, the stub is pointed at the new place and the old moved-in tuple is removed. If a later update fits on the home page again, the row moves back home and the moved-in tuple is removed.

- **Reading a row** (`Get` by RID) starts at the home slot and follows a stub to the moved-in tuple, whose back pointer must name the home RID. Anything else is corrupt.
- **A scan** returns rows at their home RID. It follows stubs and skips moved-in tuples, so a moved row is seen exactly once.

**Why forwarding, not a new RID** (today's heap moves the row and returns a new RID, and the executor rewrites its index entries):

| | Forwarding pointer (chosen) | New RID on move |
|---|---|---|
| Row identity | Stable for the row's life | Changes on every move |
| Indexes | Untouched by a move | Every index entry rewritten on a move |
| Undo records, locks (6.6) | Name one RID forever | Need the RID they had at the time, or an update of every reference |
| Old index entries (6.7) | Keep pointing at the right row | Point at a slot that may hold another row |
| Rollback (6.5) | Restores the row at its RID | Must move the row back and fix the indexes |
| Cost | One more page read for a moved row; 18 bytes at home | None while reading |

The extra read is paid only by rows that grew past their page. With MVCC, a stable RID removes a whole class of bugs.

### 2.3 Writes

All writes run inside a transaction (`wal.Txn.Write`); the transaction's XID goes into the header and the undo record. For each row:

| Statement | Undo record (written first) | Row afterwards |
|---|---|---|
| `INSERT` | Kind Insert, no image, PrevForRow 0 | header: XID, Undo → the record |
| `UPDATE` | Kind Update, image = the previous version, PrevForRow = its undo pointer | changed in place (or moved, 2.2), header: XID, Undo → the record |
| `DELETE` | Kind Delete, image = the previous version, PrevForRow = its undo pointer | a tombstone: XID, Undo → the record |

- **The undo record is written before the row**, so the row's undo pointer always names a record that exists. Both are logged, and the transaction's commit record makes them durable together; a crash before the commit discards both (13-transactions.md section 2.6).
- **The image of a version** is its XID, undo pointer and encoded row: 16 + at most 8000 bytes, so it fits `undo.MaxImage` (8090).
- **A tombstone keeps no data.** The deleted version is in the undo log, so a tombstone is 18 bytes. Purge (6.8) removes it. Until then a deleted row costs 22 bytes of its page (with its slot).
- **A deleted row cannot be updated or deleted again** (it is not found), and its slot is not reused until purge removes the tombstone.
- **Several changes to one row in one transaction** each write an undo record, chained by PrevForRow, as InnoDB does.

### 2.4 Where the code goes

- **`internal/storage`** knows the physical format: the row header, tombstones, stubs and moved-in tuples, and the moves of 2.2. Its API takes and returns a `Version{XID, Undo, Data}`. It does not know about the undo log.
- **New package `internal/mvcc`** writes versioned rows: it appends the undo record, then calls the heap. Step 6.4 adds snapshots and visibility here.
  ```go
  func Insert(ctx context.Context, w *Writer, h *storage.Heap, data []byte) (storage.RID, error)
  func Update(ctx context.Context, w *Writer, h *storage.Heap, rid storage.RID, data []byte) error
  func Delete(ctx context.Context, w *Writer, h *storage.Heap, rid storage.RID) error
  // Writer carries the undo log, the transaction's XID and the table ID.
  ```
- **The executor and the catalog** write through `mvcc`. `Update` no longer returns a RID, so the executor stops special-casing moved rows. Indexes are maintained as today (Step 6.7 changes that).
- **System tables** use the same row format and the same versioned writes. Their undo records carry table ID 0 (user tables start at 1); a RID is unique in the data file, so the table ID is informational for them.

### 2.5 Until purge and rollback exist: releasing undo at commit

Nothing reads old versions until Step 6.4, and rollback still discards a transaction by reopening the database (13-transactions.md section 2.6). Keeping undo for committed transactions would only grow the data file. So, **as an interim rule**, a transaction's last write before its commit record releases its undo segment (`undo.Log.Release`). Its pages are freed by the next checkpoints, and the file stops growing under repeated updates.

- Step 6.4 replaces the rule (undo is kept while a snapshot may need it), and Step 6.8's purge takes over releasing. The rule is one function, `mvcc.BeforeCommit`, called by the executor's two commit paths.
- Releasing inside the transaction keeps it atomic: a crash before the commit record discards the release with everything else.

### 2.6 Size limit

The maximum encoded row drops from 8148 to **8000 bytes** (approved in Step 6.2). A plain tuple is then at most 8018 bytes and a moved-in tuple 8028, both within a heap page (8148), and every version image fits an undo record. Rows of 8001 bytes or more are refused with `54000` "row is too big", as they are now above 8148.

### 2.7 Implementation notes (Step 6.3)

- **A forward stub is 18 bytes, not 12: as big as a tombstone.** Deleting a moved row turns its stub into a tombstone. With a 12-byte stub, those 6 more bytes did not always fit on a full home page, and the delete failed. The heap model test found it. The 6 extra bytes are reserved and must be zero.
- **Headers are given by a `Stamper`, not passed in.** An insert's undo record names the row's RID, and the row's header names the undo record, so neither can be written first with all it needs. The heap therefore takes a `Stamper` callback.
  - It is called once per write, with the pages latched and before anything changes, with the RID (the slot an insert will take is known first) and the version being replaced.
  - `mvcc` appends the undo record inside it.
  - A write that must latch more pages and try again (a move) reuses the header it got, after checking that the row has not changed.
- **The release at commit is in `wal.Txn.Commit`, not `mvcc.BeforeCommit`.** That is the one place every commit path goes through: the executor's statements and transactions, and the catalog's bootstrap.
  - The `wal` package's own tests of the undo log (Step 6.2) turn the release off through an unexported field.
  - Step 6.4 replaces the rule.
- **Pool sizes.** A row write pins up to three heap pages (a second move), and while it is stamped, up to two heap pages and two undo pages. The engine therefore needs at least four frames, and refuses fewer. A logged heap alone needs three.
  - With fewer frames, everything but a move still works, and a move fails cleanly with `ErrNoFreeFrames`.
  - "No steal" still bounds a transaction by the pool (13-transactions.md section 2.6).
- **A delete is logged as an update** of the slot to the tombstone. A move is logged with operation blocks (insert, update, delete), or images after a redo point, as before.
- **The catalog's DDL methods take the transaction's ID**, since they write system table rows through `mvcc`.
- **A row may be empty.** The header gives every tuple at least 18 bytes, so `ErrEmptyTuple` no longer applies to heap rows.
- **`PrevForRow` may name released undo.** A row last changed by a committed transaction keeps the undo pointer it had. With the interim release (section 2.5), that record is freed with its transaction's segment. Nothing follows such a pointer in Step 6.3. Step 6.4 must not follow one either: a reader only follows the chain while versions are too new for its snapshot, and the released undo of a committed transaction is older than every snapshot.
- **A slower test, made fast again.** `TestSelfRestartAfterIOFailure` replays its setup data for every write and sync it fails. Every insert now also writes and releases undo, and the race-detector run passed Go's 10-minute limit. The setup now inserts 40 rows per statement instead of one. The statements under test go through the same failure points as before (the counts differ by a few), and the test runs in 3 seconds instead of 55.
- **Deliberate bugs:** 30, four at a time on copies of the code, each against its packages' tests with a 60-second limit; about 1.5 minutes in all.
  - **Caught: 27.**
  - **After new tests (4):** a forward stub leading to a moved-in tuple of another row; a move's choice of page never being one of the row's own pages; executor undo records carrying the table ID.
  - **Hang fix:** one of those tests could hang when it failed, and was fixed first.
  - **Not caught (3), all defensive checks a single writer cannot reach:**
    - an insert landing in the slot `insertSlot` predicted;
    - pages restored when an operation fails after its first change, which no reachable failure does;
    - a row unchanged between its stamp and a move that latches more pages.
  - They stay as safety nets for Step 6.6, when writers run concurrently.

## 3. Formats (approved)

- **Data file format version 4:** every heap tuple has the row header. Version-3 files are refused with the existing "unsupported format version" error. The phase rules approve this change for Step 6.3.
- **WAL format version 4 (approved):** a heap record may hold **three** blocks instead of two. Moving a moved row again (2.2) changes three pages at once: the stub, the old moved-in tuple and the new one. One record keeps the three changes atomic in the log, as the two-page moves are today.
  - The alternative is two records that rely on the transaction for atomicity. It needs no format change, but a heap operation would then be atomic only inside a transaction, which the heap does not otherwise assume.
- Undo records are unchanged; their image is the version image of 2.3.

## 4. Concurrency

- **Writers:** one writing transaction at a time (Step 6.1's interim rule) holds the executor's exclusive lock, so no reader sees its uncommitted rows.
- **Latches:** a move latches its two or three pages exclusively in increasing page order, as heap moves do today, so moves cannot deadlock.
- **Readers** follow a stub after releasing the home page's latch. The back pointer is checked, and on a mismatch (the row moved again meanwhile) the read starts again at home. This is not reachable while writers hold the exclusive lock, but the heap does not rely on that.

## 5. Failure behaviour

| Event | Result |
|---|---|
| Crash during a transaction | No commit record: its heap and undo changes, and its undo release, are all discarded (no steal). |
| Crash after commit | Rows, undo records and the release are replayed; the released pages are freed by the deferred-free records. |
| Crash in the middle of a move | Impossible to see half of it: each move is one heap record. |
| A damaged header, an unknown flag combination, a stub pointing at a tuple that does not point back | `ErrCorruptHeap`; never a panic. |
| A row over 8000 bytes | `54000`, nothing written. |
| Update or delete of a deleted row | Not found: the statement does not match it. |

## 6. Alternatives considered

- **Keep PostgreSQL's out-of-place versions.** Bloat and VACUUM are what NoVacDB exists to remove.
- **A new RID on every move.** See the table in 2.2.
- **Creator and deleter XIDs in the header (PostgreSQL's xmin/xmax).** With an undo log, the newest version needs only its last writer.
- **Keeping the deleted row's data in the tombstone.** The data is already in the undo record; keeping it twice wastes page space until purge.
- **Keeping undo until purge exists (no interim release).** Correct but useless: no reader needs it before 6.4, and the data file would grow without bound under updates.
- **Undo for system tables left out.** They would need a second row format and a second write path; one format for every heap is simpler.

## 7. Testing plan

- **Format:** golden bytes of each tuple kind; every truncation and every invalid flag combination refused; `FuzzRowHeader` (any decoded header re-encodes to the same bytes, nothing panics).
- **Heap moves:** rows that grow and shrink across pages: stay home, move away, move again, move back. The RID never changes, a scan sees each row once, and `Get` follows exactly one hop.
- **Model test:** random insert, update and delete over many transactions, with row sizes chosen to force moves, checked against a model after every transaction. Before each commit, every changed row's undo chain matches the row's history in the transaction.
- **Acceptance (no growth):** the same row updated 10,000 times: the table keeps the same pages, and the data file stops growing once checkpoints free the released undo pages.
- **Crash tests:** the engine and SQL crash harnesses with power cuts and torn writes; every committed row and header present, nothing uncommitted. The existing SQL logic tests and crash workload pass unchanged.
- **Deliberate bugs:** about 25–30, as in Step 6.2: four at a time on a copy of the code, each against its package's tests with a 60-second limit.

## 8. Limitations (Step 6.3)

- Nobody reads old versions yet (6.4); rollback still reopens the database (6.5; done: 17-rollback.md).
- Undo is released at commit (2.5), so there is no history across transactions until 6.4.
- Tombstones stay until purge (6.8): 22 bytes per deleted row.
- Index entries are still removed and added at once by the writer (6.7 changes this).
- Dropping a table leaves tombstones in the system tables until purge (6.8).
- A moved row costs one extra page read.
