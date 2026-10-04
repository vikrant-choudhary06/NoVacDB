# 16 — Snapshots and Visibility (`internal/wal`, `internal/mvcc`, `internal/storage`, `internal/sql/executor`)

Status: **Step 6.4 designed, approved and implemented**, in two parts: 6.4a (snapshots, visibility, isolation levels, undo retention) and 6.4b (readers that do not wait for the writer). Notes, including two changes found by the 6.4b tests (undo released after the commit record; index scans checked as they run), in section 2.8.

## 1. Problem

Since Step 6.3 every row knows the transaction that last wrote it, and its older versions are in the undo log. But nobody reads them yet. A writing transaction holds the executor's lock exclusively until it ends (13-transactions.md section 2.6), so every reader waits for it.

Step 6.4 lets readers run while a transaction writes, and gives each reader a consistent picture:

- **A snapshot** says which transactions a reader sees: those committed before it was taken, and its own.
- **The visibility rule:** a row version written by a transaction the snapshot does not see is skipped, and the reader follows the row's undo chain back to the newest version it does see.
- **Isolation levels:**
  - `READ COMMITTED` takes a new snapshot for each statement (PostgreSQL's default).
  - `REPEATABLE READ` takes one snapshot for the whole transaction.
- **A transaction always sees its own changes.**
- **Undo is kept while a snapshot may need it**, instead of being released at commit (15-row-versioning.md section 2.5).

Out of scope, for later steps:
- several writers at once, and row locks (6.6);
- keeping old index entries (6.7);
- rollback with undo (6.5);
- purge of tombstones and old index entries (6.8);
- `BEGIN ISOLATION LEVEL` over the wire (6.10).

## 2. Design

### 2.1 Snapshots

```go
// package wal
type Snapshot struct {
	XMax   XID   // the next XID when the snapshot was taken: every XID >= XMax is unseen
	Active []XID // transactions writing when it was taken: unseen
	Me     XID   // the reader's own transaction, 0 if it has not written
}

func (e *Engine) Snapshot(me XID) *Snapshot // registered until Release
func (s *Snapshot) Release()
```

- **Taking a snapshot is cheap:** under the engine's mutex, it copies `nextXID` and the active set. There is at most one active writer (section 2.4), so the set is tiny.
- **The engine keeps every live snapshot registered.** This is how it knows which undo may still be needed (section 2.6).
- **XIDs only.** There is no commit timestamp or log position: with one writer at a time, transactions commit in XID order, so "committed before the snapshot" is exactly "below `XMax` and not active".

### 2.2 The visibility rule

A version written by transaction `x` is **seen** by snapshot `S` if:

1. `x == S.Me` (the reader's own change), or
2. `x < S.XMax`, `x` is not in `S.Active`, and the status table does not say `x` aborted.

Aborted versions do not exist on disk yet, since a rollback reopens the database and recovery removes them. The status check is there so the rule stays right when Step 6.5 adds rollback.

**Reading a row** (`mvcc.Reader`), starting from its current version `v` in the heap:

```
loop:
  v is a tombstone, deleter seen      -> the row does not exist for S
  v is a row, writer seen             -> v is the answer
  writer not seen:
    v.Undo == 0                       -> the row does not exist for S
    rec := undo.Read(v.Undo)          // must be x's record for this row
    rec.Kind == Insert                -> the row did not exist before x: none
    rec.Kind == Update or Delete      -> v = the version in rec.Image; loop
```

- **Every step checks the record:** its XID must be the version's writer, and its RID the row's home RID. A mismatch, or a chain longer than any transaction could have made it (a loop), is `ErrCorrupt`.
- **The chain only goes as far as needed.** It stops at the first version the snapshot sees. Undo that has been released is never reached (section 2.6), so a stale undo pointer is never followed.

### 2.3 Isolation levels

| Level | Snapshot | As in PostgreSQL |
|---|---|---|
| `READ COMMITTED` (default) | A new one for each statement | Yes |
| `REPEATABLE READ` | One for the transaction, taken at its first statement | Yes |

- **Own changes.** Both levels see the transaction's own changes: `Me` is set as soon as it has an XID. A statement does not see its own changes while it runs: the executor computes all of a statement's changes before applying any (10-executor.md).
- **Write conflicts under `REPEATABLE READ`.** A transaction may try to update or delete a row whose newest version it does not see: another transaction changed it after the snapshot. It then fails with `40001` "could not serialize access due to concurrent update", as in PostgreSQL. Without this, the update would overwrite a change it never saw (a lost update).
  - With one writer at a time, this needs no locks: the check compares the newest version's writer with the snapshot.
  - Step 6.6 keeps the rule and adds waiting.
- **`READ COMMITTED` writers** take their statement's snapshot after they hold the writer slot (section 2.4). Every other writer has then committed, so the newest version of every row is visible to them. They therefore update what they see.

### 2.4 Concurrency: readers no longer wait

Today a writing transaction holds the executor's lock (`DB.mu`) exclusively until it ends. After this step:

| Who | Holds |
|---|---|
| A reading statement | `DB.mu` shared, for the statement |
| A writing statement (`INSERT`, `UPDATE`, `DELETE`) | the **writer slot** (a new `DB.writer` mutex) until its transaction ends, and `DB.mu` shared for the statement |
| DDL (`CREATE`, `DROP`) | the writer slot, and `DB.mu` exclusively, until its transaction ends: the in-memory catalog is not versioned |
| A rollback or a restart (reopening the engine) | `DB.mu` exclusively: it waits for running statements |
| `Close` | the writer slot, then `DB.mu` exclusively: it waits for a writing transaction to end |

**Lock order:** the writer slot, then `DB.mu`. A statement that holds `DB.mu` shared and must reopen the engine gives it up and takes it exclusively (it holds the writer slot, so no other writer gets in between).

- **Readers run beside one writer.** The heap, the B+Trees and the buffer pool already work under concurrent access, with page latches and their own concurrency tests. The undo log is read under shared page latches too; its first concurrent tests come with this step (section 7).
- **Rows a writer moves or deletes stay readable** at their home RID (15-row-versioning.md section 2.2), so a scan sees each row once.
- **Still one writer at a time** (the engine's writer slot, 13-transactions.md section 2.4). Step 6.6 allows more.
- **A `REPEATABLE READ` transaction that lives across a reopen** (a rollback or a restart by another session) loses its snapshot. Its next statement fails with `40001`. Since Step 6.5 a rollback uses undo and reopens nothing, except for transactions that changed the schema (17-rollback.md section 2.7).

### 2.5 Indexes until Step 6.7

A writer still removes a row's old index entries and adds the new ones at once (15-row-versioning.md section 8). An index therefore describes the newest versions. A snapshot that does not see some change may find a row under the wrong key, or miss it.

**Rule:** a statement uses an index only if its snapshot sees every transaction that has written: the engine's most recent writing XID is `Me`, or below `XMax` and not active. Otherwise it reads the table (a heap scan with the visibility rule), which is always right.

With readers beside a writer, the rule is checked when the scan runs, not only when the statement is planned (section 2.8): a writer may begin in between, or during the scan.

- In practice, readers use indexes except while a writer is active or has committed since their snapshot.
- Unique checks are done by the writer, which sees everything (section 2.3), so they are unchanged.
- Step 6.7 keeps old index entries and removes this rule.

### 2.6 Keeping undo while a snapshot needs it

The undo of a committed transaction `x` holds the versions from before `x`. A snapshot that does not see `x` needs that undo. One that sees `x` never reads it. The rule:

> **A committed transaction's undo segment is released once no live snapshot misses it.**

- **When.** Releasing logs records, so it happens under the engine's writer slot, never in the middle of another transaction's records. There are two places:
  - a writing transaction, just after its commit record is durable and it has left the active set, still in the writer slot, releases every segment no snapshot needs, its own included (section 2.8 explains why after, not before);
  - a checkpoint, which already holds the slot, does the same.
- **After a crash or a reopen, no snapshot exists.** Recovery releases every segment it finds (all of them belong to committed transactions; uncommitted ones were discarded).
- **This replaces the interim rule of 15-row-versioning.md section 2.5.** Step 6.8's purge takes over and adds tombstones and index entries.
- **Long snapshots hold undo back.** Its size and the oldest snapshot holding it are reported by Step 6.8 (problem #24).

### 2.7 Code

- **`internal/wal`:** `Snapshot`, `Engine.Snapshot`, the registry of live snapshots, the most recent writing XID, and the release rule of 2.6. It replaces the release at commit.
- **`internal/storage`:** `Scan` and `Get` also return tombstones, as a `Version` with `Deleted` set, so that a reader can follow a delete's undo. Writes still treat a tombstone as a deleted row.
- **`internal/mvcc`:** `Reader{Undo, Snap}`, with `Visible(ctx, rid, v) (data []byte, ok bool, err error)`, and `Writer` gains the `REPEATABLE READ` conflict check of 2.3.
- **`internal/sql/executor`:**
  - snapshots per statement or per transaction, the locks of 2.4, and the index rule of 2.5;
  - `DB.Begin(ctx, TxOptions{Isolation})`, `READ COMMITTED` by default.
- **The catalog** reads its system tables only when it opens, when no snapshot exists, through the same reader.

### 2.8 Implementation notes

**Step 6.4a** (everything but the locks of 2.4; readers still wait for a writing transaction):

- **A snapshot does not carry `Me`.** A transaction's XID is allocated at its first write, which may come after its first snapshot (`REPEATABLE READ`). The reader's own XID is therefore passed separately (`mvcc.Reader.Me`, `stmt.me`), and is current at every statement.
- **A snapshot is released before its own transaction's commit record.** An autocommit statement's snapshot, or a `REPEATABLE READ` transaction's, cannot see its own transaction. Still registered at commit, it held back that transaction's own undo until the next commit or checkpoint. A test of freed pages found this.
- **The aborted transactions below `XMax`** (recovery's, from the status table) are copied into the snapshot when it is taken, so `Sees` needs no lock.
- **Reads that return tombstones are new methods**, `Heap.GetVersion` and `Heap.ScanVersions`. `Get` and `Scan` keep their meaning for the catalog and the heap's own users.
- **Loops in an undo chain** are looked for after 1024 steps, with the set of pointers followed. A long chain (one transaction changing a row many times) costs nothing before that.
- **A write conflict discards the transaction** the way a rollback does (reopening, until Step 6.5), and returns `40001`. Since Step 6.5 only the statement is undone, in place, and the transaction is failed (17-rollback.md section 2.5).
- **Deliberate bugs:** 22, in about a minute.
  - **Caught at first: 18.**
  - **Gaps found by the other 4, each caught after a new test:**
    - a transaction updating its own new version must not conflict with itself;
    - a `REPEATABLE READ` commit must release its own undo;
    - an aborted transaction is never seen;
    - an active transaction's undo is never released.

**Step 6.4b** (the locks of 2.4):

- **DDL also takes the writer slot**, before `DB.mu`. A DDL statement changes rows of the system tables, which is a write like any other, and the slot keeps the lock order the same for every statement (writer slot, then `DB.mu`), so DDL and DML never deadlock. A test runs DDL, transactions mixing DML and DDL (some rolled back), DML and readers at once.
- **A transaction's ID is active from the moment it is allocated.** `begin` used to allocate the XID, log `TxnBegin`, and only then add it to the active set. A snapshot taken in between had the XID below `XMax` and not active, and so saw the writer's uncommitted changes. Readers waiting for the writer had hidden this. The model checker found it; a unit test now takes a snapshot after `TxnBegin` failed to be logged.
- **Undo is released after the commit record, not before** (a change to 2.6). Released before, the committing transaction's own segment could go while it was still active: a snapshot taken between the release and the transaction leaving the active set missed the transaction and found its undo gone, and reused by the next one ("version of transaction 13 leads to a record of 14"). After the commit record, every new snapshot sees the transaction, so only the live ones can need its undo. Atomicity with the commit is not needed: a crash in between leaves undo that recovery releases anyway, as it releases all undo. If the release fails, the transaction has committed; `Commit` returns its LSN with the error, and the engine must be abandoned as after any failed commit.
- **Index scans are checked as they run.** The plan-time rule of 2.5 is not enough once readers run beside a writer: a writer may begin after the statement planned an index scan, or during it, and move index entries the snapshot cannot see. A scan therefore reads the engine's last writer, checks that the snapshot sees it, collects the index's hits (and each row's current version), and uses them only if the last writer is still the same; otherwise it reads the table instead. Every writer becomes the last writer when its XID is allocated, before it touches an index. A test hook starts a writer between planning and scanning; a counter records the scans redone.
- **A statement that must reopen the engine** (a restart, a rollback, a `40001` discard) gives up its shared lock and takes `DB.mu` exclusively, so it waits for the readers running beside it; it holds the writer slot throughout. `REPEATABLE READ` readers whose snapshot predates the reopen get `40001` (2.4).
- **Tests** (all under `-race` in `make check`):
  - a model checker: one writer commits and rolls back random transactions (updates that move rows, deletes, inserts) with frequent checkpoints and a small pool, while `REPEATABLE READ` readers check each read, by heap and by index, against the model's state after the last commit their snapshot sees;
  - no dirty reads while a transaction writes, and a second writer waits; a transaction that ran DDL keeps readers out until it ends;
  - readers during rollbacks and during restarts never fail; `Close` waits for a writing transaction;
  - the SQL crash harness runs a concurrent reader in its cycles without injected faults (its I/O would move the faults, which count operations): each read must be a committed state, in commit order.
- **Deliberate bugs:** 26, in about 7 minutes.
  - **Caught at first:** 19, though some of these kills, it turned out, came from the undo release bug above, which made the tests themselves fail now and then.
  - **On a stable baseline, gaps found and closed with new tests:**
    - a transaction's own changes must keep indexes usable;
    - DDL must exclude readers, in autocommit and until a transaction that ran it ends (a reader of tables being dropped; a reader of a table created by an open transaction);
    - `Close` must wait for a writing transaction;
    - the rollback of a transaction that ran DDL;
    - the scan-time index check (the test hook above);
    - a restart beside running readers.
  - **Equivalent, replaced:** 2.
    - Releasing undo before the commit record as well as after it frees nothing more than a checkpoint would.
    - An unseen version without an undo pointer cannot occur.

## 3. Formats

**None.** Snapshots live in memory, and every on-disk structure stays as Steps 6.2 and 6.3 left it.

## 4. Concurrency

Section 2.4, plus:

- **The engine's mutex** guards the snapshot registry and the pending segments.
  - Taking or releasing a snapshot is O(1) amortised.
  - A release check walks the pending segments against the live snapshots; both are small with one writer.
- **A reader following an undo chain** reads undo pages with shared latches, one at a time. The segments it can reach cannot be released under it: its snapshot is registered.
- **The race detector** runs the concurrency tests below.
- **The executor's locks** are those of 2.4, in the order writer slot, then `DB.mu`. The engine's own writer slot (13-transactions.md section 2.4) is taken after both, at a transaction's first write, and a checkpoint takes it under the executor's writer slot.

## 5. Failure behaviour

| Event | Result |
|---|---|
| Crash with readers and a writer active | Snapshots are gone. Recovery discards the writer's changes (no steal) and releases all undo. |
| A writer fails or rolls back (reopen) | It waits for running statements. `REPEATABLE READ` transactions that held a snapshot get `40001` at their next statement. |
| `REPEATABLE READ` update of a row changed after the snapshot | `40001`; the transaction is failed (`25P02` afterwards), as in PostgreSQL. |
| An undo record that does not match the version leading to it, or a chain that loops | `ErrCorrupt` (`XX001`); never a panic or a wrong row. |
| A reader whose undo was released (a bug) | The record check fails: `ErrCorrupt`, not a wrong answer. |

## 6. Alternatives considered

- **Commit sequence numbers (CSN) instead of an active list.** Smaller snapshots with many writers, but every version would need its transaction's commit number, which is a lookup or a header rewrite at commit. With one writer the active list holds at most one XID. This can be revisited in 6.6.
- **PostgreSQL's xmin/xmax per row version.** NoVacDB keeps old versions in undo (15-row-versioning.md section 2.1).
- **Readers that wait for writers** (today's behaviour). No snapshot is needed, but nothing runs beside a long transaction.
- **Keeping undo for a fixed time.** Either too long (undo grows) or too short (a slow reader gets a wrong answer). Tracking live snapshots is exact.

## 7. Testing plan

- **Visibility rule, unit:** every case of 2.2 (own, older, active, newer, aborted), on rows inserted, updated many times, deleted, and moved, with chains across several transactions.
- **Anomalies, with two sessions:**
  - no dirty reads at either level;
  - non-repeatable reads under `READ COMMITTED`, none under `REPEATABLE READ`;
  - phantoms: none under `REPEATABLE READ` (as in PostgreSQL), visible under `READ COMMITTED`;
  - own changes visible;
  - the `REPEATABLE READ` write conflict gives `40001`.
- **Model checker under `-race`:** a writer commits random transactions while readers take snapshots and read whole tables, by heap scan and through indexes. Every read must equal the model's state after exactly the transactions the snapshot sees (with one writer, a prefix of the commit order).
- **Undo retention:**
  - a long `REPEATABLE READ` reader keeps the undo it needs and sees its snapshot after many commits;
  - once it ends, the next commit or checkpoint releases the undo;
  - after a crash, nothing is left.
- **Index rule:** a reader whose snapshot misses a change gets the right rows, by a heap scan; one that sees everything uses the index.
- **Crash tests:** the SQL crash harness gains concurrent readers checking their snapshots.
- **Deliberate bugs:** about 25–30, as in Steps 6.2 and 6.3.

## 8. Limitations (Step 6.4)

- One writer at a time; a second writer waits for the first to end (6.6).
- Readers skip indexes while a change they cannot see exists (2.5, until 6.7).
- Rollback still reopens the database, ending `REPEATABLE READ` snapshots with `40001` (6.5; done: 17-rollback.md).
- A long snapshot holds undo back without limit (6.8).
- Isolation levels are chosen through the executor's API only (`BEGIN ISOLATION LEVEL` is 6.10).
