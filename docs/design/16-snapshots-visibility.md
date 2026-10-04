# 16 — Snapshots and Visibility (`internal/wal`, `internal/mvcc`, `internal/storage`, `internal/sql/executor`)

Status: **Step 6.4 designed; awaiting review.**

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
| DDL (`CREATE`, `DROP`) | `DB.mu` exclusively until its transaction ends, as today: the in-memory catalog is not versioned |
| A rollback or a restart (reopening the engine) | `DB.mu` exclusively: it waits for running statements |

- **Readers run beside one writer.** The heap, the B+Trees and the buffer pool already work under concurrent access, with page latches and their own concurrency tests. The undo log is read under shared page latches too; its first concurrent tests come with this step (section 7).
- **Rows a writer moves or deletes stay readable** at their home RID (15-row-versioning.md section 2.2), so a scan sees each row once.
- **Still one writer at a time** (the engine's writer slot, 13-transactions.md section 2.4). Step 6.6 allows more.
- **A `REPEATABLE READ` transaction that lives across a reopen** (a rollback or a restart by another session) loses its snapshot. Its next statement fails with `40001`. The reopen goes away with Step 6.5.

### 2.5 Indexes until Step 6.7

A writer still removes a row's old index entries and adds the new ones at once (15-row-versioning.md section 8). An index therefore describes the newest versions. A snapshot that does not see some change may find a row under the wrong key, or miss it.

**Rule:** a statement uses an index only if its snapshot sees every transaction that has written: the engine's most recent writing XID is `Me`, or below `XMax` and not active. Otherwise it reads the table (a heap scan with the visibility rule), which is always right.

- In practice, readers use indexes except while a writer is active or has committed since their snapshot.
- Unique checks are done by the writer, which sees everything (section 2.3), so they are unchanged.
- Step 6.7 keeps old index entries and removes this rule.

### 2.6 Keeping undo while a snapshot needs it

The undo of a committed transaction `x` holds the versions from before `x`. A snapshot that does not see `x` needs that undo. One that sees `x` never reads it. The rule:

> **A committed transaction's undo segment is released once no live snapshot misses it.**

- **When.** Releasing logs records, so it happens under the engine's writer slot, never in the middle of another transaction's records. There are two places:
  - a writing transaction, just before its commit record, releases every pending segment no snapshot needs, its own included, atomically with its commit;
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

## 3. Formats

**None.** Snapshots live in memory, and every on-disk structure stays as Steps 6.2 and 6.3 left it.

## 4. Concurrency

Section 2.4, plus:

- **The engine's mutex** guards the snapshot registry and the pending segments.
  - Taking or releasing a snapshot is O(1) amortised.
  - A release check walks the pending segments against the live snapshots; both are small with one writer.
- **A reader following an undo chain** reads undo pages with shared latches, one at a time. The segments it can reach cannot be released under it: its snapshot is registered.
- **The race detector** runs the concurrency tests below.

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
- Rollback still reopens the database, ending `REPEATABLE READ` snapshots with `40001` (6.5).
- A long snapshot holds undo back without limit (6.8).
- Isolation levels are chosen through the executor's API only (`BEGIN ISOLATION LEVEL` is 6.10).
