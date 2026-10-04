# 17 — Rollback (`internal/storage`, `internal/mvcc`, `internal/wal`, `internal/sql/executor`)

Status: **Step 6.5 designed, awaiting review.** The decision points to approve are in section 9.

## 1. Problem

Today a rollback throws the whole database away and reopens it (13-transactions.md section 2.6):

- The engine is abandoned as a crash would leave it.
- Recovery discards the transaction, since it has no commit record.
- The database opens again.

This works and is exact, but it has costs:

- **Every running statement waits** for the reopen.
- **Every `REPEATABLE READ` snapshot is lost.** Its next statement fails with `40001` (16-snapshots-visibility.md section 2.4).
- **A statement that fails halfway costs the whole transaction**, and the database is reopened for it. Examples:
  - a `REPEATABLE READ` write conflict on its tenth row;
  - an index key too long on its hundredth row.

Since Step 6.3, every change a transaction makes has an undo record that holds the row's previous version. Step 6.5 uses those records to roll back in place:

- **Transaction rollback.** Apply the transaction's undo records, newest first, then end it with a `TxnAbort` record.
- **Statement rollback.** A statement that fails after changing rows has exactly its own changes undone. The transaction then becomes failed, as in PostgreSQL. In autocommit, the statement is the transaction.
- **The rollback's own changes are logged** like any other change, so a crash during a rollback is safe (section 2.6).

Out of scope, for later steps:
- savepoints (`SAVEPOINT`, `ROLLBACK TO`): 6.10 decides, and the mechanism here is built for them (section 2.5);
- rolling back transactions that changed the schema (section 2.7);
- undo at recovery for transactions whose pages reached disk ("steal", 6.9).

## 2. Design

### 2.1 What one undo record reverses

A transaction's undo records are chained newest first through `PrevInTxn` (14-undo-log.md section 2.3). Each record names its table, the row's home RID, and the row's version before the change: its header (XID, undo pointer) and its data (15-row-versioning.md section 2.3).

To reverse one record, the rollback first reads the row's **current version**, `cur`. It walks the records newest first, so `cur` is always the version this record's change wrote. Its XID must be the transaction's; anything else is corrupt.

| Record | Heap | Indexes |
|---|---|---|
| **Insert** | Remove the row: its slot, plus a moved-in tuple if it moved | Remove the entries of `cur` |
| **Update** | Restore the old version: header and data, moving the row if needed (2.2) | Remove the entries of `cur`, add the entries of the old version |
| **Delete** | Restore the old version in place of the tombstone | Add the entries of the old version |

Some consequences:

- **Headers come back exactly.** The old version's XID and undo pointer come from the record's image, so every row's version chain is as it was before the transaction.
- **Several changes to one row** reverse one at a time. Each step restores the version the step before it saw.
- **Index entries come from the rows**, not from the undo log. An index key is the row's indexed columns plus its RID (`catalog.Index.Key`). It is computed from `cur` and from the old version, with the table's current schema (the transaction changed no schema, section 2.7).

### 2.2 Making index changes reversible

Undo records cover heap changes only. A statement that fails must therefore never have touched an index entry of a row that has no undo record yet.

Today `applyChanges` deletes the old index entries of **every** changed row first, and only then writes the rows. If it fails on the tenth row, rows ten onwards have lost their index entries with no undo record. Rollback could not restore them.

**Change:** `applyChanges` handles each row in turn: it writes the row (undo record first, as today), then deletes the row's old index entries, then adds its new ones.

- The old order existed so that rows could swap unique keys within one statement. That needs no special order: every index key ends with the row's RID, so two rows never have the same B+Tree key, and uniqueness is checked before any change (`checkUnique`).
- **The rollback's index operations are idempotent:**
  - it removes an entry only if it is there;
  - it adds an entry only if it is missing.

  A statement can fail between a row's heap write and its index changes. That row then has an undo record, but its indexes still describe its previous version. Reversing the record leaves the indexes exactly as they were either way.

### 2.3 Heap operations

The heap gets two operations, both logged with the existing heap record operations (insert, update and delete of a slot, or page images after a redo point).

```go
// Remove deletes the row at rid entirely: its slot (which becomes free) and
// a moved-in tuple it forwards to.
func (h *Heap) Remove(ctx context.Context, rid RID) error

// Restore writes version v at rid, whatever is there now (a plain row, a
// tombstone, or a forward stub). It moves the row when v does not fit its
// page, exactly as Update does, and moves it home when it does.
func (h *Heap) Restore(ctx context.Context, rid RID, v Version) error
```

**`Remove` frees the slot at once.** Nobody else can be relying on it:

- An uncommitted insert is invisible to every snapshot.
- A reader that found the RID through an index already copied the row's version with it, and that copy's insert record says "not visible".
- An index scan during which a writer begins is redone as a table scan (16-snapshots-visibility.md section 2.8).

### 2.4 Who does what

- **`internal/mvcc`** walks the records:
  ```go
  // Undo calls fn for each undo record of transaction xid from the record at
  // from back to, but not including, the record at to (0: the first), newest
  // first, following PrevInTxn. Each record is read with every check
  // undo.Log.Read makes.
  func Undo(ctx context.Context, log *undo.Log, xid uint64, from, to undo.Ptr, fn func(p undo.Ptr, rec undo.Record) error) error
  ```
- **The executor** reverses each record (section 2.1). Only the executor knows tables, indexes and their keys: it finds the table by the record's table ID in the catalog.
- **`internal/wal`** runs the rollback inside the transaction, so its changes are logged as the transaction's records:
  ```go
  // Savepoint returns where the transaction's undo ends now: the undo
  // pointer of its latest record, 0 if none.
  func (t *Txn) Savepoint() undo.Ptr

  // RollbackTo runs fn, which must reverse the transaction's undo from its
  // latest record back to sp, as a write of the transaction. Afterwards the
  // transaction's undo ends at sp again.
  func (t *Txn) RollbackTo(ctx context.Context, sp undo.Ptr, fn func(ctx context.Context, from undo.Ptr) error) error

  // Rollback reverses all the transaction's undo with fn, logs TxnAbort,
  // makes the log durable through it, and ends the transaction (section 2.6).
  func (t *Txn) Rollback(ctx context.Context, fn func(ctx context.Context, from undo.Ptr) error) error

  // Abandon gives the transaction up: the engine must be abandoned and
  // reopened, and recovery discards it (today's Rollback).
  func (t *Txn) Abandon() error
  ```
- **"Where the undo ends" is kept by the `Txn`, in memory.**
  - After `RollbackTo(sp)`, the transaction's last record is `sp` again, for a later `RollbackTo` or `Rollback`. The records after `sp` stay in the segment, reversed and unreachable, and are released with it.
  - A crash loses this pointer. That is harmless: the transaction has no end record, so recovery discards it whole (section 2.6).
- **A failed `Write` no longer forces a reopen.** Today any error from `fn` marks the `Txn` failed, and only a reopen ends it. After 6.5:
  - **A failure that left every page consistent** marks the transaction *aborting*. Only `RollbackTo` and `Rollback` are allowed then.
  - **A failure of logging or I/O** (the WAL writer failed, a page could not be read or written) marks it failed, as today. `Rollback` refuses it, and the caller abandons the engine.

### 2.5 Statement rollback in the executor

Before a statement's first change, the executor records `sp := wtx.Savepoint()`. If the statement fails after it has begun changing rows:

| The error | What happens |
|---|---|
| **Pages consistent:**<br>• an SQL error from `fn` (e.g. index key too long)<br>• a `REPEATABLE READ` write conflict (`40001`)<br>• the buffer pool full (`54000`) | `RollbackTo(sp)` undoes exactly the statement.<br>• **In a transaction:** the transaction becomes failed (`25P02` for later statements, as today); `Rollback` later undoes the rest.<br>• **In autocommit:** the statement is the transaction, so it is `Rollback`. |
| **Logging or I/O failed, or the data is corrupt** | As today: the database restarts itself, and recovery discards the transaction. |
| **The rollback itself fails** (an I/O error, the pool full, corruption) | As today: the database restarts itself. Recovery discards the transaction, so nothing of it remains either way. |

Each heap and B+Tree operation is atomic: it either changes its pages and logs them, or fails having changed nothing (08-btree.md, 15-row-versioning.md). That is what makes "pages consistent" true after such an error. The tests check it after every injected failure (section 7).

Further consequences:

- **`40001` no longer costs the transaction its snapshot.** The transaction is failed, but nothing reopens, so other sessions' `REPEATABLE READ` snapshots survive.
- **`Tx.Rollback` and `Tx.Commit` of a failed transaction** roll back with undo, and the database stays open.

### 2.6 Ending a rolled-back transaction, and recovery

`Txn.Rollback` runs in this order:

1. **Reverse every record** (2.1), as a write of the transaction.
2. **Log `TxnAbort`** (type 9, payload the XID). Step 6.1 reserved this type with this payload for this step (13-transactions.md section 2.2).
3. **Make the log durable through it**, as a commit does. Until then, no page of the transaction may reach disk ("no steal", the horizon).
4. **End the transaction:**
   - it leaves the active set, with outcome *aborted*;
   - its undo is released by the rule of 2.8;
   - the writer slot is released.

**Recovery repeats history.**
- **The records of a transaction that ended with `TxnAbort` are replayed**, both its changes and their reversal, exactly like a committed transaction's records.
- **Skipping them would be wrong.**
  - Their pages may have reached disk after the abort.
  - Later transactions' records assume the pages as the rollback left them. For example, a B+Tree split survives the removal of the entry that caused it. A later insert into the new page needs that page to exist.
- **Only a transaction with no end record is discarded,** as today. With "no steal", none of its pages reached disk. With one writer at a time, nothing after it depends on its records.

| Crash | After recovery |
|---|---|
| **During the rollback,** before `TxnAbort` is durable | No end record: the transaction is discarded whole, its reversal included. |
| **After `TxnAbort` is durable** | Its changes and their reversal are replayed. Rows and index entries are as before the transaction. |
| **During a statement rollback** in an open transaction | No end record: discarded whole. |

**Compensation records.** In ARIES, a rollback's changes are logged as compensation records, each naming the next record still to undo, so that recovery can finish an interrupted rollback.

- With "no steal", recovery never has to finish one: an interrupted rollback is discarded with its transaction.
- The rollback's changes are therefore ordinary heap, B+Tree and undo records of the transaction. They are the compensation, redo-only, and no new record type is needed.
- Step 6.9 lets pages of open transactions reach disk. It will then need to know how far an interrupted rollback got, and its design adds that (section 9, decision 3).

### 2.7 Transactions that changed the schema

A transaction that ran DDL (`CREATE`/`DROP TABLE`/`INDEX`) **still rolls back by reopening**, as today.

**Why:**
- **The in-memory catalog is not versioned.**
- **Its undo covers only the system tables' rows.** It does not cover:
  - the heap and B+Tree pages it created;
  - the pages it freed (deferred frees), such as those of a dropped table.

**What it would take:** undoing those needs its own design: undo for page allocation and freeing, and reloading the catalog. That belongs with transactional DDL over the wire (6.10).

**Cost:** such transactions already hold the exclusive lock (16-snapshots-visibility.md section 2.4), so the reopen keeps out nobody who was not already waiting.

The executor knows which transactions ran DDL (`Tx.locked`). If the undo walk ever meets a system table record (table ID 0) anyway, it fails, and the database restarts itself (2.5).

### 2.8 Undo of a rolled-back transaction, and snapshots

**When it is released.** A rolled-back transaction's undo records are still needed for a while. A reader that copied a row version before the rollback restored it, by a scan or an index lookup, follows that version's undo pointer into the transaction's records.

- The rule of 16-snapshots-visibility.md section 2.6 extends naturally: **a transaction's undo is released once every live snapshot was taken after the transaction ended**, committed or rolled back.
- For a committed transaction this is the current rule: such a snapshot sees it.
- For a rolled-back one, it means no live snapshot still lists it as active.

**Index use after a rollback.** A snapshot uses indexes only if it sees the last writer (16-snapshots-visibility.md section 2.5). After a rollback, the last writer is the rolled-back transaction, which no snapshot ever sees, so indexes would stay unused until the next commit.

- **New rule:** a snapshot uses indexes if the last writer **ended before the snapshot was taken**, committed or rolled back: it is below `XMax` and not active.
- A rolled-back transaction's index changes are reversed before it ends, so the indexes then describe the state without it.

**The status table.**
- A rolled-back transaction's outcome is *aborted*, and snapshots copy it as they copy recovery's aborted transactions today.
- Once a checkpoint prunes it to *resolved*, a snapshot "sees" it. That is harmless: none of its changes remain (13-transactions.md section 2.3).

## 3. Formats

**No on-disk format change.**

- `TxnAbort` (WAL type 9, payload a u64 XID) was defined and approved in WAL format version 2 as reserved for this step (13-transactions.md section 2.2). Step 6.5 starts writing it. Recovery accepts it, after checking that it names the open transaction.
- Remove and Restore use the existing heap record operations.
- **Version numbers stay** (data 4, WAL 4).
  - A log written by 6.5 is refused by an older build, as a corrupt log (unknown type 9).
  - Logs and data files from 6.4 open unchanged.
  - Section 9, decision 2, asks whether to raise the WAL version anyway.

## 4. Concurrency

- **The rollback holds the writer slot**, like any write of the transaction. Readers run beside it, under the shared lock (16-snapshots-visibility.md section 2.4).
- **Readers during a rollback** see either the transaction's version of a row, which they skip through its undo, or the restored version. Both lead to the same visible version.
  - Each heap operation is latched and atomic.
  - The rolled-back transaction stays active until it has fully reversed, so no snapshot sees its changes in between.
- **Index scans during a rollback.** The last writer is the rolling-back transaction, which no snapshot sees, so index scans become table scans until it ends (2.8).
- **The undo of a rolled-back transaction** stays until every snapshot that could follow it has gone (2.8).
- **No reopen, so no exclusive lock.** A rollback no longer waits for running statements. Rollbacks of transactions that ran DDL still reopen (2.7).

## 5. Failure behaviour

| Event | Result |
|---|---|
| An SQL error, `40001` or `54000` partway through a statement | That statement's changes are undone in place; the transaction is failed (in autocommit, rolled back). |
| An I/O or logging failure, or corruption, partway through a statement | The database restarts itself; recovery discards the transaction (as today). |
| The rollback itself fails (I/O, a full pool, corruption, a record that does not match the row) | The database restarts itself; recovery discards the transaction, so the outcome is the same. |
| Crash during a rollback | No end record: the transaction is discarded whole. |
| Crash after `TxnAbort` is durable | The transaction's changes and their reversal are replayed. |
| Rollback of a transaction that ran DDL | Reopen, as today (2.7). |
| A rollback that would change more pages than the pool holds | `54000`, then the restart path; the transaction is discarded by recovery. |

## 6. Alternatives considered

- **Keep rollback by reopening.** It is exact and crash-tested, but every rollback stalls the database and ends every `REPEATABLE READ` snapshot. With rollbacks routine (failed statements, `40001`), that is not acceptable.
- **Undo records for index changes too.** Undo would grow, and there would be a second record format. The row's two versions already determine its index entries exactly (2.1). With the per-row order of 2.2, they are enough.
- **Skip an aborted transaction's records at recovery** (as for transactions without an end record). This is wrong once its pages can reach disk, and later records depend on its page layout (2.6).
- **ARIES compensation records with `UndoNext` now.** Nothing could read them before Step 6.9. They would be format and code with no test that exercises them. Step 6.9 designs them together with steal.
- **Statement rollback by reversing the statement's own list of changes,** instead of its undo records. That would be a second mechanism next to transaction rollback. Walking the undo back to a savepoint is one mechanism, and the same one savepoints will use.
- **Keep the slot of a rolled-back insert as a tombstone** until purge. It is not needed: no one can depend on an uncommitted row's slot (2.3). A tombstone would cost the page 22 bytes until Step 6.8.

## 7. Testing plan

- **Heap:**
  - `Remove` and `Restore` on every kind of tuple (plain, tombstone, stub with moved-in tuple), with and without moves.
  - The heap model test gains them.
- **`mvcc.Undo`:**
  - walks exactly the records between two pointers, newest first;
  - refuses records of another transaction, loops and bad pointers.
- **Rollback model test (acceptance):**
  - Random transactions over tables with several indexes: inserts, updates that move rows, deletes, the same row changed many times.
  - Each is rolled back whole, or has some statements rolled back first.
  - Afterwards, every row's version (RID, flags, XID, undo pointer, data) and every index's full contents must equal the snapshot taken before the transaction, byte for byte.
  - Page layout (slot positions, free space, which page a moved row sits on, B+Tree splits) may differ. Section 9, decision 1, asks to approve this definition of "identical".
- **Statement rollback:**
  - failures injected after every change of a statement (an index key too long, a `40001` conflict, the pool full);
  - only that statement's changes are gone;
  - the earlier statements' changes are still visible to the transaction;
  - `25P02` follows;
  - `Rollback` restores everything.
- **Failures during a rollback:**
  - I/O faults at every operation of a rollback;
  - the database restarts and the transaction is absent after recovery.
- **Crash tests:**
  - MemFS crashes at every WAL record of a rollback and of a statement rollback, and right after `TxnAbort`;
  - committed transactions after a rolled-back one (with B+Tree splits and slot reuse in between) replay correctly;
  - the SQL crash harness's rollbacks now use undo.
- **Concurrency, under `-race`:**
  - The 6.4b model checker with rollbacks done by undo.
  - `REPEATABLE READ` readers no longer see `40001` from other sessions' rollbacks, and check every read.
  - A rolled-back transaction's undo is kept while a snapshot needs it, then released.
  - Indexes are usable again right after a rollback.
- **Recovery:**
  - `TxnAbort` must name the open transaction;
  - an aborted transaction is replayed and has status aborted;
  - an abort without a begin is corrupt.
- **Deliberate bugs:** about 25–30, as before: four at a time, on copies of the code, each against its package's tests with a 60-second limit.

## 8. Limitations (Step 6.5)

- **A transaction that ran DDL rolls back by reopening** (2.7).
- **A transaction, and its rollback, must fit in the buffer pool** ("no steal", until 6.9).
- **No savepoints in SQL** (6.10). The mechanism (2.4, 2.5) is the one they will use.
- **Page layout after a rollback may differ from before** (B+Tree splits stay, a moved row may sit on another page). The data and every version chain are identical.

## 9. Decision points for review

1. **What "byte-identical" means (acceptance).** Proposed: after a rollback, every row's version (RID, flags, XID, undo pointer, data bytes) and every index's full contents are identical. Raw page bytes are not: slot positions, free space, B+Tree splits and page LSNs may differ, as in PostgreSQL and InnoDB.
2. **WAL format version.** Proposed: **no change.** `TxnAbort` was already approved as type 9 in version 2, and existing databases (including your demo data) keep opening. The alternative raises the WAL version to 5: older builds would then report "unsupported version" instead of "corrupt" on a new log, and 6.4 logs would be refused.
3. **No separate compensation record type now.** Proposed: the rollback's changes are ordinary logged records of the transaction (2.6); Step 6.9 adds what "steal" needs.
4. **DDL transactions keep reopen-based rollback** (2.7) until transactional DDL is designed (6.10).
5. **Planned design doc numbers move by one.** This doc takes number 17, so locking becomes `18-locking.md`, MVCC indexes `19-mvcc-indexes.md`, and purge `20-purge.md` (PROGRESS.md).
