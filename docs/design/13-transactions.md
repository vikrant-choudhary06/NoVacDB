# 13 — Transactions (`internal/wal`, `internal/sql/executor`)

Status: **Step 6.1 (transaction manager) designed, approved and implemented** (notes in section 2.8). Later Phase 6 steps extend this document or have their own (14-undo-log.md through 20-purge.md, see PROGRESS.md).

## 1. Problem

Until now the unit of atomicity is one statement (10-executor.md section 2.4): every statement that changes data runs as a WAL *statement group*, and a crash keeps or drops each group whole. Phase 6 needs **transactions**: many statements that commit or roll back together, each identified by a transaction ID that later steps store in rows (Step 6.3), compare against snapshots (Step 6.4), undo (Steps 6.5 and 6.9) and purge by (Step 6.8).

Step 6.1 lays the part everything else stands on:

- **64-bit transaction IDs.** Allocated in increasing order and never reused, across restarts and crashes, so they can never wrap around (WORKFLOW.md problem #3).
- **Transaction states** (active, committed, aborted), and a status table that answers "did transaction X commit?".
- **`Begin`, `Commit` and `Rollback` at the engine and executor level.** Every statement outside an explicit transaction runs in its own autocommit transaction.
- **A commit record in the WAL.** `Commit` returns only once that record is durable, and a transaction without a durable commit record counts as aborted after recovery.

Out of scope (PROGRESS.md): undo, row versions, visibility and locking. Without them a transaction cannot run beside other writers, or roll back cheaply. Section 2.6 says what Step 6.1 does instead until then, and which later step replaces each piece.

## 2. Design

### 2.1 Transaction IDs

- **Type:** `wal.XID`, an unsigned 64-bit integer. `0` means "no transaction" and is never allocated; the first ID is `1`.
- **Allocated lazily, on the first write.** A transaction that only reads never gets an ID and writes nothing to the WAL, as in PostgreSQL. A read-only workload then costs no IDs and no log volume, and the IDs that exist are exactly those of transactions that changed something.
- **Never reused.** The next ID to allocate, `NextXID`, is recovered from two places:
  - The **checkpoint record**, which carries the `NextXID` of when it was taken (section 3).
  - The log after the redo point. Each transaction's first record names its ID (section 2.2), so recovery sets `NextXID = max(checkpoint's NextXID, highest ID in the log + 1)`.

  An ID is logged when it is allocated, before anything it changes can reach disk, so every ID that could appear in the database is covered. An ID allocated and lost in a crash before its record became durable may be handed out again. That is harmless: nothing refers to it.
- **Exhaustion.** At a million transactions a second, 64 bits last about 580,000 years. Even so, allocation refuses to pass `2^64 - 1` with an error instead of wrapping.

### 2.2 WAL records

Transactions replace statement groups. Two new record types, and one reserved:

| Type | Name | Payload | Written |
|---|---|---|---|
| 7 | `TxnBegin` | XID (u64) | When the transaction is allocated its ID, at its first write, before any of its changes. |
| 8 | `TxnCommit` | XID (u64) | By `Commit`, which returns once the log is durable through it. |
| 9 | `TxnAbort` | XID (u64) | Reserved for Step 6.5, whose rollback ends with it once the undo is applied. Step 6.1 never writes it (section 2.6). |

Statement groups (types 4 and 5) are retired: an autocommit transaction is the statement group's replacement, with an ID. In the new log format (section 3) types 4 and 5 are unknown types, and therefore corrupt. Records outside any transaction (the crash tests of Phases 2 and 3 write such records) still replay as before.

### 2.3 States and the status table

A transaction is **active** from `Begin` to `Commit` or `Rollback`. It then becomes **committed** (its commit record is durable) or **aborted**.

The engine's status table answers `Status(xid)`:

- **Active:** the transaction is in the table of active transactions, in memory.
- **Committed:** a durable commit record was seen, either since startup or during recovery.
- **Aborted:** the transaction rolled back since startup, or recovery found it begun and never committed.
- **Resolved:** the ID is older than everything the table remembers, below `NextXID` and not active. It is either committed, or aborted with every change already removed. For visibility (Step 6.4) the two are the same, because an aborted transaction's changes are gone before it leaves the active table; Step 6.5 keeps this true by rolling back before removing the ID. PostgreSQL's `txid_status()` returns NULL for IDs past its horizon in the same way.
- **Unknown:** the ID is `0` or at least `NextXID`: never allocated.

The table remembers outcomes from the current redo point on. Each checkpoint forgets the outcomes of transactions that ended before its redo point. Its size is therefore bounded by the work between two checkpoints, and recovery can rebuild it from the log alone.

**Why not a persistent commit log** (PostgreSQL's `pg_xact`, two bits per ID): it grows with every transaction and has to be truncated by a VACUUM-like process. With undo-based MVCC, visibility only needs to know which transactions were active when a snapshot was taken (Step 6.4): a transaction that is not active is committed, or its changes are already undone. Section 6 has the trade-off.

### 2.4 The engine API (`internal/wal`)

```go
type XID uint64

func (e *Engine) Begin() *Txn            // starts a transaction; no ID, no log record yet
func (t *Txn) XID() XID                   // 0 until the first write
func (t *Txn) Write(ctx context.Context, fn func(ctx context.Context) error) error
func (t *Txn) Commit(ctx context.Context) (LSN, error)
func (t *Txn) Rollback(ctx context.Context) error
func (e *Engine) Status(xid XID) TxnStatus
func (e *Engine) NextXID() XID
```

**`Write`** runs one change (one statement's apply phase) inside the transaction:

1. On the transaction's first `Write`, it waits for the engine's writer slot (section 2.6), allocates the ID, and logs `TxnBegin`.
2. It runs `fn`. The heap and B+Tree changes `fn` makes are logged as today.

If `fn` or the logging fails, the transaction can no longer commit. It is marked failed, and `Rollback` is the only call it still takes.

**`Commit`:**
- For a transaction that never wrote, `Commit` only ends it.
- Otherwise it logs `TxnCommit`, makes the log durable through it, marks the transaction committed, and releases the writer slot.
- If logging or flushing fails, the outcome is unknown, as with `CommitStatement` today. The engine must then be abandoned and recovered, which decides the outcome either way.

**`Rollback`** for a transaction that never wrote only ends it. Otherwise it removes the transaction's changes (section 2.6).

### 2.5 The executor API and autocommit (`internal/sql/executor`)

```go
func (db *DB) Begin(ctx context.Context) (*Tx, error)
func (tx *Tx) Exec(ctx context.Context, sql string) ([]*Result, error)
func (tx *Tx) ExecPrepared(ctx context.Context, p *Prepared, values []types.Value) (*Result, error)
func (tx *Tx) Commit(ctx context.Context) error
func (tx *Tx) Rollback(ctx context.Context) error
```

- **Autocommit.** `DB.Exec` and `DB.ExecPrepared`, used outside a transaction, run each statement in its own transaction, which commits at the end of the statement. This is today's statement group, now with an ID. A `SELECT` gets no ID and writes nothing, as today.
- **Errors inside a transaction.** As in PostgreSQL, a statement that fails inside an explicit transaction leaves the transaction *failed*: every later `Exec` returns `25P02` "current transaction is aborted, commands ignored until end of transaction block", and `Commit` rolls back instead and reports so. Savepoints are later (Step 6.10 decides whether to support them).

The SQL statements `BEGIN`, `COMMIT` and `ROLLBACK`, and `ReadyForQuery`'s transaction status, are Step 6.10. Step 6.1 offers transactions through the Go API only.

### 2.6 What Step 6.1 does until undo, versions and locks exist

Three things need later steps. Step 6.1 does each in the simplest correct way, and says which step replaces it.

1. **One writing transaction at a time.**
   - The engine's statement mutex becomes a writer slot. A transaction takes it at its first write and holds it until it commits or rolls back.
   - In the executor, a transaction that has written holds the database's exclusive lock until it ends. No other statement, reader or writer, runs in between, so no session can see uncommitted changes.
   - Read-only transactions run concurrently with each other, as statements do today.
   - Replaced by row versions, snapshots and row locks (Steps 6.3, 6.4 and 6.6).
2. **Rollback by discarding.**
   - "No steal" keeps every page a transaction changed in the buffer pool until it commits, and recovery skips a transaction without a commit record.
   - So `Rollback` of a transaction that wrote does what a failed statement does today (10-executor.md section 2.4): abandon the engine as a crash would, and reopen it. Recovery discards the transaction.
   - It costs a reopen, but it is exact, and it is the same path that is already crash-tested.
   - Replaced by undo-based rollback (Step 6.5).
3. **A transaction can change at most what the buffer pool holds**, as a statement can today. Past that it fails with `54000` "transaction changes too much data" and is rolled back. While a writing transaction is open, checkpoints wait for it. Both are replaced once undo makes "steal" possible (Steps 6.2 and 6.9).

These interim rules hold only in Step 6.1's Go API, which no client can reach. No transaction can be left open by a client until Step 6.10 puts transactions on the wire, and by then Steps 6.3 to 6.6 have replaced points 1 and 2.

### 2.7 Recovery

The first pass of recovery, which today finds unfinished statement groups, now finds unfinished transactions:

- **Unfinished:** a `TxnBegin` with no `TxnCommit` for the same ID before the end of the log.
- **What it does with them:** it skips their records, rebuilds the status table, and raises `NextXID` past every ID it saw.

With one writer at a time, a transaction's records are contiguous in the log, as statement groups are today. A `TxnBegin` while another transaction is open therefore means the earlier one was abandoned, and is corrupt otherwise. Step 6.6 allows interleaved transactions, and its design changes the pass to track transactions by ID.

The end-of-recovery checkpoint records the recovered `NextXID`.

### 2.8 Implementation notes (Step 6.1)

- **The status table keeps outcomes for two checkpoint intervals, not one.** A checkpoint forgets the outcomes that ended before the *previous* checkpoint's redo point.
  - With the one-interval rule of section 2.3, the end-of-recovery checkpoint, whose redo point is the end of the log, would forget everything recovery had just found. A transaction without a commit record would then read "resolved" instead of "aborted".
  - The table is still bounded, and still rebuilt from the log alone: within one run of the engine it covers the last two checkpoint intervals, and after a restart, the log from the last checkpoint's redo point. An outcome older than that reads "resolved", as section 2.3 allows.
- **`wal.Unchanged(err)`.** The executor's DDL keeps the catalog's rule that a `*sqlerr.Error` means nothing changed (10-executor.md section 2.4).
  - A `Write` function returns such an error wrapped in `Unchanged`. The transaction then stays usable, and `Write` returns the error itself.
  - In autocommit, the statement's transaction then commits, empty, as its statement group did. In an explicit transaction, the executor marks the transaction failed (`25P02`), as PostgreSQL does after any error.
- **Recovery's first pass checks every record's type.** Replay already refused unknown types, but only for records it replays.
  - A record inside a discarded transaction was never looked at, so a version-1 statement record, or the reserved `TxnAbort`, would have gone unnoticed there.
  - The first pass now refuses any type it does not know, wherever the record is. This was found by a test written for the reserved type.
- **IDs are checked in recovery.** Each `TxnBegin` must name a non-zero ID greater than every earlier one in the log; a repeated or decreasing ID is corruption. Each `TxnCommit` must name the open transaction's ID.
- **Inside a transaction, use the transaction's methods.** Once a transaction has written, it holds the executor's exclusive lock (section 2.6). The DB's own methods, `DB.Exec` or `DB.Prepare` included, called from the transaction's goroutine would wait for it forever. `Tx` documents this. A first version of the tests did exactly that: it prepared a statement mid-transaction and hung, which is what prompted the documentation.
- **Rollback counts.** The executor counts rollbacks that reopened the database, alongside self-restarts. Tests use the count to prove that a read-only transaction's rollback reopens nothing.

## 3. Formats (approved)

These are the only on-disk changes in Step 6.1. The phase's rules approve a format change only for Step 6.3, so this one needed the maintainer's approval, which was given with the design.

- **WAL format version 2.**
  - The checkpoint record's payload grows from 8 to 16 bytes: the redo LSN, then `NextXID`, both u64 little-endian.
  - Record types 7 and 8 are added, and 9 is reserved.
  - Types 4 and 5 are retired.
  - Segments of version 1 are refused with `ErrUnsupportedVersion`, as the version rule already does. There is no production data to migrate.
- **No change to the data file, the control file or the catalog.**

The alternative that avoids a format change is a separate `NextXID` file written at each checkpoint. That is a format change too, and one more file to keep consistent with the log, so the checkpoint record is the better home.

## 4. Concurrency

- **Writers:** the writer slot (section 2.6) serialises them. The executor's exclusive lock, held by a transaction from its first write to its end, keeps readers out of uncommitted changes.
- **`NextXID` and the status table:** both are under the engine's mutex; `Status` takes it briefly.
- **Ordering of `Commit`:** the commit record is appended and flushed before the slot is released. The next writer's records therefore follow the commit in the log, and recovery's contiguity check holds.

## 5. Failure behaviour

| Event | Result |
|---|---|
| Crash before `TxnCommit` is durable | Recovery discards the transaction; `Status` says aborted. |
| Crash after `TxnCommit` is durable, before `Commit` returns | The transaction is committed: present after recovery, `Status` committed (the client may not know; the same as PostgreSQL). |
| `fn` fails inside `Write` | The transaction is failed; `Rollback` discards it (section 2.6). |
| Logging or flushing `TxnCommit` fails | Outcome unknown until recovery; the engine is abandoned and reopened. |
| A transaction outgrows the buffer pool | `54000`; it is rolled back. |
| `NextXID` would pass 2^64-1 | Allocation error; the transaction fails. |
| A log of format version 1 | Refused at open with `ErrUnsupportedVersion`. |

## 6. Alternatives considered

- **A persistent commit log (`pg_xact`).** It gives an exact answer for every ID ever allocated, but it grows by two bits per transaction forever unless something truncates it. That truncation is part of what VACUUM does, and NoVacDB's goal is to need no such thing. Undo-based visibility does not need it (section 2.3).
- **Allocating the ID at `Begin`.** That is simpler, but read-only transactions would use up IDs and write log records for nothing. PostgreSQL also allocates lazily.
- **Keeping statement groups and adding transactions above them.** That gives two nested atomic units in the log, and recovery would have to reconcile them. An autocommit transaction is exactly a statement group with an ID, so one mechanism is enough.
- **Doing rollback, versions and locks in this step.** These are Steps 6.2 to 6.6, each with its own review. Step 6.1 stays small and leaves each of them a clear seam (section 2.6).

## 7. Testing plan

- **IDs:**
  - They increase strictly, and are allocated only on a first write.
  - `NextXID` survives restarts, and survives crashes at every point: after `TxnBegin`, after changes, between appending and flushing `TxnCommit`, after the commit, and during a checkpoint.
  - The recovered `NextXID` exceeds every ID in the log and the checkpoint.
  - Exhaustion is tested with an injected starting ID near 2^64.
- **Status table:**
  - Active, committed and aborted, both before and after recovery.
  - Resolved and unknown at the edges.
  - Pruning at checkpoints keeps it bounded.
- **Crash harness (`tests/crash`):** gains multi-statement transactions that commit or roll back at random, over MemFS power cuts and torn writes and real SIGKILLs. After recovery, every committed transaction is wholly present, every other is wholly absent, and `Status` agrees.
- **Executor:**
  - Autocommit is unchanged: the whole existing suite and the SQL logic tests pass.
  - `Begin`/`Exec`/`Commit`, and `Rollback` after inserts, updates, deletes and DDL, leave the database as before (compared with a model).
  - The failed-transaction state (`25P02`).
  - A transaction that outgrows the pool.
  - Readers blocked while a writing transaction is open, under `-race`.
- **Format:** decoding of the new records and the 16-byte checkpoint payload, with a fuzz target. Version 1 logs are refused.
- **Deliberate-bug checks:** as in every step.

## 8. Limitations (Step 6.1)

- **One writing transaction at a time.** A writing transaction also blocks readers until it ends (section 2.6, until Steps 6.3–6.6).
- **Rollback reopens the database** (until Step 6.5; since then, only for transactions that changed the schema, 17-rollback.md).
- **A transaction is bounded by the buffer pool, and checkpoints wait for it** (until Steps 6.2 and 6.9).
- **No SQL syntax or wire support yet** (Step 6.10).
- **`Status` gives exact outcomes only from the last checkpoint's redo point on.** Older IDs are "resolved" (section 2.3).
