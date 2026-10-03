# 10 — SQL Execution: Types, Rows, Catalog, Atomic Statements, Executor

Packages: `internal/sql/types` (Step 4.3), `internal/catalog` and `internal/wal` (Step 4.4), `internal/sql/executor` (Step 4.5).

Status: **Designed for Steps 4.3–4.5; each part is implemented in its step. Implemented: 4.3, 4.4, 4.5.** Written and approved under the standing autonomous-mode instruction.

The three steps share one document because they constrain each other: the row format serves the catalog, the catalog's changes must be atomic like any statement's, and the executor's write path is built around that atomicity.

## 1. Problem

The parser produces syntax trees. To run them NoVacDB needs:

- **values and types** with PostgreSQL's semantics: `NULL` and three-valued logic, comparisons, arithmetic with overflow errors, casts, and the text input and output formats clients see;
- **rows on disk**: an encoding of a row into a heap tuple, and of index columns into B+Tree keys that sort like the values;
- a **catalog** of tables, columns and indexes, stored in the database itself and loaded at startup;
- **statements that are all or nothing**, even across a crash. Transactions are Phase 6, but an `INSERT` of ten rows into a table with two indexes is thirty page changes, and a crash must never leave five rows, or a row without its index entries;
- an **executor** that plans and runs each statement and reports results and errors as PostgreSQL would.

## 2. Design

### 2.1 Types and values (Step 4.3)

| SQL type | `types.Type` | Go value | text output |
|---|---|---|---|
| `integer` | `Int4` | int32 (held as int64) | `42` |
| `bigint` | `Int8` | int64 | `42` |
| `double precision` | `Float8` | float64 | `1.5`, `1e+20`, `NaN`, `Infinity` |
| `text` | `Text` | UTF-8 string | as is |
| `boolean` | `Bool` | bool | `t`, `f` |
| `timestamptz` | `TimestampTZ` | int64 microseconds since 2000-01-01 00:00:00 UTC (PostgreSQL's representation); `±infinity` are the int64 extremes | `2024-01-02 03:04:05.5+00` |
| (string literal, `NULL`) | `Unknown` | string | |

A `types.Value` is a type, a null flag and the payload. Every operation is a pure function that returns a value or a `*sqlerr.Error`.

**NULL.** Comparisons and arithmetic with a NULL operand are NULL. `AND`, `OR` and `NOT` follow SQL's three-valued logic (`FALSE AND NULL` is `FALSE`, `TRUE OR NULL` is `TRUE`). `IS [NOT] NULL` and `IS [NOT] DISTINCT FROM` are never NULL.

**Comparison** (`Compare`) is a total order per type: numbers numerically, with `NaN` equal to itself and above every other number and `-0 = +0` (PostgreSQL's order for `double precision`); text by bytes (C collation, the only collation in Phase 4); `false < true`; timestamps by instant.

**Arithmetic.** `+ - * / %` on integers (operands already converted to one type by the executor) check overflow (`22003`, "integer out of range" / "bigint out of range"); division and modulo by zero are `22012`; integer division truncates toward zero. On `double precision`, a finite computation that overflows to infinity is `22003` ("value out of range: overflow") and one that underflows to zero is "value out of range: underflow"; division by zero is `22012`. `^` is computed in `double precision`. Unary minus checks overflow. `||` concatenates text, converting a non-text operand to its text form, as PostgreSQL does. `LIKE`/`ILIKE` match `%` and `_` with backslash escapes (a trailing backslash is `22025`); `ILIKE`, `lower` and `upper` change only ASCII letters, as the C collation does.

**Casts.** (`types.Cast`; `types.CanCast` tells the executor which exist.) `int4 ↔ int8` (range-checked), integers → `double precision`, `double precision` → integers (rounded half to even, range-checked, NaN and infinities rejected), `int4 ↔ boolean`, anything → `text` (the output form, except that `boolean` becomes `true` or `false`, as PostgreSQL's cast spells it, while its output form is `t` or `f`), `text` → anything (the input form). `timestamptz` converts only to and from text.

**Input forms** follow PostgreSQL:

- integers: surrounding whitespace, a sign, decimal digits, and PostgreSQL 16's `0x`/`0o`/`0b` prefixes and `_` separators; otherwise `22P02` "invalid input syntax for type integer: "..."", and out-of-range values `22003`;
- `double precision`: decimal and exponent forms, `NaN`, `Infinity`, `inf`, with signs and any letter case. Underscores and hexadecimal floats are rejected, and a non-zero value that rounds to zero (`1e-400`) is out of range, as in PostgreSQL;
- `boolean`: `t`/`true`/`y`/`yes`/`on`/`1` and `f`/`false`/`n`/`no`/`off`/`0`, any case, surrounding whitespace, and unambiguous prefixes (`tr`, `of`), exactly as PostgreSQL's `parse_bool`;
- `timestamptz`: ISO 8601, `YYYY-MM-DD[( |T)HH:MM[:SS[.fraction]]][zone]`, where the zone is `Z`, `UTC`, `GMT` or a numeric offset `±HH`, `±HHMM`, `±HH:MM` or `±HH:MM:SS`. With no zone the session time zone applies, which in Phase 4 is always UTC. Fractions are rounded to microseconds, half to even, in one step on the exact decimal digits. (A first version rounded to nanoseconds and then to microseconds; mutation testing exposed the double rounding, which turned `.0000014995` into 2 µs.) Dates before 0001-01-01 UTC are rejected. `epoch`, `infinity` and `-infinity` are accepted. Years 1 to 9999. Invalid dates and times are `22008`; anything else unreadable is `22007`.

**Output forms** follow PostgreSQL with `DateStyle = ISO` and `TimeZone = UTC`. `double precision` prints the shortest text that reads back as the same value, in fixed notation when the decimal exponent is between -4 and 14, otherwise as `1.5e+20`.

**Type resolution** for operators (used by the executor):

- an untyped literal (`'...'` or `NULL`) takes the type of the other operand, so `ts = '2024-01-01'` compares timestamps;
- `int4` with `int8` computes in `int8`; an integer with `double precision` computes in `double precision`;
- anything else mismatched is `42883` "operator does not exist: text = integer", with a hint to add an explicit cast.

Integer literals are `integer` if they fit in 32 bits, else `bigint`; decimal literals are `double precision`.

### 2.2 Rows on disk (Step 4.3)

A row is a heap tuple:

| offset | size | field |
|---|---|---|
| 0 | 1 | format version, `1` |
| 1 | 2 | column count *n* |
| 3 | ⌈*n*/8⌉ | null bitmap: bit *i* (LSB first) set means column *i* is NULL |
| … | | non-NULL columns in order |

Column encodings, little-endian: `integer` 4 bytes, `bigint` 8, `double precision` 8 (IEEE 754 bits), `boolean` 1 (`0` or `1`), `timestamptz` 8, `text` a u32 byte length then the bytes.

Decoding needs the table's column types. It checks every length and value, so a damaged tuple is an error, never a panic. The stored column count allows `ALTER TABLE ADD COLUMN` later (Phase 10): a tuple with fewer columns than the table reads the missing ones as NULL. A row longer than a heap tuple can be (8148 bytes) is rejected with `54000` "row is too big"; there is no out-of-line storage for large values yet.

### 2.3 Index keys (Step 4.3)

An index entry's key is its columns encoded with the B+Tree's order-preserving key encoding (08-btree.md section 2.1): integers and timestamps as int64, `double precision` as float64 (which already makes `-0 = +0` and orders NaN last, as `Compare` does), text and booleans as themselves, NULL as NULL (sorting last). Its value is the row's RID (u64 page, u16 slot).

- **Every** index appends the RID to the key, so every entry is unique and entries for equal values sort by location. (Until the B+Tree's revision 2, a unique index's key was the columns alone; storing the RID everywhere lets Phase 6 keep a delete-marked entry and a live one with equal columns side by side, 08-btree.md section 2.11.)
- A **unique** index enforces uniqueness on the columns with a prefix probe: the entries whose keys start with the new row's encoded columns (`Index.UniquePrefix`, `Index.Lookup`). NULLs are never equal to each other, so a row with a NULL in any key column has no prefix to check (any number of rows may have NULLs, as in PostgreSQL).

A key longer than a B+Tree key may be (1024 bytes, RID included) is rejected with `54000`.

### 2.4 Statements are atomic (Step 4.4)

*Since Step 6.1, statement groups are replaced by transactions (13-transactions.md): each statement outside an explicit transaction runs in its own autocommit transaction, whose `TxnBegin` and `TxnCommit` records (types 7 and 8, with the transaction ID) take the place of the statement records (types 4 and 5) below. The rules of this section, "no steal", recovery skipping what never committed, and the restart after a failure, are unchanged.*

Every statement that changes the database runs as a **statement group** in the WAL:

1. `Engine.BeginStatement` appends a *statement-begin* record (type 4) and sets the **commit horizon** to its LSN.
2. The statement makes its changes; heaps and B+Trees log them as usual.
3. `Engine.CommitStatement` appends a *statement-commit* record (type 5, naming the begin LSN), makes the log durable through it, clears the horizon, and returns the commit LSN. Only then is the statement acknowledged.

Two rules make the group all or nothing.

- **No page changed by an unfinished statement reaches disk.** While a horizon is set, the WAL-rule hook the buffer pool already consults (`FlushedLSN`) reports the log as durable only up to the horizon. Every page the statement changed carries an LSN at or after the horizon, so the pool cannot evict or flush it; eviction picks other pages. ("No steal", built on the existing WAL rule without changing the buffer pool.) A checkpoint cannot run during a statement either: the engine makes it wait.
- **Recovery replays only committed groups.** A first pass over the log, from the redo point, finds every group that began and never committed: a begin followed by another begin, by a checkpoint record (a checkpoint never runs inside a statement, so one that follows an open group means the group was abandoned) or by the end of the log. The second pass replays everything except those groups' records. Records outside any group (from code that does not use statements, such as the crash tests of Phases 2 and 3) replay as before.

The disk never holds a change from an uncommitted statement, and the log holds the rest, so after recovery each statement is entirely present or entirely absent.

**A statement that fails after it started changing pages** (an I/O error, a full buffer pool, a corrupt page) cannot be undone in memory: there is no undo until Phase 6. The database **restarts itself**:

1. `Engine.Abandon` drops the buffer pool without writing it and closes the files, exactly as a crash would.
2. The database reopens, and recovery discards the unfinished group.
3. The failed statement reports its error; the next statement runs normally.

If reopening fails, the database stays closed and every statement reports that it is unavailable. Context cancellation is honoured only before a statement starts changing data. Once it has started, it runs to the end, so cancelling a query never forces a restart.

The cost of "no steal" is that a statement can change at most as many pages as the buffer pool holds. A bigger statement fails with `54000` "statement changes too much data" (and the restart above). The default pool is 4096 pages (32 MiB). Phase 6's undo log removes this limit.

### 2.5 Catalog (Step 4.4)

The catalog is three heaps, **system tables**, whose rows use the row format of 2.2:

| table | columns |
|---|---|
| `novac_tables` | `id bigint, name text, heap bigint` (first page) |
| `novac_columns` | `table_id bigint, position integer, name text, type integer, not_null boolean, default_expr text` |
| `novac_indexes` | `id bigint, name text, table_id bigint, root bigint, is_unique boolean, is_primary boolean, columns text` |

`default_expr` is the default expression as SQL text, printed from its syntax tree and parsed again at load. `columns` lists the indexed column positions, e.g. `2,0`. `type` holds the `types.Type` number.

**Bootstrap.** The first pages of the three heaps are kept in a small **catalog file** in the database directory:

| offset | size | field |
|---|---|---|
| 0 | 8 | magic `NVDBCATL` |
| 8 | 4 | version, `1` |
| 12 | 24 | three u64 first-page IDs |
| 36 | 4 | CRC-32C of bytes 0–35 |

It is written once, when the database is created: the three heaps are created in a statement group, the group commits, and then the file is written to a temporary name, fsynced and renamed into place, and the directory is fsynced. A crash before the rename leaves no catalog file; the next open creates the catalog again (leaking three pages). The file is never rewritten afterwards.

**Loading.** At open, after WAL recovery, the catalog scans the three heaps into memory: tables with their columns, indexes and the RIDs of their catalog rows. Inconsistent catalog contents (an index on a missing table, a duplicate name, an unknown type) are `XX001` and the database does not open.

**Names.** Tables and indexes share one namespace, as relations do in PostgreSQL. A `CREATE TABLE` of an existing name is `42P07`; `IF NOT EXISTS` turns it into a notice. Names beginning `novac_` are reserved for system tables (`42939`). Object IDs come from a counter that starts above the largest ID loaded.

**DDL.** All of it runs inside one statement group:

- `CREATE TABLE` creates the heap and the catalog rows, and an index for the primary key and for each unique constraint. Primary key columns are `NOT NULL`. Index names follow PostgreSQL: `t_pkey`, `t_a_key`, `t_a_b_idx`, with a number appended on a clash.
- `CREATE INDEX` first scans the table and checks uniqueness for a unique index (`23505`, with the duplicated key). Only then does it create the B+Tree and fill it.
- `DROP TABLE` deletes the catalog rows of the table, its columns and its indexes. `DROP INDEX` deletes the index's row; a primary key's index cannot be dropped alone (`2BP01`).
- The pages of a dropped table or index are freed as unlinked B+Tree pages are: the statement logs them in a deferred-free record inside its statement group, before the commit, and the engine's checkpointer frees them once its redo point has passed that record (08-btree.md section 2.7). The request is discarded with an uncommitted statement and survives a crash with a committed one.

**Errors and abandoning.** The DDL methods follow one rule, which tells the executor whether a failed statement can still commit: a `*sqlerr.Error` means nothing was changed; any other error (I/O, a full pool, a damaged page) may leave part of the change made, and the executor must abandon the statement (2.4). Everything that can fail with a SQL error is therefore checked before the first change: names (reserved, taken, empty or over 63 bytes), duplicate and missing columns, column and key-column counts, the size of each catalog row (a long `DEFAULT` can overflow one), and, for `CREATE INDEX` on a table with rows, uniqueness and key sizes.

### 2.5.1 Implementation notes (Step 4.4)

- **Index column limit.** An index (and so a primary key or unique constraint) may name at most 32 columns, PostgreSQL's `INDEX_MAX_KEYS` (`54011`). Besides matching PostgreSQL, it bounds the `columns` text of a catalog row, so that row always fits.
- **Listing pages to free.** `storage.Heap.Pages` returns a heap's chain; `btree.Tree.Pages` walks a tree and requires every child to sit exactly one level below its parent and to be reached once, so a damaged tree fails with `ErrCorruptNode` instead of looping or listing a page twice. `DropTable` and `DropIndex` list every page before their first change.
- **Recovery statistics.** `RecoveryStats` gains `DiscardedStatements` (groups dropped) and `Discarded` (their records, begin records included). Tests use them to pin the boundaries of each discarded group exactly.
- **A pre-existing flaky test, fixed.** `TestConcurrentWritersReadersScanners` (Phase 3) failed almost every time without the race detector: its writers drifted out of step, so the tree never shrank enough to merge, and its "merges happened" assertion failed. Its writers now meet at a barrier between grow and shrink phases, as the test intended; the assertion is unchanged.

### 2.6 Executor (Step 4.5)

`executor.Open(ctx, fsys, dir, opts)` opens (or creates) a database: the WAL engine with recovery, then the catalog. `DB.Exec(ctx, sql)` runs every statement in the text in order and returns one `Result` per statement, stopping at the first error. A result has a command tag (`SELECT 3`, `INSERT 0 2`, `UPDATE 1`, `DELETE 0`, `CREATE TABLE`, ...), the output columns' names and types, the rows, and any notices.

**Concurrency.** One database-wide reader/writer lock. `SELECT` takes it shared, so reads run concurrently. Every statement that changes anything takes it exclusively, so there is one writer at a time and readers never see a statement half done. (Phase 6 replaces this with MVCC.)

**Binding.** Each statement is checked against the catalog before it runs. Unknown tables (`42P01`), columns (`42703`) and functions (`42883`), ambiguous references, and type errors are reported with the position of the offending name or expression, plus a hint where one helps (a similar column name, an explicit cast). Expressions compile to a tree of typed nodes. A constant subexpression is still evaluated per row, so errors such as division by zero appear only when the expression is evaluated, as in PostgreSQL.

**Supported functions:** `lower`, `upper`, `length` (characters), `abs`, `coalesce`, `nullif`, `greatest`, `least`, and `now()` (the statement's start time).

**Output column names** follow PostgreSQL: a column keeps its name, a function call its function name, a cast the name of what is cast, `CASE` is `case`, and anything else `?column?`, unless an alias is given.

**Plans** use the iterator (Volcano) model:

```
Limit/Offset ← Distinct ← Project ← Sort ← Filter ← (SeqScan | IndexScan)
```

- **Index scan.** The planner looks at the `WHERE` clause's top-level `AND` terms of the form `column op constant` or `constant op column` (`=`, `<`, `<=`, `>`, `>=`, and `BETWEEN`, which binds to two such terms) where the comparison is done in the column's own type (so the column is not widened) and the constant side is a literal, or casts of one, that evaluates without error to a non-NULL value. That value is exactly what the comparison compares with, so its key encoding is the encoding of equal column values. It picks the index with the longest prefix of equality terms, plus a range on the next column. The index scan reads the key range and fetches each row; the whole `WHERE` is still applied afterwards, so the index only narrows the rows read and never decides correctness.
- **Sort** sorts in memory, stably. `ORDER BY` accepts expressions, output column numbers and output column names. NULLs sort last ascending and first descending unless `NULLS FIRST/LAST` says otherwise. With `DISTINCT`, `ORDER BY` expressions must be in the select list (`42P10`).
- `LIMIT` and `OFFSET` take constant expressions; negative values are `2201W`/`2201X`.

**Writes** run in two phases, both under the exclusive lock:

1. **Compute and check.** Read the target rows (through the same scan planning), compute the new rows, apply defaults, and convert values to the column types. Check `NOT NULL` (`23502`), row and key sizes (`54000`), and uniqueness against the index (a prefix probe on the new row's columns; an entry conflicts unless its row is one the statement changes) and within the statement (`23505`, with PostgreSQL's detail "Key (a)=(1) already exists."). Uniqueness is checked against the state after the whole statement, as the SQL standard specifies: `UPDATE t SET id = id + 1` succeeds even when ids are consecutive. Nothing has changed yet, so any error simply returns.
2. **Apply** inside a statement group: for `UPDATE` and `DELETE`, first remove the old index entries, then change the heap, then add the new index entries; commit.

Assignments convert values as PostgreSQL's assignment casts do: between numeric types (range-checked), from untyped literals by parsing, and from anything to `text`. Other mismatches are `42804` "column "a" is of type integer but expression is of type text".

**Checkpoints.** After a write statement commits, the executor takes a checkpoint if the log has grown by more than `CheckpointBytes` (64 MiB by default) since the last one. `Close` ends with a checkpoint.

### 2.6.1 Implementation notes (Step 4.5)

- **Short-circuit evaluation.** `AND` stops at a FALSE left operand, `OR` at a TRUE one, `CASE` evaluates only the branch it takes and `coalesce` stops at the first non-NULL argument, as PostgreSQL's executor does, so `x <> 0 AND 10 / x > 1` never divides by zero.
- **Errors that depend on rows depend on the plan.** An index scan reads a subset of the rows, in key order, so a run-time data error (class 22 or 23) that a sequential scan meets in some row may not happen, or another row's error may come first. PostgreSQL behaves the same way. The randomized tests accept exactly this and nothing else.
- **Constants are not folded.** PostgreSQL folds constant subexpressions when planning, so `WHERE a = 5000000000::integer` fails even on an empty table; NoVacDB evaluates them per row and fails only if a row is read.
- **Error positions** point at the start of the offending expression or name. PostgreSQL points at the operator for operator errors; the syntax tree does not keep operator positions.
- **INSERT without a column list** may give fewer values than the table has columns; the rest take their defaults, as in PostgreSQL.
- **Writes that match no rows** (`UPDATE ... WHERE false`) write nothing to the log. A DDL statement that fails the catalog's checks never restarts the database: the catalog reports it after the statement group has begun but before any change (its rule, 2.5), so the empty group commits.
- **A failed checkpoint after a commit** is logged and retried after the next write: the statement has committed and succeeds.
- **Found by the SQL logic tests (Step 4.6).** Writing the expected results from PostgreSQL's behaviour exposed two differences, both fixed: `true::text` is `true`, not the output form `t` (so `'is ' || true` is `is true`), and a column qualified by a table's own name after the table was aliased (`SELECT p.id FROM p AS x`) is PostgreSQL's "invalid reference to FROM-clause entry for table "p"", with a hint naming the alias.
- **`Open`** returns Go errors (wrapping a `*sqlerr.Error` for a damaged catalog); `Exec` always returns `*sqlerr.Error`. Errors from storage map to `XX001` for corruption, `54000` for a statement larger than the buffer pool, and `58030` otherwise.

## 3. Formats

- Row (tuple) format: 2.2.
- Index key and value: 2.3.
- Catalog file and system tables: 2.5.
- WAL record type 4, **statement begin**: empty payload. Type 5, **statement commit**: u64 LSN of its begin record. Recovery treats a commit whose begin LSN does not match the open group as `ErrCorrupt`.

All existing formats are unchanged; types 4 and 5 are new record types.

## 4. Concurrency

The database lock (2.6) serialises writers and excludes readers during writes. Under it, the catalog's in-memory state changes only with the lock held exclusively. The engine allows one statement group at a time and makes checkpoints wait for it. Below that, the heap, B+Tree and buffer pool keep their own latching.

## 5. Failure and crash behaviour

| Event | Result |
|---|---|
| Crash before a statement's commit record is durable | The statement is absent after recovery. |
| Crash after it is durable | It is present in full. |
| Statement fails in its check phase (constraint, type, size) | Error returned; nothing changed. |
| Statement fails while applying (I/O, pool full, corruption) | Error returned; the database restarts itself and recovery discards the statement (2.4). |
| Statement larger than the buffer pool | `54000`, then the restart above. |
| Crash during database creation, before the catalog file exists | Next open creates the catalog afresh. |
| Catalog file damaged | `XX001`; the database does not open. |
| Corrupt tuple or catalog row | `XX001` for the statement that reads it. |
| Several statements in one `Exec` | Each commits on its own; an error stops the rest. PostgreSQL would roll back the earlier ones as one implicit transaction; that needs Phase 6. |

## 6. Alternatives considered

- **Statement atomicity through an undo log now.** That is Phase 6's design. "No steal" plus redo-only recovery gives atomic statements with a fraction of the code, at the cost of the statement-size limit.
- **Rebuilding indexes at startup** to repair a row logged without its index entry. Slow for large tables, and it does not fix multi-row statements.
- **Keeping the catalog roots in a fixed page.** Page 2 is the first page any new database allocates, but a crash between allocating it and logging its contents leaves it allocated and empty, and the storage layer cannot create a heap at a chosen page. The catalog file is simple and written once.
- **One catalog heap with serialised table definitions.** Simpler, but system tables with the ordinary row format exercise the same code as user tables and can be queried later (Phase 9).
- **Checking uniqueness row by row, as PostgreSQL does.** It makes `UPDATE t SET id = id + 1` fail on consecutive ids. Checking against the statement's final state is the SQL standard's rule and costs nothing extra here, because the whole statement is computed before it is applied.

## 7. Testing plan

- **Types (4.3):** a table of inputs and outputs for every type, including bounds, special values and every error; arithmetic and comparison against Go reference computations, for random values; casts in both directions; three-valued logic truth tables; round trips value → text → value and value → tuple → value; key encoding order equal to `Compare` order for random values; `FuzzDecodeRow` and `FuzzParseValue` (no panics, round trips).
- **Catalog (4.4):** create and drop tables and indexes, reload after reopen and after a crash; name rules; system-table protection; bootstrap interrupted at every point; corrupt catalog file and rows; IDs unique across reloads; loading independent of system-row order; failed DDL changes nothing (memory and disk) and returns `*sqlerr.Error`; an injected write or sync failure at every point of a multi-DDL statement leaves the catalog exactly before or (only if the commit itself failed) exactly after; a randomized workload of DDL and rows with crashes inside statements, checkpoints that free dropped pages, and reopenings, checked against a model, table by table and index entry by index entry.
- **Statements (4.4):** a group's pages never reach disk before commit, even with a tiny pool; crash at every point of a multi-page statement leaves it all or nothing; `Abandon` and reopen discard it; checkpoints wait for statements; crafted logs of begin, commit and checkpoint records give exactly the expected discarded groups and records; mismatched or orphan commits are `ErrCorrupt`.
- **Executor (4.5):** every statement form, error code and position; type resolution; index scans return exactly what sequential scans return, for random data and predicates, and read exactly the matching rows when an index can answer the clause; uniqueness and NOT NULL in every path, including keys swapped within a statement; concurrent readers with writers under `-race`, which must always see whole statements; a self-restart after an injected write or sync failure at every point of a statement, with the database afterwards (and after a crash) holding the statement entirely or not at all; oversized statements; a failed restart; cancellation; a randomized generator of typed and mistyped statements run against index and sequential plans, which must agree and never produce an internal, corruption or I/O error; `FuzzExec`.
- **SQL logic tests (4.6)** cover the statements end to end, and the crash harness gains an SQL workload whose statements must each be all or nothing after any crash.

## 8. Limitations (Phase 4)

- No transactions; each statement commits on its own (Phase 6). One writer at a time.
- A statement can change at most the buffer pool's size in pages (2.4).
- No joins, aggregates, subqueries or `GROUP BY` (Phase 7); `ORDER BY` sorts in memory, and results are fully built before they are returned.
- One collation (bytes); the session time zone is always UTC.
- No out-of-line storage: a row must fit in one page (8148 bytes).
- Index scans use only simple `column op constant` terms.
