package crash

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/executor"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

// sqlModel is what the database must hold: the rows of acct, which of the
// optional indexes exist, and the optional table extra.
type sqlModel struct {
	rows    map[int]acctRow
	indexes map[string]bool
	extra   int // rows in extra; -1 if the table does not exist
}

type acctRow struct {
	bal int64
	tag string
}

func (m sqlModel) clone() sqlModel {
	c := sqlModel{rows: map[int]acctRow{}, indexes: map[string]bool{}, extra: m.extra}
	for k, v := range m.rows {
		c.rows[k] = v
	}
	for k, v := range m.indexes {
		c.indexes[k] = v
	}
	return c
}

// String renders the model as dumpSQL renders the database.
func (m sqlModel) String() string {
	ids := make([]int, 0, len(m.rows))
	for id := range m.rows {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	var b strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&b, "%d|%d|%s\n", id, m.rows[id].bal, m.rows[id].tag)
	}
	for i := range sqlIndexNames {
		if m.indexes[sqlIndexNames[i]] {
			b.WriteString("index " + sqlIndexNames[i] + "\n")
		}
	}
	fmt.Fprintf(&b, "extra %d\n", m.extra)
	return b.String()
}

var sqlIndexNames = []string{"ix_bal", "ix_tag", "ix_both"}

var sqlIndexColumns = map[string]string{"ix_bal": "bal", "ix_tag": "tag", "ix_both": "bal, tag"}

// sqlStatement is a statement and the change it makes to the model.
type sqlStatement struct {
	sql   string
	apply func(*sqlModel)
	ddl   bool
}

// nextStatement makes a random statement that succeeds on a database in
// state m (unless an I/O fault stops it). Most change many rows at once,
// so a partly applied statement would show.
func nextStatement(rng *rand.Rand, m sqlModel, nextID *int) sqlStatement {
	ids := make([]int, 0, len(m.rows))
	for id := range m.rows {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	switch k := rng.IntN(20); {
	case k < 5 || len(ids) < 4:
		n := 1 + rng.IntN(12)
		var vals []string
		type nr struct {
			id  int
			row acctRow
		}
		var add []nr
		for range n {
			*nextID++
			r := acctRow{bal: int64(rng.IntN(1000)), tag: fmt.Sprintf("t%d", rng.IntN(7))}
			vals = append(vals, fmt.Sprintf("(%d, %d, '%s')", *nextID, r.bal, r.tag))
			add = append(add, nr{*nextID, r})
		}
		return sqlStatement{sql: "INSERT INTO acct VALUES " + strings.Join(vals, ", "), apply: func(m *sqlModel) {
			for _, a := range add {
				m.rows[a.id] = a.row
			}
		}}
	case k < 10:
		lo := ids[rng.IntN(len(ids))]
		hi := lo + rng.IntN(40)
		d := int64(rng.IntN(50) - 25)
		mod := 2 + rng.IntN(5)
		return sqlStatement{
			sql: fmt.Sprintf("UPDATE acct SET bal = bal + %d, tag = 't' || (id %% %d) WHERE id BETWEEN %d AND %d", d, mod, lo, hi),
			apply: func(m *sqlModel) {
				for id, r := range m.rows {
					if id >= lo && id <= hi {
						m.rows[id] = acctRow{r.bal + d, fmt.Sprintf("t%d", id%mod)}
					}
				}
			}}
	case k < 12:
		// Two rows swap their ids: the primary key holds only at the end.
		a, b := ids[rng.IntN(len(ids))], ids[rng.IntN(len(ids))]
		if a == b {
			b = ids[(sort.SearchInts(ids, a)+1)%len(ids)]
		}
		return sqlStatement{
			sql: fmt.Sprintf("UPDATE acct SET id = CASE id WHEN %d THEN %d ELSE %d END WHERE id IN (%d, %d)", a, b, a, a, b),
			apply: func(m *sqlModel) {
				m.rows[a], m.rows[b] = m.rows[b], m.rows[a]
			}}
	case k < 15:
		mod, r := 2+rng.IntN(4), rng.IntN(2)
		below := ids[rng.IntN(len(ids))]
		return sqlStatement{
			sql: fmt.Sprintf("DELETE FROM acct WHERE id %% %d = %d AND id < %d", mod, r, below),
			apply: func(m *sqlModel) {
				for id := range m.rows {
					if id%mod == r && id < below {
						delete(m.rows, id)
					}
				}
			}}
	case k < 17:
		name := sqlIndexNames[rng.IntN(len(sqlIndexNames))]
		if m.indexes[name] {
			return sqlStatement{sql: "DROP INDEX " + name, ddl: true, apply: func(m *sqlModel) { delete(m.indexes, name) }}
		}
		return sqlStatement{sql: fmt.Sprintf("CREATE INDEX %s ON acct (%s)", name, sqlIndexColumns[name]), ddl: true,
			apply: func(m *sqlModel) { m.indexes[name] = true }}
	default:
		if m.extra >= 0 && rng.IntN(3) == 0 {
			return sqlStatement{sql: "DROP TABLE extra", ddl: true, apply: func(m *sqlModel) { m.extra = -1 }}
		}
		n := 1 + rng.IntN(30)
		var vals []string
		for range n {
			*nextID++
			vals = append(vals, fmt.Sprintf("(%d, '%0300d')", *nextID, *nextID))
		}
		ins := "INSERT INTO extra VALUES " + strings.Join(vals, ", ")
		if m.extra < 0 {
			// Creation and the first rows, as two statements in one Exec:
			// the creation commits on its own.
			return sqlStatement{sql: "CREATE TABLE extra (x int PRIMARY KEY, pad text)", ddl: true, apply: func(m *sqlModel) { m.extra = 0 }}
		}
		return sqlStatement{sql: ins, apply: func(m *sqlModel) { m.extra += n }}
	}
}

// dumpSQL reads what the database holds, in sqlModel.String's form, and
// checks that the indexes agree with the table.
func dumpSQL(db *executor.DB) (string, error) {
	rs, err := db.Exec(bg, "SELECT id, bal, tag FROM acct ORDER BY id")
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, r := range rs[0].Rows {
		fmt.Fprintf(&b, "%d|%d|%s\n", r[0].I, r[1].I, r[2].S)
		// Through the primary key and the (tag, bal) index.
		for _, q := range []string{
			fmt.Sprintf("SELECT id FROM acct WHERE id = %d", r[0].I),
			fmt.Sprintf("SELECT id FROM acct WHERE tag = '%s' AND bal = %d AND id = %d", r[2].S, r[1].I, r[0].I),
		} {
			got, err := db.Exec(bg, q)
			if err != nil {
				return "", err
			}
			if len(got[0].Rows) != 1 {
				return "", fmt.Errorf("%s: %d rows", q, len(got[0].Rows))
			}
		}
	}
	for _, name := range sqlIndexNames {
		_, err := db.Exec(bg, "SELECT * FROM "+name)
		switch sqlerr.Code(err) {
		case sqlerr.WrongObjectType:
			b.WriteString("index " + name + "\n")
			// A range on the index's leading column covers every row (all
			// have one); the planner reads it through an index.
			col := strings.Split(sqlIndexColumns[name], ",")[0]
			q := fmt.Sprintf("SELECT id FROM acct WHERE %s >= -1000000000", col)
			if col == "tag" {
				q = "SELECT id FROM acct WHERE tag >= ''"
			}
			got, err := db.Exec(bg, q)
			if err != nil {
				return "", err
			}
			if len(got[0].Rows) != len(rs[0].Rows) {
				return "", fmt.Errorf("%s: %d rows, the table has %d", q, len(got[0].Rows), len(rs[0].Rows))
			}
		case sqlerr.UndefinedTable:
		default:
			return "", fmt.Errorf("probing index %s: %w", name, err)
		}
	}
	extra := -1
	if rs, err := db.Exec(bg, "SELECT x FROM extra WHERE x > 0"); err == nil {
		extra = len(rs[0].Rows)
		all, err := db.Exec(bg, "SELECT x FROM extra")
		if err != nil {
			return "", err
		}
		if len(all[0].Rows) != extra {
			return "", fmt.Errorf("extra: the primary key index has %d rows, the table %d", extra, len(all[0].Rows))
		}
	} else if sqlerr.Code(err) != sqlerr.UndefinedTable {
		return "", err
	}
	fmt.Fprintf(&b, "extra %d\n", extra)
	return b.String(), nil
}

type sqlScenarioStats struct {
	statements, ddl, crashes, faultStops, midStatement, keptInFlight, lostInFlight, torn, kills int
	txCommits, txRollbacks, txOpenAtCrash                                                       int
}

// runSQLScenario runs one seeded scenario: cycles of random statements and
// transactions of several statements (committed, rolled back, or open when
// the crash comes), sometimes stopped by an injected fault (after which the
// database restarts itself, or, if that fails too, is left mid-statement),
// then a crash and a recovery that must hold every acknowledged statement
// and transaction, and each other one entirely or not at all.
func runSQLScenario(t *testing.T, seed uint64) (st sqlScenarioStats) {
	t.Helper()
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("seed %d (reproduce with NOVACDB_SEED=%d NOVACDB_CRASH_RUNS=3): %s", seed, seed, fmt.Sprintf(format, args...))
	}
	ctl := rand.New(rand.NewPCG(seed, 77))
	rng := rand.New(rand.NewPCG(seed, 78))
	m := vfs.NewMemFS(seed)
	if err := m.MkdirAll(memDir); err != nil {
		fail("%v", err)
	}
	_ = m.SyncDir("/")
	opts := executor.Options{
		Frames:          48 + ctl.IntN(64),
		CheckpointBytes: []int64{8 << 10, 64 << 10, 1 << 40}[ctl.IntN(3)],
		WAL:             wal.Options{SegmentSize: []int64{8192, 65536, 1 << 20}[ctl.IntN(3)]},
		Now:             func() time.Time { return time.Unix(1700000000, 0) },
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	open := func() *executor.DB {
		db, err := executor.Open(bg, m, memDir, opts)
		if err != nil {
			fail("opening: %v", err)
		}
		return db
	}
	db := open()
	if _, err := db.Exec(bg, "CREATE TABLE acct (id int PRIMARY KEY, bal bigint NOT NULL, tag text); CREATE INDEX acct_tag ON acct (tag, bal)"); err != nil {
		fail("setup: %v", err)
	}
	acked := sqlModel{rows: map[int]acctRow{}, indexes: map[string]bool{}, extra: -1}
	nextID := 0

	for cycle := range 1 + ctl.IntN(4) {
		faulted := ctl.IntN(10) < 6
		if faulted {
			ops := []vfs.Op{vfs.OpWriteAt, vfs.OpWriteAt, vfs.OpSync, vfs.OpSync, vfs.OpOpenFile, vfs.OpRename, vfs.OpRemove, vfs.OpTruncate}
			m.InjectError(vfs.Fault{Op: ops[ctl.IntN(len(ops))], After: ctl.IntN(30)})
			if ctl.IntN(2) == 0 {
				// If the fault stops a statement, the restart fails too (only
				// a restart reopens the data file): the database stays
				// mid-statement until the crash.
				m.InjectError(vfs.Fault{Op: vfs.OpOpenFile, Name: path.Join(memDir, wal.DataFileName)})
			}
		}
		candidates := []sqlModel{acked}
		var openTx *executor.Tx // a transaction left open for the crash
	units:
		for range 5 + ctl.IntN(40) {
			if rng.IntN(4) == 0 {
				// A transaction of several statements, which see each
				// other's changes; acknowledged only by its commit.
				tx, err := db.Begin(bg)
				if err != nil {
					fail("cycle %d: begin: %v", cycle, err)
				}
				after := acked.clone()
				for range 2 + rng.IntN(4) {
					s := nextStatement(rng, after, &nextID)
					st.statements++
					if s.ddl {
						st.ddl++
					}
					if _, err := tx.Exec(bg, s.sql); err != nil {
						code := sqlerr.Code(err)
						if code != sqlerr.ProgramLimitExceeded && !errors.Is(err, vfs.ErrInjected) && code != sqlerr.IOError {
							fail("cycle %d: in a transaction: %s: %v", cycle, s.sql, err)
						}
						// Discarded by the restart (or the database is
						// unavailable): only the acknowledged state remains.
						_ = tx.Rollback(bg)
						if code == sqlerr.ProgramLimitExceeded {
							continue units
						}
						st.faultStops++
						break units
					}
					s.apply(&after)
				}
				switch rng.IntN(5) {
				case 0:
					openTx = tx // the crash comes with it open
					st.txOpenAtCrash++
					break units
				case 1:
					if err := tx.Rollback(bg); err != nil {
						if !errors.Is(err, vfs.ErrInjected) && sqlerr.Code(err) != sqlerr.IOError {
							fail("cycle %d: rollback: %v", cycle, err)
						}
						st.faultStops++
						break units
					}
					st.txRollbacks++
				default:
					if err := tx.Commit(bg); err != nil {
						if !errors.Is(err, vfs.ErrInjected) && sqlerr.Code(err) != sqlerr.IOError {
							fail("cycle %d: commit: %v", cycle, err)
						}
						// The commit may or may not have happened.
						st.faultStops++
						candidates = []sqlModel{acked, after}
						break units
					}
					st.txCommits++
					acked = after
					candidates = []sqlModel{acked}
				}
				continue
			}
			s := nextStatement(rng, acked, &nextID)
			after := acked.clone()
			s.apply(&after)
			st.statements++
			if s.ddl {
				st.ddl++
			}
			_, err := db.Exec(bg, s.sql)
			if err == nil {
				acked = after
				candidates = []sqlModel{acked}
				continue
			}
			code := sqlerr.Code(err)
			if code == sqlerr.ProgramLimitExceeded {
				continue // too large for the pool: absent, and the database restarted
			}
			if !errors.Is(err, vfs.ErrInjected) && code != sqlerr.IOError {
				fail("cycle %d: %s: %v", cycle, s.sql, err)
			}
			// The statement may or may not have committed.
			st.faultStops++
			candidates = []sqlModel{acked, after}
			break
		}
		// Was the database left mid-statement (its restart failed)? A
		// transaction left open holds the database: no probe then.
		if openTx == nil {
			if _, err := db.Exec(bg, "SELECT 1"); err != nil {
				st.midStatement++
			}
		}
		m.ClearFaults()
		switch ctl.IntN(4) {
		case 0:
			st.kills++
		case 1:
			m.Crash(vfs.CrashOptions{})
		default:
			st.torn++
			m.Crash(vfs.CrashOptions{TearLast: true})
		}
		st.crashes++
		db = open() // the old one is abandoned, as a dead process's would be
		got, err := dumpSQL(db)
		if err != nil {
			fail("cycle %d: reading after recovery: %v", cycle, err)
		}
		match := -1
		for i, c := range candidates {
			if got == c.String() {
				match = i
			}
		}
		if match < 0 {
			fail("cycle %d: recovered\n%s\nwhich matches none of the %d possible states; acknowledged:\n%s", cycle, got, len(candidates), acked.String())
		}
		if len(candidates) == 2 {
			if match == 1 {
				st.keptInFlight++
			} else {
				st.lostInFlight++
			}
		}
		acked = candidates[match]
	}
	if err := db.Close(bg); err != nil {
		fail("closing: %v", err)
	}
	return st
}

func TestSQLCrashRecovery(t *testing.T) {
	base := baseSeed(t)
	runs := envInt(t, "NOVACDB_CRASH_RUNS", 300) / 3
	if testing.Short() {
		runs = min(runs, 6)
	}
	var total sqlScenarioStats
	for i := range runs {
		st := runSQLScenario(t, base+uint64(i))
		total.statements += st.statements
		total.ddl += st.ddl
		total.crashes += st.crashes
		total.faultStops += st.faultStops
		total.midStatement += st.midStatement
		total.keptInFlight += st.keptInFlight
		total.lostInFlight += st.lostInFlight
		total.torn += st.torn
		total.kills += st.kills
		total.txCommits += st.txCommits
		total.txRollbacks += st.txRollbacks
		total.txOpenAtCrash += st.txOpenAtCrash
	}
	t.Logf("%d runs: %+v", runs, total)
	if runs >= 30 {
		for name, n := range map[string]int{
			"DDL statements": total.ddl, "statements stopped by a fault": total.faultStops,
			"crashes in the middle of a statement": total.midStatement,
			"in-flight statements lost":            total.lostInFlight, "torn crashes": total.torn, "process kills": total.kills,
			"committed transactions": total.txCommits, "rolled-back transactions": total.txRollbacks,
			"transactions open at a crash": total.txOpenAtCrash,
		} {
			if n < runs/10 {
				t.Errorf("only %d %s in %d runs: the scenario is not exercising enough", n, name, runs)
			}
		}
	}
}
