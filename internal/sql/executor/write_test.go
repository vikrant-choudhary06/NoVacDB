package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/catalog"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/vfs"
)

func TestInsertForms(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (a int, b text DEFAULT 'dflt', c double precision, d timestamptz DEFAULT now())")
	mustExec(t, db, "INSERT INTO t DEFAULT VALUES")
	mustExec(t, db, "INSERT INTO t (c, a) VALUES (1, 2.6), ('3', '4')")
	mustExec(t, db, "INSERT INTO t VALUES (5, 6, 7, NULL)")                // int to text, int to double
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (2147483647.4, DEFAULT)") // rounds
	mustExec(t, db, "INSERT INTO t VALUES (-1, 'short')")                  // the rest take defaults
	want := "NULL|dflt|NULL|2024-05-06 07:08:09.5+00\n" +
		"3|dflt|1|2024-05-06 07:08:09.5+00\n" +
		"4|dflt|3|2024-05-06 07:08:09.5+00\n" +
		"5|6|7|NULL\n" +
		"2147483647|dflt|NULL|2024-05-06 07:08:09.5+00\n" +
		"-1|short|NULL|2024-05-06 07:08:09.5+00"
	if got := rows(t, db, "SELECT * FROM t"); got != want {
		t.Fatalf("got\n%s", got)
	}
	cases := []struct{ sql, code, at string }{
		{"INSERT INTO t VALUES (1, 'x', 1.0, now(), 5)", sqlerr.SyntaxError, "5)"},
		{"INSERT INTO t (a, b) VALUES (1)", sqlerr.SyntaxError, "b)"},
		{"INSERT INTO t (a) VALUES (1), (2, 3)", sqlerr.SyntaxError, "2, 3"},
		{"INSERT INTO t (a, a) VALUES (1, 2)", sqlerr.DuplicateColumn, "a)"},
		{"INSERT INTO t (z) VALUES (1)", sqlerr.UndefinedColumn, "z)"},
		{"INSERT INTO nope VALUES (1)", sqlerr.UndefinedTable, "nope"},
		{"INSERT INTO t (a) VALUES ('x')", sqlerr.InvalidTextRepresentation, "'x'"},
		{"INSERT INTO t (a) VALUES (true)", sqlerr.DatatypeMismatch, "true"},
		{"INSERT INTO t (d) VALUES (1)", sqlerr.DatatypeMismatch, "1)"},
		{"INSERT INTO t (a) VALUES (2147483648)", sqlerr.NumericValueOutOfRange, ""},
		{"INSERT INTO t (a) VALUES (2147483647.5)", sqlerr.NumericValueOutOfRange, ""},
		{"INSERT INTO t (a) VALUES (a)", sqlerr.UndefinedColumn, "a)"},
		{"INSERT INTO t (a) VALUES (1/0)", sqlerr.DivisionByZero, ""},
		{"INSERT INTO t (b) VALUES ('" + strings.Repeat("x", 9000) + "')", sqlerr.ProgramLimitExceeded, ""},
	}
	for _, c := range cases {
		e := expectErrSoft(t, db, c.sql, c.code)
		if e == nil {
			continue
		}
		at := ""
		if e.Position > 0 {
			at = string([]rune(c.sql)[e.Position-1:])
		}
		if c.at == "" && e.Position != 0 || c.at != "" && !strings.HasPrefix(at, c.at) {
			t.Errorf("%s: error %v points at %.20q, want %q", c.sql, e, at, c.at)
		}
	}
	// No failed statement changed anything.
	if got := rows(t, db, "SELECT * FROM t"); got != want {
		t.Fatalf("after failures:\n%s", got)
	}
}

func TestConstraints(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, code text UNIQUE, n int NOT NULL DEFAULT 0, UNIQUE (n, code))")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'a', 1), (2, NULL, 1), (3, NULL, 1)") // NULLs never collide
	e := expectErr(t, db, "INSERT INTO t VALUES (4, 'a', 2)", sqlerr.UniqueViolation)
	if e.Message != `duplicate key value violates unique constraint "t_code_key"` || e.Detail != "Key (code)=(a) already exists." {
		t.Fatalf("%q %q", e.Message, e.Detail)
	}
	e = expectErr(t, db, "INSERT INTO t VALUES (5, 'x', 2), (6, 'x', 3)", sqlerr.UniqueViolation)
	if e.Detail != "Key (code)=(x) already exists." {
		t.Fatalf("%q", e.Detail)
	}
	expectErr(t, db, "INSERT INTO t VALUES (1, 'q', 2)", sqlerr.UniqueViolation)
	e = expectErr(t, db, "INSERT INTO t (id, code, n) VALUES (NULL, 'z', 1)", sqlerr.NotNullViolation)
	if e.Message != `null value in column "id" of relation "t" violates not-null constraint` || e.Detail != "Failing row contains (null, z, 1)." {
		t.Fatalf("%q %q", e.Message, e.Detail)
	}
	expectErr(t, db, "INSERT INTO t (id, n) VALUES (9, NULL)", sqlerr.NotNullViolation)
	expectErr(t, db, "UPDATE t SET n = NULL WHERE id = 1", sqlerr.NotNullViolation)
	if got := rows(t, db, "SELECT * FROM t ORDER BY id"); got != "1|a|1\n2|NULL|1\n3|NULL|1" {
		t.Fatalf("failed statements changed rows:\n%s", got)
	}

	// Uniqueness holds for the statement's final state, as the SQL
	// standard says: shifting consecutive keys works in either direction.
	mustExec(t, db, "UPDATE t SET id = id + 1")
	mustExec(t, db, "UPDATE t SET id = id - 1")
	if got := rows(t, db, "SELECT id FROM t ORDER BY id"); got != "1\n2\n3" {
		t.Fatalf("after shifting: %s", got)
	}
	// Swapping two keys in one statement.
	mustExec(t, db, "UPDATE t SET code = CASE code WHEN 'a' THEN 'b' END WHERE id = 1")
	mustExec(t, db, "UPDATE t SET code = CASE id WHEN 1 THEN 'c' WHEN 2 THEN 'b' END WHERE id IN (1, 2)")
	if got := rows(t, db, "SELECT id, code FROM t ORDER BY id"); got != "1|c\n2|b\n3|NULL" {
		t.Fatalf("after swapping: %s", got)
	}
	mustExec(t, db, "UPDATE t SET code = CASE id WHEN 1 THEN 'b' ELSE 'c' END WHERE id IN (1, 2)")
	if got := rows(t, db, "SELECT id, code FROM t ORDER BY id"); got != "1|b\n2|c\n3|NULL" {
		t.Fatalf("after swapping back: %s", got)
	}
	// Two rows moving onto one key, and a row moving onto a key that stays.
	expectErr(t, db, "UPDATE t SET id = 7 WHERE id IN (1, 2)", sqlerr.UniqueViolation)
	expectErr(t, db, "UPDATE t SET id = 3 WHERE id = 1", sqlerr.UniqueViolation)
	// A row keeping its own key is fine.
	mustExec(t, db, "UPDATE t SET id = id, code = code")
	// Deleting frees a key for a later statement.
	mustExec(t, db, "DELETE FROM t WHERE id = 3; INSERT INTO t VALUES (3, 'b2', 5)")
	expectErr(t, db, "CREATE UNIQUE INDEX bad ON t (n)", sqlerr.UniqueViolation)
	mustExec(t, db, "UPDATE t SET n = id")
	mustExec(t, db, "CREATE UNIQUE INDEX good ON t (n)")
	expectErr(t, db, "INSERT INTO t VALUES (8, 'q', 1)", sqlerr.UniqueViolation)
	checkConsistency(t, db)
}

func TestUpdateAndDeleteForms(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (a int, b text DEFAULT 'd'); INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, NULL)")
	cases := []struct{ sql, tag, after string }{
		{"UPDATE t SET b = upper(b) WHERE a >= 2", "UPDATE 2", "1|x\n2|Y\n3|NULL"},
		{"UPDATE t AS u SET a = u.a * 10, b = DEFAULT WHERE u.b IS NULL", "UPDATE 1", "1|x\n2|Y\n30|d"},
		{"UPDATE t SET a = a + 1, b = a", "UPDATE 3", "2|1\n3|2\n31|30"}, // all from the old row
		{"UPDATE t SET a = 0 WHERE false", "UPDATE 0", "2|1\n3|2\n31|30"},
		{"DELETE FROM t WHERE b = '2'", "DELETE 1", "2|1\n31|30"},
		{"DELETE FROM t", "DELETE 2", ""},
	}
	for _, c := range cases {
		if got := query(t, db, c.sql); got != c.tag {
			t.Fatalf("%s: %q", c.sql, got)
		}
		if got := rows(t, db, "SELECT * FROM t ORDER BY a"); got != c.after {
			t.Fatalf("after %s:\n%s", c.sql, got)
		}
	}
	expectErr(t, db, "UPDATE t SET z = 1", sqlerr.UndefinedColumn)
	expectErr(t, db, "UPDATE t SET a = 1, a = 2", sqlerr.SyntaxError)
	expectErr(t, db, "UPDATE t SET a = 'x'", sqlerr.InvalidTextRepresentation)
	expectErr(t, db, "UPDATE t SET a = 1 WHERE b", sqlerr.DatatypeMismatch)
	expectErr(t, db, "DELETE FROM t WHERE nope", sqlerr.UndefinedColumn)
	expectErr(t, db, "DELETE FROM t AS u WHERE t.a = 1", sqlerr.UndefinedTable)
	// A run-time error in any row leaves every row unchanged.
	mustExec(t, db, "INSERT INTO t VALUES (1, 'a'), (0, 'b'), (2, 'c')")
	expectErr(t, db, "UPDATE t SET a = 10 / a", sqlerr.DivisionByZero)
	if got := rows(t, db, "SELECT * FROM t ORDER BY a"); got != "0|b\n1|a\n2|c" {
		t.Fatalf("a failed UPDATE changed rows:\n%s", got)
	}
}

func TestSelectClauses(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (a int, b text, c double precision)")
	mustExec(t, db, "INSERT INTO t VALUES (3, 'x', 1.5), (1, 'y', NULL), (2, 'x', -0.0), (NULL, 'z', 0.0), (1, 'y', 'NaN')")
	cases := []struct{ sql, want string }{
		{"SELECT a FROM t ORDER BY a", "1\n1\n2\n3\nNULL"},
		{"SELECT a FROM t ORDER BY a DESC", "NULL\n3\n2\n1\n1"},
		{"SELECT a FROM t ORDER BY a NULLS FIRST", "NULL\n1\n1\n2\n3"},
		{"SELECT a FROM t ORDER BY a DESC NULLS LAST", "3\n2\n1\n1\nNULL"},
		{"SELECT a, b FROM t ORDER BY b DESC, a", "NULL|z\n1|y\n1|y\n2|x\n3|x"},
		{"SELECT b AS k FROM t ORDER BY k LIMIT 2", "x\nx"},
		{"SELECT a, b FROM t ORDER BY 2, 1 DESC", "3|x\n2|x\n1|y\n1|y\nNULL|z"},
		{"SELECT a FROM t ORDER BY -a", "3\n2\n1\n1\nNULL"},
		{"SELECT b FROM t ORDER BY a NULLS FIRST, b", "z\ny\ny\nx\nx"},
		{"SELECT c FROM t ORDER BY c", "-0\n0\n1.5\nNaN\nNULL"},
		{"SELECT DISTINCT b FROM t ORDER BY b", "x\ny\nz"},
		{"SELECT DISTINCT a, b FROM t ORDER BY 1, 2", "1|y\n2|x\n3|x\nNULL|z"},
		{"SELECT DISTINCT c = 0 FROM t ORDER BY 1", "f\nt\nNULL"},
		{"SELECT a FROM t ORDER BY a LIMIT 2 OFFSET 1", "1\n2"},
		{"SELECT a FROM t ORDER BY a OFFSET 4", "NULL"},
		{"SELECT a FROM t ORDER BY a LIMIT 0", ""},
		{"SELECT a FROM t ORDER BY a LIMIT NULL", "1\n1\n2\n3\nNULL"},
		{"SELECT a FROM t ORDER BY a LIMIT 1 + 1", "1\n1"},
		{"SELECT a FROM t ORDER BY a LIMIT '2'", "1\n1"},
		{"SELECT a FROM t ORDER BY a LIMIT 1.6", "1\n1"},
		{"SELECT a FROM t WHERE a > 1 ORDER BY a OFFSET 1 LIMIT 5", "3"},
		{"SELECT 1 WHERE false", ""},
		{"SELECT 'lit', NULL", "lit|NULL"},
		{"SELECT a + 1 AS a FROM t WHERE a = 3 ORDER BY a", "4"},
	}
	for _, c := range cases {
		if got := rows(t, db, c.sql); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.sql, got, c.want)
		}
	}
	// Without ORDER BY a LIMIT stops early, but must still give LIMIT rows.
	if r := mustExec(t, db, "SELECT * FROM t LIMIT 3")[0]; len(r.Rows) != 3 || r.Tag != "SELECT 3" {
		t.Fatalf("%+v", r)
	}
	if r := mustExec(t, db, "SELECT * FROM t OFFSET 3 LIMIT 3")[0]; len(r.Rows) != 2 {
		t.Fatalf("%+v", r)
	}
	if r := mustExec(t, db, "SELECT 'x' AS s, NULL AS n")[0]; r.Columns[0].Type.String() != "text" || r.Columns[1].Type.String() != "text" {
		t.Fatalf("untyped outputs: %+v", r.Columns)
	}
	errs := []struct{ sql, code string }{
		{"SELECT a FROM t ORDER BY 3", sqlerr.InvalidColumnReference},
		{"SELECT a FROM t ORDER BY 0", sqlerr.InvalidColumnReference},
		{"SELECT DISTINCT a FROM t ORDER BY b", sqlerr.InvalidColumnReference},
		{"SELECT a AS x, b AS x FROM t ORDER BY x", sqlerr.AmbiguousColumn},
		{"SELECT a FROM t LIMIT -1", sqlerr.InvalidRowCountInLimit},
		{"SELECT a FROM t OFFSET -1", sqlerr.InvalidRowCountInOffset},
		{"SELECT a FROM t LIMIT a", sqlerr.InvalidColumnReference},
		{"SELECT a FROM t LIMIT 'x'", sqlerr.InvalidTextRepresentation},
		{"SELECT a FROM t LIMIT true", sqlerr.DatatypeMismatch},
		{"SELECT a FROM t_pkey", sqlerr.UndefinedTable},
		{"SELECT t.a, u.b FROM t AS u", sqlerr.UndefinedTable},
	}
	for _, c := range errs {
		expectErrSoft(t, db, c.sql, c.code)
	}
	// DISTINCT with ORDER BY on an expression that is in the select list.
	if got := rows(t, db, "SELECT DISTINCT a + 1 FROM t ORDER BY a + 1 DESC"); got != "NULL\n4\n3\n2" {
		t.Fatalf("%q", got)
	}
	mustExec(t, db, "CREATE INDEX ti ON t (a)")
	expectErr(t, db, "SELECT * FROM ti", sqlerr.WrongObjectType)
	expectErr(t, db, "INSERT INTO ti VALUES (1)", sqlerr.WrongObjectType)
}

func TestWriteDetails(t *testing.T) {
	m := newFS(t)
	db := openDB(t, m, Options{})
	defer func() { _ = db.Close(bg) }()
	mustExec(t, db, "CREATE TABLE t (id int PRIMARY KEY, s text); CREATE INDEX ON t (s)")
	for i := range 60 {
		mustExec(t, db, fmt.Sprintf("INSERT INTO t VALUES (%d, '%0100d')", i, i))
	}
	// Growing rows moves them to other pages; their index entries follow.
	mustExec(t, db, "UPDATE t SET s = s || s || s || s WHERE id % 2 = 0")
	checkConsistency(t, db)
	if got := rows(t, db, fmt.Sprintf("SELECT id FROM t WHERE s = '%s'", strings.Repeat(fmt.Sprintf("%0100d", 10), 4))); got != "10" {
		t.Fatalf("index lookup of a moved row: %q", got)
	}
	// A key too large for its index fails in the check phase: no restart.
	restarts := db.restarts.Load()
	expectErr(t, db, "INSERT INTO t VALUES (100, '"+strings.Repeat("k", 1100)+"')", sqlerr.ProgramLimitExceeded)
	expectErr(t, db, "UPDATE t SET s = '"+strings.Repeat("k", 1100)+"' WHERE id = 1", sqlerr.ProgramLimitExceeded)
	if db.restarts.Load() != restarts {
		t.Fatal("an oversized key restarted the database")
	}
	// Statements that change nothing write nothing to the log.
	size := walSize(t, m)
	mustExec(t, db, "UPDATE t SET s = 'x' WHERE false; DELETE FROM t WHERE id < 0")
	if walSize(t, m) != size {
		t.Fatal("statements that changed nothing wrote to the log")
	}
	// Column definitions.
	expectErr(t, db, "CREATE TABLE p (a int NULL PRIMARY KEY)", sqlerr.SyntaxError)
	mustExec(t, db, "CREATE TABLE u (a int UNIQUE, b int UNIQUE, UNIQUE (a, b))")
	u, _ := db.cat.Table("u")
	var names []string
	for _, ix := range u.Indexes {
		names = append(names, ix.Name)
	}
	if strings.Join(names, " ") != "u_a_key u_b_key u_a_b_key" {
		t.Fatalf("unique constraint indexes %v", names)
	}
	// nullif with a NULL second argument returns the first.
	if got := rows(t, db, "SELECT nullif(0, NULL), nullif(NULL, 0), nullif(0, 0)"); got != "0|NULL|NULL" {
		t.Fatalf("nullif: %q", got)
	}
	// Two outputs of the same expression under one name are not ambiguous.
	if got := rows(t, db, "SELECT id AS x, id AS x FROM t WHERE id < 2 ORDER BY x DESC"); got != "1|1\n0|0" {
		t.Fatalf("%q", got)
	}
}

// walSize is the total size of the log's segment files.
func walSize(t *testing.T, m *vfs.MemFS) int64 {
	t.Helper()
	names, err := m.List(dir + "/wal")
	if err != nil {
		t.Fatal(err)
	}
	total := int64(0)
	for _, n := range names {
		f, err := m.OpenFile(dir+"/wal/"+n, vfs.ORead)
		if err != nil {
			t.Fatal(err)
		}
		sz, err := f.Size()
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		total += sz
	}
	return total
}

func TestDDLErrorsDoNotRestart(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (a int); INSERT INTO t VALUES (1), (1)")
	restarts := db.restarts.Load()
	expectErr(t, db, "CREATE UNIQUE INDEX u ON t (a)", sqlerr.UniqueViolation)
	expectErr(t, db, "CREATE TABLE t (b int)", sqlerr.DuplicateTable)
	expectErr(t, db, "CREATE TABLE p (a int, b int, PRIMARY KEY (a, a))", sqlerr.DuplicateColumn)
	if db.restarts.Load() != restarts {
		t.Fatal("a DDL statement that changed nothing restarted the database")
	}
	if db.cat.Exists("u") || db.cat.Exists("p") {
		t.Fatal("a failed DDL statement left an object")
	}
}

func TestMissingIndexEntryIsCorruption(t *testing.T) {
	db := newDB(t)
	mustExec(t, db, "CREATE TABLE t (a int PRIMARY KEY, b int); INSERT INTO t VALUES (1, 10), (2, 20)")
	// Remove row 1's primary key entry behind the executor's back.
	tbl, _ := db.cat.Table("t")
	ix := tbl.PrimaryKey()
	rids, err := ix.Lookup(bg, catalog.KeyPrefix([]types.Value{types.NewInt4(1)}))
	if err != nil || len(rids) != 1 {
		t.Fatal(rids, err)
	}
	key, err := ix.Key([]types.Value{types.NewInt4(1), types.NewInt4(10)}, rids[0])
	if err != nil {
		t.Fatal(err)
	}
	wtx := db.e.Begin()
	if err := wtx.Write(bg, func(context.Context) error {
		found, err := ix.Tree.Delete(bg, key)
		if err == nil && !found {
			err = errors.New("no entry")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := wtx.Commit(bg); err != nil {
		t.Fatal(err)
	}
	restarts := db.restarts.Load()
	expectErr(t, db, "UPDATE t SET b = 11 WHERE b = 10", sqlerr.DataCorrupted)
	if db.restarts.Load() != restarts+1 {
		t.Fatal("the failed UPDATE did not restart the database")
	}
	// The other row is untouched, and the statement left nothing behind.
	if got := rows(t, db, "SELECT * FROM t ORDER BY a"); got != "1|10\n2|20" {
		t.Fatalf("%q", got)
	}
}
