package executor

import (
	"context"
	"fmt"

	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/ast"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/parser"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/sqlerr"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
)

// MaxParams is the most parameters a statement can have: the protocol
// counts them in 16 bits, and the lexer refuses $N above it.
const MaxParams = 65535

// params are a statement's parameters $1..$n while it is bound. While a
// statement is prepared, values is nil and each $N binds to a paramNode
// whose type is inferred from its context, as an untyped literal's would
// be; when it runs, each $N binds to its value, a constant of its type.
type params struct {
	types  []types.Type // Unknown until declared or inferred
	values []types.Value
}

// paramNode is a parameter in a statement being prepared. It is never
// evaluated.
type paramNode struct {
	p *params
	n int // 1-based
}

func (n *paramNode) typ() types.Type { return n.p.types[n.n-1] }
func (n *paramNode) eval(*evalCtx, []types.Value) (types.Value, error) {
	return types.Value{}, fmt.Errorf("parameter $%d evaluated while preparing", n.n)
}

// param binds $N.
func (b *binder) param(e *ast.Param) (node, error) {
	p := b.params
	if p == nil || p.values != nil && e.N > len(p.values) {
		return nil, b.at(sqlerr.New(sqlerr.UndefinedParameter, "there is no parameter $%d", e.N), e.P)
	}
	if p.values != nil {
		return &constNode{p.values[e.N-1]}, nil
	}
	for len(p.types) < e.N {
		p.types = append(p.types, types.Unknown)
	}
	return &paramNode{p: p, n: e.N}, nil
}

// Prepared is a statement prepared by Prepare, to run any number of times
// with ExecPrepared. It is immutable and safe for concurrent use.
type Prepared struct {
	sql  string
	stmt ast.Stmt // nil for an empty query string
	// ParamTypes are the parameters' types: as declared, or else as
	// inferred from where they appear (text where nothing decides).
	ParamTypes []types.Type
	// Columns are the result's columns for a SELECT, nil for anything
	// else.
	Columns []Column
}

// Empty reports whether the query string held no statement.
func (p *Prepared) Empty() bool { return p.stmt == nil }

// Prepare parses sql, which must hold at most one statement, and infers
// its parameters' types and result columns against the catalog as it is
// now, without running it. declared gives the types of the first
// parameters; Unknown leaves one to inference. Errors are *sqlerr.Error.
func (db *DB) Prepare(ctx context.Context, sql string, declared []types.Type) (*Prepared, error) {
	if len(declared) > MaxParams {
		return nil, sqlerr.New(sqlerr.ProgramLimitExceeded, "a statement can have at most %d parameters", MaxParams)
	}
	stmts, err := parser.Parse(sql)
	if err != nil {
		return nil, sqlerr.From(err)
	}
	if len(stmts) > 1 {
		return nil, sqlerr.New(sqlerr.SyntaxError, "cannot insert multiple commands into a prepared statement")
	}
	p := &Prepared{sql: sql, ParamTypes: append([]types.Type(nil), declared...)}
	if len(stmts) == 0 {
		return p, nil
	}
	p.stmt = stmts[0]
	ps := &params{types: p.ParamTypes}
	r, err := db.run(ctx, nil, sql, p.stmt, ps, true)
	if err != nil {
		return nil, err
	}
	for i, t := range ps.types {
		if t == types.Unknown {
			ps.types[i] = types.Text
		}
	}
	p.ParamTypes = ps.types
	p.Columns = r.Columns
	return p, nil
}

// ExecPrepared runs a prepared statement with parameter values, one per
// parameter, each NULL or of the parameter's type. A SELECT whose result
// columns are no longer those Prepare reported (the tables changed) is an
// error, as clients rely on the description they were given. An empty
// statement returns a nil Result.
func (db *DB) ExecPrepared(ctx context.Context, p *Prepared, values []types.Value) (*Result, error) {
	return db.execPrepared(ctx, nil, p, values)
}

func (db *DB) execPrepared(ctx context.Context, tx *Tx, p *Prepared, values []types.Value) (*Result, error) {
	if len(values) != len(p.ParamTypes) {
		return nil, sqlerr.New(sqlerr.ProtocolViolation, "%d parameter values for a statement with %d parameters", len(values), len(p.ParamTypes))
	}
	for i, v := range values {
		if v.T != p.ParamTypes[i] {
			return nil, sqlerr.New(sqlerr.InternalError, "parameter $%d is a %s value, not %s", i+1, v.T, p.ParamTypes[i])
		}
	}
	if p.stmt == nil {
		return nil, nil
	}
	r, err := db.run(ctx, tx, p.sql, p.stmt, &params{types: p.ParamTypes, values: values}, false)
	if err != nil {
		return nil, err
	}
	if !sameColumns(r.Columns, p.Columns) {
		return nil, sqlerr.New(sqlerr.FeatureNotSupported, "cached plan must not change result type")
	}
	return r, nil
}

// sameColumns compares the columns of two results of one statement (so
// both are nil or neither).
func sameColumns(a, b []Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
