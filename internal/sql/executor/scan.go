package executor

import (
	"bytes"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/catalog"
	"github.com/vikrant-choudhary06/NoVacDB/internal/sql/types"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
)

// access is how a table is read: every row in heap order, or the rows an
// index range points at. The WHERE clause is always applied to every row
// read, so an index only narrows what is read and never decides the
// result.
type access struct {
	ix         *catalog.Index // nil: sequential scan
	start, end btree.Bound
}

// term is a top-level AND term of a WHERE clause of the form
// column op constant.
type term struct {
	col int
	op  string // = < <= > >=
	v   types.Value
}

// flipped is the operator with its operands swapped: c < x is x > c.
var flipped = map[string]string{"=": "=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}

// terms collects the usable terms of a bound WHERE clause: comparisons
// whose one side is a column, with the comparison done in that column's
// own type, and whose other side is a constant, possibly cast. The value
// is exactly what the comparison compares with, so its key encoding is
// that of equal column values.
func terms(where node, out []term) []term {
	switch n := where.(type) {
	case *andNode:
		return terms(n.r, terms(n.l, out))
	case *cmpNode:
		op, ok := flipped[n.op]
		if !ok {
			return out // <> narrows nothing
		}
		if c, ok := n.l.(*colNode); ok {
			if v, ok := constant(n.r); ok {
				return append(out, term{c.idx, n.op, v})
			}
		}
		if c, ok := n.r.(*colNode); ok {
			if v, ok := constant(n.l); ok {
				return append(out, term{c.idx, op, v})
			}
		}
	}
	return out
}

// constant evaluates a constant, or casts of one, if that succeeds and
// is not NULL. A cast that fails (3000000000::integer) is left to the
// WHERE clause, which reports the error; a comparison with NULL matches
// nothing, which the WHERE clause also takes care of.
func constant(n node) (types.Value, bool) {
	switch n := n.(type) {
	case *constNode:
		return n.v, !n.v.Null
	case *castNode:
		v, ok := constant(n.x)
		if !ok {
			return types.Value{}, false
		}
		w, err := types.Cast(v, n.to)
		return w, err == nil
	}
	return types.Value{}, false
}

// plan picks an index for the WHERE clause: the one whose leading columns
// have the most equality terms, then one with a range term on the next
// column; ties go to the index created first. No usable index means a
// sequential scan.
func plan(tbl *catalog.Table, where node) access {
	if where == nil {
		return access{}
	}
	ts := terms(where, nil)
	best, bestScore := access{}, 0
	for _, ix := range tbl.Indexes {
		var eq []types.Value
		for _, col := range ix.Columns {
			v, ok := find(ts, col, "=")
			if !ok {
				break
			}
			eq = append(eq, v)
		}
		score := 2 * len(eq)
		var lo, hi *term
		if len(eq) < len(ix.Columns) {
			lo, hi = rangeTerms(ts, ix.Columns[len(eq)])
			if lo != nil || hi != nil {
				score++
			}
		}
		if score > bestScore {
			best, bestScore = bounds(ix, eq, lo, hi), score
		}
	}
	return best
}

func find(ts []term, col int, op string) (types.Value, bool) {
	for _, t := range ts {
		if t.col == col && t.op == op {
			return t.v, true
		}
	}
	return types.Value{}, false
}

// rangeTerms returns the tightest lower and upper bound terms on col.
func rangeTerms(ts []term, col int) (lo, hi *term) {
	for i := range ts {
		t := &ts[i]
		if t.col != col {
			continue
		}
		switch t.op {
		case ">", ">=":
			if lo == nil || tighter(t, lo, 1) {
				lo = t
			}
		case "<", "<=":
			if hi == nil || tighter(t, hi, -1) {
				hi = t
			}
		}
	}
	return lo, hi
}

// tighter reports whether bound a excludes more than bound b; dir is +1
// for lower bounds, -1 for upper ones.
func tighter(a, b *term, dir int) bool {
	if c := types.Compare(a.v, b.v) * dir; c != 0 {
		return c > 0
	}
	return len(a.op) == 1 && len(b.op) == 2 // strict beats inclusive
}

// bounds computes the key range of an index for equality values on its
// leading columns and optional range terms on the next one. The key
// encoding is prefix-free per column, so the entries whose leading columns
// equal eq are exactly those starting with their encoding P, and those
// whose next column is v start with P+enc(v). NULL sorts after every
// value, so a range with no upper bound stops before the NULLs.
func bounds(ix *catalog.Index, eq []types.Value, lo, hi *term) access {
	p := catalog.KeyPrefix(eq)
	a := access{ix: ix, start: btree.Incl(p), end: prefixEnd(p)}
	if lo != nil {
		k := types.AppendKey(bytes.Clone(p), lo.v)
		if lo.op == ">=" {
			a.start = btree.Incl(k)
		} else {
			a.start = startAfter(k)
		}
		a.end = btree.Excl(btree.AppendNull(bytes.Clone(p)))
	}
	if hi != nil {
		k := types.AppendKey(bytes.Clone(p), hi.v)
		if hi.op == "<" {
			a.end = btree.Excl(k)
		} else {
			a.end = prefixEnd(k)
		}
	}
	return a
}

// successor returns the smallest key greater than every key starting with
// p, or nil if there is none (p is empty or all 0xff).
func successor(p []byte) []byte { return catalog.KeySuccessor(p) }

// prefixEnd is the exclusive end of the keys starting with p.
func prefixEnd(p []byte) btree.Bound {
	if s := successor(p); s != nil {
		return btree.Excl(s)
	}
	return btree.Bound{}
}

// startAfter is the inclusive start after every key starting with p.
func startAfter(p []byte) btree.Bound {
	if s := successor(p); s != nil {
		return btree.Incl(s)
	}
	return btree.Excl(p) // unreachable for real keys, which never end in 0xff only
}

// scan calls fn with every row of tbl for which where is TRUE (every row
// if where is nil), reading it through a, until fn returns false.
func (st *stmt) scan(tbl *catalog.Table, a access, where node, fn func(rid storage.RID, row []types.Value) (bool, error)) error {
	colTypes := tbl.Types()
	rd := st.reader()
	n := 0
	visit := func(rid storage.RID, v storage.Version) (bool, error) {
		data, ok, err := rd.Visible(st.ctx, rid, v)
		if err != nil || !ok {
			return err == nil, err // a version the snapshot does not see
		}
		st.db.rowsRead.Add(1)
		if n++; n%256 == 0 {
			if err := st.ctx.Err(); err != nil {
				return false, err
			}
		}
		row, err := types.DecodeRow(data, colTypes)
		if err != nil {
			return false, err
		}
		if where != nil {
			v, err := where.eval(st.ec, row)
			if err != nil {
				return false, err
			}
			if v.Null || !v.Bool() {
				return true, nil
			}
		}
		return fn(rid, row)
	}
	if a.ix != nil {
		// The index describes what the snapshot sees only if the snapshot
		// sees the last writer and no new writer touches the index during
		// the scan; every writer becomes the last writer before it changes
		// an index (docs/design/16-snapshots-visibility.md section 2.8).
		// The plan checked the first at plan time, but a writer may have
		// begun since. So the hits are collected, and used only if the last
		// writer, as of before the scan, is one the snapshot sees and is
		// still the last writer after it; otherwise the table is read
		// instead.
		if h := st.db.indexScanHook; h != nil {
			h()
		}
		before := st.db.e.LastWriter()
		if st.indexesShow(before) {
			hits, err := st.indexHits(tbl, a)
			if st.db.e.LastWriter() == before {
				if err != nil {
					return err
				}
				st.db.indexScans.Add(1)
				for _, h := range hits {
					if more, err := visit(h.rid, h.v); err != nil || !more {
						return err
					}
				}
				return nil
			}
		}
		st.db.indexScanRetries.Add(1)
	}
	s := tbl.Heap.ScanVersions()
	for {
		rid, v, ok, err := s.Next(st.ctx)
		if err != nil || !ok {
			return err
		}
		if more, err := visit(rid, v); err != nil || !more {
			return err
		}
	}
}

// indexHit is a row an index scan found, and its current version.
type indexHit struct {
	rid storage.RID
	v   storage.Version
}

// indexHits reads the index range a describes and the current version of
// each row it names.
func (st *stmt) indexHits(tbl *catalog.Table, a access) ([]indexHit, error) {
	var hits []indexHit
	it := a.ix.Tree.Scan(a.start, a.end)
	for {
		_, v, ok, err := it.Next(st.ctx)
		if err != nil || !ok {
			return hits, err
		}
		rid, err := catalog.DecodeRID(v)
		if err != nil {
			return nil, err
		}
		row, err := tbl.Heap.GetVersion(st.ctx, rid)
		if err != nil {
			return nil, err
		}
		hits = append(hits, indexHit{rid, row})
		if len(hits)%256 == 0 {
			if err := st.ctx.Err(); err != nil {
				return nil, err
			}
		}
	}
}
