package crash

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/vikrant-choudhary06/NoVacDB/internal/btree"
	"github.com/vikrant-choudhary06/NoVacDB/internal/storage"
	"github.com/vikrant-choudhary06/NoVacDB/internal/wal"
)

var bg = context.Background()

// numHeaps is how many heaps every workload uses.
const numHeaps = 2

type opKind int

const (
	opInsert opKind = iota
	opUpdate
	opDelete
	opFlush
	opCheckpoint
	opTreeInsert
	opTreeDelete
)

// The B+Tree's key space and starting size. The tree then alternates
// between growing and shrinking phases, so it keeps splitting and merging
// nodes. Long keys make it three levels deep.
const (
	treeKeys   = 1500
	treeTarget = 400
)

// initialTree is what the tree holds when it is created: keys spread over
// the key space.
func initialTree() map[uint64][]byte {
	m := map[uint64][]byte{}
	for i := range uint64(treeTarget) {
		k := i * treeKeys / treeTarget
		m[k] = treeValue(k, 0, 20)
	}
	return m
}

// op is one step of a workload. Rows are identified by a key stored in their
// first 8 bytes, so the oracle never depends on RIDs.
type op struct {
	kind    opKind
	heap    int
	key     uint64
	version uint64
	size    int
}

// tupleHeaderSize is key + version.
const tupleHeaderSize = 16

// tupleFor builds a row: key, version, then filler derived from both, so
// any mix of two versions is detectable.
func tupleFor(key, version uint64, size int) []byte {
	b := make([]byte, max(size, tupleHeaderSize))
	binary.LittleEndian.PutUint64(b, key)
	binary.LittleEndian.PutUint64(b[8:], version)
	for i := tupleHeaderSize; i < len(b); i++ {
		b[i] = byte(key*31 + version*7 + uint64(i))
	}
	return b
}

// treeKey is the B+Tree key for k: padded to a length that varies with k,
// so nodes hold different numbers of entries and the tree has several
// levels.
func treeKey(k uint64) []byte {
	key := binary.BigEndian.AppendUint64(nil, k)
	return append(key, make([]byte, int(k*37%1000))...)
}

// treeValue is the value stored under k: like tupleFor, a mix of two
// versions is detectable.
func treeValue(k, version uint64, size int) []byte {
	b := make([]byte, min(max(size, 16), btree.MaxValueSize))
	binary.LittleEndian.PutUint64(b, k)
	binary.LittleEndian.PutUint64(b[8:], version)
	for i := 16; i < len(b); i++ {
		b[i] = byte(k*13 + version*5 + uint64(i))
	}
	return b
}

// model is the oracle: the rows of every heap, keyed by row key, and the
// entries of the B+Tree.
type model struct {
	heaps   [numHeaps]map[uint64][]byte
	tree    map[uint64][]byte
	nextKey uint64
	version uint64
}

func newModel() *model {
	m := &model{nextKey: 1, tree: initialTree()}
	for i := range m.heaps {
		m.heaps[i] = map[uint64][]byte{}
	}
	return m
}

func (m *model) clone() *model {
	c := &model{nextKey: m.nextKey, version: m.version, tree: maps.Clone(m.tree)}
	for i, h := range m.heaps {
		c.heaps[i] = maps.Clone(h)
	}
	return c
}

func (m *model) apply(o op) {
	switch o.kind {
	case opInsert:
		m.heaps[o.heap][o.key] = tupleFor(o.key, o.version, o.size)
		m.nextKey = o.key + 1
		m.version = o.version
	case opUpdate:
		m.heaps[o.heap][o.key] = tupleFor(o.key, o.version, o.size)
		m.version = o.version
	case opDelete:
		delete(m.heaps[o.heap], o.key)
	case opTreeInsert:
		m.tree[o.key] = treeValue(o.key, o.version, o.size)
		m.version = o.version
	case opTreeDelete:
		delete(m.tree, o.key)
	}
}

// contents is what a database holds, read back.
type contents struct {
	heaps     [numHeaps]map[uint64][]byte
	tree      map[uint64][]byte
	treeStats btree.Stats
}

func sameMap(a, b map[uint64][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || !bytes.Equal(w, v) {
			return false
		}
	}
	return true
}

func (m *model) equal(c contents) bool {
	for i := range m.heaps {
		if !sameMap(m.heaps[i], c.heaps[i]) {
			return false
		}
	}
	return sameMap(m.tree, c.tree)
}

func (m *model) rowCount() int {
	n := len(m.tree)
	for _, h := range m.heaps {
		n += len(h)
	}
	return n
}

// workload generates operations. Each choice depends only on its random
// stream and the model, so the same seed always yields the same sequence of
// operations; a second process can regenerate what a killed one did.
type workload struct {
	rng *rand.Rand
}

func newWorkload(seed uint64) *workload {
	return &workload{rng: rand.New(rand.NewPCG(seed, seed^0x5eed))}
}

func (w *workload) rowSize() int {
	switch r := w.rng.IntN(100); {
	case r < 50:
		return tupleHeaderSize + w.rng.IntN(100)
	case r < 85:
		return tupleHeaderSize + w.rng.IntN(1500)
	case r < 98:
		return tupleHeaderSize + w.rng.IntN(4000)
	default:
		return storage.MaxRowData - w.rng.IntN(30)
	}
}

func (w *workload) next(m *model) op {
	if w.rng.IntN(100) < 45 {
		return w.nextTree(m)
	}
	h := w.rng.IntN(numHeaps)
	keys := make([]uint64, 0, len(m.heaps[h]))
	for k := range m.heaps[h] {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	switch r := w.rng.IntN(100); {
	case r < 40 || len(keys) == 0:
		return op{kind: opInsert, heap: h, key: m.nextKey, version: m.version + 1, size: w.rowSize()}
	case r < 65:
		return op{kind: opUpdate, heap: h, key: keys[w.rng.IntN(len(keys))], version: m.version + 1, size: w.rowSize()}
	case r < 80:
		return op{kind: opDelete, heap: h, key: keys[w.rng.IntN(len(keys))]}
	case r < 97:
		return op{kind: opFlush}
	default:
		return op{kind: opCheckpoint}
	}
}

// nextTree generates a B+Tree operation. The tree alternates between
// growing and shrinking phases, decided by the size of the model, so it
// splits and merges nodes at every height.
func (w *workload) nextTree(m *model) op {
	keys := make([]uint64, 0, len(m.tree))
	for k := range m.tree {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	// Phases follow the model's version counter, so a seed always yields
	// the same operations.
	insertPct := 75
	if (m.version/150)%2 == 1 || len(keys) > 2*treeTarget {
		insertPct = 15
	}
	if len(keys) == 0 || (w.rng.IntN(100) < insertPct && len(keys) < treeKeys) {
		k := uint64(w.rng.IntN(treeKeys))
		for m.tree[k] != nil {
			k = (k + 1) % treeKeys
		}
		size := w.rng.IntN(100)
		if w.rng.IntN(10) == 0 {
			size = btree.MaxValueSize
		}
		return op{kind: opTreeInsert, key: k, version: m.version + 1, size: size}
	}
	return op{kind: opTreeDelete, key: keys[w.rng.IntN(len(keys))]}
}

// ids identifies the workload's heaps and tree.
type ids struct {
	heaps [numHeaps]uint64
	tree  uint64
}

// db wraps an engine and its heaps, remembering where each row lives, and
// its tree.
type db struct {
	e     *wal.Engine
	heaps [numHeaps]*storage.Heap
	rids  [numHeaps]map[uint64]storage.RID
	tree  *btree.Tree
}

// create makes the workload's heaps and tree and acknowledges them.
func (d *db) create() (id ids, err error) {
	for i := range d.heaps {
		h, err := d.e.CreateHeap(bg)
		if err != nil {
			return id, err
		}
		d.heaps[i], d.rids[i], id.heaps[i] = h, map[uint64]storage.RID{}, h.FirstPage()
	}
	if d.tree, err = d.e.CreateBTree(bg); err != nil {
		return id, err
	}
	for k, v := range initialTree() {
		if err := d.tree.Insert(bg, treeKey(k), v); err != nil {
			return id, err
		}
	}
	id.tree = d.tree.Root()
	return id, d.e.Flush(bg)
}

// open opens the heaps and the tree and reads everything back, so the RID
// of each key is known again. The tree is checked structurally first.
func (d *db) open(id ids) (c contents, err error) {
	if d.tree, err = d.e.OpenBTree(bg, id.tree); err != nil {
		return c, fmt.Errorf("opening the tree: %w", err)
	}
	if c.treeStats, err = d.tree.Check(bg); err != nil {
		return c, fmt.Errorf("checking the tree: %w", err)
	}
	c.tree = map[uint64][]byte{}
	it := d.tree.Scan(btree.Bound{}, btree.Bound{})
	for {
		k, v, ok, err := it.Next(bg)
		if err != nil {
			return c, fmt.Errorf("scanning the tree: %w", err)
		}
		if !ok {
			break
		}
		if len(k) < 8 {
			return c, fmt.Errorf("tree key %x is too short", k)
		}
		key := binary.BigEndian.Uint64(k)
		if !bytes.Equal(k, treeKey(key)) {
			return c, fmt.Errorf("tree key %x is not the key of %d", k, key)
		}
		c.tree[key] = v
	}
	rows := &c.heaps
	for i, first := range id.heaps {
		h, err := d.e.OpenHeap(bg, first)
		if err != nil {
			return c, fmt.Errorf("opening heap %d: %w", i, err)
		}
		d.heaps[i], d.rids[i], rows[i] = h, map[uint64]storage.RID{}, map[uint64][]byte{}
		s := h.Scan()
		for {
			rid, v, ok, err := s.Next(bg)
			if err != nil {
				return c, fmt.Errorf("scanning heap %d: %w", i, err)
			}
			data := v.Data
			if !ok {
				break
			}
			if len(data) < tupleHeaderSize {
				return c, fmt.Errorf("heap %d: row %s is %d bytes", i, rid, len(data))
			}
			key := binary.LittleEndian.Uint64(data)
			if _, dup := rows[i][key]; dup {
				return c, fmt.Errorf("heap %d: key %d appears twice", i, key)
			}
			rows[i][key], d.rids[i][key] = data, rid
		}
	}
	return c, nil
}

// stamp gives every row the same header: this workload tests logging and
// recovery of heap pages, not row versions (the SQL workload does those).
func stamp(storage.RID, *storage.Version) (storage.RowHeader, error) {
	return storage.RowHeader{XID: 1}, nil
}

// do performs one operation on the database.
func (d *db) do(o op) error {
	switch o.kind {
	case opInsert:
		rid, err := d.heaps[o.heap].Insert(bg, tupleFor(o.key, o.version, o.size), stamp)
		if err == nil {
			d.rids[o.heap][o.key] = rid
		}
		return err
	case opUpdate:
		// The row keeps its RID, even when it moves.
		return d.heaps[o.heap].Update(bg, d.rids[o.heap][o.key], tupleFor(o.key, o.version, o.size), stamp)
	case opDelete:
		err := d.heaps[o.heap].Delete(bg, d.rids[o.heap][o.key], stamp)
		if err == nil {
			delete(d.rids[o.heap], o.key)
		}
		return err
	case opTreeInsert:
		return d.tree.Insert(bg, treeKey(o.key), treeValue(o.key, o.version, o.size))
	case opTreeDelete:
		found, err := d.tree.Delete(bg, treeKey(o.key))
		if err == nil && !found {
			return fmt.Errorf("tree key %d is missing", o.key)
		}
		return err
	case opFlush:
		return d.e.Flush(bg)
	case opCheckpoint:
		_, err := d.e.Checkpoint(bg)
		return err
	}
	return fmt.Errorf("unknown op %d", o.kind)
}

// envInt reads a positive integer from the environment, or returns def.
func envInt(t testing.TB, name string, def int) int {
	t.Helper()
	s := os.Getenv(name)
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		t.Fatalf("bad %s=%q", name, s)
	}
	return v
}

func baseSeed(t testing.TB) uint64 {
	t.Helper()
	seed := uint64(1)
	if s := os.Getenv("NOVACDB_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("bad NOVACDB_SEED %q: %v", s, err)
		}
		seed = v
	}
	t.Logf("base seed = %d (override with NOVACDB_SEED)", seed)
	return seed
}
