package mapsetinc

import (
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
)

func collect[K Integer, C Unsigned](t *Table[K, C]) map[K]C {
	got := make(map[K]C)
	for k, c := range t.All() {
		if _, dup := got[k]; dup {
			panic("All yielded a key twice")
		}
		got[k] = c
	}
	return got
}

func TestAddCountsEachDistinctKeyOnce(t *testing.T) {
	tbl := NewTable[uint64, uint16](0)
	for i, want := range []uint16{1, 2, 3} {
		if got := tbl.Add(42); got != want {
			t.Fatalf("Add #%d = %d, want %d", i+1, got, want)
		}
	}
	tbl.Add(0)
	tbl.Add(^uint64(0))

	want := map[uint64]uint16{42: 3, 0: 1, ^uint64(0): 1}
	if got := collect(tbl); !maps.Equal(got, want) {
		t.Fatalf("All = %v, want %v", got, want)
	}
	if tbl.Len() != 3 || tbl.Total() != 5 {
		t.Fatalf("Len, Total = %d, %d, want 3, 5", tbl.Len(), tbl.Total())
	}
	if !tbl.Contains(0) || tbl.Contains(7) || tbl.Count(7) != 0 {
		t.Fatal("Contains/Count disagree with inserted keys")
	}
}

func TestSignedKeys(t *testing.T) {
	tbl := NewTable[int8, uint8](0, WithShards(1))
	for k := -128; k <= 127; k++ {
		tbl.Add(int8(k))
	}
	tbl.Add(-1)
	if tbl.Len() != 256 || tbl.Count(-1) != 2 || tbl.Count(-128) != 1 {
		t.Fatalf("Len=%d Count(-1)=%d Count(-128)=%d", tbl.Len(), tbl.Count(-1), tbl.Count(-128))
	}
}

func TestCountSaturatesAtMaximum(t *testing.T) {
	tbl := NewTable[uint32, uint8](0)
	for range 300 {
		tbl.Add(9)
	}
	if c := tbl.Count(9); c != 255 {
		t.Fatalf("Count = %d, want 255", c)
	}
	if tbl.Saturated() != 1 || tbl.Total() != 255 {
		t.Fatalf("Saturated, Total = %d, %d, want 1, 255", tbl.Saturated(), tbl.Total())
	}
	if c := tbl.Remove(9); c != 254 || tbl.Saturated() != 0 {
		t.Fatalf("Remove = %d, Saturated = %d, want 254, 0", c, tbl.Saturated())
	}
	for range 2 {
		tbl.Add(9)
	}
	if c := tbl.Delete(9); c != 255 || tbl.Saturated() != 0 || tbl.Total() != 0 {
		t.Fatalf("Delete = %d, Saturated = %d, Total = %d", c, tbl.Saturated(), tbl.Total())
	}
}

func TestRemoveAndDelete(t *testing.T) {
	tbl := NewTable[uint64, uint16](0)
	tbl.Add(1)
	tbl.Add(1)
	if c := tbl.Remove(1); c != 1 {
		t.Fatalf("Remove = %d, want 1", c)
	}
	if c := tbl.Remove(1); c != 0 || tbl.Contains(1) || tbl.Len() != 0 {
		t.Fatalf("Remove to zero = %d, Contains = %v, Len = %d", c, tbl.Contains(1), tbl.Len())
	}
	if tbl.Remove(1) != 0 || tbl.Delete(1) != 0 {
		t.Fatal("removing an absent key must return 0")
	}
}

// TestRandomOperationsMatchReference drives a small key space with heavy
// collisions, growth and backward-shift deletion against a built-in map.
func TestRandomOperationsMatchReference(t *testing.T) {
	for shards, maxLoad := range map[int]float64{1: 0.95, 4: 0.5} {
		rng := rand.New(rand.NewPCG(1, uint64(shards)))
		tbl := NewTable[uint64, uint8](0, WithShards(shards), WithMaxLoad(maxLoad))
		ref := make(map[uint64]uint8)
		for i := range 200_000 {
			k := rng.Uint64N(3000)
			switch op := rng.IntN(10); {
			case op < 6:
				if ref[k] < 255 {
					ref[k]++
				}
				if got := tbl.Add(k); got != ref[k] {
					t.Fatalf("shards=%d op %d: Add(%d) = %d, want %d", shards, i, k, got, ref[k])
				}
			case op < 9:
				if ref[k] > 0 {
					ref[k]--
				}
				if ref[k] == 0 {
					delete(ref, k)
				}
				if got := tbl.Remove(k); got != ref[k] {
					t.Fatalf("shards=%d op %d: Remove(%d) = %d, want %d", shards, i, k, got, ref[k])
				}
			default:
				want := ref[k]
				delete(ref, k)
				if got := tbl.Delete(k); got != want {
					t.Fatalf("shards=%d op %d: Delete(%d) = %d, want %d", shards, i, k, got, want)
				}
			}
		}
		if got := collect(tbl); !maps.Equal(got, ref) {
			t.Fatalf("shards=%d: table diverged from reference", shards)
		}
		var total uint64
		for k, c := range ref {
			total += uint64(c)
			if tbl.Count(k) != c {
				t.Fatalf("shards=%d: Count(%d) = %d, want %d", shards, k, tbl.Count(k), c)
			}
		}
		if tbl.Len() != len(ref) || tbl.Total() != total {
			t.Fatalf("shards=%d: Len, Total = %d, %d, want %d, %d", shards, tbl.Len(), tbl.Total(), len(ref), total)
		}
	}
}

func bucketCount[K Integer, C Unsigned](t *Table[K, C]) int {
	n := 0
	for i := range t.shards {
		n += len(t.shards[i].buckets)
	}
	return n
}

func TestCompactReleasesMemoryAndKeepsEntries(t *testing.T) {
	tbl := NewTable[uint64, uint16](0, WithShards(4))
	for k := range uint64(100_000) {
		tbl.Add(k)
	}
	for k := range uint64(99_000) {
		tbl.Delete(k)
	}
	before := bucketCount(tbl)
	tbl.Compact()
	if after := bucketCount(tbl); after*10 > before {
		t.Fatalf("Compact kept %d of %d buckets", after, before)
	}
	if tbl.Len() != 1000 || tbl.Count(99_000) != 1 || tbl.Count(99_999) != 1 {
		t.Fatal("Compact lost entries")
	}
	tbl.Add(1)
	if tbl.Len() != 1001 {
		t.Fatal("table must keep growing after Compact")
	}
}

func TestClearKeepsCapacity(t *testing.T) {
	tbl := NewTable[uint64, uint16](10_000)
	for k := range uint64(10_000) {
		tbl.Add(k)
	}
	before := bucketCount(tbl)
	tbl.Clear()
	if tbl.Len() != 0 || tbl.Total() != 0 || tbl.Contains(5) || bucketCount(tbl) != before {
		t.Fatal("Clear must empty the table and keep its buckets")
	}
}

func TestPreallocatedTableDoesNotGrow(t *testing.T) {
	const n = 1_000_000
	tbl := NewTable[uint64, uint16](n)
	before := bucketCount(tbl)
	for k := range uint64(n) {
		tbl.Add(k)
	}
	if bucketCount(tbl) != before {
		t.Fatalf("table grew from %d to %d buckets", before, bucketCount(tbl))
	}
}

func TestMostCommon(t *testing.T) {
	tbl := NewTable[uint64, uint16](0)
	for k, n := range map[uint64]int{1: 5, 2: 9, 3: 5, 4: 1, 5: 7} {
		for range n {
			tbl.Add(k)
		}
	}
	want := []Entry[uint64, uint16]{{2, 9}, {5, 7}, {1, 5}}
	if got := tbl.MostCommon(3); !slices.Equal(got, want) {
		t.Fatalf("MostCommon(3) = %v, want %v", got, want)
	}
	if got := tbl.MostCommon(10); len(got) != 5 || got[4] != (Entry[uint64, uint16]{4, 1}) {
		t.Fatalf("MostCommon(10) = %v", got)
	}
	if got := tbl.MostCommon(0); got != nil {
		t.Fatalf("MostCommon(0) = %v, want nil", got)
	}
}

func TestAllAllowsMutationAndEarlyStop(t *testing.T) {
	tbl := NewTable[uint64, uint16](0, WithShards(1))
	for k := range uint64(100) {
		tbl.Add(k)
	}
	seen := 0
	for k := range tbl.All() {
		tbl.Add(k) // must not deadlock
		if seen++; seen == 10 {
			break
		}
	}
	if seen != 10 || tbl.Total() != 110 {
		t.Fatalf("seen = %d, Total = %d, want 10, 110", seen, tbl.Total())
	}
}

func TestAddManyMatchesAdd(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	keys := make([]uint64, 3*batchChunk+17)
	for i := range keys {
		keys[i] = rng.Uint64N(5000)
	}
	batch := NewTable[uint64, uint16](0)
	single := NewTable[uint64, uint16](0)
	batch.AddMany(keys)
	for _, k := range keys {
		single.Add(k)
	}
	if !maps.Equal(collect(batch), collect(single)) {
		t.Fatal("AddMany and Add produced different tables")
	}
}

func TestConcurrentAdd(t *testing.T) {
	const workers, keys, rounds = 8, 2000, 50
	tbl := NewTable[uint64, uint32](0, WithShards(16))
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]uint64, 0, keys)
			for range rounds {
				for k := range uint64(keys) {
					if w%2 == 0 {
						tbl.Add(k)
					} else {
						batch = append(batch, k)
					}
				}
				tbl.AddMany(batch)
				batch = batch[:0]
				tbl.Count(uint64(w))
			}
		}()
	}
	wg.Wait()
	if tbl.Len() != keys || tbl.Total() != workers*keys*rounds {
		t.Fatalf("Len, Total = %d, %d", tbl.Len(), tbl.Total())
	}
	for k, c := range tbl.All() {
		if c != workers*rounds {
			t.Fatalf("Count(%d) = %d, want %d", k, c, workers*rounds)
		}
	}
}

func TestHotPathDoesNotAllocate(t *testing.T) {
	tbl := NewTable[uint64, uint16](1000)
	for k := range uint64(1000) {
		tbl.Add(k)
	}
	var k uint64
	if a := testing.AllocsPerRun(1000, func() { tbl.Add(k % 1000); k++ }); a != 0 {
		t.Fatalf("Add allocates %v times per call", a)
	}
	if a := testing.AllocsPerRun(1000, func() { tbl.Count(k); k++ }); a != 0 {
		t.Fatalf("Count allocates %v times per call", a)
	}
}

func TestNewTableRejectsInvalidArguments(t *testing.T) {
	for name, f := range map[string]func(){
		"negative capacity": func() { NewTable[uint64, uint16](-1) },
		"zero shards":       func() { NewTable[uint64, uint16](0, WithShards(0)) },
		"non power of two":  func() { NewTable[uint64, uint16](0, WithShards(3)) },
		"too many shards":   func() { NewTable[uint64, uint16](0, WithShards(1<<17)) },
		"load too low":      func() { NewTable[uint64, uint16](0, WithMaxLoad(0.4)) },
		"load too high":     func() { NewTable[uint64, uint16](0, WithMaxLoad(0.96)) },
		"load NaN":          func() { NewTable[uint64, uint16](0, WithMaxLoad(math.NaN())) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: expected panic", name)
				}
			}()
			f()
		}()
	}
}
