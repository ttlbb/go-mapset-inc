package mapsetinc

import (
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"unsafe"
)

// Benchmarks scale with MAPSETINC_BENCH_N distinct keys (default 2^20), e.g.
//
//	MAPSETINC_BENCH_N=100000000 go test -run '^$' -bench . -benchtime 1x
func benchN(b *testing.B) int {
	if s := os.Getenv("MAPSETINC_BENCH_N"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			b.Fatalf("invalid MAPSETINC_BENCH_N %q", s)
		}
		return n
	}
	return 1 << 20
}

// benchKey maps an index to a distinct, non-sequential key: multiplying by
// an odd constant is a bijection modulo 2^64.
func benchKey(i int) uint64 {
	return uint64(i) * 0x9e3779b97f4a7c15
}

// fillTable inserts n distinct keys using one AddMany stream per CPU.
func fillTable(tbl *Table[uint64, uint16], n int) {
	workers := runtime.GOMAXPROCS(0)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]uint64, 0, batchChunk)
			for i := w; i < n; i += workers {
				if batch = append(batch, benchKey(i)); len(batch) == cap(batch) {
					tbl.AddMany(batch)
					batch = batch[:0]
				}
			}
			tbl.AddMany(batch)
		}()
	}
	wg.Wait()
}

func heapInUse() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// reportTableMemory reports the heap growth per key, which includes pooled
// AddMany scratch buffers, and the bucket arrays alone per key.
func reportTableMemory(b *testing.B, tbl *Table[uint64, uint16], base uint64, n int) {
	b.ReportMetric(float64(heapInUse()-base)/float64(n), "B/key")
	bucketBytes := bucketCount(tbl) * int(unsafe.Sizeof(bucket[uint64, uint16]{}))
	b.ReportMetric(float64(bucketBytes)/float64(n), "bucketB/key")
}

// BenchmarkMemory reports heap bytes per distinct key; run it with
// -benchtime 1x because each iteration builds a full table.
func BenchmarkMemory(b *testing.B) {
	n := benchN(b)
	b.Run("Table/prealloc", func(b *testing.B) {
		for range b.N {
			base := heapInUse()
			tbl := NewTable[uint64, uint16](n)
			fillTable(tbl, n)
			reportTableMemory(b, tbl, base, n)
		}
	})
	b.Run("Table/grow", func(b *testing.B) {
		for range b.N {
			base := heapInUse()
			tbl := NewTable[uint64, uint16](0)
			fillTable(tbl, n)
			reportTableMemory(b, tbl, base, n)
		}
	})
	b.Run("BuiltinMap/prealloc", func(b *testing.B) {
		for range b.N {
			base := heapInUse()
			m := make(map[uint64]uint16, n)
			for i := range n {
				m[benchKey(i)]++
			}
			b.ReportMetric(float64(heapInUse()-base)/float64(n), "B/key")
			runtime.KeepAlive(m)
		}
	})
}

func BenchmarkAddExisting(b *testing.B) {
	n := benchN(b)
	b.Run("Table", func(b *testing.B) {
		tbl := NewTable[uint64, uint16](n)
		fillTable(tbl, n)
		rng := rand.New(rand.NewPCG(1, 2))
		b.ResetTimer()
		for range b.N {
			tbl.Add(benchKey(rng.IntN(n)))
		}
	})
	b.Run("BuiltinMap", func(b *testing.B) {
		m := make(map[uint64]uint16, n)
		for i := range n {
			m[benchKey(i)]++
		}
		rng := rand.New(rand.NewPCG(1, 2))
		b.ResetTimer()
		for range b.N {
			m[benchKey(rng.IntN(n))]++
		}
	})
}

func BenchmarkAddParallel(b *testing.B) {
	n := benchN(b)
	tbl := NewTable[uint64, uint16](n)
	fillTable(tbl, n)
	b.Run("Add", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewPCG(rand.Uint64(), 0))
			for pb.Next() {
				tbl.Add(benchKey(rng.IntN(n)))
			}
		})
	})
	b.Run("AddMany", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewPCG(rand.Uint64(), 0))
			batch := make([]uint64, 0, batchChunk)
			for pb.Next() {
				if batch = append(batch, benchKey(rng.IntN(n))); len(batch) == cap(batch) {
					tbl.AddMany(batch)
					batch = batch[:0]
				}
			}
			tbl.AddMany(batch)
		})
	})
}

func BenchmarkInsertParallel(b *testing.B) {
	n := benchN(b)
	for range b.N {
		fillTable(NewTable[uint64, uint16](n), n)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/key")
}
