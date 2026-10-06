// Package mapsetinc provides Table, a concurrent counting set for integer
// keys designed for billions of entries: every key is stored once and each
// Add increments its exact count.
//
// A Table of uint64 keys with uint16 counts uses about 12 bytes per entry,
// roughly a third of a built-in map[uint64]uint16.
package mapsetinc

import (
	"iter"
	"math"
	"math/bits"
	"math/rand/v2"
	"slices"
	"sync"
)

// Integer is the set of key types a Table accepts.
type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

// Unsigned is the set of count types a Table accepts. Narrower counts use
// less memory when the key type is narrow enough for alignment to allow it.
type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

// Entry is a key together with its count.
type Entry[K Integer, C Unsigned] struct {
	Key   K
	Count C
}

const (
	defaultShards  = 256
	maxShards      = 1 << 16
	defaultMaxLoad = 0.9
	// batchChunk bounds the scratch memory AddMany needs per call.
	batchChunk = 1 << 14
)

type config struct {
	shards  int
	maxLoad float64
}

// Option configures a Table.
type Option func(*config)

// WithShards sets the number of independently locked shards. It must be a
// power of two between 1 and 65536. More shards reduce lock contention and
// the memory spike of growing one shard; the default is 256.
func WithShards(n int) Option {
	return func(c *config) { c.shards = n }
}

// WithMaxLoad sets the fraction of slots that may be filled before a shard
// grows, between 0.5 and 0.95. The default of 0.9 minimises memory (about
// 12 bytes per uint64/uint16 entry). Lower values trade memory for speed:
// with 1e8 uint64 keys, random single-threaded Add measured 104 ns at 0.9,
// 88 ns at 0.8 (13.3 B/entry) and 64 ns at 0.7 (15.2 B/entry).
func WithMaxLoad(f float64) Option {
	return func(c *config) { c.maxLoad = f }
}

// Table is a concurrent counting set: each distinct key is stored once with
// a count of how many times it has been added. Counts are exact; a count
// that reaches the maximum value of C stays at that maximum.
//
// All methods are safe for concurrent use. A Table must be created with
// NewTable and must not be copied.
type Table[K Integer, C Unsigned] struct {
	shards    []shard[K, C]
	shardBits uint
	seed      uint64
	scratch   sync.Pool
}

// NewTable returns an empty Table sized to hold capacity distinct keys
// without growing. A capacity of 0 starts small and grows on demand.
func NewTable[K Integer, C Unsigned](capacity int, opts ...Option) *Table[K, C] {
	if capacity < 0 {
		panic("mapsetinc: negative capacity")
	}
	cfg := config{shards: defaultShards, maxLoad: defaultMaxLoad}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.shards < 1 || cfg.shards > maxShards || cfg.shards&(cfg.shards-1) != 0 {
		panic("mapsetinc: shard count must be a power of two between 1 and 65536")
	}
	if !(cfg.maxLoad >= 0.5 && cfg.maxLoad <= 0.95) {
		panic("mapsetinc: max load must be between 0.5 and 0.95")
	}
	t := &Table[K, C]{
		shards:    make([]shard[K, C], cfg.shards),
		shardBits: uint(bits.TrailingZeros(uint(cfg.shards))),
		seed:      rand.Uint64(),
	}
	perShard := shardCapacity(capacity, cfg.shards)
	maxLoad := int(math.Round(cfg.maxLoad * 1000))
	for i := range t.shards {
		t.shards[i].init(t.seed, maxLoad, perShard)
	}
	return t
}

// shardCapacity sizes one shard for its share of capacity plus four standard
// deviations of the binomial spread, so that preallocated shards almost
// never have to grow when the table is filled to capacity.
func shardCapacity(capacity, shards int) int {
	mean := (capacity + shards - 1) / shards
	if shards == 1 || mean == 0 {
		return mean
	}
	return mean + 4*int(math.Sqrt(float64(mean))) + 1
}

func (t *Table[K, C]) hash(key K) uint64 {
	return mix64(uint64(key), t.seed)
}

// shardIndex uses the high hash bits; buckets within a shard use the low
// bits. With one shard the shift is 64, which Go defines to yield 0.
func (t *Table[K, C]) shardIndex(h uint64) uint64 {
	return h >> (64 - t.shardBits)
}

func (t *Table[K, C]) shardOf(h uint64) *shard[K, C] {
	return &t.shards[t.shardIndex(h)]
}

// Add increments key's count, inserting key with count 1 if it is absent,
// and returns the new count.
func (t *Table[K, C]) Add(key K) C {
	h := t.hash(key)
	s := t.shardOf(h)
	s.mu.Lock()
	c := s.inc(key, h)
	s.mu.Unlock()
	return c
}

type hashedKey[K Integer] struct {
	key  K
	hash uint64
}

// batchScratch is reused across AddMany calls through Table.scratch.
type batchScratch[K Integer] struct {
	sorted  []hashedKey[K]
	offsets []int
}

// AddMany increments the count of every key in keys, once per occurrence.
// Keys are grouped by shard so that each shard is locked once per chunk,
// which is faster than calling Add for each key in large batches.
func (t *Table[K, C]) AddMany(keys []K) {
	sc, _ := t.scratch.Get().(*batchScratch[K])
	if sc == nil {
		sc = &batchScratch[K]{
			sorted:  make([]hashedKey[K], batchChunk),
			offsets: make([]int, len(t.shards)+1),
		}
	}
	for chunk := range slices.Chunk(keys, batchChunk) {
		t.addChunk(chunk, sc.sorted[:len(chunk)], sc.offsets)
	}
	t.scratch.Put(sc)
}

// addChunk counting-sorts keys by shard into sorted, then applies each
// shard's run under a single lock acquisition.
func (t *Table[K, C]) addChunk(keys []K, sorted []hashedKey[K], offsets []int) {
	clear(offsets)
	for _, k := range keys {
		offsets[t.shardIndex(t.hash(k))+1]++
	}
	for i := 1; i < len(offsets); i++ {
		offsets[i] += offsets[i-1]
	}
	for _, k := range keys {
		h := t.hash(k)
		si := t.shardIndex(h)
		sorted[offsets[si]] = hashedKey[K]{k, h}
		offsets[si]++
	}
	start := 0
	for si := range t.shards {
		end := offsets[si]
		if end == start {
			continue
		}
		s := &t.shards[si]
		s.mu.Lock()
		for _, e := range sorted[start:end] {
			s.inc(e.key, e.hash)
		}
		s.mu.Unlock()
		start = end
	}
}

// Count returns key's count, or 0 if key is absent.
func (t *Table[K, C]) Count(key K) C {
	h := t.hash(key)
	s := t.shardOf(h)
	s.mu.Lock()
	c := s.count(key, h)
	s.mu.Unlock()
	return c
}

// Contains reports whether key is present.
func (t *Table[K, C]) Contains(key K) bool {
	return t.Count(key) != 0
}

// Remove decrements key's count, deleting key when the count reaches zero,
// and returns the remaining count. Removing an absent key returns 0.
func (t *Table[K, C]) Remove(key K) C {
	h := t.hash(key)
	s := t.shardOf(h)
	s.mu.Lock()
	c := s.dec(key, h)
	s.mu.Unlock()
	return c
}

// Delete removes key regardless of its count and returns the count it had.
func (t *Table[K, C]) Delete(key K) C {
	h := t.hash(key)
	s := t.shardOf(h)
	s.mu.Lock()
	c := s.remove(key, h)
	s.mu.Unlock()
	return c
}

// sum adds up a per-shard statistic, locking one shard at a time. Under
// concurrent writes the result is not an atomic snapshot of the table.
func sum[K Integer, C Unsigned, N int | uint64](t *Table[K, C], field func(*shard[K, C]) N) N {
	var total N
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		total += field(s)
		s.mu.Unlock()
	}
	return total
}

// Len returns the number of distinct keys.
func (t *Table[K, C]) Len() int {
	return sum(t, func(s *shard[K, C]) int { return s.len })
}

// Total returns the sum of all stored counts. Increments dropped because a
// count was already at its maximum are not included.
func (t *Table[K, C]) Total() uint64 {
	return sum(t, func(s *shard[K, C]) uint64 { return s.total })
}

// Saturated returns the number of keys whose count has reached the maximum
// value of C and therefore no longer reflects further additions.
func (t *Table[K, C]) Saturated() int {
	return sum(t, func(s *shard[K, C]) int { return s.saturated })
}

// All iterates over every key and its count in unspecified order. Each shard
// is copied under its lock and yielded after unlocking, so the loop body may
// call any Table method; the copy needs memory for one shard's entries. The
// view of each shard is consistent, the view across shards is not.
func (t *Table[K, C]) All() iter.Seq2[K, C] {
	return func(yield func(K, C) bool) {
		var buf []Entry[K, C]
		for i := range t.shards {
			s := &t.shards[i]
			s.mu.Lock()
			buf = slices.Grow(buf[:0], s.len)
			for bi := range s.buckets {
				b := &s.buckets[bi]
				for j := range bucketSlots {
					if b.counts[j] != 0 {
						buf = append(buf, Entry[K, C]{b.keys[j], b.counts[j]})
					}
				}
			}
			s.mu.Unlock()
			for _, e := range buf {
				if !yield(e.Key, e.Count) {
					return
				}
			}
		}
	}
}

// before orders entries by descending count, then ascending key.
func before[K Integer, C Unsigned](a, b Entry[K, C]) bool {
	return a.Count > b.Count || a.Count == b.Count && a.Key < b.Key
}

// MostCommon returns the n keys with the highest counts, ordered by count
// descending and then by key ascending. It keeps a bounded heap of n entries
// instead of sorting the whole table.
func (t *Table[K, C]) MostCommon(n int) []Entry[K, C] {
	if n <= 0 {
		return nil
	}
	top := make([]Entry[K, C], 0, min(n, t.Len()))
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		for bi := range s.buckets {
			b := &s.buckets[bi]
			for j := range bucketSlots {
				if b.counts[j] != 0 {
					top = pushTop(top, n, Entry[K, C]{b.keys[j], b.counts[j]})
				}
			}
		}
		s.mu.Unlock()
	}
	slices.SortFunc(top, func(a, b Entry[K, C]) int {
		if before(a, b) {
			return -1
		}
		return 1
	})
	return top
}

// pushTop maintains top as a heap of at most n entries whose root is the
// entry that would be dropped first.
func pushTop[K Integer, C Unsigned](top []Entry[K, C], n int, e Entry[K, C]) []Entry[K, C] {
	if len(top) < n {
		top = append(top, e)
		for i := len(top) - 1; i > 0; {
			parent := (i - 1) / 2
			if !before(top[parent], top[i]) {
				break
			}
			top[parent], top[i] = top[i], top[parent]
			i = parent
		}
		return top
	}
	if !before(e, top[0]) {
		return top
	}
	top[0] = e
	for i := 0; ; {
		worst, l, r := i, 2*i+1, 2*i+2
		if l < len(top) && before(top[worst], top[l]) {
			worst = l
		}
		if r < len(top) && before(top[worst], top[r]) {
			worst = r
		}
		if worst == i {
			return top
		}
		top[i], top[worst] = top[worst], top[i]
		i = worst
	}
}

// Compact shrinks each shard's memory to fit its current entries. Use it
// after removing many keys; the table never shrinks on its own.
func (t *Table[K, C]) Compact() {
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		s.compact()
		s.mu.Unlock()
	}
}

// Clear removes every key and keeps the allocated memory for reuse.
func (t *Table[K, C]) Clear() {
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		s.clear()
		s.mu.Unlock()
	}
}
