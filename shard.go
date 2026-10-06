package mapsetinc

import (
	"math"
	"sync"
)

// bucketSlots is chosen so that a bucket of uint64 keys and uint16 counts is
// exactly 64 bytes (6*8 + 6*2 = 60, padded to 64): one cache line per probe.
const bucketSlots = 6

// cacheLinePad separates the hot fields of neighbouring shards. 128 bytes
// covers the 128-byte lines of Apple silicon as well as 64-byte lines.
const cacheLinePad = 128

// bucket stores keys and counts in separate arrays so that no padding is
// spent between a key and its count. A slot whose count is zero is empty, so
// no extra metadata is needed and the zero key is an ordinary key.
type bucket[K Integer, C Unsigned] struct {
	keys   [bucketSlots]K
	counts [bucketSlots]C
}

// shard is an open-addressing hash table with linear probing over the
// flattened slot sequence, starting at the first slot of the home bucket.
// Deletion uses backward shifting, so there are no tombstones.
type shard[K Integer, C Unsigned] struct {
	mu        sync.Mutex
	buckets   []bucket[K, C]
	seed      uint64
	maxLoad   int // per mille of all slots
	len       int
	growAt    int
	total     uint64
	saturated int
	_         [cacheLinePad]byte
}

func maxCount[C Unsigned]() C {
	var zero C
	return ^zero
}

// bucketsFor returns the number of buckets that holds n entries without
// exceeding maxLoad per mille of all slots.
func bucketsFor(n, maxLoad int) int {
	slots := (uint64(n)*1000 + uint64(maxLoad) - 1) / uint64(maxLoad)
	buckets := max((slots+bucketSlots-1)/bucketSlots, 1)
	if buckets > math.MaxUint32 {
		panic("mapsetinc: shard capacity too large; use more shards")
	}
	return int(buckets)
}

func (s *shard[K, C]) init(seed uint64, maxLoad, capacity int) {
	s.seed = seed
	s.maxLoad = maxLoad
	s.setBuckets(make([]bucket[K, C], bucketsFor(capacity, maxLoad)))
}

func (s *shard[K, C]) setBuckets(buckets []bucket[K, C]) {
	s.buckets = buckets
	s.growAt = len(buckets) * bucketSlots * s.maxLoad / 1000
}

func (s *shard[K, C]) home(key K) int {
	return reduce(mix64(uint64(key), s.seed), len(s.buckets))
}

// find returns the slot holding key, or the empty slot where key would be
// inserted. The load factor guarantees an empty slot exists.
func (s *shard[K, C]) find(key K, h uint64) (bi, j int, found bool) {
	bi = reduce(h, len(s.buckets))
	for {
		b := &s.buckets[bi]
		for j = range bucketSlots {
			if b.counts[j] == 0 {
				return bi, j, false
			}
			if b.keys[j] == key {
				return bi, j, true
			}
		}
		if bi++; bi == len(s.buckets) {
			bi = 0
		}
	}
}

func (s *shard[K, C]) count(key K, h uint64) C {
	bi, j, found := s.find(key, h)
	if !found {
		return 0
	}
	return s.buckets[bi].counts[j]
}

// inc adds one to key's count, inserting it with count 1 if absent. A count
// that has reached the maximum of C stays there.
func (s *shard[K, C]) inc(key K, h uint64) C {
	bi, j, found := s.find(key, h)
	if found {
		b := &s.buckets[bi]
		c := b.counts[j]
		if c == maxCount[C]() {
			return c
		}
		c++
		b.counts[j] = c
		s.total++
		if c == maxCount[C]() {
			s.saturated++
		}
		return c
	}
	if s.len >= s.growAt {
		s.resize(bucketsFor(s.len+s.len/2+1, s.maxLoad))
		bi, j, _ = s.find(key, h)
	}
	b := &s.buckets[bi]
	b.keys[j] = key
	b.counts[j] = 1
	s.len++
	s.total++
	return 1
}

// dec subtracts one from key's count and removes key when it reaches zero.
func (s *shard[K, C]) dec(key K, h uint64) C {
	bi, j, found := s.find(key, h)
	if !found {
		return 0
	}
	c := s.buckets[bi].counts[j]
	if c == maxCount[C]() {
		s.saturated--
	}
	s.total--
	if c > 1 {
		s.buckets[bi].counts[j] = c - 1
		return c - 1
	}
	s.removeAt(bi*bucketSlots + j)
	return 0
}

// remove deletes key regardless of its count and returns the count it had.
func (s *shard[K, C]) remove(key K, h uint64) C {
	bi, j, found := s.find(key, h)
	if !found {
		return 0
	}
	c := s.buckets[bi].counts[j]
	if c == maxCount[C]() {
		s.saturated--
	}
	s.total -= uint64(c)
	s.removeAt(bi*bucketSlots + j)
	return c
}

// removeAt empties slot p and shifts later entries of the same probe run
// backwards so that every entry stays reachable from its home bucket.
func (s *shard[K, C]) removeAt(p int) {
	n := len(s.buckets) * bucketSlots
	s.len--
	for q := p; ; {
		if q++; q == n {
			q = 0
		}
		qb := &s.buckets[q/bucketSlots]
		qj := q % bucketSlots
		if qb.counts[qj] == 0 {
			s.buckets[p/bucketSlots].counts[p%bucketSlots] = 0
			return
		}
		home := s.home(qb.keys[qj]) * bucketSlots
		// The entry at q may fill the hole at p only if p lies on its probe
		// path, i.e. p is no closer to q than its home slot is.
		if (q-home+n)%n >= (q-p+n)%n {
			pb := &s.buckets[p/bucketSlots]
			pb.keys[p%bucketSlots] = qb.keys[qj]
			pb.counts[p%bucketSlots] = qb.counts[qj]
			p = q
		}
	}
}

// resize rehashes every entry into a new bucket array of the given size.
func (s *shard[K, C]) resize(buckets int) {
	old := s.buckets
	s.setBuckets(make([]bucket[K, C], buckets))
	for i := range old {
		b := &old[i]
		for j := range bucketSlots {
			if b.counts[j] != 0 {
				bi, nj, _ := s.find(b.keys[j], mix64(uint64(b.keys[j]), s.seed))
				s.buckets[bi].keys[nj] = b.keys[j]
				s.buckets[bi].counts[nj] = b.counts[j]
			}
		}
	}
}

func (s *shard[K, C]) clear() {
	clear(s.buckets)
	s.len, s.total, s.saturated = 0, 0, 0
}

// compact shrinks the bucket array to the smallest size that holds the
// current entries; Go maps never shrink, this table does on request.
func (s *shard[K, C]) compact() {
	if buckets := bucketsFor(s.len, s.maxLoad); buckets < len(s.buckets) {
		s.resize(buckets)
	}
}
