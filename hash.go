package mapsetinc

// mix64 is the MurmurHash3 64-bit finalizer applied to key^seed. It is a
// bijection, so distinct keys never collide in the full 64-bit hash, and it
// spreads sequential IDs evenly over both the high bits (shard selection) and
// the low 32 bits (bucket selection).
func mix64(key, seed uint64) uint64 {
	x := key ^ seed
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// reduce maps the low 32 bits of h uniformly onto [0, n) without a division
// (Lemire's multiply-shift), which lets bucket counts be any size instead of
// a power of two and avoids up to 2x memory waste from rounding.
func reduce(h uint64, n int) int {
	return int((h & 0xffffffff) * uint64(n) >> 32)
}
