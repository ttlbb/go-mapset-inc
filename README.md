# go-mapset-inc

A concurrent, exact counting set for integer keys, designed for billion-scale workloads. Every key is stored once and each `Add` increments its count.

```go
import mapsetinc "github.com/ttlbb/go-mapset-inc"

t := mapsetinc.NewTable[uint64, uint16](1_000_000_000) // prealloc for 1e9 distinct keys
t.Add(42)                 // first Add counts 1, every further Add +1, returns the new count
t.Count(42)               // 1
t.AddMany(batch)          // faster than Add for large batches
t.Remove(42)              // count -1, deleted at 0
t.Delete(42)              // remove regardless of count
top := t.MostCommon(10)   // top-10 by count, ties broken by key
for k, c := range t.All() // iterate, key + count
```

## Requirements

Go 1.23 or later. `K` is any integer type (`uint64`, `int32`, …), `C` is any unsigned integer type (`uint16`, `uint32`, …).

## Design

- Each 64-byte bucket holds 6 keys and 6 counts in parallel arrays. `count == 0` marks an empty slot, so there is no extra metadata and the zero key works normally.
- Backward-shift deletion: no tombstones, so delete-heavy workloads do not degrade the table.
- 256 independently locked shards by default (`WithShards(n)`, power of two). Growth rehashes one shard at a time, so the resize spike is ~1/256 of the table instead of a full copy.
- Counts saturate at the maximum of `C` instead of wrapping; `Saturated()` reports how many keys hit the cap. `Total()` counts only stored additions.
- `All`, `Len`, `Total`, `Saturated` lock shards one at a time: each shard's view is consistent, the result across shards is not an atomic snapshot.
- `Compact()` releases memory after mass deletions; the table never shrinks by itself.

## Memory and speed

Measured on Apple M6, uint64 keys with uint16 counts (see `bench_test.go`; scale with `MAPSETINC_BENCH_N`):

| | B/key @1e8 | B/key @1e9 | random `Add`, 1 thread | 12 threads |
| --- | --- | --- | --- | --- |
| `Table` (preallocated, maxLoad 0.9) | 11.97 | 11.88 | ~104 ns | ~17 ns/op |
| `map[uint64]uint16` (preallocated) | 24.2 | does not fit 32 GB | ~76 ns | not thread-safe |

`WithMaxLoad(f)` (0.5–0.95, default 0.9) trades memory for single-thread latency: at 1e8 keys, 0.8 measures ~88 ns (13.3 B/key) and 0.7 ~64 ns (15.2 B/key).

## Development

```sh
go vet ./... && go test -race -count=1 -cover ./...

# benchmarks (always -benchtime 1x for Memory/InsertParallel)
MAPSETINC_BENCH_N=100000000 go test -run '^$' -bench 'Memory' -benchtime 1x
MAPSETINC_BENCH_N=100000000 go test -run '^$' -bench 'Add' -benchtime 30000000x -benchmem
```

## License

Apache-2.0
