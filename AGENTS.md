# go-mapset-inc

Concurrent exact counting set `Table[K Integer, C Unsigned]` for billion-scale integer keys (module `github.com/ttlbb/go-mapset-inc`, package `mapsetinc`).

## Verification

- `go vet ./... && go test -race -count=1 -cover ./...`
- The shell may export `GOOS=linux`; run Go commands with `GOOS=darwin` on this Mac, otherwise `-race` fails with "requires cgo" and test binaries cannot run.

## Benchmarks

- Scale with `MAPSETINC_BENCH_N` (default 2^20).
- Memory: `MAPSETINC_BENCH_N=100000000 go test -run '^$' -bench 'Memory' -benchtime 1x`. Always pass `-benchtime 1x` for `BenchmarkMemory` and `BenchmarkInsertParallel`; each iteration builds a whole table.
- Throughput: `MAPSETINC_BENCH_N=100000000 go test -run '^$' -bench 'AddExisting|AddParallel' -benchtime 30000000x`.
