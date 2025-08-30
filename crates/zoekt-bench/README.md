zoekt-bench
============

Small repository-local benchmarks and microbenchmarks for `zoekt-rs`.

How to run

- Run all criterion benches:

```
cargo bench -p zoekt-bench
```

- Run an individual bench:

```
cargo bench -p zoekt-bench --bench parse_bench
cargo bench -p zoekt-bench --bench shard_write_bench
```

Notes

- These benches are intended as developer tooling. They rely on local crate APIs and may be slow (especially `shard_write_bench`).
- Consider running them on a dedicated machine when profiling.
