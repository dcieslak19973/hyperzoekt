ZOEKT Benchmarking
===================

This document explains the benchmarking tools included in the `zoekt` crates and the JSON output format produced by the bench tools (`zr-query-bench`, `zr-bench`) and how `zr-compare-bench` consumes that output.

## Goals

- Explain what each field in the bench JSON means.
- Clarify what `rss_kb` represents and limitations of the current measurement.
- Show how to run the benches and produce repeatable comparisons.
- Give tips for interpreting results and improving measurement quality.

## Where the tools live

- `crates/zoekt-rs/src/bin/zr-query-bench.rs` — runs query micro-benchmarks against an index/shard and writes `.bench_results/query-bench-<ts>.json`.
- `crates/zoekt-rs/src/bin/zr-bench.rs` — runs the indexer and emits indexing metrics (writes `.bench_results/index-bench-<ts>.json`).
- `crates/zoekt-rs/src/bin/zr-compare-bench.rs` — compares two bench JSON outputs and prints a summary table and optional JSON report.

## Bench JSON schema (important fields)

Top-level:

- `timestamp_ms` — integer, epoch millis when the bench was written.
- `path` — the repo path that was indexed / searched.
- `results` — array of per-query result objects.

Per-query object (one element in `results`):

- `query` — string representation of the query (Literal / Regex / And / Or / Not).
- `iters` — number of measured iterations.
- `times_us` — array of measured iteration times in microseconds.
- `mean_ms` / `median_ms` / `p95_ms` — precomputed aggregate statistics (milliseconds).
- `rss_kb_before` / `rss_kb_after` — optional: memory reported (kilobytes) sampled before/after the measured loop for the query.

Index metadata (added by the updated benches):

- `index_path` — optional path to the shard file (when a shard is written or provided).
- `index_size_bytes` — optional size of the shard file in bytes.
- `index_elapsed_ms` — optional: time taken to build the in-memory index (ms).
- `scanned_bytes` — optional: total bytes scanned while building the index.

## What does `rss_kb` mean?

The benchmark code reports `rss_kb_before` and `rss_kb_after` by reading `/proc/self/status` and scanning for `VmHWM:` or `VmRSS:` lines. It returns the numeric value (kilobytes) from the first matching line it finds.

Important details and caveats:

- `VmRSS` is the Resident Set Size: the current resident memory (bytes in RAM) for the process. This reflects memory currently in use.
- `VmHWM` is the "High Water Mark" (peak RSS) for the process — an historical maximum resident size.
- The current code returns the first occurrence of either `VmHWM` or `VmRSS` as it reads `/proc/self/status`. That means the field may represent either peak or current resident set depending on the order the file is read and which label appears first. On many kernels `VmHWM` appears before `VmRSS` in the file, so the reported value is often the peak (VmHWM). This is not ideal if you care about instantaneous RSS.
- This is Linux-specific (`/proc/self/status`). The tools are not cross-platform in this regard.

Recommendations / improvements:

- If you want the instantaneous resident memory, change the measurement to explicitly parse `VmRSS:` and use that value.
- If you want both peak and current, capture both `VmRSS` and `VmHWM` and include them separately in the JSON.
- For cross-platform work, use a platform-appropriate library or tool (e.g., `ps` on BSD/macOS, or platform APIs).

## How the comparator uses index metadata

- `zr-compare-bench` prefers an explicit `index_path` present in the bench JSON and will `stat()` that file to read the size. If `index_path` is not present, it falls back to a heuristic and checks `${path}/.data/index.shard` and `${path}/index.shard`.
- To make comparisons deterministic, ensure bench runs produce `index_path` and `index_size_bytes` (run `zr-query-bench` with shard-writing enabled — this is the default in the current codebase).

## Running a reliable benchmark (recommended steps)

1. Wipe old results to avoid accidental comparisons:

```bash
rm -rf .bench_results/*.json
```

2. Run the index/build bench (optional, `zr-bench` records indexing metadata):

```bash
cargo run -p zoekt-rs --bin zr-bench --release -- /path/to/repo --write-shard
```

3. Run two query benches (make code or config changes between them if you are measuring an opt change):

```bash
cargo run -p zoekt-rs --bin zr-query-bench --release -- /path/to/repo
# make changes, rebuild or switch branch
cargo run -p zoekt-rs --bin zr-query-bench --release -- /path/to/repo
```

Note: `zr-query-bench` builds the index every run and, by default, writes the shard to `<path>/.data/index.shard` and records `index_path` and `index_size_bytes` in the bench JSON.

4. Compare two bench JSONs:

```bash
cargo run -p zoekt-rs --bin zr-compare-bench --release -- .bench_results/query-bench-OLD.json .bench_results/query-bench-NEW.json --json-out .bench_results/compare.json
```

The comparator prints a per-query table and, by default, a top-N regression list where `pct_mean` (percent mean increase) exceeds the configured threshold (default 10%). Use `--threshold` and `--top` to tune.

## Interpreting results

- `mean_ms` is a good first-order metric for overall performance. Check `median_ms` and `p95_ms` to understand distribution and tail latency.
- `pct_mean` in the comparator shows relative change. Small percentage changes on very small means can be noise; look at absolute ms deltas too for context.
- Check `times_us` arrays if you suspect flakiness — the raw samples let you recompute percentiles or re-run alternative statistics.
- Use the index size change (`index_size_bytes`) to correlate regressions with index growth (e.g., an index format change may increase size and affect perf).
- `rss_kb` reported here is a simple indicator; for any serious memory regression analysis, capture both `VmRSS` and `VmHWM` and/or use a heap profiler.

## Quick FAQ

Q: Is `rss_kb` the process peak memory or current memory?

A: The current implementation returns whichever of `VmHWM` or `VmRSS` is encountered first when reading `/proc/self/status` — it may be peak (VmHWM) on many systems. For clarity, the bench tools now record `rss_kb_before` and `rss_kb_after` so you can see changes around the measured loop. For precise semantics, change the tool to record both `VmRSS` and `VmHWM` explicitly.

Q: How do I ensure `zr-compare-bench` uses correct index file sizes?

A: Make `zr-query-bench` run with shard-writing enabled (the current default). That writes `index_path` and `index_size_bytes` into the bench JSON; the comparator will prefer those fields.

Q: How do I compare only significant regressions?

A: Use `zr-compare-bench --threshold 20.0 --top 10` to list only regressions larger than 20% and show top 10.

## Next actions you may want

- I can update the benchmark code to record both `VmRSS` and `VmHWM` explicitly.
- I can add an option to `zr-query-bench` to record system load (`/proc/loadavg`) and CPU affinity during runs.
- I can add a small wrapper script to wipe old results, run two benches and compare automatically.

If you want any of those done now, tell me which one and I'll implement it.
