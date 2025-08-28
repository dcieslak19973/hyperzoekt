Zoekt porting guide (Rust parity tracker)

Source project: https://github.com/sourcegraph/zoekt

Recommended checkout (no submodule):

1) Clone Go Zoekt next to this repo (sibling directory):

   git clone https://github.com/sourcegraph/zoekt /workspaces/zoekt
   cd /workspaces/zoekt
   # Optional: pin to a known commit
   # git checkout <sha>

2) (Optional) Run Go tests to get a baseline:

   go test ./...

3) In this Rust crate, set an env var so tests/examples can reference Go testdata when needed:

   export GO_ZOEKT_ROOT=/workspaces/zoekt

Scope (minus web UI):
- Query AST and parse (and/or a builder API)
- Trigram extraction and regex analysis
- Index writer/reader and shard format (persistent), postings, ranking
- Indexer CLI and search CLI (parity with Go tools where practical)

Status (high level):
- [x] Minimal in-memory index/search scaffolding (IndexBuilder, Searcher)
- [x] Query AST and simple evaluator (Literal, Regex, And/Or/Not)
- [x] Trigram module (bootstrap)
- [x] Persistent shard writer/reader and mmap-backed searcher
- [x] CLI tools: zr-index, zr-search, zr-bench, zr-query-bench, zr-compare-bench
- [x] Benchmark JSON schema and comparator with indexing summary table output
 - [~] Regex analysis improvements (basic prefilter exists; needs hardening to match Go)

    Notes: a conservative trigram-extraction heuristic exists in `crates/zoekt/src/regex_analyze.rs`. It helps common cases but is not yet a full port of Go Zoekt's regex lowering. Recommended short-term actions:

    1. Harden the extractor using a regex AST (for example `regex-syntax`) to safely extract literal substrings and compute conjunctive trigram sets.
    2. Add Go-derived regex edge-case tests into `crates/zoekt/tests/` for parity verification.
    3. Ensure the prefilter falls back to a safe default (no prefilter) when uncertain to avoid false negatives.

- [~] Full postings format and ranking, line/match context extraction (core pieces present; more work required)
- [~] Repo/file scoping, symbol awareness (symbol extraction + per-doc symbols persisted; symbol-trigram postings and shard-side prefilter implemented but needs tuning)
- [~] Broader test parity with Go Zoekt (good unit coverage; need Go-derived fixtures and golden comparisons)

Mapping (Go -> Rust modules):
- zoekt/query -> crates/zoekt/src/query.rs (AST and execution, slice-based set ops)
- zoekt/regex -> crates/zoekt/src/regex_analyze.rs (basic; improve prefiltering)
- zoekt/trigram -> crates/zoekt/src/trigram.rs
- zoekt/index (in-memory) -> crates/zoekt/src/index.rs (IndexBuilder, InMemoryIndex)
- zoekt/shard formats -> crates/zoekt/src/shard.rs (writer/reader, mmap search)
- cmd/zoekt-index, cmd/zoekt -> crates/zoekt-cli (zr-index, zr-search, zr-bench, zr-query-bench, zr-compare-bench)

Notes:
- Keep APIs idiomatic Rust (Result, anyhow/thiserror), and prefer zero-copy/mmap for shard IO.
- Mirror critical Go tests as Rust tests; where exact test parity is tricky, add golden fixtures derived from Go's.

Bench and comparison workflow:
- zr-query-bench produces per-query timing stats and bench-level indexing/shard metrics.
- zr-compare-bench compares two bench JSONs, prints per-query diffs and Top regressions, and now emits an indexing summary table (index_size_bytes, index_elapsed_ms, shard_write_ms, shard_size_bytes) with colored deltas. It can also write a JSON report including indexing_summary.

Developer workflow reminders:
- Run cargo fmt and cargo clippy --workspace -- -D warnings before committing.
- In this repo’s dev container, builds may target /tmp/hyperzoekt-target; use cargo run to avoid stale target/release binaries.

Next steps:
- Flesh out regex prefiltering and trigram-based candidate selection to better match Go Zoekt.
- Implement richer postings structure, scoring/ranking, and line/match extraction.
- Add repo/file scoping, symbol search, and more comprehensive test parity.
- Optimize memory layout and shard encoding; add microbenchmarks and CI checks.

CLI naming:
- The command-line tools in this crate are prefixed with `zr-` (for example `zr-index`, `zr-search`).
   `zr` is short for `zoekt-rs` (the Rust port of Sourcegraph's Zoekt) and is used to avoid clashing
   with other system binaries while keeping the name compact.

## Gaps vs upstream Zoekt (excluding web UI)

- Query language/filters: `repo:`, `file:`, `lang:`, `branch:`, case sensitivity, path-only/content-only, `select=repo|file|symbol`.
- Match fidelity: line/snippet extraction, byte/rune offsets, per-match ranges, surrounding context, deduplication.
- Regex prefiltering: robust trigram-based analysis to prune candidates before full regex; special-casing small/empty trigram sets.
- Ranking/ordering: doc and match scoring heuristics, tie-breakers, path/name boosts, language-aware tweaks.
- Shard format depth: compressed postings, dictionaries, metadata (branches, languages), index versioning, forward/back compat.
- Shard lifecycle: compaction/merge tooling, size/doc-count splits, index upgrades, integrity checks.
- Incremental and multi-branch indexing: watch-based updates, reindex strategies, branch metadata and selection at query time.
- Repo/file filtering in indexer: ignore rules, binary detection, size/type limits, language detection.
- Symbol search: symbol extraction pipeline and query surface (tags-first acceptable as a start).
- Result streaming/limits: early-exit, max results per shard and globally, deterministic limiting.
- Observability: metrics, tracing spans, debug flags/endpoints, index stats dumps.
- CLI parity: indexer flags, merge/compactor, shard inspector, compatibility aides.

Status annotations (current implementation highlights)

- Query language/filters: [Done/Partial]
   - repo/file/lang parsing and filtering are implemented in `QueryPlan::parse` and `Searcher::search_plan` (supports exact, glob, regex and case handling).
   - `branch:` parsing and early rejection logic exist, but the in-memory `IndexBuilder` currently sets per-document `branches = ["HEAD"]` only. Multi-branch indexing input is not yet plumbed.
   - `path-only` is implemented; `content-only` flag exists and is enforced in `Searcher::search_plan`. Unit tests validate path-vs-content behavior.
 - Query language/filters: [Partial]
      - repo/file/lang parsing and filtering: implemented and exercised by unit tests in `QueryPlan::parse` and `Searcher::search_plan` (supports exact, glob, regex and case handling). Status: mostly done.
      - `branch:` parsing & early rejection: parsing and per-query branch checks exist, but indexing currently only records `branches = ["HEAD"]` for in-memory runs. Status: partial — multi-branch indexing input and end-to-end branch tests are outstanding.
      - `path-only` / `content-only`: `path-only` behavior is implemented; a `content_only` flag exists but is not consistently enforced across search plan execution. Action: add an explicit `content_only` gate in `Searcher::search_plan` and unit/integration tests that assert path-vs-content behavior.

   Concrete next actions (short-term, low-risk):
      1. Wire multi-branch input into `IndexBuilder` (support `--branch`/manifest) and add an end-to-end test exercising `branch:` selection at query time.
      2. Implement `content_only` semantics explicitly in `Searcher::search_plan`, add unit tests for `path-only` vs `content-only`, and add a couple of integration tests asserting expected matches.
      3. Add a small CI test that indexes a multi-branch fixture (2 branches) and verifies branch-scoped query results to prevent regressions.

   With these items done the query/filters surface will be functionally complete for typical search workflows; remaining work is tuning (branch-aware ranking, multi-branch sharding) and broader parity tests with Go.

-- select=repo|file|symbol: [Partial]
   - `select=repo` and `select=file` are implemented.
   - `select=symbol`: progress made — per-doc symbols are persisted in shards and the indexer builds in-memory symbol trigrams/terms. The shard writer can optionally append an on-disk symbol-trigram postings map (writer exposes an option to omit postings for baseline shards), and the reader exposes `symbol_postings_map()` which the shard searcher uses to prefilter candidate docs. CLI and bench tooling are wired to exercise `select=symbol`. Remaining work: tune symbol-postings performance (prefilter vs full scan), add golden tests for symbol accuracy, and ensure case-sensitivity/regex filtering is applied consistently.

- Symbol extraction: [Partial]
   - A simple regex-based extractor exists for Rust/Go/Python (`extract_symbols` in `index.rs`). The repo contains typesitter/tree-sitter ASTs and a recommended plan for a typesitter-backed extractor (higher fidelity). Action: add an opt-in typesitter extractor module and tests for Go/Rust/Python.

- Regex prefiltering: [Partial]
   - Basic trigram prefilter support exists in `ShardSearcher` and the `regex_analyze` helper, but parity with Go's robust prefiltering (edge cases and small trigram sets) needs more tests and hardening. Action: add Go-derived regex testcases and tighten `prefilter_from_regex` handling.

- Match fidelity and context: [Partial]
   - `ShardSearcher` supports line-index-based context extraction and `SearchMatch` ranges; in-memory `Searcher` fallbacks read files directly. Action: add tests for byte/rune offsets, multi-byte characters, and deduplication semantics.

- Shard format / versioning: [Implemented]
   - `ShardWriter`/`ShardReader` implement a binary format (current `VERSION = 5`) with metadata (repo name/root/hash/branches), per-doc symbol lists, and line index. Recent work added optional symbol-trigram postings (appended after the content trigram map) and reader-side parsing via `symbol_postings_map()` to enable shard-side symbol prefiltering. Reader returns contextual errors on corruption. Action: add `zr-inspect` and clearer version-mismatch error messages; consider a migration path for older formats.

- Shard-writer performance: [Todo / Recommendations present]
   - Current writer is correct but not optimized (reads files per-doc, writes with backpatching). `PORTING.md` contains prioritized optimizations (single-read-per-file, buffered writes, remove per-doc backpatching, parallelize). Action: implement low-risk writer optimizations behind a flag and add microbenchmarks.

- Incremental/multi-branch indexing & lifecycle: [Todo]
   - Watch-based updates, merges/compaction, and multi-branch/document branch metadata are not implemented. Action: add `--branch`/manifest support to `zr-index`, populate per-doc `branches`, and add simple merge/compact tooling.

- Tests & parity: [Partial]
   - There are shard roundtrip and corruption tests; broader parity tests versus Go Zoekt (queries, tricky regexes, symbol accuracy, branch selection) are missing. Action: add Go-derived golden fixtures and per-query expected outputs in `crates/zoekt/tests/fixtures` and a small CI job.

Actionable next steps (small PRs, rank-ordered)

1) Symbol-filtering small PR (low risk)
    - Filter `DocumentMeta.symbols` by pattern (literal/regex) when `select=symbol` is requested, honor `case_sensitive`, and add unit tests.

2) Content-only semantics (low risk)
    - Make `content_only` an explicit gate that disables path-matching and add tests that assert path vs content behavior.

3) Shard-writer quick perf pass (low risk)
    - Read each file once, buffer section writes via `BufWriter` (or in-memory section buffers), and remove per-doc backpatching; add microbench timings.

4) Typesitter-backed symbol extractor (stageable)
    - Add an opt-in extractor module, wire into `IndexBuilder` via a feature flag or runtime selection, and add per-language fixtures/tests.

5) Regex prefilter hardening & parity tests
    - Add Go-derived regex edge-case tests, iterate `regex_analyze::prefilter_from_regex`, and ensure `ShardSearcher`/`InMemoryIndex` use prefilter results before full scans.

6) Multi-branch and lifecycle plumbing
    - Add `--branch/manifest` support to `zr-index`, populate per-doc `branches`, and add query-time branch tests.

7) Observability & CLI parity
    - Add `zr-inspect` to dump shard headers, include basic timing metrics in `zr-search`, and add a `zr-merge`/compact proof-of-concept.

Notes
- The current codebase already implements a useful core: query parsing, trigrams, shard read/write, and simple symbol extraction. The prioritized small PRs above are designed to be low-risk and test-covered to incrementally reach parity with upstream Zoekt (excluding WebUI).


## Prioritized roadmap

1) Query correctness and filters (High value, low risk)
   - Add `repo:`, `file:`, `lang:`, `case:`; path-only vs content-only.

2) Match extraction and context (High value)
   - Return per-match lines with byte/rune ranges and N lines of context.

3) Regex prefiltering (Perf-critical)
   - Implement trigram prefilter closer to Go; handle tricky regexes safely.

4) Shard format completeness
   - Postings encoding, term/filename dictionaries, branch/lang metadata, index versioning.

5) Ranking heuristics
   - Frequency/position signals, filename/path boosts, language-aware tweaks.

6) Shard lifecycle ops
   - Merge/compact, size-based splitting, integrity checks, simple upgrade tool.

7) Incremental/multi-branch indexing
   - Watch for changes; index branch metadata; select branch at query time.

8) Symbol search (stageable)
   - Start with tags-only; consider richer symbol pipeline later.
   - Recommendation: reuse the repo's existing typesitter (tree-sitter) ASTs for high-quality symbol extraction where supported. See "Symbol extraction via typesitter" below for details and next steps.

9) Observability and CLI polish
   - Metrics/tracing; CLI flags parity; shard inspector.

## Execution notes

- Add query surface + filter plumbing first (parsing + evaluation filter checks).
- Land a minimal match collector (line boundaries, UTF-8 offsets) with unit tests.
- Iterate regex prefilter with microbenches; measure with `zr-query-bench` and compare via `zr-compare-bench`.
- Extend shard schema incrementally; version gates in reader; add a small `zr-merge`/`zr-inspect`.
- Gate new features behind flags; add golden tests mirroring Go where practical.
- Keep `cargo fmt` and `cargo clippy --workspace -- -D warnings` green in CI.

### Symbol extraction via typesitter (recommended)

- Viability: high — the workspace already includes language-specific `typesitter`/tree-sitter ASTs; reusing them for symbol extraction is fast and practical compared to regex heuristics.

- Benefits:
   - Precise symbol names and kinds (functions, methods, types, classes, etc.).
   - Accurate byte/line offsets for jump-to-symbol UX.
   - Fewer false positives/negatives than regex heuristics; supports nested and language-specific constructs.

- Tradeoffs & costs:
   - Additional build/runtime dependencies for tree-sitter grammars or typesitter bindings.
   - Higher CPU work during indexing (acceptable because indexing is offline and parallelizable).
   - Need to ensure grammar coverage for target languages; keep a lightweight fallback for unsupported languages.

- High-level integration plan:
   1. Hook parser invocation into the indexer where file content is read (in `IndexBuilder`).
   2. For each file, pick the appropriate typesitter parser based on extension/language and run a small AST query or walk to extract top-level symbols: name, kind, byte offset/line, and optional parent info.
   3. Add extracted symbols to per-document metadata and to the symbol-postings/trigram index so symbol queries can be matched efficiently at search time.
   4. Cache parser instances and parse in a thread pool to limit memory/CPU usage.
   5. Keep current regex-based extractor as a fallback for languages without grammars or for very small files.

- Immediate next tasks (short-term concrete steps):
   - Add a small typesitter-based extractor module and wire it into `crates/zoekt/src/index.rs` as a selectable extractor.
   - Extend `DocumentMeta`/shard metadata to store symbol entries (name, kind, offset) and add serialization tests for shards.
   - Add unit tests per language (start with Go, Rust, Python) that validate extracted symbol names and offsets against small fixtures.
   - Run `cargo bench`/`zr-query-bench` on a small corpus to measure indexing cost and iterate on parallelism and parser instance reuse.

Implementing typesitter-based extraction first for high-value languages (Go, Rust, Python, JavaScript/TypeScript) and falling back to regex elsewhere will give the best balance of accuracy and engineering cost.

### Shard writer performance (practical optimizations)

The current `ShardWriter::write_from_index` is straightforward and correct, but it can be significantly faster with a few pragmatic changes. Prioritized list (highest impact first):

- Read each file only once and reuse the bytes for trigram extraction, line-start computation and hash accumulation. Avoid re-reading the same file multiple times.
- Batch writes: replace many small `write_all` calls with buffered or chunked writes (use `std::io::BufWriter` or assemble per-section `Vec<u8>` buffers and write them once). This reduces syscall overhead dramatically.
- Eliminate per-doc seek/backpatch cycles. Either compute section sizes up-front (walk in-memory structures to get lengths) or build sections into in-memory buffers and write the line-index block in a single pass (record offsets in-memory while building data).
- Parallelize per-file CPU work (trigram extraction, line table building, hashing) with a thread pool (rayon) and then merge results into global postings. This uses multicore hardware to reduce wall-clock time.
- Use more efficient in-memory postings builders: accumulate per-file postings and append to per-trigram vectors, use smallvec-like storage for short lists, and avoid heavy nested maps during the hot loop.
- Serialize postings compactly (delta + varint) and optionally compress large sections (zstd/snappy) if IO-bound. Consider deterministic ordering to simplify merging.
- For very large shards, consider pre-sizing the output file and writing via `mmap` or writing large contiguous chunks to minimize kernel copy overhead.

Quick, low-risk first steps to implement (recommended order):

1. Stop rereading files: read file bytes once and reuse them for all per-file work.
2. Wrap the writer in `BufWriter` and batch per-section writes into moderate-sized buffers (a few MB) before flushing.
3. Replace seek/backpatch per-doc with either a precomputed offset pass or by writing the line index after the data sections in a single write.
4. Add simple per-file parallelism around trigram/line/hash extraction (configurable worker count).

Measure and validate: add microtimers around (a) file reading/parsing, (b) posting assembly, (c) serialization/writes. Use `zr-query-bench` or a small harness and flamegraphs to confirm where to invest further.

Notes and trade-offs:
- Buffering increases peak memory; use chunking for very large repos.
- Compression trades CPU for IO; useful when disks or network are the bottleneck.
- Keep the on-disk format versioned so you can adopt compact encodings later behind a version flag.

Implement these changes incrementally (feature-flagged or behind a config) and add benchmarks to avoid regressions.
