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
- [x] Persistent shard prototype (writer/reader) and mmap-backed searcher
- [x] CLI tools: zr-index, zr-search, zr-bench, zr-query-bench, zr-compare-bench
- [x] Benchmark JSON schema and comparator with indexing summary table output
- [ ] Regex analysis improvements (parity with Go’s regex prefiltering)
- [ ] Full postings format and ranking, line/match context extraction
- [ ] Repo/file scoping, symbol awareness
- [ ] Broader test parity with Go Zoekt

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
