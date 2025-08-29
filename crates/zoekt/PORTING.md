Zoekt porting guide (Rust parity tracker)

A cloned version of the upstream Go reference implementation is available at

   `/workspaces/hyperzoekt/.upstream/zoekt`

Our Rust implementation is in:

   `/workspaces/hyperzoekt/crates/zoekt`
   `/workspaces/hyperzoekt/crates/zoekt-cli`

Scope (minus web UI):
- Query AST and parse (and/or a builder API)
- Trigram extraction and regex analysis
- Index writer/reader and shard format (persistent), postings, ranking
- Indexer CLI and search CLI (parity with Go tools where practical)
- We don't intend to be API or File compatible with the Go implementation and prioritize optimization over compatibility

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

- [x] Regex-confirmation & anchor/windowing optimizations (shard search)
   - Implemented trigram-position-based windowing to restrict expensive regex confirmation to small byte ranges derived from per-trigram positions. See `crates/zoekt/src/shard.rs` (anchor/window builders and targeted per-doc trigram position decoder).
   - Added a byte-anchor fallback (ASCII byte seeds + memchr/memmem scanning) when useful trigrams are unavailable. This lowers full-file regex scans for many anchored or literal-rich regexes. The `memchr` dependency was added in `crates/zoekt/Cargo.toml`.
   - Integrated a conservative `required_substrings_from_regex` pass (regex AST lowering) to enforce multi-byte substring co-occurrence before windowing. Implementation is in `crates/zoekt/src/regex_analyze.rs`.
   - Tests and benchmark runs were used to validate behavior; results show significant improvement for many anchored regexes, though some small-query regressions remain driven by metadata I/O (see notes below).

- [~] Full postings format and ranking, line/match context extraction
   - Basic ranking and snippet extraction are implemented in the shard-backed search path (see “Scoring & snippets” below). Advanced ranking remains to be ported.
 - [~] Full postings format and ranking, line/match context extraction
    - Basic ranking and snippet extraction are implemented in the shard-backed search path (see “Scoring & snippets” below). Advanced ranking remains to be ported.
- [~] Repo/file scoping, symbol awareness (symbol extraction + per-doc symbols persisted; symbol-trigram postings and shard-side prefilter implemented but needs tuning)
- [~] Broader test parity with Go Zoekt (good unit coverage; need Go-derived fixtures and golden comparisons)

Recent notable changes (delta since earlier drafts):
- [x] Initial relevance ranking and snippets (shard search)
   - `SearchMatch` now includes a `score: f32` used to rank results across files; `SearchOpts` supports an optional `snippet_max_chars` to trim before/after context with ellipses.
   - Literal and regex shard searches confirm positions, compute a simple per-document score, generate before/line/after snippets, optionally trim, sort by score desc, and apply `limit`.
   - `zr-search --snippet-max-chars N` enables trimming for human-friendly output; JSON output now includes `score` and `line_text` (fixed key name).

- [x] Multi-branch indexing (in-memory prototype): `IndexBuilder::branches(...)` and supporting code were added to allow indexing multiple branches by extracting each branch's tree and creating per-branch documents with in-memory contents. See `crates/zoekt/src/index.rs` and `crates/zoekt/tests/branch_selection.rs` for an end-to-end test.
- [x] `content_only` semantics: `content_only` is now enforced at planning/execution time in the searcher, with unit/integration tests to validate path-vs-content behavior.
- [x] In-memory per-document contents: the in-memory index now stores per-doc contents (for branch-extracted docs) and the search/tokenization path prefers these in-memory contents so branch-scoped searches see the correct file contents instead of the workspace checkout.
- [x] Robust default-branch detection in tests: branch-selection tests detect the repo's initial branch via a chain (rev-parse → symbolic-ref → config → fallback) so tests run reproducibly across environments where the default branch is `main` or `master`.
- [x] Debug prints removed: temporary debug logging used during development was cleaned up.

- [x] Library-based branch extraction attempt: `extract_branch_tree_libgit2` was added to try libgit2 extraction first and fall back to `git archive | tar`. The `branch_selection` test was validated in the devcontainer after this change.
- [~] Upstream parity audit started: the upstream Go `zoekt` repository was inspected under `./.upstream/zoekt` and a per-module parity report has been drafted (recommendations and prioritized test gaps were collected). Next step: add Go-derived golden tests and carry out focused parity checks for regex prefiltering, symbol extraction, and shard semantics.

Notes about implementation details and environment dependency:

- The multi-branch indexing path now attempts a library-based extraction first (libgit2 via the `git2` crate) and falls back to the previous `git archive | tar -x` pipeline on error. This removes the hard dependency on shelling out in environments where `libgit2` is available, while preserving the previous behavior as a safe fallback.
- Important: `libgit2` requires native libgit2 headers/libs available in the build environment (CI images may need libgit2-dev or equivalent). The fallback ensures CI or developer environments without libgit2 still work.
- Because the extraction produces per-branch in-memory document contents, the searcher prefers these in-memory contents. This ensures correct trigram/symbol extraction and query behavior for branch-indexed docs without changing on-disk shard formats.
- Tests for the zoekt crate (including the branch-selection test) ran in the devcontainer after the libgit2 change; the branch-selection test passed. Add a CI job to exercise both the libgit2 path and the fallback path to avoid regressions on different runners.

Notes:
- Keep APIs idiomatic Rust (Result, anyhow/thiserror), and prefer zero-copy/mmap for shard IO.
- Mirror critical Go tests as Rust tests; where exact test parity is tricky, add golden fixtures derived from Go's.

Bench and comparison workflow:
- zr-query-bench produces per-query timing stats and bench-level indexing/shard metrics.
- zr-compare-bench compares two bench JSONs, prints per-query diffs and Top regressions, and now emits an indexing summary table (index_size_bytes, index_elapsed_ms, shard_write_ms, shard_size_bytes) with colored deltas. It can also write a JSON report including indexing_summary.
- Generally, run the benchmarks against /workspaces/linux

Next steps:
- Flesh out regex prefiltering and trigram-based candidate selection to better match Go Zoekt.
- Implement richer postings structure, scoring/ranking, and line/match extraction.
- Add repo/file scoping and symbol search.
- Optimize memory layout and shard encoding; add microbenchmarks.

CLI naming:
- The command-line tools in this crate are prefixed with `zr-` (for example `zr-index`, `zr-search`).
   `zr` is short for `zoekt-rs` (the Rust port of Sourcegraph's Zoekt) and is used to avoid clashing
   with other system binaries while keeping the name compact.



## Upstream snapshot (notable files)

The repository snapshot used as a reference is available at `/workspaces/hyperzoekt/.upstream/zoekt`.
Notable top-level items (non-exhaustive):

- `cmd/` — upstream command-line tools (index, webserver, indexserver, etc.)
- `api.go`, `api_proto.go`, `api_test.go`, `api_proto_test.go` — public API surface and proto bindings
- `index/`, `search/` — core indexing and search logic in Go
- `grpc/` — gRPC transport glue
- `web/` — web UI and static assets (not part of this Rust port)
- `languages/`, `internal/` — language helpers and internal utilities
- `Dockerfile*`, `shell.nix`, `flake.nix` — build/CI/runtime helpers
- `marshal.go`, `marshal_test.go` — on-disk or wire-format helpers and tests
- `testdata/` — fixtures and regression inputs

Use this snapshot to derive golden inputs and tricky regex/query cases for parity tests.


## Gaps vs upstream (concrete)

This is a concise, actionable mapping of important upstream components to their current Rust status.
Status legend: [Done], [Partial], [Missing]

- Web UI (`web/`): [Missing]
   - The upstream web UI and static server are not ported. This repo focuses on library and CLI parity.

- Public API / gRPC (`api.go`, `grpc/`, `api_proto*.go`): [Missing / Partial]
   - Upstream exposes a stabilized API and gRPC endpoints; the Rust port currently has no equivalent gRPC server. Consider adding a thin gRPC server that wraps the searcher if API parity is required.

- Command-line tools (`cmd/`): [Partial]
   - Core CLIs used for indexing and search are present as `zr-*` tools. Some auxiliary upstream commands (indexserver, webserver variants, helper scripts) are not ported.

- Core search/index packages (`index/`, `search/`): [Partial]
   - Fundamental pieces — query AST, trigram extraction, shard writer/reader, and an mmap-backed searcher — exist in `crates/zoekt/src`. However, several low-level postings, ranking heuristics, and exact shard-layout encodings from upstream are incomplete.
   - Ranking: We currently use a simple heuristic (TF + early-position + filename boost). Upstream uses a richer ranking that considers word boundaries, symbol/name matches, proximity, length normalization, and other signals.

- Regex prefiltering and trigram lowering: [Partial]
   - A conservative extractor exists (`regex_analyze.rs`) and is useful for many cases, but it does not yet match Go Zoekt's coverage on complex regexes and safe extraction of conjunctive trigram sets.

- Symbol extraction and symbol-postings prefilter: [Partial]
   - Basic symbol extraction and optional symbol-postings storage exist. Upstream's mature symbol pipeline (and associated performance characteristics) needs further parity testing and tuning.

- Serialization / on-disk wire formats (`marshal.go` equivalents): [Partial]
   - A shard binary format (VERSIONed) is implemented in Rust. Some compact encodings and deterministic layout choices from Go are not yet implemented; add compatibility tests if cross-read/write with Go shards is required.

- Tests, fixtures and golden inputs (`testdata/`, API tests): [Missing / Partial]
   - The upstream `testdata/` and API tests are a rich source of edge cases. The Rust crate has unit/integration tests but needs Go-derived golden fixtures and a small parity test-suite.

- CI / packaging helpers (Dockerfiles, flakes, build scripts): [Missing]
   - Upstream provides Dockerfiles and Nix flakes used by their CI. The Rust port relies on the repo's devcontainer and CI; consider adding minimal Docker/Nix parity only if required for reproducible builds.

Concrete next actions to close gaps (minimal, test-first):

1. Add a small `tests/parity/` suite that imports a subset of upstream `testdata/` fixtures (regexes and queries) and asserts matching results between Go-derived golden outputs and Rust searcher outputs.
2. Harden `regex_analyze::prefilter_from_regex` using `regex-syntax` AST and add the upstream regex edge-cases as tests.
3. Implement and test the missing compact postings encodings (delta+varint) behind a shard-version flag; add reader tests that assert round-trip parity for small fixtures.

---

## Scoring & snippets

What exists now (Rust, shard-backed literal/regex search):

- Per-document score is computed once per file that has matches and then attached to each `SearchMatch` from that file:
   - Term frequency (TF) boost: `tf_boost = log2(1 + occurrences_in_doc)`.
   - Early-position boost: `pos_boost = 1 - min(first_match_byte / 8192, 1)`; earlier first matches score higher.
   - Filename boost:
      - Literal: `filename.to_lowercase().contains(needle.to_lowercase())` → +0.5 if true.
      - Regex: pattern matches file name → +0.5 if true.
   - Final score: `score = tf_boost + 0.5 * pos_boost + 0.5 * name_boost`.
- Sorting: results are sorted by `score` descending, then by `(path, line, start)` as tie-breakers.
- Snippets: for each match we emit `before`, `line_text`, and `after` using the line index and the requested `context` (in lines). If `SearchOpts.snippet_max_chars` is set, `before` and `after` are trimmed to ≤N Unicode chars with an ellipsis. `line_text` is not trimmed.
- CLI: `zr-search` accepts `--snippet-max-chars` and includes `score` in JSON output.

How this compares to upstream Zoekt (Go reference):

- Upstream employs a more sophisticated ranking model. Public descriptions and code indicate it incorporates additional signals such as:
   - Word-boundary matches vs. mid-token matches.
   - Symbol/name matches and boosts for matches in identifiers.
   - Proximity of multiple terms, and span/fragment cohesiveness.
   - Document length/normalization and possibly inverse document frequency–like effects.
   - Additional file- and repo-level heuristics (e.g., filename weighting, language/suffix hints).
- The Rust port’s current heuristic is intentionally simple and fast; it captures useful first-order signals (TF, first occurrence position, filename) but lacks the richer semantics above. This means ordering can differ notably from upstream, especially for multi-term queries, identifiers, and large files.

Planned follow-ups (parity-oriented):

- Add word-boundary and token-aware boosts (use regex-syntax/Unicode segmentation).
- Introduce per-term IDF or length normalization akin to BM25-lite to reduce over-weighting frequent terms.
- Add proximity/fragment scoring for multi-term queries.
- Add symbol-aware boosts using the shard’s per-doc symbol table.
- Gate new behavior behind a `SearchOpts` flag to keep stability and enable A/B testing; add golden tests using upstream fixtures to compare ordering.

