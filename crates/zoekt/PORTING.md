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
- [x] Minimal in-memory index/search scaffolding
- [x] Trigram module (bootstrap)
- [ ] Regex analysis
- [ ] Query AST (operators, atomic terms, repo/file scoping)
- [ ] Persistent shard format (writer/reader, mmap)
- [ ] Postings, ranking, matches/line context
- [ ] CLI indexer/search tools

Mapping (Go -> Rust modules):
- zoekt/query -> crates/zoekt/src/query.rs (AST and execution)
- zoekt/regex -> crates/zoekt/src/regex_analyze.rs (to add)
- zoekt/trigram -> crates/zoekt/src/trigram.rs
- zoekt/index (writer/reader) -> crates/zoekt/src/index/{writer,reader}.rs (to add)
- zoekt/shard formats -> crates/zoekt/src/index/shard.rs (to add)
- cmd/zoekt-index, cmd/zoekt -> crates/zoekt-cli (to add)

Notes:
- Keep APIs idiomatic Rust (Result, anyhow/thiserror), and prefer zero-copy/mmap for shard IO.
- Mirror critical Go tests as Rust tests; where exact test parity is tricky, add golden fixtures derived from Go's.

CLI naming:
- The command-line tools in this crate are prefixed with `zr-` (for example `zr-index`, `zr-search`).
   `zr` is short for `zoekt-rs` (the Rust port of Sourcegraph's Zoekt) and is used to avoid clashing
   with other system binaries while keeping the name compact.
