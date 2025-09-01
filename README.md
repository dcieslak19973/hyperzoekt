# hyperzoekt

[![CI](https://github.com/dcieslak19973/hyperzoekt/actions/workflows/ci.yml/badge.svg)](https://github.com/dcieslak19973/hyperzoekt/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A Rust project exploring fast code search and semantic indexing by combining ideas from Sourcegraph's Zoekt (https://github.com/sourcegraph/zoekt) with a hypergraph-based code model.

Goals
-----
- Build a high-performance, trigram-backed code search engine inspired by Zoekt, implemented entirely in Rust.
- Extract and reuse Tree-sitter tooling to produce rich syntactic and semantic information for source code.
- Store Tree-sitter results and repository metadata in a flexible hypergraph to enable advanced queries, permission-aware access, and cross-repo analysis.
- Provide usable interfaces: a command-line client and a simple web UI for searching and browsing index results.
- Support indexing multiple repository hosts (GitHub, GitLab, Bitbucket) and syncing permission data for multi-user environments.

Roadmap (high level)
--------------------
1. Extract Tree-sitter integration and make it reusable across indexing workflows.
2. Design the hypergraph schema and prototype storing Tree-sitter/AST data.
3. Port core Zoekt indexing/search pieces and blend them with the hypergraph model.
4. Add multi-repo ingestion and permission syncing.

Get involved
------------
- Read `CONTRIBUTING.md` for how to run, test and submit changes.
- Open issues or enhancement requests using the provided templates.

Quick start
-----------
Build locally with Rust and Cargo (install via https://rustup.rs/):

	cargo build

Running the indexer
-------------------
The `dzr-indexer` binary supports a one-shot indexing mode which will index each configured repo once and then skip further reindex attempts. Enable it with the `--index-once` flag or set the `ZOEKTD_INDEX_ONCE=true` environment variable.

Example (run the indexer and index repos once):

```bash
RUST_LOG=info cargo run -p zoekt-distributed --bin dzr-indexer -- --remote-url /path/to/repo --listen 127.0.0.1:3000 --index-once
```

You can also control reindexing via `--disable-reindex` (prevent periodic reindex entirely) and the `ZOEKTD_ENABLE_REINDEX` env var.

License
-------
This repository is licensed under the Apache License, Version 2.0. See the `LICENSE` and `NOTICE` files for details.

