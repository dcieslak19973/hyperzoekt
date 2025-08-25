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
- See `TODO.md` for the current task list and `doc/PLANS.md` for architecture notes.
- Read `CONTRIBUTING.md` for how to run, test and submit changes.
- Open issues or enhancement requests using the provided templates.

Access control
--------------
Access control and enterprise-grade permission features will be provided as a closed-source
option available under a subscription model. The open-source core focuses on indexing,
search, and the data model; commercial extensions will integrate permission syncing,
authentication providers, and fine-grained enforcement at query time.

Quick start
-----------
Build locally with Rust and Cargo (install via https://rustup.rs/):

	cargo build

Repo indexer
-------------
This workspace includes a small Tree-sitter backed repository indexer in `crates/hyperzoekt`.

To build and run the indexer binary (may take time the first build because Tree-sitter grammars compile native code):

```bash
cd crates/hyperzoekt
cargo build --release
cargo run --release --bin hyperzoekt -- --root /path/to/repo --output out.jsonl
```

There is also an incremental/streaming mode that writes JSONL as files are processed; see `crates/hyperzoekt/src/bin/hyperzoekt.rs` for options.

License
-------
This repository is licensed under the Apache License, Version 2.0. See the `LICENSE` and `NOTICE` files for details.

