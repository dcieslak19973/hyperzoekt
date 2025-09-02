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

Configuration
-------------
The distributed system supports various environment variables for configuration:

### Redis Configuration
- `REDIS_URL`: Redis connection URL (e.g., `redis://127.0.0.1:6379`)
- `REDIS_USERNAME`: Optional Redis username for authentication
- `REDIS_PASSWORD`: Optional Redis password for authentication

If `REDIS_URL` doesn't contain authentication credentials, the system will automatically add them using `REDIS_USERNAME` and/or `REDIS_PASSWORD` if provided. For example:
- `REDIS_URL=redis://127.0.0.1:6379` with `REDIS_PASSWORD=mypass` becomes `redis://:mypass@127.0.0.1:6379`
- `REDIS_URL=redis://127.0.0.1:6379` with `REDIS_USERNAME=user` and `REDIS_PASSWORD=pass` becomes `redis://user:pass@127.0.0.1:6379`

### Node Configuration
- `ZOEKTD_NODE_ID`: Unique identifier for this node
- `ZOEKTD_LEASE_TTL_SECONDS`: Lease time-to-live in seconds (default: 150)
- `ZOEKTD_POLL_INTERVAL_SECONDS`: Polling interval in seconds (default: 5)
- `ZOEKTD_NODE_TYPE`: Node type - `indexer`, `admin`, or `search` (default: indexer)
- `ZOEKTD_ENDPOINT`: Manual endpoint override
- `ZOEKTD_ENABLE_REINDEX`: Enable/disable reindexing (default: true)
- `ZOEKTD_INDEX_ONCE`: Index each repo once and skip further re-index attempts (default: false)

### Git Repository Authentication
For indexing private repositories from GitHub, GitLab, or Bitbucket, set the following environment variables:

- `GITHUB_USERNAME` and `GITHUB_TOKEN`: For GitHub repositories
- `GITLAB_USERNAME` and `GITLAB_TOKEN`: For GitLab repositories  
- `BITBUCKET_USERNAME` and `BITBUCKET_TOKEN`: For Bitbucket repositories

When both username and token are provided for a platform, the indexer will automatically inject credentials into HTTPS URLs for authentication. For example:
- Original: `https://github.com/user/repo.git`
- With auth: `https://username:token@github.com/user/repo.git`

SSH URLs (starting with `git@` or `ssh://`) are not modified and rely on SSH key authentication.

License
-------
This repository is licensed under the Apache License, Version 2.0. See the `LICENSE` and `NOTICE` files for details.

