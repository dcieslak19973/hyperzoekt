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

### MCP Server Authentication
The MCP (Model Context Protocol) server supports Git authentication through HTTP headers. When using MCP clients to perform distributed searches, you can provide Git credentials as HTTP headers:

**Supported Headers:**
- `X-GitHub-Username` and `X-GitHub-Token`: GitHub credentials
- `X-GitLab-Username` and `X-GitLab-Token`: GitLab credentials  
- `X-BitBucket-Username` and `X-BitBucket-Token`: Bitbucket credentials

**Example HTTP Request with Headers:**
```http
POST /mcp HTTP/1.1
Content-Type: application/json
X-GitHub-Username: your-github-username
X-GitHub-Token: your-github-personal-access-token
X-GitLab-Username: your-gitlab-username
X-GitLab-Token: your-gitlab-access-token

{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "distributed_search",
    "arguments": {
      "regex": "function.*search"
    }
  }
}
```

**Using the Header Wrapper Script:**
For easier deployment, use the provided wrapper script that extracts headers and sets environment variables:

```bash
# Start MCP server with header extraction
./scripts/mcp-header-wrapper.sh cargo run --bin dzr-mcp-search -- --listen 127.0.0.1:8080
```

When deployed behind a reverse proxy (like nginx or Apache), the proxy can extract the `X-*` headers and set them as `HTTP_X_*` environment variables that the wrapper script will detect.

These headers allow the search system to access private repositories and avoid rate limits when searching across multiple Git hosting services. The credentials are extracted from the HTTP headers and used for authentication with the respective Git services.

Running the full system locally
-------------------

In generaly, you'll need a REDIs instance running for the Admin UI, the Indexer,
and the Search UI to work together.

1. Run the Indexer:
```bash
cd /workspaces/hyperzoekt && RUST_LOG=debug REDIS_URL=redis://127.0.0.1:7777 cargo run --bin dzr-indexer -- --listen 127.0.0.1:3001 --remote-url /workspaces/hyperzoekt --disable-reindex
```
OR
```bash
cd /workspaces/hyperzoekt && RUST_LOG=debug REDIS_URL=redis://127.0.0.1:7777 cargo run --bin dzr-indexer -- --listen 127.0.0.1:3002
```
2. Run the Admin UI:
```bash
cd /workspaces/hyperzoekt && RUST_LOG=debug REDIS_URL=redis://127.0.0.1:7777 ZOEKT_ADMIN_USERNAME=admin ZOEKT_ADMIN_PASSWORD=password cargo run --bin dzr-admin -- --bind 127.0.0.1:7878
```
3. Run the Search UI:
```bash
cd /workspaces/hyperzoekt && RUST_LOG=debug REDIS_URL=redis://127.0.0.1:7777 cargo run --bin dzr-http-search -- --listen 127.0.0.1:8080
```
3. Run the MCP UI:
```bash
cd /workspaces/hyperzoekt && RUST_LOG=debug REDIS_URL=redis://127.0.0.1:7777 cargo run --bin dzr-mcp-search -- --listen 127.0.0.1:8081
```

License
-------
This repository is licensed under the Apache License, Version 2.0. See the `LICENSE` and `NOTICE` files for details.

