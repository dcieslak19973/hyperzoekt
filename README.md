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
The project provides two sets of binaries:

**Legacy Zoekt-Distributed:**
The `dzr-indexer` binary supports a one-shot indexing mode which will index each configured repo once and then skip further reindex attempts. Enable it with the `--index-once` flag or set the `ZOEKTD_INDEX_ONCE=true` environment variable.

Example (run the indexer and index repos once):
```bash
RUST_LOG=info cargo run -p zoekt-distributed --bin dzr-indexer -- --remote-url /path/to/repo --listen 127.0.0.1:3000 --index-once
```

**New Hyperzoekt:**
The `hyperzoekt-indexer` binary supports enhanced indexing with Tree-sitter semantic analysis.

Example (run the indexer):
```bash
RUST_LOG=info REDIS_URL=redis://127.0.0.1:6379 SURREALDB_URL=ws://127.0.0.1:8000/rpc cargo run --bin hyperzoekt-indexer
```

Migration note: the legacy one-shot JSONL exporter has been removed. To perform a one-shot export, call the library API (`RepoIndexService`) from a small Rust program and write results to a file. For example:

```rust
let mut opts = hyperzoekt::repo_index::indexer::RepoIndexOptions::builder();
opts = opts.root("/path/to/repo").output_file("out.jsonl");
let (_svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build_with_options(opts.build())?;
```

Running the web UI
------------------
**Legacy Admin UI:**
The `dzr-admin` binary provides a web interface for managing repositories.

Example (run the admin UI):
```bash
RUST_LOG=debug REDIS_URL=redis://127.0.0.1:7777 ZOEKT_ADMIN_USERNAME=admin ZOEKT_ADMIN_PASSWORD=password cargo run --bin dzr-admin -- --bind 127.0.0.1:7878
```

**New Hyperzoekt Web UI:**
The `hyperzoekt-webui` binary provides a modern web interface for browsing indexed repositories and entities.

Example (run the web UI):
```bash
RUST_LOG=info REDIS_URL=redis://127.0.0.1:6379 SURREALDB_URL=ws://127.0.0.1:8000/rpc cargo run --bin hyperzoekt-webui -- --port 7879 --host 127.0.0.1
```

Configuration
-------------
The distributed system supports various environment variables for configuration:

For observability and performance tracing with OpenTelemetry, see the "OpenTelemetry / Tracing" section in `doc/USAGE.md`.

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

### SurrealDB Configuration
- `SURREALDB_URL`: SurrealDB WebSocket RPC URL (e.g., `ws://localhost:8000/rpc`)
- `SURREALDB_USERNAME`: SurrealDB username (default: root)
- `SURREALDB_PASSWORD`: SurrealDB password (default: root)
- `SURREAL_NS`: SurrealDB namespace (default: zoekt)
- `SURREAL_DB`: SurrealDB database name (default: repos)

If these environment variables are not set, the system will fall back to using an in-memory SurrealDB instance for development and testing.

Running with Docker Compose
---------------------------

The project includes a Docker Compose setup that runs all components with Redis and SurrealDB:

1. **Start all services:**
```bash
cd docker && docker-compose up --build
```

This will start:
- **Redis** on port 6379 (for distributed coordination)
- **SurrealDB** on port 8000 (for repository metadata and permissions)

**Zoekt-Distributed Services (Legacy):**
- **Indexer** on port 7676 (for indexing repositories)
- **Admin UI** on port 7878 (web interface for managing repositories)
- **Search API** on port 8080 (HTTP search endpoint)
- **MCP Server** on port 7979 (Model Context Protocol for AI assistants)

**Hyperzoekt Services (New):**
- **Indexer** on port 7677 (enhanced indexing with Tree-sitter)
- **Web UI** on port 7879 (modern web interface for browsing/searching)

2. **Access the services:**
- **Legacy Admin UI**: http://localhost:7878 (username: `admin`, password: `password`)
- **Legacy Search API**: http://localhost:8080
- **New Web UI**: http://localhost:7879 (browse repositories, entities, and search)
- **SurrealDB**: ws://localhost:8000/rpc

3. **Environment variables for Docker:**
The Docker Compose file sets up the following environment variables for all services:
```yaml
environment:
  - REDIS_URL=redis://redis:6379
  - SURREALDB_URL=ws://surrealdb:8000/rpc
  - SURREALDB_USERNAME=root
  - SURREALDB_PASSWORD=root
  - SURREAL_NS=zoekt
  - SURREAL_DB=repos
```

4. **Adding repositories:**
Use the Admin UI at http://localhost:7878 to add repositories for indexing, or the new Web UI at http://localhost:7879 for enhanced browsing.

5. **Stop services:**
```bash
cd docker && docker-compose down
```

Running the full system locally
-------------------

Running the full system locally
-------------------------------

You'll need Redis and SurrealDB running for all components to work together.

**Legacy Zoekt-Distributed Services:**

1. Run the Indexer:
```bash
cd /workspaces/hyperzoekt && RUST_LOG=debug REDIS_URL=redis://127.0.0.1:7777 cargo run --bin dzr-indexer -- --listen 127.0.0.1:3001 --remote-url /workspaces/hyperzoekt --disable-reindex
```
2. Run the Admin UI:
```bash
cd /workspaces/hyperzoekt && RUST_LOG=debug REDIS_URL=redis://127.0.0.1:7777 ZOEKT_ADMIN_USERNAME=admin ZOEKT_ADMIN_PASSWORD=password cargo run --bin dzr-admin -- --bind 127.0.0.1:7878
```
3. Run the Search UI:
```bash
cd /workspaces/hyperzoekt && RUST_LOG=debug REDIS_URL=redis://127.0.0.1:7777 cargo run --bin dzr-http-search -- --listen 127.0.0.1:8080
```
4. Run the MCP Server:
```bash
cd /workspaces/hyperzoekt && RUST_LOG=debug REDIS_URL=redis://127.0.0.1:7777 cargo run --bin dzr-mcp-search -- --listen 127.0.0.1:7979
```

**New Hyperzoekt Services:**

1. **Run the Indexer:**
```bash
cd /workspaces/hyperzoekt && RUST_LOG=info REDIS_URL=redis://127.0.0.1:6379 SURREALDB_URL=ws://127.0.0.1:8000/rpc cargo run --bin hyperzoekt-indexer
```
2. **Run the Web UI:**
```bash
cd /workspaces/hyperzoekt && RUST_LOG=info REDIS_URL=redis://127.0.0.1:6379 SURREALDB_URL=ws://127.0.0.1:8000/rpc cargo run --bin hyperzoekt-webui -- --port 7879 --host 127.0.0.1
```

License
-------
This repository is licensed under the Apache License, Version 2.0. See the `LICENSE` and `NOTICE` files for details.

Devcontainer SurrealDB
----------------------
The development container image now bundles the `surreal` CLI. When the devcontainer starts, the script `/.devcontainer/scripts/start_devcontainer.sh` will auto-start an in-memory SurrealDB on `0.0.0.0:8000` with credentials `root:root` unless you disable it by exporting:

```bash
export DISABLE_LOCAL_SURREAL=1
```

If you rely on Docker Compose's `surrealdb` service instead (e.g. `docker compose up surrealdb`), you can leave the auto-start enabled; the port bind may fail if both try to listen, so in that case disable the auto instance.

Environment variables (already defaults in many examples):

```bash
SURREALDB_URL=http://127.0.0.1:8000
SURREALDB_USERNAME=root
SURREALDB_PASSWORD=root
SURREAL_NS=zoekt
SURREAL_DB=repos
```

The auto-start logs are written to `/tmp/surreal.log` inside the container.

