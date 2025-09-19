# Hyperzoekt — User Guide

This short user guide explains how to set up repositories using the Admin UI, how to confirm they are processed, and how to use the search UIs to find code and entities.

Audience: operators and developers who want to run a local or development Hyperzoekt instance and index remote repositories (public or private).

Prerequisites
-------------
- Rust toolchain (for running local binaries): https://rustup.rs/
- Redis (for distributed coordination). Default: `redis://127.0.0.1:6379`.
- SurrealDB (for metadata and entity storage). Default HTTP endpoint example: `http://127.0.0.1:8001`.
- Network access to the Git repositories you want to index.

For a quick development setup use the provided Docker Compose in `docker/` which starts Redis and SurrealDB for you:

```bash
cd docker && docker compose -f docker/docker-compose.yml --env-file docker/.env up --build
```

Starting services locally (examples)
-----------------------------------
Run services locally instead of Docker when developing or debugging. Two commonly used binaries are the legacy Admin UI (`dzr-admin`) and the Hyperzoekt Web UI (`hyperzoekt-webui`).

Example: start the Admin UI (legacy) bound to `127.0.0.1:7878` and point it at a local Redis instance:

```bash
RUST_LOG=debug REDIS_URL=redis://127.0.0.1:7777 ZOEKT_ADMIN_USERNAME=admin ZOEKT_ADMIN_PASSWORD=password \
  cargo run --bin dzr-admin -- --bind 127.0.0.1:7878
```

Example: start the Hyperzoekt Web UI (browsing/search) and point it at Redis + SurrealDB:

```bash
RUST_LOG=info REDIS_URL=redis://127.0.0.1:6379 SURREALDB_URL=http://127.0.0.1:8001 \
  cargo run --bin hyperzoekt-webui -- --port 7879 --host 127.0.0.1
```

Using the Admin UI to add remote repositories
--------------------------------------------
1. Open the Admin UI in your browser (example):

   - Legacy Admin UI: http://localhost:7878
   - Use the username/password you configured via `ZOEKT_ADMIN_USERNAME` / `ZOEKT_ADMIN_PASSWORD`.

2. Add a repository

   - Enter the repository location. For HTTPS repos provide the canonical HTTPS URL (for example `https://github.com/owner/repo.git`).
   - For private repos you have two common options:
     - Provide HTTPS credentials (username + token) via the Admin UI fields (the system will inject credentials into the HTTPS clone URL).
     - Use SSH access (ensure the host running the indexer has the appropriate SSH key in its agent or key store). Note: SSH URLs (`git@...`) are not modified by credential injection.

3. Configuration options

   - Branch: choose a branch to index (default: `main` or `master` depending on the repo).
   - Index behavior: some admin UIs expose flags such as `index once` vs continuous indexing — choose according to your workflow.

4. Save the repository configuration.

What happens after adding a repo
-------------------------------
- The indexer (for example `hyperzoekt-indexer`) will receive the new repo via the orchestrator or by polling the configured repo list.

Migration note: the legacy one-shot JSONL exporter has been removed. If you need to perform a one-shot export, use the library API (`RepoIndexService`) from a short Rust program and write results to a file. Example snippet:

```rust
let mut opts = hyperzoekt::repo_index::indexer::RepoIndexOptions::builder();
opts = opts.root("/path/to/repo").output_file("out.jsonl");
let (_svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build_with_options(opts.build())?;
```
- The system will clone the repo and begin indexing. Indexing includes parsing with Tree-sitter, extracting entities, building the search index, and (optionally) persisting entity metadata to SurrealDB.

How to confirm a repository was processed
-----------------------------------------
1. Check the Admin UI / Web UI

   - The Admin UI or the Hyperzoekt Web UI typically lists repositories and their last index time or status.

2. Check indexer logs

   - If running the indexer locally, watch the log output for messages indicating the repository was cloned and indexed.
   - Example log lines to look for: cloning progress, files indexed count, entities indexed count, and successful persistence messages.

3. Check Redis queue and processing status (advanced)

   - If the deployment uses Redis for events/coordination, you can inspect the queue keys (for example `zoekt:repo_events_queue`) using the `redis-cli` or any Redis client:

     ```bash
     redis-cli -h 127.0.0.1 -p 6379 LRANGE zoekt:repo_events_queue 0 -1
     ```

   - Processing/lease keys in Redis may be present (prefix `zoekt:processing:`) while indexes are running.

4. Check SurrealDB for entity records (if configured)

   - Use the SurrealDB Web UI or an RPC client to query the `file` or `entity` tables. For example, query for recent `entity` records by repo name to confirm ingestion.

Using the search UIs
--------------------
Hyperzoekt exposes multiple ways to search indexed repositories.

Web UI (recommended)

- Open the Web UI in your browser (example): http://localhost:7879
- Use the search box to enter query terms. Hyperzoekt supports typical code search patterns (substring, token search). Add repository or language filters if available in the UI.

HTTP API / Search endpoints (advanced)

- The legacy search API and any newer HTTP endpoints accept queries over HTTP. Example (pseudo):

```bash
curl -s "http://localhost:8080/search?q=function+search&repos=owner/repo" | jq
```

Model Context Protocol (MCP) / AI integrations

- For programmatic and AI-driven workflows, use the MCP server (if deployed) which accepts JSON-RPC calls. The MCP server can optionally accept Git credentials via HTTP headers (e.g. `X-GitHub-Username`, `X-GitHub-Token`). See the Admin UI or `doc/` docs for the exact JSON-RPC shape.

Tips and troubleshooting
------------------------
- Clone/authentication failures
  - For private HTTPS repos prefer personal access tokens instead of passwords. Inject them in the Admin UI or set env vars for the indexer process (`GITHUB_USERNAME`, `GITHUB_TOKEN`, etc.).
  - For SSH-based access ensure the indexer host can reach the repo and has the SSH key loaded in `ssh-agent` or the filesystem.

- SurrealDB connectivity
  - If the indexer cannot connect to SurrealDB, confirm `SURREALDB_URL` is set correctly and that the SurrealDB server is reachable from the indexer host.

- Redis connectivity
  - Ensure `REDIS_URL` is correct and that any `REDIS_USERNAME` / `REDIS_PASSWORD` values are set if you use auth.

- If indexing stalls or items remain in a queue
  - Inspect indexer logs for panics or errors.
  - Check Redis keys mentioned above for stuck processing keys and look at TTL values.

Security notes
--------------
- Avoid embedding long-lived credentials in public config files. Use environment variables or secret management to supply tokens.
- When possible prefer token-based access with least privileges required for cloning.

Summary / quick checklist
-------------------------
1. Start Redis and SurrealDB (Docker Compose or local services).
2. Start Admin UI and/or Hyperzoekt Web UI.
3. Add repository via the Admin UI (provide auth if private).
4. Watch indexer logs and UI listing for completion.
5. Use Web UI or API to search indexed content.

Further reading
---------------
- `README.md` — project overview and quick start
- `doc/USAGE.md` — developer notes and build instructions
- `doc/SURREALDB-INTEGRATION.md` — details about SurrealDB ingestion

If you want, I can convert this guide into a short quick-start `README` snippet or add screenshots for the Admin UI and Web UI workflows.
