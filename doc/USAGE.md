# Usage and developer notes

This document provides quick instructions to build, run, and test the Tree-sitter backed repo indexer included in this workspace.

## Build

Install Rust via rustup (https://rustup.rs/) and then build the workspace or just the `hyperzoekt` crate:

```bash
# build entire workspace
cargo build

# or build only the hyperzoekt crate
cd crates/hyperzoekt
cargo build --release
```

Note: Tree-sitter grammar crates include native build steps and may take longer the first time.

## Run the indexer binary

The indexer binary is provided as `hyperzoekt-indexer` (long-running event-driven indexer) and `hyperzoekt-webui`.
For one-shot JSONL export you can use the library API (`RepoIndexService::build`) from your own small wrapper program.

```bash

cd crates/hyperzoekt
# write full JSONL to file
cargo run --release --bin hyperzoekt-indexer -- --repo-root /path/to/repos

# Note: The legacy `hyperzoekt` JSONL one-shot binary was removed. If you need
# a simple JSONL exporter consider either:
# - Using the library: call `hyperzoekt::repo_index::RepoIndexService::build_with_options(...)`
#   from a small Rust wrapper to write JSONL to a file, or
# - Create a thin wrapper binary in this crate that invokes the indexer library.
```

## Tests

Run tests for the `hyperzoekt` crate only:

```bash
cd crates/hyperzoekt
cargo test
```

Or run tests for the whole workspace:

```bash
cargo test --workspace
```

## Linting and formatting

Keep code formatted and clippy-clean:

```bash
cargo fmt --all
cargo clippy --workspace -- -D warnings

## Indexing concurrency

The indexer can process files in parallel to speed up indexing of large repositories. You control concurrency in two ways:

- Environment variable: `HZ_INDEX_MAX_CONCURRENCY` — if set and the indexer builder did not set an explicit concurrency value, this value is used as the maximum number of worker threads.
- Programmatic builder: `RepoIndexOptions::concurrency(n)` — explicitly set the number of worker threads for an indexing run. If set to `0`, the indexer will use `HZ_INDEX_MAX_CONCURRENCY` (if present) or fall back to the host CPU count.

Notes:
- Parallel indexing requires the indexer to write output to a file path (owned `File`) because arbitrary writers (for example a borrowed `&mut dyn Write`) are not Send/Sync and cannot be moved into worker threads. If you pass a writer directly, the indexer will run in serial mode.
- Reasonable defaults: when unspecified the indexer uses the `HZ_INDEX_MAX_CONCURRENCY` value if available, otherwise it uses `num_cpus::get()`.

```

## Notes about Tree-sitter grammars

- Tree-sitter grammar crates compile C/C++ code via a `build.rs` which requires a working C toolchain. If builds fail on your machine, ensure `build-essential` (or your platform's equivalent) is installed.
- First-time builds will be slower because the grammars must compile. CI should cache `target/` and any compiled artifacts.

## Troubleshooting

If you encounter mysterious parse or compilation errors after making large automated edits (for example, transforming many `starts_with`/slicing patterns), check changed files for unbalanced braces or control-flow errors and run `cargo build` to get parser errors.

---

Doc generated/edited by the repo maintainer tooling.

## Embedded SurrealDB default behavior


When running the indexer without a remote SurrealDB configured, the indexer may start an embedded, file-backed SurrealDB instance to simplify local development. By default embedded storage uses `.data/surreal.db`.

Configuration summary:

- Environment variables:
  - `SURREALDB_URL` — if set, the indexer will connect to this remote SurrealDB HTTP endpoint (for example `http://localhost:8001`) instead of embedding.
	- `SURREALDB_EMBED_MODE` — `file` (default) or `memory` (ephemeral).
	- `SURREALDB_EMBED_PATH` — path for embedded DB files (default: `.data/surreal.db`).

- CLI flags (override env vars):
	- `--surrealdb-url <url>`
	- `--embed-mode <file|memory>`
	- `--embed-path <path>`

Notes:

- The launcher will create the `.data/` directory automatically if it does not exist.
- Use `SURREAL_EMBED_MODE=memory` for ephemeral runs (tests, CI) to avoid creating persistent files.
- The repository's `.gitignore` should include `.data/` so DB files are not accidentally committed.

## Similarity (embeddings)

The similarity worker can compute and persist inter-entity similarity relations based on stored embedding vectors. This workspace uses SurrealDB vector functions to perform the heavy-lifting server-side: the similarity worker issues SELECT queries that compute cosine similarity inside SurrealDB (via `vector::similarity::cosine(embedding, $vec)`) and returns the top-N candidates which are then materialized as relation rows (`similar_same_repo` and `similar_external_repo`).

Notes and requirements:

- Server-side similarity requires a SurrealDB build that provides vector math functions (for example `vector::similarity::cosine`) and supports ordering by the computed score. If your SurrealDB instance does not implement these functions the embed worker will fall back to leaving no similarity relations.
- Because scoring is done in the DB, queries may perform full-table scans unless SurrealDB provides a vector index/KNN operator. Evaluate performance on your dataset and SurrealDB version before enabling at large scale.

Configuration (similarity worker env vars):

- `HZ_ENABLE_EMBED_SIMILARITY`: enable/disable similarity computation (set to `1`/`true` to enable).
- `HZ_SIMILARITY_MAX_SAME_REPO`: maximum number of same-repo similar entities to fetch (default: `25`).
- `HZ_SIMILARITY_MAX_EXTERNAL_REPO`: maximum number of external-repo similar entities to fetch (default: `50`).
- `HZ_SIMILARITY_THRESHOLD_SAME_REPO`, `HZ_SIMILARITY_THRESHOLD_EXTERNAL_REPO`: legacy thresholds previously used by the client-side implementation; currently unused when SurrealDB server-side scoring is enabled but preserved as configuration knobs for future filtering logic.

Behavior:

- For each persisted embedding the worker issues two SELECT queries: one restricting to the same `repo_name` and one excluding it. Results include `stable_id` and a numeric `score` computed by `vector::similarity::cosine(embedding, $vec)` and are ordered by `score DESC` and truncated to the configured maxima.
- The worker translates the returned rows into relation statements that `RELATE` the current entity to the candidate entities with a `score` field on the relation (`similar_same_repo` or `similar_external_repo`).
- If SurrealDB returns an error for these queries the worker logs a warning and skips similarity relation updates for that entity.

If you need a client-side fallback (for example when running against an older SurrealDB), consider toggling computation or adding a feature flag to compute cosine similarity locally in the similarity worker before creating relation rows.

## OpenTelemetry / Tracing

Hyperzoekt can emit tracing spans and metrics via OpenTelemetry (OTLP) when the optional `otel` feature is enabled and runtime initialization is turned on. This helps visualize indexing phases and PageRank iterations in your observability stack (e.g., Tempo + Loki + Grafana, or Honeycomb).

Enablement consists of two parts:

- Build-time feature flag:
  - Enable the `otel` feature for the `hyperzoekt` crate when running binaries:
    ```bash
    # From workspace root
    cargo run -p hyperzoekt --features otel --bin hyperzoekt-indexer
    cargo run -p hyperzoekt --features otel --bin hyperzoekt-webui
    ```
  - If you’re invoking from a subdirectory, add `--manifest-path` to point at the workspace root `Cargo.toml`.

- Runtime initialization (env-gated):
  - Set `HZ_ENABLE_OTEL=1` to activate tracing initialization at process start for both `hyperzoekt-indexer` and `hyperzoekt-webui`.
  - Configure the OTLP exporter endpoint and service name via env:
    - `OTEL_EXPORTER_OTLP_ENDPOINT` (default: `http://localhost:4317`)
    - `OTEL_SERVICE_NAME` (default: `hyperzoekt`)
  - Optionally set `RUST_LOG` to control console logging (e.g., `RUST_LOG=info,hyperzoekt=trace`).

Example runs with OTEL enabled:

```bash
# Indexer with OTEL
HZ_ENABLE_OTEL=1 OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  RUST_LOG=info,hyperzoekt=trace \
  cargo run --manifest-path /workspaces/hyperzoekt/Cargo.toml -p hyperzoekt --features otel --bin hyperzoekt-indexer

# Web UI with OTEL
HZ_ENABLE_OTEL=1 OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  RUST_LOG=info,hyperzoekt=trace \
  cargo run --manifest-path /workspaces/hyperzoekt/Cargo.toml -p hyperzoekt --features otel --bin hyperzoekt-webui -- --host 127.0.0.1 --port 7879
```

Emitted spans (when feature-enabled):

- `repo_index.build` (root span for indexing a repository)
- `walk_files` → file discovery
- `read_file` (per-file)
- `parse` (per-file; includes `lang`)
- `extract` (per-file entity extraction)
- `alias_tree` (Tree-sitter alias extraction)
- `alias_fallback` (line-based alias fallback)
- `module_map`
- `scope_containment`
- `import_edges`
- `alias_resolution`
- `calls_resolution`
- `pagerank` (overall PageRank phase)
- `pagerank_iter` (iteration loop span with per-iteration logs: `iter`, `l1_change`)

Notes:
- OTEL is optional; if you build without `--features otel`, the code compiles and runs normally, and the initialization is a no-op.
- The OTLP pipeline uses the Tokio runtime and a batch exporter; ensure your collector accepts gRPC on the configured endpoint (default 4317).
- You can combine OTEL with JSON/console logging by setting `RUST_LOG` as usual; traces are exported independently.
