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

The indexer binary `hyperzoekt` is located in `crates/hyperzoekt/src/bin/hyperzoekt.rs` and supports writing JSONL output.

```bash

cd crates/hyperzoekt
# write full JSONL to file
cargo run --release --bin hyperzoekt -- --root /path/to/repo --output out.jsonl

# streaming/incremental mode will output lines as entities are discovered
cargo run --release --bin hyperzoekt -- --root /path/to/repo --output out.jsonl --incremental
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
	- `SURREALDB_URL` — if set, the indexer will connect to this remote SurrealDB RPC/WebSocket endpoint (for example `ws://localhost:8000/rpc`) instead of embedding.
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
