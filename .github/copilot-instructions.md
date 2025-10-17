## Goal
Short, actionable guidance for an AI coding agent working on the HyperZoekt monorepo.

Follow these notes to be productive quickly: where to look, how the parts fit,
and project-specific conventions the human reviewers expect.

### Quick commands
- Never wrap commands in `bash -lc` or similar; just the command itself.
- Build the workspace (crate-level): `cargo build -p hyperzoekt --workspace -v`
- Run tests for the crate: `cargo test -p hyperzoekt`
- Format & lint before commit:
  - `cargo fmt --all`
  - `cargo clippy --workspace -- -D warnings`
- Local SurrealDB (dev): `surreal start --bind 0.0.0.0:8000 --user root --pass root memory`
- Docker compose (local dev): `docker/docker-compose.yml` (service names and envs configured there)

### Big-picture architecture (read these files)
- Workspace root: top-level `Cargo.toml` defines workspace members.
- Main indexing/service crate: `crates/hyperzoekt/` (public API surface is intentionally small — prefer adding small, explicit functions rather than large exported modules).
  - Entry points: binaries live under `crates/hyperzoekt/src/bin/` and `src/` modules for shared code.
  - Important modules:
    - `db` — SurrealDB connection helper (`SurrealConnection`) used for all DB IO.
    - `hirag` — HiRAG-style hierarchical retrieval scaffolding (clusters persisted as `hirag_cluster` records).
    - `llm` — OpenAI-compatible LLM client (reads `HZ_LLM_URL`, `HZ_OPENAI_API_KEY` / `OPENAI_API_KEY`).
    - `similarity` — local similarity helpers used by retrieval functions.

### Data model and integration points
- SurrealDB tables referenced in code:
  - `content` (deprecated - no longer used)
  - `entity` (fields used: `stable_id`, `repo_name`)
  - `entity_snapshot` (fields used: `stable_id`, `source_content`, `embedding`, `embedding_len`, `repo_name`)
  - `hirag_cluster` (created/updated by `hirag::build_first_layer` with fields: `id`, `label`, `summary`, `members`)
- The `hirag` module reads entity embeddings and source content directly from entity_snapshot:
  - `SELECT stable_id, embedding, source_content FROM entity_snapshot WHERE ...`
  - Summaries are persisted with `CREATE hirag_cluster CONTENT $data ON CLUSTER hirag`.

### LLM runtime and configuration
- The Rust `llm` client expects an OpenAI-compatible endpoint. The runtime precedence is:
  - `HZ_LLM_URL` (or `defaultLlmUrl` in Helm values)
  - `HZ_OPENAI_API_KEY` then `OPENAI_API_KEY` for bearer auth
- Helm `values.yaml` contains `defaultLlmUrl: "https://api.openai.com"` — change there or set `HZ_LLM_URL` in your environment for deployments.
- DO NOT commit API keys. Use `docker/.env.template` for local placeholders and `docker/.env` for local-only secrets (gitignored).

### Project-specific conventions
- Rust style: prefer idiomatic ownership/borrowing, iterator adapters, small functions, avoid unnecessary clones. Use `anyhow` or `thiserror` for error propagation.
- Keep public APIs small in `crates/hyperzoekt` — the project favors stable, narrow surfaces (e.g. `RepoIndexService::build` and `search`).
- When removing files use `git rm <file>` so the removal is staged; tests and CI expect tracked deletions.

### Tests & CI
- Integration tests and unit tests for `hyperzoekt` live under `crates/hyperzoekt/tests/` (run with `cargo test -p hyperzoekt`).
- Before changing public behavior, add a focused test that exercises the new path (happy path + one edge case).

### Debugging hints
- If DB reads return unexpected rows, check SQL snippets in `crates/hyperzoekt/src/hirag.rs` and `db` module for how embeddings/snippets are selected.
- To reproduce LLM behavior locally, set `HZ_LLM_URL` to a public OpenAI base and provide `HZ_OPENAI_API_KEY` locally.

### Files to inspect for further context
- `crates/hyperzoekt/src/hirag.rs` — hierarchical clustering and retrieval; shows the `hirag_cluster` persistence format.
- `crates/hyperzoekt/src/llm.rs` — OpenAI-compatible client and env-var precedence.
- `crates/hyperzoekt/src/db` — SurrealDB wrapper (`SurrealConnection`) and query helpers.
- `docker/docker-compose.yml` — local dev services and env var defaults.
- `helm/hyperzoekt/values.yaml` — deployment defaults, including `defaultLlmUrl` and TEI settings.

If anything is unclear or you want more details (e.g. examples of query shapes, a small test harness for `hirag` or a replacement GMM implementation), tell me which area to expand and I'll update this instruction file.  
