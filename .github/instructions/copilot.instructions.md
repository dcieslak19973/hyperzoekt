---
applyTo: '**'
---
## Guiding an AI coding agent for hyperzoekt

Purpose: concise, actionable instructions to let an AI coding agent be productive here.

Quick rules
- Run the build and tests after any change (see Developer Workflows below).
- Prefer small, reversible commits. Keep changes local to a crate when possible.
- Heavy native deps (tree-sitter grammars) can increase build time and introduce native toolchain requirements; consider documenting native build steps in `doc/` instead of presuming a gate.
 - Always follow Rust best practices: prefer idiomatic ownership/borrowing, clear error handling (use `Result` and `anyhow`/`thiserror` where appropriate), prefer iterator adapters over manual loops, avoid unnecessary clones, and favor small, testable functions. Run `cargo fmt` and `cargo clippy --workspace -- -D warnings` locally before committing.

Architecture snapshot
 - Cargo workspace root: top-level `Cargo.toml` controls the workspace and `workspace.members`.
 - Entry: `src/main.rs` is the top-level binary prototype.
 - Indexing: `crates/repo-index` contains the indexing service (public API: `RepoIndexService::build(root)` and `search(query)`). Keep the public API small and stable.

Developer workflows (commands)
- Build workspace: `cargo build`
- Run all tests: `cargo test --workspace`
- Add crate: create `crates/<name>/Cargo.toml` + `crates/<name>/src/` and append its path to `workspace.members` in top-level `Cargo.toml`.

- Formatting & linting (recommended):
	- `cargo fmt --all`
	- `cargo clippy --workspace -- -D warnings`

Conventions and patterns
- Crate layout: small crates under `crates/`; implementation in `src/`, public re-exports in `lib.rs`.
- API shape: `RepoIndexService` should return small serializable statistics structs (see `crates/repo-index/src/service.rs` for the current skeleton).

Integration notes
- Tree-sitter grammars are native builds; enable them selectively. Prefer adding a single language first (e.g., `tree-sitter-rust`) to validate the pipeline.
- If adding UI or devcontainer changes, document rebuild steps in `doc/` and avoid inflating CI unless necessary.

Files to inspect for examples
- `crates/repo-index/src/service.rs` — current skeleton and preferred types.
`Cargo.toml` (top-level) — workspace configuration.
- `README.md`, `doc/TODO.md`, `doc/PLANS.md` — rationale and task list.

If you need to make a larger change
- Propose a small plan and a CI-friendly prototype. If native or long-running builds are required, gate them or document how to run them locally. Run `cargo build` locally and include build/test output in the PR description.

When you finish edits
- Run `cargo build` and `cargo test --workspace` and report PASS/FAIL with the failing output.

Questions to ask the repo owner (if unclear)
- Which language grammar should be prioritized for initial Tree-sitter support?
- Should the repo-index crate expose persistent on-disk indexes or in-memory only (start with in-memory)?

Done — update this file if the workspace layout changes.
