---
applyTo: '**'
---
## Guiding an AI coding agent for hyperzoekt

Purpose: concise, actionable instructions to let an AI coding agent be productive here.

Quick rules
 - Always follow Rust best practices: prefer idiomatic ownership/borrowing, clear error handling (use `Result` and `anyhow`/`thiserror` where appropriate), prefer iterator adapters over manual loops, avoid unnecessary clones, and favor small, testable functions. Run `cargo fmt` and `cargo clippy --workspace -- -D warnings` locally before committing.

Architecture snapshot
 - Cargo workspace root: top-level `Cargo.toml` controls the workspace and `workspace.members`.
 - Entry: `src/main.rs` is the top-level binary prototype.
 - Indexing: `crates/hyperzoekt` contains the indexing service (public API: `RepoIndexService::build(root)` and `search(query)`). Keep the public API small and stable.

Developer workflows (commands)

	- `cargo fmt --all`
	- `cargo clippy --workspace -- -D warnings`

Removing files

	- Use `git rm <file>` to remove files from the working directory and stage the removal for commit.
	
Conventions and patterns

If you need to make a larger change

When you finish edits

Questions to ask the repo owner (if unclear)

Done — update this file if the workspace layout changes.
