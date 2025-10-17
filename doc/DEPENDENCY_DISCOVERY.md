# Dependency Discovery

This document explains how repository-level dependencies are discovered and normalized by the HyperZoekt toolchain.

The goal of dependency discovery is to extract declared dependencies from common build files and map them to a canonical form that can be stored in the index database. HyperZoekt performs static extraction (no build execution) using language parsers and heuristics.

Key points
- Extraction is static: no Gradle/Maven/NPM/etc. runtime is invoked.
- Tree-sitter is used for language-aware extraction where practical.
- Version catalog resolution (Gradle `libs.versions.toml`) is supported for common catalog shapes.
- Results are normalized into `Dependency { group, name, version?, language }` and persisted by `hyperzoekt`.

Where the implementation lives
- Static extraction code: `crates/zoekt-rs/src/typesitter.rs`
- Repository-level file discovery and normalization: `crates/hyperzoekt/src/repo_index/deps.rs`

Supported languages and files
- Gradle (Groovy DSL): `build.gradle` — parsed with `tree-sitter-groovy`.
- Gradle Kotlin DSL: `build.gradle.kts` — parsed using the same extractor with language tagged as `kotlin` (file-extension based). See `doc/dependencies/gradle.md` for details.
- Other languages: see `doc/dependencies/README.md` for current per-language support and notes.

Limitations and caveats
- We do not evaluate build scripts or resolve dynamic constructs — only literal and common named-argument forms are recognized.
- Kotlin DSL parsing is limited: we treat `.kts` files as `kotlin` for language tagging but rely on pattern matching for dependency declarations rather than a full Kotlin AST for Gradle's Kotlin DSL semantics.
- Version catalogs are parsed only when they conform to common `libs.versions.toml` shapes; complex substitution or custom catalog layouts may not be resolved.
- If you need more precise resolution (for example, variable interpolation or plugin-managed dependencies), consider running an external build tool and importing its output into the index.

Testing and CI notes
- Unit and integration tests live in the crates' `tests` directories; run them with `cargo test --workspace`.
- Integration tests that touch SurrealDB may behave differently when using a remote DB; set `HZ_DISABLE_SURREAL_ENV=1` to force the embedded in-memory SurrealDB during local runs, or run a permissive local SurrealDB and set `SURREALDB_URL`, `SURREALDB_USERNAME`, and `SURREALDB_PASSWORD` when you want to exercise remote behavior.

If you plan to extend dependency extraction for a new language, add a per-language document under `doc/dependencies/` and update `doc/dependencies/README.md` to reflect the new support.
