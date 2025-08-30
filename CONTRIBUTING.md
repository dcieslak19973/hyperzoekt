# Contributing to hyperzoekt

Thanks for your interest in contributing!

## Getting started
- Fork the repository and create a feature branch from `main`.
- Build the project locally:

  cargo build

- Run tests:

  cargo test

- Ensure formatting and lints pass locally before opening a PR:

  cargo fmt
  cargo clippy -- -D warnings

## Pull request process
- Open a PR against `main` with a clear title and description.
- Fill the PR template.
- Include a short summary of changes and why they are needed.
- CI (GitHub Actions) should run automatically and must pass.

## Coding conventions
- Follow the existing code style. Use `rustfmt` for formatting.
- Keep changes small and focused.

## Reporting issues
- Use the issue templates when filing bugs or feature requests.
- Include reproduction steps and environment details where possible.

## Contact
- For larger design discussions, open an issue or join the discussion on the repo.

## Test helpers

When integration tests under `crates/*/tests/` need to exercise crate-internal
behavior (for example, calling `pub(crate)` helpers), prefer placing small
test-only helper modules in the crate's `src/` directory and keeping them
clearly marked as test-only. This allows the `tests/` integration test
crates to import them (e.g. `crate_name::test_helpers::foo`) while preserving
the public API surface for downstream consumers.

Pattern summary:
- If the helper needs access to `pub(crate)` or private items: put it in
  `crates/<crate>/src/test_helpers.rs` and expose it from `lib.rs` as
  `#[doc(hidden)] pub mod test_helpers;`.
- If the helper only needs public APIs: put it under
  `crates/<crate>/tests/common/mod.rs` and import it from the other
  integration tests.

Keep test-only helpers small and well-documented. Mark them `#[doc(hidden)]`
or add comments explaining why they live in `src/`.
