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
