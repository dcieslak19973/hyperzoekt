# Git hooks (pre-commit) for this repository

This repository uses a custom git hooks directory (`.githooks`) managed by
`scripts/install-hooks.sh` (it sets `core.hooksPath` to this folder).

pre-commit behavior

- The pre-commit hook will attempt to automatically apply safe fixes for Rust
  code before running a final strict `clippy` check:
  1. `cargo fix --workspace --allow-dirty --allow-staged` — applies `rustc`
     suggested fixes.
  2. `cargo clippy --fix --allow-dirty --allow-staged` — applies fixable Clippy
     suggestions where supported.
  3. `cargo fmt --all` — formats the workspace.
  4. Any resulting changes are staged automatically.
  5. Finally, `cargo clippy --workspace -- -D warnings` is run and will block the
     commit if any warnings remain.

- The hook only runs the auto-fix/format steps if there are staged Rust files
  (files ending with `.rs`). This avoids running heavy cargo commands for
  non-Rust commits.

How to revert autofix changes before committing

If the hook staged autofix changes you don't want to include in the commit:

1. Inspect staged changes:

```bash
git diff --staged
```

2. Unstage the autofix changes (preserve your other staged files):

```bash
git reset -- <path/to/changed-file.rs>
```

3. Optionally discard local changes made by the autofix for the file:

```bash
git checkout -- <path/to/changed-file.rs>
```

4. Re-run `git add` for the files you do want to commit and try again.

Notes

- `cargo clippy --fix` does not fix every lint. The final strict `clippy` run
  remains necessary to catch cases that must be fixed manually (control flow
  changes, API refactors, etc.).
- If the hook takes too long for your workflow, consider running it locally or
  adjusting the hook to only run `cargo fmt` during pre-commit and run the
  strict clippy in CI instead.
