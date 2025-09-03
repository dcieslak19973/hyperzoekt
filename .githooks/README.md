# Git hooks (pre-commit) for this repository

This repository uses a custom git hooks directory (`.githooks`) managed by
`scripts/install-hooks.sh` (it sets `core.hooksPath` to this folder).

## Pre-commit behavior

The pre-commit hook will attempt to automatically apply safe fixes and run
validation checks before allowing commits:

### Rust Code Validation
1. `cargo fix --workspace --allow-dirty --allow-staged` — applies `rustc` suggested fixes.
2. `cargo clippy --fix --allow-dirty --allow-staged` — applies fixable Clippy suggestions.
3. `cargo fmt --all` — formats the workspace.
4. Any resulting changes are staged automatically.
5. Finally, `cargo clippy --workspace -- -D warnings` is run and will block the commit if any warnings remain.

### Helm Chart Validation
When Helm files are staged, the hook also runs:
1. `helm lint` — validates chart syntax and structure
2. `kubeconform` — validates rendered Kubernetes manifests against schemas (if available)
3. `helm-docs` — updates chart documentation (if available)
4. Any documentation changes are automatically staged

### Performance Optimizations
- The hook only runs Rust auto-fix/format steps if there are staged Rust files (`.rs` files)
- The hook only runs Helm validation if there are staged Helm files (`helm/` directory)
- This avoids running heavy commands for unrelated commits

## How to revert autofix changes before committing

If the hook staged autofix changes you don't want to include in the commit:

1. Inspect staged changes:
```bash
git diff --staged
```

2. Unstage the autofix changes (preserve your other staged files):
```bash
git reset -- <path/to/changed-file>
```

3. Optionally discard local changes made by the autofix:
```bash
git checkout -- <path/to/changed-file>
```

4. Re-run `git add` for the files you do want to commit and try again.

## Manual Validation

You can also run validation manually:

```bash
# Validate Helm charts
./scripts/validate-helm.sh

# Validate Rust code
cargo clippy --workspace -- -D warnings
cargo fmt --all
```

## Notes

- `cargo clippy --fix` does not fix every lint. The final strict `clippy` run remains necessary to catch cases that must be fixed manually.
- If the hook takes too long for your workflow, consider running it locally or adjusting the hook to only run `cargo fmt` during pre-commit and run the strict clippy in CI instead.
- Helm validation requires `helm`, `kubeconform`, and `helm-docs` to be installed for full functionality.
