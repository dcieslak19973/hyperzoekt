zoekt-tools — developer utilities

Purpose

This crate contains small developer tools for working with the `zoekt-rs` codebase. These are convenience programs for debugging and inspection and are not part of the public library API.

inspect_prefilter

- What it does: reads the test source at `crates/zoekt-rs/tests/regex/go/parity.rs`, extracts regex test vectors, runs the project's regex prefilter logic on each pattern, and writes a CSV with the pattern and the observed prefilter result.
- Output: writes `/tmp/prefilter_inspect.csv` with columns: `pattern,expect,actual` (CSV-escaped). It also prints a short summary to stderr.

How to run

From the workspace root (recommended):

```bash
# build & run the binary from the workspace
cargo run -p zoekt-tools --bin inspect_prefilter
```

Or, build the crate and run the produced binary directly:

```bash
cargo build -p zoekt-tools
# then
./target/debug/inspect_prefilter
```

Notes

- This is a developer tool; it expects the `zoekt-rs` crate to live at `crates/zoekt-rs` (the workspace path dependency). If you moved the crate, update the path dependency accordingly.
- The tool is intentionally simple and writes to `/tmp` so it can be run without modifying repository files.
- If you prefer the binary to have a different name (to avoid filename collisions when building multiple crates), rename the binary via a `[[bin]]` section in `Cargo.toml` or run it with `cargo run -p zoekt-tools --bin <name>` after changing the `name`.

