use std::env;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

use zoekt_rs::IndexBuilder;

/// Integration test that attempts a real git clone fallback.
///
/// This test is gated behind the `ZOEKT_TEST_CLONE_FALLBACK` environment variable.
/// It will be a no-op (return Ok) unless the env var is set to `1`.
#[test]
fn real_clone_fallback_integration() -> Result<(), Box<dyn std::error::Error>> {
    // Skip unless explicitly enabled in the environment / CI.
    if env::var("ZOEKT_TEST_CLONE_FALLBACK").unwrap_or_default() != "1" {
        eprintln!(
            "skipping real_clone_fallback_integration (set ZOEKT_TEST_CLONE_FALLBACK=1 to enable)"
        );
        return Ok(());
    }

    // Use a small, public repository (this repo) as the target to exercise network cloning.
    // The test relies on the index builder falling back to the `git` CLI if libgit2 fails.
    let url = "https://github.com/dcieslak19973/hyperzoekt";

    let td = tempdir()?;
    let dst = td.path();

    // Call the public helper which accepts a path; for remote URLs, the builder should clone.
    // We simulate a spec that is a remote URL by calling build_in_memory_index with the URL string
    // written into a temporary file tree root. To keep compatibility with the helper, we'll
    // directly call the builder entry that accepts a path-like spec by creating a tiny wrapper
    // directory and passing the URL as a single-repo spec file isn't necessary — instead we
    // call the public API with the URL string via the builder when supported. If the public
    // helper doesn't accept URLs, this test will still be useful to run manually while
    // debugging the clone fallback.

    // For now, attempt to build the index using the local helper which will detect the URL.
    // The helper accepts a path, so create a directory and rely on the internal detection.
    fs::create_dir_all(dst)?;

    // The current public convenience helper `build_in_memory_index` in `zoekt_rs` accepts a
    // path to a workspace root. The builder in the library detects remote specs when a root
    // string looks like a URL; for this integration test we call the lower-level API by
    // constructing a builder via the public crate surface if available. If that surface
    // differs, this test still early-returns with an informative error.

    // Build the index using the public IndexBuilder entrypoint. This constructs the
    // builder with a PathBuf created from the URL string; the builder will detect
    // remote URLs and perform a clone when appropriate.
    let idx = match IndexBuilder::new(PathBuf::from(url)).build() {
        Ok(i) => i,
        Err(e) => {
            // Propagate the error to fail the test so maintainers can see the failure mode.
            return Err(Box::new(e));
        }
    };

    // Basic smoke assertion: index should contain at least one document after cloning.
    assert!(
        idx.doc_count() > 0,
        "index should contain documents after cloning"
    );

    Ok(())
}
