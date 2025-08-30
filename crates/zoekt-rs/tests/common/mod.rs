use std::path::Path;
pub use tempfile;

use std::result::Result as StdResult;
use zoekt_rs::build_in_memory_index;
use zoekt_rs::index::IndexError;
use zoekt_rs::InMemoryIndex;

/// Create a temporary directory and return its path and a guard.
pub fn new_repo() -> tempfile::TempDir {
    tempfile::tempdir().expect("create tempdir")
}

/// Write a file relative to the repo root.
pub fn write_file(repo: &Path, rel: &str, contents: &[u8]) {
    let p = repo.join(rel);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    std::fs::write(p, contents).expect("write file");
}

/// Build an in-memory index for the repo and return a Searcher.
pub fn build_index(repo: &Path) -> StdResult<InMemoryIndex, IndexError> {
    let idx = build_in_memory_index(repo)?;
    Ok(idx)
}
