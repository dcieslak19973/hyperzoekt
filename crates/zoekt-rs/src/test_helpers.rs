//! Test-only helpers that require access to crate internals.
//!
//! This module lives in `src/` so integration tests under `tests/` (which are
//! compiled as a separate test crate) can access helpers that call `pub(crate)`
//! items. Keeping these helpers in `src/` preserves the ability to exercise
//! internal implementation details from integration tests while avoiding
//! widening the public API surface for consumers.
//!
//! See CONTRIBUTING.md for the recommended pattern.

use crate::shard::ShardReader;
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;

/// Integration-test helper to call `process_pending` with a vector of
/// pending files constructed from provided paths. This keeps `process_pending`
/// crate-private while allowing integration tests under `tests/` to exercise it.
pub fn process_pending_from_paths(
    root: PathBuf,
    paths: Vec<PathBuf>,
    enable_symbols: bool,
    thread_cap: Option<usize>,
) -> Vec<(PathBuf, bool)> {
    let pending: Vec<crate::index::PendingFile> = paths
        .into_iter()
        .map(|p| crate::index::PendingFile {
            rel: p.clone(),
            size: 0,
            lang: None,
            branches: vec!["HEAD".to_string()],
            base: Some(root.clone()),
        })
        .collect();
    let processed =
        crate::index::process::process_pending(pending, root, enable_symbols, thread_cap);
    processed
        .into_iter()
        .map(|p| (p.doc.path, p.content.is_some()))
        .collect()
}

/// Test-only wrapper to access `ShardReader::symbol_postings_map` (a `pub(crate)` API).
///
/// Integration tests call this helper via `zoekt_rs::test_helpers::symbol_postings_map(&r)`.
/// This keeps `symbol_postings_map` crate-private while allowing tests to verify
/// its behavior.
pub fn symbol_postings_map(r: &ShardReader) -> Result<HashMap<[u8; 3], Vec<u32>>> {
    r.symbol_postings_map()
}

// --- wrappers for internal helpers used by integration tests ---

pub fn test_write_var_u32(buf: &mut Vec<u8>, v: u32) -> Result<()> {
    crate::shard::writer_utils::write_var_u32(buf, v)
}

pub fn test_read_var_u32(cursor: &mut std::io::Cursor<Vec<u8>>) -> Result<u32> {
    crate::shard::writer_utils::read_var_u32(cursor)
}

pub fn test_radix_sort_u128(buf: &mut [u128]) {
    crate::shard::writer_utils::radix_sort_u128(buf)
}

pub fn repo_matches_wrapper(
    repo_name: &str,
    repo_branches: &[String],
    plan: &crate::QueryPlan,
) -> bool {
    crate::query::searcher::plan_helpers::repo_matches(repo_name, repo_branches, plan)
}

pub fn build_path_docs_wrapper(
    docs: &[crate::types::DocumentMeta],
    pattern: &str,
    case_sensitive: bool,
) -> Vec<crate::RepoDocId> {
    crate::query::searcher::plan_helpers::build_path_docs(docs, pattern, case_sensitive)
}

pub fn apply_file_filters_wrapper(
    idx: &crate::index::InMemoryIndex,
    base: Vec<crate::RepoDocId>,
    plan: &crate::QueryPlan,
) -> Vec<crate::RepoDocId> {
    crate::query::searcher::plan_helpers::apply_file_filters(idx, base, plan)
}

pub fn symbol_prefilter_wrapper(
    idx: &crate::index::InMemoryIndex,
    plan: &crate::QueryPlan,
) -> Option<Vec<crate::RepoDocId>> {
    crate::query::searcher::plan_helpers::symbol_prefilter(idx, plan)
}

pub fn make_index_with_trigrams(
    docs: Vec<crate::types::DocumentMeta>,
    repo_name: &str,
    branches: Vec<String>,
    tris: Option<([u8; 3], Vec<u32>)>,
) -> crate::index::InMemoryIndex {
    use crate::index::InMemoryIndexInner;
    use crate::types::RepoMeta;
    let mut symbol_trigrams = std::collections::HashMap::new();
    if let Some((t, v)) = tris {
        symbol_trigrams.insert(t, v);
    }
    let inner = InMemoryIndexInner {
        repo: RepoMeta {
            name: repo_name.to_string(),
            root: std::path::PathBuf::from("/tmp"),
            branches,
            visibility: crate::types::RepoVisibility::Public, // Default to public for tests
            owner: None,
            allowed_users: Vec::new(),
            last_commit_sha: None,
        },
        docs,
        terms: std::collections::HashMap::new(),
        symbol_terms: std::collections::HashMap::new(),
        symbol_trigrams,
        doc_contents: vec![],
    };
    crate::index::InMemoryIndex::from_inner(inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_write_var_u32_roundtrip() {
        let mut buf = vec![];
        test_write_var_u32(&mut buf, 0).unwrap();
        test_write_var_u32(&mut buf, 1).unwrap();
        test_write_var_u32(&mut buf, 300).unwrap();
        test_write_var_u32(&mut buf, u32::MAX).unwrap();
        // now read back
        let mut cursor = Cursor::new(buf);
        assert_eq!(test_read_var_u32(&mut cursor).unwrap(), 0);
        assert_eq!(test_read_var_u32(&mut cursor).unwrap(), 1);
        assert_eq!(test_read_var_u32(&mut cursor).unwrap(), 300);
        assert_eq!(test_read_var_u32(&mut cursor).unwrap(), u32::MAX);
    }

    #[test]
    fn radix_sort_u128_sorts() {
        let mut arr = [3u128, 1u128, 2u128, 0u128, u128::MAX];
        test_radix_sort_u128(&mut arr);
        assert_eq!(arr, [0u128, 1u128, 2u128, 3u128, u128::MAX]);
    }
}
