//! Minimal Zoekt-like indexer/searcher skeleton in Rust.
//! Focus: clear API surface to plug into hyperzoekt without coupling.

pub mod index;
pub mod query;
pub mod regex_analyze;
pub mod shard;
pub mod trigram;
pub mod types;
pub mod typesitter;

pub use crate::index::{InMemoryIndex, IndexBuilder, RepoDocId};
pub use crate::query::{Query, QueryPlan, QueryResult, Searcher, SelectKind};
pub use crate::shard::{SearchMatch, SearchOpts, ShardReader, ShardSearcher, ShardWriter};
pub use crate::trigram::trigrams;

/// Convenience re-export for callers who want a simple one-shot build and search
pub fn build_in_memory_index(
    repo_root: impl AsRef<std::path::Path>,
) -> anyhow::Result<InMemoryIndex> {
    IndexBuilder::new(repo_root.as_ref().to_path_buf()).build()
}

pub mod test_helpers {
    use crate::shard::ShardReader;
    use anyhow::Result;
    use std::collections::HashMap;

    /// Test-only wrapper to access ShardReader::symbol_postings_map (pub(crate)).
    /// Returns a public HashMap type so integration tests can use it.
    pub fn symbol_postings_map(r: &ShardReader) -> Result<HashMap<[u8; 3], Vec<u32>>> {
        r.symbol_postings_map()
    }
}
