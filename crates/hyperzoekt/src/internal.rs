// Compatibility layer: re-export `repo_index::indexer` public API under `crate::internal`.
// Placed at `src/internal.rs` so the `internal/` directory may be removed.

pub use crate::repo_index::indexer::detect_language;
pub use crate::repo_index::indexer::extract_entities;
pub use crate::repo_index::indexer::extract_import_modules;
pub use crate::repo_index::indexer::index_repository;
pub use crate::repo_index::indexer::lang_to_ts;

pub use crate::repo_index::indexer::Entity;
pub use crate::repo_index::indexer::EntityKind;
pub use crate::repo_index::indexer::Progress;
pub use crate::repo_index::indexer::ProgressCallback;
pub use crate::repo_index::indexer::RankWeights;
pub use crate::repo_index::indexer::RepoIndexOptions;
pub use crate::repo_index::indexer::RepoIndexOptionsBuilder;
pub use crate::repo_index::indexer::RepoIndexOutput;
pub use crate::repo_index::indexer::RepoIndexStats;

// Provide inline modules that re-export the moved implementation so code that
// imports `crate::internal::types` / `crate::internal::helpers` continues to work.
pub mod types {
    pub use crate::repo_index::indexer::types::*;
}

pub mod helpers {
    pub use crate::repo_index::indexer::helpers::*;
}

pub mod langspec {
    pub use crate::repo_index::indexer::langspec::*;
}

pub mod extract {
    pub use crate::repo_index::indexer::extract::*;
}

pub mod index {
    pub use crate::repo_index::indexer::index::*;
}
