// Service (renamed to repo_index) module facade — re-export smaller modules for clarity.

pub mod builder;
pub mod graph;
pub mod indexer;
pub mod pagerank;
pub mod search;
pub mod types;

pub use types::{FileRecord, RepoEntity, RepoIndexService, StoredEntity};
// Re-export indexer payload types at crate::repo_index::payload for convenience
// payload types are available under `repo_index::indexer::payload`.
