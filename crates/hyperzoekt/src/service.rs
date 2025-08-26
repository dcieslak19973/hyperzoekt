// Service module facade — re-export smaller modules for clarity.

pub mod builder;
pub mod graph;
pub mod pagerank;
pub mod search;
pub mod types;

pub use types::{FileRecord, RepoEntity, RepoIndexService, StoredEntity};
