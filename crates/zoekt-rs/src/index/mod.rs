pub mod builder;
mod git;
pub mod in_memory;
pub(crate) mod process;
mod utils;

pub use builder::IndexBuilder;
pub use in_memory::{InMemoryIndex, InMemoryIndexInner};

pub type RepoDocId = u32;
// Re-export process types for crate-internal test helpers
pub(crate) use process::PendingFile;
