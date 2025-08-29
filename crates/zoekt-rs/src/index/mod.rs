pub mod builder;
pub mod in_memory;
mod utils;

pub use builder::IndexBuilder;
pub use in_memory::{InMemoryIndex, InMemoryIndexInner};

pub type RepoDocId = u32;
