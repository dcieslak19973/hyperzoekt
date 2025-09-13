// Copyright 2025 HyperZoekt Project
// Derived from sourcegraph/zoekt (https://github.com/sourcegraph/zoekt)
// Copyright 2016 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Minimal Zoekt-like indexer/searcher skeleton in Rust.
//! Focus: clear API surface to plug into hyperzoekt without coupling.

pub mod index;
pub mod query;
pub mod regex_analyze;
pub mod shard;
pub mod trigram;
pub mod types;
pub mod typesitter;

// Re-export selected helpers for downstream callers that prefer a flat import.
// This keeps the public API small while allowing callers to reference helpers
// without relying on module path resolution differences across builds.

pub use crate::index::{InMemoryIndex, IndexBuilder, RepoDocId};
// Query submodules are internal; re-export their public API here.
pub use crate::query::{Query, QueryPlan, QueryResult, Searcher, SelectKind};
pub use crate::shard::{SearchMatch, SearchOpts, ShardReader, ShardSearcher, ShardWriter};
pub use crate::trigram::trigrams;

// Re-export the typesitter with-tree helper at the crate root for stable cross-crate
// calling sites. Some consumers reference this helper directly; providing a
// crate-root re-export ensures callers can find the symbol even if module
// layouts change during refactors.
// Note: do not re-export typesitter helpers from the crate root here. Some
// consumers may import directly from `crate::typesitter` to avoid exposing a
// large flat API surface. Previous attempts to re-export caused unresolved
// import errors during development; keep the module API stable and explicit.

/// Convenience re-export for callers who want a simple one-shot build and search
pub fn build_in_memory_index(
    repo_root: impl AsRef<std::path::Path>,
) -> std::result::Result<InMemoryIndex, crate::index::IndexError> {
    IndexBuilder::new(repo_root.as_ref().to_path_buf()).build()
}

#[doc(hidden)]
pub mod test_helpers;

// (No crate-root re-export for typesitter helpers — prefer callers to import
// from the `typesitter` module to keep the crate root small and avoid
// accidental name collisions.)
