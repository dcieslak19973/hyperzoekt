// Copyright 2025 HyperZoekt Project
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

// Service (renamed to repo_index) module facade — re-export smaller modules for clarity.

pub mod builder;
pub mod deps;
pub mod graph;
pub mod indexer;
pub mod pagerank;
pub mod search;
pub mod types;

pub use types::{FileRecord, RepoEntity, RepoIndexService, StoredEntity};
// Re-export indexer payload types at crate::repo_index::payload for convenience
// payload types are available under `repo_index::indexer::payload`.
