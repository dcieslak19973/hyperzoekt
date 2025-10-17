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

pub mod extract;
pub mod helpers;
pub mod index;
pub mod langspec;
pub mod payload;
pub mod types;

pub use extract::extract_entities;
pub use helpers::extract_import_modules;
pub use index::{detect_language, index_repository, lang_to_ts};
pub use types::{
    Entity, EntityKind, Progress, ProgressCallback, RankWeights, RepoIndexOptions,
    RepoIndexOptionsBuilder, RepoIndexOutput, RepoIndexStats,
};

pub use langspec::*;
