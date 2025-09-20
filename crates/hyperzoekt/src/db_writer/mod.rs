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
// db_writer module: split from original large file into focused submodules.
// Public re-exports keep external API stable.

mod config;
pub mod connection;
mod content;
mod helpers;
mod refs;
mod repo_deps;
mod writer;

pub use config::{DbWriterConfig, SpawnResult};
pub use connection::SHARED_MEM; // keep backward compatibility
pub use content::{
    upsert_content_if_missing, upsert_entity_snapshot, write_content_embedding,
    EntitySnapshotUpsert,
};
pub use helpers::{init_call_edge_capture, CALL_EDGE_CAPTURE};
pub use refs::{create_branch, create_commit, create_snapshot_meta, create_tag, move_branch};
pub use repo_deps::{
    persist_repo_dependencies, persist_repo_dependencies_with_connection,
    RepoDependencyPersistResult,
};
pub use writer::spawn_db_writer;
