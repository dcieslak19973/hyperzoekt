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
use crate::repo_index::indexer::payload::EntityPayload;
use std::sync::mpsc::SyncSender;

#[derive(Clone, Debug)]
pub struct PersistedEntityMeta {
    pub stable_id: String,
    pub repo_name: String,
    pub language: String,
    pub kind: String,
    pub name: String,
    pub source_url: Option<String>,
}
use std::thread;

pub type SpawnResult = anyhow::Result<(
    SyncSender<Vec<EntityPayload>>,
    thread::JoinHandle<anyhow::Result<()>>,
)>;

// New type for ack channel when streaming post-persist actions (embedding enqueue)
pub type PersistAckSender = SyncSender<Vec<PersistedEntityMeta>>;

#[derive(Clone, Debug)]
pub struct RepoSnapshotMetadata {
    pub commit_id: String,
    pub parents: Vec<String>,
    pub tree: Option<String>,
    pub author: Option<String>,
    pub message: Option<String>,
    pub size_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct DbWriterConfig {
    pub channel_capacity: usize,
    pub batch_capacity: Option<usize>,
    pub batch_timeout_ms: Option<u64>,
    pub max_retries: Option<usize>,
    pub surreal_url: Option<String>,
    pub surreal_username: Option<String>,
    pub surreal_password: Option<String>,
    pub surreal_ns: String,
    pub surreal_db: String,
    pub initial_batch: bool,
    pub auto_create_missing_repos_for_file_edges: bool,
    // Optional snapshot context used to populate entity_snapshot mapping rows.
    pub snapshot_id: Option<String>,
    pub commit_id: Option<String>,
    /// Optional authoritative git URL for the primary repo being indexed. When
    /// present, the DB writer will use this value when creating/updating the
    /// repo row that matches `repo_name` to avoid fabricating a URL from the
    /// repo name.
    pub repo_git_url: Option<String>,
    pub repo_name: String,
    pub commit_meta: Option<RepoSnapshotMetadata>,
}

impl Default for DbWriterConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 100,
            batch_capacity: Some(500),
            batch_timeout_ms: Some(500),
            max_retries: Some(3),
            surreal_url: None,
            surreal_username: None,
            surreal_password: None,
            surreal_ns: "zoekt".into(),
            surreal_db: "repos".into(),
            initial_batch: false,
            auto_create_missing_repos_for_file_edges: true,
            snapshot_id: None,
            commit_id: None,
            repo_git_url: None,
            repo_name: String::new(),
            commit_meta: None,
        }
    }
}

pub fn surreal_verbose_payloads_enabled() -> bool {
    std::env::var("SURREAL_DB_VERBOSE_PAYLOADS").ok().as_deref() == Some("1")
}
