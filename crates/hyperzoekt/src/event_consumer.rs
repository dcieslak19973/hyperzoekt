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

use crate::db_writer;
use crate::repo_index::deps;
use crate::repo_index::indexer::payload::{EntityPayload, ImportItem, UnresolvedImport};
use crate::repo_index::indexer::EntityKind;
use crate::repo_index::RepoIndexService;
use deadpool_redis::redis::{AsyncCommands, Client};
use deadpool_redis::Pool;
use log;
use std::path::PathBuf;
use std::time::Duration;
// SurrealDB usage delegated to db_writer::persist_repo_dependencies
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use zoekt_distributed::lease_manager::RepoEvent;

/// Event consumer that subscribes to zoekt-distributed repo events
pub struct EventConsumer {
    redis_pool: Option<Pool>,
    event_tx: UnboundedSender<RepoEvent>,
}

/// Metadata computed for a repository snapshot.
#[derive(Debug)]
pub struct RepoSnapshotMetadata {
    pub commit_id: String,
    pub parents: Vec<String>,
    pub tree: Option<String>,
    pub author: Option<String>,
    pub message: Option<String>,
    pub size_bytes: u64,
}

/// Compute snapshot metadata from a cloned repository directory.
fn compute_repo_snapshot_metadata(
    clone_dir: &std::path::PathBuf,
    prefer_commit: Option<&str>,
) -> Result<RepoSnapshotMetadata, anyhow::Error> {
    // Try to open the repo using git2
    let repo = git2::Repository::open(clone_dir)?;
    // Determine commit id: prefer provided commit sha (if present and exists), otherwise HEAD
    let commit_obj = if let Some(pr) = prefer_commit {
        if let Ok(oid) = git2::Oid::from_str(pr) {
            repo.find_commit(oid)?
        } else {
            repo.head()?.peel_to_commit()?
        }
    } else {
        repo.head()?.peel_to_commit()?
    };
    let commit_id = commit_obj.id().to_string();
    // Parents
    let mut parents: Vec<String> = Vec::new();
    for p in commit_obj.parents() {
        parents.push(p.id().to_string());
    }
    // Tree id
    let tree_id = commit_obj.tree_id().to_string();
    let tree = Some(tree_id);
    // Author
    let author = commit_obj.author();
    let author_name = author.name().map(|s| s.to_string());
    // Message
    let message = commit_obj.message().map(|s| s.to_string());
    // Size: sum file sizes under clone_dir
    let mut total: u64 = 0;
    for entry in walkdir::WalkDir::new(clone_dir)
        .into_iter()
        .filter_map(Result::ok)
    {
        if entry.file_type().is_file() {
            if let Ok(metadata) = entry.metadata() {
                total = total.saturating_add(metadata.len());
            }
        }
    }
    Ok(RepoSnapshotMetadata {
        commit_id,
        parents,
        tree,
        author: author_name,
        message,
        size_bytes: total,
    })
}

impl EventConsumer {
    /// Create a new event consumer
    pub fn new(event_tx: UnboundedSender<RepoEvent>) -> Self {
        let redis_pool = Self::create_redis_pool();
        Self {
            redis_pool,
            event_tx,
        }
    }

    /// Create Redis pool for event consumption
    fn create_redis_pool() -> Option<Pool> {
        zoekt_distributed::redis_adapter::create_redis_pool()
    }

    /// Create a direct Redis client for pubsub operations
    fn _create_redis_client() -> Result<Client, anyhow::Error> {
        if let Some(url) = Self::build_redis_url_from_env() {
            Client::open(url).map_err(|e| anyhow::anyhow!("Failed to create Redis client: {}", e))
        } else {
            Client::open("redis://127.0.0.1:6379")
                .map_err(|e| anyhow::anyhow!("Failed to create Redis client: {}", e))
        }
    }

    /// Build a Redis URL from environment variables, inserting auth if needed.
    /// Returns None if no URL can be constructed and caller may choose a default.
    fn build_redis_url_from_env() -> Option<String> {
        // Prefer explicit REDIS_URL
        if let Ok(url_str) = std::env::var("REDIS_URL") {
            // If the provided REDIS_URL doesn't include a scheme (e.g. "host:7000"),
            // prefix it with the redis scheme so url::Url can parse it properly.
            let normalized_input = if url_str.contains("://") {
                url_str.clone()
            } else {
                format!("redis://{}", url_str)
            };

            // Try to parse using the `url` crate. Note: `url = "2"` is declared
            // as a dependency in `crates/hyperzoekt/Cargo.toml`, so this symbol
            // (`url::Url`) is available to the crate.
            if let Ok(mut url) = url::Url::parse(&normalized_input) {
                // Normalize output: remove trailing '/' if path is root
                let make_out = |u: &url::Url| {
                    let s = u.to_string();
                    if s.ends_with('/') && u.path() == "/" {
                        // safe to drop single trailing slash
                        return s.trim_end_matches('/').to_string();
                    }
                    s
                };

                // If user info already present, return normalized string
                if url.username() != "" || url.password().is_some() {
                    return Some(make_out(&url));
                }

                // Otherwise inject credentials if present
                if let Ok(username) = std::env::var("REDIS_USERNAME") {
                    if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                        url.set_username(&username).ok();
                        url.set_password(Some(&password)).ok();
                        return Some(make_out(&url));
                    }
                    // username only
                    url.set_username(&username).ok();
                    return Some(make_out(&url));
                }
                if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                    // url::Url doesn't allow password without username; set username to empty
                    url.set_username("").ok();
                    url.set_password(Some(&password)).ok();
                    return Some(make_out(&url));
                }

                return Some(make_out(&url));
            } else {
                // This branch is unlikely because we prefixed a scheme above, but
                // keep a defensive fallback that builds a redis:// URL from the
                // raw REDIS_URL and any available credentials.
                let fallback = if let Ok(username) = std::env::var("REDIS_USERNAME") {
                    if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                        format!("redis://{}:{}@{}", username, password, url_str)
                    } else {
                        format!("redis://{}@{}", username, url_str)
                    }
                } else if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                    format!("redis://:{}@{}", password, url_str)
                } else {
                    format!("redis://{}", url_str)
                };
                return Some(fallback);
            }
        }

        // No REDIS_URL; build from credentials if available
        if let (Ok(username), Ok(password)) = (
            std::env::var("REDIS_USERNAME"),
            std::env::var("REDIS_PASSWORD"),
        ) {
            let mut url = format!("redis://{}:{}@127.0.0.1:6379", username, password);
            if let Ok(db) = std::env::var("TEST_REDIS_DB").or_else(|_| std::env::var("REDIS_DB")) {
                if !db.trim().is_empty() {
                    if !url.ends_with('/') {
                        url.push('/');
                    }
                    url.push_str(&db);
                }
            }
            return Some(url);
        }
        if let Ok(password) = std::env::var("REDIS_PASSWORD") {
            let mut url = format!("redis://:{}@127.0.0.1:6379", password);
            if let Ok(db) = std::env::var("TEST_REDIS_DB").or_else(|_| std::env::var("REDIS_DB")) {
                if !db.trim().is_empty() {
                    if !url.ends_with('/') {
                        url.push('/');
                    }
                    url.push_str(&db);
                }
            }
            return Some(url);
        }

        // No explicit REDIS_URL or credentials; allow TEST_REDIS_DB to pick a local DB
        if let Ok(db) = std::env::var("TEST_REDIS_DB").or_else(|_| std::env::var("REDIS_DB")) {
            if !db.trim().is_empty() {
                return Some(format!("redis://127.0.0.1:6379/{}", db));
            }
        }

        None
    }

    /// Check if Redis is available
    pub fn is_redis_available(&self) -> bool {
        self.redis_pool.is_some()
    }

    /// Start consuming events from the zoekt:repo_events channel
    pub async fn start_consuming(&self) -> Result<(), anyhow::Error> {
        self.start_consuming_with_ttl(300).await // Default to 5 minutes
    }

    /// Start consuming events from the zoekt:repo_events channel with configurable TTL
    pub async fn start_consuming_with_ttl(
        &self,
        processing_ttl_seconds: u64,
    ) -> Result<(), anyhow::Error> {
        let pool = match self.redis_pool.clone() {
            Some(p) => p,
            None => {
                log::warn!("No Redis pool available for event consumption");
                return Ok(());
            }
        };

        log::info!("Starting event consumer for zoekt:repo_events channel");

        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            loop {
                match Self::consume_events_with_ttl(
                    pool.clone(),
                    event_tx.clone(),
                    processing_ttl_seconds,
                )
                .await
                {
                    Ok(_) => {
                        // This shouldn't happen in normal operation
                        log::info!("Event consumption completed unexpectedly");
                        break;
                    }
                    Err(e) => {
                        log::error!("Event consumption failed: {}", e);
                        // Wait before retrying
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Consume events from Redis using a reliable queue pattern
    async fn consume_events(
        pool: Pool,
        event_tx: UnboundedSender<RepoEvent>,
    ) -> Result<(), anyhow::Error> {
        Self::consume_events_with_ttl(pool, event_tx, 300).await // Default to 5 minutes
    }

    /// Consume events from Redis using a reliable queue pattern with configurable TTL
    async fn consume_events_with_ttl(
        pool: Pool,
        event_tx: UnboundedSender<RepoEvent>,
        processing_ttl_seconds: u64,
    ) -> Result<(), anyhow::Error> {
        let mut conn = pool.get().await?;
        let queue_key = "zoekt:repo_events_queue";
        let processing_key_prefix = "zoekt:processing:";
        let consumer_id = format!("indexer-{}", std::process::id());

        log::debug!(
            "Starting Redis queue consumer: {} (TTL: {}s)",
            consumer_id,
            processing_ttl_seconds
        );

        loop {
            // First, try to recover any expired messages from other consumers
            if let Err(e) =
                Self::recover_expired_messages(&mut conn, processing_key_prefix, queue_key).await
            {
                log::warn!("Failed to recover expired messages: {}", e);
            }

            // Try to get a message from the queue using BRPOPLPUSH for atomic operation
            // Use a unique processing key for this message
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis();
            let processing_key = format!("{}:{}:{}", processing_key_prefix, consumer_id, timestamp);

            let message: Option<String> = conn.brpoplpush(queue_key, &processing_key, 5.0).await?;

            if let Some(event_json) = message {
                // Message received (reduced verbosity)
                log::debug!("🚀 Received indexing request: {}", event_json);

                // Set TTL on processing key for failure recovery
                let _: () = conn
                    .expire(&processing_key, processing_ttl_seconds as i64)
                    .await?;

                match serde_json::from_str::<RepoEvent>(&event_json) {
                    Ok(event) => {
                        // Log more details about the processing start
                        log::debug!(
                            "📋 Starting processing for repo '{}' from node '{}' (branch: {:?})",
                            event.repo_name,
                            event.node_id,
                            event.branch
                        );

                        // Send event to processor
                        if event_tx.send(event).is_err() {
                            log::warn!("Event channel closed, stopping consumer");
                            return Ok(());
                        }

                        // Message processed successfully, remove from processing queue
                        let _: () = conn.del(&processing_key).await?;
                        log::debug!("Successfully processed and acknowledged message");
                    }
                    Err(e) => {
                        log::error!("Failed to deserialize event: {}", e);
                        // Remove malformed message from processing queue
                        let _: () = conn.del(&processing_key).await?;
                    }
                }
            } else {
                // No messages available, small delay to prevent busy loop
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    /// Recover messages from expired processing keys
    async fn recover_expired_messages(
        conn: &mut deadpool_redis::Connection,
        processing_key_prefix: &str,
        queue_key: &str,
    ) -> Result<(), anyhow::Error> {
        // Find all processing keys for this consumer type
        let processing_keys: Vec<String> = conn.keys(format!("{}*", processing_key_prefix)).await?;

        for key in processing_keys {
            // Check if this key has expired (TTL <= 0)
            let ttl: i64 = conn.ttl(&key).await?;
            if ttl <= 0 {
                // Key has expired, recover the message
                let message: Option<String> = conn.get(&key).await?;
                if let Some(msg) = message {
                    // Put the message back on the main queue
                    let _: () = conn.lpush(queue_key, &msg).await?;
                    // Remove the expired processing key
                    let _: () = conn.del(&key).await?;
                    log::warn!("Recovered expired message and re-queued it: {}", msg);
                }
                // Clean up the key even if it has no value
                let _: () = conn.del(&key).await?;
            }
        }

        Ok(())
    }
}

/// Event processor that handles incoming repo events
pub struct EventProcessor {
    event_rx: tokio::sync::mpsc::UnboundedReceiver<RepoEvent>,
}

impl EventProcessor {
    /// Create a new event processor
    pub fn new(event_rx: tokio::sync::mpsc::UnboundedReceiver<RepoEvent>) -> Self {
        Self { event_rx }
    }

    /// Start processing events
    pub async fn start_processing(mut self) -> Result<(), anyhow::Error> {
        log::info!("Starting event processor");

        while let Some(event) = self.event_rx.recv().await {
            if let Err(e) = self.process_event(event).await {
                log::error!("Failed to process event: {}", e);
            }
        }

        log::info!("Event processor stopped");
        Ok(())
    }

    /// Extract owner from a git URL
    /// Examples:
    /// - https://github.com/owner/repo.git -> owner
    /// - git@github.com:owner/repo.git -> owner
    /// - https://gitlab.com/owner/repo -> owner
    fn extract_owner_from_git_url(git_url: &str) -> Option<String> {
        // Handle SSH style: git@host:owner/repo.git
        if git_url.starts_with("git@") {
            if let Some(colon) = git_url.find(':') {
                let rest = &git_url[colon + 1..];
                if let Some(slash) = rest.find('/') {
                    return Some(rest[..slash].to_string());
                }
            }
        }

        // Handle HTTP/HTTPS style: https://host/owner/repo
        if git_url.starts_with("http://") || git_url.starts_with("https://") {
            let url = git_url.trim_end_matches(".git");
            if let Some(host_start) = url.find("://") {
                let after_host = &url[host_start + 3..];
                let parts: Vec<&str> = after_host.split('/').collect();
                if parts.len() >= 2 {
                    return Some(parts[1].to_string());
                }
            }
        }

        None
    }

    /// Process a single repo event
    pub async fn process_event(&self, event: RepoEvent) -> Result<(), anyhow::Error> {
        match event.event_type.as_str() {
            "indexing_started" => {
                log::info!(
                    "Processing indexing_started event for repo {} (branch: {:?})",
                    event.repo_name,
                    event.branch
                );
                // TODO: Implement indexing started handling
                // This could include:
                // - Updating internal state
                // - Notifying other components
                // - Preparing for incoming index data
            }
            "indexing_finished" => {
                log::info!(
                    "Processing indexing_finished event for repo {} (branch: {:?})",
                    event.repo_name,
                    event.branch
                );
                // previously printed to stderr; use structured logging instead
                log::debug!(
                    "Processing indexing_finished event (stderr->log) for repo {} (branch: {:?})",
                    event.repo_name,
                    event.branch
                );

                // Add prominent log when starting repository processing
                log::info!(
                    "🚀 Starting repository indexing for {} (branch: {:?}) from node {}",
                    event.repo_name,
                    event.branch,
                    event.node_id
                );
                // also emit as structured log rather than stderr
                log::info!(
                    "🚀 Starting repository indexing for {} (branch: {:?}) from node {}",
                    event.repo_name,
                    event.branch,
                    event.node_id
                );

                // Extract owner from git URL
                let owner = Self::extract_owner_from_git_url(&event.git_url);
                if let Some(ref owner_name) = owner {
                    log::debug!(
                        "📋 Extracted owner '{}' from git URL: {}",
                        owner_name,
                        event.git_url
                    );
                } else {
                    log::debug!(
                        "⚠️  Could not extract owner from git URL: {}",
                        event.git_url
                    );
                }

                // Clone the remote repository to a temporary directory
                let temp_dir = match Self::clone_repository(&event).await {
                    Ok(dir) => dir,
                    Err(e) => {
                        log::error!("Failed to clone repository {}: {}", event.repo_name, e);
                        return Ok(()); // Don't retry, let zoekt-distributed handle it
                    }
                };

                let repo_name = event.repo_name.clone();
                let temp_dir_clone = temp_dir.clone();
                // capture owner for use inside blocking closure
                let owner_clone = owner.clone();

                // Collect dependencies in a blocking way before spawning the main blocking indexer
                let deps_temp_dir = temp_dir.clone();
                let deps_found: Vec<deps::Dependency> =
                    match tokio::task::spawn_blocking(move || {
                        deps::collect_dependencies(&deps_temp_dir)
                    })
                    .await
                    {
                        Ok(d) => d,
                        Err(e) => {
                            log::warn!("Failed to collect dependencies (join error): {}", e);
                            Vec::new()
                        }
                    };

                // Persist repo metadata and any discovered dependencies.
                // Previously this call occurred only when dependencies were found;
                // ensure we always create/upsert a `repo` record even if no deps
                // were discovered so users can discover indexed repositories in
                // SurrealDB regardless of dependency extraction results.
                if deps_found.is_empty() {
                    log::debug!(
                        "No dependency entries found for {}; persisting repo metadata only",
                        repo_name
                    );
                } else {
                    log::info!(
                        "Found {} dependency entries for {}",
                        deps_found.len(),
                        repo_name
                    );
                }

                // Persist dependencies using the db_writer helper (normalized schema + idempotent upsert)
                let surreal_url = std::env::var("SURREALDB_URL").ok();
                let cfg = db_writer::DbWriterConfig {
                    surreal_url,
                    surreal_username: std::env::var("SURREALDB_USERNAME").ok(),
                    surreal_password: std::env::var("SURREALDB_PASSWORD").ok(),
                    surreal_ns: std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into()),
                    surreal_db: std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into()),
                    ..Default::default()
                };

                // Best-effort: persist using the db writer helper
                if let Err(e) = db_writer::persist_repo_dependencies(
                    &cfg,
                    &repo_name,
                    &event.git_url,
                    owner.as_deref(),
                    event.branch.as_deref(),
                    event.last_commit_sha.as_deref(),
                    event.last_indexed_at,
                    &deps_found,
                )
                .await
                {
                    log::warn!(
                        "Failed to persist normalized dependencies via db_writer: {}",
                        e
                    );
                }

                // Run blocking index build off the tokio runtime
                // Run indexing and DB writes; return payloads for optional embed enqueue
                match tokio::task::spawn_blocking(move || -> Result<Vec<EntityPayload>, anyhow::Error> {
                    let (mut svc, stats) = RepoIndexService::build(temp_dir_clone.as_path())?;
                    // Compute pagerank (may be cheap) and surface stats
                    svc.compute_pagerank();
                    log::info!(
                        "Completed ingestion for {}: files_indexed={} entities_indexed={}",
                        repo_name,
                        stats.files_indexed,
                        stats.entities_indexed
                    );

                    // Helper: normalize git_url to an https base (strip .git, convert ssh forms)
                    fn normalize_git_url(git_url: &str) -> Option<String> {
                        if git_url.is_empty() {
                            return None;
                        }
                        // Handle SSH style: git@github.com:owner/repo.git -> https://github.com/owner/repo
                        if git_url.starts_with("git@") {
                            // git@host:owner/repo.git
                            if let Some(colon) = git_url.find(':') {
                                let host = &git_url[4..colon];
                                let rest = &git_url[colon + 1..];
                                let rest = rest.strip_suffix(".git").unwrap_or(rest);
                                return Some(format!("https://{}/{}", host, rest));
                            }
                        }

                        // Handle http/https
                        if git_url.starts_with("http://") || git_url.starts_with("https://") {
                            let s = git_url.trim_end_matches(".git");
                            return Some(s.to_string());
                        }

                        // Fallback: try to treat as path-like and prefix https://
                        let s = git_url.trim_end_matches(".git");
                        Some(format!("https://{}", s))
                    }

                    // Extract EntityPayload from the service for persistence
                    // Helper: extract full text for a span from a file (1-based inclusive line numbers)
                    fn slice_file_lines(path: &str, start1: Option<u32>, end1: Option<u32>) -> Option<String> {
                        let src = std::fs::read_to_string(path).ok()?;
                        let lines: Vec<&str> = src.lines().collect();
                        if lines.is_empty() {
                            return Some(String::new());
                        }
                        // Convert 1-based inclusive to 0-based
                        let s0 = start1.unwrap_or(0).saturating_sub(1) as usize;
                        let e0 = end1.unwrap_or(0).saturating_sub(1) as usize;
                        let s = s0.min(lines.len().saturating_sub(1));
                        let e = e0.min(lines.len().saturating_sub(1));
                        if s > e { return Some(String::new()); }
                        Some(lines[s..=e].join("\n"))
                    }

                    // Progress logging interval (entities) configurable via env; default 5000
                    let progress_interval: usize = std::env::var("HZ_INDEX_PROGRESS_INTERVAL")
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(5000);
                    let total_entities = svc.entities.len();
                    log::info!(
                        "payload build starting: repo={} total_entities={} progress_interval={}",
                        repo_name,
                        total_entities,
                        progress_interval
                    );
                    use std::collections::HashMap;
                    let mut file_cache: HashMap<u32, String> = HashMap::new();
                    let mut payloads: Vec<EntityPayload> = Vec::with_capacity(total_entities);
                    for (idx, ent) in svc.entities.iter().enumerate() {
                        let file = &svc.files[ent.file_id as usize];
                        let mut imports: Vec<ImportItem> = Vec::new();
                        let mut unresolved_imports: Vec<UnresolvedImport> = Vec::new();
                        let mut calls: Vec<String> = Vec::new();
                        if matches!(ent.kind, EntityKind::File) {
                            if let Some(edge_list) = svc.import_edges.get(ent.id as usize) {
                                let lines = svc.import_lines.get(ent.id as usize);
                                for (i, &target_eid) in edge_list.iter().enumerate() {
                                    if let Some(target_ent) = svc.entities.get(target_eid as usize) {
                                        let target_file_idx = target_ent.file_id as usize;
                                        if let Some(target_file) = svc.files.get(target_file_idx) {
                                            let line_no = lines
                                                .and_then(|l| l.get(i))
                                                .cloned()
                                                .unwrap_or(0)
                                                .saturating_add(1);
                                            let raw_path = target_file.path.clone();
                                            let repo_rel = crate::repo_index::indexer::payload::compute_repo_relative(&raw_path, &repo_name);
                                            imports.push(ImportItem { path: repo_rel, line: line_no });
                                        }
                                    }
                                }
                            }
                            if let Some(unres) = svc.unresolved_imports.get(ent.file_id as usize) {
                                for (m, lineno) in unres {
                                    unresolved_imports.push(UnresolvedImport { module: m.clone(), line: lineno.saturating_add(1) });
                                }
                            }
                        }
                        let (start_field, end_field) = if matches!(ent.kind, EntityKind::File) {
                            let has_imports = !imports.is_empty();
                            let has_unresolved = !unresolved_imports.is_empty();
                            if has_imports || has_unresolved {
                                (Some(ent.start_line.saturating_add(1)), Some(ent.end_line.saturating_add(1)))
                            } else {
                                (None, None)
                            }
                        } else {
                            (Some(ent.start_line.saturating_add(1)), Some(ent.end_line.saturating_add(1)))
                        };
                        let project = std::env::var("SURREAL_PROJECT").unwrap_or_else(|_| "hyperzoekt-indexer".into());
                        let repo = repo_name.clone();
                        let branch = std::env::var("SURREAL_BRANCH").unwrap_or_else(|_| { event.branch.clone().unwrap_or_else(|| "main".into()) });
                        let commit = std::env::var("SURREAL_COMMIT").unwrap_or_else(|_| "unknown-commit".into());
                        let stable_id = crate::utils::generate_stable_id(&project, &repo, &branch, &commit, &file.path, &ent.name, &ent.signature);
                        let branch_ref = event.branch.clone().unwrap_or_else(|| "main".into());
                        let source_base = normalize_git_url(&event.git_url);
                        if let Some(ref g) = Some(event.git_url.clone()) {
                            log::debug!("source_url_debug: git_url='{}' owner='{:?}' repo='{}' branch='{}'", g, owner_clone, repo_name, branch_ref);
                        }
                        if let Some(ref base) = source_base { log::debug!("source_url_debug: normalized_base='{}'", base); } else { log::debug!("source_url_debug: normalized_base is None; no source_url will be set"); }
                        let computed_source = source_base.map(|base| {
                            let rel_path = match std::path::Path::new(&file.path).strip_prefix(&temp_dir_clone) { Ok(p) => p.to_string_lossy().to_string(), Err(_) => file.path.trim_start_matches('/').to_string() };
                            let rel = rel_path.trim_start_matches('/');
                            let url = format!("{}/blob/{}/{}", base.trim_end_matches('/'), branch_ref, rel);
                            log::debug!("source_url_debug: computed source_url='{}'", url);
                            url
                        });
                        // caching: read file content once per file id for repeated line slicing
                        fn cached_slice(file_cache: &mut std::collections::HashMap<u32, String>, file_id: u32, path: &str, start1: Option<u32>, end1: Option<u32>) -> Option<String> {
                            use std::collections::hash_map::Entry;
                            // Use the entry API to avoid a potential unwrap after insert.
                            // If reading the file fails, keep behavior of returning None.
                            let content_ref: &String = match file_cache.entry(file_id) {
                                Entry::Occupied(o) => o.into_mut(),
                                Entry::Vacant(v) => {
                                    let s = std::fs::read_to_string(path).ok()?;
                                    v.insert(s)
                                }
                            };
                            let content = content_ref;
                            if content.is_empty() { return Some(String::new()); }
                            let lines: Vec<&str> = content.lines().collect();
                            if lines.is_empty() { return Some(String::new()); }
                            let s0 = start1.unwrap_or(0).saturating_sub(1) as usize;
                            let e0 = end1.unwrap_or(0).saturating_sub(1) as usize;
                            let s = s0.min(lines.len().saturating_sub(1));
                            let e = e0.min(lines.len().saturating_sub(1));
                            if s > e { return Some(String::new()); }
                            Some(lines[s..=e].join("\n"))
                        }
                        let ent_source_content = if !matches!(ent.kind, EntityKind::File) { cached_slice(&mut file_cache, ent.file_id, &file.path, start_field, end_field) } else { None };
                        let methods_with_src: Vec<crate::repo_index::indexer::payload::MethodItem> = ent.methods.iter().map(|mi| {
                            let start1 = mi.start_line.map(|v| v.saturating_add(1));
                            let end1 = mi.end_line.map(|v| v.saturating_add(1));
                            let src_txt = cached_slice(&mut file_cache, ent.file_id, &file.path, start1, end1);
                            crate::repo_index::indexer::payload::MethodItem { name: mi.name.clone(), visibility: mi.visibility.clone(), signature: mi.signature.clone(), start_line: mi.start_line, end_line: mi.end_line, source_content: src_txt }
                        }).collect();
                        if !matches!(ent.kind, EntityKind::File) {
                            if let Some(edges) = svc.call_edges.get(ent.id as usize) {
                                for &callee_eid in edges { if let Some(callee_ent) = svc.entities.get(callee_eid as usize) { calls.push(callee_ent.name.clone()); } }
                            }
                        }
                        payloads.push(EntityPayload { language: file.language.clone(), kind: ent.kind.as_str().to_string(), name: ent.name.clone(), parent: ent.parent.clone(), signature: ent.signature.clone(), start_line: start_field, end_line: end_field, doc: ent.doc.clone(), rank: Some(ent.rank), imports, unresolved_imports, methods: methods_with_src, stable_id, repo_name: repo_name.clone(), source_url: computed_source, source_display: None, calls, source_content: ent_source_content });
                        if progress_interval > 0 && (idx + 1) % progress_interval == 0 {
                            log::info!("payload build progress: repo={} {}/{} ({:.2}%)", repo_name, idx + 1, total_entities, ((idx + 1) as f64 * 100.0 / total_entities as f64));
                        }
                    }
                    log::info!("payload build complete: repo={} total_entities={}", repo_name, total_entities);

                    // Compute commit/snapshot metadata early so the DB writer can
                    // populate `entity_snapshot` mapping rows while persisting entities.
                    let mut maybe_commit: Option<String> = None;
                    let mut maybe_snapshot_id: Option<String> = None;
                    match compute_repo_snapshot_metadata(&temp_dir_clone, event.last_commit_sha.as_deref()) {
                        Ok(meta) => {
                            maybe_commit = Some(meta.commit_id.clone());
                            maybe_snapshot_id = Some(format!("snapshot:{}:{}", repo_name, meta.commit_id));
                        }
                        Err(e) => {
                            log::warn!("Failed to compute repo snapshot metadata before DB write for {}: {}", repo_name, e);
                        }
                    }

                    // Configure and spawn DB writer, providing snapshot context when available
                    let surreal_url = std::env::var("SURREALDB_URL").ok();
                    let surreal_username = std::env::var("SURREALDB_USERNAME").ok();
                    let surreal_password = std::env::var("SURREALDB_PASSWORD").ok();
                    let surreal_ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
                    let surreal_db = std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());

                    let mut db_cfg = db_writer::DbWriterConfig {
                        surreal_url,
                        surreal_username,
                        surreal_password,
                        surreal_ns,
                        surreal_db,
                        ..Default::default()
                    };
                    db_cfg.snapshot_id = maybe_snapshot_id.clone();
                    db_cfg.commit_id = maybe_commit.clone();

                    let (tx, db_join) = db_writer::spawn_db_writer(payloads.clone(), db_cfg, None)?;

                    // Chunk payloads so writer processes multiple batches when large.
                    let chunk_size = 500; // keep in sync with default batch_capacity
                    for ch in payloads.chunks(chunk_size) {
                        if let Err(e) = tx.send(ch.to_vec()) {
                            log::error!("Failed to send payload chunk (size={}) to DB writer: {}", ch.len(), e);
                            break;
                        } else {
                            log::debug!("sent payload chunk size {}", ch.len());
                        }
                    }
                    // Drop sender so writer can detect channel close and exit its loop naturally.
                    drop(tx);

                    // Wait for DB writer to finish after all chunks processed.
                    match db_join.join() {
                        Ok(Ok(())) => {
                            log::info!(
                                "Successfully persisted {} entities for {}",
                                stats.entities_indexed,
                                repo_name
                            );
                            // After persistence, record a commit and snapshot metadata in SurrealDB.
                            // Use the existing db_writer connection helpers.
                            let surreal_url = std::env::var("SURREALDB_URL").ok();
                            let surreal_username = std::env::var("SURREALDB_USERNAME").ok();
                            let surreal_password = std::env::var("SURREALDB_PASSWORD").ok();
                            let surreal_ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
                            let surreal_db = std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());
                            // We are in a blocking thread — create a small Tokio runtime to perform async SurrealDB calls.
                            match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                                Ok(rt2) => {
                                    let snapshot_dir = temp_dir_clone.clone();
                                    let res = rt2.block_on(async move {
                                        match db_writer::connection::connect(&surreal_url, &surreal_username, &surreal_password, &surreal_ns, &surreal_db).await {
                                            Ok(conn) => {
                                                // Compute git-derived metadata (commit id, parents, tree, author, message, size)
                                                match compute_repo_snapshot_metadata(&snapshot_dir, event.last_commit_sha.as_deref()) {
                                                    Ok(meta) => {
                                                        // Convert parents Vec<String> -> Vec<&str> for call
                                                        let parents_refs: Vec<&str> = meta.parents.iter().map(|s| s.as_str()).collect();
                                                        if let Err(e) = db_writer::create_commit(&conn, &meta.commit_id, &repo_name, &parents_refs, meta.tree.as_deref(), meta.author.as_deref(), meta.message.as_deref()).await {
                                                            log::warn!("Failed to record commit {} for repo {}: {}", meta.commit_id, repo_name, e);
                                                        } else {
                                                            log::info!("Recorded commit {} for repo {}", meta.commit_id, repo_name);
                                                        }
                                                        // Create snapshot metadata pointing to the snapshot id.
                                                        let snapshot_id = format!("snapshot:{}:{}", repo_name, meta.commit_id);
                                                        let size_opt = Some(meta.size_bytes);
                                                        if let Err(e) = db_writer::create_snapshot_meta(&conn, &repo_name, &meta.commit_id, &snapshot_id, size_opt).await {
                                                            log::warn!("Failed to create snapshot_meta {} for repo {} commit {}: {}", snapshot_id, repo_name, meta.commit_id, e);
                                                        } else {
                                                            log::info!("Created snapshot_meta {} -> commit {}", snapshot_id, meta.commit_id);
                                                        }
                                                    }
                                                    Err(e) => {
                                                        log::warn!("Failed to compute repo snapshot metadata for {}: {}", repo_name, e);
                                                    }
                                                }
                                                Ok(())
                                            }
                                            Err(e) => {
                                                log::warn!("Failed to connect to SurrealDB to persist commit/snapshot metadata: {}", e);
                                                Err(e)
                                            }
                                        }
                                    });
                                    if let Err(e) = res {
                                        log::debug!("commit/snapshot metadata runtime error: {:?}", e);
                                    }
                                }
                                Err(e) => {
                                    log::warn!("Failed to create temp runtime to persist commit/snapshot metadata: {}", e);
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            log::error!("DB writer failed for {}: {}", repo_name, e);
                        }
                        Err(e) => {
                            log::error!("DB writer thread panicked for {}: {:?}", repo_name, e);
                        }
                    }

                    // Clean up the temporary directory
                    if let Err(e) = std::fs::remove_dir_all(&temp_dir_clone) {
                        log::warn!(
                            "Failed to clean up temp directory {}: {}",
                            temp_dir_clone.display(),
                            e
                        );
                    } else {
                        log::debug!(
                            "Cleaned up temporary directory: {}",
                            temp_dir_clone.display()
                        );
                    }

                    Ok(payloads)
                })
                .await
                {
                    Ok(Ok(payloads)) => {
                        log::info!(
                            "Successfully processed indexing_finished for {}",
                            event.repo_name
                        );
                        // Emit a clear, always-on info line showing the embedding gate decision so
                        // operators do not have to rely on a missing debug log to infer why jobs
                        // were (not) enqueued. This avoids confusion when RUST_LOG excludes debug.
                        let embed_env = std::env::var("HZ_ENABLE_EMBED_JOBS").unwrap_or_default();
                        let will_enqueue = Self::embed_jobs_enabled();
                        log::info!(
                            "embedding enqueue gate: repo={} HZ_ENABLE_EMBED_JOBS='{}' payloads={} will_enqueue={}",
                            event.repo_name,
                            embed_env,
                            payloads.len(),
                            will_enqueue
                        );
                        // Additional diagnostic summary for repository indexing results.
                        // This helps operators quickly assess what was produced before optional embedding.
                        if !payloads.is_empty() {
                            use std::collections::HashMap;
                            let mut by_kind: HashMap<&str, usize> = HashMap::new();
                            let mut with_source = 0usize;
                            let mut with_imports = 0usize;
                            let mut with_methods = 0usize;
                            for p in &payloads {
                                *by_kind.entry(p.kind.as_str()).or_default() += 1;
                                if p.source_content.as_ref().map(|s| !s.is_empty()).unwrap_or(false) { with_source += 1; }
                                if !p.imports.is_empty() { with_imports += 1; }
                                if !p.methods.is_empty() { with_methods += 1; }
                            }
                            // Build compact kind summary like: func=120,class=33,file=10
                            let mut kind_pairs: Vec<String> = by_kind.iter().map(|(k,v)| format!("{}={}", k, v)).collect();
                            kind_pairs.sort();
                            let kind_summary = kind_pairs.join(",");
                            let sample: Vec<&str> = payloads.iter().take(5).map(|p| p.stable_id.as_str()).collect();
                            log::info!(
                                "Index summary repo={} total_entities={} kinds=[{}] with_source={} with_imports={} with_methods={} sample_stable_ids={:?}",
                                event.repo_name,
                                payloads.len(),
                                kind_summary,
                                with_source,
                                with_imports,
                                with_methods,
                                sample
                            );
                            // Structured event to help attribute which indexer produced these entities
                            log::info!(
                                "event=HYPERZOEKT_INDEX_COMPLETE repo={} total_entities={} kinds={} with_source={} with_imports={} with_methods={} sample_stable_ids={:?}",
                                event.repo_name,
                                payloads.len(),
                                kind_summary,
                                with_source,
                                with_imports,
                                with_methods,
                                sample
                            );
                        } else {
                            log::warn!("Index produced zero entities for repo={}; skipping embedding enqueue", event.repo_name);
                        }
                        // Optionally enqueue embedding jobs to Redis
                        // Legacy post-all enqueue removed: per-batch enqueue now handled in db_writer after each batch persistence.
                        if Self::embed_jobs_enabled() { log::info!("per-batch embedding enqueue active (post-persist)"); } else { log::debug!("Embedding jobs disabled (HZ_ENABLE_EMBED_JOBS not set)"); }

                        // Final completion marker (stderr + info) so operators can reliably detect end-of-repo lifecycle.
                        log::info!(
                            "INDEX_DONE repo={} total_entities={} ts={}",
                            event.repo_name,
                            payloads.len(),
                            chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                        );
                        log::info!(
                            "index_done repo={} total_entities={} (all batches persisted and embed jobs enqueued)",
                            event.repo_name,
                            payloads.len()
                        );
                    }
                    Ok(Err(e)) => {
                        log::error!("Indexer failed for {}: {}", event.repo_name, e);
                        log::error!("Indexer failed for {}: {}", event.repo_name, e);
                        // Clean up temp directory on error too
                        if let Err(cleanup_err) =
                            tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&temp_dir))
                                .await
                        {
                            log::warn!(
                                "Failed to clean up temp directory on error: {:?}",
                                cleanup_err
                            );
                        }
                    }
                    Err(e) => {
                        log::error!("Indexer task panicked for {}: {:?}", event.repo_name, e);
                        log::error!("Indexer panicked for {}: {:?}", event.repo_name, e);
                        // Clean up temp directory on panic too
                        if let Err(cleanup_err) =
                            tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&temp_dir))
                                .await
                        {
                            log::warn!(
                                "Failed to clean up temp directory on panic: {:?}",
                                cleanup_err
                            );
                        }
                    }
                }
            }
            _ => {
                log::warn!("Unknown event type: {}", event.event_type);
            }
        }

        Ok(())
    }

    /// Clone a repository to a temporary directory
    async fn clone_repository(event: &RepoEvent) -> Result<PathBuf, anyhow::Error> {
        // Sanitize repo name to prevent path traversal attacks
        let sanitized_repo_name = event
            .repo_name
            .chars()
            .map(|c| match c {
                '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
                c if c.is_control() => '_',
                other => other,
            })
            .collect::<String>();

        // Create a temporary directory for the clone
        let temp_dir = std::env::temp_dir().join("hyperzoekt-clones").join(format!(
            "{}-{}",
            sanitized_repo_name,
            Uuid::new_v4()
        ));

        // Ensure parent directory exists
        if let Some(parent) = temp_dir.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Remove temp directory if it already exists (cleanup from previous failed run)
        if temp_dir.exists() {
            tokio::fs::remove_dir_all(&temp_dir).await?;
        }

        log::debug!(
            "Cloning {} from {} to {}",
            sanitized_repo_name,
            event.git_url,
            temp_dir.display()
        );

        // Clone the repository (prefer a shallow CLI clone for speed and low bandwidth).
        // This runs in a blocking task.
        let git_url = event.git_url.clone();
        let branch = event.branch.clone();
        let temp_dir_clone = temp_dir.clone();

        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            // Helper to inject credentials into HTTPS URLs similar to zoekt-rs behavior
            fn inject_credentials(url: &str) -> String {
                if !url.starts_with("https://") {
                    return url.to_string();
                }
                // Detect platform and get credentials
                let (username_env, token_env) = if url.contains("github.com") {
                    ("GITHUB_USERNAME", "GITHUB_TOKEN")
                } else if url.contains("gitlab.com") {
                    ("GITLAB_USERNAME", "GITLAB_TOKEN")
                } else if url.contains("bitbucket.org") {
                    ("BITBUCKET_USERNAME", "BITBUCKET_TOKEN")
                } else {
                    return url.to_string();
                };
                let username = std::env::var(username_env).ok();
                let token = std::env::var(token_env).ok();
                if let (Some(user), Some(tok)) = (username, token) {
                    if let Some(host_start) = url.find("://") {
                        let host_path = &url[host_start + 3..];
                        if let Some(slash_pos) = host_path.find('/') {
                            let host = &host_path[..slash_pos];
                            let path = &host_path[slash_pos..];
                            return format!("https://{}:{}@{}{}", user, tok, host, path);
                        } else {
                            return format!("https://{}:{}@{}", user, tok, host_path);
                        }
                    }
                }
                url.to_string()
            }

            // 1) Try shallow libgit2 clone (preferred)
            let mut libgit2_shallow_ok = false;
            {
                let mut fo = git2::FetchOptions::new();
                // Depth is supported on this git2 version; attempt a shallow fetch
                fo.depth(1);

                let mut callbacks = git2::RemoteCallbacks::new();
                if let Ok(username) = std::env::var("GIT_USERNAME") {
                    if let Ok(password) = std::env::var("GIT_PASSWORD") {
                        let user = username.clone();
                        let pass = password.clone();
                        callbacks.credentials(move |_url, _username_from_url, _allowed_types| {
                            git2::Cred::userpass_plaintext(&user, &pass)
                        });
                    }
                }
                fo.remote_callbacks(callbacks);

                let mut rb = git2::build::RepoBuilder::new();
                rb.fetch_options(fo);
                if let Some(ref br) = branch {
                    rb.branch(br);
                }
                if rb.clone(&git_url, &temp_dir_clone).is_ok() {
                    libgit2_shallow_ok = true;
                }
            }

            if libgit2_shallow_ok {
                return Ok(());
            }

            // 2) Fallback to shallow CLI clone
            let mut cli_url = git_url.clone();
            if cli_url.starts_with("https://") {
                cli_url = inject_credentials(&cli_url);
            }

            let mut args: Vec<String> = vec![
                "clone".into(),
                "--depth".into(),
                "1".into(),
                cli_url.clone(),
            ];
            if let Some(ref br) = branch {
                args.push("--branch".into());
                args.push(br.clone());
            }
            args.push(temp_dir_clone.to_string_lossy().to_string());

            let clone_result = std::process::Command::new("git")
                .args(&args)
                .env("GIT_TERMINAL_PROMPT", "0")
                .output();

            match clone_result {
                Ok(output) if output.status.success() => return Ok(()),
                Ok(output) => {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    log::warn!("shallow git CLI clone failed: {}", stderr);
                }
                Err(e) => {
                    log::warn!("failed to spawn git clone CLI: {}", e);
                }
            }

            // 3) Final fallback: full libgit2 clone with credentials
            let mut full_fo = git2::FetchOptions::new();
            let mut callbacks = git2::RemoteCallbacks::new();
            if let Ok(username) = std::env::var("GIT_USERNAME") {
                if let Ok(password) = std::env::var("GIT_PASSWORD") {
                    let user = username.clone();
                    let pass = password.clone();
                    callbacks.credentials(move |_url, _username_from_url, _allowed_types| {
                        git2::Cred::userpass_plaintext(&user, &pass)
                    });
                }
            }
            full_fo.remote_callbacks(callbacks);

            let mut repo_builder = git2::build::RepoBuilder::new();
            repo_builder.fetch_options(full_fo);
            if let Some(ref br) = branch {
                repo_builder.branch(br);
            }

            let _repo = repo_builder.clone(&git_url, &temp_dir_clone)?;
            Ok(())
        })
        .await??;

        log::debug!(
            "Successfully cloned {} to {}",
            sanitized_repo_name,
            temp_dir.display()
        );
        Ok(temp_dir)
    }
}

impl EventProcessor {
    fn embed_jobs_enabled() -> bool {
        let v = std::env::var("HZ_ENABLE_EMBED_JOBS").unwrap_or_default();
        matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES")
    }

    async fn enqueue_embedding_jobs(
        repo_name: &str,
        payloads: &[EntityPayload],
    ) -> Result<usize, anyhow::Error> {
        let pool_opt = zoekt_distributed::redis_adapter::create_redis_pool();
        if pool_opt.is_none() {
            log::warn!(
                "Redis pool not available when attempting to enqueue embedding jobs for repo={}; check REDIS_URL/credentials",
                repo_name
            );
            return Err(anyhow::anyhow!(
                "Redis not configured (REDIS_URL/REDIS_USERNAME/REDIS_PASSWORD)"
            ));
        }
        let pool = pool_opt.unwrap();

        #[derive(serde::Serialize)]
        struct EmbeddingJob<'a> {
            stable_id: &'a str,
            repo_name: &'a str,
            language: &'a str,
            kind: &'a str,
            name: &'a str,
            source_url: Option<&'a str>,
        }

        let queue_key =
            std::env::var("HZ_EMBED_JOBS_QUEUE").unwrap_or_else(|_| "zoekt:embed_jobs".to_string());

        let mut jobs: Vec<String> = Vec::with_capacity(payloads.len());
        for p in payloads {
            let job = EmbeddingJob {
                stable_id: &p.stable_id,
                repo_name,
                language: &p.language,
                kind: &p.kind,
                name: &p.name,
                source_url: p.source_url.as_deref(),
            };
            log::debug!("Enqueuing embed job for stable_id={}", p.stable_id);
            match serde_json::to_string(&job) {
                Ok(s) => jobs.push(s),
                Err(e) => {
                    log::warn!(
                        "Skipping embed job for stable_id={} due to serialization error: {}",
                        p.stable_id,
                        e
                    );
                }
            }
        }

        let mut conn = pool.get().await?;
        if jobs.is_empty() {
            log::debug!(
                "No embed jobs to enqueue for repo={}; nothing pushed to queue='{}'",
                repo_name,
                queue_key
            );
            log::debug!("Skipping enqueue: no jobs to push for repo={}", repo_name);
            return Ok(0usize);
        }

        log::debug!(
            "Attempting to enqueue {} embed jobs to queue='{}' for repo={}",
            jobs.len(),
            queue_key,
            repo_name
        );

        let mut total = 0usize;
        for chunk in jobs.chunks(500) {
            let _: () =
                deadpool_redis::redis::AsyncCommands::rpush(&mut conn, &queue_key, chunk).await?;
            total += chunk.len();
            log::debug!(
                "Pushed chunk of {} jobs to queue='{}' (repo={})",
                chunk.len(),
                queue_key,
                repo_name
            );
        }
        log::info!("Enqueued {} embedding jobs to '{}'", total, queue_key);
        Ok(total)
    }
}

/// Create and start the event consumer and processor
pub async fn start_event_system() -> Result<tokio::task::JoinHandle<()>, anyhow::Error> {
    start_event_system_with_ttl(300).await // Default to 5 minutes
}

/// Create and start the event consumer and processor with configurable TTL
pub async fn start_event_system_with_ttl(
    processing_ttl_seconds: u64,
) -> Result<tokio::task::JoinHandle<()>, anyhow::Error> {
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

    let consumer = EventConsumer::new(event_tx);
    let processor = EventProcessor::new(event_rx);

    if consumer.is_redis_available() {
        log::info!("Redis is available, starting event consumption");
        consumer
            .start_consuming_with_ttl(processing_ttl_seconds)
            .await?;
    } else {
        log::info!("Redis is not available, event consumption disabled");
    }

    // Start the processor in a separate task and return the join handle
    let processor_handle = tokio::spawn(async move {
        if let Err(e) = processor.start_processing().await {
            log::error!("Event processor failed: {}", e);
        }
    });

    Ok(processor_handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repo_event_deserialize_alias() {
        let json = r#"{"event":"indexing_finished","repo_name":"r","git_url":"file:///tmp","branch":null,"node_id":"n","timestamp":0}"#;
        let e: RepoEvent = serde_json::from_str(json).expect("deserialize");
        assert_eq!(e.event_type, "indexing_finished");
        assert_eq!(e.repo_name, "r");
    }

    #[test]
    fn repo_name_sanitization() {
        // Test that dangerous characters are sanitized
        let test_cases = vec![
            ("../../../etc/passwd", ".._.._.._etc_passwd"),
            ("repo/with/slashes", "repo_with_slashes"),
            ("repo\\with\\backslashes", "repo_with_backslashes"),
            ("repo:with:colons", "repo_with_colons"),
            ("repo*with*asterisks", "repo_with_asterisks"),
            ("repo?with?questions", "repo_with_questions"),
            ("repo\"with\"quotes", "repo_with_quotes"),
            ("repo<with>brackets", "repo_with_brackets"),
            ("repo|with|pipes", "repo_with_pipes"),
            ("normal-repo", "normal-repo"),
        ];

        for (input, expected) in test_cases {
            let sanitized = input
                .chars()
                .map(|c| match c {
                    '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
                    c if c.is_control() => '_',
                    c => c,
                })
                .collect::<String>();
            assert_eq!(sanitized, expected, "Failed to sanitize: {}", input);
        }
    }

    #[test]
    fn build_redis_url_from_env_tests() {
        // Helper to clear relevant env vars
        fn clear_env() {
            std::env::remove_var("REDIS_URL");
            std::env::remove_var("REDIS_USERNAME");
            std::env::remove_var("REDIS_PASSWORD");
        }

        // Helper to temporarily unset TEST_REDIS_DB/REDIS_DB for assertions that
        // expect no DB injection. Returns previous values so they can be
        // restored.
        fn unset_test_db_vars() -> (Option<String>, Option<String>) {
            let prev_test = std::env::var("TEST_REDIS_DB").ok();
            let prev_db = std::env::var("REDIS_DB").ok();
            std::env::remove_var("TEST_REDIS_DB");
            std::env::remove_var("REDIS_DB");
            (prev_test, prev_db)
        }

        fn restore_test_db_vars(prev: (Option<String>, Option<String>)) {
            if let Some(v) = prev.0 {
                std::env::set_var("TEST_REDIS_DB", v);
            } else {
                std::env::remove_var("TEST_REDIS_DB");
            }
            if let Some(v) = prev.1 {
                std::env::set_var("REDIS_DB", v);
            } else {
                std::env::remove_var("REDIS_DB");
            }
        }

        clear_env();

        // 1) No env -> None
        // Ensure TEST_REDIS_DB/REDIS_DB are not present for this assertion.
        let prev = unset_test_db_vars();
        assert_eq!(EventConsumer::build_redis_url_from_env(), None);
        restore_test_db_vars(prev);

        // 2) Password-only -> redis://:password@127.0.0.1:6379
        clear_env();
        // ensure TEST_REDIS_DB/REDIS_DB do not affect this assertion
        let prev = unset_test_db_vars();
        std::env::set_var("REDIS_PASSWORD", "p");
        assert_eq!(
            EventConsumer::build_redis_url_from_env(),
            Some("redis://:p@127.0.0.1:6379".to_string())
        );
        restore_test_db_vars(prev);

        // 3) Username+password -> redis://u:p@127.0.0.1:6379
        clear_env();
        std::env::set_var("REDIS_USERNAME", "u");
        std::env::set_var("REDIS_PASSWORD", "p");
        let got = EventConsumer::build_redis_url_from_env().unwrap();
        assert!(got.starts_with("redis://u:p@"));

        // 4) REDIS_URL full with auth preserved (normalize trailing slash)
        clear_env();
        std::env::set_var("REDIS_URL", "redis://a:b@host:6380");
        let got_full = EventConsumer::build_redis_url_from_env().unwrap();
        assert_eq!(got_full.trim_end_matches('/'), "redis://a:b@host:6380");

        // 5) REDIS_URL without auth + credentials injected
        clear_env();
        std::env::set_var("REDIS_URL", "redis://host:6380");
        std::env::set_var("REDIS_USERNAME", "x");
        std::env::set_var("REDIS_PASSWORD", "y");
        let got2 = EventConsumer::build_redis_url_from_env().unwrap();
        assert!(got2.starts_with("redis://x:y@host:6380"));

        // 6) REDIS_URL as host:port (not full URL)
        clear_env();
        std::env::set_var("REDIS_URL", "host:7000");
        std::env::set_var("REDIS_PASSWORD", "z");
        let got3 = EventConsumer::build_redis_url_from_env().unwrap();
        assert!(got3.contains("host:7000") && got3.contains("z"));

        clear_env();
    }

    #[test]
    fn compute_repo_snapshot_metadata_smoke() {
        // Create a temporary directory and init a git repo with one file and a commit
        let td = tempfile::tempdir().expect("tempdir");
        let repo_path = td.path().to_path_buf();
        // Init repo
        let repo = git2::Repository::init(&repo_path).expect("init repo");
        // Create a file
        let file_path = repo_path.join("README.md");
        std::fs::write(&file_path, "hello world\n").expect("write file");
        // Add and commit
        let mut index = repo.index().expect("index");
        index
            .add_path(std::path::Path::new("README.md"))
            .expect("add");
        index.write().expect("index write");
        let tree_id = index.write_tree().expect("write tree");
        let tree = repo.find_tree(tree_id).expect("find tree");
        let sig = git2::Signature::now("Test User", "test@example.com").expect("sig");
        let oid = repo
            .commit(Some("HEAD"), &sig, &sig, "initial commit", &tree, &[])
            .expect("commit");
        // Call helper
        let meta =
            compute_repo_snapshot_metadata(&repo_path, Some(&oid.to_string())).expect("compute");
        assert_eq!(meta.commit_id, oid.to_string());
        assert!(meta.parents.is_empty());
        assert!(meta.tree.is_some());
        assert!(meta.author.is_some());
        assert_eq!(meta.message.unwrap_or_default(), "initial commit");
        assert!(meta.size_bytes > 0);
    }
}
