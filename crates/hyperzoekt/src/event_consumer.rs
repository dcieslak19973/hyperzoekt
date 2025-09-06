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
use crate::repo_index::indexer::payload::{EntityPayload, ImportItem, UnresolvedImport};
use crate::repo_index::indexer::EntityKind;
use crate::repo_index::RepoIndexService;
use deadpool_redis::redis::{AsyncCommands, Client};
use deadpool_redis::{Config as RedisConfig, Pool};
use log;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use zoekt_distributed::lease_manager::RepoEvent;

/// Event consumer that subscribes to zoekt-distributed repo events
pub struct EventConsumer {
    redis_pool: Option<Pool>,
    event_tx: UnboundedSender<RepoEvent>,
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
        match std::env::var("REDIS_URL").ok() {
            Some(mut url) => {
                // Check if URL already has authentication
                let has_auth = url.contains('@');

                // If no auth in URL, try to add it from environment variables
                if !has_auth {
                    if let Ok(username) = std::env::var("REDIS_USERNAME") {
                        if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                            // Insert username:password@ after redis://
                            if let Some(pos) = url.find("://") {
                                let auth_part = format!("{}:{}@", username, password);
                                url.insert_str(pos + 3, &auth_part);
                            }
                        } else {
                            // Username without password
                            if let Some(pos) = url.find("://") {
                                let auth_part = format!("{}@", username);
                                url.insert_str(pos + 3, &auth_part);
                            }
                        }
                    } else if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                        // Password without username
                        if let Some(pos) = url.find("://") {
                            let auth_part = format!(":{}@", password);
                            url.insert_str(pos + 3, &auth_part);
                        }
                    }
                }

                RedisConfig::from_url(&url).create_pool(None).ok()
            }
            None => {
                // No REDIS_URL provided, but check if we have auth credentials to construct one
                if let (Ok(username), Ok(password)) = (
                    std::env::var("REDIS_USERNAME"),
                    std::env::var("REDIS_PASSWORD"),
                ) {
                    let url = format!("redis://{}:{}@127.0.0.1:6379", username, password);
                    RedisConfig::from_url(&url).create_pool(None).ok()
                } else if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                    let url = format!("redis://:{}@127.0.0.1:6379", password);
                    RedisConfig::from_url(&url).create_pool(None).ok()
                } else {
                    None
                }
            }
        }
    }

    /// Create a direct Redis client for pubsub operations
    fn _create_redis_client() -> Result<Client, anyhow::Error> {
        match std::env::var("REDIS_URL").ok() {
            Some(mut url) => {
                // Check if URL already has authentication
                let has_auth = url.contains('@');

                // If no auth in URL, try to add it from environment variables
                if !has_auth {
                    if let Ok(username) = std::env::var("REDIS_USERNAME") {
                        if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                            // Insert username:password@ after redis://
                            if let Some(pos) = url.find("://") {
                                let auth_part = format!("{}:{}@", username, password);
                                url.insert_str(pos + 3, &auth_part);
                            }
                        } else {
                            // Username without password
                            if let Some(pos) = url.find("://") {
                                let auth_part = format!("{}@", username);
                                url.insert_str(pos + 3, &auth_part);
                            }
                        }
                    } else if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                        // Password without username
                        if let Some(pos) = url.find("://") {
                            let auth_part = format!(":{}@", password);
                            url.insert_str(pos + 3, &auth_part);
                        }
                    }
                }

                Client::open(url)
            }
            None => {
                // No REDIS_URL provided, but check if we have auth credentials to construct one
                if let (Ok(username), Ok(password)) = (
                    std::env::var("REDIS_USERNAME"),
                    std::env::var("REDIS_PASSWORD"),
                ) {
                    let url = format!("redis://{}:{}@127.0.0.1:6379", username, password);
                    Client::open(url)
                } else if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                    let url = format!("redis://:{}@127.0.0.1:6379", password);
                    Client::open(url)
                } else {
                    Client::open("redis://127.0.0.1:6379")
                }
            }
        }
        .map_err(|e| anyhow::anyhow!("Failed to create Redis client: {}", e))
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

        log::info!(
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
                log::info!("Processing message: {}", event_json);

                // Set TTL on processing key for failure recovery
                let _: () = conn
                    .expire(&processing_key, processing_ttl_seconds as i64)
                    .await?;

                match serde_json::from_str::<RepoEvent>(&event_json) {
                    Ok(event) => {
                        // Send event to processor
                        if event_tx.send(event).is_err() {
                            log::warn!("Event channel closed, stopping consumer");
                            return Ok(());
                        }

                        // Message processed successfully, remove from processing queue
                        let _: () = conn.del(&processing_key).await?;
                        log::info!("Successfully processed and acknowledged message");
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

    /// Process a single repo event
    async fn process_event(&self, event: RepoEvent) -> Result<(), anyhow::Error> {
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

                // Add prominent log when starting repository processing
                log::info!(
                    "🚀 Starting repository indexing for {} (branch: {:?}) from node {}",
                    event.repo_name,
                    event.branch,
                    event.node_id
                );

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

                // Run blocking index build off the tokio runtime
                match tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
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
                    let payloads: Vec<EntityPayload> = svc
                        .entities
                        .iter()
                        .map(|ent| {
                            let file = &svc.files[ent.file_id as usize];
                            let mut imports: Vec<ImportItem> = Vec::new();
                            let mut unresolved_imports: Vec<UnresolvedImport> = Vec::new();
                            if matches!(ent.kind, EntityKind::File) {
                                if let Some(edge_list) = svc.import_edges.get(ent.id as usize) {
                                    let lines = svc.import_lines.get(ent.id as usize);
                                    for (i, &target_eid) in edge_list.iter().enumerate() {
                                        if let Some(target_ent) =
                                            svc.entities.get(target_eid as usize)
                                        {
                                            let target_file_idx = target_ent.file_id as usize;
                                            if let Some(target_file) =
                                                svc.files.get(target_file_idx)
                                            {
                                                let line_no = lines
                                                    .and_then(|l| l.get(i))
                                                    .cloned()
                                                    .unwrap_or(0)
                                                    .saturating_add(1);
                                                imports.push(ImportItem {
                                                    path: target_file.path.clone(),
                                                    line: line_no,
                                                });
                                            }
                                        }
                                    }
                                }
                                if let Some(unres) =
                                    svc.unresolved_imports.get(ent.file_id as usize)
                                {
                                    for (m, lineno) in unres {
                                        unresolved_imports.push(UnresolvedImport {
                                            module: m.clone(),
                                            line: lineno.saturating_add(1),
                                        });
                                    }
                                }
                            }
                            let (start_field, end_field) = if matches!(ent.kind, EntityKind::File) {
                                let has_imports = !imports.is_empty();
                                let has_unresolved = !unresolved_imports.is_empty();
                                if has_imports || has_unresolved {
                                    (
                                        Some(ent.start_line.saturating_add(1)),
                                        Some(ent.end_line.saturating_add(1)),
                                    )
                                } else {
                                    (None, None)
                                }
                            } else {
                                (
                                    Some(ent.start_line.saturating_add(1)),
                                    Some(ent.end_line.saturating_add(1)),
                                )
                            };

                            // Generate stable id for the entity
                            let project = std::env::var("SURREAL_PROJECT")
                                .unwrap_or_else(|_| "hyperzoekt-indexer".into());
                            let repo = repo_name.clone();
                            let branch = std::env::var("SURREAL_BRANCH").unwrap_or_else(|_| {
                                event.branch.clone().unwrap_or_else(|| "main".into())
                            });
                            let commit = std::env::var("SURREAL_COMMIT")
                                .unwrap_or_else(|_| "unknown-commit".into());
                            let stable_id = crate::utils::generate_stable_id(
                                &project,
                                &repo,
                                &branch,
                                &commit,
                                &file.path,
                                &ent.name,
                                &ent.signature,
                            );

                            // Build a source_url from the RepoEvent.git_url and branch, prefer event.branch
                            let branch_ref = event.branch.clone().unwrap_or_else(|| "main".into());
                            let source_base = normalize_git_url(&event.git_url);
                            let computed_source = source_base.map(|base| {
                                // file.path is the repo-relative path; construct blob URL
                                let rel = file.path.trim_start_matches('/');
                                format!(
                                    "{}/blob/{}/{}",
                                    base.trim_end_matches('/'),
                                    branch_ref,
                                    rel
                                )
                            });

                            EntityPayload {
                                file: file.path.clone(),
                                language: file.language.clone(),
                                kind: ent.kind.as_str().to_string(),
                                name: ent.name.clone(),
                                parent: ent.parent.clone(),
                                signature: ent.signature.clone(),
                                start_line: start_field,
                                end_line: end_field,
                                calls: ent.calls.clone(),
                                doc: ent.doc.clone(),
                                rank: ent.rank,
                                imports,
                                unresolved_imports,
                                stable_id,
                                repo_name: repo_name.clone(),
                                source_url: computed_source,
                                source_display: None,
                            }
                        })
                        .collect();

                    // Configure and spawn DB writer
                    let surreal_url = std::env::var("SURREALDB_URL").ok();
                    let surreal_username = std::env::var("SURREALDB_USERNAME").ok();
                    let surreal_password = std::env::var("SURREALDB_PASSWORD").ok();
                    let surreal_ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
                    let surreal_db = std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());

                    let db_cfg = db_writer::DbWriterConfig {
                        channel_capacity: 100,
                        batch_capacity: Some(500),
                        batch_timeout_ms: Some(500),
                        max_retries: Some(3),
                        surreal_url,
                        surreal_username,
                        surreal_password,
                        surreal_ns,
                        surreal_db,
                        initial_batch: false, // Indexer processes events continuously
                    };

                    let (tx, db_join) = db_writer::spawn_db_writer(payloads.clone(), db_cfg)?;

                    // Send payloads to DB writer
                    if let Err(e) = tx.send(payloads) {
                        log::error!("Failed to send payloads to DB writer: {}", e);
                    }

                    // Wait for DB writer to finish
                    match db_join.join() {
                        Ok(Ok(())) => {
                            log::info!(
                                "Successfully persisted {} entities for {}",
                                stats.entities_indexed,
                                repo_name
                            );
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
                        log::info!(
                            "Cleaned up temporary directory: {}",
                            temp_dir_clone.display()
                        );
                    }

                    Ok(())
                })
                .await
                {
                    Ok(Ok(())) => {
                        log::info!(
                            "Successfully processed indexing_finished for {}",
                            event.repo_name
                        );
                    }
                    Ok(Err(e)) => {
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

        log::info!(
            "Cloning {} from {} to {}",
            sanitized_repo_name,
            event.git_url,
            temp_dir.display()
        );

        // Clone the repository (this is blocking, so run in spawn_blocking)
        let git_url = event.git_url.clone();
        let branch = event.branch.clone();
        let temp_dir_clone = temp_dir.clone();

        tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
            // Clone the repository
            let mut clone_options = git2::FetchOptions::new();

            // Add authentication if available
            if let Ok(username) = std::env::var("GIT_USERNAME") {
                if let Ok(password) = std::env::var("GIT_PASSWORD") {
                    let mut callbacks = git2::RemoteCallbacks::new();
                    callbacks.credentials(move |_url, _username_from_url, _allowed_types| {
                        git2::Cred::userpass_plaintext(&username, &password)
                    });
                    clone_options.remote_callbacks(callbacks);
                }
            }

            let mut repo_builder = git2::build::RepoBuilder::new();
            repo_builder.fetch_options(clone_options);

            // Clone to the specified branch if provided
            let should_checkout_default = branch.is_none();
            if let Some(ref branch_name) = branch {
                repo_builder.branch(branch_name);
            }

            let repo = repo_builder.clone(&git_url, &temp_dir_clone)?;

            // If no branch was specified, checkout the default branch
            if should_checkout_default {
                let head = repo.head()?;
                let head_commit = head.peel_to_commit()?;
                let mut checkout_builder = git2::build::CheckoutBuilder::new();
                checkout_builder.force();
                repo.checkout_tree(head_commit.as_object(), Some(&mut checkout_builder))?;
            }

            Ok(())
        })
        .await??;

        log::info!(
            "Successfully cloned {} to {}",
            sanitized_repo_name,
            temp_dir.display()
        );
        Ok(temp_dir)
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
}
