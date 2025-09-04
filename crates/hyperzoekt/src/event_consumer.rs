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

use crate::repo_index::RepoIndexService;
use deadpool_redis::redis::{AsyncCommands, Client};
use deadpool_redis::{Config as RedisConfig, Pool};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;

/// Repository indexing event emitted by zoekt-distributed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoEvent {
    #[serde(alias = "event")]
    pub event_type: String,
    pub repo_name: String,
    pub git_url: String,
    pub branch: Option<String>,
    pub node_id: String,
    pub timestamp: i64,
}

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
                warn!("No Redis pool available for event consumption");
                return Ok(());
            }
        };

        info!("Starting event consumer for zoekt:repo_events channel");

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
                        info!("Event consumption completed unexpectedly");
                        break;
                    }
                    Err(e) => {
                        error!("Event consumption failed: {}", e);
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

        info!(
            "Starting Redis queue consumer: {} (TTL: {}s)",
            consumer_id, processing_ttl_seconds
        );

        loop {
            // First, try to recover any expired messages from other consumers
            if let Err(e) =
                Self::recover_expired_messages(&mut conn, processing_key_prefix, queue_key).await
            {
                warn!("Failed to recover expired messages: {}", e);
            }

            // Try to get a message from the queue using BRPOPLPUSH for atomic operation
            // Use a unique processing key for this message
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis();
            let processing_key = format!("{}:{}:{}", processing_key_prefix, consumer_id, timestamp);

            let message: Option<String> = conn.brpoplpush(queue_key, &processing_key, 5.0).await?;

            if let Some(event_json) = message {
                info!("Processing message: {}", event_json);

                // Set TTL on processing key for failure recovery
                let _: () = conn
                    .expire(&processing_key, processing_ttl_seconds as i64)
                    .await?;

                match serde_json::from_str::<RepoEvent>(&event_json) {
                    Ok(event) => {
                        // Send event to processor
                        if event_tx.send(event).is_err() {
                            warn!("Event channel closed, stopping consumer");
                            return Ok(());
                        }

                        // Message processed successfully, remove from processing queue
                        let _: () = conn.del(&processing_key).await?;
                        info!("Successfully processed and acknowledged message");
                    }
                    Err(e) => {
                        error!("Failed to deserialize event: {}", e);
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
                    warn!("Recovered expired message and re-queued it: {}", msg);
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
        info!("Starting event processor");

        while let Some(event) = self.event_rx.recv().await {
            if let Err(e) = self.process_event(event).await {
                error!("Failed to process event: {}", e);
            }
        }

        info!("Event processor stopped");
        Ok(())
    }

    /// Process a single repo event
    async fn process_event(&self, event: RepoEvent) -> Result<(), anyhow::Error> {
        match event.event_type.as_str() {
            "indexing_started" => {
                info!(
                    "Processing indexing_started event for repo {} (branch: {:?})",
                    event.repo_name, event.branch
                );
                // TODO: Implement indexing started handling
                // This could include:
                // - Updating internal state
                // - Notifying other components
                // - Preparing for incoming index data
            }
            "indexing_finished" => {
                info!(
                    "Processing indexing_finished event for repo {} (branch: {:?})",
                    event.repo_name, event.branch
                );
                // Implement a minimal ingestion path:
                // - Try to locate a local repo path under HYPERZOEKT_REPO_ROOT (or cwd)
                // - If present, run the repo index builder to (re)build internal indexes
                // - Compute pagerank and log stats
                // This keeps behavior safe and local-only; fetching remote repos is
                // intentionally omitted here.

                // Determine repo root from env or default to current dir
                let repo_root =
                    std::env::var("HYPERZOEKT_REPO_ROOT").unwrap_or_else(|_| ".".to_string());
                let mut candidate = PathBuf::from(&repo_root).join(&event.repo_name);

                // If the constructed path doesn't exist, try to interpret git_url as file://
                if !candidate.exists() {
                    if let Some(stripped) = event.git_url.strip_prefix("file://") {
                        let p = PathBuf::from(stripped);
                        if p.exists() {
                            candidate = p;
                        }
                    }
                }

                if !candidate.exists() {
                    info!(
                        "No local repo path found for {} (tried {}); skipping ingestion",
                        event.repo_name,
                        candidate.display()
                    );
                } else {
                    let repo_name = event.repo_name.clone();
                    let path_clone = candidate.clone();
                    // Run blocking index build off the tokio runtime
                    match tokio::task::spawn_blocking(move || -> Result<(), anyhow::Error> {
                        let (mut svc, stats) = RepoIndexService::build(path_clone.as_path())?;
                        // Compute pagerank (may be cheap) and surface stats
                        svc.compute_pagerank();
                        info!(
                            "Completed ingestion for {}: files_indexed={} entities_indexed={}",
                            repo_name, stats.files_indexed, stats.entities_indexed
                        );
                        Ok(())
                    })
                    .await
                    {
                        Ok(Ok(())) => {
                            info!(
                                "Successfully processed indexing_finished for {}",
                                event.repo_name
                            );
                        }
                        Ok(Err(e)) => {
                            error!("Indexer failed for {}: {}", event.repo_name, e);
                        }
                        Err(e) => {
                            error!("Indexer task panicked for {}: {:?}", event.repo_name, e);
                        }
                    }
                }
            }
            _ => {
                warn!("Unknown event type: {}", event.event_type);
            }
        }

        Ok(())
    }
}

/// Create and start the event consumer and processor
pub async fn start_event_system() -> Result<(), anyhow::Error> {
    start_event_system_with_ttl(300).await // Default to 5 minutes
}

/// Create and start the event consumer and processor with configurable TTL
pub async fn start_event_system_with_ttl(processing_ttl_seconds: u64) -> Result<(), anyhow::Error> {
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

    let consumer = EventConsumer::new(event_tx);
    let processor = EventProcessor::new(event_rx);

    if consumer.is_redis_available() {
        info!("Redis is available, starting event consumption");
        consumer
            .start_consuming_with_ttl(processing_ttl_seconds)
            .await?;
    } else {
        info!("Redis is not available, event consumption disabled");
    }

    // Start the processor in a separate task
    tokio::spawn(async move {
        if let Err(e) = processor.start_processing().await {
            error!("Event processor failed: {}", e);
        }
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn repo_event_deserialize_alias() {
        let json = r#"{"event":"indexing_finished","repo_name":"r","git_url":"file:///tmp","branch":null,"node_id":"n","timestamp":0}"#;
        let e: RepoEvent = serde_json::from_str(json).expect("deserialize");
        assert_eq!(e.event_type, "indexing_finished");
        assert_eq!(e.repo_name, "r");
    }

    #[tokio::test]
    async fn indexing_finished_triggers_build() {
        // Create a temp parent directory and a repo subdir
        let parent = tempdir().expect("tempdir");
        let repo_dir = parent.path().join("testrepo");
        fs::create_dir_all(&repo_dir).expect("create repo dir");
        // Add a tiny file so the indexer has something to scan
        fs::write(repo_dir.join("lib.rs"), "fn main() { println!(\"hi\"); }").expect("write file");

        // Point HYPERZOEKT_REPO_ROOT to the parent so join(repo_name) resolves
        env::set_var("HYPERZOEKT_REPO_ROOT", parent.path());

        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let processor = EventProcessor::new(rx);

        // Spawn the processor
        let handle = tokio::spawn(async move { processor.start_processing().await });

        // Send indexing_finished event targeting our testrepo
        let ev = RepoEvent {
            event_type: "indexing_finished".to_string(),
            repo_name: "testrepo".to_string(),
            git_url: format!("file://{}", repo_dir.display()),
            branch: None,
            node_id: "node1".to_string(),
            timestamp: 0,
        };

        _tx.send(ev).expect("send event");
        // close the sender so processor exits after processing
        drop(_tx);

        // Wait for the processor to finish (with timeout)
        let join_res = tokio::time::timeout(Duration::from_secs(20), handle)
            .await
            .expect("processor timed out")
            .expect("processor panicked");

        assert!(join_res.is_ok(), "processor returned error");
    }
}
