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

use base64::Engine;
use chrono::{DateTime, Utc};
use deadpool_redis::redis::{self, AsyncCommands, RedisResult};
use deadpool_redis::Pool;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

// Repository indexing event emitted by zoekt-distributed
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

// Type aliases to reduce clippy type-complexity warnings around the meta sender.
// MetaMsg: (name, branch, last_indexed_ms, last_duration_ms, memory_bytes, leased_node)
type MetaMsg = (String, String, i64, i64, i64, String);
type MetaSender = Sender<MetaMsg>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RemoteRepo {
    pub name: String,
    pub git_url: String,
    pub branch: Option<String>,
    pub visibility: zoekt_rs::types::RepoVisibility,
    pub owner: Option<String>,
    pub allowed_users: Vec<String>,
    pub last_commit_sha: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Lease {
    pub holder: String,
    pub until: DateTime<Utc>,
}

/// LeaseManager backed by Redis (SET NX + EX) with an in-memory fallback for tests.
/// The API is synchronous and uses a background Tokio runtime to run async Redis ops.
#[derive(Clone)]
pub struct LeaseManager {
    // shared pool for async redis connections
    redis_pool: Option<Pool>,
    inner: Arc<RwLock<HashMap<RemoteRepo, Lease>>>,
    // optional test hook to observe repo meta writes (name, last_indexed_ms, last_duration_ms, leased_node)
    meta_sender: Arc<RwLock<Option<MetaSender>>>,
    // SurrealDB store for repository metadata and permissions
    surreal_store: Option<Arc<crate::surreal_repo_store::SurrealRepoStore>>,
}

impl LeaseManager {
    /// Create a LeaseManager. If the `REDIS_URL` env var is set and a client can be
    /// created, it will be used; otherwise the manager uses an in-memory map (tests).
    pub async fn new() -> Self {
        // Use the shared Redis pool creation function with authentication support
        let redis_pool = crate::redis_adapter::create_redis_pool();

        // Initialize SurrealDB store for repository metadata
        let surreal_store = match Self::init_surreal_store().await {
            Ok(store) => Some(Arc::new(store)),
            Err(e) => {
                tracing::warn!("Failed to initialize SurrealDB store: {}", e);
                None
            }
        };

        Self {
            redis_pool,
            inner: Arc::new(RwLock::new(HashMap::new())),
            meta_sender: Arc::new(RwLock::new(None)),
            surreal_store,
        }
    }

    async fn init_surreal_store() -> anyhow::Result<crate::surreal_repo_store::SurrealRepoStore> {
        let surreal_url = std::env::var("SURREALDB_URL").ok();
        let surreal_username = std::env::var("SURREALDB_USERNAME").ok();
        let surreal_password = std::env::var("SURREALDB_PASSWORD").ok();
        let surreal_ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
        let surreal_db = std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());

        // Check if we have all required environment variables for remote connection
        let use_remote =
            surreal_url.is_some() && surreal_username.is_some() && surreal_password.is_some();

        if use_remote {
            tracing::info!("Connecting to remote SurrealDB instance");
            crate::surreal_repo_store::SurrealRepoStore::new_remote(
                surreal_url.unwrap(),
                surreal_username.unwrap(),
                surreal_password.unwrap(),
                surreal_ns,
                surreal_db,
            )
            .await
        } else {
            if surreal_url.is_some() || surreal_username.is_some() || surreal_password.is_some() {
                tracing::warn!("Incomplete SurrealDB configuration. Missing SURREALDB_URL, SURREALDB_USERNAME, or SURREALDB_PASSWORD. Falling back to in-memory instance.");
            } else {
                tracing::info!("No SurrealDB configuration found, using in-memory instance");
            }
            crate::surreal_repo_store::SurrealRepoStore::new_in_memory(surreal_ns, surreal_db).await
        }
    }

    // NOTE: repo-level metadata writes were removed in favor of branch-scoped metadata.
    // The previous `set_repo_meta` helper has been intentionally removed. Tests and
    // runtime code should use `set_branch_meta(repo, branch, ...)` which stores data
    // in the `zoekt:repo_branch_meta` hash under the field "<repo>|<branch>".

    /// Set or update branch-specific metadata in the `zoekt:repo_branch_meta` hash.
    /// Field is stored as "<repo>|<branch>" and value is a JSON blob similar to repo_meta
    /// Note: leased_node is no longer stored in metadata - use get_current_lease_holder instead
    pub async fn set_branch_meta(
        &self,
        repo: &str,
        branch: &str,
        last_indexed_ms: i64,
        last_duration_ms: i64,
        memory_bytes: i64,
        node_id: &str,
    ) {
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                let key = "zoekt:repo_branch_meta";
                let field = format!("{}|{}", repo, branch);
                // Fetch existing meta if present
                let existing: Option<String> = conn.hget(key, &field).await.ok().flatten();
                let mut v = if let Some(s) = existing {
                    serde_json::from_str::<serde_json::Value>(&s).unwrap_or(json!({}))
                } else {
                    json!({})
                };
                v["last_indexed"] = json!(last_indexed_ms);
                v["last_duration_ms"] = json!(last_duration_ms);
                v["memory_bytes"] = json!(memory_bytes);
                // Note: leased_node is no longer stored in metadata
                // Persist back (ignore errors)
                let _ = deadpool_redis::redis::cmd("HSET")
                    .arg(key)
                    .arg(field)
                    .arg(v.to_string())
                    .query_async::<i32>(&mut conn)
                    .await;
                // Attempt to notify any registered test hook with the updated meta
                if let Some(sender) = &*self.meta_sender.read() {
                    // best-effort, do not block on failure
                    let _ = sender.try_send((
                        repo.to_string(),
                        branch.to_string(),
                        last_indexed_ms,
                        last_duration_ms,
                        memory_bytes,
                        node_id.to_string(),
                    ));
                }
            }
        } else {
            // When not using Redis (tests), still notify the meta sender if present
            if let Some(sender) = &*self.meta_sender.read() {
                let _ = sender.try_send((
                    repo.to_string(),
                    branch.to_string(),
                    last_indexed_ms,
                    last_duration_ms,
                    memory_bytes,
                    node_id.to_string(),
                ));
            }
        }
    }

    /// Register an async sender to observe repo meta writes (for tests).
    pub fn set_meta_sender(&self, s: Sender<MetaMsg>) {
        let mut guard = self.meta_sender.write();
        *guard = Some(s);
    }

    /// Set or update indexer endpoint in the `zoekt:indexers` hash. This will merge
    /// with any existing JSON blob and set the endpoint and last_heartbeat.
    pub async fn set_indexer_endpoint(&self, node_id: &str, endpoint: &str) {
        if let Some(pool) = &self.redis_pool {
            match pool.get().await {
                Ok(mut conn) => {
                    let key = "zoekt:indexers";
                    let timestamp = chrono::Utc::now().timestamp_millis();

                    // Fetch existing meta if present to calculate proper heartbeat age
                    let existing: Option<String> = conn.hget(key, node_id).await.ok().flatten();

                    // Calculate age based on the previous heartbeat value before updating
                    let prev_heartbeat = if let Some(ref existing_str) = existing {
                        if let Ok(prev_v) = serde_json::from_str::<serde_json::Value>(existing_str)
                        {
                            prev_v.get("last_heartbeat").and_then(|h| h.as_i64())
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    let mut v = if let Some(s) = existing {
                        serde_json::from_str::<serde_json::Value>(&s).unwrap_or(json!({}))
                    } else {
                        json!({})
                    };

                    let was_new = v.get("endpoint").is_none();
                    v["endpoint"] = json!(endpoint);
                    v["last_heartbeat"] = json!(timestamp);

                    // Persist back
                    match deadpool_redis::redis::cmd("HSET")
                        .arg(key)
                        .arg(node_id)
                        .arg(v.to_string())
                        .query_async::<i32>(&mut conn)
                        .await
                    {
                        Ok(_) => {
                            if was_new {
                                tracing::info!(node_id=%node_id, endpoint=%endpoint, "indexer endpoint registered");
                            } else {
                                let heartbeat_age = if let Some(prev_hb) = prev_heartbeat {
                                    let now = chrono::Utc::now().timestamp_millis();
                                    let age_seconds = (now - prev_hb) / 1000;
                                    format!("{}s ago", age_seconds)
                                } else {
                                    "unknown".to_string()
                                };
                                tracing::info!(
                                    node_id=%node_id,
                                    endpoint=%endpoint,
                                    timestamp=%timestamp,
                                    last_heartbeat=%heartbeat_age,
                                    "indexer heartbeat sent to Redis"
                                );
                                tracing::debug!(node_id=%node_id, endpoint=%endpoint, timestamp=%timestamp, "indexer heartbeat details");
                            }
                        }
                        Err(e) => {
                            tracing::error!(node_id=%node_id, endpoint=%endpoint, error=%e, "failed to update indexer endpoint");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(node_id=%node_id, endpoint=%endpoint, error=%e, "failed to get Redis connection for heartbeat");
                }
            }
        } else {
            tracing::debug!(node_id=%node_id, endpoint=%endpoint, "skipping heartbeat: no Redis connection");
        }
    }

    /// Get all indexer endpoints from the `zoekt:indexers` hash.
    /// Returns a map of node_id to endpoint URL.
    pub async fn get_indexer_endpoints(&self) -> HashMap<String, String> {
        let mut result = HashMap::new();
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                let key = "zoekt:indexers";
                let all: RedisResult<HashMap<String, String>> = conn.hgetall(key).await;
                if let Ok(map) = all {
                    for (node_id, json_str) in map {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&json_str) {
                            if let Some(endpoint) = v.get("endpoint").and_then(|e| e.as_str()) {
                                result.insert(node_id, endpoint.to_string());
                            }
                        }
                    }
                }
            }
        }
        result
    }

    /// Get all indexer endpoints with their last heartbeat information.
    /// Returns a map of node_id to (endpoint, last_heartbeat_timestamp).
    pub async fn get_indexer_endpoints_with_heartbeat(&self) -> HashMap<String, (String, i64)> {
        let mut result = HashMap::new();
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                let key = "zoekt:indexers";
                let all: RedisResult<HashMap<String, String>> = conn.hgetall(key).await;
                if let Ok(map) = all {
                    for (node_id, json_str) in map {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&json_str) {
                            if let (Some(endpoint), Some(last_heartbeat)) = (
                                v.get("endpoint").and_then(|e| e.as_str()),
                                v.get("last_heartbeat").and_then(|h| h.as_i64()),
                            ) {
                                result.insert(node_id, (endpoint.to_string(), last_heartbeat));
                            }
                        }
                    }
                }
            }
        }
        result
    }

    /// Check if an indexer is healthy based on its last heartbeat.
    /// Returns true if the indexer has sent a heartbeat within the specified timeout.
    pub async fn is_indexer_healthy(&self, node_id: &str, max_age_seconds: i64) -> bool {
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                let key = "zoekt:indexers";
                let existing: Option<String> = conn.hget(key, node_id).await.ok().flatten();
                if let Some(json_str) = existing {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&json_str) {
                        if let Some(last_heartbeat) =
                            v.get("last_heartbeat").and_then(|h| h.as_i64())
                        {
                            let now = chrono::Utc::now().timestamp_millis();
                            let age_seconds = (now - last_heartbeat) / 1000;
                            return age_seconds <= max_age_seconds;
                        }
                    }
                }
            }
        }
        false
    }

    /// Get all repo branch entries from the `zoekt:repo_branches` hash.
    /// Returns a vector of (repo_name, branch, url).
    pub async fn get_repo_branches(&self) -> Vec<(String, String, String)> {
        let mut out = Vec::new();
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                let key = "zoekt:repo_branches";
                let all: RedisResult<HashMap<String, String>> = conn.hgetall(key).await;
                if let Ok(map) = all {
                    for (field, url) in map {
                        // field format: "<repo_name>|<branch>"
                        if let Some((name, branch)) = field.split_once('|') {
                            out.push((name.to_string(), branch.to_string(), url));
                        }
                    }
                }
            }
        }
        out
    }

    /// Get all unleased branches from the registered repo branches.
    /// Returns a vector of (repo_name, branch, url) for branches that don't have active leases.
    pub async fn get_unleased_branches(&self) -> Vec<(String, String, String)> {
        let mut unleased = Vec::new();
        let all_branches = self.get_repo_branches().await;

        for (name, branch, url) in all_branches {
            let repo = RemoteRepo {
                name: name.clone(),
                git_url: url.clone(),
                branch: Some(branch.clone()),
                visibility: zoekt_rs::types::RepoVisibility::Public, // Default visibility
                owner: None,
                allowed_users: Vec::new(),
                last_commit_sha: None,
            };

            // Check if this branch has an active lease
            if self.get_current_lease_holder(&repo).await.is_none() {
                unleased.push((name, branch, url));
            }
        }

        unleased
    }

    /// Check if a specific repo branch is still registered in the system.
    /// Returns true if the branch exists in zoekt:repo_branches, false otherwise.
    pub async fn is_repo_branch_registered(&self, repo_name: &str, branch: &str) -> bool {
        let all_branches = self.get_repo_branches().await;
        all_branches
            .iter()
            .any(|(name, br, _)| name == repo_name && br == branch)
    }

    fn repo_key(repo: &RemoteRepo) -> String {
        // include branch when present so leases can be scoped to specific branches
        let branch_part = repo.branch.as_deref().unwrap_or("");
        base64::engine::general_purpose::URL_SAFE
            .encode(format!("{}|{}|{}", repo.name, repo.git_url, branch_part))
    }

    /// Try to acquire a lease. Prefer Redis (atomic SET NX EX). Fall back to an in-memory
    /// map if no Redis is configured (not safe across processes, but ok for tests).
    pub async fn try_acquire(&self, repo: &RemoteRepo, holder_id: String, ttl: Duration) -> bool {
        let key = Self::repo_key(repo);
        let ttl_secs = ttl.as_secs() as usize;
        let inner = self.inner.clone();
        let holder = holder_id.clone();

        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                let set_res: RedisResult<Option<String>> = redis::cmd("SET")
                    .arg(key.clone())
                    .arg(holder.clone())
                    .arg("NX")
                    .arg("EX")
                    .arg(ttl_secs)
                    .query_async(&mut conn)
                    .await;
                if let Ok(Some(s)) = set_res {
                    if s == "OK" {
                        tracing::info!(
                            repo=%repo.name,
                            branch=?repo.branch,
                            holder=%holder,
                            ttl_secs=%ttl_secs,
                            "lease acquired successfully"
                        );
                        inner.write().insert(
                            repo.clone(),
                            Lease {
                                holder: holder.clone(),
                                until: Utc::now() + chrono::Duration::seconds(ttl_secs as i64),
                            },
                        );
                        return true;
                    } else {
                        tracing::debug!(
                            repo=%repo.name,
                            branch=?repo.branch,
                            holder=%holder,
                            "lease acquisition failed: already held by another node"
                        );
                    }
                } else if let Ok(None) = set_res {
                    // SET NX EX returned nil, meaning key already exists
                    // Check if we are already the holder
                    let get_res: RedisResult<String> = conn.get(&key).await;
                    if let Ok(existing_holder) = get_res {
                        if existing_holder == holder {
                            tracing::debug!(
                                repo=%repo.name,
                                branch=?repo.branch,
                                holder=%holder,
                                "lease already held by this node, proceeding"
                            );
                            // Update the in-memory cache
                            inner.write().insert(
                                repo.clone(),
                                Lease {
                                    holder: holder.clone(),
                                    until: Utc::now() + chrono::Duration::seconds(ttl_secs as i64),
                                },
                            );
                            return true;
                        } else {
                            tracing::debug!(
                                repo=%repo.name,
                                branch=?repo.branch,
                                holder=%holder,
                                existing_holder=%existing_holder,
                                "lease acquisition failed: already held by another node"
                            );
                        }
                    } else {
                        tracing::debug!(
                            repo=%repo.name,
                            branch=?repo.branch,
                            holder=%holder,
                            "lease acquisition failed: could not check existing holder"
                        );
                    }
                } else {
                    // Actual Redis error
                    tracing::warn!(
                        repo=%repo.name,
                        branch=?repo.branch,
                        holder=%holder,
                        error=?set_res.as_ref().err(),
                        "lease acquisition failed: Redis error"
                    );
                }
            } else {
                tracing::warn!(
                    repo=%repo.name,
                    branch=?repo.branch,
                    holder=%holder,
                    "lease acquisition failed: Redis connection error"
                );
            }
            return false;
        }

        // In-memory fallback
        let mut guard = inner.write();
        let now = Utc::now();
        if let Some(existing) = guard.get(repo) {
            if existing.until > now {
                if existing.holder == holder {
                    tracing::debug!(
                        repo=%repo.name,
                        branch=?repo.branch,
                        holder=%holder,
                        "lease already held by this node (in-memory), proceeding"
                    );
                    // Update the expiry time
                    guard.insert(
                        repo.clone(),
                        Lease {
                            holder: holder.clone(),
                            until: now + chrono::Duration::from_std(ttl).unwrap(),
                        },
                    );
                    return true;
                } else {
                    tracing::debug!(
                        repo=%repo.name,
                        branch=?repo.branch,
                        holder=%holder,
                        existing_holder=%existing.holder,
                        "lease acquisition failed: already held by another node (in-memory)"
                    );
                    return false;
                }
            }
        }
        tracing::info!(
            repo=%repo.name,
            branch=?repo.branch,
            holder=%holder,
            ttl_secs=%ttl_secs,
            "lease acquired successfully (in-memory)"
        );
        guard.insert(
            repo.clone(),
            Lease {
                holder: holder.clone(),
                until: now + chrono::Duration::from_std(ttl).unwrap(),
            },
        );
        true
    }

    /// Release a lease: delete the key in Redis (if configured) and remove from cache.
    /// Release a lease safely: only delete the key if its value matches our holder id.
    /// This uses a small Lua script via EVAL to perform the check-and-delete atomically.
    pub async fn release(&self, repo: &RemoteRepo, holder_id: &str) {
        let key = Self::repo_key(repo);
        let inner = self.inner.clone();
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                let script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
                let res: RedisResult<i32> = redis::cmd("EVAL")
                    .arg(script)
                    .arg(1)
                    .arg(key.clone())
                    .arg(holder_id.to_string())
                    .query_async(&mut conn)
                    .await;
                match res {
                    Ok(1) => {
                        tracing::info!(
                            repo=%repo.name,
                            branch=?repo.branch,
                            holder=%holder_id,
                            "lease released successfully"
                        );
                    }
                    Ok(0) => {
                        tracing::warn!(
                            repo=%repo.name,
                            branch=?repo.branch,
                            holder=%holder_id,
                            "lease release failed: not held by this holder"
                        );
                    }
                    Ok(code) => {
                        tracing::warn!(
                            repo=%repo.name,
                            branch=?repo.branch,
                            holder=%holder_id,
                            return_code=%code,
                            "lease release: unexpected Redis script return code"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            repo=%repo.name,
                            branch=?repo.branch,
                            holder=%holder_id,
                            error=%e,
                            "lease release failed: Redis error"
                        );
                    }
                }
            } else {
                tracing::warn!(
                    repo=%repo.name,
                    branch=?repo.branch,
                    holder=%holder_id,
                    "lease release failed: Redis connection error"
                );
            }
            inner.write().remove(repo);
        } else {
            tracing::info!(
                repo=%repo.name,
                branch=?repo.branch,
                holder=%holder_id,
                "lease released (in-memory)"
            );
            inner.write().remove(repo);
        }
    }

    /// Renew a lease by extending the TTL if we are still the holder.
    /// Returns true if renewal succeeded.
    pub async fn renew(&self, repo: &RemoteRepo, holder_id: String, ttl: Duration) -> bool {
        let key = Self::repo_key(repo);
        let inner = self.inner.clone();
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                // If the stored value matches our holder id, update the expiry.
                let script = "if redis.call('get', KEYS[1]) == ARGV[1] then redis.call('expire', KEYS[1], tonumber(ARGV[2])); return 1 else return 0 end";
                let ttl_secs = ttl.as_secs() as usize;
                let res: RedisResult<i32> = redis::cmd("EVAL")
                    .arg(script)
                    .arg(1)
                    .arg(key.clone())
                    .arg(holder_id.clone())
                    .arg(ttl_secs)
                    .query_async(&mut conn)
                    .await;
                return match res {
                    Ok(1) => {
                        tracing::debug!(
                            repo=%repo.name,
                            branch=?repo.branch,
                            holder=%holder_id,
                            ttl_secs=%ttl_secs,
                            "lease renewed successfully"
                        );
                        true
                    }
                    Ok(0) => {
                        tracing::warn!(
                            repo=%repo.name,
                            branch=?repo.branch,
                            holder=%holder_id,
                            "lease renewal failed: not held by this holder"
                        );
                        false
                    }
                    Ok(code) => {
                        tracing::warn!(
                            repo=%repo.name,
                            branch=?repo.branch,
                            holder=%holder_id,
                            return_code=%code,
                            "lease renewal: unexpected Redis script return code"
                        );
                        false
                    }
                    Err(e) => {
                        tracing::error!(
                            repo=%repo.name,
                            branch=?repo.branch,
                            holder=%holder_id,
                            error=%e,
                            "lease renewal failed: Redis error"
                        );
                        false
                    }
                };
            }
            tracing::warn!(
                repo=%repo.name,
                branch=?repo.branch,
                holder=%holder_id,
                "lease renewal failed: Redis connection error"
            );
            return false;
        }

        // In-memory fallback: only renew if holder matches
        let mut guard = inner.write();
        if let Some(existing) = guard.get_mut(repo) {
            if existing.holder == holder_id {
                let ttl_secs = ttl.as_secs() as usize;
                existing.until = Utc::now() + chrono::Duration::from_std(ttl).unwrap();
                tracing::debug!(
                    repo=%repo.name,
                    branch=?repo.branch,
                    holder=%holder_id,
                    ttl_secs=%ttl_secs,
                    "lease renewed successfully (in-memory)"
                );
                return true;
            } else {
                tracing::warn!(
                    repo=%repo.name,
                    branch=?repo.branch,
                    holder=%holder_id,
                    existing_holder=%existing.holder,
                    "lease renewal failed: held by different holder (in-memory)"
                );
            }
        } else {
            tracing::warn!(
                repo=%repo.name,
                branch=?repo.branch,
                holder=%holder_id,
                "lease renewal failed: lease not found (in-memory)"
            );
        }
        false
    }

    /// Get the current lease holder for a repository branch by checking Redis directly
    pub async fn get_current_lease_holder(&self, repo: &RemoteRepo) -> Option<String> {
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                let key = Self::repo_key(repo);
                let result: RedisResult<String> = conn.get(&key).await;
                match result {
                    Ok(holder) => {
                        tracing::debug!(repo=%repo.name, branch=?repo.branch, key=%key, holder=%holder, "get_current_lease_holder: found holder in Redis");
                        Some(holder)
                    }
                    Err(e) => {
                        tracing::debug!(repo=%repo.name, branch=?repo.branch, key=%key, error=%e, "get_current_lease_holder: Redis error");
                        None
                    }
                }
            } else {
                tracing::debug!(repo=%repo.name, branch=?repo.branch, "get_current_lease_holder: Redis connection error");
                None
            }
        } else {
            // In-memory fallback
            let holder = self
                .inner
                .read()
                .get(repo)
                .map(|lease| lease.holder.clone());
            tracing::debug!(repo=%repo.name, branch=?repo.branch, holder=?holder, "get_current_lease_holder: in-memory fallback");
            holder
        }
    }

    /// Get the current lease for a repository branch
    pub async fn get_lease(&self, repo: &RemoteRepo) -> Option<Lease> {
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                let key = Self::repo_key(repo);
                let result: RedisResult<String> = conn.get(&key).await;
                match result {
                    Ok(holder) => {
                        // For Redis-backed leases, we don't have the exact expiry time
                        // but we know it's held by someone, so we return a lease with current time + some buffer
                        Some(Lease {
                            holder,
                            until: Utc::now() + chrono::Duration::seconds(30), // Assume 30 second TTL for display
                        })
                    }
                    Err(_) => None,
                }
            } else {
                None
            }
        } else {
            // In-memory fallback
            self.inner.read().get(repo).cloned()
        }
    }

    /// Check if a user can access a repository based on its visibility settings
    pub async fn can_user_access_repo(&self, git_url: &str, user_id: Option<&str>) -> bool {
        if let Some(store) = &self.surreal_store {
            match store.can_user_access_repo(git_url, user_id).await {
                Ok(can_access) => can_access,
                Err(e) => {
                    tracing::warn!("Failed to check repo access for {}: {}", git_url, e);
                    true // Default to allowing access on error
                }
            }
        } else {
            true // If no SurrealDB store, allow access
        }
    }

    /// Update repository metadata in SurrealDB
    pub async fn update_repo_metadata(&self, repo: &RemoteRepo, last_commit_sha: Option<String>) {
        if let Some(store) = &self.surreal_store {
            let metadata = crate::surreal_repo_store::RepoMetadata {
                id: format!("repo:{}", repo.git_url.replace("/", "_").replace(":", "_")),
                name: repo.name.clone(),
                git_url: repo.git_url.clone(),
                branch: repo.branch.clone(),
                visibility: repo.visibility.clone(),
                owner: repo.owner.clone(),
                allowed_users: repo.allowed_users.clone(),
                last_commit_sha,
                last_indexed_at: Some(chrono::Utc::now()),
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };

            if let Err(e) = store.upsert_repo_metadata(metadata).await {
                tracing::warn!("Failed to update repo metadata for {}: {}", repo.git_url, e);
            }
        }
    }

    /// Check if indexing should be skipped for a repository
    /// Note: SHA-based skipping is now handled at the Node level for remote repositories.
    /// This method is kept for backwards compatibility and potential future use.
    pub async fn should_skip_indexing(&self, _git_url: &str, _current_sha: &str) -> bool {
        // SHA-based skipping is now handled at the Node level for remote repositories
        // to avoid unnecessary lease acquisition when content hasn't changed.
        false
    }

    /// Publish a repo event to Redis queue (not pub/sub) for reliable consumption
    pub async fn publish_repo_event(&self, event_type: &str, repo: &RemoteRepo, node_id: &str) {
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                let event = json!({
                    "event": event_type,
                    "repo_name": repo.name,
                    "git_url": repo.git_url,
                    "branch": repo.branch,
                    "node_id": node_id,
                    "timestamp": chrono::Utc::now().timestamp_millis()
                });
                let queue_key = "zoekt:repo_events_queue";
                let _: RedisResult<()> = conn.lpush(queue_key, event.to_string()).await;
                tracing::info!(event_type=%event_type, repo=%repo.name, branch=?repo.branch, node_id=%node_id, "emitted repo event to Redis queue");
                tracing::debug!(event_type=%event_type, repo=%repo.name, branch=?repo.branch, "published repo event to Redis queue");
            } else {
                tracing::debug!("failed to get Redis connection for publishing repo event");
            }
        } else {
            tracing::debug!("no Redis pool available for publishing repo event");
        }
    }
}
