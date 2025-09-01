//! Minimal "distributed" layer prototype for zoekt-rs.
//!
//! Goals in this initial patch:
//! - Provide a small in-process lease manager that tracks "remote repos"
//! - Provide a Node that periodically tries to acquire leases and runs a provided indexer
//! - Keep the API small so tests can plug a fake indexer easily

use anyhow::Result;
use base64::Engine;
use chrono::{DateTime, Utc};
use deadpool_redis::redis::{self, AsyncCommands, RedisResult};
use deadpool_redis::{Config as RedisConfig, Pool};
use parking_lot::RwLock;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

// Type aliases to reduce clippy type-complexity warnings around the meta sender.
// MetaMsg: (name, last_indexed_ms, last_duration_ms, memory_bytes, leased_node)
type MetaMsg = (String, i64, i64, i64, String);
type MetaSender = Sender<MetaMsg>;

pub use zoekt_rs::InMemoryIndex;
mod config;
pub use config::{load_node_config, MergeOpts};
pub mod redis_adapter;
pub mod web_utils;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RemoteRepo {
    pub name: String,
    pub git_url: String,
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
}

impl LeaseManager {
    /// Create a LeaseManager. If the `REDIS_URL` env var is set and a client can be
    /// created, it will be used; otherwise the manager uses an in-memory map (tests).
    pub async fn new() -> Self {
        // If REDIS_URL is present try to create a deadpool redis pool.
        let redis_pool = match env::var("REDIS_URL").ok() {
            Some(url) => RedisConfig::from_url(url.as_str()).create_pool(None).ok(),
            None => None,
        };

        Self {
            redis_pool,
            inner: Arc::new(RwLock::new(HashMap::new())),
            meta_sender: Arc::new(RwLock::new(None)),
        }
    }

    /// Set or update repo metadata in the `zoekt:repo_meta` hash. This will merge
    /// with any existing JSON blob and set last_indexed, last_duration_ms and leased_node.
    pub async fn set_repo_meta(
        &self,
        name: &str,
        last_indexed_ms: i64,
        last_duration_ms: i64,
        memory_bytes: i64,
        leased_node: &str,
    ) {
        // call test hook if present (non-blocking)
        // clone the optional sender out of the lock so we don't hold the RwLock guard across an await
        let maybe_tx: Option<MetaSender> = {
            let guard = self.meta_sender.read();
            guard.clone()
        };
        if let Some(tx) = maybe_tx {
            let name_s = name.to_string();
            let leased_s = leased_node.to_string();
            tracing::debug!(repo=%name_s, "sending repo meta to test hook");
            // best-effort send; do not fail if receiver gone
            match tx
                .send((
                    name_s.clone(),
                    last_indexed_ms,
                    last_duration_ms,
                    memory_bytes,
                    leased_s.clone(),
                ))
                .await
            {
                Ok(_) => tracing::debug!(repo=%name_s, "meta hook send succeeded"),
                Err(e) => tracing::warn!(repo=%name_s, error=%e, "meta hook send failed"),
            }
        }
        if let Some(pool) = &self.redis_pool {
            if let Ok(mut conn) = pool.get().await {
                let key = "zoekt:repo_meta";
                // Fetch existing meta if present
                let existing: Option<String> = conn.hget(key, name).await.ok().flatten();
                let mut v = if let Some(s) = existing {
                    serde_json::from_str::<serde_json::Value>(&s).unwrap_or(json!({}))
                } else {
                    json!({})
                };
                v["last_indexed"] = json!(last_indexed_ms);
                v["last_duration_ms"] = json!(last_duration_ms);
                v["memory_bytes"] = json!(memory_bytes);
                v["leased_node"] = json!(leased_node);
                // Persist back (ignore errors)
                let _ = deadpool_redis::redis::cmd("HSET")
                    .arg(key)
                    .arg(name)
                    .arg(v.to_string())
                    .query_async::<i32>(&mut conn)
                    .await;
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
            if let Ok(mut conn) = pool.get().await {
                let key = "zoekt:indexers";
                // Fetch existing meta if present
                let existing: Option<String> = conn.hget(key, node_id).await.ok().flatten();
                let mut v = if let Some(s) = existing {
                    serde_json::from_str::<serde_json::Value>(&s).unwrap_or(json!({}))
                } else {
                    json!({})
                };
                v["endpoint"] = json!(endpoint);
                v["last_heartbeat"] = json!(chrono::Utc::now().timestamp_millis());
                // Persist back (ignore errors)
                let _ = deadpool_redis::redis::cmd("HSET")
                    .arg(key)
                    .arg(node_id)
                    .arg(v.to_string())
                    .query_async::<i32>(&mut conn)
                    .await;
            }
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

    fn repo_key(repo: &RemoteRepo) -> String {
        base64::engine::general_purpose::URL_SAFE.encode(format!("{}|{}", repo.name, repo.git_url))
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
                let set_res: RedisResult<String> = redis::cmd("SET")
                    .arg(key.clone())
                    .arg(holder.clone())
                    .arg("NX")
                    .arg("EX")
                    .arg(ttl_secs)
                    .query_async(&mut conn)
                    .await;
                if let Ok(s) = set_res {
                    if s == "OK" {
                        inner.write().insert(
                            repo.clone(),
                            Lease {
                                holder: holder.clone(),
                                until: Utc::now() + chrono::Duration::seconds(ttl_secs as i64),
                            },
                        );
                        return true;
                    }
                }
            }
            return false;
        }

        // In-memory fallback
        let mut guard = inner.write();
        let now = Utc::now();
        if let Some(existing) = guard.get(repo) {
            if existing.until > now {
                return false;
            }
        }
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
                let _res: RedisResult<i32> = redis::cmd("EVAL")
                    .arg(script)
                    .arg(1)
                    .arg(key.clone())
                    .arg(holder_id.to_string())
                    .query_async(&mut conn)
                    .await;
            }
            inner.write().remove(repo);
        } else {
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
                    .arg(holder_id)
                    .arg(ttl_secs)
                    .query_async(&mut conn)
                    .await;
                return match res {
                    Ok(v) => v == 1,
                    Err(_) => false,
                };
            }
            return false;
        }

        // In-memory fallback: only renew if holder matches
        let mut guard = inner.write();
        if let Some(existing) = guard.get_mut(repo) {
            if existing.holder == holder_id {
                existing.until = Utc::now() + chrono::Duration::from_std(ttl).unwrap();
                return true;
            }
        }
        false
    }

    pub async fn get_lease(&self, repo: &RemoteRepo) -> Option<Lease> {
        let repo = repo.clone();
        let inner = self.inner.clone();
        let guard = inner.read();
        let res = guard.get(&repo).cloned();
        drop(guard);
        res
    }

    // bidding and auction evaluation implemented in the main flow; test helper removed.
}

/// The Indexer trait abstracts building/holding an index for a repo. Tests can provide
/// a simple implementation that uses `zoekt_rs::build_in_memory_index`.
pub trait Indexer: Send + Sync + 'static {
    /// Build/index the repo at `repo_path` and return some index handle. For the prototype,
    /// we return an `InMemoryIndex` from `zoekt_rs`.
    fn index_repo(&self, repo_path: PathBuf) -> Result<InMemoryIndex>;
}

/// Node configuration.
pub struct NodeConfig {
    pub id: String,
    pub lease_ttl: Duration,
    pub poll_interval: Duration,
    pub node_type: NodeType,
    pub endpoint: Option<String>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        // Read configuration from environment variables when present. Useful for k8s.
        // Env vars:
        // - ZOEKTD_NODE_ID
        // - ZOEKTD_LEASE_TTL_SECONDS
        // - ZOEKTD_POLL_INTERVAL_SECONDS
        // - ZOEKTD_ENDPOINT (manual override)
        // - ZOEKTD_SERVICE_NAME (for k8s service discovery)
        // - ZOEKTD_SERVICE_PORT (for k8s service discovery)
        // - ZOEKTD_SERVICE_PROTOCOL (http/https, defaults to http)
        let id = std::env::var("ZOEKTD_NODE_ID").unwrap_or_else(|_| "node-1".into());
        let lease_ttl = std::env::var("ZOEKTD_LEASE_TTL_SECONDS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(150));
        let poll_interval = std::env::var("ZOEKTD_POLL_INTERVAL_SECONDS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(5));

        let node_type: NodeType = std::env::var("ZOEKTD_NODE_TYPE")
            .ok()
            .and_then(|s| s.parse::<NodeType>().ok())
            .unwrap_or(NodeType::Indexer);

        // Determine endpoint: manual override takes precedence, then try k8s auto-discovery
        let endpoint = if let Ok(manual_endpoint) = std::env::var("ZOEKTD_ENDPOINT") {
            Some(manual_endpoint)
        } else if node_type == NodeType::Indexer {
            // Try Kubernetes service discovery for indexers
            Self::discover_kubernetes_endpoint()
        } else {
            None
        };

        Self {
            id,
            lease_ttl,
            poll_interval,
            node_type,
            endpoint,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeType {
    Indexer,
    Admin,
    Search,
}

impl std::str::FromStr for NodeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "indexer" => Ok(NodeType::Indexer),
            "admin" => Ok(NodeType::Admin),
            "search" => Ok(NodeType::Search),
            _ => Err(format!("Unknown node type: {}", s)),
        }
    }
}

impl NodeConfig {
    /// Attempt to auto-discover the endpoint using Kubernetes service environment variables.
    /// This supports both direct service discovery and custom service name configuration.
    /// For StatefulSets, each pod discovers its own individual endpoint.
    fn discover_kubernetes_endpoint() -> Option<String> {
        let protocol = std::env::var("ZOEKTD_SERVICE_PROTOCOL").unwrap_or_else(|_| "http".into());

        // First, try using ZOEKTD_SERVICE_NAME if explicitly set
        if let Ok(service_name) = std::env::var("ZOEKTD_SERVICE_NAME") {
            let normalized_service_name = service_name.replace("-", "_").to_uppercase();
            let service_host_env = format!("{}_SERVICE_HOST", normalized_service_name);
            let service_port_env = format!("{}_SERVICE_PORT", normalized_service_name);

            if let (Ok(host), Ok(port)) = (
                std::env::var(&service_host_env),
                std::env::var(&service_port_env),
            ) {
                tracing::info!(service_name = %service_name, host = %host, port = %port, protocol = %protocol, "discovered kubernetes service endpoint");
                return Some(format!("{}://{}:{}", protocol, host, port));
            } else {
                tracing::warn!(service_name = %service_name, "kubernetes service environment variables not found");
            }
        }

        // For StatefulSets: try to discover pod-specific endpoint
        // Each pod in a StatefulSet gets a stable DNS name like: <statefulset-name>-<ordinal>.<service-name>
        if let Some(pod_endpoint) = Self::discover_statefulset_pod_endpoint(&protocol) {
            return Some(pod_endpoint);
        }

        // Fallback: try common Kubernetes service patterns
        // Look for any environment variables that match the pattern *_SERVICE_HOST
        for (key, value) in std::env::vars() {
            if key.ends_with("_SERVICE_HOST") && !key.starts_with("KUBERNETES_") {
                let service_name = key.trim_end_matches("_SERVICE_HOST").to_lowercase();
                let port_env = format!("{}_SERVICE_PORT", service_name.to_uppercase());

                if let Ok(port) = std::env::var(&port_env) {
                    let protocol =
                        std::env::var("ZOEKTD_SERVICE_PROTOCOL").unwrap_or_else(|_| "http".into());
                    tracing::info!(service_name = %service_name, host = %value, port = %port, protocol = %protocol, "auto-discovered kubernetes service endpoint");
                    return Some(format!("{}://{}:{}", protocol, value, port));
                }
            }
        }

        tracing::debug!("no kubernetes service endpoint discovered, will use manual configuration");
        None
    }

    /// Discover the endpoint for a StatefulSet pod using pod metadata and service information.
    /// Each pod in a StatefulSet gets a stable DNS name: <pod-name>.<service-name>
    fn discover_statefulset_pod_endpoint(protocol: &str) -> Option<String> {
        // Try to get pod name from environment (injected by Downward API or via env)
        let pod_name = std::env::var("POD_NAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .ok()?;

        // Try to get namespace
        let namespace = std::env::var("POD_NAMESPACE")
            .or_else(|_| std::env::var("NAMESPACE"))
            .unwrap_or_else(|_| "default".into());

        // Try to get service name from various sources
        let service_name = std::env::var("ZOEKTD_SERVICE_NAME")
            .or_else(|_| {
                // Try to infer from pod name (remove ordinal suffix)
                pod_name
                    .rfind('-')
                    .map(|pos| pod_name[..pos].to_string())
                    .ok_or_else(|| "Could not infer service name from pod name".to_string())
            })
            .or_else(|_| {
                // Look for service environment variables and infer service name
                std::env::vars()
                    .find(|(key, _)| {
                        key.ends_with("_SERVICE_HOST") && !key.starts_with("KUBERNETES_")
                    })
                    .map(|(key, _)| key.trim_end_matches("_SERVICE_HOST").to_lowercase())
                    .ok_or_else(|| "No service environment variables found".to_string())
            })
            .ok()?;

        // Try to get port from service environment variables
        let service_port_env = format!(
            "{}_SERVICE_PORT",
            service_name.replace("-", "_").to_uppercase()
        );
        let port = std::env::var(&service_port_env)
            .or_else(|_| std::env::var("ZOEKTD_SERVICE_PORT"))
            .unwrap_or_else(|_| "8080".into());

        // Construct the pod-specific DNS name for StatefulSet
        let pod_dns = format!("{}.{}.svc.cluster.local", pod_name, service_name);

        tracing::info!(
            pod_name = %pod_name,
            service_name = %service_name,
            namespace = %namespace,
            pod_dns = %pod_dns,
            port = %port,
            protocol = %protocol,
            "discovered statefulset pod endpoint"
        );

        Some(format!("{}://{}:{}", protocol, pod_dns, port))
    }
}

/// Simple Node that periodically tries to acquire leases for known remote repos and then
/// calls the provided indexer to index them.
pub struct Node<I: Indexer> {
    config: NodeConfig,
    lease_mgr: LeaseManager,
    repos: Arc<RwLock<Vec<RemoteRepo>>>,
    indexer: Arc<I>,
}

impl<I: Indexer> Node<I> {
    pub fn new(config: NodeConfig, lease_mgr: LeaseManager, indexer: I) -> Self {
        Self {
            config,
            lease_mgr,
            repos: Arc::new(RwLock::new(Vec::new())),
            indexer: Arc::new(indexer),
        }
    }

    pub fn add_remote(&self, repo: RemoteRepo) {
        self.repos.write().push(repo);
    }

    /// Run the node loop for `duration`. Async version for use in async apps/tests.
    pub async fn run_for(&self, duration: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        // If this is an indexer with an endpoint, register it
        if self.config.node_type == NodeType::Indexer {
            if let Some(ref endpoint) = self.config.endpoint {
                tracing::info!(node_id = %self.config.id, endpoint = %endpoint, "registering indexer endpoint");
                let _ = self
                    .lease_mgr
                    .set_indexer_endpoint(&self.config.id, endpoint)
                    .await;
            }
        }
        while start.elapsed() < duration {
            // Periodically update endpoint if indexer
            if self.config.node_type == NodeType::Indexer && self.config.endpoint.is_some() {
                let _ = self
                    .lease_mgr
                    .set_indexer_endpoint(&self.config.id, self.config.endpoint.as_ref().unwrap())
                    .await;
            }
            let repos = self.repos.read().clone();
            for repo in repos {
                // If the repo git_url points to a local path (absolute path or file://) we skip
                // the lease acquisition and index directly. This makes local dev workflows fast
                // and avoids requiring Redis/leases for local repos.
                let git_url = repo.git_url.clone();
                let is_local =
                    PathBuf::from(&git_url).is_absolute() || git_url.starts_with("file://");

                if is_local {
                    tracing::info!(repo = %repo.name, "local repo, indexing without lease");
                    let repo_for_index = repo.clone();
                    let indexer = self.indexer.clone();

                    // Log ownership/permission bits for debugging prior to indexing.
                    log_path_permissions(&repo_for_index.git_url);

                    let index_started = std::time::Instant::now();
                    // run the synchronous indexer in a blocking task
                    let index_result = match tokio::task::spawn_blocking(move || {
                        indexer.index_repo(PathBuf::from(&repo_for_index.git_url))
                    })
                    .await
                    {
                        Ok(r) => r,
                        Err(e) => Err(anyhow::anyhow!(e.to_string())),
                    };

                    match index_result {
                        Ok(index) => {
                            tracing::info!(repo = %repo.name, docs = index.doc_count(), "indexed");
                            let dur = index_started.elapsed();
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            let dur_ms = dur.as_millis() as i64;
                            let mem_est = index.total_scanned_bytes() as i64;
                            // best-effort: persist meta (no lease holder semantics for local)
                            tracing::debug!(repo=%repo.name, "about to set repo meta (local)");
                            let _ = self
                                .lease_mgr
                                .set_repo_meta(&repo.name, now_ms, dur_ms, mem_est, &self.config.id)
                                .await;
                        }
                        Err(e) => {
                            tracing::warn!(repo = %repo.name, error = %e, "failed to index");
                        }
                    }
                    continue;
                }

                // For remote (non-local) repos, also log path metadata before attempting to acquire a lease
                // so permission/ownership issues are visible in logs.
                log_path_permissions(&git_url);

                if self
                    .lease_mgr
                    .try_acquire(&repo, self.config.id.clone(), self.config.lease_ttl)
                    .await
                {
                    tracing::info!(repo = %repo.name, "acquired lease, indexing");
                    // Run indexing in a blocking task (indexer is synchronous). While indexing
                    // runs, periodically renew the lease so long-running indexing doesn't expire it.
                    let repo_for_index = repo.clone();
                    let holder = self.config.id.clone();
                    let lease_mgr = self.lease_mgr.clone();
                    let indexer = self.indexer.clone();

                    let index_started = std::time::Instant::now();
                    let mut index_handle = tokio::task::spawn_blocking(move || {
                        indexer.index_repo(PathBuf::from(&repo_for_index.git_url))
                    });

                    // Renewal loop: wake up every (ttl/3) seconds to renew; stop when indexing completes.
                    let renew_interval = std::cmp::max(1, self.config.lease_ttl.as_secs() / 3);
                    let mut renew_tick =
                        tokio::time::interval(tokio::time::Duration::from_secs(renew_interval));
                    renew_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                    let mut indexing_done = false;
                    let mut index_result = Err(anyhow::anyhow!("indexing not run"));

                    while !indexing_done {
                        tokio::select! {
                            biased;
                            _ = renew_tick.tick() => {
                                // try renew; if renewal fails we stop renewing (lease may be lost)
                                let _ = lease_mgr.renew(&repo, holder.clone(), self.config.lease_ttl).await;
                            }
                            res = &mut index_handle => {
                                indexing_done = true;
                                match res {
                                    Ok(r) => index_result = r,
                                    Err(e) => index_result = Err(anyhow::anyhow!(e.to_string())),
                                }
                            }
                        }
                    }

                    match index_result {
                        Ok(index) => {
                            tracing::info!(repo = %repo.name, docs = index.doc_count(), "indexed");
                            // compute duration and record meta in redis
                            let dur = index_started.elapsed();
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            let dur_ms = dur.as_millis() as i64;
                            // best-effort: persist meta
                            let mem_est = index.total_scanned_bytes() as i64;
                            tracing::debug!(repo=%repo.name, "about to set repo meta (remote)");
                            let _ = self
                                .lease_mgr
                                .set_repo_meta(&repo.name, now_ms, dur_ms, mem_est, &self.config.id)
                                .await;
                        }
                        Err(e) => {
                            tracing::warn!(repo = %repo.name, error = %e, "failed to index");
                            self.lease_mgr.release(&repo, &self.config.id).await;
                        }
                    }
                }
            }
            tokio::time::sleep(self.config.poll_interval).await;
        }
        Ok(())
    }
}

/// Log basic ownership and permission bits for the provided path for debugging.
fn log_path_permissions(path: &str) {
    let p = PathBuf::from(path);
    // Only attempt to stat when the path looks like a local filesystem path.
    // Remote git URLs (http(s)://, git@, ssh://) will not be statted.
    let is_remote = looks_like_remote_git_url(path);
    let is_file_scheme = path.starts_with("file://");

    if is_remote && !is_file_scheme {
        tracing::debug!(path = %path, "skipping stat for remote git URL");
        return;
    }

    match std::fs::metadata(&p) {
        Ok(md) => {
            let mode = md.permissions().mode();
            let uid = md.uid();
            let gid = md.gid();
            tracing::info!(path = %path, uid = uid, gid = gid, mode = format_args!("{:o}", mode), "path metadata");
        }
        Err(e) => {
            tracing::warn!(path = %path, error = %e, "failed to stat path");
        }
    }
}

// Lightweight helper used by `log_path_permissions` and unit tests.
fn looks_like_remote_git_url(path: &str) -> bool {
    path.starts_with("http://")
        || path.starts_with("https://")
        || path.starts_with("git@")
        || path.starts_with("ssh://")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Once;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tracing_subscriber::EnvFilter;

    /// Test helper to manage environment variables and ensure proper cleanup
    struct EnvGuard {
        original_values: HashMap<String, Option<String>>,
    }

    impl EnvGuard {
        fn new() -> Self {
            Self {
                original_values: HashMap::new(),
            }
        }

        fn save_and_clear(&mut self, vars: &[&str]) {
            for &var in vars {
                let original = std::env::var(var).ok();
                self.original_values.insert(var.to_string(), original);
                std::env::remove_var(var);
            }
        }

        fn set(&self, var: &str, value: &str) {
            std::env::set_var(var, value);
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // Restore original environment variable values
            for (var, original_value) in &self.original_values {
                match original_value {
                    Some(value) => std::env::set_var(var, value),
                    None => std::env::remove_var(var),
                }
            }
        }
    }

    struct FakeIndexer {
        count: Arc<AtomicUsize>,
    }

    // Initialize tracing only once for tests so logs are visible when running
    // `cargo test -- --nocapture`. Respects RUST_LOG when set; defaults to debug
    // to keep output readable.
    static TRACING_INIT: Once = Once::new();
    fn init_test_logging() {
        TRACING_INIT.call_once(|| {
            // Prefer env when set; fall back to a quieter default to keep output readable.
            let filter =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
            let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
        });
    }

    impl FakeIndexer {
        fn new() -> Self {
            Self {
                count: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_count(c: Arc<AtomicUsize>) -> Self {
            Self { count: c }
        }
    }

    impl Indexer for FakeIndexer {
        fn index_repo(&self, _repo_path: PathBuf) -> Result<InMemoryIndex> {
            let _n = self.count.fetch_add(1, Ordering::SeqCst);
            let idx =
                zoekt_rs::test_helpers::make_index_with_trigrams(vec![], "fake", vec![], None);
            Ok(idx)
        }
    }

    #[tokio::test]
    async fn node_acquires_and_indexes() -> Result<()> {
        init_test_logging();
        tracing::info!("TEST START: node_acquires_and_indexes");
        let lease = LeaseManager::new().await;
        let cfg = NodeConfig {
            id: "test-node".into(),
            lease_ttl: Duration::from_secs(2),
            poll_interval: Duration::from_millis(10),
            node_type: NodeType::Indexer,
            endpoint: None,
        };
        let node = Node::new(cfg, lease.clone(), FakeIndexer::new());
        let repo = RemoteRepo {
            name: "r1".into(),
            // Use a non-local URL so the node attempts to acquire a lease in tests.
            git_url: "https://example.com/fake-repo.git".into(),
        };
        node.add_remote(repo.clone());
        node.run_for(Duration::from_millis(50)).await?;
        let l = lease.get_lease(&repo).await.expect("lease should exist");
        assert_eq!(l.holder, "test-node");
        tracing::info!("TEST END: node_acquires_and_indexes");
        Ok(())
    }

    #[tokio::test]
    async fn node_records_repo_meta_on_index() -> Result<()> {
        init_test_logging();
        // This test requires Redis to be available; skip when REDIS_URL is not set so local runs stay fast.
        if env::var("REDIS_URL").is_err() {
            tracing::info!("TEST SKIP: node_records_repo_meta_on_index (no REDIS_URL)");
            return Ok(());
        }
        tracing::info!("TEST START: node_records_repo_meta_on_index");

        // Create a lease manager and set a meta callback to capture values
        let lease = LeaseManager::new().await;
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        lease.set_meta_sender(tx);

        let cfg = NodeConfig {
            id: "test-node-meta".into(),
            lease_ttl: Duration::from_secs(2),
            poll_interval: Duration::from_millis(10),
            node_type: NodeType::Indexer,
            endpoint: None,
        };

        // Fake indexer that sleeps briefly to simulate work
        struct SleepIndexer;
        impl Indexer for SleepIndexer {
            fn index_repo(&self, _repo_path: PathBuf) -> Result<InMemoryIndex> {
                std::thread::sleep(std::time::Duration::from_millis(30));
                let idx =
                    zoekt_rs::test_helpers::make_index_with_trigrams(vec![], "fake", vec![], None);
                Ok(idx)
            }
        }

        let node = Node::new(cfg, lease.clone(), SleepIndexer);
        let repo = RemoteRepo {
            name: "r-meta".into(),
            git_url: "/tmp/fake-repo-meta".into(),
        };
        node.add_remote(repo.clone());
        // Run the node for a short duration to trigger indexing
        let run_result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            node.run_for(Duration::from_millis(50)),
        )
        .await;
        match run_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("node run failed: {}", e),
            Err(_) => panic!("node run timed out after 10 seconds"),
        }

        // Wait for meta message and verify values look reasonable
        let rec = match tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await {
            Ok(Some(r)) => r,
            Ok(None) => panic!("meta sender channel closed unexpectedly"),
            Err(_) => panic!("timed out waiting for meta message after 5 seconds"),
        };
        assert_eq!(rec.0, "r-meta");
        // last_indexed should be close to now (within 10s)
        let now_ms = chrono::Utc::now().timestamp_millis();
        assert!(rec.1 <= now_ms && rec.1 > now_ms - 10000);
        // duration should be positive
        assert!(rec.2 > 0);
        // memory estimate should be non-negative
        assert!(rec.3 >= 0);
        // leased node should match config id
        assert_eq!(rec.4, "test-node-meta");

        tracing::info!("TEST END: node_records_repo_meta_on_index");
        Ok(())
    }

    // Concurrent integration test that requires a real Redis instance. The test will be
    // skipped when REDIS_URL is not set so local runs stay fast.
    #[tokio::test]
    async fn concurrent_nodes_contend_for_lease() -> Result<()> {
        init_test_logging();
        if env::var("REDIS_URL").is_err() {
            tracing::info!("TEST SKIP: concurrent_nodes_contend_for_lease (no REDIS_URL)");
            return Ok(());
        }
        tracing::info!("TEST START: concurrent_nodes_contend_for_lease");

        let lease = LeaseManager::new().await;
        let repo = RemoteRepo {
            name: "r-contend".into(),
            // Use a non-local URL so nodes contend for a Redis-backed lease in this test
            git_url: "https://example.com/fake-repo-contend.git".into(),
        };

        // Two fake indexers that count how many times they were invoked (i.e. wins)
        let a_cnt = Arc::new(AtomicUsize::new(0));
        let b_cnt = Arc::new(AtomicUsize::new(0));
        let idx_a = FakeIndexer::with_count(a_cnt.clone());
        let idx_b = FakeIndexer::with_count(b_cnt.clone());

        let cfg_a = NodeConfig {
            id: "node-a".into(),
            lease_ttl: Duration::from_secs(1),
            poll_interval: Duration::from_millis(5),
            node_type: NodeType::Indexer,
            endpoint: None,
        };
        let cfg_b = NodeConfig {
            id: "node-b".into(),
            lease_ttl: Duration::from_secs(1),
            poll_interval: Duration::from_millis(5),
            node_type: NodeType::Indexer,
            endpoint: None,
        };

        let node_a = Node::new(cfg_a, lease.clone(), idx_a);
        let node_b = Node::new(cfg_b, lease.clone(), idx_b);

        // Both nodes observe the same repo and will run their loops, placing bids when they fail to acquire.
        node_a.add_remote(repo.clone());
        node_b.add_remote(repo.clone());

        let t1 = tokio::spawn(async move { node_a.run_for(Duration::from_millis(200)).await });
        let t2 = tokio::spawn(async move { node_b.run_for(Duration::from_millis(200)).await });

        let _ = tokio::join!(t1, t2);

        let a_wins = a_cnt.load(Ordering::SeqCst);
        let b_wins = b_cnt.load(Ordering::SeqCst);
        assert!(
            a_wins + b_wins > 0,
            "expected some successful acquisitions, got {}+{}",
            a_wins,
            b_wins
        );
        tracing::info!("TEST END: concurrent_nodes_contend_for_lease");
        Ok(())
    }

    #[test]
    fn test_looks_like_remote_git_url() {
        // remote forms
        assert!(super::looks_like_remote_git_url(
            "https://example.com/repo.git"
        ));
        assert!(super::looks_like_remote_git_url("http://example.com/repo"));
        assert!(super::looks_like_remote_git_url(
            "git@github.com:owner/repo.git"
        ));
        assert!(super::looks_like_remote_git_url(
            "ssh://git@host/owner/repo.git"
        ));

        // local/file forms
        assert!(!super::looks_like_remote_git_url("/tmp/repo"));
        assert!(!super::looks_like_remote_git_url("file:///tmp/repo"));
        assert!(!super::looks_like_remote_git_url("./relative/path"));
    }

    #[test]
    fn test_node_type_from_str() {
        assert_eq!("indexer".parse::<NodeType>().unwrap(), NodeType::Indexer);
        assert_eq!("INDEXER".parse::<NodeType>().unwrap(), NodeType::Indexer);
        assert_eq!("admin".parse::<NodeType>().unwrap(), NodeType::Admin);
        assert_eq!("search".parse::<NodeType>().unwrap(), NodeType::Search);

        assert!("invalid".parse::<NodeType>().is_err());
        assert!("".parse::<NodeType>().is_err());
    }

    #[test]
    fn test_discover_statefulset_pod_endpoint_success() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "my_service_SERVICE_HOST",
            "my_service_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
        ]);

        // Set up environment variables to simulate a StatefulSet pod
        env_guard.set("POD_NAME", "zoekt-indexer-2");
        env_guard.set("ZOEKTD_SERVICE_NAME", "zoekt-indexer");
        env_guard.set("ZOEKTD_SERVICE_PORT", "8080");
        env_guard.set("ZOEKTD_SERVICE_PROTOCOL", "http");

        let result = NodeConfig::discover_statefulset_pod_endpoint("http");
        assert_eq!(
            result,
            Some("http://zoekt-indexer-2.zoekt-indexer.svc.cluster.local:8080".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[test]
    fn test_discover_statefulset_pod_endpoint_hostname_fallback() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "my_service_SERVICE_HOST",
            "my_service_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
        ]);

        // Test using HOSTNAME instead of POD_NAME
        env_guard.set("HOSTNAME", "zoekt-indexer-1");
        env_guard.set("ZOEKTD_SERVICE_NAME", "zoekt-indexer");
        env_guard.set("ZOEKTD_SERVICE_PORT", "9090");

        let result = NodeConfig::discover_statefulset_pod_endpoint("https");
        assert_eq!(
            result,
            Some("https://zoekt-indexer-1.zoekt-indexer.svc.cluster.local:9090".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[test]
    fn test_discover_statefulset_pod_endpoint_infer_service_name() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "my_service_SERVICE_HOST",
            "my_service_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
        ]);

        // Test inferring service name from pod name
        env_guard.set("POD_NAME", "my-indexer-0");
        env_guard.set("ZOEKTD_SERVICE_PORT", "8080");

        let result = NodeConfig::discover_statefulset_pod_endpoint("http");
        assert_eq!(
            result,
            Some("http://my-indexer-0.my-indexer.svc.cluster.local:8080".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[test]
    fn test_discover_statefulset_pod_endpoint_no_pod_name() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&["POD_NAME", "HOSTNAME"]);

        let result = NodeConfig::discover_statefulset_pod_endpoint("http");
        assert_eq!(result, None);

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[test]
    fn test_discover_statefulset_pod_endpoint_no_service_name() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "my_service_SERVICE_HOST",
            "my_service_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
        ]);

        // Test when pod name is available but no service name can be determined
        // Use a pod name that doesn't contain '-' to avoid service name inference
        env_guard.set("POD_NAME", "singlepodnoordinal");
        env_guard.set("ZOEKTD_SERVICE_PORT", "8080");

        let result = NodeConfig::discover_statefulset_pod_endpoint("http");
        assert_eq!(result, None);

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[test]
    fn test_discover_kubernetes_endpoint_manual_override() {
        // Clear any existing environment variables that might interfere
        std::env::remove_var("ZOEKTD_ENDPOINT");
        std::env::remove_var("POD_NAME");
        std::env::remove_var("HOSTNAME");
        std::env::remove_var("ZOEKTD_SERVICE_NAME");
        std::env::remove_var("ZOEKTD_SERVICE_PORT");
        std::env::remove_var("ZOEKTD_SERVICE_PROTOCOL");
        std::env::remove_var("ZOEKTD_NODE_TYPE");

        // Test manual override in NodeConfig::default() (not in discover_kubernetes_endpoint directly)
        std::env::set_var("ZOEKTD_ENDPOINT", "http://custom-endpoint:9999");
        std::env::set_var("ZOEKTD_NODE_TYPE", "indexer");

        let config = NodeConfig::default();
        assert_eq!(
            config.endpoint,
            Some("http://custom-endpoint:9999".to_string())
        );

        // Clean up
        std::env::remove_var("ZOEKTD_ENDPOINT");
        std::env::remove_var("ZOEKTD_NODE_TYPE");
    }

    #[test]
    fn test_discover_kubernetes_endpoint_service_name_priority() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
        ]);

        // Test ZOEKTD_SERVICE_NAME takes priority over auto-discovery
        env_guard.set("ZOEKTD_SERVICE_NAME", "my-service");
        env_guard.set("MY_SERVICE_SERVICE_HOST", "10.0.1.1");
        env_guard.set("MY_SERVICE_SERVICE_PORT", "8080");

        let result = NodeConfig::discover_kubernetes_endpoint();
        assert_eq!(result, Some("http://10.0.1.1:8080".to_string()));

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[test]
    fn test_discover_kubernetes_endpoint_statefulset_fallback() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "my_service_SERVICE_HOST",
            "my_service_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
        ]);

        // Test StatefulSet discovery as fallback when no service env vars
        env_guard.set("POD_NAME", "zoekt-indexer-0");
        env_guard.set("ZOEKTD_SERVICE_NAME", "zoekt-indexer");
        env_guard.set("ZOEKTD_SERVICE_PORT", "8080");

        let result = NodeConfig::discover_kubernetes_endpoint();
        assert_eq!(
            result,
            Some("http://zoekt-indexer-0.zoekt-indexer.svc.cluster.local:8080".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[test]
    fn test_discover_kubernetes_endpoint_service_env_vars() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
            "TESTSERVICE_SERVICE_HOST",
            "TESTSERVICE_SERVICE_PORT",
        ]);

        // Test auto-discovery from service environment variables
        // Use a simple service name to avoid conflicts
        env_guard.set("testservice_SERVICE_HOST", "10.0.2.1");
        env_guard.set("TESTSERVICE_SERVICE_PORT", "9090");

        let result = NodeConfig::discover_kubernetes_endpoint();
        assert_eq!(result, Some("http://10.0.2.1:9090".to_string()));

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[test]
    fn test_discover_kubernetes_endpoint_no_discovery() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
        ]);

        // Test when no discovery method works
        let result = NodeConfig::discover_kubernetes_endpoint();
        assert_eq!(result, None);

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[test]
    fn test_node_config_default_with_statefulset_env() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "ZOEKTD_NODE_TYPE",
        ]);

        // Test NodeConfig::default() with StatefulSet environment
        env_guard.set("POD_NAME", "zoekt-indexer-1");
        env_guard.set("ZOEKTD_SERVICE_NAME", "zoekt-indexer");
        env_guard.set("ZOEKTD_SERVICE_PORT", "8080");
        env_guard.set("ZOEKTD_NODE_TYPE", "indexer");

        let config = NodeConfig::default();
        assert_eq!(config.node_type, NodeType::Indexer);
        assert_eq!(
            config.endpoint,
            Some("http://zoekt-indexer-1.zoekt-indexer.svc.cluster.local:8080".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[test]
    fn test_node_config_default_manual_endpoint_override() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "ZOEKTD_NODE_TYPE",
        ]);

        // Test that manual endpoint override works in NodeConfig::default()
        env_guard.set("ZOEKTD_ENDPOINT", "http://manual-override:9999");
        env_guard.set("POD_NAME", "zoekt-indexer-0"); // This should be ignored

        let config = NodeConfig::default();
        assert_eq!(
            config.endpoint,
            Some("http://manual-override:9999".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }
}
