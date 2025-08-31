//! Minimal "distributed" layer prototype for zoekt-rs.
//!
//! Goals in this initial patch:
//! - Provide a small in-process lease manager that tracks "remote repos"
//! - Provide a Node that periodically tries to acquire leases and runs a provided indexer
//! - Keep the API small so tests can plug a fake indexer easily

use anyhow::Result;
use base64::Engine;
use chrono::{DateTime, Utc};
use deadpool_redis::redis::{self, RedisResult};
use deadpool_redis::{Config as RedisConfig, Pool};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub use zoekt_rs::InMemoryIndex;
mod config;
pub use config::{load_node_config, MergeOpts};

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
        }
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
}

impl Default for NodeConfig {
    fn default() -> Self {
        // Read configuration from environment variables when present. Useful for k8s.
        // Env vars:
        // - ZOEKTD_NODE_ID
        // - ZOEKTD_LEASE_TTL_SECONDS
        // - ZOEKTD_POLL_INTERVAL_SECONDS
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

        Self {
            id,
            lease_ttl,
            poll_interval,
            node_type: std::env::var("ZOEKTD_NODE_TYPE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(NodeType::Indexer),
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
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "indexer" => Ok(NodeType::Indexer),
            "admin" => Ok(NodeType::Admin),
            "search" => Ok(NodeType::Search),
            _ => Err(()),
        }
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
        while start.elapsed() < duration {
            let repos = self.repos.read().clone();
            for repo in repos {
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
                            tracing::info!(repo = %repo.name, docs = index.doc_count(), "indexed")
                        }
                        Err(_) => {
                            tracing::warn!(repo = %repo.name, "failed to index");
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeIndexer {
        count: AtomicUsize,
    }

    impl FakeIndexer {
        fn new() -> Self {
            Self {
                count: AtomicUsize::new(0),
            }
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
        let lease = LeaseManager::new().await;
        let cfg = NodeConfig {
            id: "test-node".into(),
            lease_ttl: Duration::from_secs(2),
            poll_interval: Duration::from_millis(10),
            node_type: NodeType::Indexer,
        };
        let node = Node::new(cfg, lease.clone(), FakeIndexer::new());
        let repo = RemoteRepo {
            name: "r1".into(),
            git_url: "/tmp/fake-repo".into(),
        };
        node.add_remote(repo.clone());
        node.run_for(Duration::from_millis(50)).await?;
        let l = lease.get_lease(&repo).await.expect("lease should exist");
        assert_eq!(l.holder, "test-node");
        Ok(())
    }

    // Concurrent integration test that requires a real Redis instance. The test will be
    // skipped when REDIS_URL is not set so local runs stay fast.
    #[tokio::test]
    async fn concurrent_nodes_contend_for_lease() -> Result<()> {
        if env::var("REDIS_URL").is_err() {
            eprintln!("skipping redis integration test; set REDIS_URL to run it");
            return Ok(());
        }

        let lease = LeaseManager::new().await;
        let repo = RemoteRepo {
            name: "r-contend".into(),
            git_url: "/tmp/fake-repo-contend".into(),
        };

        // spawn two tasks that will both try to acquire the same lease repeatedly
        let lease1 = lease.clone();
        let lease2 = lease.clone();

        let repo1 = repo.clone();
        let repo2 = repo.clone();

        let t1 = tokio::spawn(async move {
            let mut wins = 0u32;
            for _ in 0..20 {
                if lease1
                    .try_acquire(&repo1, "node-a".to_string(), Duration::from_secs(1))
                    .await
                {
                    wins += 1;
                    lease1.release(&repo1, "node-a").await;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            wins
        });

        let t2 = tokio::spawn(async move {
            let mut wins = 0u32;
            for _ in 0..20 {
                if lease2
                    .try_acquire(&repo2, "node-b".to_string(), Duration::from_secs(1))
                    .await
                {
                    wins += 1;
                    lease2.release(&repo2, "node-b").await;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            wins
        });

        let (a, b) = tokio::join!(t1, t2);
        let a = a.unwrap_or(0);
        let b = b.unwrap_or(0);

        // Both tasks attempted acquisition; at least one should have succeeded a few times.
        assert!(
            a + b > 0,
            "expected some successful acquisitions, got {}+{}",
            a,
            b
        );
        Ok(())
    }
}
