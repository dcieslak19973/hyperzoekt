use base64::Engine;
use chrono::{DateTime, Utc};
use deadpool_redis::redis::{self, AsyncCommands, RedisResult};
use deadpool_redis::{Config as RedisConfig, Pool};
use parking_lot::RwLock;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

// Type aliases to reduce clippy type-complexity warnings around the meta sender.
// MetaMsg: (name, last_indexed_ms, last_duration_ms, memory_bytes, leased_node)
type MetaMsg = (String, i64, i64, i64, String);
type MetaSender = Sender<MetaMsg>;

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
}
