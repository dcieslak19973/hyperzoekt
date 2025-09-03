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

use async_trait::async_trait;
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::{Config as RedisConfig, Pool};

/// Create a Redis pool with authentication support from environment variables.
///
/// This function handles Redis authentication by checking for:
/// - REDIS_URL: Base Redis URL (may already contain auth)
/// - REDIS_USERNAME: Optional username for authentication
/// - REDIS_PASSWORD: Optional password for authentication
///
/// If REDIS_URL doesn't contain authentication and REDIS_USERNAME/REDIS_PASSWORD
/// are provided, they will be automatically injected into the URL.
///
/// If no REDIS_URL is provided but authentication credentials exist,
/// a default URL will be constructed with the credentials.
///
/// Returns None if no valid Redis configuration is found.
pub fn create_redis_pool() -> Option<Pool> {
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

#[async_trait]
pub trait DynRedis: Send + Sync {
    async fn ping(&self) -> anyhow::Result<()>;
    async fn hgetall(&self, key: &str) -> anyhow::Result<Vec<(String, String)>>;
    async fn eval_i32(&self, script: &str, keys: &[&str], args: &[&str]) -> anyhow::Result<i32>;
    async fn eval_vec_string(
        &self,
        script: &str,
        keys: &[&str],
        args: &[&str],
    ) -> anyhow::Result<Vec<String>>;
    async fn hset(&self, key: &str, field: &str, value: &str) -> anyhow::Result<()>;
    async fn hget(&self, key: &str, field: &str) -> anyhow::Result<Option<String>>;
    async fn get(&self, key: &str) -> anyhow::Result<Option<String>>;
    async fn hdel(&self, key: &str, field: &str) -> anyhow::Result<bool>;
}

pub struct RealRedis {
    pub pool: Pool,
}

#[async_trait]
impl DynRedis for RealRedis {
    async fn ping(&self) -> anyhow::Result<()> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let _res: String = deadpool_redis::redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(())
    }

    async fn hgetall(&self, key: &str) -> anyhow::Result<Vec<(String, String)>> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let entries: Vec<(String, String)> = conn
            .hgetall(key)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(entries)
    }

    async fn eval_i32(&self, script: &str, keys: &[&str], args: &[&str]) -> anyhow::Result<i32> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let mut cmd = deadpool_redis::redis::cmd("EVAL");
        cmd.arg(script).arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        for a in args {
            cmd.arg(a);
        }
        let v: i32 = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(v)
    }

    async fn eval_vec_string(
        &self,
        script: &str,
        keys: &[&str],
        args: &[&str],
    ) -> anyhow::Result<Vec<String>> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let mut cmd = deadpool_redis::redis::cmd("EVAL");
        cmd.arg(script).arg(keys.len());
        for k in keys {
            cmd.arg(k);
        }
        for a in args {
            cmd.arg(a);
        }
        let v: Vec<String> = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(v)
    }

    async fn hset(&self, key: &str, field: &str, value: &str) -> anyhow::Result<()> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let _: () = conn
            .hset(key, field, value)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(())
    }

    async fn hget(&self, key: &str, field: &str) -> anyhow::Result<Option<String>> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let v: Option<String> = conn
            .hget(key, field)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(v)
    }

    async fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let v: Option<String> = conn
            .get(key)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(v)
    }

    async fn hdel(&self, key: &str, field: &str) -> anyhow::Result<bool> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let n: i32 = conn
            .hdel(key, field)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(n > 0)
    }
}
