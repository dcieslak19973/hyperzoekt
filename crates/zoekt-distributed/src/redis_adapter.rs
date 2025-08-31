use async_trait::async_trait;
use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::Pool;

#[async_trait]
pub trait DynRedis: Send + Sync {
    async fn ping(&self) -> anyhow::Result<()>;
    async fn hgetall(&self, key: &str) -> anyhow::Result<Vec<(String, String)>>;
    async fn eval_i32(&self, script: &str, keys: &[&str], args: &[&str]) -> anyhow::Result<i32>;
    async fn hset(&self, key: &str, field: &str, value: &str) -> anyhow::Result<()>;
    async fn hget(&self, key: &str, field: &str) -> anyhow::Result<Option<String>>;
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
