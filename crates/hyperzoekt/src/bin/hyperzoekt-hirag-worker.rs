use anyhow::Result;
use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
// ...existing code...
use deadpool_redis::redis::AsyncCommands;
use hyperzoekt::db::connection::connect as hz_connect;
use log::{info, warn};
use serde_json::Value;
use std::env;
use std::net::SocketAddr;
use std::sync::{atomic::AtomicU64, Arc};
use std::time::Duration;

#[derive(Debug, Clone)]
struct HiragWorkerMetrics {
    jobs_processed: Arc<AtomicU64>,
    jobs_failed: Arc<AtomicU64>,
    redis_errors: Arc<AtomicU64>,
}

impl HiragWorkerMetrics {
    fn new() -> Self {
        Self {
            jobs_processed: Arc::new(AtomicU64::new(0)),
            jobs_failed: Arc::new(AtomicU64::new(0)),
            redis_errors: Arc::new(AtomicU64::new(0)),
        }
    }
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn metrics_handler(State(metrics): axum::extract::State<Arc<HiragWorkerMetrics>>) -> String {
    let jobs_processed = metrics
        .jobs_processed
        .load(std::sync::atomic::Ordering::Relaxed);
    let jobs_failed = metrics
        .jobs_failed
        .load(std::sync::atomic::Ordering::Relaxed);
    let redis_errors = metrics
        .redis_errors
        .load(std::sync::atomic::Ordering::Relaxed);

    format!(
        "# HELP hyperzoekt_hirag_worker_jobs_processed_total Total number of HiRAG jobs processed\n\
         # TYPE hyperzoekt_hirag_worker_jobs_processed_total counter\n\
         hyperzoekt_hirag_worker_jobs_processed_total {}\n\
         # HELP hyperzoekt_hirag_worker_jobs_failed_total Total number of HiRAG jobs that failed\n\
         # TYPE hyperzoekt_hirag_worker_jobs_failed_total counter\n\
         hyperzoekt_hirag_worker_jobs_failed_total {}\n\
         # HELP hyperzoekt_hirag_worker_redis_errors_total Total number of Redis errors\n\
         # TYPE hyperzoekt_hirag_worker_redis_errors_total counter\n\
         hyperzoekt_hirag_worker_redis_errors_total {}\n",
        jobs_processed, jobs_failed, redis_errors
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    info!("hyperzoekt-hirag-worker starting up");

    // Optionally enable SurrealDB client debug output for this worker if
    // HZ_HIRAG_VERBOSE_SQL is truthy (1/true/yes). This avoids forcing
    // SURREAL_DEBUG globally when not desired; the toggle only affects this
    // worker at startup.
    let hirag_verbose = std::env::var("HZ_HIRAG_VERBOSE_SQL")
        .ok()
        .map(|v| {
            let v = v.to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes"
        })
        .unwrap_or(false);
    if hirag_verbose {
        std::env::set_var("SURREAL_DEBUG", "1");
        info!("HZ_HIRAG_VERBOSE_SQL enabled; setting SURREAL_DEBUG=1 for hirag worker");
    } else {
        info!(
            "HZ_HIRAG_VERBOSE_SQL not set; leaving SURREAL_DEBUG as-is: {:?}",
            std::env::var("SURREAL_DEBUG").ok()
        );
    }

    let queue_key = env::var("HZ_HIRAG_JOBS_QUEUE").unwrap_or_else(|_| "hirag_jobs".to_string());
    let processing_prefix = env::var("HZ_HIRAG_PROCESSING_PREFIX")
        .unwrap_or_else(|_| "zoekt:hirag_processing".to_string());
    let processing_ttl: u64 = env::var("HZ_HIRAG_PROCESSING_TTL_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);

    let pool = match zoekt_distributed::redis_adapter::create_redis_pool() {
        Some(p) => p,
        None => {
            warn!("Redis not configured; set REDIS_URL or credentials");
            std::process::exit(1);
        }
    };

    // Initialize metrics and HTTP server (health + metrics)
    let metrics = Arc::new(HiragWorkerMetrics::new());
    let port = env::var("HZ_HIRAG_WORKER_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8084);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    #[derive(serde::Deserialize)]
    struct DebugQuery {
        n: Option<usize>,
    }

    async fn debug_create_responses(Query(q): Query<DebugQuery>) -> Json<serde_json::Value> {
        let n = q.n.unwrap_or(20);
        let items = hyperzoekt::hirag::get_create_responses(n);
        Json(serde_json::Value::Array(items))
    }

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/debug/create_responses", get(debug_create_responses))
        .with_state(Arc::clone(&metrics));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("HTTP server listening on {}", addr);
    let server = axum::serve(listener, app);

    tokio::spawn(async move {
        let _ = server.await;
    });

    loop {
        // Recover expired leases
        if let Err(e) = recover_expired(&pool, &queue_key, &processing_prefix).await {
            warn!("hirag recover_expired error: {}", e);
            metrics
                .redis_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                warn!("redis pool.get failed: {}", e);
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };

        let consumer_id = format!("hirag-{}", std::process::id());
        // Start by doing a blocking BRPOPLPUSH to create the first processing key
        let start_pop = std::time::Instant::now();
        let processing_key = format!(
            "{}:{}:{}",
            processing_prefix,
            consumer_id,
            start_pop.elapsed().as_millis()
        );

        // We'll try to pop one blocking, then batch additional items with RPOPLPUSH
        let mut batch_msgs: Vec<(String, String)> = Vec::new();
        let msg_first = match conn.brpoplpush(&queue_key, &processing_key, 10.0).await {
            Ok(v) => v,
            Err(e) => {
                warn!("hirag brpoplpush error: {}", e);
                None
            }
        };

        if let Some(first_msg) = msg_first {
            // set TTL on this processing key
            if let Err(e) = conn
                .expire::<_, ()>(&processing_key, processing_ttl as i64)
                .await
            {
                warn!("expire error for {}: {}", processing_key, e);
            }
            info!(
                "got hirag job (first): {} -> processing_key={}",
                first_msg, processing_key
            );
            batch_msgs.push((processing_key.clone(), first_msg));

            // Try to quickly accumulate more (non-blocking) up to 10 items
            while batch_msgs.len() < 10 {
                let k2 = format!(
                    "{}:{}:{}",
                    processing_prefix,
                    consumer_id,
                    start_pop.elapsed().as_millis()
                );
                match conn.rpoplpush(&queue_key, &k2).await {
                    Ok(Some(m)) => {
                        if let Err(e) = conn.expire::<_, ()>(&k2, processing_ttl as i64).await {
                            warn!("expire error for {}: {}", k2, e);
                        }
                        info!("batched hirag job: {} -> processing_key={}", m, k2);
                        batch_msgs.push((k2, m));
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!("rpoplpush error while batching: {}", e);
                        break;
                    }
                }
            }
        }

        if !batch_msgs.is_empty() {
            for (proc_key, msg) in batch_msgs.iter() {
                info!("processing hirag message from {}: {}", proc_key, msg);

                // parse job
                let job: Value = match serde_json::from_str(msg) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("malformed hirag job payload: {} -- {}", e, msg);
                        let _: () = conn.del(proc_key).await.unwrap_or(());
                        metrics
                            .jobs_failed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    }
                };

                // Connect to Surreal and run build_first_layer
                let ns = env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
                let db = env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());
                match hz_connect(&None, &None, &None, &ns, &db).await {
                    Ok(conn_s) => {
                        info!(
                            "connected to SurrealDB: url={:?} ns={} db={}",
                            std::env::var("SURREALDB_URL").ok(),
                            ns,
                            db
                        );
                        info!("SURREAL_DEBUG={:?}", std::env::var("SURREAL_DEBUG").ok());
                        // If the job contains repo/commit, run per-repo incremental builder.
                        if let Some(repo) = job.get("repo").and_then(|v| v.as_str()) {
                            let commit = job.get("commit").and_then(|v| v.as_str());
                            // Default to 0 which signals the builder to pick an organic k
                            // (heuristic: sqrt(N) with min=2).
                            let k = job.get("k").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                            info!(
                                "running per-repo hirag build for repo='{}' commit={:?} k={}",
                                repo, commit, k
                            );
                            if let Err(e) = hyperzoekt::hirag::build_first_layer_for_repo(
                                &conn_s, repo, commit, k,
                            )
                            .await
                            {
                                warn!("hirag per-repo build failed: {}", e);
                                metrics
                                    .jobs_failed
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            } else {
                                info!(
                                    "hirag per-repo build completed for repo='{}' (k={})",
                                    repo, k
                                );

                                // Now build all hierarchical layers (layer1, layer2, ...) until termination
                                let min_clusters =
                                    job.get("min_clusters")
                                        .and_then(|v| v.as_u64())
                                        .unwrap_or(3) as usize;
                                info!(
                                    "running hierarchical layers build for repo='{}' k={} min_clusters={}",
                                    repo, k, min_clusters
                                );
                                if let Err(e) = hyperzoekt::hirag::build_hierarchical_layers(
                                    &conn_s,
                                    Some(repo),
                                    k,
                                    min_clusters,
                                )
                                .await
                                {
                                    warn!("hirag hierarchical layers build failed: {}", e);
                                    metrics
                                        .jobs_failed
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                } else {
                                    info!(
                                        "hirag hierarchical layers build completed for repo='{}'",
                                        repo
                                    );
                                }

                                metrics
                                    .jobs_processed
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        } else {
                            // Fallback to global build if no repo provided
                            // Default to 0 (auto). The builder will compute an appropriate
                            // branching factor based on the number of embeddings found.
                            let k = job.get("k").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                            if let Err(e) = hyperzoekt::hirag::build_first_layer(&conn_s, k).await {
                                warn!("hirag build failed: {}", e);
                                metrics
                                    .jobs_failed
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            } else {
                                info!("hirag build_first_layer completed (k={})", k);

                                // Now build all hierarchical layers globally
                                let min_clusters =
                                    job.get("min_clusters")
                                        .and_then(|v| v.as_u64())
                                        .unwrap_or(3) as usize;
                                info!(
                                    "running global hierarchical layers build k={} min_clusters={}",
                                    k, min_clusters
                                );
                                if let Err(e) = hyperzoekt::hirag::build_hierarchical_layers(
                                    &conn_s,
                                    None,
                                    k,
                                    min_clusters,
                                )
                                .await
                                {
                                    warn!("hirag hierarchical layers build failed: {}", e);
                                    metrics
                                        .jobs_failed
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                } else {
                                    info!("hirag hierarchical layers build completed");
                                }

                                metrics
                                    .jobs_processed
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("failed to connect to surrealdb: {}", e);
                        metrics
                            .jobs_failed
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }

                // remove processing key after each processed job
                let _: () = conn.del(proc_key).await.unwrap_or(());
                info!("deleted processing key {} after processing", proc_key);
            }
        } else {
            // idle sleep
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn recover_expired(
    pool: &deadpool_redis::Pool,
    queue_key: &str,
    processing_prefix: &str,
) -> Result<(), deadpool_redis::redis::RedisError> {
    let mut conn = match pool.get().await {
        Ok(c) => c,
        Err(e) => {
            warn!("recover_expired: redis pool.get failed: {}", e);
            return Ok(());
        }
    };
    let keys: Vec<String> = conn.keys(format!("{}*", processing_prefix)).await?;
    for k in keys {
        let ttl: i64 = conn.ttl(&k).await?;
        if ttl <= 0 {
            if let Ok(Some(v)) = conn.get::<_, Option<String>>(&k).await {
                let _: () = conn.lpush::<_, _, ()>(queue_key, v).await?;
            }
            let _: () = conn.del::<_, ()>(&k).await?;
        }
    }
    Ok(())
}
