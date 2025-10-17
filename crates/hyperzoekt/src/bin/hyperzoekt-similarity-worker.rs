// Similarity worker with queue-based job processing
// Replaces the old polling-based approach
use anyhow::Result;
use axum::{extract::State, response::IntoResponse, routing::get, Router};
use deadpool_redis::redis::AsyncCommands;
use log::{debug, error, info, warn};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use zoekt_distributed::redis_adapter::{create_redis_pool, DynRedis, RealRedis};

/// Metrics for the similarity worker
#[derive(Debug)]
struct SimilarityWorkerMetrics {
    jobs_processed: AtomicU64,
    jobs_failed: AtomicU64,
    last_job_unix: AtomicU64,
}

impl SimilarityWorkerMetrics {
    fn new() -> Self {
        Self {
            jobs_processed: AtomicU64::new(0),
            jobs_failed: AtomicU64::new(0),
            last_job_unix: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> SimilarityWorkerMetricsSnapshot {
        SimilarityWorkerMetricsSnapshot {
            jobs_processed: self.jobs_processed.load(Ordering::Relaxed),
            jobs_failed: self.jobs_failed.load(Ordering::Relaxed),
            last_job_unix: self.last_job_unix.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct SimilarityWorkerMetricsSnapshot {
    jobs_processed: u64,
    jobs_failed: u64,
    last_job_unix: u64,
}

/// Shared state for the HTTP server
#[derive(Clone)]
struct AppState {
    metrics: Arc<SimilarityWorkerMetrics>,
    redis: Option<Arc<RealRedis>>,
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    match &state.redis {
        Some(r) => match r.ping().await {
            Ok(_) => (axum::http::StatusCode::OK, "OK".to_string()),
            Err(e) => (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                format!("ERR: {}", e),
            ),
        },
        None => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "no redis".to_string(),
        ),
    }
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.metrics.snapshot();
    let body = format!(
        "# HELP hyperzoekt_similarity_worker_jobs_processed_total Total jobs processed\n\
         # TYPE hyperzoekt_similarity_worker_jobs_processed_total counter\n\
         hyperzoekt_similarity_worker_jobs_processed_total {}\n\
         # HELP hyperzoekt_similarity_worker_jobs_failed_total Jobs that failed processing\n\
         # TYPE hyperzoekt_similarity_worker_jobs_failed_total counter\n\
         hyperzoekt_similarity_worker_jobs_failed_total {}\n\
         # HELP hyperzoekt_similarity_worker_last_job_unix_seconds Last job time (unix seconds)\n\
         # TYPE hyperzoekt_similarity_worker_last_job_unix_seconds gauge\n\
         hyperzoekt_similarity_worker_last_job_unix_seconds {}\n",
        s.jobs_processed, s.jobs_failed, s.last_job_unix
    );
    (
        axum::http::StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    info!("🚀 hyperzoekt-similarity-worker starting up [BUILD_WITH_IMPROVED_LOGGING]");
    info!(
        "📦 Version: {} PID: {}",
        env!("CARGO_PKG_VERSION"),
        std::process::id()
    );

    let metrics = Arc::new(SimilarityWorkerMetrics::new());

    // Create Redis pool once at startup for health checks
    let redis_for_health = create_redis_pool().map(|pool| Arc::new(RealRedis { pool }));
    if redis_for_health.is_some() {
        info!("Redis pool created for health endpoint");
    } else {
        info!("No Redis pool for health endpoint (in-memory fallback)");
    }

    // Start HTTP server for health/metrics
    let host = std::env::var("HZ_SIMILARITY_WORKER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = std::env::var("HZ_SIMILARITY_WORKER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7208);

    let app_state = AppState {
        metrics: Arc::clone(&metrics),
        redis: redis_for_health,
    };
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/healthz", get(health_handler)) // Kubernetes-style alias
        .route("/metrics", get(metrics_handler))
        .with_state(app_state);

    let addr = format!("{}:{}", host, port)
        .parse::<std::net::SocketAddr>()
        .unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("HTTP server listening on {}", addr);

    let serve_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            error!("HTTP server failed: {:?}", e);
        }
    });

    // Spawn the similarity job consumer
    let similarity_queue = std::env::var("HZ_SIMILARITY_JOBS_QUEUE")
        .unwrap_or_else(|_| "zoekt:similarity_jobs".to_string());
    let processing_prefix = format!("{}:processing", similarity_queue);
    let processing_ttl: u64 = std::env::var("HZ_SIMILARITY_PROCESSING_TTL_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    info!(
        "Starting similarity job consumer (queue: {}, ttl: {}s)",
        similarity_queue, processing_ttl
    );

    let metrics_for_consumer = Arc::clone(&metrics);
    let consumer_handle = tokio::spawn(async move {
        // Create Redis pool once outside the loop
        let pool = match create_redis_pool() {
            Some(p) => p,
            None => {
                error!("No Redis pool available, cannot start similarity consumer");
                return;
            }
        };
        info!("Redis pool created for similarity job consumer");

        loop {
            // Get Redis connection from the pool
            let mut conn = match pool.get().await {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to get Redis connection: {}", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            // Create processing key with unique ID
            let consumer_id = format!("sim-{}", std::process::id());
            let processing_key = format!(
                "{}:{}:{}",
                processing_prefix,
                consumer_id,
                chrono::Utc::now().timestamp_millis()
            );

            // Atomically pop job from queue and push to processing key
            let msg_opt: Option<String> = match conn
                .brpoplpush::<String, String, Option<String>>(
                    similarity_queue.clone(),
                    processing_key.clone(),
                    10.0,
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    debug!("brpoplpush timeout or error: {} (will retry)", e);
                    continue;
                }
            };

            let msg = match msg_opt {
                Some(m) => m,
                None => {
                    debug!("No jobs in similarity queue");
                    continue;
                }
            };

            // Set TTL on processing key
            let _: () = conn
                .expire(&processing_key, processing_ttl as i64)
                .await
                .unwrap_or(());

            info!("📩 Received similarity job from queue: {}", msg);

            // Parse job payload
            let job: serde_json::Value = match serde_json::from_str(&msg) {
                Ok(v) => v,
                Err(e) => {
                    warn!("❌ Malformed similarity job payload - JSON parse error: {} — Raw payload: {}", e, msg);
                    let _: () = conn.del(&processing_key).await.unwrap_or(());
                    metrics_for_consumer
                        .jobs_failed
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            let repo_name = match job.get("repo_name").and_then(|v| v.as_str()) {
                Some(r) => r.to_string(),
                None => {
                    warn!(
                        "❌ Similarity job missing repo_name field in payload: {}",
                        msg
                    );
                    let _: () = conn.del(&processing_key).await.unwrap_or(());
                    metrics_for_consumer
                        .jobs_failed
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            let commit = match job.get("commit").and_then(|v| v.as_str()) {
                Some(c) => c.to_string(),
                None => {
                    warn!("❌ Similarity job missing commit field in payload: {}", msg);
                    let _: () = conn.del(&processing_key).await.unwrap_or(());
                    metrics_for_consumer
                        .jobs_failed
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            info!(
                "🔍 Processing similarity job: repo='{}' commit='{}' (payload: {})",
                repo_name, commit, msg
            );

            // Process the similarity job
            match process_similarity_job(&repo_name, &commit).await {
                Ok(_) => {
                    info!(
                        "✅ Successfully processed similarity job for repo='{}' commit='{}'",
                        repo_name, commit
                    );
                    metrics_for_consumer
                        .jobs_processed
                        .fetch_add(1, Ordering::Relaxed);
                    metrics_for_consumer
                        .last_job_unix
                        .store(chrono::Utc::now().timestamp() as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    warn!(
                        "❌ Failed to process similarity job for repo='{}' commit='{}': {}",
                        repo_name, commit, e
                    );
                    metrics_for_consumer
                        .jobs_failed
                        .fetch_add(1, Ordering::Relaxed);
                }
            }

            // Delete processing key to mark job as complete
            let _: () = conn.del(&processing_key).await.unwrap_or(());
        }
    });

    // Keep the process alive until signalled to shutdown
    tokio::select! {
        _ = consumer_handle => { warn!("Similarity consumer exited unexpectedly"); }
        _ = signal::ctrl_c() => { info!("Received shutdown signal"); }
    }

    serve_handle.abort();
    Ok(())
}

/// Process a similarity job for the given repo and commit
async fn process_similarity_job(repo_name: &str, commit: &str) -> Result<()> {
    use hyperzoekt::db::connection::connect;

    info!(
        "Processing similarity for repo='{}' commit='{}'",
        repo_name, commit
    );

    // Connect to SurrealDB
    let conn = connect(
        &std::env::var("SURREALDB_URL").ok(),
        &std::env::var("SURREALDB_USERNAME").ok(),
        &std::env::var("SURREALDB_PASSWORD").ok(),
        &std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".to_string()),
        &std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".to_string()),
    )
    .await?;

    // Format commit as a record ID reference to match the database format
    let commit_ref = format!("commits:{}", commit);

    info!(
        "🔍 Querying entity_snapshots: repo='{}' commit='{}' (sourcecontrol_commit field matches 'commits:<sha>')",
        repo_name, commit
    );

    // Count total entity_snapshots with embeddings for this commit
    let count_sql = format!(
        "SELECT COUNT() FROM entity_snapshot \
         WHERE repo_name = \"{}\" AND sourcecontrol_commit = \"{}\" AND embedding_len > 0 AND source_content != NONE AND string::len(source_content) >= 256 \
         GROUP ALL",
        repo_name.replace("\"", "\\\""),
        commit_ref.replace("\"", "\\\"")
    );

    let total_count = if let Ok(resp) = conn.query(&count_sql).await {
        if let Some(json) = hyperzoekt::db::helpers::response_to_json(resp) {
            json.as_array()
                .and_then(|arr| arr.first())
                .and_then(|obj| obj.get("count"))
                .and_then(|c| c.as_u64())
                .unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };

    if total_count == 0 {
        info!(
            "No entity_snapshots found with embeddings for repo='{}' commit='{}'",
            repo_name, commit
        );
        return Ok(());
    }

    info!(
        "Found {} entity_snapshots with embeddings for repo='{}' commit='{}'",
        total_count, repo_name, commit
    );

    // Fetch all entity_snapshots in batches to avoid memory issues
    // Process in chunks of 1000 to handle large repos
    const BATCH_SIZE: u64 = 1000;
    let mut all_snapshots = Vec::new();
    let mut offset = 0u64;

    info!(
        "Fetching {} entity_snapshots in batches of {}",
        total_count, BATCH_SIZE
    );

    while offset < total_count {
        // Use meta::id() to convert record ID to string - this fixes SurrealDB Thing deserialization issues
        let batch_sql = format!(
            "SELECT meta::id(id) AS id, stable_id, embedding, sourcecontrol_commit, embedding_len \
             FROM entity_snapshot \
             WHERE repo_name = \"{}\" AND sourcecontrol_commit = \"{}\" AND embedding_len > 0 AND source_content != NONE AND string::len(source_content) >= 256 \
             LIMIT {} START {}",
            repo_name.replace("\"", "\\\""),
            commit_ref.replace("\"", "\\\""),
            BATCH_SIZE,
            offset
        );

        debug!(
            "Fetching batch {}/{} (offset: {}, limit: {})",
            (offset / BATCH_SIZE) + 1,
            total_count.div_ceil(BATCH_SIZE),
            offset,
            BATCH_SIZE
        );

        let mut resp = conn.query(&batch_sql).await?;

        // Use typed take() directly - bypasses response_to_json helper
        #[derive(serde::Deserialize, Debug)]
        #[allow(dead_code)]
        struct SnapshotRow {
            id: String, // meta::id() returns string
            stable_id: String,
            embedding: Option<Vec<f32>>,
            sourcecontrol_commit: Option<String>,
            embedding_len: Option<i64>,
        }

        let batch_snapshots: Vec<SnapshotRow> = match resp.take::<Vec<SnapshotRow>>(0) {
            Ok(rows) => {
                info!(
                    "Fetched batch {}/{}: {} records",
                    (offset / BATCH_SIZE) + 1,
                    total_count.div_ceil(BATCH_SIZE),
                    rows.len()
                );
                rows
            }
            Err(e) => {
                error!("Failed to deserialize batch: {}", e);
                break;
            }
        };

        if batch_snapshots.is_empty() {
            debug!("Empty batch, stopping pagination");
            break;
        }

        all_snapshots.extend(batch_snapshots);
        offset += BATCH_SIZE;
    }

    info!(
        "Fetched {} entity_snapshots with embeddings",
        all_snapshots.len()
    );

    let snapshots = all_snapshots;

    if snapshots.is_empty() {
        info!(
            "No entity_snapshots with embeddings found for repo='{}' commit='{}'",
            repo_name, commit
        );
        return Ok(());
    }

    info!(
        "Found {} entity_snapshots to process for repo='{}' commit='{}'",
        snapshots.len(),
        repo_name,
        commit
    );

    // Read configuration
    let max_same: usize = std::env::var("HZ_SIMILARITY_MAX_SAME_REPO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let max_external: usize = std::env::var("HZ_SIMILARITY_MAX_EXTERNAL_REPO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(25);
    let threshold_same: f32 = std::env::var("HZ_SIMILARITY_THRESHOLD_SAME_REPO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.85);
    let threshold_external: f32 = std::env::var("HZ_SIMILARITY_THRESHOLD_EXTERNAL_REPO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.90);
    let min_store: usize = std::env::var("HZ_SIMILARITY_MIN_STORE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    info!(
        "Similarity config: same_repo(max={}, threshold={:.2}, min_store={}), external_repo(max={}, threshold={:.2}, min_store={})",
        max_same, threshold_same, min_store, max_external, threshold_external, min_store
    );

    let mut same_repo_created = 0;
    let mut external_repo_created = 0;
    let mut processed_count = 0;
    let total_count = snapshots.len();

    for snapshot in snapshots {
        // id is now already a string from meta::id()
        let id_str = &snapshot.id;
        let source_stable_id = &snapshot.stable_id;
        let embedding = match snapshot.embedding {
            Some(e) if !e.is_empty() => e,
            _ => {
                debug!(
                    "Snapshot {} has no embedding or empty embedding, skipping",
                    source_stable_id
                );
                continue;
            }
        };
        let source_commit = snapshot.sourcecontrol_commit.unwrap_or_default();

        // Log first entity with INFO level to see what's happening
        if processed_count == 0 {
            info!(
                "Processing first entity: id={}, stable_id={}, commit={}, embedding_len={}",
                id_str,
                source_stable_id,
                source_commit,
                embedding.len()
            );

            // Test: count how many entity_snapshots match the WHERE clause
            let test_count_sql = "SELECT count() as cnt FROM entity_snapshot \
                 WHERE embedding_len > 0 AND repo_name = $repo AND sourcecontrol_commit = $commit \
                 GROUP ALL"
                .to_string();

            if let Ok(count_resp) = conn
                .query_with_binds(
                    &test_count_sql,
                    vec![
                        ("repo", serde_json::json!(repo_name)),
                        ("commit", serde_json::json!(&source_commit)),
                    ],
                )
                .await
            {
                if let Some(json) = hyperzoekt::db::helpers::response_to_json(count_resp) {
                    info!("Test count query result: {:?}", json);
                }
            }
        }

        debug!(
            "Processing similarities for entity_snapshot {} (stable_id={})",
            id_str, source_stable_id
        );

        // Find similar entities in same repo (matching commit)
        // Query candidates, then create relations in Rust (more reliable than FOR loops)
        let same_repo_sql = format!(
            "LET $source_embedding = (SELECT embedding FROM entity_snapshot WHERE stable_id = $source_stable_id LIMIT 1)[0].embedding; \
             SELECT id, stable_id, sourcecontrol_commit, vector::similarity::cosine(embedding, $source_embedding) AS score \
             FROM entity_snapshot \
             WHERE embedding_len > 0 AND repo_name = $repo AND sourcecontrol_commit = $commit AND stable_id != $source_stable_id AND source_content != NONE AND string::len(source_content) >= 256 \
             ORDER BY score DESC \
             LIMIT {}",
            max_same
        );

        debug!(
            "Same-repo query for {}: SQL='{}' repo='{}' commit='{}' stable_id='{}' threshold={}",
            source_stable_id,
            same_repo_sql,
            repo_name,
            &source_commit,
            source_stable_id,
            threshold_same
        );

        let same_resp = conn
            .query_with_binds(
                &same_repo_sql,
                vec![
                    ("source_stable_id", serde_json::json!(source_stable_id)),
                    (
                        "source_id",
                        serde_json::json!(format!("entity_snapshot:{}", id_str)),
                    ),
                    ("repo", serde_json::json!(repo_name)),
                    ("commit", serde_json::json!(&source_commit)),
                    ("threshold", serde_json::json!(threshold_same)),
                ],
            )
            .await?;

        #[derive(serde::Deserialize, Debug)]
        #[allow(dead_code)]
        struct SimilarRow {
            id: surrealdb::sql::Thing,
            stable_id: String,
            sourcecontrol_commit: String,
            score: f32,
        }

        // LET is slot 0, SELECT is slot 1
        let mut resp = same_resp;
        let same_results: Vec<SimilarRow> = match resp.take::<Vec<SimilarRow>>(1) {
            Ok(rows) => {
                let above_threshold: Vec<_> =
                    rows.iter().filter(|r| r.score >= threshold_same).collect();

                // Find max score for diagnostics
                let max_score = rows
                    .iter()
                    .map(|r| r.score)
                    .fold(f32::NEG_INFINITY, f32::max);

                if processed_count == 0 {
                    info!(
                        "First entity same-repo: {} candidates, {} above threshold ({}), max_score={:.3}",
                        rows.len(),
                        above_threshold.len(),
                        threshold_same,
                        max_score
                    );
                    if !above_threshold.is_empty() {
                        info!(
                            "First result: stable_id={}, score={:.3}",
                            above_threshold[0].stable_id, above_threshold[0].score
                        );
                    } else if !rows.is_empty() {
                        warn!(
                            "⚠️  No candidates above threshold! Best candidate: stable_id={}, score={:.3} (threshold={})",
                            rows[0].stable_id, rows[0].score, threshold_same
                        );
                    }
                }

                // Log max score periodically
                if processed_count % 100 == 0 && !rows.is_empty() {
                    debug!(
                        "Entity {} same-repo: max_score={:.3}, above_threshold={}/{}",
                        source_stable_id,
                        max_score,
                        above_threshold.len(),
                        rows.len()
                    );
                }

                rows
            }
            Err(e) => {
                if processed_count == 0 {
                    warn!("Failed to deserialize same-repo results from slot 1: {}", e);
                }
                vec![]
            }
        };

        debug!(
            "Same-repo query for {} returned {} candidates",
            source_stable_id,
            same_results.len()
        );

        // Create relations for candidates above threshold OR ensure minimum stored
        let mut stored_count = 0;
        for result in &same_results {
            // Store if above threshold OR if we haven't reached min_store yet
            if result.score < threshold_same && stored_count >= min_store {
                continue;
            }

            // Extract target ID from Thing type
            let target_id_str = result.id.to_string();

            // Check if reverse relation already exists (target->source)
            // This prevents duplicate bidirectional relations
            let check_reverse_sql = format!(
                "SELECT VALUE id FROM similar_same_repo WHERE in = {} AND out = entity_snapshot:{} LIMIT 1",
                target_id_str, id_str
            );

            let reverse_exists = if let Ok(mut check_resp) = conn.query(&check_reverse_sql).await {
                !check_resp
                    .take::<Vec<serde_json::Value>>(0)
                    .unwrap_or_default()
                    .is_empty()
            } else {
                false
            };

            if reverse_exists {
                debug!(
                    "Skipping duplicate relation: reverse already exists ({} -> {})",
                    result.stable_id, source_stable_id
                );
                continue;
            }

            // Create the RELATE statement using similar_same_repo relation
            let relate_sql = format!(
                "RELATE entity_snapshot:{}->similar_same_repo->{} SET score = $score",
                id_str, target_id_str
            );

            match conn
                .query_with_binds(
                    &relate_sql,
                    vec![("score", serde_json::json!(result.score))],
                )
                .await
            {
                Ok(_) => {
                    same_repo_created += 1;
                    stored_count += 1;
                }
                Err(e) => {
                    warn!(
                        "Failed to create same_repo relation {} -> {}: {}",
                        source_stable_id, result.stable_id, e
                    );
                }
            }
        }

        debug!(
            "Same-repo: created {} relations for {}",
            same_results
                .iter()
                .filter(|r| r.score >= threshold_same)
                .count(),
            source_stable_id
        );

        // Log for first entity after creating relations
        if processed_count == 0 && same_repo_created > 0 {
            info!(
                "Sample same-repo: created {} relations for first entity",
                same_repo_created
            );

            // DIAGNOSTIC: Check if the relation actually exists in the database
            let check_sql = format!(
                "SELECT * FROM similar_same_repo WHERE in = entity_snapshot:{} LIMIT 5",
                id_str
            );
            if let Ok(mut check_resp) = conn.query(&check_sql).await {
                if let Ok(relations) = check_resp.take::<Vec<serde_json::Value>>(0) {
                    info!(
                        "✓ Database check: Found {} similar_same_repo relations for first entity",
                        relations.len()
                    );
                    if !relations.is_empty() {
                        info!("  First relation: {:?}", relations[0]);
                    } else {
                        warn!(
                            "⚠️  No relations found in database for first entity (expected {})",
                            same_repo_created
                        );
                    }
                }
            }
        }

        // Find similar entities in external repos
        let external_sql = format!(
            "LET $source_embedding = (SELECT embedding FROM entity_snapshot WHERE stable_id = $source_stable_id LIMIT 1)[0].embedding; \
             SELECT id, stable_id, repo_name, sourcecontrol_commit, vector::similarity::cosine(embedding, $source_embedding) AS score \
             FROM entity_snapshot \
             WHERE embedding_len > 0 AND repo_name != $repo AND source_content != NONE AND string::len(source_content) >= 256 \
             ORDER BY score DESC \
             LIMIT {}",
            max_external
        );

        debug!(
            "External-repo query for {}: repo!='{}'",
            source_stable_id, repo_name
        );

        let external_resp = conn
            .query_with_binds(
                &external_sql,
                vec![
                    ("source_stable_id", serde_json::json!(source_stable_id)),
                    ("repo", serde_json::json!(repo_name)),
                ],
            )
            .await?;

        #[derive(serde::Deserialize, Debug)]
        struct ExternalRow {
            id: surrealdb::sql::Thing,
            stable_id: String,
            repo_name: Option<String>,
            sourcecontrol_commit: Option<String>,
            score: f32,
        }

        // LET is slot 0, SELECT is slot 1
        let mut ext_resp = external_resp;
        let external_results: Vec<ExternalRow> = match ext_resp.take::<Vec<ExternalRow>>(1) {
            Ok(rows) => {
                // Find max score for diagnostics
                let max_score = rows
                    .iter()
                    .map(|r| r.score)
                    .fold(f32::NEG_INFINITY, f32::max);
                let above_threshold_count = rows
                    .iter()
                    .filter(|r| r.score >= threshold_external)
                    .count();

                if processed_count == 0 && !rows.is_empty() {
                    info!(
                        "First entity external-repo: {} candidates, {} above threshold ({}), max_score={:.3}",
                        rows.len(),
                        above_threshold_count,
                        threshold_external,
                        max_score
                    );
                }

                rows
            }
            Err(e) => {
                warn!(
                    "Failed to deserialize external-repo results from slot 1: {}",
                    e
                );
                vec![]
            }
        };

        debug!(
            "External-repo query for {} returned {} candidates",
            source_stable_id,
            external_results.len()
        );

        // Create relations for candidates above threshold OR ensure minimum stored
        // Also check that they are on default branch
        let mut stored_count = 0;
        for result in &external_results {
            // Store if above threshold OR if we haven't reached min_store yet
            if result.score < threshold_external && stored_count >= min_store {
                continue;
            }

            // Check if this commit is on the default branch
            let empty_string = String::new();
            let target_repo = result.repo_name.as_ref().unwrap_or(&empty_string);
            let target_commit = result
                .sourcecontrol_commit
                .as_ref()
                .unwrap_or(&empty_string);

            let is_default_sql = "SELECT VALUE is_default_branch FROM sourcecontrol_commit \
                 WHERE repo_name = $repo AND commit_sha = $commit LIMIT 1"
                .to_string();

            let is_default = if let Ok(mut check_resp) = conn
                .query_with_binds(
                    &is_default_sql,
                    vec![
                        ("repo", serde_json::json!(target_repo)),
                        ("commit", serde_json::json!(target_commit)),
                    ],
                )
                .await
            {
                check_resp
                    .take::<Option<bool>>(0)
                    .unwrap_or(Some(false))
                    .unwrap_or(false)
            } else {
                false
            };

            if !is_default {
                continue;
            }

            // Extract target ID from Thing type
            let target_id_str = result.id.to_string();

            // Check if reverse relation already exists (target->source)
            // This prevents duplicate bidirectional relations
            let check_reverse_sql = format!(
                "SELECT VALUE id FROM similar_external_repo WHERE in = {} AND out = entity_snapshot:{} LIMIT 1",
                target_id_str, id_str
            );

            let reverse_exists = if let Ok(mut check_resp) = conn.query(&check_reverse_sql).await {
                !check_resp
                    .take::<Vec<serde_json::Value>>(0)
                    .unwrap_or_default()
                    .is_empty()
            } else {
                false
            };

            if reverse_exists {
                debug!(
                    "Skipping duplicate external relation: reverse already exists ({} -> {})",
                    result.stable_id, source_stable_id
                );
                continue;
            }

            // Create the RELATE statement using similar_external_repo relation
            let relate_sql = format!(
                "RELATE entity_snapshot:{}->similar_external_repo->{} SET score = $score",
                id_str, target_id_str
            );

            match conn
                .query_with_binds(
                    &relate_sql,
                    vec![("score", serde_json::json!(result.score))],
                )
                .await
            {
                Ok(_) => {
                    external_repo_created += 1;
                    stored_count += 1;
                }
                Err(e) => {
                    warn!(
                        "Failed to create external_repo relation {} -> {}: {}",
                        source_stable_id, result.stable_id, e
                    );
                }
            }
        }

        debug!(
            "External-repo: created {} relations for {}",
            external_repo_created, source_stable_id
        );

        processed_count += 1;
        if processed_count % 100 == 0 {
            info!(
                "Progress: {}/{} entities processed, {} same-repo relations, {} external-repo relations",
                processed_count, total_count, same_repo_created, external_repo_created
            );

            // Verify relations are actually in the database every 100 entities
            if let Ok(mut check_resp) = conn
                .query("SELECT COUNT() FROM similar_same_repo GROUP ALL")
                .await
            {
                if let Ok(Some(count_val)) = check_resp.take::<Option<serde_json::Value>>(0) {
                    if let Some(count) = count_val.get("count").and_then(|c| c.as_u64()) {
                        info!(
                            "✓ Database verification: {} similar_same_repo relations actually exist in DB",
                            count
                        );
                    }
                }
            }
        }
    }

    info!(
        "Completed similarity processing for repo='{}' commit='{}': {} same-repo relations, {} external-repo relations",
        repo_name, commit, same_repo_created, external_repo_created
    );

    Ok(())
}
