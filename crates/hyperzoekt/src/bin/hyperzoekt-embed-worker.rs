use anyhow::Result;
use deadpool_redis::redis::AsyncCommands;
use hyperzoekt::db_writer::connection::{connect, SurrealConnection};
use log::{error, info, warn};
use serde::Deserialize;
use std::time::Duration;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct EmbeddingJob {
    stable_id: String,
    repo_name: String,
    language: String,
    kind: String,
    name: String,
    source_url: Option<String>,
}

// Response parsing helpers (moved to module-level so tests can access them)
#[derive(serde::Deserialize)]
struct TeiResp {
    embeddings: Vec<Vec<f32>>,
}

// OpenAI-style: { data: [{ embedding: [...] }, ... ] }
#[derive(serde::Deserialize)]
struct OpenAiItem {
    embedding: Vec<f32>,
}
#[derive(serde::Deserialize)]
struct OpenAiResp {
    data: Vec<OpenAiItem>,
}

// Parse embeddings from either TEI native shape or OpenAI-style shape.
// Returns (embeddings, optional_model_id).
fn parse_embeddings_and_model(
    b: &[u8],
) -> Result<(Vec<Vec<f32>>, Option<String>), serde_json::Error> {
    // Try TEI-native first
    match serde_json::from_slice::<TeiResp>(b) {
        Ok(t) => {
            // Try to extract a model id if present in the payload
            let model = serde_json::from_slice::<serde_json::Value>(b)
                .ok()
                .and_then(|v| {
                    v.get("model")
                        .and_then(|m| m.as_str().map(|s| s.to_string()))
                        .or_else(|| {
                            v.get("model_id")
                                .and_then(|m| m.as_str().map(|s| s.to_string()))
                        })
                        .or_else(|| {
                            v.get("modelName")
                                .and_then(|m| m.as_str().map(|s| s.to_string()))
                        })
                });
            Ok((t.embeddings, model))
        }
        Err(_first_err) => {
            // Try OpenAI-style
            match serde_json::from_slice::<OpenAiResp>(b) {
                Ok(oa) => {
                    let vecs: Vec<Vec<f32>> = oa.data.into_iter().map(|it| it.embedding).collect();
                    let model = serde_json::from_slice::<serde_json::Value>(b)
                        .ok()
                        .and_then(|v| {
                            v.get("model")
                                .and_then(|m| m.as_str().map(|s| s.to_string()))
                        });
                    Ok((vecs, model))
                }
                Err(second_err) => Err(second_err),
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    info!("hyperzoekt-embed-worker starting up");

    let queue_key =
        std::env::var("HZ_EMBED_JOBS_QUEUE").unwrap_or_else(|_| "zoekt:embed_jobs".to_string());
    let processing_prefix = std::env::var("HZ_EMBED_PROCESSING_PREFIX")
        .unwrap_or_else(|_| "zoekt:embed_processing".to_string());
    let processing_ttl: u64 = std::env::var("HZ_EMBED_PROCESSING_TTL_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);

    let pool = match zoekt_distributed::redis_adapter::create_redis_pool() {
        Some(p) => p,
        None => {
            error!("Redis not configured; set REDIS_URL or credentials");
            std::process::exit(1);
        }
    };

    // Connect to SurrealDB using existing env wiring
    let surreal_url = std::env::var("SURREALDB_URL").ok();
    let surreal_username = std::env::var("SURREALDB_USERNAME").ok();
    let surreal_password = std::env::var("SURREALDB_PASSWORD").ok();
    let surreal_ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
    let surreal_db = std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());

    let db = connect(
        &surreal_url,
        &surreal_username,
        &surreal_password,
        &surreal_ns,
        &surreal_db,
    )
    .await?;
    info!(
        "embed-worker connected to Surreal ns={} db={}",
        surreal_ns, surreal_db
    );

    // Ensure embedding fields are defined with DEFAULTs (not VALUE), so writes persist.
    // This is idempotent and safe: attempts DEFINE, then REMOVE+DEFINE fallback, then ALTER.
    if let Err(e) = ensure_embedding_schema(&db).await {
        warn!("embedding schema ensure failed (non-fatal): {}", e);
    }

    // NOTE: schema initialization moved to the indexer (owner of `entity`).

    // TEI settings and client
    let tei_base = std::env::var("HZ_TEI_BASE").unwrap_or_else(|_| "http://tei:80".to_string());
    let tei_endpoint = format!("{}/embeddings", tei_base.trim_end_matches('/'));
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let embed_model = match std::env::var("HZ_EMBED_MODEL") {
        Ok(m) if !m.trim().is_empty() => m,
        _ => {
            error!(
                "HZ_EMBED_MODEL environment variable not set; it is required. Recommended: 'jinaai/jina-embeddings-v2-base-code'"
            );
            std::process::exit(1);
        }
    };
    // Respect user-set batch; default to 8 but allow larger values for other providers.
    let mut max_batch: usize = std::env::var("HZ_EMBED_BATCH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let max_retries: usize = std::env::var("HZ_EMBED_RETRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let max_text_bytes: usize = std::env::var("HZ_EMBED_MAX_TEXT_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8192);
    let slow_log_ms: u128 = std::env::var("HZ_EMBED_SLOW_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1500);

    // Similarity feature gating and thresholds (array field storage)
    let enable_similarity = std::env::var("HZ_ENABLE_EMBED_SIMILARITY")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
        .unwrap_or(false);
    let _same_repo_threshold: f32 = std::env::var("HZ_SIMILARITY_THRESHOLD_SAME_REPO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.90);
    let _external_repo_threshold: f32 = std::env::var("HZ_SIMILARITY_THRESHOLD_EXTERNAL_REPO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.95);
    let max_similar_same: usize = std::env::var("HZ_SIMILARITY_MAX_SAME_REPO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(25);
    let max_similar_external: usize = std::env::var("HZ_SIMILARITY_MAX_EXTERNAL_REPO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    let _sample_same: usize = std::env::var("HZ_SIMILARITY_SAMPLE_SAME_REPO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let _sample_external: usize = std::env::var("HZ_SIMILARITY_SAMPLE_EXTERNAL_REPO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);
    if enable_similarity {
        info!(
            "similarity enabled max_same={} max_external={}",
            max_similar_same, max_similar_external
        );
    }

    loop {
        if let Err(e) = recover_expired(&pool, &queue_key, &processing_prefix).await {
            warn!("recover_expired error: {}", e);
        }
        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                warn!("redis pool.get failed: {}", e);
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };

        // Pop first job (blocking) to start a micro-batch
        let mut batch_msgs: Vec<(String, String)> = Vec::new(); // (processing_key, json)
        let start_pop = std::time::Instant::now();
        let consumer_id = format!("embed-{}", std::process::id());
        let processing_key = format!(
            "{}:{}:{}",
            processing_prefix,
            consumer_id,
            start_pop.elapsed().as_millis()
        );
        if let Some(first) = conn.brpoplpush(&queue_key, &processing_key, 5.0).await? {
            let _: () = conn.expire(&processing_key, processing_ttl as i64).await?;
            batch_msgs.push((processing_key.clone(), first));
            // Try to quickly accumulate more (non-blocking) up to max_batch
            while batch_msgs.len() < max_batch {
                let k2 = format!(
                    "{}:{}:{}",
                    processing_prefix,
                    consumer_id,
                    start_pop.elapsed().as_millis()
                );
                match conn.rpoplpush(&queue_key, &k2).await {
                    Ok(Some(m)) => {
                        let _: () = conn.expire(&k2, processing_ttl as i64).await?;
                        batch_msgs.push((k2, m));
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!("rpoplpush error while batching: {}", e);
                        break;
                    }
                }
            }
        } else {
            // no messages
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }

        // Parse jobs and build inputs
        #[derive(serde::Serialize)]
        struct TeiReq<'a> {
            model: &'a str,
            input: &'a [&'a str],
        }
        // ...existing code...

        struct WorkItem {
            key: String,
            job: EmbeddingJob,
            // Full, untruncated text to persist on the entity
            source_content: String,
            // Truncated text used for embedding requests (to honor TEI limits)
            embed_text: String,
            // Whether the source_content came from DB (authoritative) and should be persisted
            persist_source: bool,
        }

        let mut work: Vec<WorkItem> = Vec::new();
        for (key, json) in batch_msgs.iter() {
            match serde_json::from_str::<EmbeddingJob>(json) {
                Ok(job) => {
                    // Prefer full function body from DB source_content; otherwise fall back
                    let mut combined: Option<String> = None;
                    let mut from_db = false;
                    if let Ok(mut r) = db
                        .query_with_binds(
                            "SELECT source_content, name, signature, doc FROM entity WHERE id = $eid LIMIT 1;",
                            vec![
                                (
                                    "eid",
                                    serde_json::Value::String(format!(
                                        "entity:{}",
                                        sanitize_id(&job.stable_id)
                                    )),
                                ),
                            ],
                        )
                        .await
                    {
                        let row: Vec<serde_json::Value> = r.take(0).unwrap_or_default();
                        if let Some(obj) = row.first().and_then(|v| v.as_object()) {
                            if let Some(sc) = obj.get("source_content").and_then(|v| v.as_str()) {
                                if !sc.is_empty() {
                                    combined = Some(sc.to_string());
                                    from_db = true;
                                }
                            }
                            if combined.is_none() {
                                let name = obj.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                let sig = obj.get("signature").and_then(|v| v.as_str()).unwrap_or("");
                                let doc = obj.get("doc").and_then(|v| v.as_str()).unwrap_or("");
                                combined = Some(format!("{}\n{}\n{}", name, sig, doc));
                            }
                        }
                    }
                    let source_content = combined.unwrap_or_else(|| job.name.clone());
                    let mut embed_text = source_content.clone();
                    if embed_text.len() > max_text_bytes {
                        embed_text.truncate(max_text_bytes);
                    }
                    work.push(WorkItem {
                        key: key.clone(),
                        job,
                        source_content,
                        embed_text,
                        persist_source: from_db,
                    });
                }
                Err(e) => {
                    warn!("bad embed job JSON: {} (dropping)", e);
                    let _: () = conn.del(key).await?;
                }
            }
        }

        if work.is_empty() {
            continue;
        }

        // Call TEI in batches (we already limited to max_batch but keep chunks)
        let mut idx = 0usize;
        while idx < work.len() {
            let end = (idx + max_batch).min(work.len());
            let slice = &work[idx..end];
            let input_refs: Vec<&str> = slice.iter().map(|w| w.embed_text.as_str()).collect();
            // Retry loop
            let mut attempt = 0usize;
            let call_start = std::time::Instant::now();
            let mut embeddings: Option<Vec<Vec<f32>>> = None;
            let mut response_model: Option<String> = None;
            loop {
                attempt += 1;
                // Debug: log a compact representation of the TEI request payload (truncated inputs)
                if std::env::var("HZ_EMBED_DEBUG_SQL").ok().as_deref() == Some("1") {
                    let mut snippet_inputs: Vec<String> = Vec::new();
                    for s in input_refs.iter().take(4) {
                        let mut t = s.to_string();
                        if t.len() > 200 {
                            t.truncate(200);
                            t.push_str("...[truncated]");
                        }
                        snippet_inputs.push(t);
                    }
                    info!(
                        "TEI request model={} inputs_sample={:?} count={}",
                        embed_model,
                        snippet_inputs,
                        input_refs.len()
                    );
                }

                let resp = http
                    .post(&tei_endpoint)
                    .json(&TeiReq {
                        model: embed_model.as_str(),
                        input: &input_refs,
                    })
                    .send()
                    .await;
                match resp {
                    Ok(r) if r.status().is_success() => {
                        // Read raw bytes first so we can both attempt JSON parse and
                        // log the body on parse failure for diagnostics.
                        match r.bytes().await {
                            Ok(b) => match parse_embeddings_and_model(&b) {
                                Ok((vecs, maybe_model)) => {
                                    embeddings = Some(vecs);
                                    response_model = maybe_model.clone();
                                    if let Some(mid) = maybe_model {
                                        info!("TEI response reported model={}", mid);
                                    }
                                    if std::env::var("HZ_EMBED_DEBUG_SQL").ok().as_deref()
                                        == Some("1")
                                    {
                                        let body_snip = String::from_utf8_lossy(&b);
                                        let snippet = if body_snip.len() > 2000 {
                                            &body_snip[..2000]
                                        } else {
                                            &body_snip
                                        };
                                        info!(
                                            "TEI response body snippet (truncated 2k): {}",
                                            snippet
                                        );
                                    }
                                }
                                Err(e) => {
                                    let body_snippet = String::from_utf8_lossy(&b);
                                    let snippet = if body_snippet.len() > 1024 {
                                        &body_snippet[..1024]
                                    } else {
                                        &body_snippet
                                    };
                                    warn!(
                                        "TEI parse error attempt {}: {}; body snippet: {}",
                                        attempt, e, snippet
                                    );
                                }
                            },
                            Err(e) => warn!("TEI read bytes error attempt {}: {}", attempt, e),
                        }
                    }
                    Ok(r) => {
                        warn!("TEI non-success attempt {} status {}", attempt, r.status());
                        // Try to autodetect a lower max-batch hint from headers
                        if let Some(hv) = r.headers().get("x-max-batch") {
                            if let Ok(s) = hv.to_str() {
                                if let Ok(n) = s.parse::<usize>() {
                                    if n > 0 && n < max_batch {
                                        warn!(
                                            "TEI suggested max_batch={} via header; lowering from {}",
                                            n, max_batch
                                        );
                                        max_batch = n;
                                    }
                                }
                            }
                        }
                        // Also inspect textual body for hints (best-effort)
                        if let Ok(text) = r.text().await {
                            let lowered = parse_max_batch_from_text(&text, max_batch);
                            if lowered < max_batch {
                                warn!(
                                    "TEI message suggests lowering max_batch to {} (was {})",
                                    lowered, max_batch
                                );
                                max_batch = lowered;
                            }
                        }
                    }
                    Err(e) => warn!("TEI request error attempt {}: {}", attempt, e),
                }
                if embeddings.is_some() || attempt >= max_retries {
                    break;
                }
                let backoff = 100u64 * (1 << (attempt - 1)).min(5);
                tokio::time::sleep(Duration::from_millis(backoff)).await;
            }
            let elapsed = call_start.elapsed().as_millis();
            if elapsed >= slow_log_ms {
                warn!("slow embed batch: {} ms for {} items", elapsed, slice.len());
            }

            if let Some(vecs) = embeddings {
                // Write results; lengths should match. Log counts for diagnostics.
                info!(
                    "TEI returned {} embeddings for {} inputs",
                    vecs.len(),
                    slice.len()
                );
                if vecs.len() != slice.len() {
                    warn!(
                        "embedding count mismatch: got {} embeddings for {} inputs",
                        vecs.len(),
                        slice.len()
                    );
                }

                // Use parameterized updates per-entity to avoid SQL quoting/type issues.
                for (i, w) in slice.iter().enumerate() {
                    if let Some(v) = vecs.get(i) {
                        // Debug: log a small sample of embedding values for visibility
                        if std::env::var("HZ_EMBED_DEBUG_SQL").ok().as_deref() == Some("1") {
                            let sample: Vec<f32> = v.iter().take(5).cloned().collect();
                            info!(
                                "embedding sample for {}: len={} first_values={:?}",
                                sanitize_id(&w.job.stable_id),
                                v.len(),
                                sample
                            );
                        }
                        match serde_json::to_value(v) {
                            Ok(vval) => {
                                let model_id = response_model
                                    .as_deref()
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| embed_model.clone());
                                let dim = v.len() as i64;

                                let rid = sanitize_id(&w.job.stable_id);
                                let _binds = vec![
                                    ("embedding", vval),
                                    ("embedding_len", serde_json::Value::Number(dim.into())),
                                    ("embedding_dim", serde_json::Value::Number(dim.into())),
                                    (
                                        "embedding_model",
                                        serde_json::Value::String(model_id.clone()),
                                    ),
                                ];

                                let q = if w.persist_source {
                                    format!(
                                        "UPDATE entity:{} SET embedding = $embedding, embedding_len = $embedding_len, embedding_dim = $embedding_dim, embedding_model = $embedding_model, embedding_created_at = time::now(), source_content = $source_content;",
                                        rid
                                    )
                                } else {
                                    format!(
                                        "UPDATE entity:{} SET embedding = $embedding, embedding_len = $embedding_len, embedding_dim = $embedding_dim, embedding_model = $embedding_model, embedding_created_at = time::now();",
                                        rid
                                    )
                                };
                                if std::env::var("HZ_EMBED_DEBUG_SQL").ok().as_deref() == Some("1")
                                {
                                    // Log a compact debug message (avoid huge vector prints)
                                    info!(
                                        "embed SQL (param): {} -> embedding_len={} model={}",
                                        sanitize_id(&w.job.stable_id),
                                        dim,
                                        model_id
                                    );
                                    // Detailed binds: include sample of embedding and serialized embedding value
                                    let mut map = serde_json::Map::new();
                                    map.insert(
                                        "embedding_len".to_string(),
                                        serde_json::Value::Number(dim.into()),
                                    );
                                    let sample_vals: Vec<serde_json::Value> = v
                                        .iter()
                                        .take(5)
                                        .map(|f| serde_json::Value::from(*f))
                                        .collect();
                                    map.insert(
                                        "embedding_sample".to_string(),
                                        serde_json::Value::Array(sample_vals),
                                    );
                                    // Clone vval (the full serialized embedding) into the log map
                                    // vval is a serde_json::Value created earlier
                                    match serde_json::to_value(v) {
                                        Ok(full) => {
                                            map.insert("embedding_full".to_string(), full);
                                        }
                                        Err(_) => {
                                            map.insert(
                                                "embedding_full".to_string(),
                                                serde_json::Value::String(
                                                    "<unserializable>".to_string(),
                                                ),
                                            );
                                        }
                                    }
                                    match serde_json::to_string(&map) {
                                        Ok(s) => info!("embed SQL full binds: {}", s),
                                        Err(_) => info!("embed SQL full binds: <unserializable>"),
                                    }
                                    info!("embed SQL string: {}", q);
                                }

                                // Use underlying Surreal client `.query().bind()` so the client
                                // serializes types (Vec<f32>, i64, String) in a way Surreal expects.
                                // Precompute similarity arrays if enabled
                                let (similar_same_vals, similar_external_vals) =
                                    if enable_similarity {
                                        // Need repo name; we have it on job
                                        let sim_params = SimilarityParams {
                                            max_same: max_similar_same,
                                            max_external: max_similar_external,
                                        };
                                        match compute_similarity_arrays(
                                            &db,
                                            &w.job.stable_id,
                                            &w.job.repo_name,
                                            v,
                                            &sim_params,
                                        )
                                        .await
                                        {
                                            Ok((a, b)) => (a, b),
                                            Err(e) => {
                                                warn!(
                                                    "similarity arrays compute failed for {}: {}",
                                                    w.job.stable_id, e
                                                );
                                                (Vec::new(), Vec::new())
                                            }
                                        }
                                    } else {
                                        (Vec::new(), Vec::new())
                                    };

                                match &db {
                                    SurrealConnection::Local(db_conn) => {
                                        let mut call = db_conn.query(&q);
                                        call = call.bind(("embedding", v.clone()));
                                        call = call.bind(("embedding_len", dim));
                                        call = call.bind(("embedding_dim", dim));
                                        call = call.bind(("embedding_model", model_id.clone()));
                                        if w.persist_source {
                                            call = call
                                                .bind(("source_content", w.source_content.clone()));
                                        }
                                        // similarity edges are written after this update
                                        match call.await {
                                            Ok(resp) => {
                                                if std::env::var("HZ_EMBED_DEBUG_DB")
                                                    .ok()
                                                    .as_deref()
                                                    == Some("1")
                                                {
                                                    info!("Surreal update response: {:?}", resp);
                                                }
                                                if enable_similarity {
                                                    let mut rel_stmts: Vec<String> = Vec::new();
                                                    rel_stmts.push(format!("DELETE similar_same_repo WHERE in = entity:{};", rid));
                                                    rel_stmts.push(format!("DELETE similar_external_repo WHERE in = entity:{};", rid));
                                                    for v in &similar_same_vals {
                                                        if let Some(obj) = v.as_object() {
                                                            if let (Some(tid), Some(score)) = (
                                                                obj.get("stable_id")
                                                                    .and_then(|x| x.as_str()),
                                                                obj.get("score")
                                                                    .and_then(|x| x.as_f64()),
                                                            ) {
                                                                rel_stmts.push(format!(
                                                                    "RELATE entity:{}->similar_same_repo->entity:{} SET score = {};",
                                                                    rid, sanitize_id(tid), score
                                                                ));
                                                            }
                                                        }
                                                    }
                                                    for v in &similar_external_vals {
                                                        if let Some(obj) = v.as_object() {
                                                            if let (Some(tid), Some(score)) = (
                                                                obj.get("stable_id")
                                                                    .and_then(|x| x.as_str()),
                                                                obj.get("score")
                                                                    .and_then(|x| x.as_f64()),
                                                            ) {
                                                                rel_stmts.push(format!(
                                                                    "RELATE entity:{}->similar_external_repo->entity:{} SET score = {};",
                                                                    rid, sanitize_id(tid), score
                                                                ));
                                                            }
                                                        }
                                                    }
                                                    if !rel_stmts.is_empty() {
                                                        let batch = format!(
                                                            "BEGIN; {} COMMIT;",
                                                            rel_stmts.join(" ")
                                                        );
                                                        if let Err(e) = db.query(&batch).await {
                                                            warn!("similarity RELATE batch failed for {}: {}", w.job.stable_id, e);
                                                        }
                                                    }
                                                }
                                                let _: () = conn.del(&w.key).await?;
                                                if std::env::var("HZ_EMBED_DEBUG_DB")
                                                    .ok()
                                                    .as_deref()
                                                    == Some("1")
                                                {
                                                    let q2 = format!(
                                                        "SELECT embedding, embedding_len, embedding_dim, embedding_model, embedding_created_at, string::len(source_content) AS src_len FROM entity:{} LIMIT 1;",
                                                        rid
                                                    );
                                                    match db.query(&q2).await {
                                                        Ok(mut r) => {
                                                            if let Ok(rows) =
                                                                r.take::<Vec<serde_json::Value>>(0)
                                                            {
                                                                info!(
                                                                    "post-update select for {} -> {:?}",
                                                                    w.job.stable_id, rows
                                                                );
                                                            } else {
                                                                warn!(
                                                                    "post-update select returned no rows for {}",
                                                                    w.job.stable_id
                                                                );
                                                            }
                                                        }
                                                        Err(e) => warn!(
                                                            "post-update select failed for {}: {}",
                                                            w.job.stable_id, e
                                                        ),
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                warn!(
                                                    "Surreal update failed for {}: {}",
                                                    w.job.stable_id, e
                                                );
                                            }
                                        }
                                    }
                                    SurrealConnection::RemoteHttp(db_conn) => {
                                        let mut call = db_conn.query(&q);
                                        call = call.bind(("embedding", v.clone()));
                                        call = call.bind(("embedding_len", dim));
                                        call = call.bind(("embedding_dim", dim));
                                        call = call.bind(("embedding_model", model_id.clone()));
                                        if w.persist_source {
                                            call = call
                                                .bind(("source_content", w.source_content.clone()));
                                        }
                                        // similarity edges are written after this update
                                        match call.await {
                                            Ok(resp) => {
                                                if std::env::var("HZ_EMBED_DEBUG_DB")
                                                    .ok()
                                                    .as_deref()
                                                    == Some("1")
                                                {
                                                    info!("Surreal update response: {:?}", resp);
                                                }
                                                if enable_similarity {
                                                    let mut rel_stmts: Vec<String> = Vec::new();
                                                    rel_stmts.push(format!("DELETE similar_same_repo WHERE in = entity:{};", rid));
                                                    rel_stmts.push(format!("DELETE similar_external_repo WHERE in = entity:{};", rid));
                                                    for v in &similar_same_vals {
                                                        if let Some(obj) = v.as_object() {
                                                            if let (Some(tid), Some(score)) = (
                                                                obj.get("stable_id")
                                                                    .and_then(|x| x.as_str()),
                                                                obj.get("score")
                                                                    .and_then(|x| x.as_f64()),
                                                            ) {
                                                                rel_stmts.push(format!(
                                                                    "RELATE entity:{}->similar_same_repo->entity:{} SET score = {};",
                                                                    rid, sanitize_id(tid), score
                                                                ));
                                                            }
                                                        }
                                                    }
                                                    for v in &similar_external_vals {
                                                        if let Some(obj) = v.as_object() {
                                                            if let (Some(tid), Some(score)) = (
                                                                obj.get("stable_id")
                                                                    .and_then(|x| x.as_str()),
                                                                obj.get("score")
                                                                    .and_then(|x| x.as_f64()),
                                                            ) {
                                                                rel_stmts.push(format!(
                                                                    "RELATE entity:{}->similar_external_repo->entity:{} SET score = {};",
                                                                    rid, sanitize_id(tid), score
                                                                ));
                                                            }
                                                        }
                                                    }
                                                    if !rel_stmts.is_empty() {
                                                        let batch = format!(
                                                            "BEGIN; {} COMMIT;",
                                                            rel_stmts.join(" ")
                                                        );
                                                        if let Err(e) = db.query(&batch).await {
                                                            warn!("similarity RELATE batch failed for {}: {}", w.job.stable_id, e);
                                                        }
                                                    }
                                                }
                                                let _: () = conn.del(&w.key).await?;
                                            }
                                            Err(e) => {
                                                warn!(
                                                    "Surreal update failed for {}: {}",
                                                    w.job.stable_id, e
                                                );
                                            }
                                        }
                                    }
                                    SurrealConnection::RemoteWs(db_conn) => {
                                        let mut call = db_conn.query(&q);
                                        call = call.bind(("embedding", v.clone()));
                                        call = call.bind(("embedding_len", dim));
                                        call = call.bind(("embedding_dim", dim));
                                        call = call.bind(("embedding_model", model_id.clone()));
                                        if w.persist_source {
                                            call = call
                                                .bind(("source_content", w.source_content.clone()));
                                        }
                                        // similarity edges are written after this update
                                        match call.await {
                                            Ok(resp) => {
                                                if std::env::var("HZ_EMBED_DEBUG_DB")
                                                    .ok()
                                                    .as_deref()
                                                    == Some("1")
                                                {
                                                    info!("Surreal update response: {:?}", resp);
                                                }
                                                if enable_similarity {
                                                    let mut rel_stmts: Vec<String> = Vec::new();
                                                    rel_stmts.push(format!("DELETE similar_same_repo WHERE in = entity:{};", rid));
                                                    rel_stmts.push(format!("DELETE similar_external_repo WHERE in = entity:{};", rid));
                                                    for v in &similar_same_vals {
                                                        if let Some(obj) = v.as_object() {
                                                            if let (Some(tid), Some(score)) = (
                                                                obj.get("stable_id")
                                                                    .and_then(|x| x.as_str()),
                                                                obj.get("score")
                                                                    .and_then(|x| x.as_f64()),
                                                            ) {
                                                                rel_stmts.push(format!(
                                                                    "RELATE entity:{}->similar_same_repo->entity:{} SET score = {};",
                                                                    rid, sanitize_id(tid), score
                                                                ));
                                                            }
                                                        }
                                                    }
                                                    for v in &similar_external_vals {
                                                        if let Some(obj) = v.as_object() {
                                                            if let (Some(tid), Some(score)) = (
                                                                obj.get("stable_id")
                                                                    .and_then(|x| x.as_str()),
                                                                obj.get("score")
                                                                    .and_then(|x| x.as_f64()),
                                                            ) {
                                                                rel_stmts.push(format!(
                                                                    "RELATE entity:{}->similar_external_repo->entity:{} SET score = {};",
                                                                    rid, sanitize_id(tid), score
                                                                ));
                                                            }
                                                        }
                                                    }
                                                    if !rel_stmts.is_empty() {
                                                        let batch = format!(
                                                            "BEGIN; {} COMMIT;",
                                                            rel_stmts.join(" ")
                                                        );
                                                        if let Err(e) = db.query(&batch).await {
                                                            warn!("similarity RELATE batch failed for {}: {}", w.job.stable_id, e);
                                                        }
                                                    }
                                                }
                                                let _: () = conn.del(&w.key).await?;
                                            }
                                            Err(e) => {
                                                warn!(
                                                    "Surreal update failed for {}: {}",
                                                    w.job.stable_id, e
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => warn!(
                                "failed to convert embedding to json value for {}: {}",
                                w.job.stable_id, e
                            ),
                        }
                    } else {
                        warn!("no embedding vector for index {} when writing batch", i);
                    }
                }
            } else {
                warn!(
                    "TEI failed for batch of {} items; leaving for retry",
                    slice.len()
                );
            }
            idx = end;
        }
    }
}

async fn ensure_embedding_schema(
    db: &hyperzoekt::db_writer::connection::SurrealConnection,
) -> Result<()> {
    let stmts: &[&str] = &[
        // embedding array
        "DEFINE FIELD embedding ON entity TYPE array DEFAULT [];",
        "REMOVE FIELD embedding ON entity; DEFINE FIELD embedding ON entity TYPE array DEFAULT [];",
        "ALTER TABLE entity CREATE FIELD embedding TYPE array;",
        // embedding_len
        "DEFINE FIELD embedding_len ON entity TYPE int DEFAULT 0;",
        "REMOVE FIELD embedding_len ON entity; DEFINE FIELD embedding_len ON entity TYPE int DEFAULT 0;",
        "ALTER TABLE entity CREATE FIELD embedding_len TYPE int;",
        // embedding_model
        "DEFINE FIELD embedding_model ON entity TYPE string DEFAULT '';",
        "REMOVE FIELD embedding_model ON entity; DEFINE FIELD embedding_model ON entity TYPE string DEFAULT '';",
        "ALTER TABLE entity CREATE FIELD embedding_model TYPE string;",
        // embedding_dim
        "DEFINE FIELD embedding_dim ON entity TYPE int DEFAULT 0;",
        "REMOVE FIELD embedding_dim ON entity; DEFINE FIELD embedding_dim ON entity TYPE int DEFAULT 0;",
        "ALTER TABLE entity CREATE FIELD embedding_dim TYPE int;",
        // embedding_created_at
        "DEFINE FIELD embedding_created_at ON entity TYPE datetime DEFAULT time::now();",
        "REMOVE FIELD embedding_created_at ON entity; DEFINE FIELD embedding_created_at ON entity TYPE datetime DEFAULT time::now();",
        "ALTER TABLE entity CREATE FIELD embedding_created_at TYPE datetime;",
    // source_content (full text used for embeddings)
    "DEFINE FIELD source_content ON entity TYPE string DEFAULT '';",
    "REMOVE FIELD source_content ON entity; DEFINE FIELD source_content ON entity TYPE string DEFAULT '';",
    "ALTER TABLE entity CREATE FIELD source_content TYPE string;",
    // similarity relations and metadata (score)
    "DEFINE TABLE similar_same_repo TYPE RELATION FROM entity TO entity;",
    "DEFINE TABLE similar_same_repo TYPE RELATION;",
    "CREATE TABLE similar_same_repo;",
    "DEFINE INDEX idx_similar_same_repo_unique ON similar_same_repo FIELDS in, out UNIQUE;",
    "DEFINE INDEX idx_similar_same_repo_unique ON similar_same_repo FIELDS in, out UNIQUE;",
    "DEFINE FIELD score ON similar_same_repo TYPE number DEFAULT 0;",
    "REMOVE FIELD score ON similar_same_repo; DEFINE FIELD score ON similar_same_repo TYPE number DEFAULT 0;",
    "ALTER TABLE similar_same_repo CREATE FIELD score TYPE number;",
    "DEFINE TABLE similar_external_repo TYPE RELATION FROM entity TO entity;",
    "DEFINE TABLE similar_external_repo TYPE RELATION;",
    "CREATE TABLE similar_external_repo;",
    "DEFINE INDEX idx_similar_external_repo_unique ON similar_external_repo FIELDS in, out UNIQUE;",
    "DEFINE INDEX idx_similar_external_repo_unique ON similar_external_repo FIELDS in, out UNIQUE;",
    "DEFINE FIELD score ON similar_external_repo TYPE number DEFAULT 0;",
    "REMOVE FIELD score ON similar_external_repo; DEFINE FIELD score ON similar_external_repo TYPE number DEFAULT 0;",
    "ALTER TABLE similar_external_repo CREATE FIELD score TYPE number;",
    ];
    for s in stmts {
        match db.query(s).await {
            Ok(_) => {
                if std::env::var("HZ_EMBED_DEBUG_SQL").ok().as_deref() == Some("1") {
                    info!("schema applied: {}", s);
                }
            }
            Err(e) => {
                // Best-effort: log at trace and continue
                if std::env::var("HZ_EMBED_DEBUG_SQL").ok().as_deref() == Some("1") {
                    warn!("schema variant failed: {} -> {}", s, e);
                }
            }
        }
    }
    // best-effort upgrade of relation tables if only created generically earlier
    for upgrade in [
        "ALTER TABLE similar_same_repo TYPE RELATION FROM entity TO entity;",
        "ALTER TABLE similar_external_repo TYPE RELATION FROM entity TO entity;",
    ] {
        if let Err(e) = db.query(upgrade).await {
            if std::env::var("HZ_EMBED_DEBUG_SQL").ok().as_deref() == Some("1") {
                warn!("relation upgrade attempt failed: {} -> {}", upgrade, e);
            }
        } else if std::env::var("HZ_EMBED_DEBUG_SQL").ok().as_deref() == Some("1") {
            info!("relation upgrade applied: {}", upgrade);
        }
    }
    Ok(())
}

async fn recover_expired(
    pool: &deadpool_redis::Pool,
    queue_key: &str,
    processing_prefix: &str,
) -> Result<()> {
    let mut conn = pool.get().await?;
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

fn sanitize_id(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut last_was_us = false;
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            last_was_us = false;
        } else {
            if !last_was_us {
                out.push('_');
            }
            last_was_us = true;
        }
    }
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "_".to_string()
    } else {
        trimmed.to_string()
    }
}

// Best-effort parser to extract a suggested max batch from textual TEI messages.
fn parse_max_batch_from_text(body: &str, current: usize) -> usize {
    // Look for phrases like "max batch is 8", "max_batch=8", "batch size 8"
    let lowercase = body.to_lowercase();
    let tokens: Vec<&str> = lowercase
        .split(|c: char| !c.is_ascii_alphanumeric())
        .collect();
    let mut best = current;
    for (i, t) in tokens.iter().enumerate() {
        if *t == "max" || *t == "max_batch" || *t == "batch" || *t == "size" {
            // check neighboring tokens for a number
            for j in 1..=2 {
                if let Some(nb) = tokens.get(i + j).and_then(|s| s.parse::<usize>().ok()) {
                    if nb > 0 && nb < best {
                        best = nb;
                    }
                }
            }
        }
        // also handle token like "8" following "batch" earlier
        if *t == "batch" {
            if let Some(nb) = tokens.get(i + 1).and_then(|s| s.parse::<usize>().ok()) {
                if nb > 0 && nb < best {
                    best = nb;
                }
            }
        }
    }
    best
}

// Compute similarity arrays (same repo & external) returning JSON arrays of objects {stable_id, score}.
struct SimilarityParams {
    max_same: usize,
    max_external: usize,
}

async fn compute_similarity_arrays(
    db: &hyperzoekt::db_writer::connection::SurrealConnection,
    _stable_id: &str,
    repo_name: &str,
    embedding: &[f32],
    params: &SimilarityParams,
) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>), anyhow::Error> {
    // If embedding is empty, nothing to do
    if embedding.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    // Prepare vector param as JSON array of numbers (f64)
    let vec_param: Vec<serde_json::Value> = embedding
        .iter()
        .map(|f| serde_json::Value::from(*f as f64))
        .collect();
    let dim = embedding.len() as i64;

    // Server-side cosine similarity query for same-repo candidates
    let same_q = "SELECT stable_id, vector::similarity::cosine(embedding, $vec) AS score FROM entity WHERE repo_name = $r AND embedding_len = $dim AND embedding_len > 0 ORDER BY score DESC LIMIT $lim;";
    let same_rows: Vec<serde_json::Value> = match db
        .query_with_binds(
            same_q,
            vec![
                ("r", serde_json::Value::String(repo_name.to_string())),
                ("vec", serde_json::Value::Array(vec_param.clone())),
                (
                    "lim",
                    serde_json::Value::Number((params.max_same as i64).into()),
                ),
                ("dim", serde_json::Value::Number(dim.into())),
            ],
        )
        .await
    {
        Ok(mut r) => r.take(0).unwrap_or_default(),
        Err(e) => {
            warn!("server-side same-repo similarity query failed: {}", e);
            Vec::new()
        }
    };
    let mut same_json: Vec<serde_json::Value> = Vec::new();
    for row in same_rows.iter() {
        if let Some(obj) = row.as_object() {
            if let Some(sid) = obj.get("stable_id").and_then(|v| v.as_str()) {
                if let Some(score) = obj.get("score").and_then(|v| v.as_f64()) {
                    let mut o = serde_json::Map::new();
                    o.insert(
                        "stable_id".to_string(),
                        serde_json::Value::String(sid.to_string()),
                    );
                    o.insert("score".to_string(), serde_json::Value::from(score));
                    same_json.push(serde_json::Value::Object(o));
                }
            }
        }
    }

    // Server-side cosine similarity query for external-repo candidates
    let ext_q = "SELECT stable_id, vector::similarity::cosine(embedding, $vec) AS score FROM entity WHERE repo_name != $r AND embedding_len = $dim AND embedding_len > 0 ORDER BY score DESC LIMIT $lim;";
    let ext_rows: Vec<serde_json::Value> = match db
        .query_with_binds(
            ext_q,
            vec![
                ("r", serde_json::Value::String(repo_name.to_string())),
                ("vec", serde_json::Value::Array(vec_param)),
                (
                    "lim",
                    serde_json::Value::Number((params.max_external as i64).into()),
                ),
                ("dim", serde_json::Value::Number(dim.into())),
            ],
        )
        .await
    {
        Ok(mut r) => r.take(0).unwrap_or_default(),
        Err(e) => {
            warn!("server-side external-repo similarity query failed: {}", e);
            Vec::new()
        }
    };
    let mut ext_json: Vec<serde_json::Value> = Vec::new();
    for row in ext_rows.iter() {
        if let Some(obj) = row.as_object() {
            if let Some(sid) = obj.get("stable_id").and_then(|v| v.as_str()) {
                if let Some(score) = obj.get("score").and_then(|v| v.as_f64()) {
                    let mut o = serde_json::Map::new();
                    o.insert(
                        "stable_id".to_string(),
                        serde_json::Value::String(sid.to_string()),
                    );
                    o.insert("score".to_string(), serde_json::Value::from(score));
                    ext_json.push(serde_json::Value::Object(o));
                }
            }
        }
    }

    Ok((same_json, ext_json))
}

#[cfg(test)]
mod tests {
    use super::parse_embeddings_and_model;

    #[test]
    fn parse_tei_native() {
        let json = r#"{"embeddings":[[0.1,0.2,0.3],[0.4,0.5,0.6]], "model":"mymodel"}"#;
        let res = parse_embeddings_and_model(json.as_bytes()).expect("parse failed");
        assert_eq!(res.0.len(), 2);
        assert_eq!(res.0[0][0], 0.1f32);
        assert_eq!(res.1.unwrap(), "mymodel");
    }

    #[test]
    fn parse_openai_style() {
        let json =
            r#"{"data":[{"embedding":[0.1,0.2]},{"embedding":[0.3,0.4]}], "model":"openai-xyz"}"#;
        let res = parse_embeddings_and_model(json.as_bytes()).expect("parse failed");
        assert_eq!(res.0.len(), 2);
        assert_eq!(res.0[1][1], 0.4f32);
        assert_eq!(res.1.unwrap(), "openai-xyz");
    }
}
