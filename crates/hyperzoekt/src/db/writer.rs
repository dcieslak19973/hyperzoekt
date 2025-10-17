// Copyright 2025 HyperZoekt Project

use crate::db::config::DbWriterConfig;
use crate::db::config::{PersistAckSender, PersistedEntityMeta, SpawnResult};
use crate::db::connection::{connect, SurrealConnection};
use crate::db::helpers::CALL_EDGE_CAPTURE;
use crate::repo_index::indexer::payload::EntityPayload;
use anyhow::Result;
use log::{debug, info, trace, warn};
use reqwest::Client as HttpClient;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::sync::mpsc::sync_channel;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;
use tiktoken_rs::cl100k_base;

pub fn spawn_db_writer(
    payloads: Vec<EntityPayload>,
    cfg: DbWriterConfig,
    ack_tx: Option<PersistAckSender>,
) -> SpawnResult {
    let (tx, rx) = sync_channel::<Vec<EntityPayload>>(cfg.channel_capacity);
    let payloads_clone = payloads.clone();
    let batch_capacity = cfg.batch_capacity.unwrap_or(500);
    let batch_timeout = Duration::from_millis(cfg.batch_timeout_ms.unwrap_or(500));
    let max_retries = cfg.max_retries.unwrap_or(3);
    let cfg_clone = cfg.clone();
    let join = thread::spawn(move || {
        // Allow tests to request a reset of global dedup/creation sets for deterministic
        // edge counts.
        if std::env::var("HZ_RESET_GLOBALS").ok().as_deref() == Some("1") {
            if let Some(set) = FILE_REPO_EDGE_SEEN.get() {
                if let Ok(mut g) = set.lock() {
                    g.clear();
                }
            }
            if let Some(set) = FILE_IDS_CREATED.get() {
                if let Ok(mut g) = set.lock() {
                    g.clear();
                }
            }
        }
        info!("db_writer thread spawned");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async move {
            info!("db_writer runtime async context entered");
            let db = connect(&cfg_clone.surreal_url, &cfg_clone.surreal_username, &cfg_clone.surreal_password, &cfg_clone.surreal_ns, &cfg_clone.surreal_db).await?;
            info!("db_writer connected to surreal ns={} db={}", cfg_clone.surreal_ns, cfg_clone.surreal_db);
            // Debug: Verify we're in the correct namespace/database
            if let Ok(mut resp) = db.query("SELECT * FROM $session;").await {
                if let Ok(session_info) = resp.take::<Vec<serde_json::Value>>(0) {
                    info!("db_writer session info: {:?}", session_info);
                }
            }
            if cfg_clone.initial_batch {
                if !payloads_clone.is_empty() {
                    initial_batch_insert(&db, &payloads_clone, batch_capacity).await?;
                    return Ok(());
                } else {
                    info!("initial_batch=true but no initial payloads provided; continuing to normal writer loop");
                }
            }
            info!("db_writer starting schema init");
            init_schema(&db, &cfg_clone.surreal_ns, &cfg_clone.surreal_db).await;
            info!("db_writer finished schema init");
            if let Some(meta) = &cfg.commit_meta {
                let parents_refs: Vec<&str> = meta.parents.iter().map(|s| s.as_str()).collect();
                if let Err(e) = super::create_commit(&db, &meta.commit_id, &cfg.repo_name, &parents_refs, meta.tree.as_deref(), meta.author.as_deref(), meta.message.as_deref()).await {
                    warn!("Failed to create commit {} in db_writer: {}", meta.commit_id, e);
                }
            }
            use std::sync::mpsc::{RecvTimeoutError, TryRecvError};
            // Embedding enqueue setup (reuse existing gate env var). We now enqueue per batch AFTER persistence.
            let embed_jobs_enabled = matches!(std::env::var("HZ_ENABLE_EMBED_JOBS").unwrap_or_default().as_str(), "1" | "true" | "TRUE" | "yes" | "YES");
            let embed_queue = std::env::var("HZ_EMBED_JOBS_QUEUE").unwrap_or_else(|_| "zoekt:embed_jobs".to_string());
            let embed_pool = if embed_jobs_enabled { zoekt_distributed::redis_adapter::create_redis_pool() } else { None };
            if embed_jobs_enabled && embed_pool.is_none() { info!("embed enqueue disabled: redis pool not available"); }
            // Pre-create embedding client/endpoint if a model is configured so we can reuse it per batch.
            let embed_model = std::env::var("HZ_EMBED_MODEL").unwrap_or_else(|_| String::new());
            let (tei_endpoint_opt, http_client_opt) = if !embed_model.is_empty() {
                let tei_base = std::env::var("HZ_TEI_BASE").unwrap_or_else(|_| "http://tei:80".to_string());
                let tei_endpoint = format!("{}/embeddings", tei_base.trim_end_matches('/'));
                let http_client = HttpClient::builder()
                    .timeout(Duration::from_secs(30))
                    .build()
                    .expect("http client");
                (Some(tei_endpoint), Some(http_client))
            } else {
                (None, None)
            };
            let mut batches_sent=0usize; let mut entities_sent=0usize; let mut total_retries=0usize; let mut sum_ms: u128=0; let mut min_ms: Option<u128>=None; let mut max_ms: Option<u128>=None; let mut failures=0usize; let mut attempt_counts=std::collections::BTreeMap::new();
            loop {
                info!("writer loop tick: waiting for first batch (timeout {:?})", batch_timeout);
                let first = match rx.recv_timeout(batch_timeout) { Ok(v)=>v, Err(RecvTimeoutError::Timeout)=>{ match rx.try_recv(){ Ok(v)=>v, Err(TryRecvError::Empty)=>continue, Err(TryRecvError::Disconnected)=>break } }, Err(RecvTimeoutError::Disconnected)=>break };
                let first_len = first.len();
                info!("writer received initial batch fragment of size {}", first_len);
                let mut acc=first; while acc.len()<batch_capacity { match rx.try_recv(){ Ok(mut more)=>{ acc.append(&mut more); if acc.len()>=batch_capacity { break; } }, Err(TryRecvError::Empty)=>break, Err(TryRecvError::Disconnected)=>break } }
                info!("writer accumulated batch size {} (capacity {})", acc.len(), batch_capacity);
                if acc.is_empty(){ continue; }
                if let Some(cap)=CALL_EDGE_CAPTURE.get(){ if let Ok(mut v)=cap.lock(){ use std::collections::HashSet; let mut names=HashSet::new(); for e in acc.iter(){ names.insert(e.name.as_str()); } for e in acc.iter(){ for c in e.calls.iter(){ if names.contains(c.as_str()){ v.push((e.name.clone(), c.clone())); } } } } }
                let mut attempt = 0usize;
                let mut success = false;
                loop {
                    attempt += 1;
                    // If an embed model is configured and we are not enqueuing embed jobs,
                    // compute embeddings now so they can be persisted on entity_snapshot.
                    // NOTE: embeddings are mostly working; however, some entities may
                    // still be left without embeddings when their `source_content`
                    // is empty. This often stems from upstream tree-sitter extraction
                    // not populating `source_content` for certain entities (or when
                    // extraction is partial). If you observe many missing embeddings
                    // investigate the indexer/tree-sitter extraction path first —
                    // falling back to fabricating snippets (e.g. using `name`) was
                    // intentionally removed to avoid masking this root cause.
                    let embeddings_for_batch: Vec<MaybeEmbedding> = if !embed_jobs_enabled {
                        if !embed_model.is_empty() {
                            if let (Some(endpoint), Some(client)) = (tei_endpoint_opt.as_deref(), http_client_opt.as_ref()) {
                                compute_embeddings_for_payloads(client, endpoint, &embed_model, &acc).await
                            } else {
                                vec![None; acc.len()]
                            }
                        } else {
                            vec![None; acc.len()]
                        }
                    } else {
                        vec![None; acc.len()]
                    };
                    let (mut statements_raw, entity_count) = build_batch_sql(&acc, &cfg_clone, &embeddings_for_batch);
                    if statements_raw.is_empty() {
                        success = true;
                        info!("empty sql batch (no ops) size {} attempt {}", acc.len(), attempt);
                        break;
                    }
                    if std::env::var("HZ_DUMP_SQL_STATEMENTS").ok().as_deref() == Some("1") {
                        // Print to stdout so test harness captures the full statements.
                        println!("DUMPING RAW STATEMENTS ({} total):", statements_raw.len());
                        for (i, (s, _)) in statements_raw.iter().enumerate() {
                            println!("STMT[{}]: {}", i, s);
                        }
                    }
                    if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                        println!("STATEMENTS_RAW LEN: {}", statements_raw.len());
                        for (i, (s, _)) in statements_raw.iter().enumerate() {
                            if s.starts_with("RELATE") {
                                println!("STATEMENT {}: RELATE: {}", i, s);
                            }
                        }
                    }
                    // Group non-RELATE statements into transactional chunks to reduce round trips.
                    let group_size: usize = std::env::var("HZ_STATEMENT_GROUP")
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .filter(|v| *v > 0)
                        .unwrap_or(200);
                    // Partition statements so RELATEs are executed separately. RELATEs can fail
                    // with unique-index conflicts; running them separately allows us to ignore
                    // duplicate-edge errors without aborting the main transactional writes.
                    // Additionally, split CREATE entity statements into their own chunks so
                    // duplicate-create errors don't abort other updates in the same transaction.
                    type StmtBinds = Vec<(&'static str, serde_json::Value)>;
                    type SqlStmt = (String, Option<StmtBinds>);
                    let mut create_entities: Vec<SqlStmt> = Vec::new();
                    let mut non_create: Vec<SqlStmt> = Vec::new();
                    let mut relates: Vec<SqlStmt> = Vec::new();
                    for (sql, binds) in statements_raw.iter() {
                        let ts = sql.trim_start();
                        if ts.to_uppercase().starts_with("RELATE ") {
                            if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") { println!("SEPARATING RELATE: {}", sql); }
                            relates.push((sql.clone(), binds.clone()));
                        } else if ts.starts_with("CREATE entity:") || ts.starts_with("CREATE repo:") || ts.starts_with("CREATE entity_snapshot:") {
                            create_entities.push((sql.clone(), binds.clone()));
                        } else {
                            non_create.push((sql.clone(), binds.clone()));
                        }
                    }
                    // For grouping, only group non-create statements that have no binds.
                    let create_statements = Vec::<String>::new();
                    let mut non_create_no_binds: Vec<String> = Vec::new();
                    let mut non_create_with_binds: Vec<(String, StmtBinds)> = Vec::new();
                    for (s, b) in non_create.iter() {
                        if let Some(binds) = b {
                            non_create_with_binds.push((s.clone(), binds.clone()));
                        } else {
                            non_create_no_binds.push(s.clone());
                        }
                    }
                    let statements = group_statements(&non_create_no_binds, group_size);
                    if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                        let total_len: usize = statements.iter().map(|s| s.len()).sum();
                        info!("BATCH SQL ({} grouped chunks, original {} stmts non_create={} relate={}) total_len={} chars", statements.len(), statements_raw.len(), non_create.len(), relates.len(), total_len);
                    }
                    let start = std::time::Instant::now();
                    let mut exec_error: Option<String> = None;
                    info!("executing create-entity chunks={} and non-create chunks={} (orig stmts create={} non_create={} relate={})", create_statements.len(), statements.len(), create_entities.len(), non_create.len(), relates.len());
                    // Execute CREATE entity statements individually so we can detect which
                    // specific ids failed and issue a targeted UPDATE for that entity only.
                    for (s_sql, s_binds) in &create_entities {
                        let q = format!("BEGIN; {} COMMIT;", s_sql);
                        if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                            println!("EXECUTING CREATE-ENTITY (len={}): {}", q.len(), q);
                        }
                        let exec_result = if let Some(binds) = s_binds {
                            db.query_with_binds(&q, binds.clone()).await
                        } else {
                            db.query(&q).await
                        };
                        match exec_result {
                            Ok(resp) => {
                                let resp_str = format!("{:?}", resp);
                                if resp_str.contains("Err(Api(Query") {
                                    // If the error is a duplicate-create, perform a per-entity UPDATE fallback.
                                    if resp_str.contains("already exists") || resp_str.contains("already contains") {
                                        // Parse id and json from the original CREATE statement and run UPDATE fallback.
                                        let mut handled = false;
                                        if let Some(rest) = s_sql.strip_prefix("CREATE entity:") {
                                            if let Some((id_part, json_part)) = rest.split_once(" CONTENT ") {
                                                let id = id_part.trim();
                                                let mut json = json_part.trim();
                                                if json.ends_with(';') { json = json.trim_end_matches(';').trim(); }
                                                let update_q = format!("BEGIN; UPDATE entity:{id} CONTENT {json}; COMMIT;", id=id, json=json);
                                                if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") { println!("CREATE duplicate for {} detected; running per-entity fallback: {}", id, update_q); }
                                                match db.query(&update_q).await {
                                                    Ok(_uresp) => {
                                                        if std::env::var("HZ_DEBUG_SQL_RESP").ok().as_deref() == Some("1") { info!("per-entity UPDATE ok for {}", id); }
                                                    }
                                                    Err(ue) => { warn!("per-entity UPDATE failed for {} err={}", id, ue); }
                                                }
                                                handled = true;
                                            }
                                        }
                                        if !handled {
                                            if let Some(rest) = s_sql.strip_prefix("CREATE repo:") {
                                                if let Some((id_part, json_part)) = rest.split_once(" CONTENT ") {
                                                    let id = id_part.trim();
                                                    let mut json = json_part.trim();
                                                    if json.ends_with(';') { json = json.trim_end_matches(';').trim(); }
                                                    let update_q = format!("BEGIN; UPDATE repo:{id} CONTENT {json}; COMMIT;", id=id, json=json);
                                                    if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") { println!("CREATE duplicate for repo {} detected; running per-repo fallback: {}", id, update_q); }
                                                    match db.query(&update_q).await {
                                                        Ok(_uresp) => {
                                                            if std::env::var("HZ_DEBUG_SQL_RESP").ok().as_deref() == Some("1") { info!("per-repo UPDATE ok for {}", id); }
                                                        }
                                                        Err(ue) => { warn!("per-repo UPDATE failed for {} err={}", id, ue); }
                                                    }
                                                    handled = true;
                                                }
                                            }
                                        }
                                        if !handled {
                                            if let Some(rest) = s_sql.strip_prefix("CREATE entity_snapshot:") {
                                                if let Some((id_part, json_part)) = rest.split_once(" CONTENT ") {
                                                    let id = id_part.trim();
                                                    let mut json = json_part.trim();
                                                    if json.ends_with(';') { json = json.trim_end_matches(';').trim(); }
                                                    let update_q = format!("BEGIN; UPDATE entity_snapshot:{id} CONTENT {json}; COMMIT;", id=id, json=json);
                                                    if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") { println!("CREATE duplicate for entity_snapshot {} detected; running per-entity_snapshot fallback: {}", id, update_q); }
                                                    match db.query(&update_q).await {
                                                        Ok(_uresp) => {
                                                            if std::env::var("HZ_DEBUG_SQL_RESP").ok().as_deref() == Some("1") { info!("per-entity_snapshot UPDATE ok for {}", id); }
                                                        }
                                                        Err(ue) => { warn!("per-entity_snapshot UPDATE failed for {} err={}", id, ue); }
                                                    }
                                                    handled = true;
                                                }
                                            }
                                        }
                                        if !handled { info!("create-entity duplicate ignored (couldn't parse id/json): {}", resp_str); }
                                    } else {
                                        // Unexpected error on create: log and continue
                                        warn!("create-entity had unexpected error (continuing): {}", resp_str);
                                    }
                                } else if std::env::var("HZ_DEBUG_SQL_RESP").ok().as_deref() == Some("1") {
                                    // Print to stdout so test harness captures it reliably.
                                    println!("CREATE-ENTITY RESP: {:?}", resp_str);
                                }
                            }
                            Err(e) => {
                                // Network/other error on create: warn but continue with remaining work.
                                warn!("create-entity query failed (continuing): {} err={}", q, e);
                            }
                        }
                    }
                    // Execute non-create statements that have binds (each as its own transaction).
                    for (s_sql, binds) in &non_create_with_binds {
                        let q = format!("BEGIN; {} COMMIT;", s_sql);
                        if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") { println!("EXECUTING NON-CREATE WITH BINDS: {}", q); }
                        match db.query_with_binds(&q, binds.clone()).await {
                            Ok(resp) => {
                                let resp_str = format!("{:?}", resp);
                                if resp_str.contains("Err(Api(Query") {
                                    warn!("non-create with-binds response contained errors: {}", resp_str);
                                    exec_error = Some(resp_str);
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("non-create with-binds failed: {} err:{}", q, e);
                                exec_error = Some(e.to_string());
                                break;
                            }
                        }
                    }
                    // Execute primary non-RELATE transactional chunks next. Failures here abort the batch.
                    for chunk in &statements {
                        if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") { println!("EXECUTING CHUNK: {}", chunk); }
                        match db.query(chunk).await {
                            Ok(resp) => {
                                let resp_str = format!("{:?}", resp);
                                if resp_str.contains("Err(Api(Query") && !(resp_str.contains("already contains") || resp_str.contains("already exists")) {
                                    warn!("non-RELATE chunk response contained errors: {}", resp_str);
                                    exec_error = Some(resp_str);
                                    break;
                                } else if std::env::var("HZ_DEBUG_SQL_RESP").ok().as_deref() == Some("1") { info!("chunk ok len={} bytes", chunk.len()); debug!("resp: {:?}", resp); }
                            }
                            Err(e) => {
                                warn!("chunk failed (len={}): first 200 chars: {} err:{}", chunk.len(), &chunk.chars().take(200).collect::<String>(), e);
                                exec_error = Some(e.to_string());
                                break;
                            }
                        }
                    }
                    // If main transactional writes failed, we'll retry per existing heuristics.
                    if exec_error.is_some() {
                        // proceed to retry handling below
                    } else {
                        // Now execute RELATE statements individually and tolerate duplicate/index errors.
                        for (rel_sql, _rel_binds) in &relates {
                            let q = format!("BEGIN; {} COMMIT;", rel_sql);
                            if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") { println!("EXECUTING RELATE CHUNK: {}", q); }
                            match db.query(&q).await {
                                Ok(resp) => {
                                    let resp_str = format!("{:?}", resp);
                                    if resp_str.contains("already contains") || resp_str.contains("already exists") {
                                        // benign duplicate edge
                                        info!("relate duplicate ignored: {}", resp_str);
                                        continue;
                                    }
                                    if resp_str.contains("Err(Api(Query") {
                                        // Unknown error on RELATE: log and continue (do not abort main batch)
                                        warn!("relate had unexpected error: {}", resp_str);
                                        continue;
                                    }
                                }
                                Err(e) => {
                                    // Network/other error on RELATE; log and continue
                                    warn!("relate query failed: {} err:{}", rel_sql, e);
                                    continue;
                                }
                            }
                        }
                    }
                    if let Some(e) = exec_error {
                        failures += 1;
                        warn!("batch failed attempt {} err: {}", attempt, e);
                        // Heuristic: if failure due to a CREATE repo conflict, try converting
                        // CREATE repo:<id> -> UPDATE repo:<id> and retry once. This handles
                        // cases where another path (e.g., persist_repo_dependencies) already
                        // created the deterministic repo id before the writer batch runs.
                        if attempt < max_retries && e.contains("Database record") && e.contains("already exists") {
                            info!("batch failure looks like 'already exists'; transforming CREATE -> UPDATE for deterministic ids and retrying");
                            // Transform CREATE repo:... and CREATE entity:... into UPDATE to handle races
                            for s in statements_raw.iter_mut() {
                                if s.0.trim_start().starts_with("CREATE entity:") {
                                    s.0 = s.0.replacen("CREATE entity:", "UPDATE entity:", 1);
                                }
                                if s.0.trim_start().starts_with("CREATE entity_snapshot:") {
                                    s.0 = s.0.replacen("CREATE entity_snapshot:", "UPDATE entity_snapshot:", 1);
                                }
                            }
                            total_retries += 1;
                            std::thread::sleep(Duration::from_millis(100 * (1 << (attempt - 1)).min(8)));
                            continue;
                        }
                        if attempt >= max_retries {
                            break;
                        }
                        total_retries += 1;
                        std::thread::sleep(Duration::from_millis(100 * (1 << (attempt - 1)).min(8)));
                        continue;
                    }
                    let dur = start.elapsed().as_millis();
                    if std::env::var("HZ_DUMP_SQL_STATEMENTS").ok().as_deref() == Some("1") {
                        // Post-batch diagnostics: counts and sample edge rows.
                        if let Ok(mut diag) = db.query("SELECT count() AS ec FROM entity GROUP ALL; SELECT count() AS cc FROM calls GROUP ALL; SELECT in,out FROM calls LIMIT 5;").await {
                            let entc: Vec<serde_json::Value> = diag.take(0).unwrap_or_default();
                            let callc: Vec<serde_json::Value> = diag.take(1).unwrap_or_default();
                            let callrows: Vec<serde_json::Value> = diag.take(2).unwrap_or_default();
                            info!("DIAG entity_count_rows={:?} calls_count_rows={:?} sample_calls={:?}", entc, callc, callrows);
                        }
                    }
                    batches_sent += 1;
                    entities_sent += entity_count;
                    sum_ms += dur;
                    min_ms = Some(min_ms.map_or(dur, |m| m.min(dur)));
                    max_ms = Some(max_ms.map_or(dur, |m| m.max(dur)));
                    *attempt_counts.entry(attempt).or_insert(0) += 1;
                    success = true;
                    // Per-batch embedding enqueue (after successful persistence)
                    if embed_jobs_enabled {
                        if let Some(ref pool) = embed_pool {
                            if !acc.is_empty() {
                                #[derive(serde::Serialize)]
                                struct EmbeddingJob<'a> {
                                    stable_id: &'a str,
                                    repo_name: &'a str,
                                    language: &'a str,
                                    kind: &'a str,
                                    name: &'a str,
                                    source_url: Option<&'a str>,
                                }
                                // Build job JSON strings for this batch
                                let mut job_buf: Vec<String> = Vec::with_capacity(acc.len());
                                for e in &acc {
                                    let job = EmbeddingJob { stable_id: &e.stable_id, repo_name: &e.repo_name, language: &e.language, kind: &e.kind, name: &e.name, source_url: e.source_url.as_deref() };
                                    match serde_json::to_string(&job) { Ok(js) => job_buf.push(js), Err(err) => { warn!("serialize embed job failed stable_id={} err={}", e.stable_id, err); } }
                                }
                                if !job_buf.is_empty() {
                                    match pool.get().await { Ok(mut conn) => {
                                        // Chunk pushes to avoid extremely large single RPUSH
                                        let mut pushed_total = 0usize;
                                        for chunk in job_buf.chunks(500) {
                                            let res: Result<usize, _> = deadpool_redis::redis::AsyncCommands::rpush(&mut conn, &embed_queue, chunk).await;
                                            match res { Ok(_n) => { pushed_total += chunk.len(); }, Err(e) => { warn!("embed enqueue rpush failed chunk={} err={}", chunk.len(), e); break; } }
                                        }
                                        info!("embed enqueue batch complete repo={} jobs={} queue={} batches_sent={}", acc.first().map(|e| e.repo_name.as_str()).unwrap_or("?"), pushed_total, embed_queue, batches_sent);
                                    }, Err(e) => warn!("embed enqueue redis get conn failed: {}", e) }
                                }
                            }
                        }
                    }
                    // After successful batch commit, emit acks if channel provided.
                    if let Some(ref sender) = ack_tx {
                        let mut metas: Vec<PersistedEntityMeta> = Vec::with_capacity(acc.len());
                        for e in &acc { metas.push(PersistedEntityMeta { stable_id: e.stable_id.clone(), repo_name: e.repo_name.clone(), language: e.language.clone(), kind: e.kind.clone(), name: e.name.clone(), source_url: e.source_url.clone(), }); }
                        if let Err(e) = sender.send(metas) { log::warn!("persist ack send failed: {}", e); }
                    }
                    info!("batch success size {} attempt {} ms {}", entity_count, attempt, dur);
                    // Separate count queries to avoid multi-statement response shape ambiguity
                    let repo_count = if let Ok(mut r) = db.query("SELECT count() FROM repo GROUP ALL;").await { let rows: Vec<serde_json::Value> = r.take(0).unwrap_or_default(); rows.first().and_then(|o| o.get("count").and_then(|v| v.as_i64())) } else { None };
                    let file_count = if let Ok(mut r) = db.query("SELECT count() FROM file GROUP ALL;").await { let rows: Vec<serde_json::Value> = r.take(0).unwrap_or_default(); rows.first().and_then(|o| o.get("count").and_then(|v| v.as_i64())) } else { None };
                    info!("post-batch table counts repo_count={:?} file_count={:?}", repo_count, file_count);
                    break;
                }
                if !success { warn!("dropping batch size={} after {} attempts", acc.len(), attempt); }
                if batches_sent > 0 && batches_sent.is_multiple_of(10) {
                    let avg = (sum_ms as f64) / (batches_sent as f64);
                    info!("DB metrics: batches_sent={} entities_sent={} total_retries={} avg_batch_ms={:.2} min_ms={:?} max_ms={:?} failures={} attempts={:?}", batches_sent, entities_sent, total_retries, avg, min_ms, max_ms, failures, attempt_counts);
                }
                if std::env::var("HZ_SINGLE_BATCH").ok().as_deref()==Some("1") { info!("HZ_SINGLE_BATCH set; breaking writer loop after {} batches", batches_sent); break; }
                info!("writer loop end iteration; continuing");
            }
            info!("writer loop exited cleanly");
            Ok(())
        })
    });
    Ok((tx, join))
}

async fn initial_batch_insert(
    db: &SurrealConnection,
    payloads: &[EntityPayload],
    chunk: usize,
) -> Result<()> {
    info!("Initial batch mode: inserting {} entities", payloads.len());

    // Compute embeddings for the initial batch and persist them on the
    // entity_snapshot records so no external worker/enqueue is necessary.
    let tei_base = std::env::var("HZ_TEI_BASE").unwrap_or_else(|_| "http://tei:80".to_string());
    let tei_endpoint = format!("{}/embeddings", tei_base.trim_end_matches('/'));
    let embed_model = std::env::var("HZ_EMBED_MODEL").unwrap_or_else(|_| String::new());
    let http_client = HttpClient::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("http client");
    let embeddings_for_batch = if !embed_model.is_empty() {
        compute_embeddings_for_payloads(&http_client, &tei_endpoint, &embed_model, payloads).await
    } else {
        vec![None; payloads.len()]
    };

    // Persist entities + snapshots in chunks to avoid huge transactions
    let mut base_idx = 0usize;
    for ch in payloads.chunks(chunk.clamp(1, 5000)) {
        let st = std::time::Instant::now();
        let mut success_count = 0usize;
        // Diagnostics: count how many embeddings were available for this chunk
        let mut embeddings_present = 0usize;
        let mut embeddings_missing = 0usize;
        for (local_i, p) in ch.iter().enumerate() {
            // Determine index into embeddings_for_batch for this payload
            // (we computed embeddings aligned with payloads slice)
            let global_idx = base_idx + local_i;

            // Build entity JSON
            let mut entity_obj = serde_json::Map::new();
            entity_obj.insert(
                "language".to_string(),
                serde_json::Value::String(p.language.clone()),
            );
            entity_obj.insert(
                "kind".to_string(),
                serde_json::Value::String(p.kind.clone()),
            );
            entity_obj.insert(
                "name".to_string(),
                serde_json::Value::String(p.name.clone()),
            );
            entity_obj.insert(
                "repo_name".to_string(),
                serde_json::Value::String(p.repo_name.clone()),
            );
            entity_obj.insert(
                "signature".to_string(),
                serde_json::Value::String(p.signature.clone()),
            );
            entity_obj.insert(
                "stable_id".to_string(),
                serde_json::Value::String(p.stable_id.clone()),
            );
            if let Some(sc) = &p.source_content {
                entity_obj.insert(
                    "source_content".to_string(),
                    serde_json::Value::String(sc.clone()),
                );
            }
            let mut entity_json = serde_json::Value::Object(entity_obj);
            sanitize_json_strings(&mut entity_json);

            // Build snapshot JSON (include embedding fields if computed)
            let mut snapshot_obj = serde_json::Map::new();
            snapshot_obj.insert(
                "repo_name".to_string(),
                serde_json::Value::String(p.repo_name.clone()),
            );
            snapshot_obj.insert(
                "stable_id".to_string(),
                serde_json::Value::String(p.stable_id.clone()),
            );
            snapshot_obj.insert(
                "name".to_string(),
                serde_json::Value::String(p.name.clone()),
            );
            if let Some(f) = &p.file {
                snapshot_obj.insert("file".to_string(), serde_json::Value::String(f.clone()));
                // Add embedded record reference to file table
                // Generate a deterministic file ID from repo_name and file path
                let file_id = format!("{}:{}", p.repo_name, f);
                let file_id_hash = format!("{:x}", Sha256::digest(file_id.as_bytes()));
                snapshot_obj.insert(
                    "file_ref".to_string(),
                    serde_json::Value::String(format!("file:{}", file_id_hash)),
                );
            }
            if let Some(par) = &p.parent {
                snapshot_obj.insert("parent".to_string(), serde_json::Value::String(par.clone()));
            }
            if let Some(sl) = p.start_line {
                snapshot_obj.insert(
                    "start_line".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(sl)),
                );
            }
            if let Some(el) = p.end_line {
                snapshot_obj.insert(
                    "end_line".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(el)),
                );
            }
            if let Some(d) = &p.doc {
                snapshot_obj.insert("doc".to_string(), serde_json::Value::String(d.clone()));
            }
            if !p.imports.is_empty() {
                snapshot_obj.insert(
                    "imports".to_string(),
                    serde_json::to_value(&p.imports).unwrap_or(serde_json::Value::Array(vec![])),
                );
            }
            if !p.unresolved_imports.is_empty() {
                snapshot_obj.insert(
                    "unresolved_imports".to_string(),
                    serde_json::to_value(&p.unresolved_imports)
                        .unwrap_or(serde_json::Value::Array(vec![])),
                );
            }
            if !p.methods.is_empty() {
                snapshot_obj.insert(
                    "methods".to_string(),
                    serde_json::to_value(&p.methods).unwrap_or(serde_json::Value::Array(vec![])),
                );
            }
            if let Some(su) = &p.source_url {
                snapshot_obj.insert(
                    "source_url".to_string(),
                    serde_json::Value::String(su.clone()),
                );
            }
            if let Some(sd) = &p.source_display {
                snapshot_obj.insert(
                    "source_display".to_string(),
                    serde_json::Value::String(sd.clone()),
                );
            }
            if !p.calls.is_empty() {
                snapshot_obj.insert(
                    "calls".to_string(),
                    serde_json::Value::Array(
                        p.calls
                            .iter()
                            .map(|s| serde_json::Value::String(s.clone()))
                            .collect(),
                    ),
                );
            }
            if let Some(rank) = p.rank {
                snapshot_obj.insert(
                    "page_rank_value".to_string(),
                    serde_json::Value::Number(serde_json::Number::from_f64(rank as f64).unwrap()),
                );
            }
            // Persist full source text (used for embeddings) on the snapshot
            if let Some(sc) = &p.source_content {
                snapshot_obj.insert(
                    "source_content".to_string(),
                    serde_json::Value::String(sc.clone()),
                );
            } else {
                snapshot_obj.insert(
                    "source_content".to_string(),
                    serde_json::Value::String(String::new()),
                );
            }

            // Embedding fields
            if let Some((vec, model, chunk_count)) =
                embeddings_for_batch.get(global_idx).and_then(|e| e.clone())
            {
                embeddings_present += 1;
                let arr: Vec<serde_json::Value> = vec
                    .iter()
                    .map(|f| serde_json::Value::from(*f as f64))
                    .collect();
                snapshot_obj.insert("embedding".to_string(), serde_json::Value::Array(arr));
                snapshot_obj.insert(
                    "embedding_len".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(vec.len() as i64)),
                );
                snapshot_obj.insert(
                    "embedding_model".to_string(),
                    serde_json::Value::String(model),
                );
                snapshot_obj.insert(
                    "embedding_dim".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(vec.len() as i64)),
                );
                // Record how many chunks contributed to this snapshot embedding
                snapshot_obj.insert(
                    "embedding_chunk_count".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(chunk_count as i64)),
                );
                snapshot_obj.insert(
                    "embedding_created_at".to_string(),
                    serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
                );
                snapshot_obj.insert(
                    "similarity_status".to_string(),
                    serde_json::Value::String("ready".to_string()),
                );
            } else {
                embeddings_missing += 1;
                debug!("initial_batch: snapshot left without embedding id={} reason=no_embedding snippet_len={}", p.stable_id, p.source_content.as_ref().map(|s| s.len()).unwrap_or(0));
                snapshot_obj.insert("embedding".to_string(), serde_json::Value::Array(vec![]));
                snapshot_obj.insert(
                    "embedding_len".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(0)),
                );
                snapshot_obj.insert(
                    "embedding_model".to_string(),
                    serde_json::Value::String(String::new()),
                );
                snapshot_obj.insert(
                    "embedding_dim".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(0)),
                );
                snapshot_obj.insert("embedding_created_at".to_string(), serde_json::Value::Null);
                snapshot_obj.insert(
                    "similarity_status".to_string(),
                    serde_json::Value::String("pending".to_string()),
                );
            }

            // No commit_id available in this initial-batch helper; set empty commit ref.
            snapshot_obj.insert(
                "sourcecontrol_commit".to_string(),
                serde_json::Value::String(String::new()),
            );

            let mut snapshot_json = serde_json::Value::Object(snapshot_obj);
            sanitize_json_strings(&mut snapshot_json);

            // Deterministic id for entity/snapshot
            let eid = sanitize_id(&p.stable_id);

            // Build SQL with optional snapshot_file relation
            let mut sql = format!(
                "BEGIN; CREATE entity:{eid} CONTENT $e; CREATE entity_snapshot:{eid} CONTENT $s; RELATE entity:{eid}->has_snapshot->entity_snapshot:{eid};",
                eid = eid
            );

            // If we have a file, create the snapshot_file relation
            if p.file.is_some() {
                let file_id = format!("{}:{}", p.repo_name, p.file.as_ref().unwrap());
                let file_id_hash = format!("{:x}", Sha256::digest(file_id.as_bytes()));
                sql.push_str(&format!(
                    " RELATE entity_snapshot:{eid}->snapshot_file->file:{file_id_hash};",
                    eid = eid,
                    file_id_hash = file_id_hash
                ));
            }

            sql.push_str(" COMMIT;");
            match db
                .query_with_binds(&sql, vec![("e", entity_json), ("s", snapshot_json)])
                .await
            {
                Ok(resp) => {
                    // Emit the raw response for diagnostics when requested by env.
                    if std::env::var("HZ_DEBUG_SQL_RESP").ok().as_deref() == Some("1") {
                        debug!(
                            "initial batch insert resp for id={} -> {:?}",
                            p.stable_id, resp
                        );
                    }
                    success_count += 1;
                }
                Err(e) => {
                    warn!(
                        "initial batch entity+snapshot insert failed id={} err={}",
                        p.stable_id, e
                    );
                    // Persist a DLQ record so operators can inspect failed snapshots
                    let head = p
                        .source_content
                        .as_deref()
                        .unwrap_or("")
                        .chars()
                        .take(256)
                        .collect::<String>();
                    append_to_embed_dlq(
                        &p.stable_id,
                        &format!("db_insert_error: {}", e),
                        p.source_content.as_ref().map(|s| s.len()).unwrap_or(0),
                        &head,
                    );
                }
            }
        }
        base_idx += ch.len();
        info!(
            "initial chunk inserted size={} succeeded={} ms={} embeddings_present={} embeddings_missing={}",
            ch.len(),
            success_count,
            st.elapsed().as_millis(),
            embeddings_present,
            embeddings_missing,
        );
    }
    Ok(())
}

static SCHEMA_INIT_ONCE: OnceLock<()> = OnceLock::new();
// Global (process-wide) deduplication of repo<->file edges so multi-batch ingestion doesn't
// emit duplicate RELATE statements that could skew directional counts if Surreal applies
// optimizations differently. Each (file_id, repo_id) pair will only be emitted once.
static FILE_REPO_EDGE_SEEN: OnceLock<Mutex<std::collections::HashSet<(String, String)>>> =
    OnceLock::new();
// Track created file ids so we can issue CREATE once then UPDATE subsequently (UPDATE alone won't create rows).
static FILE_IDS_CREATED: OnceLock<Mutex<std::collections::HashSet<String>>> = OnceLock::new();

async fn init_schema(db: &SurrealConnection, namespace: &str, database: &str) {
    if std::env::var("HZ_DISABLE_SCHEMA_CACHE").ok().as_deref() != Some("1")
        && SCHEMA_INIT_ONCE.get().is_some()
    {
        trace!("schema init skipped (cached)");
        return;
    }
    for stmt in [
        format!("DEFINE NAMESPACE {}", namespace),
        format!("DEFINE DATABASE {}", database),
        format!("USE NS {}; USE DB {}", namespace, database),
    ] {
        match db.query(&stmt).await {
            Ok(_) => debug!("schema preamble applied: {}", stmt),
            Err(e) => trace!("schema preamble failed: {} -> {}", stmt, e),
        }
    }
    let schema_groups: Vec<Vec<&str>> = vec![
        // Entity table: prefer the explicit SCHEMALESS + PERMISSIONS declaration.
        // We intentionally avoid adding duplicate plain `DEFINE TABLE entity;` or
        // `CREATE TABLE entity;` variants here to reduce ambiguity and repeated
        // creation attempts. Other groups include simple fallbacks when needed
        // for compatibility with older SurrealDB builds.
        vec![
            "DEFINE TABLE entity SCHEMALESS PERMISSIONS FULL;",
        ],
        vec![
            "DEFINE TABLE file SCHEMALESS PERMISSIONS FULL;",
            "DEFINE TABLE file;",
        ],
        // explicit repo table definition (was previously only created implicitly during upserts)
        vec![
            "DEFINE TABLE repo SCHEMALESS PERMISSIONS FULL;",
            "DEFINE TABLE repo;",
        ],
        vec![
            "DEFINE TABLE commits SCHEMALESS PERMISSIONS FULL;",
            "DEFINE TABLE commits;",
        ],
        // Entity snapshot table for commit-specific entity metadata
        vec![
            "DEFINE TABLE entity_snapshot SCHEMALESS PERMISSIONS FULL;",
            "DEFINE TABLE entity_snapshot;",
        ],
        // repo/file relation tables (graph)
        // Provide directional relation definitions first, then fall back to legacy generic declarations if the
        // SurrealDB version in use does not support the FROM/TO clause (older nightly/in-memory builds).
        vec![
            "DEFINE TABLE in_repo TYPE RELATION;",
            "DEFINE TABLE in_repo TYPE RELATION;",
        ],
        vec![
            "DEFINE TABLE has_file TYPE RELATION;",
            "DEFINE TABLE has_file TYPE RELATION;",
        ],
        // relation tables for graph edges (entity -> entity)
        vec![
            "DEFINE TABLE calls TYPE RELATION FROM entity_snapshot TO entity_snapshot;",
            "DEFINE TABLE calls TYPE RELATION;",
        ],
        vec![
            "DEFINE TABLE has_method TYPE RELATION FROM entity_snapshot TO entity_snapshot;",
            "DEFINE TABLE has_method TYPE RELATION;",
        ],
        vec![
            "DEFINE TABLE imports TYPE RELATION FROM entity_snapshot TO entity_snapshot;",
            "DEFINE TABLE imports TYPE RELATION;",
        ],
        // entity to entity_snapshot relation
        vec![
            "DEFINE TABLE has_snapshot TYPE RELATION FROM entity TO entity_snapshot;",
            "DEFINE TABLE has_snapshot TYPE RELATION;",
        ],
        // entity_snapshot to file relation
        vec![
            "DEFINE TABLE snapshot_file TYPE RELATION FROM entity_snapshot TO file;",
            "DEFINE TABLE snapshot_file TYPE RELATION;",
        ],
        // entity_snapshot to commits relation
        vec![
            "DEFINE TABLE snapshot_commit TYPE RELATION FROM entity_snapshot TO commits;",
            "DEFINE TABLE snapshot_commit TYPE RELATION;",
        ],
        // refs to commits relation
        vec![
            "DEFINE TABLE points_to TYPE RELATION FROM refs TO commits;",
            "DEFINE TABLE points_to TYPE RELATION;",
        ],
        vec![
            "DEFINE FIELD stable_id ON entity TYPE string;",
        ],
        vec![
            "DEFINE FIELD repo_name ON entity TYPE string;",
        ],
        vec![
            "DEFINE FIELD stable_id ON entity_snapshot TYPE string;",
        ],
        vec![
            "DEFINE FIELD sourcecontrol_commit ON entity_snapshot TYPE string;",
            "REMOVE FIELD sourcecontrol_commit ON entity_snapshot; DEFINE FIELD sourcecontrol_commit ON entity_snapshot TYPE string;",
        ],
        vec![
            // Track similarity processing status on snapshots (was embedding_status on content previously)
            "DEFINE FIELD similarity_status ON entity_snapshot TYPE string DEFAULT 'pending';",
            "REMOVE FIELD similarity_status ON entity_snapshot; DEFINE FIELD similarity_status ON entity_snapshot TYPE string DEFAULT 'pending';",
        ],
        vec![
            "DEFINE INDEX idx_entity_stable_id ON entity COLUMNS stable_id;",
        ],
        vec![
            "DEFINE INDEX idx_file_path ON file COLUMNS path;",
        ],
        vec![
            "DEFINE INDEX idx_calls_unique ON calls FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_imports_unique ON imports FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_has_method_unique ON has_method FIELDS in, out UNIQUE;",
        ],
        // Note: do not create an explicit depends_on relation table here; rely on SurrealDB
        // relation/traversal features and let RELATE create relation rows as needed.
        vec![
            "DEFINE INDEX idx_has_file_unique ON has_file FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_in_repo_unique ON in_repo FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_has_snapshot_unique ON has_snapshot FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_snapshot_file_unique ON snapshot_file FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_snapshot_commit_unique ON snapshot_commit FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_points_to_unique ON points_to FIELDS in, out UNIQUE;",
        ],
        // Store the full source content used to compute embeddings for inspection.
        vec![
            "DEFINE FIELD source_content ON entity TYPE string DEFAULT '';",
            "REMOVE FIELD source_content ON entity; DEFINE FIELD source_content ON entity TYPE string DEFAULT '';",
        ],
        // Similarity relations between entities with score metadata
        vec![
            "DEFINE TABLE similar_same_repo TYPE RELATION FROM entity_snapshot TO entity_snapshot;",
            "DEFINE TABLE similar_same_repo TYPE RELATION;",
        ],
        vec![
            "DEFINE INDEX idx_similar_same_repo_unique ON similar_same_repo FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE FIELD score ON similar_same_repo TYPE number DEFAULT 0;",
            "REMOVE FIELD score ON similar_same_repo; DEFINE FIELD score ON similar_same_repo TYPE number DEFAULT 0;",
        ],
        vec![
            "DEFINE TABLE similar_external_repo TYPE RELATION FROM entity_snapshot TO entity_snapshot;",
            "DEFINE TABLE similar_external_repo TYPE RELATION;",
        ],
        vec![
            "DEFINE INDEX idx_similar_external_repo_unique ON similar_external_repo FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE FIELD score ON similar_external_repo TYPE number DEFAULT 0;",
            "REMOVE FIELD score ON similar_external_repo; DEFINE FIELD score ON similar_external_repo TYPE number DEFAULT 0;",
        ],
    ];
    for group in schema_groups.iter() {
        let mut applied = false;
        for q in group.iter() {
            match db.query(q).await {
                Ok(_) => {
                    debug!("schema applied: {}", q);
                    applied = true;
                    break;
                }
                Err(e) => trace!("schema variant failed: {} -> {}", q, e),
            }
        }
        if !applied {
            warn!("schema group failed: {:?}", group);
        }
    }
    // Attempt to upgrade relation tables in case only generic CREATE TABLE succeeded earlier.
    // Surrealist UI shows relationships only when tables are typed relations.
    // First remove any existing FROM/TO constraints by dropping and recreating tables
    for reset_table in ["in_repo", "has_file"] {
        let drop_sql = format!("REMOVE TABLE {};", reset_table);
        if let Err(e) = db.query(&drop_sql).await {
            trace!(
                "table drop attempt failed (non-fatal): {} -> {}",
                drop_sql,
                e
            );
        } else {
            debug!("table dropped: {}", reset_table);
            // Recreate as generic relation
            let create_sql = format!("DEFINE TABLE {} TYPE RELATION;", reset_table);
            if let Err(e) = db.query(&create_sql).await {
                trace!(
                    "table recreate attempt failed (non-fatal): {} -> {}",
                    create_sql,
                    e
                );
            } else {
                debug!("table recreated: {}", reset_table);
            }
        }
    }
    let _ = SCHEMA_INIT_ONCE.set(());
    let _ = SCHEMA_INIT_ONCE.set(());
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

fn build_file_record_id(repo_name: &str, commit_id: Option<&str>, path: &str) -> String {
    let repo_component = if repo_name.trim().is_empty() {
        "repo".to_string()
    } else {
        sanitize_id(repo_name)
    };
    let commit_component = commit_id
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(sanitize_id)
        .unwrap_or_else(|| "no_commit".to_string());
    let path_component = sanitize_id(path);
    format!("{}_{}_{}", repo_component, commit_component, path_component)
}

type BatchStatements = Vec<(String, Option<Vec<(&'static str, serde_json::Value)>>)>;
#[allow(clippy::type_complexity)]
/// Optional embedding shape per payload: None if not computed, otherwise
/// (Vec<f32>, model_id, chunk_count).
type MaybeEmbedding = Option<(Vec<f32>, String, usize)>;

fn build_batch_sql(
    acc: &[EntityPayload],
    cfg: &DbWriterConfig,
    embeddings: &[MaybeEmbedding],
) -> (BatchStatements, usize) {
    // Return pre-separated statements as tuples of (sql, optional binds).
    // Using binds for JSON payloads prevents manual JSON embedding while
    // allowing deterministic ids to remain interpolated (they're sanitized).
    type StmtBinds = Vec<(&'static str, serde_json::Value)>;
    type SqlStmt = (String, Option<StmtBinds>);
    let mut statements: Vec<SqlStmt> = Vec::new();

    if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
        println!(
            "BUILD_BATCH_SQL called with {} payloads, commit_id={:?}",
            acc.len(),
            cfg.commit_id
        );
    }
    use std::collections::HashMap;
    // de-duplicate files by repo + repo-relative path (store relative paths in DB)
    let mut file_map: HashMap<(String, String), usize> = HashMap::new();
    let mut file_list: Vec<(String, String, String)> = Vec::new();
    // Helper: derive a file-like hint from the payload. Prefer a precomputed
    // `source_display` if present. We intentionally avoid complex URL parsing
    // here; if no display hint exists, skip file metadata for that entity.
    fn payload_file_hint(p: &EntityPayload) -> Option<String> {
        // Prefer explicit display hint when available
        if let Some(sd) = &p.source_display {
            if !sd.is_empty() {
                return Some(sd.clone());
            }
        }
        // Fallback: try to derive a reasonable repo-relative path from source_url.
        // Common patterns produced by indexers include URLs containing '/blob/<branch>/<path>' or
        // file://<abs-path> fragments. Try a few simple heuristics rather than pulling
        // in a heavy URL parsing dependency.
        if let Some(su) = &p.source_url {
            if su.is_empty() {
                return None;
            }
            // Pattern: .../blob/<branch>/<path> -> drop the branch segment
            if let Some(idx) = su.find("/blob/") {
                let after = &su[idx + "/blob/".len()..];
                let mut parts: Vec<&str> = after.split('/').filter(|s| !s.is_empty()).collect();
                if parts.len() > 1 {
                    // drop branch and return the remaining path
                    parts.remove(0);
                    return Some(parts.join("/"));
                } else if parts.len() == 1 {
                    return Some(parts[0].to_string());
                }
            }
            // Pattern: file://<abs-path> -> return the filesystem path without leading '/'
            if let Some(idx) = su.find("file://") {
                let after = &su[idx + "file://".len()..];
                return Some(after.trim_start_matches('/').to_string());
            }
            // Generic URL: remove scheme and optional host, return the path portion
            if let Some(idx) = su.find("://") {
                let after = &su[idx + 3..];
                if let Some(slash) = after.find('/') {
                    let path = &after[slash + 1..];
                    return Some(path.to_string());
                } else {
                    return Some(after.to_string());
                }
            }
            // As a last resort, return the raw source_url so compute_repo_relative
            // may still find something useful.
            return Some(su.clone());
        }
        None
    }

    for p in acc.iter() {
        if let Some(fp) = payload_file_hint(p) {
            let repo_name = p.repo_name.clone();
            let repo_rel = compute_repo_relative(&fp, &repo_name);
            let key = (repo_name.clone(), repo_rel.clone());
            if let std::collections::hash_map::Entry::Vacant(e) = file_map.entry(key) {
                let idx = file_list.len();
                e.insert(idx);
                file_list.push((repo_name, repo_rel, p.language.clone()));
            }
        }
    }
    if !file_list.is_empty() {
        let created_set =
            FILE_IDS_CREATED.get_or_init(|| Mutex::new(std::collections::HashSet::new()));
        if std::env::var("HZ_RESET_GLOBALS").ok().as_deref() == Some("1") {
            created_set.lock().unwrap().clear();
        }
        let mut guard = created_set.lock().unwrap();
        for (repo_name, path, lang) in file_list.iter() {
            let fid = build_file_record_id(repo_name, cfg.commit_id.as_deref(), path);
            if guard.insert(fid.clone()) {
                // First time we've seen this file in-process: create the row with binds
                let mut map = serde_json::Map::new();
                map.insert("path".to_string(), serde_json::Value::String(path.clone()));
                map.insert(
                    "language".to_string(),
                    serde_json::Value::String(lang.clone()),
                );
                map.insert(
                    "repo_name".to_string(),
                    serde_json::Value::String(repo_name.clone()),
                );
                match cfg.commit_id.as_deref() {
                    Some(commit) if !commit.is_empty() => {
                        map.insert(
                            "commit_id".to_string(),
                            serde_json::Value::String(commit.to_string()),
                        );
                        // Add record reference to commits table
                        map.insert(
                            "commit_ref".to_string(),
                            serde_json::Value::String(format!("commits:{}", commit)),
                        );
                    }
                    _ => {
                        map.insert("commit_id".to_string(), serde_json::Value::Null);
                        map.insert("commit_ref".to_string(), serde_json::Value::Null);
                    }
                }
                let sql = format!("CREATE file:{fid} CONTENT $f;", fid = fid);
                statements.push((sql, Some(vec![("f", serde_json::Value::Object(map))])));
            } else {
                // Subsequent occurrences: update the existing row.
                let mut binds = vec![
                    ("p", serde_json::Value::String(path.clone())),
                    ("l", serde_json::Value::String(lang.clone())),
                    ("r", serde_json::Value::String(repo_name.clone())),
                ];
                let mut set_fragments = vec!["path = $p", "language = $l", "repo_name = $r"];
                match cfg.commit_id.as_deref() {
                    Some(commit) if !commit.is_empty() => {
                        binds.push(("c", serde_json::Value::String(commit.to_string())));
                        binds.push((
                            "cr",
                            serde_json::Value::String(format!("commits:{}", commit)),
                        ));
                        set_fragments.push("commit_id = $c");
                        set_fragments.push("commit_ref = $cr");
                    }
                    _ => {
                        binds.push(("c", serde_json::Value::Null));
                        binds.push(("cr", serde_json::Value::Null));
                        set_fragments.push("commit_id = $c");
                        set_fragments.push("commit_ref = $cr");
                    }
                }
                let sql = format!(
                    "UPDATE file:{fid} SET {};",
                    set_fragments.join(", "),
                    fid = fid
                );
                statements.push((sql, Some(binds)));
            }
        }
        // guard dropped here
    }
    // map name->stable_id for calls resolution within batch
    let mut name_to_stable: HashMap<&str, &str> = HashMap::new();
    for p in acc {
        name_to_stable.insert(p.name.as_str(), p.stable_id.as_str());
    }
    // entities (CREATE-once, then UPDATE for subsequent batches)
    static ENTITY_IDS_CREATED: std::sync::OnceLock<Mutex<std::collections::HashSet<String>>> =
        std::sync::OnceLock::new();
    let created_entities =
        ENTITY_IDS_CREATED.get_or_init(|| Mutex::new(std::collections::HashSet::new()));
    if std::env::var("HZ_RESET_GLOBALS").ok().as_deref() == Some("1") {
        created_entities.lock().unwrap().clear();
    }
    {
        let _guard = created_entities.lock().unwrap();
        for (idx, p) in acc.iter().enumerate() {
            // Create entity with embedded snapshot data
            let mut entity_obj = serde_json::Map::new();
            entity_obj.insert(
                "language".to_string(),
                serde_json::Value::String(p.language.clone()),
            );
            entity_obj.insert(
                "kind".to_string(),
                serde_json::Value::String(p.kind.clone()),
            );
            entity_obj.insert(
                "name".to_string(),
                serde_json::Value::String(p.name.clone()),
            );
            // NOTE: rank is now stored on the entity_snapshot record (snapshot-level)
            // to allow PageRank to be associated with a specific snapshot/commit.
            // Keep rank out of the top-level entity JSON to avoid duplication.
            entity_obj.insert(
                "repo_name".to_string(),
                serde_json::Value::String(p.repo_name.clone()),
            );
            entity_obj.insert(
                "signature".to_string(),
                serde_json::Value::String(p.signature.clone()),
            );
            entity_obj.insert(
                "stable_id".to_string(),
                serde_json::Value::String(p.stable_id.clone()),
            );
            if let Some(sc) = &p.source_content {
                entity_obj.insert(
                    "source_content".to_string(),
                    serde_json::Value::String(sc.clone()),
                );
            }

            // Embed snapshot data directly in the entity
            let mut snapshot_obj = serde_json::Map::new();
            if let Some(f) = &p.file {
                snapshot_obj.insert("file".to_string(), serde_json::Value::String(f.clone()));
            }
            if let Some(p) = &p.parent {
                snapshot_obj.insert("parent".to_string(), serde_json::Value::String(p.clone()));
            }
            if let Some(sl) = p.start_line {
                snapshot_obj.insert(
                    "start_line".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(sl)),
                );
            }
            if let Some(el) = p.end_line {
                snapshot_obj.insert(
                    "end_line".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(el)),
                );
            }
            if let Some(d) = &p.doc {
                snapshot_obj.insert("doc".to_string(), serde_json::Value::String(d.clone()));
            }
            if !p.imports.is_empty() {
                snapshot_obj.insert(
                    "imports".to_string(),
                    serde_json::to_value(&p.imports).unwrap_or(serde_json::Value::Array(vec![])),
                );
            }
            if !p.unresolved_imports.is_empty() {
                snapshot_obj.insert(
                    "unresolved_imports".to_string(),
                    serde_json::to_value(&p.unresolved_imports)
                        .unwrap_or(serde_json::Value::Array(vec![])),
                );
            }
            if !p.methods.is_empty() {
                snapshot_obj.insert(
                    "methods".to_string(),
                    serde_json::to_value(&p.methods).unwrap_or(serde_json::Value::Array(vec![])),
                );
            }
            if let Some(su) = &p.source_url {
                snapshot_obj.insert(
                    "source_url".to_string(),
                    serde_json::Value::String(su.clone()),
                );
            }
            if let Some(sd) = &p.source_display {
                snapshot_obj.insert(
                    "source_display".to_string(),
                    serde_json::Value::String(sd.clone()),
                );
            }
            if !p.calls.is_empty() {
                snapshot_obj.insert(
                    "calls".to_string(),
                    serde_json::Value::Array(
                        p.calls
                            .iter()
                            .map(|s| serde_json::Value::String(s.clone()))
                            .collect(),
                    ),
                );
            }
            // Attach per-snapshot PageRank value when provided by the indexer.
            // Persist under `page_rank_value` to avoid conflict with SQL keywords
            // and to make it explicit this is a snapshot-scoped metric.
            if let Some(rank) = p.rank {
                snapshot_obj.insert(
                    "page_rank_value".to_string(),
                    serde_json::Value::Number(serde_json::Number::from_f64(rank as f64).unwrap()),
                );
            }
            entity_obj.insert(
                "snapshot".to_string(),
                serde_json::Value::Object(snapshot_obj),
            );

            // We no longer deduplicate text into a separate `content` table.
            // Persist source_content on the snapshot and keep entity simple.

            let eid = sanitize_id(&p.stable_id);
            let mut entity_json = serde_json::Value::Object(entity_obj);
            sanitize_json_strings(&mut entity_json);
            let sql = format!("CREATE entity:{eid} CONTENT $e;", eid = eid);
            statements.push((sql, Some(vec![("e", entity_json)])));

            // Create entity_snapshot with the same ID for relations
            let mut snapshot_obj = serde_json::Map::new();
            // Include repo_name so snapshot-scoped queries can filter by repo
            snapshot_obj.insert(
                "repo_name".to_string(),
                serde_json::Value::String(p.repo_name.clone()),
            );
            snapshot_obj.insert(
                "stable_id".to_string(),
                serde_json::Value::String(p.stable_id.clone()),
            );
            snapshot_obj.insert(
                "name".to_string(),
                serde_json::Value::String(p.name.clone()),
            );
            if let Some(f) = &p.file {
                snapshot_obj.insert("file".to_string(), serde_json::Value::String(f.clone()));
            }
            if let Some(p) = &p.parent {
                snapshot_obj.insert("parent".to_string(), serde_json::Value::String(p.clone()));
            }
            if let Some(sl) = p.start_line {
                snapshot_obj.insert(
                    "start_line".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(sl)),
                );
            }
            if let Some(el) = p.end_line {
                snapshot_obj.insert(
                    "end_line".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(el)),
                );
            }
            if let Some(d) = &p.doc {
                snapshot_obj.insert("doc".to_string(), serde_json::Value::String(d.clone()));
            }
            if !p.imports.is_empty() {
                snapshot_obj.insert(
                    "imports".to_string(),
                    serde_json::to_value(&p.imports).unwrap_or(serde_json::Value::Array(vec![])),
                );
            }
            if !p.unresolved_imports.is_empty() {
                snapshot_obj.insert(
                    "unresolved_imports".to_string(),
                    serde_json::to_value(&p.unresolved_imports)
                        .unwrap_or(serde_json::Value::Array(vec![])),
                );
            }
            if !p.methods.is_empty() {
                snapshot_obj.insert(
                    "methods".to_string(),
                    serde_json::to_value(&p.methods).unwrap_or(serde_json::Value::Array(vec![])),
                );
            }
            if let Some(su) = &p.source_url {
                snapshot_obj.insert(
                    "source_url".to_string(),
                    serde_json::Value::String(su.clone()),
                );
            }
            if let Some(sd) = &p.source_display {
                snapshot_obj.insert(
                    "source_display".to_string(),
                    serde_json::Value::String(sd.clone()),
                );
            }
            if !p.calls.is_empty() {
                snapshot_obj.insert(
                    "calls".to_string(),
                    serde_json::Value::Array(
                        p.calls
                            .iter()
                            .map(|s| serde_json::Value::String(s.clone()))
                            .collect(),
                    ),
                );
            }
            // Persist full source text (used for embeddings) on the snapshot
            if let Some(sc) = &p.source_content {
                snapshot_obj.insert(
                    "source_content".to_string(),
                    serde_json::Value::String(sc.clone()),
                );
            } else {
                snapshot_obj.insert(
                    "source_content".to_string(),
                    serde_json::Value::String(String::new()),
                );
            }
            // If an embedding was computed for this payload, persist it directly
            // on the snapshot to avoid a separate upsert step. Otherwise set
            // default placeholder fields.
            if let Some((vec, model, chunk_count)) = embeddings.get(idx).and_then(|e| e.clone()) {
                // store embedding array
                let arr: Vec<serde_json::Value> = vec
                    .iter()
                    .map(|f| serde_json::Value::from(*f as f64))
                    .collect();
                snapshot_obj.insert("embedding".to_string(), serde_json::Value::Array(arr));
                snapshot_obj.insert(
                    "embedding_len".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(vec.len() as i64)),
                );
                snapshot_obj.insert(
                    "embedding_model".to_string(),
                    serde_json::Value::String(model),
                );
                snapshot_obj.insert(
                    "embedding_dim".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(vec.len() as i64)),
                );
                snapshot_obj.insert(
                    "embedding_chunk_count".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(chunk_count as i64)),
                );
                snapshot_obj.insert(
                    "embedding_created_at".to_string(),
                    serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
                );
                snapshot_obj.insert(
                    "similarity_status".to_string(),
                    serde_json::Value::String("ready".to_string()),
                );
            } else {
                // Prepare embedding metadata fields on the snapshot (defaults)
                snapshot_obj.insert("embedding".to_string(), serde_json::Value::Array(vec![]));
                snapshot_obj.insert(
                    "embedding_len".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(0)),
                );
                snapshot_obj.insert(
                    "embedding_model".to_string(),
                    serde_json::Value::String(String::new()),
                );
                snapshot_obj.insert(
                    "embedding_dim".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(0)),
                );
                snapshot_obj.insert("embedding_created_at".to_string(), serde_json::Value::Null);
                snapshot_obj.insert(
                    "similarity_status".to_string(),
                    serde_json::Value::String("pending".to_string()),
                );
            }
            // Add embedded commit reference if available
            if let Some(commit_id) = &cfg.commit_id {
                let commit_ref = format!("commits:{}", commit_id);
                // Keep the original field for backward compatibility
                snapshot_obj.insert(
                    "sourcecontrol_commit".to_string(),
                    serde_json::Value::String(commit_ref.clone()),
                );
                // Add explicit record reference field for graph queries
                snapshot_obj.insert(
                    "sourcecontrol_commit_ref".to_string(),
                    serde_json::Value::String(commit_ref),
                );
            } else {
                // Set to empty string when no commit_id is available
                snapshot_obj.insert(
                    "sourcecontrol_commit".to_string(),
                    serde_json::Value::String(String::new()),
                );
                snapshot_obj.insert(
                    "sourcecontrol_commit_ref".to_string(),
                    serde_json::Value::Null,
                );
            }
            let mut snapshot_json = serde_json::Value::Object(snapshot_obj);
            sanitize_json_strings(&mut snapshot_json);
            let snapshot_sql = format!("CREATE entity_snapshot:{eid} CONTENT $s;", eid = eid);
            statements.push((snapshot_sql, Some(vec![("s", snapshot_json)])));

            // Link the primary entity to its snapshot for graph traversals
            let snapshot_rel = format!(
                "RELATE entity:{eid}->has_snapshot->entity_snapshot:{eid};",
                eid = eid
            );
            statements.push((snapshot_rel, None));

            // Link entity_snapshot to file if available
            if p.file.is_some() {
                let file_id = format!("{}:{}", cfg.repo_name, p.file.as_ref().unwrap());
                let file_id_hash = format!("{:x}", Sha256::digest(file_id.as_bytes()));
                let file_rel = format!(
                    "RELATE entity_snapshot:{eid}->snapshot_file->file:{file_id_hash};",
                    eid = eid,
                    file_id_hash = file_id_hash
                );
                statements.push((file_rel, None));
            }

            // Link entity_snapshot to commit if available
            if let Some(commit_id) = &cfg.commit_id {
                let commit_rel = format!(
                    "RELATE entity_snapshot:{eid}->snapshot_commit->commits:{commit_id};",
                    eid = eid,
                    commit_id = commit_id
                );
                statements.push((commit_rel, None));
            }

            // We no longer create separate content records; embeddings and text live on entity_snapshot.

            // Create entity_snapshot if we have a commit_id (linking entity to commit)
            if let Some(_commit_id) = &cfg.commit_id {
                // For now, skip creating entity_snapshot in batch SQL
                // This needs to be done after the batch completes
                // TODO: Create entity_snapshot with additional fields
            }

            // Emit any method items as their own entity rows and relate them to the parent
            for mi in p.methods.iter() {
                // Create a deterministic id for the method entity based on parent stable id
                // and the method signature: hash(stable_id + '|' + signature) to produce
                // a stable, collision-resistant identifier.
                let mut hasher = Sha256::new();
                hasher.update(p.stable_id.as_bytes());
                hasher.update(b"|");
                hasher.update(mi.signature.as_bytes());
                let digest = hasher.finalize();
                let method_hash = format!("{:x}", digest);
                let mid = sanitize_id(&format!("m_{}", method_hash));
                // Minimal method entity JSON: include name, signature, visibility, and parent link via parent_name
                let mut m_obj = serde_json::Map::new();
                m_obj.insert(
                    "language".to_string(),
                    serde_json::Value::String(p.language.clone()),
                );
                m_obj.insert(
                    "kind".to_string(),
                    serde_json::Value::String("function".to_string()),
                );
                m_obj.insert(
                    "name".to_string(),
                    serde_json::Value::String(mi.name.clone()),
                );
                m_obj.insert(
                    "signature".to_string(),
                    serde_json::Value::String(mi.signature.clone()),
                );
                if let Some(vv) = &mi.visibility {
                    m_obj.insert(
                        "visibility".to_string(),
                        serde_json::Value::String(vv.clone()),
                    );
                }
                m_obj.insert(
                    "start_line".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(mi.start_line.unwrap_or(0))),
                );
                // Include a deterministic stable_id for the method entity so it can be
                // referenced and queried directly. Use the same hash we built for the
                // method id (method_hash) but keep the stable form without sanitization.
                m_obj.insert(
                    "stable_id".to_string(),
                    serde_json::Value::String(method_hash.clone()),
                );
                m_obj.insert(
                    "repo_name".to_string(),
                    serde_json::Value::String(p.repo_name.clone()),
                );
                // Add embedded commit reference if available
                if let Some(commit_id) = &cfg.commit_id {
                    m_obj.insert(
                        "sourcecontrol_commit".to_string(),
                        serde_json::Value::String(format!("commits:{}", commit_id)),
                    );
                }
                let m_val = serde_json::Value::Object(m_obj.clone());
                let mut sanitized_m_val = m_val.clone();
                sanitize_json_strings(&mut sanitized_m_val);
                let sql = format!("CREATE entity:{mid} CONTENT $m;", mid = mid);
                statements.push((sql, Some(vec![("m", sanitized_m_val.clone())])));
                // Create entity_snapshot for method
                let method_snapshot_sql =
                    format!("CREATE entity_snapshot:{mid} CONTENT $ms;", mid = mid);
                statements.push((
                    method_snapshot_sql,
                    Some(vec![("ms", sanitized_m_val.clone())]),
                ));

                // Link method entity to its snapshot for downstream queries
                let method_snapshot_rel = format!(
                    "RELATE entity:{mid}->has_snapshot->entity_snapshot:{mid};",
                    mid = mid
                );
                statements.push((method_snapshot_rel, None));
                // RELATE parent entity -> has_method -> method entity
                let rel = format!(
                    "RELATE entity_snapshot:{eid}->has_method->entity_snapshot:{mid};",
                    eid = eid,
                    mid = mid
                );
                statements.push((rel, None));
            }
        }
    }
    // repo/file placeholder creation & edges
    use std::collections::HashSet;
    let mut repo_names: HashSet<&str> = HashSet::new();
    for p in acc {
        if !p.repo_name.is_empty() {
            repo_names.insert(p.repo_name.as_str());
        }
    }
    if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
        println!("DEBUG: collected repo_names: {:?}", repo_names);
    }
    if !repo_names.is_empty() {
        // Maintain a global set of created repo ids (lazy static) similar to file creation logic.
        static REPO_IDS_CREATED: std::sync::OnceLock<Mutex<std::collections::HashSet<String>>> =
            std::sync::OnceLock::new();
        let created_repos =
            REPO_IDS_CREATED.get_or_init(|| Mutex::new(std::collections::HashSet::new()));
        if std::env::var("HZ_RESET_GLOBALS").ok().as_deref() == Some("1") {
            created_repos.lock().unwrap().clear();
        }
        let mut repo_guard = created_repos.lock().unwrap();
        for rn in repo_names.iter() {
            let rid = sanitize_id(rn);
            // Prefer an authoritative git_url provided via the writer config
            // (populated by the event consumer). Do not fabricate an https URL
            // from the repo name. If no git_url was provided, leave empty.
            let repo_url = cfg.repo_git_url.clone().unwrap_or_default();
            if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                println!("DEBUG: processing repo {} -> id {}", rn, rid);
            }
            if repo_guard.insert(rid.clone()) {
                if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                    println!("DEBUG: creating repo {}", rid);
                }
                // Create deterministic repo ids so writer creates the repo rows when missing.
                let mut map = serde_json::Map::new();
                map.insert(
                    "name".to_string(),
                    serde_json::Value::String(rn.to_string()),
                );
                map.insert(
                    "git_url".to_string(),
                    serde_json::Value::String(repo_url.clone()),
                );
                map.insert(
                    "visibility".to_string(),
                    serde_json::Value::String("public".to_string()),
                );
                map.insert(
                    "allowed_users".to_string(),
                    serde_json::Value::Array(vec![]),
                );
                let sql = format!("CREATE repo:{rid} CONTENT $r;", rid = rid);
                statements.push((sql, Some(vec![("r", serde_json::Value::Object(map))])));
            } else {
                if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                    println!("DEBUG: updating repo {}", rid);
                }
                // Fallback to UPDATE if we've already recorded creation in this process.
                let sql = format!(
                    "UPDATE repo:{rid} SET name=$n, git_url=$g, visibility='public', allowed_users=[];",
                    rid = rid
                );
                statements.push((
                    sql,
                    Some(vec![
                        ("n", serde_json::Value::String(rn.to_string())),
                        ("g", serde_json::Value::String(repo_url.clone())),
                    ]),
                ));
            }
        }
        // repo_guard dropped here
        let global =
            FILE_REPO_EDGE_SEEN.get_or_init(|| Mutex::new(std::collections::HashSet::new()));
        if std::env::var("HZ_RESET_GLOBALS").ok().as_deref() == Some("1") {
            global.lock().unwrap().clear();
        }
        let mut guard = global.lock().unwrap();
        for (repo_name, path, _lang) in file_list.iter() {
            let fid = build_file_record_id(repo_name, cfg.commit_id.as_deref(), path);
            // match entities by repo-relative path equivalence
            for ent in acc.iter().filter(|e| {
                if let Some(fp) = payload_file_hint(e) {
                    compute_repo_relative(&fp, &e.repo_name) == *path
                } else {
                    false
                }
            }) {
                if !ent.repo_name.is_empty() {
                    if let Some(commit_id) = &cfg.commit_id {
                        log::info!(
                            "DB writer: creating commit-based relations for commit_id={}",
                            commit_id
                        );
                        // Link to commit instead of repo
                        if guard.insert((fid.clone(), commit_id.clone())) {
                            let r1 = format!(
                                "RELATE commits:{commit}->in_repo->repo:{rid};",
                                commit = commit_id,
                                rid = sanitize_id(ent.repo_name.as_str())
                            );
                            let r2 = format!(
                                "RELATE commits:{commit}->has_file->file:{fid};",
                                commit = commit_id,
                                fid = fid
                            );
                            statements.push((r1, None));
                            statements.push((r2, None));
                        }
                    } else {
                        log::info!("DB writer: no commit_id set, falling back to repo links");
                        // Fallback to repo links if no commit_id
                        let rid = sanitize_id(ent.repo_name.as_str());
                        if guard.insert((fid.clone(), rid.clone())) {
                            let r1 = format!(
                                "RELATE file:{fid}->in_repo->repo:{rid};",
                                fid = fid,
                                rid = rid
                            );
                            let r2 = format!(
                                "RELATE repo:{rid}->has_file->file:{fid};",
                                rid = rid,
                                fid = fid
                            );
                            statements.push((r1, None));
                            statements.push((r2, None));
                        }
                    }
                }
            }
        }
        drop(guard);
    }
    // call edges (dedup) referencing deterministic ids directly (entity:{sanitize(stable_id)})
    use std::collections::HashSet as _HashSet;
    let mut call_pairs: _HashSet<(String, String)> = _HashSet::new();
    for p in acc {
        if p.calls.is_empty() {
            continue;
        }
        let src_eid = sanitize_id(p.stable_id.as_str());
        for callee_name in &p.calls {
            if let Some(dst_sid) = name_to_stable.get(callee_name.as_str()) {
                let dst_eid = sanitize_id(dst_sid);
                if call_pairs.insert((src_eid.clone(), dst_eid.clone())) {
                    if std::env::var("HZ_CALL_EDGE_DEBUG").ok().as_deref() == Some("1") {
                        eprintln!(
                            "DEBUG adding call edge src={} dst={} (name={})",
                            src_eid, dst_eid, callee_name
                        );
                    }
                    statements.push((
                        format!(
                            "RELATE entity_snapshot:{src}->calls->entity_snapshot:{dst};",
                            src = src_eid,
                            dst = dst_eid
                        ),
                        None,
                    ));
                }
            }
        }
    }
    (statements, acc.len())
}

/// Recursively sanitize all string values in-place by replacing NUL bytes with spaces.
pub fn sanitize_json_strings(val: &mut serde_json::Value) {
    match val {
        serde_json::Value::String(s) => {
            if s.contains('\u{0000}') {
                let cleaned: String = s
                    .chars()
                    .map(|c| if c == '\u{0000}' { ' ' } else { c })
                    .collect();
                *s = cleaned;
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                sanitize_json_strings(v);
            }
        }
        serde_json::Value::Object(map) => {
            for (_k, v) in map.iter_mut() {
                sanitize_json_strings(v);
            }
        }
        _ => {}
    }
}

/// Append a minimal DLQ record for failed embedding persistence/requests.
pub fn append_to_embed_dlq(stable_id: &str, reason: &str, snippet_len: usize, snippet_head: &str) {
    let path =
        std::env::var("HZ_EMBED_DLQ_PATH").unwrap_or_else(|_| "/tmp/hz_embed_dlq.log".to_string());
    let record = format!(
        "{} | ts={} | reason={} | snippet_len={} | head={}\n",
        stable_id,
        chrono::Utc::now().to_rfc3339(),
        reason,
        snippet_len,
        snippet_head.replace('\n', "\\n")
    );
    if let Err(e) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut f| f.write_all(record.as_bytes()))
    {
        warn!("failed to write embedding DLQ record to {}: {}", path, e);
    } else {
        warn!(
            "wrote embed DLQ record for {} reason={} path={}",
            stable_id, reason, path
        );
    }
}

fn normalize_string_for_json(s: &str) -> String {
    // Ensure we produce a valid UTF-8 string and remove embedded NULs
    let lossy = String::from_utf8_lossy(s.as_bytes()).to_string();
    if lossy.contains('\u{0000}') {
        return lossy
            .chars()
            .map(|c| if c == '\u{0000}' { ' ' } else { c })
            .collect();
    }
    lossy
}

/// Compute a repository-relative path from a file path and repo name.
/// This is a lightweight version of the webui helper to avoid importing UI code.
fn compute_repo_relative(file_path: &str, repo_name: &str) -> String {
    // Normalize: remove leading / and split
    let mut parts: Vec<&str> = file_path.split('/').filter(|p| !p.is_empty()).collect();

    // Remove common clone roots like tmp/hyperzoekt-clones/<uuid>
    let mut i = 0usize;
    while i + 2 < parts.len() {
        if parts[i] == "tmp" && parts[i + 1] == "hyperzoekt-clones" {
            parts.drain(i..=i + 2);
            continue;
        }
        i += 1;
    }
    // Also remove standalone hyperzoekt-clones
    parts.retain(|p| *p != "hyperzoekt-clones");

    // Strip trailing uuid suffix from the leading segment if present
    if !parts.is_empty() {
        if let Some(first) = parts.first() {
            let leading = *first;
            if let Some(last_dash) = leading.rfind('-') {
                let potential_uuid = &leading[last_dash + 1..];
                let cleaned: String = potential_uuid.chars().filter(|c| *c != '-').collect();
                if cleaned.len() >= 32 && cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
                    // replace leading with portion before uuid
                    let repo_root = &leading[..last_dash];
                    parts[0] = repo_root;
                }
            }
        }
    }

    // Try to locate the repo_name segment
    let clean_repo = repo_name;
    for (idx, seg) in parts.iter().enumerate() {
        if *seg == clean_repo || seg.starts_with(&format!("{}-", clean_repo)) {
            if idx + 1 < parts.len() {
                return parts[idx + 1..].join("/");
            } else {
                return String::new();
            }
        }
    }

    // Strip common leading segments (uuid-like or tmp) until reasonable
    let mut start = 0usize;
    while start < parts.len() {
        let p = parts[start];
        if p == "tmp" || p == "hyperzoekt-clones" {
            start += 1;
            continue;
        }
        if p.len() >= 8 && p.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
            start += 1;
            continue;
        }
        break;
    }
    if start < parts.len() {
        return parts[start..].join("/");
    }

    // Fallback: drop first segment if multiple
    if parts.len() > 1 {
        return parts[1..].join("/");
    }

    file_path.to_string()
}

fn group_statements(stmts: &[String], group_size: usize) -> Vec<String> {
    if group_size <= 1 {
        return stmts.to_vec();
    }
    let mut out = Vec::new();
    let mut current = String::new();
    let mut count = 0usize;
    for s in stmts {
        if count == 0 {
            current.push_str("BEGIN; ");
        }
        current.push_str(s);
        if !s.ends_with(';') {
            current.push(';');
        }
        current.push(' ');
        count += 1;
        if count >= group_size {
            current.push_str("COMMIT; ");
            out.push(current);
            current = String::new();
            count = 0;
        }
    }
    if count > 0 {
        current.push_str("COMMIT; ");
        out.push(current);
    }
    out
}

/// Compute embeddings for a slice of EntityPayloads using the TEI endpoint.
/// Returns a vector of length `payloads.len()` where each entry is None on
/// failure or Some((vec_f32, model_id, chunk_count)) on success.
pub async fn compute_embeddings_for_payloads(
    client: &HttpClient,
    tei_endpoint: &str,
    model: &str,
    payloads: &[EntityPayload],
) -> Vec<MaybeEmbedding> {
    // Keep a reference to the original payload slice so we don't accidentally
    // shadow it later when working with chunked payloads.
    let orig_payloads = payloads;

    // We'll build the actual inputs for TEI from the chunked payloads below so
    // indices align with `chunked_payloads`. We'll populate `inputs_owned` and
    // `inputs` after performing token-aware chunking (avoid placeholder
    // allocations that become overwritten and trigger unused-assignment warnings).

    // Allow configuring per-request sub-batch size to avoid TEI rejecting large
    // requests (413). Default to 100 inputs per request which is a reasonable
    // hard cap; operators can tune HZ_EMBED_BATCH_SIZE in env.
    let batch_size: usize = std::env::var("HZ_EMBED_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &usize| *v > 0)
        .unwrap_or(100usize);

    // Allow configuring a maximum serialized payload size per request. TEI
    // deployments often enforce a request body size limit which manifests as
    // HTTP 413. Default to 80KB but operators can tune HZ_EMBED_MAX_BYTES.
    let max_bytes: usize = std::env::var("HZ_EMBED_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &usize| *v > 0)
        .unwrap_or(80_000usize);

    // Token-aware handling configuration (moved early so chunking logic can use it)
    let token_limit: usize = std::env::var("HZ_EMBED_TOKEN_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &usize| *v > 0)
        .unwrap_or(8192usize);
    let truncate_dir =
        std::env::var("HZ_EMBED_TRUNCATE_DIR").unwrap_or_else(|_| "Right".to_string());

    // Initialize tokenizer for token counting.
    let tk = match cl100k_base() {
        Ok(enc) => Some(enc),
        Err(e) => {
            warn!("failed to initialize tokenizer (tiktoken-rs): {}", e);
            None
        }
    };

    // Build chunked payloads when possible and pre-allocate results
    let orig_len = orig_payloads.len();
    let chunk_overlap: usize = std::env::var("HZ_EMBED_CHUNK_OVERLAP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256usize);
    let (send_payloads, chunk_to_orig) =
        chunk_payloads_for_embeddings(orig_payloads, token_limit, chunk_overlap, tk.clone());
    // Chunked view of payloads for network requests.
    let chunked_payloads = &send_payloads[..];
    // Build inputs for TEI from the chunked payloads so the serialized
    // JSON matches the chunk indices and to ensure each payload is properly
    // normalized for JSON. This avoids using the original (unchunked)
    // inputs which would misalign indices and cause DLQ/mapping issues.
    let mut inputs_owned: Vec<String> = chunked_payloads
        .iter()
        .map(|p| {
            p.source_content
                .as_deref()
                .unwrap_or(p.name.as_str())
                .to_string()
        })
        .collect();
    for s in inputs_owned.iter_mut() {
        *s = normalize_string_for_json(s);
    }
    let inputs: Vec<&str> = inputs_owned.iter().map(|s| s.as_str()).collect();
    // Pre-allocate results aligned with chunked payload list. Each entry will
    // eventually be aggregated back to its original payload via chunking.
    let mut results: Vec<Option<(Vec<f32>, String, usize)>> = vec![None; chunked_payloads.len()];

    // Early exit: when no model configured, return all None for original inputs
    if model.is_empty() {
        return vec![None; orig_len];
    }

    // Helper: post a chunk of inputs, handling HTTP 413 by splitting the chunk
    // iteratively to avoid recursive async calls. This will split oversized
    // requests into smaller pieces up to a limited recursion depth (attempts).
    #[allow(clippy::too_many_arguments)]
    async fn send_chunk_and_handle(
        client: &HttpClient,
        tei_endpoint: &str,
        model: &str,
        initial_inputs: Vec<String>,
        initial_indices: Vec<usize>,
        chunked_payloads: &[EntityPayload],
        results: &mut [Option<(Vec<f32>, String, usize)>],
        tk: &Option<tiktoken_rs::CoreBPE>,
        token_limit: usize,
        truncate_dir: &str,
        attempts: usize,
        max_bytes: usize,
    ) {
        use std::collections::VecDeque;
        use tokio::time::{sleep, Duration as TokioDuration};

        let mut queue: VecDeque<(Vec<String>, Vec<usize>, usize)> = VecDeque::new();
        queue.push_back((initial_inputs, initial_indices, attempts));

        while let Some((inputs, indices, attempts_left)) = queue.pop_front() {
            if inputs.is_empty() {
                continue;
            }
            let req_body = serde_json::json!({"model": model, "input": inputs});
            match client.post(tei_endpoint).json(&req_body).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    debug!(
                        "tei response status={} for chunk {}..{}",
                        status,
                        indices.first().copied().unwrap_or(0),
                        indices.last().copied().unwrap_or(0)
                    );
                    if resp.status().is_success() {
                        match resp.bytes().await {
                            Ok(b) => match local_parse_embeddings_and_model(&b) {
                                Ok((vecs, maybe_model)) => {
                                    // Ensure embeddings array is non-empty; if empty, treat as failure and retry
                                    if vecs.is_empty() {
                                        let snippet = inputs
                                            .get(indices.first().copied().unwrap_or(0))
                                            .map(|s| s.chars().take(4096).collect::<String>())
                                            .unwrap_or_default();
                                        warn!("tei returned empty embeddings array for chunk {}..{}; marking for retry (attempts_left={}) body_snippet='{}'", indices.first().copied().unwrap_or(0), indices.last().copied().unwrap_or(0), attempts_left, snippet);
                                        // Requeue for retry if attempts remain
                                        if attempts_left > 0 {
                                            queue.push_back((
                                                inputs.clone(),
                                                indices.clone(),
                                                attempts_left - 1,
                                            ));
                                            // small backoff
                                            sleep(TokioDuration::from_millis(50)).await;
                                            continue;
                                        } else {
                                            // write DLQ entries for each index in this chunk
                                            for &gi in indices.iter() {
                                                if let Some(p) = chunked_payloads.get(gi) {
                                                    let head = p
                                                        .source_content
                                                        .as_deref()
                                                        .unwrap_or(&p.name)
                                                        .chars()
                                                        .take(256)
                                                        .collect::<String>();
                                                    append_to_embed_dlq(
                                                        &p.stable_id,
                                                        "empty_embeddings",
                                                        p.source_content
                                                            .as_ref()
                                                            .map(|s| s.len())
                                                            .unwrap_or(0),
                                                        &head,
                                                    );
                                                }
                                            }
                                            for &gi in indices.iter() {
                                                if gi < results.len() {
                                                    results[gi] = None;
                                                }
                                            }
                                            continue;
                                        }
                                    }
                                    let model_id = maybe_model.unwrap_or_else(|| model.to_string());
                                    for (j, v) in vecs.into_iter().enumerate() {
                                        if let Some(global_idx) = indices.get(j).copied() {
                                            if global_idx < results.len() {
                                                // Single-chunk response -> chunk_count = 1 for now
                                                results[global_idx] =
                                                    Some((v, model_id.clone(), 1usize));
                                            } else {
                                                warn!("tei returned embedding for out-of-range index {} (global_idx {}), ignoring", j, global_idx);
                                            }
                                        } else {
                                            warn!("tei returned more embeddings than tracked inputs (index {})", j);
                                        }
                                    }
                                }
                                Err(e) => {
                                    let snippet = String::from_utf8_lossy(&b)
                                        .chars()
                                        .take(4096)
                                        .collect::<String>();
                                    warn!("failed to parse tei response (chunk {}..{}): {} ; body_snippet='{}'", indices.first().copied().unwrap_or(0), indices.last().copied().unwrap_or(0), e, snippet);
                                    for &gi in indices.iter() {
                                        if gi < results.len() {
                                            results[gi] = None;
                                        }
                                    }
                                }
                            },
                            Err(e) => {
                                warn!(
                                    "failed to read tei response bytes for chunk {}..{}: {}",
                                    indices.first().copied().unwrap_or(0),
                                    indices.last().copied().unwrap_or(0),
                                    e
                                );
                                for &gi in indices.iter() {
                                    if gi < results.len() {
                                        results[gi] = None;
                                    }
                                }
                            }
                        }
                    } else if status.as_u16() == 413 {
                        warn!("tei returned 413 for chunk {}..{} (attempts_left={}) - will try splitting/truncating", indices.first().copied().unwrap_or(0), indices.last().copied().unwrap_or(0), attempts_left);
                        // If we have multiple inputs, split the chunk and retry halves.
                        if inputs.len() > 1 && attempts_left > 0 {
                            let mid = inputs.len() / 2;
                            let mut left_inputs = inputs.clone();
                            let right_inputs = left_inputs.split_off(mid);
                            let mut left_indices = indices.clone();
                            let right_indices = left_indices.split_off(mid);
                            queue.push_back((left_inputs, left_indices, attempts_left - 1));
                            // small backoff between retries
                            sleep(TokioDuration::from_millis(25)).await;
                            queue.push_back((right_inputs, right_indices, attempts_left - 1));
                        } else if inputs.len() == 1 {
                            // Single-input case: try to safely truncate and retry even if
                            // truncation wasn't explicitly enabled. This addresses TEI
                            // rejecting single large inputs (413) due to token limits.
                            let gi = indices[0];
                            if attempts_left > 0 {
                                let orig = inputs[0].clone();
                                // Try token-aware truncation if tokenizer available
                                if let Some(enc) = tk.as_ref() {
                                    let tokens = enc.encode_ordinary(&orig);
                                    if tokens.len() > token_limit {
                                        let truncated_tokens = if truncate_dir
                                            .eq_ignore_ascii_case("Left")
                                        {
                                            let start = tokens.len().saturating_sub(token_limit);
                                            tokens[start..].to_vec()
                                        } else {
                                            let end = token_limit.min(tokens.len());
                                            tokens[..end].to_vec()
                                        };
                                        if let Ok(decoded) = enc.decode(truncated_tokens) {
                                            debug!("forcing token-truncation for single-input index {}: original_tokens={} truncated_tokens={}", gi, tokens.len(), token_limit);
                                            queue.push_back((
                                                vec![decoded],
                                                vec![gi],
                                                attempts_left - 1,
                                            ));
                                            continue;
                                        }
                                    }
                                }
                                // Fallback to byte-length truncation based on max_bytes
                                let byte_trim = if max_bytes > 0 { max_bytes / 2 } else { 40_000 };
                                if orig.len() > byte_trim {
                                    let trimmed = orig.chars().take(byte_trim).collect::<String>();
                                    debug!("forcing byte-truncation for single-input index {}: original_bytes={} trimmed_bytes={}", gi, orig.len(), trimmed.len());
                                    queue.push_back((vec![trimmed], vec![gi], attempts_left - 1));
                                    continue;
                                }
                            }
                            // If we reach here, truncation not possible or attempts exhausted
                            warn!("single input chunk too large and retries exhausted or truncation failed; marking as None for index {}", gi);
                            if gi < results.len() {
                                results[gi] = None;
                            }
                        } else {
                            match resp.text().await {
                                Ok(text) => {
                                    let snippet = text.chars().take(4096).collect::<String>();
                                    warn!(
                                        "tei returned 413 body snippet='{}' for chunk {}..{}",
                                        snippet,
                                        indices.first().copied().unwrap_or(0),
                                        indices.last().copied().unwrap_or(0)
                                    );
                                }
                                Err(e) => {
                                    warn!("tei returned 413 and body could not be read: {}", e);
                                }
                            }
                            for &gi in indices.iter() {
                                if gi < results.len() {
                                    results[gi] = None;
                                }
                            }
                        }
                    } else {
                        match resp.text().await {
                            Ok(text) => {
                                let snippet = text.chars().take(4096).collect::<String>();
                                warn!("tei returned non-success status={} for chunk {}..{} body_snippet='{}'", status, indices.first().copied().unwrap_or(0), indices.last().copied().unwrap_or(0), snippet);
                            }
                            Err(e) => {
                                warn!("tei returned non-success status={} for chunk {}..{} and body could not be read: {}", status, indices.first().copied().unwrap_or(0), indices.last().copied().unwrap_or(0), e);
                            }
                        }
                        for &gi in indices.iter() {
                            if gi < results.len() {
                                results[gi] = None;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "failed to POST to tei endpoint {} for chunk {}..{} stable_ids=[{}]: {}",
                        tei_endpoint,
                        indices.first().copied().unwrap_or(0),
                        indices.last().copied().unwrap_or(0),
                        chunked_payloads
                            .get(indices.first().copied().unwrap_or(0))
                            .map(|p| p.stable_id.clone())
                            .unwrap_or_default(),
                        e
                    );
                    for &gi in indices.iter() {
                        if gi < results.len() {
                            results[gi] = None;
                        }
                    }
                }
            }
        }
    }

    // Build dynamic chunks that respect both a per-input cap (batch_size),
    // an approximate serialized byte limit (max_bytes), and a token limit per input.
    // Use owned strings for current_inputs so we can safely store truncated text
    let mut current_inputs: Vec<String> = Vec::new();
    let mut idx_in_current: Vec<usize> = Vec::new();

    // Iterate inputs and flush chunks inline when limits are hit.
    for (i, input) in inputs.iter().enumerate() {
        // Token-aware check/truncation per input -> produce an owned String to push.
        // Proactively truncate oversized inputs to avoid TEI 413s even if truncation
        // wasn't explicitly enabled. This addresses the root cause.
        let mut effective_owned = input.to_string();
        if let Some(enc) = tk.as_ref() {
            let tokens = enc.encode_ordinary(input);
            if tokens.len() > token_limit {
                // Truncate to token_limit according to direction.
                let truncated_tokens = if truncate_dir.eq_ignore_ascii_case("Left") {
                    let start = tokens.len().saturating_sub(token_limit);
                    tokens[start..].to_vec()
                } else {
                    let end = token_limit.min(tokens.len());
                    tokens[..end].to_vec()
                };
                if let Ok(decoded) = enc.decode(truncated_tokens) {
                    debug!("proactively token-truncated input index {} original_tokens={} truncated_tokens={}", i, tokens.len(), token_limit);
                    effective_owned = decoded;
                }
            }
        } else {
            static TOKEN_WARN_ONCE: std::sync::Once = std::sync::Once::new();
            TOKEN_WARN_ONCE.call_once(|| {
                warn!("tokenizer initialization failed – HZ_EMBED_TOKEN_LIMIT cannot be enforced precisely; falling back to byte-size heuristic");
            });
            // Byte-based fallback: clamp to max_bytes/2 to avoid giant payloads
            if max_bytes > 0 && effective_owned.len() > max_bytes / 2 {
                let allowed = max_bytes / 2;
                effective_owned = effective_owned.chars().take(allowed).collect();
                debug!(
                    "proactively byte-truncated input index {} to {} bytes (no tokenizer)",
                    i, allowed
                );
            }
        }
        current_inputs.push(effective_owned);
        idx_in_current.push(i);
        // Estimate size with current inputs
        let req_body = serde_json::json!({"model": model, "input": current_inputs});
        let size = match serde_json::to_vec(&req_body) {
            Ok(b) => b.len(),
            Err(e) => {
                // Serialization failed for current_inputs: log and DLQ each payload
                warn!(
                    "failed to serialize embedding request body for chunk (will DLQ) err={}",
                    e
                );
                for &gi in idx_in_current.iter() {
                    if let Some(p) = chunked_payloads.get(gi) {
                        let head = p
                            .source_content
                            .as_deref()
                            .unwrap_or("")
                            .chars()
                            .take(256)
                            .collect::<String>();
                        append_to_embed_dlq(
                            &p.stable_id,
                            "serialize_error",
                            p.source_content.as_ref().map(|s| s.len()).unwrap_or(0),
                            &head,
                        );
                        if gi < results.len() {
                            results[gi] = None;
                        }
                    }
                }
                0usize
            }
        };

        // Decide whether we've exceeded size or count limits. If so, remove the
        // last added element and flush the chunk, then start a new chunk with
        // the last element.
        if (size > max_bytes && current_inputs.len() > 1) || current_inputs.len() >= batch_size {
            // Pop last to requeue into next chunk
            let last = current_inputs.pop().unwrap();
            let _last_idx = idx_in_current.pop().unwrap();
            // Send current chunk (without last)
            // Build request using the provided `client` and await response inline.
            let req_body = serde_json::json!({"model": model, "input": current_inputs});
            let size = match serde_json::to_vec(&req_body) {
                Ok(b) => b.len(),
                Err(e) => {
                    warn!("failed to serialize single-oversized embedding request body (will DLQ) err={}", e);
                    for &gi in idx_in_current.iter() {
                        if let Some(p) = payloads.get(gi) {
                            let head = p
                                .source_content
                                .as_deref()
                                .unwrap_or("")
                                .chars()
                                .take(256)
                                .collect::<String>();
                            append_to_embed_dlq(
                                &p.stable_id,
                                "serialize_error",
                                p.source_content.as_ref().map(|s| s.len()).unwrap_or(0),
                                &head,
                            );
                            if gi < results.len() {
                                results[gi] = None;
                            }
                        }
                    }
                    0usize
                }
            };
            // Compute chunk index range and stable ids for better diagnostics
            let chunk_start = idx_in_current.first().copied().unwrap_or(0);
            let chunk_end = idx_in_current.last().copied().unwrap_or(0);
            let stable_ids: Vec<String> = idx_in_current
                .iter()
                .filter_map(|&gi| chunked_payloads.get(gi))
                .map(|p| p.stable_id.clone())
                .collect();
            debug!(
                "posting embeddings request size_bytes={} inputs_in_chunk={} chunk_range={}..{} stable_ids=[{}]",
                size,
                current_inputs.len(),
                chunk_start,
                chunk_end,
                stable_ids.first().map(|s| s.as_str()).unwrap_or("")
            );
            // Use helper that will retry/split on 413
            send_chunk_and_handle(
                client,
                tei_endpoint,
                model,
                current_inputs.clone(),
                idx_in_current.clone(),
                chunked_payloads,
                &mut results[..],
                &tk,
                token_limit,
                &truncate_dir,
                3,
                max_bytes,
            )
            .await;
            // Start new chunk with last
            current_inputs.clear();
            idx_in_current.clear();
            current_inputs.push(last);
            idx_in_current.push(i);
        } else if size > max_bytes && current_inputs.len() == 1 {
            // Single input exceeds max_bytes; send it anyway to avoid infinite loop
            // Send single oversized input anyway
            let req_body = serde_json::json!({"model": model, "input": current_inputs});
            let size = serde_json::to_vec(&req_body).map(|b| b.len()).unwrap_or(0);
            // Single-input oversized branch: add same diagnostics as multi-input
            let chunk_start = idx_in_current.first().copied().unwrap_or(0);
            let chunk_end = idx_in_current.last().copied().unwrap_or(0);
            let stable_ids: Vec<String> = idx_in_current
                .iter()
                .filter_map(|&gi| chunked_payloads.get(gi))
                .map(|p| p.stable_id.clone())
                .collect();
            debug!(
                "posting embeddings request size_bytes={} inputs_in_chunk={} chunk_range={}..{} stable_ids=[{}]",
                size,
                current_inputs.len(),
                chunk_start,
                chunk_end,
                stable_ids.first().map(|s| s.as_str()).unwrap_or("")
            );
            // Use helper that will retry/split on 413
            send_chunk_and_handle(
                client,
                tei_endpoint,
                model,
                current_inputs.clone(),
                idx_in_current.clone(),
                chunked_payloads,
                &mut results[..],
                &tk,
                token_limit,
                &truncate_dir,
                3,
                max_bytes,
            )
            .await;
            current_inputs.clear();
            idx_in_current.clear();
        }
    }

    // Send any remaining inputs (reuse same inline logic as above)
    if !current_inputs.is_empty() {
        let req_body = serde_json::json!({"model": model, "input": current_inputs});
        let size = serde_json::to_vec(&req_body).map(|b| b.len()).unwrap_or(0);
        debug!(
            "posting embeddings request size_bytes={} inputs_in_chunk={}",
            size,
            current_inputs.len()
        );
        // Final remaining inputs: diagnostic similar to earlier branches
        let chunk_start = idx_in_current.first().copied().unwrap_or(0);
        let chunk_end = idx_in_current.last().copied().unwrap_or(0);
        let stable_ids: Vec<String> = idx_in_current
            .iter()
            .filter_map(|&gi| chunked_payloads.get(gi))
            .map(|p| p.stable_id.clone())
            .collect();
        debug!(
            "posting embeddings request size_bytes={} inputs_in_chunk={} chunk_range={}..{} stable_ids=[{}]",
            size,
            current_inputs.len(),
            chunk_start,
            chunk_end,
            stable_ids.first().map(|s| s.as_str()).unwrap_or("")
        );
        // Use helper that will retry/split on 413
        send_chunk_and_handle(
            client,
            tei_endpoint,
            model,
            current_inputs.clone(),
            idx_in_current.clone(),
            chunked_payloads,
            &mut results[..],
            &tk,
            token_limit,
            &truncate_dir,
            3,
            max_bytes,
        )
        .await;
    }

    // At this point `results` holds per-chunk embeddings aligned with the
    // `send_payloads` slice. Aggregate element-wise into one embedding per
    // original payload by averaging successful chunk embeddings. If some
    // chunks failed, we still average the successful ones and write a DLQ
    // entry noting a partial failure so operators can triage.
    let mut final_out: Vec<MaybeEmbedding> = vec![None; orig_len];
    // count total chunks per original
    let mut total_chunks: Vec<usize> = vec![0usize; orig_len];
    for &orig in chunk_to_orig.iter() {
        if orig < total_chunks.len() {
            total_chunks[orig] += 1;
        }
    }
    // accumulate successful chunks
    let mut acc: Vec<Vec<Vec<f32>>> = vec![Vec::new(); orig_len];
    let mut acc_model_per_orig: Vec<Option<String>> = vec![None; orig_len];
    for (chunk_idx, res_opt) in results.into_iter().enumerate() {
        let orig = chunk_to_orig.get(chunk_idx).copied().unwrap_or(0);
        if let Some((vec_f, model_id, _chunk_count)) = res_opt {
            if orig < acc.len() {
                acc[orig].push(vec_f);
                if acc_model_per_orig[orig].is_none() {
                    acc_model_per_orig[orig] = Some(model_id);
                }
            }
        }
    }
    for i in 0..orig_len {
        let success = &acc[i];
        if success.is_empty() {
            // no successful chunks -> None
            final_out[i] = None;
            // If we had chunks but none succeeded, write DLQ to help ops
            if total_chunks[i] > 0 {
                // Use the original payloads slice when emitting DLQ entries so the
                // stable_id and snippet map to the original entity (not a chunked
                // representation). Previously we accidentally referenced the
                // chunked view here which produced incorrect DLQ lines.
                if let Some(p) = orig_payloads.get(i) {
                    let head = p
                        .source_content
                        .as_deref()
                        .unwrap_or("")
                        .chars()
                        .take(256)
                        .collect::<String>();
                    append_to_embed_dlq(
                        &p.stable_id,
                        "all_chunks_failed",
                        p.source_content.as_ref().map(|s| s.len()).unwrap_or(0),
                        &head,
                    );
                }
            }
            continue;
        }
        // Ensure consistent dimensionality; skip mismatched
        let dim = success[0].len();
        let mut sum: Vec<f32> = vec![0.0f32; dim];
        let mut count = 0usize;
        for v in success.iter() {
            if v.len() != dim {
                warn!(
                    "skipping chunk with mismatched embedding dim for orig={} expected={} got={}",
                    i,
                    dim,
                    v.len()
                );
                continue;
            }
            for (k, val) in v.iter().enumerate() {
                sum[k] += *val;
            }
            count += 1;
        }
        if count == 0 {
            final_out[i] = None;
            continue;
        }
        for s in sum.iter_mut() {
            *s /= count as f32;
        }
        let model_id = acc_model_per_orig[i]
            .clone()
            .unwrap_or_else(|| model.to_string());
        // chunk_count = number of successful chunks aggregated
        final_out[i] = Some((sum, model_id, count));
        // If partial failure occured (some chunks failed), write DLQ note
        if total_chunks[i] > count {
            if let Some(p) = orig_payloads.get(i) {
                let head = p
                    .source_content
                    .as_deref()
                    .unwrap_or("")
                    .chars()
                    .take(256)
                    .collect::<String>();
                append_to_embed_dlq(
                    &p.stable_id,
                    "partial_chunks",
                    p.source_content.as_ref().map(|s| s.len()).unwrap_or(0),
                    &head,
                );
            }
        }
    }
    final_out
}

// Local parsing helper: accept TEI-native or OpenAI-style responses and return
// embeddings + optional model id.
fn local_parse_embeddings_and_model(
    b: &[u8],
) -> Result<(Vec<Vec<f32>>, Option<String>), serde_json::Error> {
    // Try TEI-native first
    #[derive(serde::Deserialize)]
    struct TeiRespLocal {
        embeddings: Vec<Vec<f32>>,
    }
    #[derive(serde::Deserialize)]
    struct OpenAiItemLocal {
        embedding: Vec<f32>,
    }
    #[derive(serde::Deserialize)]
    struct OpenAiRespLocal {
        data: Vec<OpenAiItemLocal>,
    }

    match serde_json::from_slice::<TeiRespLocal>(b) {
        Ok(t) => {
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
        Err(_first) => match serde_json::from_slice::<OpenAiRespLocal>(b) {
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
            Err(second) => Err(second),
        },
    }
}

/// Chunk payloads into smaller payloads based on tokenizer and token_limit.
pub fn chunk_payloads_for_embeddings(
    payloads: &[EntityPayload],
    token_limit: usize,
    chunk_overlap: usize,
    tk: Option<tiktoken_rs::CoreBPE>,
) -> (Vec<EntityPayload>, Vec<usize>) {
    let mut send_payloads: Vec<EntityPayload> = Vec::new();
    let mut chunk_to_orig: Vec<usize> = Vec::new();
    for (i, p) in payloads.iter().enumerate() {
        let text = p.source_content.as_deref().unwrap_or("").to_string();
        if let Some(ref enc) = tk {
            let tokens = enc.encode_ordinary(&text);
            if tokens.len() > token_limit {
                let step = token_limit.saturating_sub(chunk_overlap).max(1);
                let mut start = 0usize;
                while start < tokens.len() {
                    let end = (start + token_limit).min(tokens.len());
                    let window = tokens[start..end].to_vec();
                    if let Ok(decoded) = enc.decode(window) {
                        let mut cp = p.clone();
                        cp.source_content = Some(decoded);
                        send_payloads.push(cp);
                        chunk_to_orig.push(i);
                    } else {
                        let total = tokens.len();
                        let s_char = text.len() * start / total;
                        let e_char = text.len() * end / total;
                        let substr = text
                            .chars()
                            .skip(s_char)
                            .take(e_char.saturating_sub(s_char))
                            .collect::<String>();
                        let mut cp = p.clone();
                        cp.source_content = Some(substr);
                        send_payloads.push(cp);
                        chunk_to_orig.push(i);
                    }
                    if end == tokens.len() {
                        break;
                    }
                    start = start.saturating_add(step);
                }
                continue;
            }
        }
        send_payloads.push(p.clone());
        chunk_to_orig.push(i);
    }
    (send_payloads, chunk_to_orig)
}
