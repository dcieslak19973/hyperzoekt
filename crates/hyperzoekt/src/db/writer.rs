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
use super::config::{DbWriterConfig, PersistAckSender, PersistedEntityMeta, SpawnResult};
use super::helpers::CALL_EDGE_CAPTURE;
use crate::db::connection::{connect, SurrealConnection};

use crate::repo_index::indexer::payload::EntityPayload;
use anyhow::Result;
use log::{debug, info, trace, warn};
use sha2::{Digest, Sha256};
use std::sync::mpsc::sync_channel;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

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
        // Allow tests to request a reset of global dedup/creation sets for deterministic edge counts.
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
            if cfg_clone.initial_batch { initial_batch_insert(&db, &payloads_clone, batch_capacity).await?; return Ok(()); }
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
                    let (mut statements_raw, entity_count) = build_batch_sql(&acc, &cfg_clone);
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
                            if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                                println!("SEPARATING RELATE: {}", sql);
                            }
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
                        info!("BATCH SQL ({} grouped chunks, original {} stmts non-create={} relate={}) total_len={} chars", statements.len(), statements_raw.len(), non_create.len(), relates.len(), total_len);
                    }
                    let start = std::time::Instant::now();
                    let mut exec_error: Option<String> = None;
                    info!("executing create-entity chunks={} and non-create chunks={} (orig stmts create={} non_create={} relate={})",
                        create_statements.len(), statements.len(), create_entities.len(), non_create.len(), relates.len());
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
                                        // Support both entity and repo CREATE forms.
                                        let mut handled = false;
                                        if let Some(rest) = s_sql.strip_prefix("CREATE entity:") {
                                            if let Some((id_part, json_part)) = rest.split_once(" CONTENT ") {
                                                let id = id_part.trim();
                                                let mut json = json_part.trim();
                                                if json.ends_with(';') {
                                                    json = json.trim_end_matches(';').trim();
                                                }
                                                let update_q = format!("BEGIN; UPDATE entity:{id} CONTENT {json}; COMMIT;", id=id, json=json);
                                                if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                                                    println!("CREATE duplicate for {} detected; running per-entity fallback: {}", id, update_q);
                                                }
                                                match db.query(&update_q).await {
                                                    Ok(uresp) => {
                                                        if std::env::var("HZ_DEBUG_SQL_RESP").ok().as_deref() == Some("1") {
                                                            info!("per-entity UPDATE ok for {}", id);
                                                            debug!("update resp: {:?}", uresp);
                                                        }
                                                    }
                                                    Err(ue) => {
                                                        warn!("per-entity UPDATE failed for {} err={}", id, ue);
                                                    }
                                                }
                                                handled = true;
                                            }
                                        }
                                        if !handled {
                                            if let Some(rest) = s_sql.strip_prefix("CREATE repo:") {
                                                if let Some((id_part, json_part)) = rest.split_once(" CONTENT ") {
                                                    let id = id_part.trim();
                                                    let mut json = json_part.trim();
                                                    if json.ends_with(';') {
                                                        json = json.trim_end_matches(';').trim();
                                                    }
                                                    let update_q = format!("BEGIN; UPDATE repo:{id} CONTENT {json}; COMMIT;", id=id, json=json);
                                                    if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                                                        println!("CREATE duplicate for repo {} detected; running per-repo fallback: {}", id, update_q);
                                                    }
                                                    match db.query(&update_q).await {
                                                        Ok(uresp) => {
                                                            if std::env::var("HZ_DEBUG_SQL_RESP").ok().as_deref() == Some("1") {
                                                                info!("per-repo UPDATE ok for {}", id);
                                                                debug!("update resp: {:?}", uresp);
                                                            }
                                                        }
                                                        Err(ue) => {
                                                            warn!("per-repo UPDATE failed for {} err={}", id, ue);
                                                        }
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
                                                    if json.ends_with(';') {
                                                        json = json.trim_end_matches(';').trim();
                                                    }
                                                    let update_q = format!("BEGIN; UPDATE entity_snapshot:{id} CONTENT {json}; COMMIT;", id=id, json=json);
                                                    if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                                                        println!("CREATE duplicate for entity_snapshot {} detected; running per-entity_snapshot fallback: {}", id, update_q);
                                                    }
                                                    match db.query(&update_q).await {
                                                        Ok(uresp) => {
                                                            if std::env::var("HZ_DEBUG_SQL_RESP").ok().as_deref() == Some("1") {
                                                                info!("per-entity_snapshot UPDATE ok for {}", id);
                                                                debug!("update resp: {:?}", uresp);
                                                            }
                                                        }
                                                        Err(ue) => {
                                                            warn!("per-entity_snapshot UPDATE failed for {} err={}", id, ue);
                                                        }
                                                    }
                                                    handled = true;
                                                }
                                            }
                                        }
                                        if !handled {
                                            info!("create-entity duplicate ignored (couldn't parse id/json): {}", resp_str);
                                        }
                                    } else {
                                        // Unexpected error on create: log and continue
                                        warn!("create-entity had unexpected error (continuing): {}", resp_str);
                                    }
                                } else if std::env::var("HZ_DEBUG_SQL_RESP").ok().as_deref() == Some("1") {
                                    debug!("create-entity ok resp: {:?}", resp);
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
                        if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                            println!("EXECUTING NON-CREATE WITH BINDS: {}", q);
                        }
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
                        if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                            println!("EXECUTING CHUNK: {}", chunk);
                        }
                        match db.query(chunk).await {
                            Ok(resp) => {
                                let resp_str = format!("{:?}", resp);
                                if resp_str.contains("Err(Api(Query") && !(resp_str.contains("already contains") || resp_str.contains("already exists")) {
                                    warn!("non-RELATE chunk response contained errors: {}", resp_str);
                                    exec_error = Some(resp_str);
                                    break;
                                } else if std::env::var("HZ_DEBUG_SQL_RESP").ok().as_deref() == Some("1") {
                                    info!("chunk ok len={} bytes", chunk.len());
                                    debug!("resp: {:?}", resp);
                                }
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
                            if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                                println!("EXECUTING RELATE CHUNK: {}", q);
                            }
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
                        // If we still have retry budget, try a heuristic transform then retry.
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
                                    match serde_json::to_string(&job) {
                                        Ok(js) => job_buf.push(js),
                                        Err(err) => { warn!("serialize embed job failed stable_id={} err={}", e.stable_id, err); }
                                    }
                                }
                                if !job_buf.is_empty() {
                                    match pool.get().await { Ok(mut conn) => {
                                        // Chunk pushes to avoid extremely large single RPUSH
                                        let mut pushed_total = 0usize;
                                        for chunk in job_buf.chunks(500) {
                                            let res: Result<usize, _> = deadpool_redis::redis::AsyncCommands::rpush(&mut conn, &embed_queue, chunk).await;
                                            match res {
                                                Ok(_n) => { pushed_total += chunk.len(); },
                                                Err(e) => { warn!("embed enqueue rpush failed chunk={} err={}", chunk.len(), e); break; }
                                            }
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
                        for e in &acc {
                            metas.push(PersistedEntityMeta {
                                stable_id: e.stable_id.clone(),
                                repo_name: e.repo_name.clone(),
                                language: e.language.clone(),
                                kind: e.kind.clone(),
                                name: e.name.clone(),
                                source_url: e.source_url.clone(),
                            });
                        }
                        if let Err(e) = sender.send(metas) { log::warn!("persist ack send failed: {}", e); }
                    }
                    info!("batch success size {} attempt {} ms {}", entity_count, attempt, dur);
                    // Separate count queries to avoid multi-statement response shape ambiguity
                    let repo_count = if let Ok(mut r) = db.query("SELECT count() FROM repo GROUP ALL;").await {
                        let rows: Vec<serde_json::Value> = r.take(0).unwrap_or_default();
                        rows.first().and_then(|o| o.get("count").and_then(|v| v.as_i64()))
                    } else { None };
                    let file_count = if let Ok(mut r) = db.query("SELECT count() FROM file GROUP ALL;").await {
                        let rows: Vec<serde_json::Value> = r.take(0).unwrap_or_default();
                        rows.first().and_then(|o| o.get("count").and_then(|v| v.as_i64()))
                    } else { None };
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
    let mut vals = Vec::new();
    for p in payloads {
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
        if let Some(rank) = p.rank {
            entity_obj.insert(
                "rank".to_string(),
                serde_json::Value::Number(serde_json::Number::from_f64(rank as f64).unwrap()),
            );
        }
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
        entity_obj.insert(
            "snapshot".to_string(),
            serde_json::Value::Object(snapshot_obj),
        );

        let mut entity_json = serde_json::Value::Object(entity_obj);
        sanitize_json_strings(&mut entity_json);
        vals.push(entity_json);
    }
    for ch in vals.chunks(chunk.clamp(1, 5000)) {
        // For safety, insert each entity in the chunk using parameterized binds
        // instead of embedding serialized JSON into SQL strings. This keeps
        // Surreal tokens and JSON content from being accidentally quoted or
        // interpolated and keeps initial-batch logic simple and correct.
        let mut success_count = 0usize;
        let st = std::time::Instant::now();
        for it in ch {
            let val = it.clone();
            // Ensure string fields are sanitized already.
            let sql = "BEGIN; CREATE entity CONTENT $e; COMMIT;";
            match db.query_with_binds(sql, vec![("e", val)]).await {
                Ok(_) => {
                    success_count += 1;
                }
                Err(e) => {
                    warn!("entity insert failed err={}", e);
                }
            }
        }
        info!(
            "chunk inserted size={} succeeded={} ms={}",
            ch.len(),
            success_count,
            st.elapsed().as_millis()
        );
    }
    // Enqueue embed jobs if enabled
    if let Ok(embed_jobs_enabled) = std::env::var("HZ_ENABLE_EMBED_JOBS") {
        if matches!(
            embed_jobs_enabled.as_str(),
            "1" | "true" | "TRUE" | "yes" | "YES"
        ) {
            if let Some(pool) = zoekt_distributed::redis_adapter::create_redis_pool() {
                let queue_key = std::env::var("HZ_EMBED_JOBS_QUEUE")
                    .unwrap_or_else(|_| "zoekt:embed_jobs".to_string());
                let mut job_buf: Vec<String> = Vec::new();
                for p in payloads {
                    #[derive(serde::Serialize)]
                    struct EmbeddingJob<'a> {
                        stable_id: &'a str,
                        repo_name: &'a str,
                        language: &'a str,
                        kind: &'a str,
                        name: &'a str,
                        source_url: Option<&'a str>,
                    }
                    let job = EmbeddingJob {
                        stable_id: &p.stable_id,
                        repo_name: &p.repo_name,
                        language: &p.language,
                        kind: &p.kind,
                        name: &p.name,
                        source_url: p.source_url.as_deref(),
                    };
                    if let Ok(js) = serde_json::to_string(&job) {
                        job_buf.push(js);
                    }
                }
                if !job_buf.is_empty() {
                    if let Ok(mut conn) = pool.get().await {
                        let mut pushed_total = 0usize;
                        for chunk in job_buf.chunks(500) {
                            let res: Result<usize, _> =
                                deadpool_redis::redis::AsyncCommands::rpush(
                                    &mut conn, &queue_key, chunk,
                                )
                                .await;
                            match res {
                                Ok(_) => {
                                    pushed_total += chunk.len();
                                }
                                Err(e) => {
                                    warn!(
                                        "embed enqueue rpush failed chunk={} err={}",
                                        chunk.len(),
                                        e
                                    );
                                    break;
                                }
                            }
                        }
                        info!(
                            "initial batch embed enqueue complete repo=? jobs={} queue={}",
                            pushed_total, queue_key
                        );
                    }
                }
            }
        }
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
        // Content table for deduplicated source content and embeddings
        vec![
            "DEFINE TABLE content SCHEMALESS PERMISSIONS FULL;",
            "DEFINE TABLE content;",
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
        // entity_snapshot to content relation
        vec![
            "DEFINE TABLE has_content TYPE RELATION FROM entity_snapshot TO content;",
            "DEFINE TABLE has_content TYPE RELATION;",
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
        // Content table embedding fields
        vec![
            "DEFINE FIELD embedding ON content TYPE array DEFAULT [];",
            "REMOVE FIELD embedding ON content; DEFINE FIELD embedding ON content TYPE array DEFAULT [];",
        ],
        vec![
            "DEFINE FIELD embedding_len ON content TYPE int DEFAULT 0;",
            "REMOVE FIELD embedding_len ON content; DEFINE FIELD embedding_len ON content TYPE int DEFAULT 0;",
        ],
        vec![
            "DEFINE FIELD embedding_model ON content TYPE string DEFAULT '';",
            "REMOVE FIELD embedding_model ON content; DEFINE FIELD embedding_model ON content TYPE string DEFAULT '';",
        ],
        vec![
            "DEFINE FIELD embedding_dim ON content TYPE int DEFAULT 0;",
            "REMOVE FIELD embedding_dim ON content; DEFINE FIELD embedding_dim ON content TYPE int DEFAULT 0;",
        ],
        vec![
            "DEFINE FIELD embedding_created_at ON content TYPE datetime DEFAULT time::now();",
            "REMOVE FIELD embedding_created_at ON content; DEFINE FIELD embedding_created_at ON content TYPE datetime DEFAULT time::now();",
        ],
        vec![
            "DEFINE FIELD embedding_status ON content TYPE string DEFAULT 'pending';",
            "REMOVE FIELD embedding_status ON content; DEFINE FIELD embedding_status ON content TYPE string DEFAULT 'pending';",
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
            "DEFINE INDEX idx_content_id ON content COLUMNS id;",
        ],
        vec![
            "DEFINE INDEX idx_has_content_unique ON has_content FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_has_snapshot_unique ON has_snapshot FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_snapshot_file_unique ON snapshot_file FIELDS in, out UNIQUE;",
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

type BatchStatements = Vec<(String, Option<Vec<(&'static str, serde_json::Value)>>)>;
#[allow(clippy::type_complexity)]
fn build_batch_sql(acc: &[EntityPayload], cfg: &DbWriterConfig) -> (BatchStatements, usize) {
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
    // de-duplicate files by repo-relative path (store relative paths in DB)
    let mut file_map: HashMap<String, usize> = HashMap::new();
    let mut file_list: Vec<(String, &str)> = Vec::new();
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

    for p in acc {
        if let Some(fp) = payload_file_hint(p) {
            let repo_rel = compute_repo_relative(&fp, &p.repo_name);
            if !file_map.contains_key(&repo_rel) {
                let idx = file_list.len();
                file_map.insert(repo_rel.clone(), idx);
                file_list.push((repo_rel, &p.language));
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
        for (path, lang) in file_list.iter() {
            let fid = sanitize_id(path);
            if guard.insert(fid.clone()) {
                // First time we've seen this file in-process: create the row with binds
                let mut map = serde_json::Map::new();
                map.insert("path".to_string(), serde_json::Value::String(path.clone()));
                map.insert(
                    "language".to_string(),
                    serde_json::Value::String(lang.to_string()),
                );
                let sql = format!("CREATE file:{fid} CONTENT $f;", fid = fid);
                statements.push((sql, Some(vec![("f", serde_json::Value::Object(map))])));
            } else {
                // Subsequent occurrences: update the existing row.
                let binds = vec![
                    ("p", serde_json::Value::String(path.clone())),
                    ("l", serde_json::Value::String(lang.to_string())),
                ];
                let sql = format!("UPDATE file:{fid} SET path = $p, language = $l;", fid = fid);
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
        for p in acc {
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
            if let Some(rank) = p.rank {
                entity_obj.insert(
                    "rank".to_string(),
                    serde_json::Value::Number(serde_json::Number::from_f64(rank as f64).unwrap()),
                );
            }
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
            entity_obj.insert(
                "snapshot".to_string(),
                serde_json::Value::Object(snapshot_obj),
            );

            // Compute content_id for deduplication
            let content_text = if let Some(sc) = &p.source_content {
                if !sc.is_empty() {
                    sc.clone()
                } else {
                    format!(
                        "{}\n{}\n{}",
                        p.name,
                        p.signature,
                        p.doc.clone().unwrap_or_default()
                    )
                }
            } else {
                format!(
                    "{}\n{}\n{}",
                    p.name,
                    p.signature,
                    p.doc.clone().unwrap_or_default()
                )
            };
            let mut hasher = Sha256::new();
            hasher.update(content_text.as_bytes());
            let digest = hasher.finalize();
            let content_id = format!("{:x}", digest);
            entity_obj.insert(
                "content_id".to_string(),
                serde_json::Value::String(content_id.clone()),
            );

            let eid = sanitize_id(&p.stable_id);
            let mut entity_json = serde_json::Value::Object(entity_obj);
            sanitize_json_strings(&mut entity_json);
            let sql = format!("CREATE entity:{eid} CONTENT $e;", eid = eid);
            statements.push((sql, Some(vec![("e", entity_json)])));

            // Create entity_snapshot with the same ID for relations
            let mut snapshot_obj = serde_json::Map::new();
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
            // Add embedded commit reference if available
            if let Some(commit_id) = &cfg.commit_id {
                snapshot_obj.insert(
                    "sourcecontrol_commit".to_string(),
                    serde_json::Value::String(format!("commits:{}", commit_id)),
                );
            } else {
                // Set to empty string when no commit_id is available
                snapshot_obj.insert(
                    "sourcecontrol_commit".to_string(),
                    serde_json::Value::String(String::new()),
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

            // Create content record if it doesn't exist and relate to entity_snapshot
            let content_sql = format!(
                "CREATE content:{content_id} CONTENT {{}};",
                content_id = content_id
            );
            statements.push((content_sql, None));

            // Create has_content relation
            let content_rel = format!(
                "RELATE entity_snapshot:{eid}->has_content->content:{content_id};",
                eid = eid,
                content_id = content_id
            );
            statements.push((content_rel, None));

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
        for (path, _lang) in file_list.iter() {
            let fid = sanitize_id(path);
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
