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
use super::config::{DbWriterConfig, SpawnResult};
use super::connection::{connect, SurrealConnection};
use super::helpers::CALL_EDGE_CAPTURE;
use crate::repo_index::indexer::payload::EntityPayload;
use anyhow::Result;
use log::{debug, info, trace, warn};
use sha2::{Digest, Sha256};
use std::sync::mpsc::sync_channel;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

pub fn spawn_db_writer(payloads: Vec<EntityPayload>, cfg: DbWriterConfig) -> SpawnResult {
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
            use std::sync::mpsc::{RecvTimeoutError, TryRecvError};
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
                    let (mut statements_raw, entity_count) = build_batch_sql(&acc);
                    if statements_raw.is_empty() {
                        success = true;
                        info!("empty sql batch (no ops) size {} attempt {}", acc.len(), attempt);
                        break;
                    }
                    if std::env::var("HZ_DUMP_SQL_STATEMENTS").ok().as_deref() == Some("1") {
                        // Print to stdout so test harness captures the full statements.
                        println!("DUMPING RAW STATEMENTS ({} total):", statements_raw.len());
                        for (i, s) in statements_raw.iter().enumerate() {
                            println!("STMT[{}]: {}", i, s);
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
                    let mut create_entities: Vec<String> = Vec::new();
                    let mut non_create: Vec<String> = Vec::new();
                    let mut relates: Vec<String> = Vec::new();
                    for s in statements_raw.iter() {
                        let ts = s.trim_start();
                        if ts.to_uppercase().starts_with("RELATE ") {
                            relates.push(s.clone());
                        } else if ts.starts_with("CREATE entity:") || ts.starts_with("CREATE repo:") {
                            create_entities.push(s.clone());
                        } else {
                            non_create.push(s.clone());
                        }
                    }
                    let create_statements = group_statements(&create_entities, group_size);
                    let statements = group_statements(&non_create, group_size);
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
                    for s in &create_entities {
                        let q = format!("BEGIN; {} COMMIT;", s);
                        if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                            println!("EXECUTING CREATE-ENTITY (len={}): {}", q.len(), q);
                        }
                        match db.query(&q).await {
                            Ok(resp) => {
                                let resp_str = format!("{:?}", resp);
                                if resp_str.contains("Err(Api(Query") {
                                    // If the error is a duplicate-create, perform a per-entity UPDATE fallback.
                                    if resp_str.contains("already exists") || resp_str.contains("already contains") {
                                        // Parse id and json from the original CREATE statement and run UPDATE fallback.
                                        // Support both entity and repo CREATE forms.
                                        let mut handled = false;
                                        if let Some(rest) = s.strip_prefix("CREATE entity:") {
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
                                            if let Some(rest) = s.strip_prefix("CREATE repo:") {
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
                    // Execute primary non-RELATE transactional chunks next. Failures here abort the batch.
                    for chunk in &statements {
                        // Debug: show full transactional chunk being executed
                        if std::env::var("HZ_DEBUG_SQL").ok().as_deref() == Some("1") {
                            println!("EXECUTING CHUNK (len={}): {}", chunk.len(), chunk);
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
                        for rel in &relates {
                            let q = format!("BEGIN; {} COMMIT;", rel);
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
                                    warn!("relate query failed: {} err:{}", rel, e);
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
                                let ts = s.trim_start();
                                if ts.starts_with("CREATE repo:") {
                                    *s = s.replacen("CREATE repo:", "UPDATE repo:", 1);
                                } else if ts.starts_with("CREATE entity:") {
                                    *s = s.replacen("CREATE entity:", "UPDATE entity:", 1);
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
                if batches_sent % 10 == 0 && batches_sent>0 { let avg= if batches_sent>0 { (sum_ms as f64)/(batches_sent as f64) } else {0.0}; info!("DB metrics: batches_sent={} entities_sent={} total_retries={} avg_batch_ms={:.2} min_ms={:?} max_ms={:?} failures={} attempts={:?}", batches_sent, entities_sent, total_retries, avg, min_ms, max_ms, failures, attempt_counts); }
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
        if let Ok(mut v) = serde_json::to_value(p) {
            // do not persist the `file` field on Entity rows; remove it from the
            // serialized JSON. File metadata remains in the separate `file` table.
            if let Some(obj) = v.as_object_mut() {
                obj.remove("file");
            }
            vals.push(v);
        }
    }
    for ch in vals.chunks(chunk.clamp(1, 5000)) {
        let mut parts = Vec::new();
        for it in ch {
            parts.push(format!("CREATE entity CONTENT {};", it));
        }
        let q = format!("BEGIN; {} COMMIT;", parts.join(" "));
        let st = std::time::Instant::now();
        match db.query(&q).await {
            Ok(_) => {
                info!(
                    "chunk inserted size={} ms={}",
                    ch.len(),
                    st.elapsed().as_millis()
                );
            }
            Err(e) => warn!("chunk insert failed size={} err={}", ch.len(), e),
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
        vec![
            "DEFINE TABLE entity SCHEMALESS PERMISSIONS FULL;",
            "DEFINE TABLE entity;",
            "CREATE TABLE entity;",
        ],
        vec![
            "DEFINE TABLE file SCHEMALESS PERMISSIONS FULL;",
            "DEFINE TABLE file;",
            "CREATE TABLE file;",
        ],
        // explicit repo table definition (was previously only created implicitly during upserts)
        vec![
            "DEFINE TABLE repo SCHEMALESS PERMISSIONS FULL;",
            "DEFINE TABLE repo;",
            "CREATE TABLE repo;",
        ],
        // repo/file relation tables (graph)
        // Provide directional relation definitions first, then fall back to legacy generic declarations if the
        // SurrealDB version in use does not support the FROM/TO clause (older nightly/in-memory builds).
        vec![
            "DEFINE TABLE in_repo TYPE RELATION FROM file TO repo;",
            "DEFINE TABLE in_repo TYPE RELATION;",
            "CREATE TABLE in_repo;",
        ],
        vec![
            "DEFINE TABLE has_file TYPE RELATION FROM repo TO file;",
            "DEFINE TABLE has_file TYPE RELATION;",
            "CREATE TABLE has_file;",
        ],
        // relation tables for graph edges (entity -> entity)
        vec![
            "DEFINE TABLE calls TYPE RELATION FROM entity TO entity;",
            "DEFINE TABLE calls TYPE RELATION;",
            "CREATE TABLE calls;",
        ],
        vec![
            "DEFINE TABLE has_method TYPE RELATION FROM entity TO entity;",
            "DEFINE TABLE has_method TYPE RELATION;",
            "CREATE TABLE has_method;",
        ],
        vec![
            "DEFINE TABLE imports TYPE RELATION FROM entity TO entity;",
            "DEFINE TABLE imports TYPE RELATION;",
            "CREATE TABLE imports;",
        ],
        vec![
            "DEFINE FIELD stable_id ON entity TYPE string;",
            "ALTER TABLE entity CREATE FIELD stable_id TYPE string;",
        ],
        vec![
            "DEFINE FIELD repo_name ON entity TYPE string;",
            "ALTER TABLE entity CREATE FIELD repo_name TYPE string;",
        ],
        vec![
            "DEFINE INDEX idx_entity_stable_id ON entity COLUMNS stable_id;",
            "CREATE INDEX idx_entity_stable_id ON entity (stable_id);",
        ],
        vec![
            "DEFINE INDEX idx_file_path ON file COLUMNS path;",
            "CREATE INDEX idx_file_path ON file (path);",
        ],
        vec![
            "DEFINE INDEX idx_calls_unique ON calls FIELDS in, out UNIQUE;",
            "DEFINE INDEX idx_calls_unique ON calls FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_imports_unique ON imports FIELDS in, out UNIQUE;",
            "DEFINE INDEX idx_imports_unique ON imports FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_has_method_unique ON has_method FIELDS in, out UNIQUE;",
            "DEFINE INDEX idx_has_method_unique ON has_method FIELDS in, out UNIQUE;",
        ],
        // Note: do not create an explicit depends_on relation table here; rely on SurrealDB
        // relation/traversal features and let RELATE create relation rows as needed.
        vec![
            "DEFINE INDEX idx_has_file_unique ON has_file FIELDS in, out UNIQUE;",
            "DEFINE INDEX idx_has_file_unique ON has_file FIELDS in, out UNIQUE;",
        ],
        vec![
            "DEFINE INDEX idx_in_repo_unique ON in_repo FIELDS in, out UNIQUE;",
            "DEFINE INDEX idx_in_repo_unique ON in_repo FIELDS in, out UNIQUE;",
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
    for upgrade in [
        "ALTER TABLE in_repo TYPE RELATION FROM file TO repo;",
        "ALTER TABLE has_file TYPE RELATION FROM repo TO file;",
    ] {
        if let Err(e) = db.query(upgrade).await {
            trace!(
                "relation upgrade attempt failed (non-fatal): {} -> {}",
                upgrade,
                e
            );
        } else {
            debug!("relation upgrade applied: {}", upgrade);
        }
    }
    let _ = SCHEMA_INIT_ONCE.set(());
}

fn build_batch_sql(acc: &[EntityPayload]) -> (Vec<String>, usize) {
    // Return pre-separated statements to avoid later naive semicolon splitting that
    // corrupts JSON (e.g., signatures containing ';'). This eliminates parse warnings
    // seen previously and reduces string concatenation overhead.
    let mut statements: Vec<String> = Vec::new();
    use std::collections::HashMap;
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
        let mut guard = created_set.lock().unwrap();
        for (path, lang) in file_list.iter() {
            let p_lit = serde_json::to_string(path).unwrap();
            let l_lit = serde_json::to_string(lang).unwrap();
            let fid = sanitize_id(path);
            if guard.insert(fid.clone()) {
                // First time we've seen this file in-process: create the row.
                statements.push(format!(
                    "CREATE file:{fid} CONTENT {{\"path\": {p}, \"language\": {l}}};",
                    fid = fid,
                    p = p_lit,
                    l = l_lit
                ));
            } else {
                // Subsequent occurrences: update the existing row.
                statements.push(format!(
                    "UPDATE file:{fid} SET path = {p}, language = {l};",
                    fid = fid,
                    p = p_lit,
                    l = l_lit
                ));
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
    {
        let _guard = created_entities.lock().unwrap();
        for p in acc {
            if let Ok(mut v) = serde_json::to_value(p) {
                // Remove the `file` field from the persisted Entity JSON. The
                // per-file metadata is stored separately in the `file` table.
                if let Some(obj) = v.as_object_mut() {
                    obj.remove("file");
                    // Normalize any import paths to be repo-relative so we don't
                    // persist full clone/absolute paths (e.g. /tmp/hyperzoekt-clones/...).
                    if let Some(imports_val) = obj.get_mut("imports") {
                        if let Some(imports_arr) = imports_val.as_array_mut() {
                            for im in imports_arr.iter_mut() {
                                if let Some(im_obj) = im.as_object_mut() {
                                    if let Some(path_val) =
                                        im_obj.get("path").and_then(|v| v.as_str())
                                    {
                                        let repo_rel =
                                            compute_repo_relative(path_val, &p.repo_name);
                                        im_obj.insert(
                                            "path".to_string(),
                                            serde_json::Value::String(repo_rel),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                let eid = sanitize_id(&p.stable_id);
                let e_json = v.to_string();
                // Use CREATE for deterministic entity ids so the row is actually created
                // when it doesn't yet exist. Duplicate-create errors are tolerated later
                // when executing chunks (they show up as 'already exists').
                statements.push(format!(
                    "CREATE entity:{eid} CONTENT {e};",
                    eid = eid,
                    e = e_json
                ));
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
                        serde_json::Value::Number(serde_json::Number::from(
                            mi.start_line.unwrap_or(0),
                        )),
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
                    let m_json = serde_json::Value::Object(m_obj).to_string();
                    statements.push(format!(
                        "CREATE entity:{mid} CONTENT {mj};",
                        mid = mid,
                        mj = m_json
                    ));
                    // RELATE parent entity -> has_method -> method entity
                    statements.push(format!(
                        "RELATE entity:{eid}->has_method->entity:{mid};",
                        eid = eid,
                        mid = mid
                    ));
                }
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
    if !repo_names.is_empty() {
        // Maintain a global set of created repo ids (lazy static) similar to file creation logic.
        static REPO_IDS_CREATED: std::sync::OnceLock<Mutex<std::collections::HashSet<String>>> =
            std::sync::OnceLock::new();
        let created_repos =
            REPO_IDS_CREATED.get_or_init(|| Mutex::new(std::collections::HashSet::new()));
        let mut repo_guard = created_repos.lock().unwrap();
        for rn in repo_names.iter() {
            let rn_lit = serde_json::to_string(rn).unwrap();
            let rid = sanitize_id(rn);
            if repo_guard.insert(rid.clone()) {
                // Create deterministic repo ids so writer creates the repo rows when missing.
                statements.push(format!(
                    "CREATE repo:{rid} CONTENT {{\"name\": {n}, \"git_url\": \"\", \"visibility\": \"public\", \"allowed_users\": []}};",
                    rid = rid,
                    n = rn_lit
                ));
            } else {
                // Fallback to UPDATE if we've already recorded creation in this process.
                statements.push(format!(
                    "UPDATE repo:{rid} SET name={n}, git_url='', visibility='public', allowed_users=[];",
                    rid = rid,
                    n = rn_lit
                ));
            }
        }
        // repo_guard dropped here
        let global =
            FILE_REPO_EDGE_SEEN.get_or_init(|| Mutex::new(std::collections::HashSet::new()));
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
                    let rid = sanitize_id(ent.repo_name.as_str());
                    if guard.insert((fid.clone(), rid.clone())) {
                        statements.push(format!(
                            "RELATE file:{fid}->in_repo->repo:{rid};",
                            fid = fid,
                            rid = rid
                        ));
                        statements.push(format!(
                            "RELATE repo:{rid}->has_file->file:{fid};",
                            rid = rid,
                            fid = fid
                        ));
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
                    statements.push(format!(
                        "RELATE entity:{src}->calls->entity:{dst};",
                        src = src_eid,
                        dst = dst_eid
                    ));
                }
            }
        }
    }
    (statements, acc.len())
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
