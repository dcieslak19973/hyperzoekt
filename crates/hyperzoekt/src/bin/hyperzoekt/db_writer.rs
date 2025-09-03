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

use anyhow::Result;
use hyperzoekt::repo_index::indexer::payload::EntityPayload;
use log::{error, info, trace, warn};
// use serde_json; // use fully-qualified path where needed
use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread;
use std::time::Duration;
use surrealdb::engine::local::Mem;
use surrealdb::Surreal;

pub type SpawnResult = Result<
    (
        SyncSender<Vec<EntityPayload>>,
        thread::JoinHandle<Result<(), anyhow::Error>>,
    ),
    anyhow::Error,
>;

#[derive(Clone, Debug)]
pub struct DbWriterConfig {
    pub channel_capacity: usize,
    pub batch_capacity: Option<usize>,
    pub batch_timeout_ms: Option<u64>,
    pub max_retries: Option<usize>,
    pub surreal_url: Option<String>,
    pub surreal_ns: String,
    pub surreal_db: String,
    pub initial_batch: bool,
}

pub fn spawn_db_writer(payloads: Vec<EntityPayload>, cfg: DbWriterConfig) -> SpawnResult {
    let (tx, rx) = sync_channel::<Vec<EntityPayload>>(cfg.channel_capacity);
    let payloads_clone = payloads.clone();
    let batch_capacity = cfg.batch_capacity.unwrap_or(500usize);
    let batch_timeout_ms = cfg.batch_timeout_ms.unwrap_or(500);
    let max_retries = cfg.max_retries.unwrap_or(3usize);

    let join = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build runtime");
        rt.block_on(async move {
            let db = {
                if let Some(url) = &cfg.surreal_url {
                    warn!("SURREAL_URL is set ({}), but remote connections are not yet supported in this build; falling back to embedded Mem", url);
                }
                info!("Starting embedded SurrealDB (Mem) namespace={} db={}", cfg.surreal_ns, cfg.surreal_db);
                match Surreal::new::<Mem>(()).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to start embedded SurrealDB: {}", e);
                        return Err::<(), anyhow::Error>(anyhow::anyhow!(e));
                    }
                }
            };
            if let Err(e) = db.use_ns(&cfg.surreal_ns).use_db(&cfg.surreal_db).await {
                error!("use_ns/use_db failed: {}", e);
                return Err::<(), anyhow::Error>(anyhow::anyhow!(e));
            }

            // If initial_batch env var is set, perform a single batch insert of all payloads
            if cfg.initial_batch {
                info!("Initial batch mode: inserting {} entities", payloads_clone.len());
                // Prepare a single CREATE query with array of entities (parameterized)
                let mut vals: Vec<serde_json::Value> = Vec::new();
                for p in payloads_clone.iter() {
                    if let Ok(v) = serde_json::to_value(p) {
                        vals.push(v);
                    }
                }

                // Chunk size derived from config; ensure we don't send a single massive
                // transaction larger than the configured batch_capacity. If total
                // items <= chunk_size we attempt a single parameterized transaction,
                // otherwise we send per-chunk parameterized transactions sized to
                // `chunk_size`.
                let chunk_size = batch_capacity.clamp(1, 5000);

                let mut batches_sent_local: usize = 0;
                let mut entities_sent_local: usize = 0;
                let mut sum_ms: u128 = 0;
                let mut min_ms: Option<u128> = None;
                let mut max_ms: Option<u128> = None;
                for chunk in vals.chunks(chunk_size) {
                    // Inline JSON CREATEs to avoid parser incompatibilities with
                    // parameterized CONTENTS across embedded SurrealDB releases.
                    let mut parts: Vec<String> = Vec::new();
                    for item in chunk.iter() {
                        let e_json = item.to_string();
                        parts.push(format!("CREATE entity CONTENT {};", e_json));
                    }
                    let q_chunk = format!("BEGIN; {} COMMIT;", parts.join(" "));
                    let chunk_start = std::time::Instant::now();
                    match db.query(&q_chunk).await {
                        Ok(_) => {
                            let dur = chunk_start.elapsed();
                            let dur_ms = dur.as_millis();
                            batches_sent_local += 1;
                            entities_sent_local += chunk.len();
                            sum_ms += dur_ms;
                            min_ms = Some(min_ms.map_or(dur_ms, |m| m.min(dur_ms)));
                            max_ms = Some(max_ms.map_or(dur_ms, |m| m.max(dur_ms)));
                            info!("Chunk insert succeeded: size={} duration_ms={}", chunk.len(), dur_ms);
                        }
                        Err(e) => warn!("Chunk insert failed (size={}): {}", chunk.len(), e),
                    }
                }

                // Ensure we write metrics file summarizing the chunked inserts
                let metrics_path = std::env::var("SURREAL_METRICS_FILE").unwrap_or_else(|_| ".data/db_metrics.json".to_string());
                let _ = std::fs::create_dir_all(std::path::Path::new(&metrics_path).parent().unwrap_or(std::path::Path::new(".")));
                let avg_ms = if batches_sent_local > 0 { (sum_ms as f64) / (batches_sent_local as f64) } else { 0.0 };
                let metrics = serde_json::json!({
                    "mode": "initial_batch",
                    "method": "chunked_parameterized",
                    "entities_sent": entities_sent_local,
                    "batches_sent": batches_sent_local,
                    "avg_batch_ms": avg_ms,
                    "total_time_ms": sum_ms,
                    "min_batch_ms": min_ms,
                    "max_batch_ms": max_ms,
                    "note": "completed",
                });
                let _ = std::fs::write(&metrics_path, serde_json::to_string_pretty(&metrics).unwrap_or_else(|_| "{}".to_string()));
                return Ok::<(), anyhow::Error>(());
            }

            // Schema initialization (best-effort)
            let schema_groups: Vec<Vec<&str>> = vec![
                vec!["DEFINE TABLE entity;", "CREATE TABLE entity;"],
                vec!["DEFINE TABLE file;", "CREATE TABLE file;"],
                vec![
                    "DEFINE FIELD stable_id ON entity TYPE string;",
                    "ALTER TABLE entity CREATE FIELD stable_id TYPE string;",
                ],
                vec![
                    "DEFINE INDEX idx_entity_stable_id ON entity COLUMNS stable_id;",
                    "CREATE INDEX idx_entity_stable_id ON entity (stable_id);",
                ],
                vec![
                    "DEFINE INDEX idx_file_path ON file COLUMNS path;",
                    "CREATE INDEX idx_file_path ON file (path);",
                ],
            ];

            for group in schema_groups.iter() {
                let mut applied = false;
                for q in group.iter() {
                    match db.query(*q).await {
                        Ok(_) => {
                            info!("Schema applied: {}", *q);
                            applied = true;
                            break;
                        }
                        Err(e) => {
                            trace!("Schema variant failed: {} -> {}", *q, e);
                        }
                    }
                }
                if !applied {
                    warn!("No schema variant succeeded for group (current+1 fallback): {:?}", group);
                }
            }

            // Batch and retry configuration
            let batch_capacity = batch_capacity;
            let batch_timeout = Duration::from_millis(batch_timeout_ms);
            let max_retries = max_retries;
            let streaming_chunked = std::env::var("SURREAL_STREAM_CHUNKED").ok().as_deref() == Some("1");
            // Simple metrics
            let mut batches_sent: usize = 0;
            let mut entities_sent: usize = 0;
            let mut total_retries: usize = 0;
            // Detailed timing metrics for tuning
            let mut batch_durations_sum_ms: u128 = 0;
            let mut batch_durations_min_ms: Option<u128> = None;
            let mut batch_durations_max_ms: Option<u128> = None;
            let mut batch_failures: usize = 0;
            let mut attempt_counts: std::collections::HashMap<usize, usize> = std::collections::HashMap::new();
            loop {
                // Accumulate a batch, waiting up to `batch_timeout` for first item
                let mut acc: Vec<EntityPayload> = Vec::new();
                match rx.recv_timeout(batch_timeout) {
                    Ok(batch) => {
                        acc.extend(batch);
                        // Drain any immediately-available messages until capacity
                        loop {
                            if acc.len() >= batch_capacity {
                                break;
                            }
                            match rx.try_recv() {
                                Ok(next) => acc.extend(next),
                                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                                Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                            }
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        // No incoming data within timeout; loop again
                        continue;
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        // Channel closed: drain remaining messages via try_recv
                        while let Ok(next) = rx.try_recv() {
                            acc.extend(next);
                        }
                        if acc.is_empty() {
                            break; // nothing left to do
                        }
                    }
                }

                if acc.is_empty() {
                    // Nothing to flush
                    continue;
                }

                // Retry loop for the whole batch
                let mut attempt: usize = 0;
                loop {
                    attempt += 1;

                    // Build a single batched query for files and entities to reduce round trips.
                    let mut q_parts: Vec<String> = Vec::new();

                    // Unique files
                    let mut file_map: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
                    let mut file_list: Vec<(&String, &String)> = Vec::new();
                    for p in acc.iter() {
                        if !file_map.contains_key(p.file.as_str()) {
                            let idx = file_list.len();
                            file_map.insert(p.file.as_str(), idx);
                            file_list.push((&p.file, &p.language));
                        }
                    }
                    for (path, lang) in file_list.iter() {
                        let p_lit = serde_json::to_string(path).unwrap_or_else(|_| format!("\"{}\"", path));
                        let l_lit = serde_json::to_string(lang).unwrap_or_else(|_| format!("\"{}\"", lang));
                        q_parts.push(format!(
                            "UPDATE file SET path = {p}, language = {l} WHERE path = {p}; CREATE file CONTENT {{ path: {p}, language: {l} }};",
                            p = p_lit,
                            l = l_lit
                        ));
                    }

                    // Entities
                    for p in acc.iter() {
                        match serde_json::to_value(p) {
                            Ok(v) => {
                                let e_json = v.to_string();
                                let s_lit = serde_json::to_string(&p.stable_id).unwrap_or_else(|_| format!("\"{}\"", p.stable_id));
                                q_parts.push(format!(
                                    "UPDATE entity CONTENT {e} WHERE stable_id = {s}; CREATE entity CONTENT {e};",
                                    e = e_json,
                                    s = s_lit
                                ));
                            }
                            Err(e) => {
                                warn!("Failed to serialize entity payload for DB: {}", e);
                                // skip this entity's DB work but continue batching others
                                continue;
                            }
                        }
                    }

                    if q_parts.is_empty() {
                        // nothing to send
                        batches_sent += 1;
                        entities_sent += acc.len();
                        break;
                    }

                    if streaming_chunked {
                        // When enabled, ensure file-related updates are applied
                        // first (so files exist), then split the entity payloads
                        // into chunks of `batch_capacity` and send inline CREATE
                        // transactions per-chunk. This mirrors the initial-batch
                        // chunking and ensures we actually produce multiple
                        // DB requests for large `acc` sizes.
                        // First, apply the file updates (if any) derived from q_parts
                        let mut file_qs: Vec<String> = Vec::new();
                        for part in q_parts.iter() {
                            if part.starts_with("UPDATE file") {
                                file_qs.push(part.clone());
                            }
                        }
                        if !file_qs.is_empty() {
                            let q_files = file_qs.join(" ");
                            let file_start = std::time::Instant::now();
                            match db.query(&q_files).await {
                                Ok(_) => {
                                    let dur = file_start.elapsed();
                                    let dur_ms = dur.as_millis();
                                    batches_sent += 1;
                                    batch_durations_sum_ms += dur_ms;
                                    batch_durations_min_ms = Some(batch_durations_min_ms.map_or(dur_ms, |m| m.min(dur_ms)));
                                    batch_durations_max_ms = Some(batch_durations_max_ms.map_or(dur_ms, |m| m.max(dur_ms)));
                                    info!("File-update batch applied: parts={} duration_ms={}", file_qs.len(), dur_ms);
                                }
                                Err(e) => {
                                    warn!("File-update batch failed: {}", e);
                                }
                            }
                        }

                        // Then send entity CREATEs in chunks
                        let mut local_batches = 0usize;
                        let mut local_entities = 0usize;
                        let mut local_sum_ms: u128 = 0;
                        let mut local_min_ms: Option<u128> = None;
                        let mut local_max_ms: Option<u128> = None;
                        for chunk in acc.chunks(batch_capacity) {
                            let mut parts: Vec<String> = Vec::new();
                            for p in chunk.iter() {
                                match serde_json::to_value(p) {
                                    Ok(v) => {
                                        let e_json = v.to_string();
                                        parts.push(format!("CREATE entity CONTENT {};", e_json));
                                    }
                                    Err(e) => {
                                        warn!("Failed to serialize entity payload for chunked streaming: {}", e);
                                    }
                                }
                            }
                            if parts.is_empty() {
                                continue;
                            }
                            let q_chunk = format!("BEGIN; {} COMMIT;", parts.join(" "));
                            let chunk_start = std::time::Instant::now();
                            match db.query(&q_chunk).await {
                                Ok(_) => {
                                    let dur = chunk_start.elapsed();
                                    let dur_ms = dur.as_millis();
                                    local_batches += 1;
                                    local_entities += chunk.len();
                                    local_sum_ms += dur_ms;
                                    local_min_ms = Some(local_min_ms.map_or(dur_ms, |m| m.min(dur_ms)));
                                    local_max_ms = Some(local_max_ms.map_or(dur_ms, |m| m.max(dur_ms)));
                                    info!("Chunked streaming insert succeeded: size={} duration_ms={}", chunk.len(), dur_ms);
                                }
                                Err(e) => {
                                    let dur = chunk_start.elapsed();
                                    let dur_ms = dur.as_millis();
                                    batch_failures += 1;
                                    warn!("Chunked streaming insert failed (size={} duration_ms={}): {}", chunk.len(), dur_ms, e);
                                }
                            }
                        }
                        if local_batches > 0 {
                            batches_sent += local_batches;
                            entities_sent += local_entities;
                            batch_durations_sum_ms += local_sum_ms;
                            batch_durations_min_ms = Some(batch_durations_min_ms.map_or(local_min_ms.unwrap_or(0), |m| m.min(local_min_ms.unwrap_or(m))));
                            batch_durations_max_ms = Some(batch_durations_max_ms.map_or(local_max_ms.unwrap_or(0), |m| m.max(local_max_ms.unwrap_or(m))));
                            *attempt_counts.entry(attempt).or_insert(0) += 1;
                            info!("Streaming (chunked) sent: size={} batches={} attempt={}", local_entities, local_batches, attempt);
                        }
                        break;
                    } else {
                        let q_all = q_parts.join(" ");

                        let batch_start = std::time::Instant::now();
                        match db.query(&q_all).await {
                            Ok(_) => {
                                let dur = batch_start.elapsed();
                                let dur_ms = dur.as_millis();
                                batches_sent += 1;
                                entities_sent += acc.len();
                                batch_durations_sum_ms += dur_ms;
                                batch_durations_min_ms = Some(batch_durations_min_ms.map_or(dur_ms, |m| m.min(dur_ms)));
                                batch_durations_max_ms = Some(batch_durations_max_ms.map_or(dur_ms, |m| m.max(dur_ms)));
                                *attempt_counts.entry(attempt).or_insert(0) += 1;
                                info!("Batch sent: size={} duration_ms={} attempt={}", acc.len(), dur_ms, attempt);
                                break;
                            }
                            Err(e) => {
                                let dur = batch_start.elapsed();
                                let dur_ms = dur.as_millis();
                                batch_failures += 1;
                                warn!("Batched DB write failed (size={} duration_ms={}): {}", acc.len(), dur_ms, e);
                            }
                        }
                    }

                    // failed
                    if attempt >= max_retries {
                        error!("Batch failed after {} attempts, dropping {} entities", attempt, acc.len());
                        total_retries += attempt - 1;
                        *attempt_counts.entry(attempt).or_insert(0) += 1;
                        break;
                    }
                    total_retries += 1;
                    let backoff = Duration::from_millis(100 * (1 << (attempt - 1)).min(8));
                    warn!("Retrying batch in {:?} (attempt {}/{})", backoff, attempt, max_retries);
                    std::thread::sleep(backoff);
                }

                if batches_sent % 10 == 0 && batches_sent > 0 {
                    let avg = if batches_sent > 0 {
                        (batch_durations_sum_ms as f64) / (batches_sent as f64)
                    } else {
                        0.0
                    };
                    info!(
                        "DB metrics: batches_sent={} entities_sent={} total_retries={} avg_batch_ms={:.2} min_ms={:?} max_ms={:?} failures={} attempts={:?}",
                        batches_sent,
                        entities_sent,
                        total_retries,
                        avg,
                        batch_durations_min_ms,
                        batch_durations_max_ms,
                        batch_failures,
                        attempt_counts
                    );
                }
            }
            // Write metrics file on shutdown for offline analysis
            let metrics_path = std::env::var("SURREAL_METRICS_FILE").unwrap_or_else(|_| ".data/db_metrics.json".to_string());
            let metrics = serde_json::json!({
                "batches_sent": batches_sent,
                "entities_sent": entities_sent,
                "total_retries": total_retries,
                "avg_batch_ms": if batches_sent>0 { (batch_durations_sum_ms as f64)/(batches_sent as f64) } else { 0.0 },
                "total_time_ms": batch_durations_sum_ms,
                "min_batch_ms": batch_durations_min_ms,
                "max_batch_ms": batch_durations_max_ms,
                "batch_failures": batch_failures,
                "attempt_counts": attempt_counts,
            });
            if let Some(parent) = std::path::Path::new(&metrics_path).parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Ok(s) = serde_json::to_string_pretty(&metrics) {
                if let Err(e) = std::fs::write(&metrics_path, s) {
                    warn!("Failed to write DB metrics file {}: {}", metrics_path, e);
                } else {
                    info!("Wrote DB metrics to {}", metrics_path);
                }
            }
            Ok::<(), anyhow::Error>(())
        })
    });

    Ok((tx, join))
}
