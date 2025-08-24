// ...existing code...

use clap::Parser;
use hyperzoekt::service::RepoIndexService;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, sync_channel, RecvTimeoutError, TryRecvError};
// Arc/Mutex previously considered but not needed
use std::collections::HashSet;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use log::{error, info, warn};
use sha2::Digest;

// Typed in-process payloads to avoid JSON string churn
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImportItem {
    path: String,
    line: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UnresolvedImport {
    module: String,
    line: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EntityPayload {
    file: String,
    language: String,
    kind: String,
    name: String,
    parent: Option<String>,
    signature: String,
    start_line: Option<u32>,
    end_line: Option<u32>,
    calls: Vec<String>,
    doc: Option<String>,
    rank: f32,
    imports: Vec<ImportItem>,
    unresolved_imports: Vec<UnresolvedImport>,
    stable_id: String,
}

// Tokio + SurrealDB for direct loading (embedded Mem used by default).
use surrealdb::engine::local::Mem;
use surrealdb::Surreal;

// File system watcher
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

/// Simple repo indexer that writes one JSON object per line (JSONL).
#[derive(Parser)]
struct Args {
    /// Optional config file path (TOML). Defaults to .hyperzoekt.toml in cwd.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Optional root path to override config
    root: Option<PathBuf>,
    /// Optional output path to override config (JSONL debug output)
    #[arg(long, short = 'o')]
    out: Option<PathBuf>,
    /// Positional legacy output path (kept for backward compatibility with older tests)
    out_pos: Option<PathBuf>,
    /// Run incremental JSONL writer path (write JSONL and exit)
    #[arg(long)]
    incremental: bool,
    /// Run in debug mode (one-shot send to DB or used by tests)
    #[arg(long)]
    debug: bool,
}

#[derive(Debug, Deserialize)]
struct AppConfig {
    // Repo root to index
    root: Option<PathBuf>,
    // Output file for debug JSONL
    out: Option<PathBuf>,
    // Incremental mode
    incremental: Option<bool>,
    // Debug mode toggles JSONL output instead of streaming to DB
    debug: Option<bool>,

    // Streaming and ingestion tuning
    channel_capacity: Option<usize>,
    debounce_ms: Option<u64>,
    batch_capacity: Option<usize>,
    batch_timeout_ms: Option<u64>,
    max_retries: Option<usize>,
}

impl AppConfig {
    fn load(path: Option<&PathBuf>) -> Result<(Self, PathBuf), anyhow::Error> {
        let cfg_path = path
            .cloned()
            .unwrap_or_else(|| PathBuf::from("crates/hyperzoekt/hyperzoekt.toml"));
        if cfg_path.exists() {
            let s = std::fs::read_to_string(&cfg_path)?;
            let cfg: AppConfig = toml::from_str(&s)?;
            Ok((cfg, cfg_path))
        } else {
            Ok((
                AppConfig {
                    root: None,
                    out: None,
                    incremental: None,
                    debug: None,
                    channel_capacity: None,
                    debounce_ms: None,
                    batch_capacity: None,
                    batch_timeout_ms: None,
                    max_retries: None,
                },
                cfg_path,
            ))
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    // Initialize logging (default to info if RUST_LOG is not set)
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::Builder::from_env(env).init();
    let (app_cfg, cfg_path) = AppConfig::load(args.config.as_ref())?;
    info!("Loaded config from {}", cfg_path.display());

    // Determine effective root: CLI root overrides config, which defaults to args.root required earlier
    let effective_root = args
        .root
        .as_ref()
        .cloned()
        .or_else(|| app_cfg.root.clone())
        .unwrap_or_else(|| PathBuf::from("."));
    let out_dir = PathBuf::from(".data");
    let _ = std::fs::create_dir_all(&out_dir);
    // canonical output filename when the user doesn't provide one
    let default_out = out_dir.join("bin_integration_out.jsonl");

    // Determine effective output path: CLI flag overrides config, otherwise config overrides default
    let effective_out: PathBuf = args
        .out_pos
        .as_ref()
        .cloned()
        .or_else(|| args.out.as_ref().cloned())
        .or_else(|| app_cfg.out.clone())
        .unwrap_or(default_out.clone());

    // Index all detected languages by default

    // CLI flags take precedence over config
    if args.incremental || app_cfg.incremental.unwrap_or(false) {
        // Stream results by passing a writer to options
        let out_path = effective_out.as_path();
        let mut file_writer = BufWriter::new(File::create(out_path)?);
        let mut opts_builder = hyperzoekt::internal::RepoIndexOptions::builder();
        opts_builder = opts_builder.root(&effective_root);
        // If incremental + debug, stream to file; otherwise stream to stdout (or to DB later)
        let opts = opts_builder.output_writer(&mut file_writer).build();
        let (_svc, stats) = hyperzoekt::service::RepoIndexService::build_with_options(opts)?;
        // flush just in case
        file_writer.flush()?;
        println!(
            "Indexed {} files, {} entities in {:.3}s",
            stats.files_indexed,
            stats.entities_indexed,
            stats.duration.as_secs_f64()
        );
        return Ok(());
    }
    // Removed include_langs processing, indexing all detected languages by default

    // Non-incremental: build in-memory and then either write JSONL (--debug)
    // or stream directly into SurrealDB.
    let opts = hyperzoekt::internal::RepoIndexOptions::builder()
        .root(&effective_root)
        .output_null()
        .build();
    info!("Starting index build for {}", effective_root.display());
    let build_start = std::time::Instant::now();
    let (svc, stats) = RepoIndexService::build_with_options(opts)?;
    info!(
        "Index build finished in {:.2}s (files_indexed={}, entities_indexed={})",
        build_start.elapsed().as_secs_f64(),
        stats.files_indexed,
        stats.entities_indexed
    );
    let out_path = effective_out.as_path();

    // If debug is set (CLI or config), dump JSONL to disk and exit. Otherwise stream to DB.
    if args.debug || app_cfg.debug.unwrap_or(false) {
        let mut writer = BufWriter::new(File::create(out_path)?);
        for ent in &svc.entities {
            let file = &svc.files[ent.file_id as usize];
            // Do not clone payloads for the DB thread; main will send payloads when needed
            let mut imports: Vec<serde_json::Value> = Vec::new();
            let mut unresolved_imports: Vec<serde_json::Value> = Vec::new();
            if matches!(ent.kind, hyperzoekt::internal::EntityKind::File) {
                // import_edges stores target entity ids (file pseudo-entity ids)
                if let Some(edge_list) = svc.import_edges.get(ent.id as usize) {
                    let lines = svc.import_lines.get(ent.id as usize);
                    for (i, &target_eid) in edge_list.iter().enumerate() {
                        if let Some(target_ent) = svc.entities.get(target_eid as usize) {
                            let target_file_idx = target_ent.file_id as usize;
                            if let Some(target_file) = svc.files.get(target_file_idx) {
                                let line_no = lines
                                    .and_then(|l| l.get(i))
                                    .cloned()
                                    .unwrap_or(0)
                                    .saturating_add(1);
                                imports.push(json!({"path": target_file.path, "line": line_no}));
                            }
                        }
                    }
                }
                // unresolved imports are stored per file index as (module,line)
                if let Some(unres) = svc.unresolved_imports.get(ent.file_id as usize) {
                    for (m, lineno) in unres {
                        unresolved_imports
                            .push(json!({"module": m, "line": lineno.saturating_add(1)}));
                    }
                }
            }

            // decide whether to emit start/end lines. For file pseudo-entities we only
            // emit numeric 1-based start/end if there are import lines or unresolved
            // imports recorded; otherwise emit null to avoid misleading 1/1 values.
            let (start_field, end_field) =
                if matches!(ent.kind, hyperzoekt::internal::EntityKind::File) {
                    let has_imports = !imports.is_empty();
                    let has_unresolved = !unresolved_imports.is_empty();
                    if has_imports || has_unresolved {
                        (
                            serde_json::Value::from(ent.start_line.saturating_add(1)),
                            serde_json::Value::from(ent.end_line.saturating_add(1)),
                        )
                    } else {
                        (serde_json::Value::Null, serde_json::Value::Null)
                    }
                } else {
                    (
                        serde_json::Value::from(ent.start_line.saturating_add(1)),
                        serde_json::Value::from(ent.end_line.saturating_add(1)),
                    )
                };

            let obj = json!({
                "file": file.path,
                "language": file.language,
                "kind": ent.kind.as_str(),
                "name": ent.name,
                "parent": ent.parent,
                "signature": ent.signature,
                "start_line": start_field,
                "end_line": end_field,
                "calls": ent.calls,
                "doc": ent.doc,
                "rank": ent.rank,
                "imports": imports,
                "unresolved_imports": unresolved_imports,
            });
            writeln!(writer, "{}", obj)?;
        }
        writer.flush()?;
        println!(
            "Wrote {} entities to {}",
            svc.entities.len(),
            out_path.display()
        );
        println!(
            "Indexed {} files, {} entities in {:.3}s",
            stats.files_indexed,
            stats.entities_indexed,
            stats.duration.as_secs_f64()
        );
        return Ok(());
    }

    // If SURREAL_INITIAL_BATCH=1, perform a single batch insert into the DB
    let initial_batch = std::env::var("SURREAL_INITIAL_BATCH").ok().as_deref() == Some("1");

    // Streaming path: start an embedded SurrealDB (Mem) unless SURREAL_URL is set.
    // We'll spawn a tokio runtime on a new thread to run async upserts so the
    // synchronous indexer can stay unchanged.
    let surreal_url = std::env::var("SURREAL_URL").ok();
    let surreal_ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "test".into());
    let surreal_db = std::env::var("SURREAL_DB").unwrap_or_else(|_| "test".into());

    // Build typed EntityPayloads to send to the DB task
    let payloads: Vec<EntityPayload> = svc
        .entities
        .iter()
        .map(|ent| {
            let file = &svc.files[ent.file_id as usize];
            let mut imports: Vec<ImportItem> = Vec::new();
            let mut unresolved_imports: Vec<UnresolvedImport> = Vec::new();
            if matches!(ent.kind, hyperzoekt::internal::EntityKind::File) {
                if let Some(edge_list) = svc.import_edges.get(ent.id as usize) {
                    let lines = svc.import_lines.get(ent.id as usize);
                    for (i, &target_eid) in edge_list.iter().enumerate() {
                        if let Some(target_ent) = svc.entities.get(target_eid as usize) {
                            let target_file_idx = target_ent.file_id as usize;
                            if let Some(target_file) = svc.files.get(target_file_idx) {
                                let line_no = lines
                                    .and_then(|l| l.get(i))
                                    .cloned()
                                    .unwrap_or(0)
                                    .saturating_add(1);
                                imports.push(ImportItem {
                                    path: target_file.path.clone(),
                                    line: line_no,
                                });
                            }
                        }
                    }
                }
                if let Some(unres) = svc.unresolved_imports.get(ent.file_id as usize) {
                    for (m, lineno) in unres {
                        unresolved_imports.push(UnresolvedImport {
                            module: m.clone(),
                            line: lineno.saturating_add(1),
                        });
                    }
                }
            }
            let (start_field, end_field) =
                if matches!(ent.kind, hyperzoekt::internal::EntityKind::File) {
                    let has_imports = !imports.is_empty();
                    let has_unresolved = !unresolved_imports.is_empty();
                    if has_imports || has_unresolved {
                        (
                            Some(ent.start_line.saturating_add(1)),
                            Some(ent.end_line.saturating_add(1)),
                        )
                    } else {
                        (None, None)
                    }
                } else {
                    (
                        Some(ent.start_line.saturating_add(1)),
                        Some(ent.end_line.saturating_add(1)),
                    )
                };

            // Compute stable id from environment and entity fields
            let project =
                std::env::var("SURREAL_PROJECT").unwrap_or_else(|_| "local-project".into());
            let repo = std::env::var("SURREAL_REPO").unwrap_or_else(|_| {
                std::path::Path::new(&effective_root)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("local-repo")
                    .to_string()
            });
            let branch = std::env::var("SURREAL_BRANCH").unwrap_or_else(|_| "local-branch".into());
            let commit = std::env::var("SURREAL_COMMIT").unwrap_or_else(|_| "local-commit".into());
            let key = format!(
                "{}|{}|{}|{}|{}|{}|{}",
                project, repo, branch, commit, file.path, ent.name, ent.signature
            );
            let mut hasher = sha2::Sha256::new();
            hasher.update(key.as_bytes());
            let stable_id = format!("{:x}", hasher.finalize());

            EntityPayload {
                file: file.path.clone(),
                language: file.language.clone(),
                kind: ent.kind.as_str().to_string(),
                name: ent.name.clone(),
                parent: ent.parent.clone(),
                signature: ent.signature.clone(),
                start_line: start_field,
                end_line: end_field,
                calls: ent.calls.clone(),
                doc: ent.doc.clone(),
                rank: ent.rank,
                imports,
                unresolved_imports,
                stable_id,
            }
        })
        .collect();

    // If initial_batch is requested, perform per-record parameterized inserts
    // from the main thread using a small tokio runtime. This avoids the
    // SurrealQL $items parameter parsing issues and ensures the process exits
    // after completing the initial load.
    if initial_batch {
        info!(
            "Initial batch mode (main): inserting {} entities",
            payloads.len()
        );
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build runtime");
        let surreal_ns = surreal_ns.clone();
        let surreal_db = surreal_db.clone();
        let metrics_path = std::env::var("SURREAL_METRICS_FILE")
            .unwrap_or_else(|_| ".data/db_metrics.json".to_string());
        let entities_count = payloads.len();
        let start = std::time::Instant::now();
        let res: Result<(), anyhow::Error> = rt.block_on(async move {
            let db = match Surreal::new::<Mem>(()).await {
                Ok(s) => s,
                Err(e) => return Err(anyhow::anyhow!(e)),
            };
            db.use_ns(&surreal_ns)
                .use_db(&surreal_db)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            for p in payloads.into_iter() {
                if let Ok(v) = serde_json::to_value(p) {
                    let q = "CREATE entity CONTENT $e";
                    db.query(q)
                        .bind(("e", v))
                        .await
                        .map_err(|e| anyhow::anyhow!(e))?;
                }
            }
            Ok(())
        });
        let dur = start.elapsed();
        let dur_ms = dur.as_millis();
        let metrics = if res.is_ok() {
            serde_json::json!({
                "mode": "initial_batch",
                "method": "per_record_parameterized",
                "entities_sent": entities_count,
                "batches_sent": 1,
                "avg_batch_ms": dur_ms,
                "min_batch_ms": dur_ms,
                "max_batch_ms": dur_ms,
                "note": "success",
            })
        } else {
            serde_json::json!({
                "mode": "initial_batch",
                "method": "per_record_parameterized",
                "entities_sent": entities_count,
                "error": format!("{:?}", res.err()),
            })
        };
        if let Some(parent) = std::path::Path::new(&metrics_path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(
            &metrics_path,
            serde_json::to_string_pretty(&metrics).unwrap_or_else(|_| "{}".to_string()),
        );
        info!("Wrote initial-batch metrics to {}", metrics_path);
        return Ok(());
    }

    // We'll run a DB task on a background thread and send payloads to it via
    // a bounded MPSC channel. This keeps the watcher and indexer synchronous
    // while preventing unbounded memory usage under load.
    let channel_capacity = app_cfg.channel_capacity.unwrap_or(100usize);
    let (tx, rx) = sync_channel::<Vec<EntityPayload>>(channel_capacity);
    // Clone payloads for the DB thread so the main thread can still use the original
    let payloads_clone = payloads.clone();
    // Setup a channel for metrics-dump signals (SIGUSR1). This allows dumping
    // metrics on demand without shutting down the process.
    let (metrics_signal_tx, metrics_signal_rx) = std::sync::mpsc::channel::<()>();
    // Register SIGUSR1 handler (best-effort); spawn a thread to forward the
    // signal into our metrics channel so the DB thread can react.
    if let Ok(mut signals) = signal_hook::iterator::Signals::new([signal_hook::consts::SIGUSR1]) {
        let tx_clone2 = metrics_signal_tx.clone();
        thread::spawn(move || {
            for _sig in signals.forever() {
                // best-effort: ignore send errors
                let _ = tx_clone2.send(());
            }
        });
    }

    let db_join = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build runtime");
        rt.block_on(async move {
            let db = {
                if let Some(url) = &surreal_url {
                    warn!("SURREAL_URL is set ({}), but remote connections are not yet supported in this build; falling back to embedded Mem", url);
                }
                info!("Starting embedded SurrealDB (Mem) namespace={} db={}", surreal_ns, surreal_db);
                match Surreal::new::<Mem>(()).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to start embedded SurrealDB: {}", e);
                        return Err::<(), anyhow::Error>(anyhow::anyhow!("{}", e));
                    }
                }
            };
            if let Err(e) = db.use_ns(&surreal_ns).use_db(&surreal_db).await {
                error!("use_ns/use_db failed: {}", e);
                return Err::<(), anyhow::Error>(anyhow::anyhow!(e));
            }

            // If initial_batch env var is set, perform a single batch insert of all payloads
            if initial_batch {
                info!("Initial batch mode: inserting {} entities", payloads_clone.len());
                // Prepare a single CREATE query with array of entities (parameterized)
                let mut vals: Vec<serde_json::Value> = Vec::new();
                for p in payloads_clone.iter() {
                    if let Ok(v) = serde_json::to_value(p) {
                        vals.push(v);
                    }
                }

                // Try preferred: single-transaction parameterized batch
                let items = serde_json::Value::Array(vals.clone());
                let q_param = "BEGIN; CREATE entity CONTENTS $items; COMMIT;";
                let ib_start = std::time::Instant::now();
                match db.query(q_param).bind(("items", items.clone())).await {
                    Ok(_) => {
                        let dur = ib_start.elapsed();
                        let dur_ms = dur.as_millis();
                        info!("Initial batch insert succeeded (parameterized) duration_ms={}", dur_ms);
                        // write a concise metrics snapshot for initial batch success
                        let metrics_path = std::env::var("SURREAL_METRICS_FILE").unwrap_or_else(|_| ".data/db_metrics.json".to_string());
                        if let Some(parent) = std::path::Path::new(&metrics_path).parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        let metrics = serde_json::json!({
                            "mode": "initial_batch",
                            "method": "parameterized_single_transaction",
                            "entities_sent": vals.len(),
                            "batches_sent": 1,
                            "avg_batch_ms": dur_ms,
                            "min_batch_ms": dur_ms,
                            "max_batch_ms": dur_ms,
                            "note": "success",
                        });
                        if let Ok(s) = serde_json::to_string_pretty(&metrics) {
                            let _ = std::fs::write(&metrics_path, s);
                            info!("Wrote initial-batch metrics to {}", metrics_path);
                        }
                        return Ok::<(), anyhow::Error>(());
                    }
                    Err(e) => {
                        warn!("Parameterized initial batch failed: {}. Falling back to chunked inserts", e);
                        // Fall through to chunked per-record parameterized INSERTs below
                    }
                }

                // Fallback: chunk into smaller batches and insert parameterized per-chunk
                let chunk_size = app_cfg.batch_capacity.unwrap_or(500usize).clamp(1, 1000);
                for chunk in vals.chunks(chunk_size) {
                    let chunk_items = serde_json::Value::Array(chunk.to_vec());
                    let q_chunk = "BEGIN; CREATE entity CONTENTS $items; COMMIT;";
                    match db.query(q_chunk).bind(("items", chunk_items)).await {
                        Ok(_) => info!("Chunk insert succeeded: size={}", chunk.len()),
                        Err(e) => warn!("Chunk insert failed (size={}): {}", chunk.len(), e),
                    }
                }

                // Ensure we write metrics file even when the initial_batch path returns early
                let metrics_path = std::env::var("SURREAL_METRICS_FILE").unwrap_or_else(|_| ".data/db_metrics.json".to_string());
                let _ = std::fs::create_dir_all(std::path::Path::new(&metrics_path).parent().unwrap_or(std::path::Path::new(".")));
                let metrics = serde_json::json!({
                    "batches_sent": 0,
                    "entities_sent": vals.len(),
                    "note": "initial_batch completed",
                });
                let _ = std::fs::write(&metrics_path, serde_json::to_string_pretty(&metrics).unwrap_or_else(|_| "{}".to_string()));
                return Ok::<(), anyhow::Error>(());
            }

            // Schema initialization (best-effort, try several DDL variants to
            // support different SurrealDB releases, including the latest).
            // For each logical schema change we try alternatives and stop when
            // one succeeds. Failures are logged but do not abort startup.
            // Schema initialization optimized for the current SurrealDB
            // release: prefer CREATE-style DDL first, with a single fallback
            // for older DEFINE/ALTER forms. We only keep two variants per
            // logical item to support the current + one previous version.
            let schema_groups: Vec<Vec<&str>> = vec![
                // Prefer CREATE TABLE (newer) then fallback to DEFINE TABLE
                vec!["CREATE TABLE entity;", "DEFINE TABLE entity;"],
                vec!["CREATE TABLE file;", "DEFINE TABLE file;"],
                // Prefer CREATE/ALTER-style field creation then fallback to DEFINE FIELD
                vec![
                    "ALTER TABLE entity CREATE FIELD stable_id TYPE string;",
                    "DEFINE FIELD stable_id ON entity TYPE string;",
                ],
                // Prefer CREATE INDEX then fallback to DEFINE INDEX
                vec![
                    "CREATE INDEX idx_entity_stable_id ON entity (stable_id);",
                    "DEFINE INDEX idx_entity_stable_id ON entity COLUMNS stable_id;",
                ],
                vec![
                    "CREATE INDEX idx_file_path ON file (path);",
                    "DEFINE INDEX idx_file_path ON file COLUMNS path;",
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
                            // Try next variant; only log at debug/trace level would be ideal,
                            // but keep warn so operators can see compatibility issues.
                            warn!("Schema variant failed: {} -> {}", *q, e);
                        }
                    }
                }
                if !applied {
                    warn!("No schema variant succeeded for group (current+1 fallback): {:?}", group);
                }
            }

            // Batch and retry configuration
            let batch_capacity: usize = app_cfg.batch_capacity.unwrap_or(500usize);
            let batch_timeout = Duration::from_millis(app_cfg.batch_timeout_ms.unwrap_or(500));
            let max_retries: usize = app_cfg.max_retries.unwrap_or(3usize);

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
                // Check for metrics-dump signal without blocking the async runtime
                if metrics_signal_rx.try_recv().is_ok() {
                    // Compose current metrics snapshot and write to file
                    let metrics_path = std::env::var("SURREAL_METRICS_FILE").unwrap_or_else(|_| ".data/db_metrics.json".to_string());
                    if let Some(parent) = std::path::Path::new(&metrics_path).parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    let metrics = serde_json::json!({
                        "batches_sent": batches_sent,
                        "entities_sent": entities_sent,
                        "total_retries": total_retries,
                        "avg_batch_ms": if batches_sent>0 { (batch_durations_sum_ms as f64)/(batches_sent as f64) } else { 0.0 },
                        "min_batch_ms": batch_durations_min_ms,
                        "max_batch_ms": batch_durations_max_ms,
                        "batch_failures": batch_failures,
                        "attempt_counts": attempt_counts,
                    });
                    let _ = std::fs::write(&metrics_path, serde_json::to_string_pretty(&metrics).unwrap_or_else(|_| "{}".to_string()));
                    info!("Metrics dumped to {} due to signal", std::env::var("SURREAL_METRICS_FILE").unwrap_or_else(|_| ".data/db_metrics.json".to_string()));
                }
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
                                Err(TryRecvError::Empty) => break,
                                Err(TryRecvError::Disconnected) => break,
                            }
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        // No incoming data within timeout; loop again
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
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
                    // For each unique file: UPDATE ...; CREATE ... IF NONE;
                    // For each entity: UPDATE entity CONTENT $eN WHERE stable_id = $sN; CREATE entity CONTENT $eN IF NONE;
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
                            "UPDATE file SET path = {p}, language = {l} WHERE path = {p}; CREATE file CONTENT {{ path: {p}, language: {l} }} IF NONE;",
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
                                    "UPDATE entity CONTENT {e} WHERE stable_id = {s}; CREATE entity CONTENT {e} IF NONE;",
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
                            // will retry below
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
                    sleep(backoff);
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

    // If we're running as daemon/watch mode, start a filesystem watcher that
    // reindexes modified files and sends payloads to the DB channel. Otherwise
    // we fall back to the one-shot payloads previously built.
    if app_cfg.debug.unwrap_or(false) {
        // One-shot: send existing payloads then exit
        // Send the full batch as a single message to the DB thread
        tx.send(payloads).ok();
        drop(tx); // close channel
        match db_join.join() {
            Ok(Ok(())) => println!("Streaming import finished"),
            Ok(Err(e)) => error!("DB task failed: {}", e),
            Err(e) => error!("DB thread panicked: {:?}", e),
        }
        return Ok(());
    }

    // Watch mode: monitor the repository root for changes and reindex changed files.
    let root = effective_root.clone();
    let tx_clone = tx.clone();

    let (watch_tx, watch_rx) = channel();
    let mut watcher: RecommendedWatcher = RecommendedWatcher::new(
        move |res: notify::Result<Event>| match res {
            Ok(ev) => {
                let _ = watch_tx.send(ev);
            }
            Err(e) => warn!("watch error: {}", e),
        },
        Config::default(),
    )?;
    watcher.watch(&root, RecursiveMode::Recursive)?;

    info!("Watching {} for changes...", root.display());

    // Process watch events with debounce: collect modified files for a short
    // window and then reindex them in a single batch to avoid duplicate work.
    let debounce_window = Duration::from_millis(app_cfg.debounce_ms.unwrap_or(200));
    while let Ok(first_event) = watch_rx.recv() {
        let mut changed: HashSet<PathBuf> = HashSet::new();
        // include the first event
        match first_event.kind {
            EventKind::Modify(_) | EventKind::Create(_) => {
                for path in first_event.paths.iter() {
                    if path.is_file() {
                        changed.insert(path.clone());
                    }
                }
            }
            _ => {}
        }

        // Drain additional events for debounce_window
        let start = std::time::Instant::now();
        while start.elapsed() < debounce_window {
            match watch_rx.recv_timeout(debounce_window - start.elapsed()) {
                Ok(ev) => match ev.kind {
                    EventKind::Modify(_) | EventKind::Create(_) => {
                        for path in ev.paths.iter() {
                            if path.is_file() {
                                changed.insert(path.clone());
                            }
                        }
                    }
                    _ => {}
                },
                Err(RecvTimeoutError::Timeout) => break,
                Err(_) => break,
            }
        }

        if changed.is_empty() {
            continue;
        }

        // Re-index each unique file and send batches per file
        for path in changed.into_iter() {
            if let Ok((file_payloads, _stats)) = index_single_file(&path) {
                tx_clone.send(file_payloads).ok();
            }
        }
    }

    drop(tx);
    match db_join.join() {
        Ok(Ok(())) => println!("Streaming import finished"),
        Ok(Err(e)) => error!("DB task failed: {}", e),
        Err(e) => error!("DB thread panicked: {:?}", e),
    }

    Ok(())
}

// Index a single file and return typed payloads representing its entities.
fn index_single_file(
    path: &Path,
) -> Result<(Vec<EntityPayload>, hyperzoekt::internal::RepoIndexStats), anyhow::Error> {
    // Build options that target the single file via root and incremental writer
    let mut opts_builder = hyperzoekt::internal::RepoIndexOptions::builder();
    opts_builder = opts_builder.root(path);
    // Use output_null as we will collect entities via the returned service
    let opts = opts_builder.output_null().build();
    let (svc, stats) = RepoIndexService::build_with_options(opts)?;
    let mut payloads: Vec<EntityPayload> = Vec::new();
    for ent in svc.entities.iter() {
        let file = &svc.files[ent.file_id as usize];
        let mut imports: Vec<ImportItem> = Vec::new();
        let mut unresolved_imports: Vec<UnresolvedImport> = Vec::new();
        if matches!(ent.kind, hyperzoekt::internal::EntityKind::File) {
            if let Some(edge_list) = svc.import_edges.get(ent.id as usize) {
                let lines = svc.import_lines.get(ent.id as usize);
                for (i, &target_eid) in edge_list.iter().enumerate() {
                    if let Some(target_ent) = svc.entities.get(target_eid as usize) {
                        let target_file_idx = target_ent.file_id as usize;
                        if let Some(target_file) = svc.files.get(target_file_idx) {
                            let line_no = lines
                                .and_then(|l| l.get(i))
                                .cloned()
                                .unwrap_or(0)
                                .saturating_add(1);
                            imports.push(ImportItem {
                                path: target_file.path.clone(),
                                line: line_no,
                            });
                        }
                    }
                }
            }
            if let Some(unres) = svc.unresolved_imports.get(ent.file_id as usize) {
                for (m, lineno) in unres {
                    unresolved_imports.push(UnresolvedImport {
                        module: m.clone(),
                        line: lineno.saturating_add(1),
                    });
                }
            }
        }
        let (start_field, end_field) = if matches!(ent.kind, hyperzoekt::internal::EntityKind::File)
        {
            let has_imports = !imports.is_empty();
            let has_unresolved = !unresolved_imports.is_empty();
            if has_imports || has_unresolved {
                (
                    Some(ent.start_line.saturating_add(1)),
                    Some(ent.end_line.saturating_add(1)),
                )
            } else {
                (None, None)
            }
        } else {
            (
                Some(ent.start_line.saturating_add(1)),
                Some(ent.end_line.saturating_add(1)),
            )
        };
        // compute stable id
        let project = std::env::var("SURREAL_PROJECT").unwrap_or_else(|_| "local-project".into());
        let repo = std::env::var("SURREAL_REPO").unwrap_or_else(|_| {
            std::path::Path::new(&path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("local-repo")
                .to_string()
        });
        let branch = std::env::var("SURREAL_BRANCH").unwrap_or_else(|_| "local-branch".into());
        let commit = std::env::var("SURREAL_COMMIT").unwrap_or_else(|_| "local-commit".into());
        let key = format!(
            "{}|{}|{}|{}|{}|{}|{}",
            project, repo, branch, commit, file.path, ent.name, ent.signature
        );
        let mut hasher = sha2::Sha256::new();
        hasher.update(key.as_bytes());
        let stable_id = format!("{:x}", hasher.finalize());

        payloads.push(EntityPayload {
            file: file.path.clone(),
            language: file.language.clone(),
            kind: ent.kind.as_str().to_string(),
            name: ent.name.clone(),
            parent: ent.parent.clone(),
            signature: ent.signature.clone(),
            start_line: start_field,
            end_line: end_field,
            calls: ent.calls.clone(),
            doc: ent.doc.clone(),
            rank: ent.rank,
            imports,
            unresolved_imports,
            stable_id,
        });
    }
    Ok((payloads, stats))
}

#[cfg(test)]
mod tests {
    use super::AppConfig;
    use std::path::PathBuf;

    #[test]
    fn appconfig_load_from_file() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let cfg_path = dir.path().join("test_config.toml");
        let toml = r#"
        debug = true
        out = ".data/test_out.jsonl"
        channel_capacity = 42
        "#;
        std::fs::write(&cfg_path, toml)?;
        let (cfg, path) = AppConfig::load(Some(&PathBuf::from(&cfg_path)))?;
        assert_eq!(cfg.debug.unwrap_or(false), true);
        assert_eq!(cfg.channel_capacity.unwrap_or(0), 42);
        assert_eq!(path, cfg_path);
        Ok(())
    }
}
