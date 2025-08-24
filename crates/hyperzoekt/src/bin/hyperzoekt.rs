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
            .map(|p| p.clone())
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

    // Index all detected languages by default

    if app_cfg.incremental.unwrap_or(false) {
        // Stream results by passing a writer to options
        let out_path = app_cfg.out.as_deref().unwrap_or(default_out.as_path());
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
    let (svc, stats) = RepoIndexService::build_with_options(opts)?;
    let out_path = app_cfg.out.as_deref().unwrap_or(default_out.as_path());

    // If debug is set, dump JSONL to disk and exit. Otherwise stream to DB.
    if app_cfg.debug.unwrap_or(false) {
        let mut writer = BufWriter::new(File::create(out_path)?);
        for ent in &svc.entities {
            let file = &svc.files[ent.file_id as usize];
            // Attach file-level imports for File pseudo-entities
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
                                    .saturating_add(1)
                                    as u32;
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
                            line: lineno.saturating_add(1) as u32,
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
                            Some((ent.start_line.saturating_add(1)) as u32),
                            Some((ent.end_line.saturating_add(1)) as u32),
                        )
                    } else {
                        (None, None)
                    }
                } else {
                    (
                        Some((ent.start_line.saturating_add(1)) as u32),
                        Some((ent.end_line.saturating_add(1)) as u32),
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

    // We'll run a DB task on a background thread and send payloads to it via
    // a bounded MPSC channel. This keeps the watcher and indexer synchronous
    // while preventing unbounded memory usage under load.
    let channel_capacity = app_cfg.channel_capacity.unwrap_or(100usize);
    let (tx, rx) = sync_channel::<Vec<EntityPayload>>(channel_capacity);
    let db_join = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build runtime");
        rt.block_on(async move {
            let db = {
                if surreal_url.is_some() {
                    let url = surreal_url.as_ref().unwrap();
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
                        loop {
                            match rx.try_recv() {
                                Ok(next) => acc.extend(next),
                                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                            }
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
                    let mut failed = false;
                    for p in acc.iter() {
                        // Upsert file by path
                        let q_file = "UPDATE file SET path = $path, language = $language WHERE path = $path; CREATE file CONTENT { path: $path, language: $language } IF NONE;";
                        if let Err(e) = db
                            .query(q_file)
                            .bind(("path", p.file.clone()))
                            .bind(("language", p.language.clone()))
                            .await
                        {
                            warn!("DB write for file failed: {}", e);
                            failed = true;
                            break;
                        }

                        // Prepare idempotent entity upsert: update by file+name, create if none
                        match serde_json::to_value(&p) {
                            Ok(v) => {
                                // Fast path: attempt CREATE and on duplicate-key error
                                // fall back to UPDATE by stable_id. This avoids the
                                // SELECT round-trip and relies on a unique index on
                                // entity.stable_id which we attempted to create above.
                                let q_create = "CREATE entity CONTENT $entity";
                                match db.query(q_create).bind(("entity", v.clone())).await {
                                    Ok(_) => {
                                        // created successfully
                                    }
                                    Err(e) => {
                                        let msg = format!("{}", e);
                                        if msg.to_lowercase().contains("duplicate") || msg.to_lowercase().contains("unique") {
                                            // Duplicate — update existing record by stable_id
                                            let q_update = "UPDATE entity CONTENT $entity WHERE stable_id = $stable_id";
                                            if let Err(e2) = db.query(q_update).bind(("entity", v.clone())).bind(("stable_id", p.stable_id.clone())).await {
                                                warn!("DB update-after-duplicate failed: {}", e2);
                                                failed = true;
                                                break;
                                            }
                                        } else {
                                            warn!("DB create for entity failed: {}", e);
                                            failed = true;
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to serialize entity payload for DB: {}", e);
                                // Skip this entity but don't fail entire batch for serialization error
                                continue;
                            }
                        }
                    }

                    if !failed {
                        batches_sent += 1;
                        entities_sent += acc.len();
                        break;
                    }

                    // failed
                    if attempt >= max_retries {
                        error!("Batch failed after {} attempts, dropping {} entities", attempt, acc.len());
                        total_retries += attempt - 1;
                        break;
                    }
                    total_retries += 1;
                    let backoff = Duration::from_millis(100 * (1 << (attempt - 1)).min(8));
                    warn!("Retrying batch in {:?} (attempt {}/{})", backoff, attempt, max_retries);
                    sleep(backoff);
                }

                if batches_sent % 10 == 0 && batches_sent > 0 {
                    info!("DB metrics: batches_sent={} entities_sent={} total_retries={}", batches_sent, entities_sent, total_retries);
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

    return Ok(());
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
                                .saturating_add(1) as u32;
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
                        line: lineno.saturating_add(1) as u32,
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
                    Some((ent.start_line.saturating_add(1)) as u32),
                    Some((ent.end_line.saturating_add(1)) as u32),
                )
            } else {
                (None, None)
            }
        } else {
            (
                Some((ent.start_line.saturating_add(1)) as u32),
                Some((ent.end_line.saturating_add(1)) as u32),
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
