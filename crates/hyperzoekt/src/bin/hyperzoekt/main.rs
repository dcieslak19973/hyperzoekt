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

use clap::Parser;
use log::{error, info, warn};
use serde_json::json;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

use hyperzoekt::db_writer;
use hyperzoekt::event_consumer;

use hyperzoekt::repo_index::indexer::payload::{EntityPayload, ImportItem, UnresolvedImport};

use hyperzoekt::repo_index::indexer::{EntityKind, RepoIndexOptions};
use hyperzoekt::repo_index::RepoIndexService;

/// Simple repo indexer that writes one JSON object per line (JSONL).
#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    root: Option<PathBuf>,
    #[arg(long, short = 'o')]
    out: Option<PathBuf>,
    // Deprecated positional out has been removed; use --out / -o instead.
    #[arg(long)]
    incremental: bool,
    #[arg(long)]
    debug: bool,
    #[arg(long)]
    stream_once: bool,
}

#[derive(Debug, serde::Deserialize)]
struct AppConfig {
    root: Option<PathBuf>,
    out: Option<PathBuf>,
    incremental: Option<bool>,
    debug: Option<bool>,
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
                },
                cfg_path,
            ))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    // If HYPERZOEKT_LOG_FILE is set, redirect stderr to that file for logging
    if let Ok(log_file_path) = std::env::var("HYPERZOEKT_LOG_FILE") {
        let log_file = File::create(&log_file_path).expect("Failed to create log file");
        let fd = log_file.as_raw_fd();
        unsafe {
            libc::dup2(fd, libc::STDERR_FILENO);
        }
        // Keep the file open until the end, but dup2 duplicates the fd
    }

    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::Builder::from_env(env).init();
    let (app_cfg, cfg_path) = AppConfig::load(args.config.as_ref())?;
    info!("Loaded config from {}", cfg_path.display());

    // Determine effective root
    let effective_root = args
        .root
        .as_ref()
        .cloned()
        .or_else(|| app_cfg.root.clone())
        .unwrap_or_else(|| PathBuf::from("."));
    let out_dir = PathBuf::from(".data");
    if let Err(e) = std::fs::create_dir_all(&out_dir) {
        warn!("Failed to create out dir {}: {}", out_dir.display(), e);
    }
    let default_out = out_dir.join("bin_integration_out.jsonl");

    let effective_out: PathBuf = args
        .out
        .as_ref()
        .cloned()
        .or_else(|| app_cfg.out.clone())
        .unwrap_or(default_out.clone());

    // Build index
    let opts = RepoIndexOptions::builder()
        .root(&effective_root)
        .output_null()
        .build();
    info!("Starting index build for {}", effective_root.display());
    let (svc, stats) = RepoIndexService::build_with_options(opts)?;
    info!(
        "Index build finished (files_indexed={} entities_indexed={})",
        stats.files_indexed, stats.entities_indexed
    );

    // Debug / incremental JSONL output
    let is_incremental = args.incremental || app_cfg.incremental.unwrap_or(false);
    if args.debug || app_cfg.debug.unwrap_or(false) || is_incremental {
        let mut writer = BufWriter::new(File::create(&effective_out)?);
        for ent in &svc.entities {
            let file = &svc.files[ent.file_id as usize];
            let mut imports: Vec<serde_json::Value> = Vec::new();
            let mut unresolved_imports: Vec<serde_json::Value> = Vec::new();
            if matches!(ent.kind, EntityKind::File) {
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
                if let Some(unres) = svc.unresolved_imports.get(ent.file_id as usize) {
                    for (m, lineno) in unres {
                        unresolved_imports
                            .push(json!({"module": m, "line": lineno.saturating_add(1)}));
                    }
                }
            }

            let (start_field, end_field) = if matches!(ent.kind, EntityKind::File) {
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
            effective_out.display()
        );
        println!(
            "Indexed {} files, {} entities",
            stats.files_indexed, stats.entities_indexed
        );
        return Ok(());
    }

    // --- Streaming path: build typed EntityPayloads to send to DB thread ---
    // If SURREAL_INITIAL_BATCH=1, main will perform an initial batch and exit.
    let initial_batch = std::env::var("SURREAL_INITIAL_BATCH").ok().as_deref() == Some("1");
    let surreal_url = std::env::var("SURREAL_URL").ok();
    let surreal_ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "test".into());
    let surreal_db = std::env::var("SURREAL_DB").unwrap_or_else(|_| "test".into());

    let payloads: Vec<EntityPayload> = svc
        .entities
        .iter()
        .map(|ent| {
            let file = &svc.files[ent.file_id as usize];
            let mut imports: Vec<ImportItem> = Vec::new();
            let mut unresolved_imports: Vec<UnresolvedImport> = Vec::new();
            if matches!(ent.kind, EntityKind::File) {
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
            let (start_field, end_field) = if matches!(ent.kind, EntityKind::File) {
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

            // stable id
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
            let stable_id = hyperzoekt::utils::generate_stable_id(
                &project,
                &repo,
                &branch,
                &commit,
                &file.path,
                &ent.name,
                &ent.signature,
            );

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
                repo_name: repo.clone(),
            }
        })
        .collect();

    // Start DB writer thread
    let db_cfg = db_writer::DbWriterConfig {
        channel_capacity: 100,
        batch_capacity: None,
        batch_timeout_ms: None,
        max_retries: None,
        surreal_url,
        surreal_username: std::env::var("SURREALDB_USERNAME").ok(),
        surreal_password: std::env::var("SURREALDB_PASSWORD").ok(),
        surreal_ns: surreal_ns.clone(),
        surreal_db: surreal_db.clone(),
        initial_batch,
    };

    #[cfg(test)]
    mod tests {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct LocalAppConfig {
            debug: Option<bool>,
            channel_capacity: Option<usize>,
        }

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
            let s = std::fs::read_to_string(&cfg_path)?;
            let cfg: LocalAppConfig = toml::from_str(&s)?;
            assert!(cfg.debug.unwrap_or(false));
            assert_eq!(cfg.channel_capacity.unwrap_or(0), 42);
            Ok(())
        }
    }
    let (tx, db_join) = db_writer::spawn_db_writer(payloads.clone(), db_cfg)?;

    // If CLI requests a single streaming run (send all payloads to DB batching and exit)
    if args.stream_once {
        if let Err(e) = tx.send(payloads) {
            error!("Failed to send streaming payloads to DB thread: {}", e);
        }
        drop(tx);
        match db_join.join() {
            Ok(Ok(())) => {
                println!("Streaming import finished (stream-once)");
            }
            Ok(Err(e)) => error!("DB task failed: {}", e),
            Err(e) => error!("DB thread panicked: {:?}", e),
        }
        return Ok(());
    }

    // Start the event consumer system for zoekt-distributed integration
    if let Err(e) = event_consumer::start_event_system().await {
        warn!("Failed to start event system: {}", e);
    }

    // Wait for event system to run (this will run indefinitely)
    // The event system handles all indexing now
    std::future::pending::<()>().await;
    Ok(())
}
