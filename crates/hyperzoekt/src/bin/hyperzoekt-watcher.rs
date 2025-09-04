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
use hyperzoekt::db_writer;
use hyperzoekt::event_consumer;
use hyperzoekt::repo_index::indexer::payload::EntityPayload;
use hyperzoekt::watcher;
use log::{error, info, warn};
use sha2::Digest;
use std::path::PathBuf;

/// File watcher that monitors a repository for changes and updates the index.
#[derive(Parser)]
struct Args {
    #[arg(long)]
    root: Option<PathBuf>,
    #[arg(long, default_value = "200")]
    debounce_ms: u64,
    #[arg(long)]
    surreal_url: Option<String>,
    #[arg(long, default_value = "test")]
    surreal_ns: String,
    #[arg(long, default_value = "test")]
    surreal_db: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::Builder::from_env(env).init();

    let effective_root = args.root.unwrap_or_else(|| PathBuf::from("."));

    // Create initial index for the repository
    info!("Creating initial index for {}", effective_root.display());
    let (svc, stats) = hyperzoekt::repo_index::RepoIndexService::build(&effective_root)?;
    info!(
        "Initial index created (files_indexed={} entities_indexed={})",
        stats.files_indexed, stats.entities_indexed
    );

    // Convert to payloads
    let payloads: Vec<EntityPayload> = svc
        .entities
        .iter()
        .map(|ent| {
            let file = &svc.files[ent.file_id as usize];
            let mut imports: Vec<hyperzoekt::repo_index::indexer::payload::ImportItem> = Vec::new();
            let mut unresolved_imports: Vec<
                hyperzoekt::repo_index::indexer::payload::UnresolvedImport,
            > = Vec::new();

            if matches!(ent.kind, hyperzoekt::repo_index::indexer::EntityKind::File) {
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
                                imports.push(
                                    hyperzoekt::repo_index::indexer::payload::ImportItem {
                                        path: target_file.path.clone(),
                                        line: line_no,
                                    },
                                );
                            }
                        }
                    }
                }
                if let Some(unres) = svc.unresolved_imports.get(ent.file_id as usize) {
                    for (m, lineno) in unres {
                        unresolved_imports.push(
                            hyperzoekt::repo_index::indexer::payload::UnresolvedImport {
                                module: m.clone(),
                                line: lineno.saturating_add(1),
                            },
                        );
                    }
                }
            }

            let (start_line, end_line) =
                if matches!(ent.kind, hyperzoekt::repo_index::indexer::EntityKind::File) {
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

            // Generate stable ID
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
                start_line,
                end_line,
                calls: ent.calls.clone(),
                doc: ent.doc.clone(),
                rank: ent.rank,
                imports,
                unresolved_imports,
                stable_id,
            }
        })
        .collect();

    // Start DB writer
    let db_cfg = db_writer::DbWriterConfig {
        channel_capacity: 100,
        batch_capacity: Some(500),
        batch_timeout_ms: Some(500),
        max_retries: Some(3),
        surreal_url: args.surreal_url,
        surreal_ns: args.surreal_ns,
        surreal_db: args.surreal_db,
        initial_batch: true,
    };

    let (tx, db_join) = db_writer::spawn_db_writer(payloads, db_cfg)?;

    // Start event consumer for zoekt-distributed integration
    if let Err(e) = event_consumer::start_event_system().await {
        warn!("Failed to start event system: {}", e);
    }

    // Start file watcher
    info!("Starting file watcher for {}", effective_root.display());
    watcher::run_watcher(effective_root, args.debounce_ms, tx)?;

    // Wait for DB thread to finish
    match db_join.join() {
        Ok(Ok(())) => {
            info!("Watcher finished successfully");
        }
        Ok(Err(e)) => error!("DB task failed: {}", e),
        Err(e) => error!("DB thread panicked: {:?}", e),
    }

    Ok(())
}
