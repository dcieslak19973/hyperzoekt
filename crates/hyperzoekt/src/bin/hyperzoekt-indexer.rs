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
use hyperzoekt::event_consumer;
use log::info;
use std::path::PathBuf;

/// Continuous repo indexer that subscribes to Redis events from zoekt-distributed
#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    repo_root: Option<PathBuf>,
}

#[derive(Debug, serde::Deserialize)]
struct AppConfig {
    repo_root: Option<PathBuf>,
    processing_ttl_seconds: Option<u64>,
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
                    repo_root: None,
                    processing_ttl_seconds: None,
                },
                cfg_path,
            ))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();

    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::Builder::from_env(env).init();
    let (app_cfg, cfg_path) = AppConfig::load(args.config.as_ref())?;
    info!("Loaded config from {}", cfg_path.display());

    // Determine effective repo root for finding repositories
    let repo_root = args
        .repo_root
        .as_ref()
        .or(app_cfg.repo_root.as_ref())
        .cloned()
        .unwrap_or_else(|| PathBuf::from("."));

    // Determine processing TTL (default to 5 minutes = 300 seconds)
    let processing_ttl_seconds = std::env::var("HYPERZOEKT_PROCESSING_TTL_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .or(app_cfg.processing_ttl_seconds)
        .unwrap_or(300); // 5 minutes default

    // Set HYPERZOEKT_REPO_ROOT environment variable for the event processor
    std::env::set_var("HYPERZOEKT_REPO_ROOT", &repo_root);
    info!("Using repo root: {}", repo_root.display());
    info!("Using processing TTL: {} seconds", processing_ttl_seconds);

    info!("Starting continuous indexer, waiting for Redis events from zoekt-distributed...");

    // Start the event consumer system
    if let Err(e) = event_consumer::start_event_system_with_ttl(processing_ttl_seconds).await {
        eprintln!("Failed to start event system: {}", e);
        std::process::exit(1);
    }

    Ok(())
}
