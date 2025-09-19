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
use log::{error, info, LevelFilter};
use std::path::PathBuf;

/// Continuous repo indexer that subscribes to Redis events from zoekt-distributed
#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    repo_root: Option<PathBuf>,
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 3001)]
    port: u16,
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
    // Fallback early marker (stderr) so we can distinguish binary start even if logger output is buffered or filtered.
    eprintln!(
        "EARLY_START_MARKER_HYPERZOEKT_INDEXER ts={} pid={} version={}",
        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        std::process::id(),
        env!("CARGO_PKG_VERSION")
    );
    // Attempt immediate flush
    use std::io::Write as _;
    let _ = std::io::stderr().flush();
    let args = Args::parse();

    // Initialize OpenTelemetry/tracing if enabled via env and feature compiled.
    // This is a no-op when the `otel` feature is not enabled.
    let enable_otel = std::env::var("HZ_ENABLE_OTEL")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True" | "yes" | "YES"))
        .unwrap_or(false);
    if enable_otel {
        hyperzoekt::otel::init_otel_from_env();
    }

    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    let mut builder = env_logger::Builder::from_env(env);
    // Quiet down chatty HTTP/client internals while preserving debug for our crates
    builder
        .filter_module("hyper_util", LevelFilter::Warn)
        .filter_module("hyper", LevelFilter::Warn)
        .filter_module("h2", LevelFilter::Warn)
        .filter_module("reqwest", LevelFilter::Warn)
        .filter_module("tower_http", LevelFilter::Warn);
    builder.init();
    let (app_cfg, cfg_path) = AppConfig::load(args.config.as_ref())?;
    info!("Loaded config from {}", cfg_path.display());

    // Emit a concise startup diagnostic line so operators can confirm the active binary build.
    info!(
        "startup diagnostic: version={} otel_feature_compiled={} otel_env_enabled={} embed_jobs_env={} RUST_LOG={} pid={}",
        env!("CARGO_PKG_VERSION"),
        cfg!(feature = "otel"),
        enable_otel,
        std::env::var("HZ_ENABLE_EMBED_JOBS").unwrap_or_default(),
        std::env::var("RUST_LOG").unwrap_or_default(),
        std::process::id()
    );
    if !enable_otel {
        info!("otel disabled: set HZ_ENABLE_OTEL=1 to activate tracing subscriber + OTLP export");
    }
    eprintln!("startup diagnostic: version={} otel_feature_compiled={} otel_env_enabled={} embed_jobs_env={} RUST_LOG={} pid={}",
        env!("CARGO_PKG_VERSION"),
        cfg!(feature = "otel"),
        enable_otel,
        std::env::var("HZ_ENABLE_EMBED_JOBS").unwrap_or_default(),
        std::env::var("RUST_LOG").unwrap_or_default(),
        std::process::id()
    );

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

    // Ensure Surreal schema for embeddings exists. The indexer owns the
    // entity table lifecycle so it is responsible for initializing fields
    // when standing up fresh Surreal instances for local/dev environments.
    // Schema initialization for core tables (including embedding fields) is performed
    // by `db_writer::init_schema`. The indexer does not own table lifecycle.
    info!("indexer skipping schema init; db_writer is authoritative for schema");

    // Start the event consumer system and get the processor handle
    let processor_handle =
        match event_consumer::start_event_system_with_ttl(processing_ttl_seconds).await {
            Ok(handle) => handle,
            Err(e) => {
                error!("Failed to start event system: {}", e);
                std::process::exit(1);
            }
        };

    info!("Event system started successfully. Indexer will run continuously waiting for Redis events...");

    // Wait for the processor to complete (this should never happen in normal operation)
    if let Err(e) = processor_handle.await {
        error!("Event processor task panicked: {:?}", e);
        std::process::exit(1);
    }

    Ok(())
}
