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

use axum::{extract::State, response::IntoResponse, routing::get, Router};
use clap::Parser;
use hyperzoekt::event_consumer;
use log::{error, info, LevelFilter};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::signal;
use zoekt_distributed::redis_adapter::{DynRedis, RealRedis};

/// Shared state for the HTTP server
#[derive(Clone)]
struct AppState {
    metrics: Arc<IndexerMetrics>,
    redis: Option<Arc<RealRedis>>,
}

/// Metrics for the indexer
#[derive(Debug)]
struct IndexerMetrics {
    events_processed: AtomicU64,
    events_failed: AtomicU64,
    last_event_unix: AtomicU64,
}

impl IndexerMetrics {
    fn new() -> Self {
        Self {
            events_processed: AtomicU64::new(0),
            events_failed: AtomicU64::new(0),
            last_event_unix: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> IndexerMetricsSnapshot {
        IndexerMetricsSnapshot {
            events_processed: self.events_processed.load(Ordering::Relaxed),
            events_failed: self.events_failed.load(Ordering::Relaxed),
            last_event_unix: self.last_event_unix.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct IndexerMetricsSnapshot {
    events_processed: u64,
    events_failed: u64,
    last_event_unix: u64,
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let build_timestamp = option_env!("BUILD_TIMESTAMP").unwrap_or("unknown");
    let build_id = option_env!("HYPERZOEKT_BUILD_ID").unwrap_or("unknown");

    // Use the pre-created redis pool and ping it
    match &state.redis {
        Some(r) => match r.ping().await {
            Ok(_) => (
                axum::http::StatusCode::OK,
                format!(
                    "OK\nBUILD_TIMESTAMP: {}\nBUILD_ID: {}\nVERSION: {}",
                    build_timestamp,
                    build_id,
                    env!("CARGO_PKG_VERSION")
                ),
            ),
            Err(e) => (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                format!(
                    "ERR: {}\nBUILD_TIMESTAMP: {}\nBUILD_ID: {}",
                    e, build_timestamp, build_id
                ),
            ),
        },
        None => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "no redis\nBUILD_TIMESTAMP: {}\nBUILD_ID: {}",
                build_timestamp, build_id
            ),
        ),
    }
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.metrics.snapshot();
    let body = format!("# HELP hyperzoekt_indexer_events_processed_total Total events processed\n# TYPE hyperzoekt_indexer_events_processed_total counter\nhyperzoekt_indexer_events_processed_total {}\n# HELP hyperzoekt_indexer_events_failed_total Events that failed processing\n# TYPE hyperzoekt_indexer_events_failed_total counter\nhyperzoekt_indexer_events_failed_total {}\n# HELP hyperzoekt_indexer_last_event_unix_seconds Last event time (unix seconds)\n# TYPE hyperzoekt_indexer_last_event_unix_seconds gauge\nhyperzoekt_indexer_last_event_unix_seconds {}\n", s.events_processed, s.events_failed, s.last_event_unix);
    (
        axum::http::StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

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
        "🚀🚀🚀 INDEXER_BUILD_WITH_BRANCH_LOGGING_v2 🚀🚀🚀 version={} otel_feature_compiled={} otel_env_enabled={} RUST_LOG={} pid={} SIMILARITY_ALWAYS_ENABLED=true",
        env!("CARGO_PKG_VERSION"),
        cfg!(feature = "otel"),
        enable_otel,
        std::env::var("RUST_LOG").unwrap_or_default(),
        std::process::id()
    );
    if !enable_otel {
        info!("otel disabled: set HZ_ENABLE_OTEL=1 to activate tracing subscriber + OTLP export");
    }
    info!("🔍 Similarity jobs: ALWAYS ENABLED (no env var required)");
    info!("📌 Branch ref creation: ENABLED with enhanced logging");
    eprintln!("🚀🚀🚀 INDEXER_BUILD_WITH_BRANCH_LOGGING_v2 🚀🚀🚀 version={} otel_feature_compiled={} otel_env_enabled={} RUST_LOG={} pid={} SIMILARITY_ALWAYS_ENABLED=true",
        env!("CARGO_PKG_VERSION"),
        cfg!(feature = "otel"),
        enable_otel,
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

    // Initialize metrics
    let metrics = Arc::new(IndexerMetrics::new());

    // Create Redis pool once at startup (not in health handler loop!)
    let redis = zoekt_distributed::redis_adapter::create_redis_pool()
        .map(|pool| Arc::new(RealRedis { pool }));
    if redis.is_some() {
        info!("Redis pool created successfully for health checks");
    } else {
        info!("No Redis pool available (in-memory fallback mode)");
    }

    // Start HTTP server for health/metrics
    let app_state = AppState {
        metrics: Arc::clone(&metrics),
        redis,
    };
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/healthz", get(health_handler)) // Kubernetes-style alias
        .route("/metrics", get(metrics_handler))
        .with_state(app_state);

    let addr = format!("{}:{}", args.host, args.port)
        .parse::<std::net::SocketAddr>()
        .unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("HTTP server listening on {}", addr);
    let serve_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            error!("HTTP server failed: {:?}", e);
        }
    });

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

    // Wait for either the processor or HTTP server to complete
    tokio::select! {
        result = processor_handle => {
            if let Err(e) = result {
                error!("Event processor task panicked: {:?}", e);
                std::process::exit(1);
            }
        }
        _ = signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
    }

    // Shutdown HTTP server gracefully
    serve_handle.abort();

    Ok(())
}
