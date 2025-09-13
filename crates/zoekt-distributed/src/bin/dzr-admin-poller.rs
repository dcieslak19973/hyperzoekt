use anyhow::Result;
use axum::{response::IntoResponse, routing::get, Router};
use std::time::Duration;
use tokio::signal;
use tokio::sync::watch;

use zoekt_distributed::poller::{run_poller, snapshot_metrics, PollerConfig};
use zoekt_distributed::redis_adapter::{create_redis_pool, DynRedis, RealRedis};

async fn health_handler() -> impl IntoResponse {
    // Try to create a redis pool and ping it
    match create_redis_pool() {
        Some(p) => {
            let r = RealRedis { pool: p };
            match r.ping().await {
                Ok(_) => (axum::http::StatusCode::OK, "OK".to_string()),
                Err(e) => (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    format!("ERR: {}", e),
                ),
            }
        }
        None => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "no redis".to_string(),
        ),
    }
}

async fn metrics_handler() -> impl IntoResponse {
    let s = snapshot_metrics();
    let body = format!("# HELP dzr_poller_polls_run_total Total poll attempts\n# TYPE dzr_poller_polls_run_total counter\ndzr_poller_polls_run_total {}\n# HELP dzr_poller_polls_failed_total Poll attempts that failed\n# TYPE dzr_poller_polls_failed_total counter\ndzr_poller_polls_failed_total {}\n# HELP dzr_poller_last_poll_unix_seconds Last poll time (unix seconds)\n# TYPE dzr_poller_last_poll_unix_seconds gauge\ndzr_poller_last_poll_unix_seconds {}\n", s.polls_run, s.polls_failed, s.last_poll_unix);
    (
        axum::http::StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::from_default_env();
    tracing_subscriber::fmt().with_env_filter(filter).init();

    // Read poll interval from env or default to 5s
    let poll_secs = std::env::var("ZOEKTD_POLL_INTERVAL_SECONDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5);

    let cfg = PollerConfig {
        poll_interval: Duration::from_secs(poll_secs),
    };

    let (tx, rx) = watch::channel(());
    let handle = tokio::spawn(async move {
        if let Err(e) = run_poller(rx, cfg).await {
            tracing::error!("poller failed: ?{:?}", e)
        }
    });

    // Start a small HTTP server for health/metrics
    let metrics_port = std::env::var("ZOEKT_POLLER_METRICS_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(9900);
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler));
    // Bind a TcpListener and run axum::serve for portability with existing binaries
    let addr = format!("0.0.0.0:{}", metrics_port)
        .parse::<std::net::SocketAddr>()
        .unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let serve_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("metrics server failed: {:?}", e);
        }
    });

    // Wait for ctrl-c
    signal::ctrl_c().await?;
    let _ = tx.send(());
    let _ = handle.await;
    // shutdown metrics server gracefully
    serve_handle.abort();
    Ok(())
}
