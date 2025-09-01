use anyhow::Result;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::Json;
use clap::Parser;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;

use zoekt_distributed::{LeaseManager, NodeConfig, NodeType};

#[derive(Parser)]
struct Opts {
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    #[arg(long)]
    id: Option<String>,
    #[arg(long)]
    lease_ttl_seconds: Option<u64>,
    #[arg(long)]
    poll_interval_seconds: Option<u64>,
    /// Address to listen on for HTTP search (env: ZOEKTD_BIND_ADDR)
    #[arg(long)]
    listen: Option<String>,
}

#[derive(Deserialize, Clone)]
struct SearchParams {
    repo: String,
    q: String,
    /// number of context lines to include before/after the match (optional)
    context_lines: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing using the RUST_LOG env var when present, default to `info`
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let opts = Opts::parse();

    let _cfg = zoekt_distributed::load_node_config(
        NodeConfig {
            node_type: NodeType::Search,
            ..Default::default()
        },
        zoekt_distributed::MergeOpts {
            config_path: opts.config,
            cli_id: opts.id,
            cli_lease_ttl_seconds: opts.lease_ttl_seconds,
            cli_poll_interval_seconds: opts.poll_interval_seconds,
            cli_endpoint: None,
        },
    )?;

    let lease_mgr = LeaseManager::new().await;
    let client = Client::new();

    // determine bind address from CLI flag, env var, or default
    let bind_addr = opts
        .listen
        .or_else(|| std::env::var("ZOEKTD_BIND_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:8080".into());
    tracing::info!(binding = %bind_addr, "starting distributed http search server");

    let app = axum::Router::new()
        .route(
            "/search",
            axum::routing::get(|Query(params): Query<SearchParams>| async move {
                tracing::info!(repo=%params.repo, q=%params.q, "received distributed search request");

                // Get all indexer endpoints
                let endpoints = lease_mgr.get_indexer_endpoints().await;
                if endpoints.is_empty() {
                    tracing::warn!("no indexer endpoints found");
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error": "no indexers available"})),
                    );
                }

                // Query all indexers in parallel
                let mut tasks = Vec::new();
                for (node_id, endpoint) in endpoints {
                    let client = client.clone();
                    let params = params.clone();
                    let task = tokio::spawn(async move {
                        let url = format!("{}/search?repo={}&q={}&context_lines={}",
                            endpoint,
                            urlencoding::encode(&params.repo),
                            urlencoding::encode(&params.q),
                            params.context_lines.unwrap_or(2)
                        );
                        match timeout(Duration::from_secs(10), client.get(&url).send()).await {
                            Ok(Ok(resp)) if resp.status().is_success() => {
                                match resp.json::<serde_json::Value>().await {
                                    Ok(json) => Some((node_id, json)),
                                    Err(e) => {
                                        tracing::warn!(node_id=%node_id, error=%e, "failed to parse response");
                                        None
                                    }
                                }
                            }
                            Ok(Ok(resp)) => {
                                tracing::warn!(node_id=%node_id, status=%resp.status(), "search request failed");
                                None
                            }
                            Ok(Err(e)) => {
                                tracing::warn!(node_id=%node_id, error=%e, "search request error");
                                None
                            }
                            Err(_) => {
                                tracing::warn!(node_id=%node_id, "search request timed out");
                                None
                            }
                        }
                    });
                    tasks.push(task);
                }

                // Collect results
                let mut all_results = Vec::new();
                for task in tasks {
                    if let Ok(Some((node_id, json))) = task.await {
                        if let Some(results) = json.get("results").and_then(|r| r.as_array()) {
                            for result in results {
                                let mut result = result.clone();
                                // Add node_id to the result
                                if let Some(obj) = result.as_object_mut() {
                                    obj.insert("node_id".to_string(), json!(node_id));
                                }
                                all_results.push(result);
                            }
                        }
                    }
                }

                // Sort results by score (highest first)
                all_results.sort_by(|a, b| {
                    let score_a = a.get("score").and_then(|s| s.as_f64()).unwrap_or(0.0);
                    let score_b = b.get("score").and_then(|s| s.as_f64()).unwrap_or(0.0);
                    score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
                });

                tracing::info!(repo=%params.repo, total_results=%all_results.len(), "distributed search completed");
                (StatusCode::OK, Json(json!({"results": all_results})))
            }),
        )
        .route(
            "/status",
            axum::routing::get(|| async move {
                (StatusCode::OK, Json(json!({"status": "ok"})))
            }),
        );

    let addr = bind_addr.parse().expect("invalid bind address");
    let server = axum::Server::bind(&addr).serve(app.into_make_service());

    // run server until interrupted
    server.await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(())
}
