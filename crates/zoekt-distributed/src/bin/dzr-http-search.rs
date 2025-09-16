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
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use clap::Parser;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tower_http::services::ServeDir;
use tracing_subscriber::EnvFilter;
use zoekt_distributed::{
    distributed_search::{DistributedSearchService, DistributedSearchTool},
    LeaseManager, NodeConfig, NodeType,
};

// Embed static templates so the binary can serve them directly.
const SEARCH_INDEX_TEMPLATE: &str = include_str!("../../static/search/index.html");

#[derive(Deserialize, Debug)]
struct SearchQuery {
    /// Regex pattern to search for (required)
    q: String,
    /// Path patterns to include (optional, supports glob patterns like "src/**/*.rs")
    include: Option<String>,
    /// Path patterns to exclude (optional, supports glob patterns like "target/**")
    exclude: Option<String>,
    /// Repository to search in (optional, searches all if not specified)
    repo: Option<String>,
    /// Maximum number of results to return (optional, default 100)
    max_results: Option<usize>,
    /// Number of context lines around matches (optional, default 2)
    context: Option<usize>,
    /// Case sensitive search (optional, default false)
    case: Option<bool>,
}

#[derive(Serialize)]
struct SearchResponse {
    results: Vec<serde_json::Value>,
    summary: String,
    total_results: usize,
    elapsed_ms: u128,
}

async fn search_handler(
    State(search_service): State<DistributedSearchService>,
    Query(query): Query<SearchQuery>,
) -> impl IntoResponse {
    tracing::info!("HTTP search request: {:?}", query);

    let params = DistributedSearchTool {
        regex: query.q,
        include: query.include,
        exclude: query.exclude,
        repo: query.repo,
        max_results: query.max_results,
        context: query.context,
        case_sensitive: query.case,
        github_username: None,
        github_token: None,
        gitlab_username: None,
        gitlab_token: None,
        bitbucket_username: None,
        bitbucket_token: None,
    };

    let started = std::time::Instant::now();
    match search_service.execute_distributed_search_json(params).await {
        Ok(results) => {
            let elapsed_ms = started.elapsed().as_millis();
            let total_results = results.len().saturating_sub(1); // Subtract 1 for summary
            let summary = if let Some(first) = results.first() {
                first
                    .get("summary")
                    .and_then(|s| s.as_str())
                    .unwrap_or("Search completed")
                    .to_string()
            } else {
                "No results found".to_string()
            };

            let response = SearchResponse {
                results,
                summary,
                total_results,
                elapsed_ms,
            };

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            tracing::error!("Search failed: {}", e);
            let body = serde_json::json!({
                "error": format!("{}", e),
            });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response()
        }
    }
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn search_ui_handler() -> Html<&'static str> {
    Html(SEARCH_INDEX_TEMPLATE)
}

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
    host: Option<String>,
    /// Port to listen on for HTTP search
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing using the RUST_LOG env var when present, default to `info`
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,hyper_util=warn,hyper=warn,h2=warn,reqwest=warn,tower_http=warn,ignore=warn",
        )
    });
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
            cli_enable_reindex: None,
            cli_index_once: None,
        },
    )?;

    let lease_mgr = LeaseManager::new().await;
    let client = Client::new();
    let search_service = DistributedSearchService::new(lease_mgr, client);

    // Build the application with routes
    let app = Router::new()
        .route("/search", get(search_handler))
        .route("/health", get(health_handler))
        .route("/", get(search_ui_handler))
        .nest_service("/static", ServeDir::new("../../static"))
        .with_state(search_service);

    // Determine bind address
    let host = opts
        .host
        .or_else(|| std::env::var("ZOEKTD_HOST").ok())
        .unwrap_or_else(|| "127.0.0.1".into());
    let port = opts
        .port
        .or_else(|| {
            std::env::var("ZOEKTD_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(8080);

    let addr: SocketAddr = format!("{}:{}", host, port)
        .parse()
        .unwrap_or_else(|_| "127.0.0.1:8080".parse().unwrap());

    tracing::info!("Starting HTTP search server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
