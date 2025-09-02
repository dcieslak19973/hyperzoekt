use anyhow::Result;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, Json},
    routing::get,
    Router,
};
use clap::Parser;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;
use zoekt_distributed::{
    distributed_search::{DistributedSearchService, DistributedSearchTool},
    LeaseManager, NodeConfig, NodeType,
};

// Embed static templates and assets so the binary can serve them directly.
const SEARCH_INDEX_TEMPLATE: &str = include_str!("../../static/search/index.html");
const COMMON_STYLES: &str = include_str!("../../static/common/styles.css");
const COMMON_THEME_JS: &str = include_str!("../../static/common/theme.js");

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
}

async fn search_handler(
    State(search_service): State<DistributedSearchService>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, StatusCode> {
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

    match search_service.execute_distributed_search_json(params).await {
        Ok(results) => {
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
            };

            Ok(Json(response))
        }
        Err(e) => {
            tracing::error!("Search failed: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn search_ui_handler() -> Html<&'static str> {
    Html(SEARCH_INDEX_TEMPLATE)
}

async fn styles_handler() -> &'static str {
    COMMON_STYLES
}

async fn theme_js_handler() -> &'static str {
    COMMON_THEME_JS
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
    listen: Option<String>,
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
        .route("/static/common/styles.css", get(styles_handler))
        .route("/static/common/theme.js", get(theme_js_handler))
        .with_state(search_service);

    // Determine bind address
    let bind_addr = opts
        .listen
        .or_else(|| std::env::var("ZOEKTD_BIND_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:8080".into());

    let addr: SocketAddr = bind_addr
        .parse()
        .unwrap_or_else(|_| "127.0.0.1:8080".parse().unwrap());

    tracing::info!("Starting HTTP search server on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
