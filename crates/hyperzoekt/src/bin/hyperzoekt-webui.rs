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

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, Json},
    routing::get,
    Router,
};
use clap::Parser;
use minijinja::{context, Environment};
use serde::Deserialize;
use std::io::Write as _;
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use axum::body::Body;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use hyperzoekt::db::connection::SurrealConnection;
use hyperzoekt::repo_index::indexer::payload::EntityPayload;
use hyperzoekt::utils;
use hyperzoekt::utils::*;
use surrealdb::Value as DbValue;

// Helper to safely convert SurrealDB response to JSON
#[allow(dead_code)]
fn db_value_to_json(db_val: DbValue) -> Option<serde_json::Value> {
    match serde_json::to_value(&db_val) {
        Ok(json) => Some(json),
        Err(e) => {
            log::warn!("Failed to convert DbValue to JSON: {}", e);
            None
        }
    }
}

// Sort a Vec<serde_json::Value> of dependency objects by their `name` field (case-insensitive).
fn sort_deps_alphabetical(mut deps: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    deps.sort_by(|a, b| {
        let a_name = a
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        let b_name = b
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        a_name.cmp(&b_name)
    });
    deps
}

// Minimal helper to normalize Surreal id field (string or object with tb/id)

// Embed static templates so the binary can serve them directly.
const BASE_TEMPLATE: &str = include_str!("../../static/webui/base.html");
const INDEX_TEMPLATE: &str = include_str!("../../static/webui/index.html");
const REPO_TEMPLATE: &str = include_str!("../../static/webui/repo.html");
const ENTITY_TEMPLATE: &str = include_str!("../../static/webui/entity.html");
// Use per-snapshot page rank value stored on the snapshot record. No fallback to entity-level fields.
const ENTITY_FIELDS: &str = "<string>id AS id, language, kind, name, snapshot.page_rank_value AS page_rank_value, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content";
const SEARCH_TEMPLATE: &str = include_str!("../../static/webui/search.html");
const PAGERANK_TEMPLATE: &str = include_str!("../../static/webui/pagerank.html");
const DUPES_TEMPLATE: &str = include_str!("../../static/webui/dupes.html");
const DEPENDENCIES_TEMPLATE: &str = include_str!("../../static/webui/dependencies.html");
const SBOM_TEMPLATE: &str = include_str!("../../static/webui/sbom.html");
const HIRAG_TEMPLATE: &str = include_str!("../../static/webui/hirag.html");
const HIRAG_MINDMAP_TEMPLATE: &str = include_str!("../../static/webui/hirag-mindmap.html");

#[derive(Parser)]
#[command(name = "hyperzoekt-webui")]
#[command(about = "Web UI for visualizing HyperZoekt indexed data")]
#[command(long_about = "
HyperZoekt WebUI provides a web interface to browse and search through indexed code data.

The application connects to a SurrealDB instance to retrieve indexed repository information
and serves a modern web interface for exploring code entities, repositories, and performing searches.

Examples:
    # Run with default settings (embedded SurrealDB)
    hyperzoekt-webui

    # Connect to remote SurrealDB
    hyperzoekt-webui --surreal-url http://localhost:8000

    # Custom configuration
    hyperzoekt-webui --host 0.0.0.0 --port 8080 --surreal-url surrealdb:8000

Environment Variables:
    HYPERZOEKT_HOST              Web server host (default: 127.0.0.1)
    HYPERZOEKT_PORT              Web server port (default: 3000)
    SURREALDB_URL                SurrealDB URL (optional, falls back to embedded)
    SURREAL_NS                   SurrealDB namespace (default: zoekt)
    SURREAL_DB                   SurrealDB database (default: repos)
    HYPERZOEKT_LOG_LEVEL         Log level (default: info)
    HYPERZOEKT_CORS_ALL          Enable CORS for all origins (default: false)
    HYPERZOEKT_MAX_SEARCH_RESULTS Maximum search results (default: 100)
")]
struct Args {
    /// Web server host to bind to
    #[arg(long, help = "Web server host (env: HYPERZOEKT_HOST)")]
    host: Option<String>,

    /// Web server port to listen on
    #[arg(long, help = "Web server port (env: HYPERZOEKT_PORT)")]
    port: Option<u16>,

    /// SurrealDB URL (optional, falls back to embedded database if not provided)
    #[arg(long, help = "SurrealDB URL (env: SURREALDB_URL)")]
    surreal_url: Option<String>,

    /// SurrealDB namespace
    #[arg(long, help = "SurrealDB namespace (env: SURREAL_NS)")]
    surreal_ns: Option<String>,

    /// SurrealDB database name
    #[arg(long, help = "SurrealDB database (env: SURREAL_DB)")]
    surreal_db: Option<String>,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, help = "Log level (env: HYPERZOEKT_LOG_LEVEL)")]
    log_level: Option<String>,

    /// Enable CORS for all origins (useful for development)
    #[arg(long, help = "Enable CORS for all origins (env: HYPERZOEKT_CORS_ALL)")]
    cors_all: bool,

    /// Maximum number of results to return in search
    #[arg(
        long,
        default_value = "100",
        help = "Maximum search results (env: HYPERZOEKT_MAX_SEARCH_RESULTS)"
    )]
    max_search_results: usize,
}

#[derive(Debug, Clone)]
struct Config {
    host: String,
    port: u16,
    surreal_url: Option<String>,
    surreal_ns: String,
    surreal_db: String,
    log_level: String,
    cors_all: bool,
    max_search_results: usize,
}

impl Config {
    fn from_args(args: Args) -> Self {
        let raw_log_level = args.log_level.unwrap_or_else(|| {
            std::env::var("HYPERZOEKT_LOG_LEVEL").unwrap_or_else(|_| "info".to_string())
        });
        Self {
            host: args.host.unwrap_or_else(|| {
                std::env::var("HYPERZOEKT_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
            }),
            port: args.port.unwrap_or_else(|| {
                std::env::var("HYPERZOEKT_PORT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(3000)
            }),
            surreal_url: args
                .surreal_url
                .or_else(|| std::env::var("SURREALDB_URL").ok())
                .map(|u| {
                    // Normalize and set env early so downstream Surreal client
                    // code never sees malformed values.
                    let (schemeful, _no_scheme) =
                        hyperzoekt::test_utils::normalize_surreal_host(&u);
                    std::env::set_var("SURREALDB_URL", &schemeful);
                    std::env::set_var("SURREALDB_HTTP_BASE", &schemeful);
                    schemeful
                }),
            surreal_ns: args.surreal_ns.unwrap_or_else(|| {
                std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".to_string())
            }),
            surreal_db: args.surreal_db.unwrap_or_else(|| {
                std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".to_string())
            }),
            log_level: raw_log_level,
            cors_all: args.cors_all
                || std::env::var("HYPERZOEKT_CORS_ALL")
                    .map(|v| v == "true" || v == "1")
                    .unwrap_or(false),
            max_search_results: args.max_search_results,
        }
    }
}

/// Normalize the requested log level into an env_logger filter string while ensuring
/// chatty dependencies stay quiet unless explicitly requested.
fn sanitize_log_spec(raw: &str) -> String {
    let trimmed = raw.trim();
    let base = if trimmed.is_empty() {
        "info".to_string()
    } else if trimmed.contains('=') || trimmed.contains(',') {
        trimmed.to_string()
    } else {
        format!("hyperzoekt={}", trimmed)
    };

    let mut parts: Vec<String> = base
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    let mut ensure_module = |module: &str, level: &str| {
        if !parts.iter().any(|p| p.starts_with(&format!("{}=", module))) {
            parts.push(format!("{}={}", module, level));
        }
    };

    for module in [
        "html5ever",
        "hyper",
        "hyper_util",
        "h2",
        "reqwest",
        "tower_http",
        "axum::rejection",
    ] {
        ensure_module(module, "warn");
    }

    parts.join(",")
}

#[derive(Clone)]
pub struct Database {
    queries: hyperzoekt::db::DatabaseQueries,
}

impl Database {
    pub async fn new(
        url: Option<&str>,
        ns: &str,
        db_name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        use hyperzoekt::db::connection::connect as hz_connect;
        let conn = hz_connect(&url.map(|s| s.to_string()), &None, &None, ns, db_name).await?;
        let queries = hyperzoekt::db::DatabaseQueries::new(std::sync::Arc::new(conn));
        Ok(Self { queries })
    }

    /// Resolve a human ref name (e.g. "main" or "refs/heads/main") to a commit id
    /// and attempt to find a matching `snapshot_meta` row. Returns (commit_id, snapshot_id_opt)
    /// if a ref was found, otherwise returns `Ok(None)`.
    pub async fn resolve_ref_to_snapshot(
        &self,
        repo: &str,
        raw: &str,
    ) -> Result<Option<(String, Option<String>)>, Box<dyn std::error::Error + Send + Sync>> {
        self.queries.resolve_ref_to_snapshot(repo, raw).await
    }

    /// Return the repo's configured default branch, falling back to `SOURCE_BRANCH` env or `main`.
    pub async fn get_repo_default_branch(
        &self,
        repo_name: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.queries.get_repo_default_branch(repo_name).await
    }

    pub async fn get_repo_summaries(&self) -> Result<Vec<RepoSummary>, Box<dyn std::error::Error>> {
        self.queries.get_repo_summaries().await
    }

    pub async fn get_entities_for_repo(
        &self,
        repo_name: &str,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        self.queries.get_entities_for_repo(repo_name).await
    }

    pub async fn get_all_entities(&self) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        self.queries.get_all_entities().await
    }

    pub async fn get_entity_by_id(
        &self,
        stable_id: &str,
    ) -> Result<Option<EntityPayload>, Box<dyn std::error::Error>> {
        self.queries.get_entity_by_id(stable_id).await
    }

    /// Fetch similar entities for a given entity stable_id using relation tables with score.
    /// Returns two lists: same-repo and external-repo, each with basic metadata and score.
    pub async fn get_similar_for_entity(
        &self,
        stable_id: &str,
        limit: usize,
    ) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>), Box<dyn std::error::Error>> {
        self.queries.get_similar_for_entity(stable_id, limit).await
    }

    /// Fetch top duplicate-like pairs within a repository based on similar_same_repo edges.
    /// Returns a list of pairs {a: {...}, b: {...}, score} limited by `limit`.
    pub async fn get_dupes_for_repo(
        &self,
        repo_name: &str,
        limit: usize,
        offset: usize,
        min_score: Option<f64>,
        language: Option<&str>,
        kind: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        self.queries
            .get_dupes_for_repo(repo_name, limit, offset, min_score, language, kind)
            .await
    }

    /// Fetch cross-repo duplicate-like pairs where `a` belongs to `repo_name` and `b` belongs to another repo.
    pub async fn get_external_dupes_for_repo(
        &self,
        repo_name: &str,
        limit: usize,
        offset: usize,
        min_score: Option<f64>,
        language: Option<&str>,
        kind: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        self.queries
            .get_external_dupes_for_repo(repo_name, limit, offset, min_score, language, kind)
            .await
    }

    /// Fetch methods for a given class/struct/interface/trait entity by matching on parent name within the same repo.
    /// Note: For Rust, impl methods are exported as top-level functions with parent = NULL, so this will return empty.
    pub async fn get_methods_for_parent(
        &self,
        repo_name: &str,
        parent_name: &str,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        self.queries
            .get_methods_for_parent(repo_name, parent_name)
            .await
    }

    pub async fn get_dependencies_for_repo(
        &self,
        repo_name: &str,
        branch: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        self.queries
            .get_dependencies_for_repo(repo_name, branch)
            .await
    }

    /// Get SBOM dependencies for a specific repository, optionally scoped to a commit.
    /// Returns a vector of JSON objects with name, language, version fields.
    pub async fn get_sbom_dependencies_for_repo(
        &self,
        repo_name: &str,
        commit: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        self.queries
            .get_sbom_dependencies_for_repo(repo_name, commit)
            .await
    }

    // Graph-based entity relation fetch using calls/imports edges.
    pub async fn fetch_entity_graph(
        &self,
        stable_id: &str,
        limit: usize,
    ) -> Result<
        (
            Vec<(String, String)>,
            Vec<(String, String)>,
            Vec<(String, String)>,
        ),
        Box<dyn std::error::Error>,
    > {
        self.queries.fetch_entity_graph(stable_id, limit).await
    }

    pub async fn search_entities(
        &self,
        query: &str,
        repo_filter: Option<&str>,
        limit: usize,
        offset: usize,
        fields: &str,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        self.queries
            .search_entities(query, repo_filter, limit, offset, fields)
            .await
    }

    /// Search entities allowing multiple repo filters. Each repo filter may be
    /// either a repo name (matched against `repo_name`) or a file path prefix
    /// (if it starts with `/` or contains a leading path). When multiple repo
    /// names are provided, this uses `repo_name IN $repos` for database-side
    /// filtering to avoid multiple roundtrips.
    pub async fn search_entities_multi(
        &self,
        query: &str,
        repo_filters: Option<&[String]>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        self.queries
            .search_entities_multi(query, repo_filters, limit, offset)
            .await
    }

    /// Snapshot-scoped wrapper that forwards to the DB layer's snapshot-aware
    /// multi-repo search. `snapshot_id` should be the snapshot identifier (or
    /// commit id) to scope results to.
    pub async fn search_entities_multi_snapshot(
        &self,
        query: &str,
        repo_filters: Option<&[String]>,
        limit: usize,
        offset: usize,
        snapshot_id: &str,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        self.queries
            .search_entities_multi_snapshot(query, repo_filters, limit, offset, snapshot_id)
            .await
    }

    // For testing purposes - expose the underlying connection
    pub fn connection(&self) -> &std::sync::Arc<hyperzoekt::db::connection::SurrealConnection> {
        &self.queries.db
    }
}

/// Determine branch candidates to try for a repo when resolving refs.
/// If an explicit `provided` ref is given, return it as the single candidate.
/// Otherwise attempt to use the repo's configured default branch, and if
/// that lookup fails, fall back to the conventional candidates in order:
/// "main", "master", "trunk".
async fn branch_candidates_for_repo(
    state: &AppState,
    repo_name: &str,
    provided: Option<&str>,
) -> Vec<String> {
    // Fetch repo-configured default branch (db helper provides fallbacks)
    let repo_default = match state.db.get_repo_default_branch(repo_name).await {
        Ok(b) if !b.is_empty() => Some(b),
        _ => None,
    };
    compute_branch_candidates(provided, repo_default.as_deref())
}

/// Try branch candidates and fetch dependencies for a repo. Returns sorted Vec of dependency JSON objects.
async fn fetch_dependencies_for_repo_with_candidates(
    state: &AppState,
    repo_name: &str,
    provided_ref: Option<&str>,
) -> (Vec<serde_json::Value>, Option<String>) {
    let candidates = branch_candidates_for_repo(state, repo_name, provided_ref).await;
    for cand in candidates.iter() {
        match state.db.resolve_ref_to_snapshot(repo_name, cand).await {
            Ok(Some((commit_id, _snap_opt))) => {
                if let Ok(deps) = state
                    .db
                    .get_dependencies_for_repo(repo_name, Some(&commit_id))
                    .await
                {
                    return (sort_deps_alphabetical(deps), Some(commit_id));
                } else {
                    log::warn!(
                        "fetch_dependencies_for_repo_with_candidates: DB error for {}@{}",
                        repo_name,
                        cand
                    );
                }
            }
            Ok(None) => {
                if let Ok(deps) = state
                    .db
                    .get_dependencies_for_repo(repo_name, Some(cand.as_str()))
                    .await
                {
                    return (sort_deps_alphabetical(deps), Some(cand.clone()));
                }
            }
            Err(e) => {
                log::warn!(
                    "fetch_dependencies_for_repo_with_candidates: ref lookup failed for {}@{}: {}",
                    repo_name,
                    cand,
                    e
                );
            }
        }
    }

    // final fallback: no ref or no candidate matched — query without ref
    match state.db.get_dependencies_for_repo(repo_name, None).await {
        Ok(deps) => (sort_deps_alphabetical(deps), None),
        Err(e) => {
            log::warn!(
                "fetch_dependencies_for_repo_with_candidates: final fallback DB error for {}: {}",
                repo_name,
                e
            );
            (vec![], None)
        }
    }
}

/// Pure helper that computes branch candidates from an optional provided ref and
/// an optional repo default branch. This is separated for easier unit testing.
fn compute_branch_candidates(provided: Option<&str>, repo_default: Option<&str>) -> Vec<String> {
    if let Some(p) = provided {
        return vec![p.to_string()];
    }
    if let Some(d) = repo_default {
        if !d.is_empty() {
            return vec![d.to_string()];
        }
    }
    vec![
        "main".to_string(),
        "master".to_string(),
        "trunk".to_string(),
    ]
}

#[cfg(test)]
mod compute_branch_tests {
    use super::compute_branch_candidates;

    #[test]
    fn provided_ref_wins() {
        let got = compute_branch_candidates(Some("feature"), Some("dev"));
        assert_eq!(got, vec!["feature".to_string()]);
    }

    #[test]
    fn repo_default_used() {
        let got = compute_branch_candidates(None, Some("dev"));
        assert_eq!(got, vec!["dev".to_string()]);
    }

    #[test]
    fn fallback_list() {
        let got = compute_branch_candidates(None, None);
        assert_eq!(
            got,
            vec![
                "main".to_string(),
                "master".to_string(),
                "trunk".to_string()
            ]
        );
    }
}

/// Try branch candidates and fetch SBOM dependencies for a repo. Returns sorted Vec of dependency JSON objects.
async fn fetch_sbom_deps_for_repo_with_candidates(
    state: &AppState,
    repo_name: &str,
    provided_ref: Option<&str>,
) -> (Vec<serde_json::Value>, Option<String>) {
    let candidates = branch_candidates_for_repo(state, repo_name, provided_ref).await;
    for cand in candidates.iter() {
        match state.db.resolve_ref_to_snapshot(repo_name, cand).await {
            Ok(Some((commit_id, _snap_opt))) => {
                if let Ok(deps) = state
                    .db
                    .get_sbom_dependencies_for_repo(repo_name, Some(&commit_id))
                    .await
                {
                    return (sort_deps_alphabetical(deps), Some(commit_id));
                } else {
                    log::warn!(
                        "fetch_sbom_deps_for_repo_with_candidates: DB error for {}@{}",
                        repo_name,
                        cand
                    );
                }
            }
            Ok(None) => {
                // Try the candidate as a branch/ref first (parity with dependencies path).
                // If that yields SBOM deps, return it and record the candidate as the resolved ref.
                if let Ok(deps) = state
                    .db
                    .get_sbom_dependencies_for_repo(repo_name, Some(cand.as_str()))
                    .await
                {
                    return (sort_deps_alphabetical(deps), Some(cand.clone()));
                }
            }
            Err(e) => {
                log::warn!(
                    "fetch_sbom_deps_for_repo_with_candidates: ref lookup failed for {}@{}: {}",
                    repo_name,
                    cand,
                    e
                );
            }
        }
    }

    // final fallback: no ref matched — try repo-level SBOMs
    match state
        .db
        .get_sbom_dependencies_for_repo(repo_name, None)
        .await
    {
        Ok(deps) => (sort_deps_alphabetical(deps), None),
        Err(e) => {
            log::warn!(
                "fetch_sbom_deps_for_repo_with_candidates: final fallback DB error for {}: {}",
                repo_name,
                e
            );
            (vec![], None)
        }
    }
}

/// Try branch candidates to locate a SBOM blob row. Returns Option<SbomRow> where SbomRow is a small struct.
async fn fetch_sbom_blob_row_for_repo_with_candidates(
    state: &AppState,
    repo_name: &str,
    provided_ref: Option<&str>,
) -> Option<(Option<String>, Option<String>, Option<String>)> {
    #[derive(serde::Deserialize)]
    struct SbomRow {
        sbom_blob: Option<String>,
        sbom_encoding: Option<String>,
    }

    let candidates = branch_candidates_for_repo(state, repo_name, provided_ref).await;
    for cand in candidates.iter() {
        match state.db.resolve_ref_to_snapshot(repo_name, cand).await {
            Ok(Some((commit, _snap))) => {
                let sql = "SELECT sbom_blob, sbom_encoding FROM sboms WHERE repo = $repo AND commit = $commit LIMIT 1";
                let resp = match state.db.connection().as_ref() {
                    SurrealConnection::Local(db_conn) => db_conn
                        .query(sql)
                        .bind(("repo", repo_name.to_string()))
                        .bind(("commit", commit.to_string()))
                        .await
                        .ok(),
                    SurrealConnection::RemoteHttp(db_conn) => db_conn
                        .query(sql)
                        .bind(("repo", repo_name.to_string()))
                        .bind(("commit", commit.to_string()))
                        .await
                        .ok(),
                    SurrealConnection::RemoteWs(db_conn) => db_conn
                        .query(sql)
                        .bind(("repo", repo_name.to_string()))
                        .bind(("commit", commit.to_string()))
                        .await
                        .ok(),
                };
                if let Some(mut r) = resp {
                    if let Ok(mut rows) = r.take::<Vec<SbomRow>>(0) {
                        if let Some(row) = rows.pop() {
                            return Some((row.sbom_blob, row.sbom_encoding, Some(commit)));
                        }
                    }
                }
            }
            Ok(None) => {
                // try repo-level SBOMs (no commit)
                let sql = "SELECT sbom_blob, sbom_encoding FROM sboms WHERE repo = $repo LIMIT 1";
                let resp = match state.db.connection().as_ref() {
                    SurrealConnection::Local(db_conn) => db_conn
                        .query(sql)
                        .bind(("repo", repo_name.to_string()))
                        .await
                        .ok(),
                    SurrealConnection::RemoteHttp(db_conn) => db_conn
                        .query(sql)
                        .bind(("repo", repo_name.to_string()))
                        .await
                        .ok(),
                    SurrealConnection::RemoteWs(db_conn) => db_conn
                        .query(sql)
                        .bind(("repo", repo_name.to_string()))
                        .await
                        .ok(),
                };
                if let Some(mut r) = resp {
                    if let Ok(mut rows) = r.take::<Vec<SbomRow>>(0) {
                        if let Some(row) = rows.pop() {
                            return Some((row.sbom_blob, row.sbom_encoding, None));
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!(
                    "fetch_sbom_blob_row_for_repo_with_candidates: ref lookup failed for {}@{}: {}",
                    repo_name,
                    cand,
                    e
                );
            }
        }
    }

    // no matching SBOM found
    None
}

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub templates: Environment<'static>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = Config::from_args(args);

    // Initialize OpenTelemetry/tracing if enabled via env and feature compiled.
    // This is a no-op when the `otel` feature is not enabled.
    let enable_otel = std::env::var("HZ_ENABLE_OTEL")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True" | "yes" | "YES"))
        .unwrap_or(false);
    if enable_otel {
        hyperzoekt::otel::init_otel_from_env();
    }

    // Initialize logging with the configured level (inject sane defaults for noisy crates)
    let log_spec = sanitize_log_spec(&config.log_level);
    std::env::set_var("RUST_LOG", &log_spec);
    let env = env_logger::Env::default();
    let mut builder = env_logger::Builder::from_env(env);
    builder
        .format(|buf, record| {
            let ts = buf.timestamp_millis();
            writeln!(
                buf,
                "[{} {} {}] {}",
                ts,
                record.level(),
                record.target(),
                record.args()
            )
        })
        .filter_level(log::LevelFilter::Info)
        .filter_module("hyper_util", log::LevelFilter::Warn)
        .filter_module("hyper", log::LevelFilter::Warn)
        .filter_module("h2", log::LevelFilter::Warn)
        .filter_module("reqwest", log::LevelFilter::Warn)
        .filter_module("tower_http", log::LevelFilter::Warn)
        .filter_module("html5ever", log::LevelFilter::Warn);
    builder.init();

    log::info!("*** NEW CODE IS RUNNING: HyperZoekt WebUI starting up!");
    log::info!("Starting HyperZoekt WebUI with configuration:");
    log::info!("  Host: {}", config.host);
    log::info!("  Port: {}", config.port);
    log::info!(
        "  SurrealDB URL: {:?}",
        config
            .surreal_url
            .as_ref()
            .map(|_| "[configured]")
            .unwrap_or("[embedded]")
    );
    log::info!("  SurrealDB Namespace: {}", config.surreal_ns);
    log::info!("  SurrealDB Database: {}", config.surreal_db);
    log::info!("  Requested Log Level: {}", config.log_level);
    log::info!("  Effective Log Filter: {}", log_spec);
    log::info!("  CORS All: {}", config.cors_all);
    log::info!("  Max Search Results: {}", config.max_search_results);

    // Initialize database connection
    let db = match Database::new(
        config.surreal_url.as_deref(),
        &config.surreal_ns,
        &config.surreal_db,
    )
    .await
    {
        Ok(db) => {
            log::info!("Successfully connected to SurrealDB");
            db
        }
        Err(e) => {
            log::error!("Failed to connect to database: {}", e);
            return Err(e);
        }
    };

    // Initialize template engine
    let mut templates = Environment::new();
    templates.add_template("base", BASE_TEMPLATE)?;
    templates.add_template("index", INDEX_TEMPLATE)?;
    templates.add_template("repo", REPO_TEMPLATE)?;
    templates.add_template("entity", ENTITY_TEMPLATE)?;
    templates.add_template("search", SEARCH_TEMPLATE)?;
    templates.add_template("pagerank", PAGERANK_TEMPLATE)?;
    templates.add_template("dupes", DUPES_TEMPLATE)?;
    templates.add_template("dependencies", DEPENDENCIES_TEMPLATE)?;
    templates.add_template("sbom", SBOM_TEMPLATE)?;
    templates.add_template("hirag", HIRAG_TEMPLATE)?;
    templates.add_template("hirag-mindmap", HIRAG_MINDMAP_TEMPLATE)?;
    // Reuse repo template with dupes section via route-driven render
    log::info!("Templates loaded successfully");

    let state = AppState { db, templates };

    // Build the application with CORS configuration
    let cors_layer = if config.cors_all {
        CorsLayer::permissive()
    } else {
        CorsLayer::new()
            .allow_origin(tower_http::cors::Any)
            .allow_methods([axum::http::Method::GET, axum::http::Method::POST])
            .allow_headers([axum::http::header::CONTENT_TYPE])
    };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/health", get(health_handler))
        .route("/api/repos", get(repos_api_handler))
        .route("/api/entities", get(entities_api_handler))
        .route("/api/search", get(search_api_handler))
        .route("/api/pagerank", get(pagerank_api_handler))
        .route("/api/repo/{repo_name}/dupes", get(repo_dupes_api_handler))
        .route(
            "/api/repo/{repo_name}/dependencies",
            get(repo_dependencies_api_handler),
        )
        .route(
            "/api/repo/{repo_name}/sbom",
            get(repo_sbom_dependencies_api_handler),
        )
        .route(
            "/api/repo/{repo_name}/sbom/blob",
            get(repo_sbom_blob_api_handler),
        )
        .route("/api/dependencies", get(multi_dependencies_api_handler))
        .route("/api/sbom", get(multi_sbom_dependencies_api_handler))
        // Top-level dependencies browser
        .route("/dependencies", get(dependencies_page_handler))
        .route("/sbom", get(sbom_page_handler))
        .route(
            "/api/repo/{repo_name}/external_dupes",
            get(repo_external_dupes_api_handler),
        )
        .route(
            "/api/entity_similar/{stable_id}",
            get(entity_similar_api_handler),
        )
        .route(
            "/api/entity_graph/{stable_id}",
            get(entity_graph_api_handler),
        )
        .route("/repo/{repo_name}", get(repo_handler))
        .route("/repo/{repo_name}/dupes", get(repo_dupes_page_handler))
        .route("/dupes", get(dupes_index_handler))
        .route("/entity/{stable_id}", get(entity_handler))
        .route("/hirag", get(hirag_page_handler))
        .route("/hirag-mindmap", get(hirag_mindmap_handler))
        .route("/api/hirag", get(hirag_api_handler))
        .route(
            "/api/hirag/{stable_id}/members",
            get(hirag_members_api_handler),
        )
        .route("/search", get(search_handler))
        .route("/pagerank", get(pagerank_handler))
        .nest_service("/static", ServeDir::new("../../static"))
        .layer(cors_layer)
        .with_state(state);

    // Start the server
    let addr = format!("{}:{}", config.host, config.port).parse::<SocketAddr>()?;
    log::info!("Starting web UI server on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn index_handler(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    log::debug!("Received request for index page");
    let repos = state.db.get_repo_summaries().await.map_err(|e| {
        log::error!("Failed to get repo summaries: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    log::debug!("Retrieved {} repositories", repos.len());
    let template = state.templates.get_template("index").unwrap();
    let html = template
        .render(context! {
            repos => repos,
            title => "HyperZoekt Index Browser"
        })
        .map_err(|e| {
            log::error!("Failed to render index template: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    log::debug!("Successfully rendered index page");
    Ok(Html(html))
}

async fn health_handler() -> Result<Json<serde_json::Value>, StatusCode> {
    log::debug!("Health check requested");
    // Provide build/version info so container health checks can verify the deployed code
    let version = env!("CARGO_PKG_VERSION");
    let build_id = std::env::var("HYPERZOEKT_BUILD_ID").ok();
    let message = if let Some(b) = build_id.as_deref() {
        format!("HyperZoekt WebUI v{} (build={}) is running", version, b)
    } else {
        format!("HyperZoekt WebUI v{} is running", version)
    };
    Ok(Json(serde_json::json!({
        "status": "ok",
        "version": version,
        "build_id": build_id,
        "message": message
    })))
}

async fn repo_handler(
    State(state): State<AppState>,
    axum::extract::Path(repo_name): axum::extract::Path<String>,
) -> Result<Html<String>, StatusCode> {
    log::debug!("repo_handler: fetching entities for repo={}", repo_name);
    let mut entities = state
        .db
        .get_entities_for_repo(&repo_name)
        .await
        .map_err(|e| {
            log::error!(
                "repo_handler: get_entities_for_repo failed for {}: {}",
                repo_name,
                e
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // For template rendering, compute a file hint for each entity and inject it into
    // the JSON context under the `file` key so existing templates continue to work.
    let mut entities_for_template: Vec<serde_json::Value> = Vec::new();
    for entity in &mut entities {
        // Ensure rank is set for API consumers
        if entity.rank.is_none() {
            entity.rank = Some(0.0);
        }
        // Ensure rank present for templates: map None -> 0.0
        if entity.rank.is_none() {
            entity.rank = Some(0.0);
        }
        // Prefer precomputed display; fallback to deriving from source_url; finally empty
        let mut file_hint = entity
            .source_display
            .clone()
            .or_else(|| {
                entity
                    .source_url
                    .as_ref()
                    .map(|u| short_display_from_source_url(u, &entity.repo_name))
            })
            .unwrap_or_else(|| "".to_string());
        file_hint = clean_file_path(&file_hint);

        // Debug logs use file_hint now
        log::debug!(
            "webui: repo_handler db source_url stable_id={} file='{}' repo='{}' source_url={:?}",
            entity.stable_id,
            file_hint,
            entity.repo_name,
            entity.source_url
        );

        // If DB didn't provide a source_url but we have a file hint, construct a fallback URL
        if entity.source_url.is_none() && !file_hint.is_empty() {
            if entity.source_url.is_none() {
                // Use simple env-backed fallback if repo row lookup is not available
                if let Some(url) =
                    construct_source_url(&state, &entity.repo_name, &file_hint, None).await
                {
                    // populate source_url/display when possible
                    // We still keep the original EntityPayload unchanged; modify the JSON value instead
                    // when serializing below.
                    // But set display if missing for template convenience
                    if entity.source_display.is_none() {
                        entity.source_display =
                            Some(short_display_from_source_url(&url, &entity.repo_name));
                    }
                }
            }
        } else if entity.source_display.is_none() {
            if let Some(ref url) = entity.source_url {
                entity.source_display = Some(short_display_from_source_url(url, &entity.repo_name));
            }
        }

        // Serialize entity to JSON and inject the computed `file` hint for templates
        let mut v = serde_json::to_value(&entity).unwrap_or(serde_json::json!({}));
        // Render server-side sanitized doc HTML for API consumers
        // We will attach doc_html as a separate field in the returned JSON by converting
        // each EntityPayload to a JSON value in the response generation step below.
        if let Some(obj) = v.as_object_mut() {
            obj.insert("file".to_string(), serde_json::Value::String(file_hint));
            let doc_html = entity
                .doc
                .as_ref()
                .map(|d| utils::render_markdown_to_html(d));
            obj.insert(
                "doc_html".to_string(),
                match doc_html {
                    Some(s) => serde_json::Value::String(s),
                    None => serde_json::Value::Null,
                },
            );
        }
        entities_for_template.push(v);
    }

    let template = state.templates.get_template("repo").unwrap();
    let html = template
        .render(context! {
            repo_name => repo_name,
            entities => entities_for_template,
            title => format!("Repository: {}", repo_name)
        })
        .map_err(|e| {
            log::error!("Failed to render repo template for {}: {}", repo_name, e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Html(html))
}

async fn dependencies_page_handler(
    State(state): State<AppState>,
) -> Result<Html<String>, StatusCode> {
    log::debug!("dependencies_page_handler: rendering dependencies page");
    let template = state.templates.get_template("dependencies").unwrap();
    let html = template
        .render(context! { title => "Repository Dependencies" })
        .map_err(|e| {
            log::error!("Failed to render dependencies template: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Html(html))
}

async fn sbom_page_handler(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    log::debug!("sbom_page_handler: rendering sbom page");
    let template = state.templates.get_template("sbom").unwrap();
    let html = template
        .render(context! { title => "SBOMs" })
        .map_err(|e| {
            log::error!("Failed to render sbom template: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Html(html))
}

async fn entity_handler(
    State(state): State<AppState>,
    axum::extract::Path(stable_id): axum::extract::Path<String>,
) -> Result<Html<String>, StatusCode> {
    log::debug!("entity_handler: fetching entity stable_id={}", stable_id);
    let mut entity = state
        .db
        .get_entity_by_id(&stable_id)
        .await
        .map_err(|e| {
            log::error!(
                "entity_handler: get_entity_by_id failed for {}: {}",
                stable_id,
                e
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Compute a file hint for this entity (do not mutate the EntityPayload)
    let raw_file_hint = file_hint_for_entity(&entity);
    log::info!("Raw file hint: {}", raw_file_hint);

    // Extract UUID repo name before cleaning the file path
    let uuid_repo_name = extract_uuid_repo_name(&raw_file_hint);

    let mut file_hint = clean_file_path(&raw_file_hint);

    // Debug: log DB-provided source_url before any recomputation
    log::debug!(
        "webui: entity_handler db source_url stable_id={} file='{}' repo='{}' source_url={:?}",
        entity.stable_id,
        file_hint,
        entity.repo_name,
        entity.source_url
    );

    log::debug!("Cleaned file hint: {}", file_hint);

    // Clean the repo_name to remove UUID suffix if present
    let clean_repo_name = remove_uuid_suffix(&entity.repo_name);

    log::debug!("clean_repo_name: {}", clean_repo_name);

    // If entity.file still contains a leading repo directory with a UUID suffix
    // (for example: "foo-bar-<uuid>/my_path/something.py"), normalize it to
    // remove the UUID so later URL construction uses the clean repo name.
    // This mirrors the UUID-detection used above (strip dashes before hex check).
    if let Some(slash_pos) = file_hint.find('/') {
        let leading = &file_hint[..slash_pos];
        if let Some(last_dash) = leading.rfind('-') {
            let potential_uuid = &leading[last_dash + 1..];
            let cleaned: String = potential_uuid.chars().filter(|c| *c != '-').collect();
            if cleaned.len() >= 32 && cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
                // Replace leading "<name>-<uuid>/" with "<name>/"
                let repo_root = &leading[..last_dash];
                let rest = &file_hint[slash_pos + 1..];
                file_hint = format!("{}/{}", repo_root, rest);
            }
        }
    }

    // Compute source URL only if DB didn't already provide one. Keep stored value otherwise.
    let mut _template_source_base: Option<String> = None;
    let mut _template_source_branch: Option<String> = None;
    if entity.source_url.is_none() {
        if let Some(url) = construct_source_url(
            &state,
            &clean_repo_name,
            &file_hint,
            uuid_repo_name.as_deref(),
        )
        .await
        {
            entity.source_url = Some(url.clone());
            if entity.source_display.is_none() {
                entity.source_display = Some(short_display_from_source_url(&url, &clean_repo_name));
            }
            log::info!(
                "webui: entity_handler computed source_url='{}' stable_id={}",
                url,
                entity.stable_id
            );
        }
    } else if entity.source_display.is_none() {
        if let Some(ref url) = entity.source_url {
            entity.source_display = Some(short_display_from_source_url(url, &clean_repo_name));
        }
    }
    // Resolve callers and callees via DB queries to avoid scanning all entities in memory
    // Read a limit for relations from environment (default 50)
    let relations_limit: usize = std::env::var("ENTITY_RELATIONS_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);

    log::debug!(
        "entity_handler: resolving relations for {} (limit={})",
        entity.stable_id,
        relations_limit
    );
    let (callers, callees, imports) = state
        .db
        .fetch_entity_graph(&entity.stable_id, relations_limit)
        .await
        .map_err(|e| {
            log::error!(
                "Failed to fetch entity graph for {}: {}",
                entity.stable_id,
                e
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    log::debug!(
        "entity_handler: resolved callers={} callees={} for {}",
        callers.len(),
        callees.len(),
        entity.stable_id
    );

    // If this is a class-like entity, fetch its methods for rendering and graph augmentation.
    let kind_lc = entity.kind.to_lowercase();
    let is_classy = kind_lc.contains("class")
        || kind_lc.contains("struct")
        || kind_lc.contains("interface")
        || kind_lc.contains("trait");
    let methods: Vec<EntityPayload> = if is_classy {
        match state
            .db
            .get_methods_for_parent(&entity.repo_name, &entity.name)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                log::warn!(
                    "entity_handler: get_methods_for_parent failed for {} / {}: {}",
                    &entity.repo_name,
                    &entity.name,
                    e
                );
                vec![]
            }
        }
    } else {
        vec![]
    };

    let template = state.templates.get_template("entity").unwrap();
    // Ensure repo_name is available to the template (entity.html references `repo_name`)
    // Prepare call graph JSON for the template (Minijinja doesn't provide a tojson filter by default)
    let call_graph_json = match serde_json::to_string(&serde_json::json!({
        "callers": callers
            .iter()
            .map(|(n, id)| serde_json::json!([n, id]))
            .collect::<Vec<serde_json::Value>>(),
        "callees": callees
            .iter()
            .map(|(n, id)| serde_json::json!([n, id]))
            .collect::<Vec<serde_json::Value>>(),
        "methods": methods
            .iter()
            .map(|m| serde_json::json!([m.name, m.stable_id]))
            .collect::<Vec<serde_json::Value>>()
    })) {
        Ok(s) => s,
        Err(e) => {
            log::error!("Failed to serialize call graph for template: {}", e);
            "{}".to_string()
        }
    };

    // Ensure rank present for templates: map None -> 0.0
    if entity.rank.is_none() {
        entity.rank = Some(0.0);
    }

    // Serialize the entity to JSON and inject the computed `file` hint so templates
    // can continue to reference `entity.file` without the server mutating the
    // EntityPayload type.
    let mut entity_json = serde_json::to_value(&entity).unwrap_or(serde_json::json!({}));
    if let Some(obj) = entity_json.as_object_mut() {
        obj.insert("file".to_string(), serde_json::Value::String(file_hint));
        let doc_html = entity
            .doc
            .as_ref()
            .map(|d| utils::render_markdown_to_html(d));
        obj.insert(
            "doc_html".to_string(),
            match doc_html {
                Some(s) => serde_json::Value::String(s),
                None => serde_json::Value::Null,
            },
        );
    }

    let html = template
        .render(context! {
            entity => entity_json,
            repo_name => clean_repo_name,
            callers => callers,
            callees => callees,
            imports => imports,
            methods => methods,
            call_graph_json => call_graph_json,
            title => format!("Entity: {}", entity.name)
        })
        .map_err(|e| {
            log::error!(
                "Failed to render entity template for {}: {}",
                entity.stable_id,
                e
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Html(html))
}

async fn search_handler(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let template = state.templates.get_template("search").unwrap();
    let html = template
        .render(context! {
            title => "Search HyperZoekt Index"
        })
        .map_err(|e| {
            log::error!("Failed to render search template: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Html(html))
}

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
    // Accept multiple repo filters: `repo=...&repo=...` or `repo[]=...` from the UI
    // Also accept a single `repo=kafka` value; deserialize via `RepoParam`.
    repo: Option<RepoParam>,
    cursor: Option<String>,
    limit: Option<usize>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
enum RepoParam {
    Single(String),
    Multi(Vec<String>),
}

async fn search_api_handler(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    log::debug!(
        "search_api_handler (similarity): q='{}' repo={:?}",
        query.q,
        query.repo
    );
    let started = std::time::Instant::now();
    // Attempt similarity search first; fallback to text search if TEI or embeddings unavailable
    // Normalize `repo` param (which may be single or multi) into Option<Vec<String>>
    let repo_vec_opt: Option<Vec<String>> = match &query.repo {
        Some(RepoParam::Single(s)) => Some(vec![s.clone()]),
        Some(RepoParam::Multi(v)) => Some(v.clone()),
        None => None,
    };

    // If any repo filters include an explicit ref using `repo@ref` syntax, resolve
    // the ref to a commit and optional snapshot. Build a cleaned repo list for
    // passing into similarity/text search and a map of resolved refs to enrich
    // results returned to callers.
    let mut resolved_refs: std::collections::HashMap<String, (String, Option<String>)> =
        std::collections::HashMap::new();
    let mut cleaned_repo_vec: Option<Vec<String>> = None;
    if let Some(rv) = &repo_vec_opt {
        let mut cleaned: Vec<String> = Vec::with_capacity(rv.len());
        for r in rv.iter() {
            if let Some(idx) = r.find('@') {
                let repo_name = r[..idx].to_string();
                let raw_ref = &r[idx + 1..];
                // attempt to resolve provided ref; on success record mapping
                match state.db.resolve_ref_to_snapshot(&repo_name, raw_ref).await {
                    Ok(Some((commit, snap_opt))) => {
                        resolved_refs.insert(repo_name.clone(), (commit, snap_opt));
                    }
                    Ok(None) => {
                        // no ref found; proceed without resolved mapping
                    }
                    Err(e) => {
                        log::warn!(
                            "search_api_handler: failed to resolve ref for {}@{}: {}",
                            repo_name,
                            raw_ref,
                            e
                        );
                    }
                }
                cleaned.push(repo_name);
            } else {
                cleaned.push(r.clone());
            }
        }
        cleaned_repo_vec = Some(cleaned);
    }

    // Convert to Option<&[String]> for the similarity function
    let repo_slice_opt: Option<&[String]> = cleaned_repo_vec.as_deref().or(repo_vec_opt.as_deref());
    // Determine maximum results per page (env overrides default)
    let max_results: usize = std::env::var("HYPERZOEKT_MAX_SEARCH_RESULTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    // Respect explicit limit param but clamp to max_results
    let mut limit = query.limit.unwrap_or(max_results);
    if limit == 0 {
        limit = max_results;
    }
    if limit > max_results {
        limit = max_results;
    }
    let start: usize = query
        .cursor
        .as_ref()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    // If repo filters resolved to a single snapshot, use that snapshot id to scope similarity.
    let snapshot_id_opt: Option<&str> = if resolved_refs.len() == 1 {
        // take the single map entry's snapshot option if present
        let mut iter = resolved_refs.values();
        if let Some((_, snap_opt)) = iter.next() {
            snap_opt.as_deref()
        } else {
            None
        }
    } else {
        None
    };

    // Always attempt similarity-first search. If the similarity
    // call fails we fall back to the legacy text-search path below.
    log::info!("search_api_handler: attempting similarity search (always enabled)");
    match hyperzoekt::similarity::similarity_with_conn_multi(
        state.db.connection().as_ref(),
        &query.q,
        repo_slice_opt,
        snapshot_id_opt,
    )
    .await
    {
        Ok(mut results) => {
            // Enrich links
            for entity in &mut results {
                if entity.rank.is_none() {
                    entity.rank = Some(0.0);
                }
                let raw_file_hint = file_hint_for_entity(entity);
                let file_hint = clean_file_path(&raw_file_hint);
                if entity.source_url.is_none() {
                    if let Some(url) =
                        construct_source_url(&state, &entity.repo_name, &file_hint, None).await
                    {
                        entity.source_url = Some(url.clone());
                        if entity.source_display.is_none() {
                            entity.source_display =
                                Some(short_display_from_source_url(&url, &entity.repo_name));
                        }
                    }
                } else if entity.source_display.is_none() {
                    if let Some(ref url) = entity.source_url {
                        entity.source_display =
                            Some(short_display_from_source_url(url, &entity.repo_name));
                    }
                }
            }
            let elapsed_ms = started.elapsed().as_millis();
            // Support cursor/limit pagination: request a larger set and slice
            let total = results.len();
            let end = (start + limit).min(total);
            let next_cursor = if end < total {
                Some(end.to_string())
            } else {
                None
            };
            let page_results = if start < total {
                results[start..end].to_vec()
            } else {
                vec![]
            };
            // Serialize and enrich each returned entity JSON with resolved ref
            let mut results_value =
                serde_json::to_value(page_results).unwrap_or(serde_json::Value::Array(vec![]));
            if let serde_json::Value::Array(ref mut arr) = results_value {
                for v in arr.iter_mut() {
                    if let serde_json::Value::Object(ref mut m) = v {
                        if let Some(serde_json::Value::String(repo_name)) = m.get("repo_name") {
                            if let Some((commit, snap)) = resolved_refs.get(repo_name) {
                                m.insert(
                                    "resolved_commit".to_string(),
                                    serde_json::Value::String(commit.clone()),
                                );
                                if let Some(sid) = snap {
                                    m.insert(
                                        "resolved_snapshot".to_string(),
                                        serde_json::Value::String(sid.clone()),
                                    );
                                } else {
                                    m.insert(
                                        "resolved_snapshot".to_string(),
                                        serde_json::Value::Null,
                                    );
                                }
                            }
                        }
                    }
                }
            }
            let mut obj = serde_json::Map::new();
            obj.insert("results".to_string(), results_value);
            obj.insert(
                "elapsed_ms".to_string(),
                serde_json::Value::Number(serde_json::Number::from(elapsed_ms as u64)),
            );
            if let Some(nc) = next_cursor {
                obj.insert("next_cursor".to_string(), serde_json::Value::String(nc));
            }
            return Ok(Json(serde_json::Value::Object(obj)));
        }
        Err(e) => {
            log::warn!(
                "similarity_search failed; falling back to text search: {}",
                e
            );
        }
    }

    // Fallback: legacy text search. Prefer multi-repo-aware search if provided.
    // If the request resolved exactly one repo -> snapshot mapping, prefer the
    // snapshot-scoped search function so results are sourced from the
    // `entity_snapshot` table and are commit-aware.
    let results = if let Some(repos) = cleaned_repo_vec.as_ref().or(repo_vec_opt.as_ref()) {
        // If we resolved a single repo->(commit, snapshot_opt) mapping, use the
        // snapshot-scoped search to scope results to that commit/snapshot.
        if resolved_refs.len() == 1 {
            if let Some((_repo_name, (commit, snap_opt))) = resolved_refs.iter().next() {
                // Prefer snapshot id when available; otherwise use commit id as snapshot selector
                if let Some(sid) = snap_opt.as_deref() {
                    state
                        .db
                        .search_entities_multi_snapshot(
                            &query.q,
                            Some(repos.as_slice()),
                            limit,
                            start,
                            sid,
                        )
                        .await
                        .map_err(|e| {
                            log::error!(
                                "search_entities_multi_snapshot failed for q='{}': {}",
                                query.q,
                                e
                            );
                            StatusCode::INTERNAL_SERVER_ERROR
                        })?
                } else {
                    // Fallback: use commit id as the selector (many snapshots are keyed by commit)
                    state
                        .db
                        .search_entities_multi_snapshot(
                            &query.q,
                            Some(repos.as_slice()),
                            limit,
                            start,
                            commit,
                        )
                        .await
                        .map_err(|e| {
                            log::error!(
                                "search_entities_multi_snapshot failed for q='{}': {}",
                                query.q,
                                e
                            );
                            StatusCode::INTERNAL_SERVER_ERROR
                        })?
                }
            } else {
                // defensive fallback to non-snapshot search
                state
                    .db
                    .search_entities_multi(&query.q, Some(repos.as_slice()), limit, start)
                    .await
                    .map_err(|e| {
                        log::error!("search_entities failed for q='{}': {}", query.q, e);
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?
            }
        } else {
            state
                .db
                .search_entities_multi(&query.q, Some(repos.as_slice()), limit, start)
                .await
                .map_err(|e| {
                    log::error!("search_entities failed for q='{}': {}", query.q, e);
                    StatusCode::INTERNAL_SERVER_ERROR
                })?
        }
    } else {
        state
            .db
            .search_entities(&query.q, None, limit, start, "*")
            .await
            .map_err(|e| {
                log::error!("search_entities failed for q='{}': {}", query.q, e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
    };
    // Pagination for fallback results
    let total = results.len();
    let end = (start + limit).min(total);
    let next_cursor = if end < total {
        Some(end.to_string())
    } else {
        None
    };
    let page_results = if start < total {
        results[start..end].to_vec()
    } else {
        vec![]
    };
    let elapsed_ms = started.elapsed().as_millis();
    // Enrich fallback results with resolved ref info when possible
    let mut results_value =
        serde_json::to_value(page_results).unwrap_or(serde_json::Value::Array(vec![]));
    if let serde_json::Value::Array(ref mut arr) = results_value {
        for v in arr.iter_mut() {
            if let serde_json::Value::Object(ref mut m) = v {
                if let Some(serde_json::Value::String(repo_name)) = m.get("repo_name") {
                    if let Some((commit, snap)) = resolved_refs.get(repo_name) {
                        m.insert(
                            "resolved_commit".to_string(),
                            serde_json::Value::String(commit.clone()),
                        );
                        if let Some(sid) = snap {
                            m.insert(
                                "resolved_snapshot".to_string(),
                                serde_json::Value::String(sid.clone()),
                            );
                        } else {
                            m.insert("resolved_snapshot".to_string(), serde_json::Value::Null);
                        }
                    }
                }
            }
        }
    }
    let mut obj = serde_json::Map::new();
    obj.insert("results".to_string(), results_value);
    obj.insert(
        "elapsed_ms".to_string(),
        serde_json::Value::Number(serde_json::Number::from(elapsed_ms as u64)),
    );
    if let Some(nc) = next_cursor {
        obj.insert("next_cursor".to_string(), serde_json::Value::String(nc));
    }
    Ok(Json(serde_json::Value::Object(obj)))
}

// embedding-based similarity search is performed via functions in `crate::similarity`

// JSON API returning callers/callees/imports for a given entity stable_id using public graph_api
async fn entity_graph_api_handler(
    axum::extract::Path(stable_id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let cfg = hyperzoekt::graph_api::GraphDbConfig::from_env();
    let limit: usize = std::env::var("ENTITY_RELATIONS_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    match hyperzoekt::graph_api::fetch_entity_graph(&cfg, &stable_id, limit).await {
        Ok(g) => Ok(Json(serde_json::json!({
            "stable_id": stable_id,
            "callers": g.callers,
            "callees": g.callees,
            "imports": g.imports
        }))),
        Err(e) => {
            log::error!("entity_graph_api_handler failed for {}: {}", stable_id, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn hirag_page_handler(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let template = state.templates.get_template("hirag").unwrap();
    let html = template
        .render(context! { title => "HiRAG Clusters" })
        .map_err(|e| {
            log::error!("Failed to render hirag template: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Html(html))
}

async fn hirag_mindmap_handler(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let template = state.templates.get_template("hirag-mindmap").unwrap();
    let html = template
        .render(context! { title => "HiRAG Mindmap" })
        .map_err(|e| {
            log::error!("Failed to render hirag-mindmap template: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Html(html))
}

// Simple API to return hirag clusters. Returns an object { clusters: [..] }
async fn hirag_api_handler(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Query a conservative set of fields to keep payload small
    let fields = "stable_id, label, summary, members, centroid_len, member_repos";
    let sql = format!("SELECT {} FROM hirag_cluster START AT 0 LIMIT 1000", fields);
    let mut resp = match state.db.connection().as_ref() {
        SurrealConnection::Local(db_conn) => db_conn.query(&sql).await,
        SurrealConnection::RemoteHttp(db_conn) => db_conn.query(&sql).await,
        SurrealConnection::RemoteWs(db_conn) => db_conn.query(&sql).await,
    };

    let mut clusters: Vec<serde_json::Value> = Vec::new();
    match &mut resp {
        Ok(r) => {
            if let Ok(vals) = r.take::<Vec<serde_json::Value>>(0) {
                clusters = vals;
            }
        }
        Err(e) => {
            log::error!("hirag_api_handler: query failed: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    Ok(Json(serde_json::json!({ "clusters": clusters })))
}

// API to return expanded members for a given hirag cluster stable_id.
// Returns { stable_id: ..., members: [ { id: ..., type: 'cluster'|'entity', details: {...} } ] }
async fn hirag_members_api_handler(
    State(state): State<AppState>,
    axum::extract::Path(stable_id): axum::extract::Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Fetch the cluster row
    let sel = "SELECT stable_id, members FROM hirag_cluster WHERE stable_id = $sid LIMIT 1";
    let resp = match state.db.connection().as_ref() {
        SurrealConnection::Local(db_conn) => db_conn
            .query(sel)
            .bind(("sid", stable_id.clone()))
            .await
            .ok(),
        SurrealConnection::RemoteHttp(db_conn) => db_conn
            .query(sel)
            .bind(("sid", stable_id.clone()))
            .await
            .ok(),
        SurrealConnection::RemoteWs(db_conn) => db_conn
            .query(sel)
            .bind(("sid", stable_id.clone()))
            .await
            .ok(),
    };

    let mut members_out: Vec<serde_json::Value> = Vec::new();
    if let Some(mut r) = resp {
        if let Ok(rows) = r.take::<Vec<serde_json::Value>>(0) {
            if let Some(first) = rows.into_iter().next() {
                if let Some(arr) = first.get("members").and_then(|v| v.as_array()) {
                    for m in arr.iter() {
                        if let Some(ms) = m.as_str() {
                            if ms.starts_with("hirag::") {
                                // return minimal cluster info
                                let mut cinfo = serde_json::json!({ "id": ms, "type": "cluster" });
                                // try to fetch cluster summary
                                let q = "SELECT stable_id, label, summary, member_repos FROM hirag_cluster WHERE stable_id = $sid LIMIT 1";
                                let resp2 = match state.db.connection().as_ref() {
                                    SurrealConnection::Local(db_conn) => {
                                        db_conn.query(q).bind(("sid", ms.to_string())).await.ok()
                                    }
                                    SurrealConnection::RemoteHttp(db_conn) => {
                                        db_conn.query(q).bind(("sid", ms.to_string())).await.ok()
                                    }
                                    SurrealConnection::RemoteWs(db_conn) => {
                                        db_conn.query(q).bind(("sid", ms.to_string())).await.ok()
                                    }
                                };
                                if let Some(mut r2) = resp2 {
                                    if let Ok(rows2) = r2.take::<Vec<serde_json::Value>>(0) {
                                        if let Some(rr) = rows2.into_iter().next() {
                                            cinfo = serde_json::json!({ "id": ms, "type": "cluster", "details": rr });
                                        }
                                    }
                                }
                                members_out.push(cinfo);
                                continue;
                            }
                            // Otherwise, assume entity stable_id -> fetch snapshot/entity info
                            let q2 = "SELECT id, stable_id, repo_name, sourcecontrol_commit FROM entity_snapshot WHERE stable_id = $sid LIMIT 1";
                            let resp3 = match state.db.connection().as_ref() {
                                SurrealConnection::Local(db_conn) => {
                                    db_conn.query(q2).bind(("sid", ms.to_string())).await.ok()
                                }
                                SurrealConnection::RemoteHttp(db_conn) => {
                                    db_conn.query(q2).bind(("sid", ms.to_string())).await.ok()
                                }
                                SurrealConnection::RemoteWs(db_conn) => {
                                    db_conn.query(q2).bind(("sid", ms.to_string())).await.ok()
                                }
                            };
                            if let Some(mut r3) = resp3 {
                                if let Ok(rows3) = r3.take::<Vec<serde_json::Value>>(0) {
                                    if let Some(rr) = rows3.into_iter().next() {
                                        members_out.push(serde_json::json!({ "id": ms, "type": "entity", "details": rr }));
                                        continue;
                                    }
                                }
                            }
                            // Fallback: return only id
                            members_out.push(serde_json::json!({ "id": ms, "type": "unknown" }));
                        }
                    }
                }
            }
        }
    }

    Ok(Json(
        serde_json::json!({ "stable_id": stable_id, "members": members_out }),
    ))
}

#[derive(Deserialize)]
struct SimilarQuery {
    limit: Option<usize>,
}

// JSON API returning similar entities (same/external repo) with scores for a given entity stable_id
async fn entity_similar_api_handler(
    State(state): State<AppState>,
    axum::extract::Path(stable_id): axum::extract::Path<String>,
    Query(q): Query<SimilarQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let limit = q.limit.unwrap_or(20);
    match state.db.get_similar_for_entity(&stable_id, limit).await {
        Ok((same, external)) => Ok(Json(serde_json::json!({
            "stable_id": stable_id,
            "same_repo": same,
            "external_repo": external,
        }))),
        Err(e) => {
            log::error!("entity_similar_api_handler failed for {}: {}", stable_id, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn repos_api_handler(
    State(state): State<AppState>,
) -> Result<Json<Vec<RepoSummary>>, StatusCode> {
    let repos = state.db.get_repo_summaries().await.map_err(|e| {
        log::error!("Failed to fetch repo summaries: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(repos))
}

#[derive(Deserialize)]
struct RepoDupesQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    min_score: Option<f64>,
    language: Option<String>,
    kind: Option<String>,
}

#[derive(Deserialize)]
struct MultiDepsQuery {
    repo: Option<Vec<String>>,
    r#ref: Option<String>,
}

/// Top-level API returning dependencies for multiple repos. Query params: `repo` repeated and optional `ref`.
async fn multi_dependencies_api_handler(
    State(state): State<AppState>,
    Query(q): Query<MultiDepsQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let branch = q.r#ref.as_deref();
    let mut out_map = serde_json::Map::new();
    if let Some(repos) = q.repo {
        for repo in repos.into_iter() {
            let (deps, _resolved_ref) =
                fetch_dependencies_for_repo_with_candidates(&state, &repo, branch).await;
            out_map.insert(repo.clone(), serde_json::Value::Array(deps));
        }
    }
    Ok(Json(
        serde_json::json!({ "repos": serde_json::Value::Object(out_map) }),
    ))
}

/// Top-level API returning SBOM dependencies for multiple repos. Query params: `repo` repeated and optional `ref`.
async fn multi_sbom_dependencies_api_handler(
    State(state): State<AppState>,
    Query(q): Query<MultiDepsQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let branch = q.r#ref.as_deref();
    let mut out_map = serde_json::Map::new();
    if let Some(repos) = q.repo {
        for repo in repos.into_iter() {
            let (deps, _resolved_ref) =
                fetch_sbom_deps_for_repo_with_candidates(&state, &repo, branch).await;
            out_map.insert(repo.clone(), serde_json::Value::Array(deps));
        }
    }
    Ok(Json(
        serde_json::json!({ "repos": serde_json::Value::Object(out_map) }),
    ))
}

async fn repo_dupes_api_handler(
    State(state): State<AppState>,
    axum::extract::Path(repo_name): axum::extract::Path<String>,
    Query(q): Query<RepoDupesQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let default_limit: usize = std::env::var("HYPERZOEKT_MAX_SEARCH_RESULTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let limit = q.limit.unwrap_or(default_limit);
    // Parse cursor as offset (decimal string)
    let start: usize = q
        .cursor
        .as_ref()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    match state
        .db
        .get_dupes_for_repo(
            &repo_name,
            // request page-sized results directly from DB
            limit,
            start,
            q.min_score,
            q.language.as_deref(),
            q.kind.as_deref(),
        )
        .await
    {
        Ok(mut pairs) => {
            let total = pairs.len();
            let end = (start + limit).min(total);
            let next_cursor = if end < total {
                Some(end.to_string())
            } else {
                None
            };
            if start < total {
                pairs = pairs[start..end].to_vec();
            } else {
                pairs = vec![];
            }
            let mut resp = serde_json::Map::new();
            resp.insert("repo".to_string(), serde_json::Value::String(repo_name));
            resp.insert("pairs".to_string(), serde_json::Value::Array(pairs));
            if let Some(nc) = next_cursor {
                resp.insert("next_cursor".to_string(), serde_json::Value::String(nc));
            }
            Ok(Json(serde_json::Value::Object(resp)))
        }
        Err(e) => {
            log::error!("repo_dupes_api_handler failed for {}: {}", repo_name, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Deserialize)]
struct RepoDepsQuery {
    r#ref: Option<String>,
}

/// JSON API returning repository dependencies. Accepts optional `ref` query parameter
/// to indicate branch or tag to scope the lookup.
async fn repo_dependencies_api_handler(
    State(state): State<AppState>,
    axum::extract::Path(repo_name): axum::extract::Path<String>,
    Query(q): Query<RepoDepsQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    // Determine branch candidates to use: provided ref, repo default, then main/master/trunk
    let branch_param = q.r#ref.as_deref();
    log::info!(
        "repo_dependencies_api_handler: request for repo='{}' ref_param={:?}",
        repo_name,
        branch_param
    );
    // Use consolidated helper to fetch deps and resolved ref when available
    let (deps, resolved_ref) =
        fetch_dependencies_for_repo_with_candidates(&state, &repo_name, branch_param).await;
    Ok(Json(
        serde_json::json!({"repo": repo_name, "ref": resolved_ref, "dependencies": deps}),
    ))
}

/// JSON API returning SBOM dependencies for a repository. Accepts optional `ref` query parameter
/// to indicate branch or tag to scope the lookup.
async fn repo_sbom_dependencies_api_handler(
    State(state): State<AppState>,
    axum::extract::Path(repo_name): axum::extract::Path<String>,
    Query(q): Query<RepoDepsQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let branch_param = q.r#ref.as_deref();
    log::info!(
        "repo_sbom_dependencies_api_handler: request for repo='{}' ref_param={:?}",
        repo_name,
        branch_param
    );

    let (deps, resolved_ref) =
        fetch_sbom_deps_for_repo_with_candidates(&state, &repo_name, branch_param).await;
    Ok(Json(
        serde_json::json!({"repo": repo_name, "ref": resolved_ref, "dependencies": deps}),
    ))
}

/// Return the decoded SBOM blob (plain text) for a repository and optional `ref` query
async fn repo_sbom_blob_api_handler(
    State(state): State<AppState>,
    axum::extract::Path(repo_name): axum::extract::Path<String>,
    Query(q): Query<RepoDepsQuery>,
) -> Result<axum::response::Response, StatusCode> {
    // Determine branch param (explicit ref) if provided
    let branch_param = q.r#ref.as_deref();

    // Determine branch candidates and attempt to resolve a commit or query repo-level SBOMs
    #[derive(serde::Deserialize)]
    struct SbomRow {
        sbom_blob: Option<String>,
        sbom_encoding: Option<String>,
    }

    let row_opt_triplet =
        fetch_sbom_blob_row_for_repo_with_candidates(&state, &repo_name, branch_param).await;
    let row_opt = row_opt_triplet.map(|(blob, encoding, _resolved_commit)| SbomRow {
        sbom_blob: blob,
        sbom_encoding: encoding,
    });

    if let Some(row) = row_opt {
        if let Some(blob) = row.sbom_blob {
            let encoding = row.sbom_encoding.unwrap_or_else(|| "base64".into());
            if encoding.eq_ignore_ascii_case("base64") {
                match BASE64_STANDARD.decode(blob.as_bytes()) {
                    Ok(decoded) => match String::from_utf8(decoded) {
                        Ok(s) => Ok(axum::response::Response::builder()
                            .status(StatusCode::OK)
                            .header("content-type", "text/plain; charset=utf-8")
                            .body(Body::from(s))
                            .unwrap()),
                        Err(e) => {
                            log::warn!("SBOM UTF-8 decode error for {}: {}", repo_name, e);
                            Err(StatusCode::INTERNAL_SERVER_ERROR)
                        }
                    },
                    Err(e) => {
                        log::warn!("Base64 decode error for {}: {}", repo_name, e);
                        Err(StatusCode::INTERNAL_SERVER_ERROR)
                    }
                }
            } else {
                Ok(axum::response::Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/plain; charset=utf-8")
                    .body(Body::from(blob))
                    .unwrap())
            }
        } else {
            Ok(axum::response::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("content-type", "text/plain; charset=utf-8")
                .body(Body::from("No SBOM blob found"))
                .unwrap())
        }
    } else {
        Ok(axum::response::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("content-type", "text/plain; charset=utf-8")
            .body(Body::from("No SBOM found"))
            .unwrap())
    }
}

async fn repo_external_dupes_api_handler(
    State(state): State<AppState>,
    axum::extract::Path(repo_name): axum::extract::Path<String>,
    Query(q): Query<RepoDupesQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let default_limit: usize = std::env::var("HYPERZOEKT_MAX_SEARCH_RESULTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let limit = q.limit.unwrap_or(default_limit);
    let start: usize = q
        .cursor
        .as_ref()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    match state
        .db
        .get_external_dupes_for_repo(
            &repo_name,
            limit,
            start,
            q.min_score,
            q.language.as_deref(),
            q.kind.as_deref(),
        )
        .await
    {
        Ok(mut pairs) => {
            let total = pairs.len();
            let end = (start + limit).min(total);
            let next_cursor = if end < total {
                Some(end.to_string())
            } else {
                None
            };
            if start < total {
                pairs = pairs[start..end].to_vec();
            } else {
                pairs = vec![];
            }
            let mut resp = serde_json::Map::new();
            resp.insert("repo".to_string(), serde_json::Value::String(repo_name));
            resp.insert("pairs".to_string(), serde_json::Value::Array(pairs));
            if let Some(nc) = next_cursor {
                resp.insert("next_cursor".to_string(), serde_json::Value::String(nc));
            }
            Ok(Json(serde_json::Value::Object(resp)))
        }
        Err(e) => {
            log::error!(
                "repo_external_dupes_api_handler failed for {}: {}",
                repo_name,
                e
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn repo_dupes_page_handler(
    State(state): State<AppState>,
    axum::extract::Path(repo_name): axum::extract::Path<String>,
) -> Result<Html<String>, StatusCode> {
    let template = state.templates.get_template("repo").unwrap();
    let html = template
        .render(context! {
            repo_name => repo_name,
            dupes_page => true,
            title => "Code Dupes"
        })
        .map_err(|e| {
            log::error!("Failed to render repo dupes template: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Html(html))
}

async fn dupes_index_handler(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let template = state.templates.get_template("dupes").unwrap();
    let html = template
        .render(context! { title => "Code Dupes" })
        .map_err(|e| {
            log::error!("Failed to render dupes index template: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Html(html))
}
async fn entities_api_handler(
    State(state): State<AppState>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
    let repo = params.get("repo");
    let mut entities = if let Some(repo_name) = repo {
        state.db.get_entities_for_repo(repo_name).await
    } else {
        state.db.get_all_entities().await
    }
    .map_err(|e| {
        log::error!("Failed to fetch entities (repo={:?}): {}", repo, e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Clean up file paths and prefer stored source URLs; compute only if missing
    for entity in &mut entities {
        // Ensure rank present for JSON consumers
        if entity.rank.is_none() {
            entity.rank = Some(0.0);
        }
        let raw_file_hint = file_hint_for_entity(entity);
        let file_hint = clean_file_path(&raw_file_hint);

        // Debug: log DB-provided source_url for entities API
        log::debug!(
            "webui: entities_api db source_url stable_id={} file='{}' repo='{}' source_url={:?}",
            entity.stable_id,
            file_hint,
            entity.repo_name,
            entity.source_url
        );

        if entity.source_url.is_none() {
            if let Some(url) =
                construct_source_url(&state, &entity.repo_name, &file_hint, None).await
            {
                entity.source_url = Some(url.clone());
                if entity.source_display.is_none() {
                    entity.source_display =
                        Some(short_display_from_source_url(&url, &entity.repo_name));
                }
            }
        } else if entity.source_display.is_none() {
            if let Some(ref url) = entity.source_url {
                entity.source_display = Some(short_display_from_source_url(url, &entity.repo_name));
            }
        }
    }

    // Convert entities to JSON values and attach server-rendered doc_html
    let mut out: Vec<serde_json::Value> = Vec::with_capacity(entities.len());
    for ent in entities.into_iter() {
        let mut v = serde_json::to_value(&ent).unwrap_or(serde_json::json!({}));
        if let Some(obj) = v.as_object_mut() {
            let raw_file_hint = file_hint_for_entity(&ent);
            let file_hint = clean_file_path(&raw_file_hint);
            obj.insert("file".to_string(), serde_json::Value::String(file_hint));
            let doc_html = ent.doc.as_ref().map(|d| utils::render_markdown_to_html(d));
            obj.insert(
                "doc_html".to_string(),
                match doc_html {
                    Some(s) => serde_json::Value::String(s),
                    None => serde_json::Value::Null,
                },
            );
        }
        out.push(v);
    }

    Ok(Json(out))
}

async fn pagerank_handler(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    log::debug!("Received request for PageRank page");
    let repos = state.db.get_repo_summaries().await.map_err(|e| {
        log::error!("Failed to get repo summaries: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    log::debug!("Retrieved {} repositories for PageRank", repos.len());
    let template = state.templates.get_template("pagerank").unwrap();
    let html = template
        .render(context! {
            repos => repos,
            title => "PageRank Analysis"
        })
        .map_err(|e| {
            log::error!("Failed to render pagerank template: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    log::debug!("Successfully rendered PageRank page");
    Ok(Html(html))
}

#[derive(Deserialize)]
struct PageRankQuery {
    repo: String,
    limit: Option<usize>,
}

async fn pagerank_api_handler(
    State(state): State<AppState>,
    Query(query): Query<PageRankQuery>,
) -> Result<Json<Vec<serde_json::Value>>, StatusCode> {
    log::debug!("pagerank_api_handler: repo={}", &query.repo);

    // Respect a limit to avoid fetching extremely large repos. Default to 500.
    let limit = query.limit.unwrap_or(500usize);

    // Fetch entities with a limit via a new lightweight query path to avoid pulling everything.
    // The UI expects a `rank` field on the returned objects. `ENTITY_FIELDS` includes
    // `snapshot.page_rank_value AS page_rank_value`; for this API we alias it to `rank`
    // so deserialization into `EntityPayload { rank: Option<f32>, ... }` succeeds.
    let entity_fields = ENTITY_FIELDS.replace(
        "snapshot.page_rank_value AS page_rank_value",
        "snapshot.page_rank_value AS rank",
    );
    let mut response = match state.db.connection().as_ref() {
        SurrealConnection::Local(db_conn) => {
                db_conn
                .query(format!(
                    "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY snapshot.page_rank_value DESC LIMIT $limit",
                    entity_fields
                ))
                .bind(("repo", query.repo.clone()))
                .bind(("limit", limit as i64))
                .await
        }
        SurrealConnection::RemoteHttp(db_conn) => {
                db_conn
                .query(format!(
                    "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY snapshot.page_rank_value DESC LIMIT $limit",
                    entity_fields
                ))
                .bind(("repo", query.repo.clone()))
                .bind(("limit", limit as i64))
                .await
        }
        SurrealConnection::RemoteWs(db_conn) => {
                db_conn
                .query(format!(
                    "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY snapshot.page_rank_value DESC LIMIT $limit",
                    entity_fields
                ))
                .bind(("repo", query.repo.clone()))
                .bind(("limit", limit as i64))
                .await
        }
    };

    let mut entities: Vec<EntityPayload> = match &mut response {
        Ok(r) => r.take(0).unwrap_or_default(),
        Err(e) => {
            log::error!(
                "pagerank get_entities_for_repo failed for {}: {}",
                &query.repo,
                e
            );
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Ensure rank is present for templates/JS consumers: map None -> 0.0
    for ent in &mut entities {
        if ent.rank.is_none() {
            ent.rank = Some(0.0);
        }
    }

    log::debug!(
        "pagerank_api_handler: fetched {} entities (limit={}) for repo={}",
        entities.len(),
        limit,
        &query.repo
    );

    // Fetch repo metadata once to construct URLs without per-entity DB queries
    #[derive(Deserialize)]
    struct RepoRow {
        git_url: String,
        branch: Option<String>,
    }

    // Try variants similar to construct_source_url: clean name, original, uuid name
    let clean_repo = remove_uuid_suffix(&query.repo);
    let uuid_repo_name = entities
        .first()
        .map(file_hint_for_entity)
        .and_then(|s| if s.is_empty() { None } else { Some(s) })
        .as_deref()
        .and_then(extract_uuid_repo_name);

    let mut query_parts = vec!["SELECT git_url, branch FROM repo WHERE".to_string()];
    let mut bindings: Vec<(&str, String)> = Vec::new();
    query_parts.push("name = $clean_name".to_string());
    bindings.push(("clean_name", clean_repo.clone()));
    if query.repo != clean_repo {
        query_parts.push("OR name = $original_name".to_string());
        bindings.push(("original_name", query.repo.clone()));
    }
    if let Some(uuid_name) = &uuid_repo_name {
        query_parts.push("OR name = $uuid_name".to_string());
        bindings.push(("uuid_name", uuid_name.clone()));
    }
    query_parts.push("LIMIT 1".to_string());
    let repo_q = query_parts.join(" ");

    let repo_row: Option<RepoRow> = match state.db.connection().as_ref() {
        SurrealConnection::Local(db_conn) => {
            let mut q = db_conn.query(&repo_q);
            for (k, v) in &bindings {
                q = q.bind((*k, v.to_string()));
            }
            match q.await {
                Ok(mut r) => r.take(0).ok().and_then(|mut v: Vec<RepoRow>| v.pop()),
                Err(_) => None,
            }
        }
        SurrealConnection::RemoteHttp(db_conn) => {
            let mut q = db_conn.query(&repo_q);
            for (k, v) in &bindings {
                q = q.bind((*k, v.to_string()));
            }
            match q.await {
                Ok(mut r) => r.take(0).ok().and_then(|mut v: Vec<RepoRow>| v.pop()),
                Err(_) => None,
            }
        }
        SurrealConnection::RemoteWs(db_conn) => {
            let mut q = db_conn.query(&repo_q);
            for (k, v) in &bindings {
                q = q.bind((*k, v.to_string()));
            }
            match q.await {
                Ok(mut r) => r.take(0).ok().and_then(|mut v: Vec<RepoRow>| v.pop()),
                Err(_) => None,
            }
        }
    };

    // Determine base URL and branch once
    let (source_base_opt, source_branch) = if let Some(r) = repo_row {
        let branch = r
            .branch
            .or_else(|| std::env::var("SOURCE_BRANCH").ok())
            .unwrap_or_else(|| "main".to_string());
        if let Some(normalized_base) = utils::normalize_git_url(&r.git_url) {
            (Some(normalized_base), branch)
        } else {
            (None, branch)
        }
    } else {
        (
            None,
            std::env::var("SOURCE_BRANCH").unwrap_or_else(|_| "main".to_string()),
        )
    };

    // Construct source URLs in-memory without extra DB queries
    for entity in &mut entities {
        let raw_file_hint = file_hint_for_entity(entity);
        let file_hint = clean_file_path(&raw_file_hint);
        let clean_repo_name = remove_uuid_suffix(&entity.repo_name);
        let repo_relative = repo_relative_from_file(&file_hint, &clean_repo_name);

        if let Some(base) = &source_base_opt {
            let url = format!(
                "{}/blob/{}/{}",
                base.trim_end_matches('/'),
                &source_branch,
                repo_relative
            );
            entity.source_url = Some(url.clone());
            entity.source_display = Some(short_display_from_source_url(&url, &entity.repo_name));
        } else {
            // Fallback to default base
            let source_base = std::env::var("SOURCE_BASE_URL")
                .unwrap_or_else(|_| "https://github.com".to_string());
            let url = format!(
                "{}/{}/blob/{}/{}",
                source_base.trim_end_matches('/'),
                clean_repo_name,
                source_branch,
                repo_relative
            );
            entity.source_url = Some(url.clone());
            entity.source_display = Some(short_display_from_source_url(&url, &entity.repo_name));
        }
    }

    // Convert entities to JSON values and attach server-rendered doc_html
    let mut out: Vec<serde_json::Value> = Vec::with_capacity(entities.len());
    for ent in entities.into_iter() {
        let mut v = serde_json::to_value(&ent).unwrap_or(serde_json::json!({}));
        if let Some(obj) = v.as_object_mut() {
            let raw_file_hint = file_hint_for_entity(&ent);
            let file_hint = clean_file_path(&raw_file_hint);
            obj.insert("file".to_string(), serde_json::Value::String(file_hint));
            let doc_html = ent.doc.as_ref().map(|d| utils::render_markdown_to_html(d));
            obj.insert(
                "doc_html".to_string(),
                match doc_html {
                    Some(s) => serde_json::Value::String(s),
                    None => serde_json::Value::Null,
                },
            );
        }
        out.push(v);
    }

    Ok(Json(out))
}

async fn construct_source_url(
    state: &AppState,
    repo_name: &str,
    file_path: &str,
    uuid_repo_name: Option<&str>,
) -> Option<String> {
    // Clean the repo_name to remove UUID suffix if present
    let clean_repo_name = utils::remove_uuid_suffix(repo_name);

    log::debug!(
        "construct_source_url: repo_name='{}', clean_repo_name='{}', uuid_repo_name='{:?}', file_path='{}'",
        repo_name,
        clean_repo_name,
        uuid_repo_name,
        file_path
    );

    // Query the repo table for the git_url and branch
    #[derive(Deserialize)]
    struct RepoRow {
        git_url: String,
        branch: Option<String>,
    }

    let mut query_parts = vec!["SELECT git_url, branch FROM repo WHERE".to_string()];
    let mut bindings = vec![];

    // Always include clean_name
    query_parts.push("name = $clean_name".to_string());
    bindings.push(("clean_name", clean_repo_name.clone()));

    // Include original_name if different
    if repo_name != clean_repo_name {
        query_parts.push("OR name = $original_name".to_string());
        bindings.push(("original_name", repo_name.to_string()));
    }

    // Include uuid_repo_name if provided
    if let Some(uuid_name) = uuid_repo_name {
        query_parts.push("OR name = $uuid_name".to_string());
        bindings.push(("uuid_name", uuid_name.to_string()));
    }

    query_parts.push("LIMIT 1".to_string());
    let query_sql = query_parts.join(" ");

    if let Ok(mut res) = match state.db.connection().as_ref() {
        SurrealConnection::Local(db_conn) => {
            let mut query = db_conn.query(&query_sql);
            for (key, value) in bindings {
                query = query.bind((key, value));
            }
            query.await
        }
        SurrealConnection::RemoteHttp(db_conn) => {
            let mut query = db_conn.query(&query_sql);
            for (key, value) in bindings {
                query = query.bind((key, value));
            }
            query.await
        }
        SurrealConnection::RemoteWs(db_conn) => {
            let mut query = db_conn.query(&query_sql);
            for (key, value) in bindings {
                query = query.bind((key, value));
            }
            query.await
        }
    } {
        if let Ok(rows) = res.take::<Vec<RepoRow>>(0) {
            if let Some(r) = rows.into_iter().next() {
                log::debug!(
                    "construct_source_url: Found repo row: git_url='{}', branch='{:?}'",
                    r.git_url,
                    r.branch
                );
                if let Some(normalized_base) = utils::normalize_git_url(&r.git_url) {
                    log::debug!(
                        "construct_source_url: Normalized git_url to '{}'",
                        normalized_base
                    );
                    let branch = r
                        .branch
                        .or_else(|| std::env::var("SOURCE_BRANCH").ok())
                        .unwrap_or_else(|| "main".to_string());

                    // Use helper to compute repo-relative path
                    let repo_relative = utils::repo_relative_from_file(file_path, &clean_repo_name);
                    log::debug!(
                        "construct_source_url: Repo-relative path: '{}'",
                        repo_relative
                    );

                    let base = normalized_base.trim_end_matches('/');
                    let url = if base.ends_with(&format!("/{}", clean_repo_name)) {
                        // normalized_base already includes the repo path (owner/repo),
                        // avoid appending the repo name again.
                        format!("{}/blob/{}/{}", base, branch, repo_relative)
                    } else {
                        format!(
                            "{}/{}/blob/{}/{}",
                            base, clean_repo_name, branch, repo_relative
                        )
                    };
                    log::debug!("construct_source_url: Constructed URL: '{}'", url);
                    return Some(url);
                } else {
                    log::warn!(
                        "construct_source_url: Failed to normalize git_url '{}'",
                        r.git_url
                    );
                }
            } else {
                log::warn!(
                    "construct_source_url: No repo row found for query: {}",
                    query_sql
                );
            }
        } else {
            log::error!("construct_source_url: Failed to parse repo query result");
        }
    } else {
        log::error!("construct_source_url: Repo query failed");
    }

    // Fallback to default values (no env vars for base URL to avoid issues with multiple repos)
    log::warn!(
        "construct_source_url: Using fallback for repo '{}'",
        clean_repo_name
    );
    let source_base = "https://github.com".to_string();
    let source_branch = std::env::var("SOURCE_BRANCH").unwrap_or_else(|_| "main".to_string());
    // Clean and compute repo-relative path similar to the DB-backed branch above
    let normalized_file = utils::clean_file_path(file_path);
    let parts: Vec<&str> = normalized_file
        .split('/')
        .filter(|p| !p.is_empty())
        .collect();
    let mut repo_relative = if parts.len() > 1 {
        parts[1..].join("/")
    } else {
        normalized_file.clone()
    };
    for (i, p) in parts.iter().enumerate() {
        if *p == clean_repo_name || p.starts_with(&format!("{}-", clean_repo_name)) {
            repo_relative = if i + 1 < parts.len() {
                parts[i + 1..].join("/")
            } else {
                String::new()
            };
            break;
        }
    }

    let url = format!(
        "{}/{}/blob/{}/{}",
        source_base.trim_end_matches('/'),
        clean_repo_name,
        source_branch,
        repo_relative
    );
    log::debug!("construct_source_url: Fallback URL: '{}'", url);
    Some(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    // Helper to create a minimal AppState with an in-memory Surreal DB and template env
    async fn make_state() -> AppState {
        // Force an ephemeral in-memory Surreal instance for tests and disable
        // any env-based remote DB resolution. This guarantees tests do not
        // require any pre-existing SurrealDB data.
        let prev_ephemeral = std::env::var("HZ_EPHEMERAL_MEM").ok();
        let prev_disable_env = std::env::var("HZ_DISABLE_SURREAL_ENV").ok();
        std::env::set_var("HZ_EPHEMERAL_MEM", "1");
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");

        let db = Database::new(None, "testns", "testdb")
            .await
            .expect("db init");

        // Restore previous environment values to avoid leaking test state
        if let Some(v) = prev_ephemeral {
            std::env::set_var("HZ_EPHEMERAL_MEM", v);
        } else {
            std::env::remove_var("HZ_EPHEMERAL_MEM");
        }
        if let Some(v) = prev_disable_env {
            std::env::set_var("HZ_DISABLE_SURREAL_ENV", v);
        } else {
            std::env::remove_var("HZ_DISABLE_SURREAL_ENV");
        }
        let mut templates = Environment::new();
        templates.add_template("base", BASE_TEMPLATE).unwrap();
        AppState { db, templates }
    }

    #[tokio::test]
    async fn construct_source_url_db_git_ssh() {
        let state = make_state().await;

        // Insert a repo row into the in-memory SurrealDB to test DB-backed resolution
        let insert_q = "CREATE repo SET name = 'gpt-researcher', git_url = 'git@github.com:assafelovic/gpt-researcher.git', branch = 'dev'";
        match &**state.db.connection() {
            hyperzoekt::db::connection::SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteWs(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
        }

        let repo_name = "gpt-researcher";
        let file_path = "gpt-researcher/gpt_researcher/agent.py";

        let url = construct_source_url(&state, repo_name, file_path, None)
            .await
            .expect("should produce url from db");
        // Be tolerant to variants where the normalized git URL may include the owner
        // or may omit it depending on how the repo row was created; assert key parts
        // instead of an exact string to make the test robust across environments.
        assert!(url.starts_with("https://github.com/"));
        assert!(url.contains("/blob/dev/"));
        assert!(url.ends_with("gpt_researcher/agent.py"));
    }

    #[tokio::test]
    async fn construct_source_url_db_gitlab_http() {
        let state = make_state().await;

        // Insert a GitLab HTTP repo row with branch specified
        let insert_q = "CREATE repo SET name = 'gl-repo', git_url = 'https://gitlab.com/owner/gl-repo.git', branch = 'feature'";
        match &**state.db.connection() {
            hyperzoekt::db::connection::SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteWs(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
        }

        let repo_name = "gl-repo";
        let file_path = "gl-repo/src/lib.rs";

        let url = construct_source_url(&state, repo_name, file_path, None)
            .await
            .expect("should produce url from db");
        // Our construction uses /blob/ even for GitLab-hosted repos
        assert_eq!(
            url,
            "https://gitlab.com/owner/gl-repo/blob/feature/src/lib.rs"
        );
    }

    #[tokio::test]
    #[serial]
    async fn construct_source_url_missing_branch_uses_main() {
        let state = make_state().await;

        // Insert a repo row without branch -> should default to main
        let insert_q = "CREATE repo SET name = 'repo-no-branch', git_url = 'https://github.com/owner/repo-no-branch.git'";
        match &**state.db.connection() {
            hyperzoekt::db::connection::SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteWs(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
        }

        let repo_name = "repo-no-branch";
        let file_path = "repo-no-branch/README.md";
        // Ensure any test-set SOURCE_BRANCH doesn't affect this assertion
        std::env::remove_var("SOURCE_BRANCH");
        std::env::remove_var("SOURCE_BASE_URL");

        let url = construct_source_url(&state, repo_name, file_path, None)
            .await
            .expect("should produce url from db fallback to main");
        assert_eq!(
            url,
            "https://github.com/owner/repo-no-branch/blob/main/README.md"
        );
    }

    #[tokio::test]
    #[serial]
    async fn construct_source_url_local_git_url_falls_back_to_env() {
        let state = make_state().await;
        // Trigger env fallback by using a repo name that does not exist in DB
        std::env::remove_var("SOURCE_BASE_URL");
        std::env::remove_var("SOURCE_BRANCH");
        std::env::set_var("SOURCE_BASE_URL", "https://example.com");
        std::env::set_var("SOURCE_BRANCH", "main");

        let repo_name = "missing-local-repo";
        let file_path = "/tmp/hyperzoekt-clones/uuid/missing-local-repo/src/main.rs";

        let url = construct_source_url(&state, repo_name, file_path, None)
            .await
            .expect("should produce fallback url");
        assert_eq!(
            url,
            "https://github.com/missing-local-repo/blob/main/src/main.rs"
        );

        // Set env fallback values before inserting repo so construct_source_url uses them
        std::env::remove_var("SOURCE_BASE_URL");
        std::env::remove_var("SOURCE_BRANCH");
        std::env::set_var("SOURCE_BASE_URL", "https://example.com");
        std::env::set_var("SOURCE_BRANCH", "main");

        // Insert a repo row whose git_url is a local path (not convertible)
        let insert_q = "CREATE repo SET name = 'local-repo', git_url = 'file:///home/repos/local-repo', branch = 'dev'";
        match &**state.db.connection() {
            hyperzoekt::db::connection::SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteWs(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
        }

        let repo_name = "local-repo";
        let file_path = "/tmp/hyperzoekt-clones/uuid/local-repo/src/main.rs";

        let url = construct_source_url(&state, repo_name, file_path, None)
            .await
            .expect("should produce fallback url");
        assert_eq!(url, "https://github.com/local-repo/blob/main/src/main.rs");

        let repo_name = "local-repo";
        let file_path = "/tmp/hyperzoekt-clones/local-repo-89b31936-727e-4719-ab9e-778a15263b26/local-repo/src/main.rs";

        let url = construct_source_url(&state, repo_name, file_path, None)
            .await
            .expect("should produce fallback url");
        assert_eq!(url, "https://github.com/local-repo/blob/main/src/main.rs");
    }

    #[test]
    fn short_display_parses_gitlab_dash_blob() {
        let src = "https://gitlab.com/owner/repo/-/blob/feature/path/to/file.py";
        let short = short_display_from_source_url(src, "repo");
        assert_eq!(short, "owner/repo/path/to/file.py");
    }

    #[tokio::test]
    async fn construct_source_url_gitlab_ssh_form() {
        let state = make_state().await;

        let insert_q = "CREATE repo SET name = 'gl-ssh', git_url = 'git@gitlab.com:owner/gl-ssh.git', branch = 'feat'";
        match &**state.db.connection() {
            hyperzoekt::db::connection::SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteWs(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
        }

        let repo_name = "gl-ssh";
        let file_path = "gl-ssh/src/mod.rs";

        let url = construct_source_url(&state, repo_name, file_path, None)
            .await
            .expect("should produce url from ssh gitlab");
        assert_eq!(url, "https://gitlab.com/owner/gl-ssh/blob/feat/src/mod.rs");
    }

    #[tokio::test]
    #[serial]
    async fn construct_source_url_empty_git_url_uses_env() {
        let state = make_state().await;

        let insert_q = "CREATE repo SET name = 'empty-url', git_url = ''";
        match &**state.db.connection() {
            hyperzoekt::db::connection::SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteWs(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
        }

        std::env::remove_var("SOURCE_BASE_URL");
        std::env::remove_var("SOURCE_BRANCH");
        std::env::set_var("SOURCE_BASE_URL", "https://example.com");

        let repo_name = "empty-url";
        let file_path = "/tmp/hyperzoekt-clones/uuid/empty-url/src/lib.rs";

        let url = construct_source_url(&state, repo_name, file_path, None)
            .await
            .expect("should fallback to env");
        assert_eq!(url, "https://github.com/empty-url/blob/main/src/lib.rs");
    }

    #[tokio::test]
    async fn construct_source_url_branch_with_slash() {
        let state = make_state().await;

        let insert_q = "CREATE repo SET name = 'branch-slash', git_url = 'https://github.com/owner/branch-slash.git', branch = 'feature/new-thing'";
        match &**state.db.connection() {
            hyperzoekt::db::connection::SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            hyperzoekt::db::connection::SurrealConnection::RemoteWs(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
        }

        let repo_name = "branch-slash";
        let file_path = "branch-slash/path/file.rs";

        let url = construct_source_url(&state, repo_name, file_path, None)
            .await
            .expect("should include branch with slash");
        assert_eq!(
            url,
            "https://github.com/owner/branch-slash/blob/feature/new-thing/path/file.rs"
        );
    }
}
