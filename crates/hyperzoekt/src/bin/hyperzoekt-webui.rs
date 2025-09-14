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
use pulldown_cmark::{Options as MdOptions, Parser as MdParser};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use surrealdb::Surreal;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use hyperzoekt::repo_index::indexer::payload::EntityPayload;

// Minimal helper to normalize Surreal id field (string or object with tb/id)

// Embed static templates so the binary can serve them directly.
const BASE_TEMPLATE: &str = include_str!("../../static/webui/base.html");
const INDEX_TEMPLATE: &str = include_str!("../../static/webui/index.html");
const REPO_TEMPLATE: &str = include_str!("../../static/webui/repo.html");
const ENTITY_TEMPLATE: &str = include_str!("../../static/webui/entity.html");
const SEARCH_TEMPLATE: &str = include_str!("../../static/webui/search.html");
const PAGERANK_TEMPLATE: &str = include_str!("../../static/webui/pagerank.html");

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
            log_level: args.log_level.unwrap_or_else(|| {
                std::env::var("HYPERZOEKT_LOG_LEVEL").unwrap_or_else(|_| "info".to_string())
            }),
            cors_all: args.cors_all
                || std::env::var("HYPERZOEKT_CORS_ALL")
                    .map(|v| v == "true" || v == "1")
                    .unwrap_or(false),
            max_search_results: args.max_search_results,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoSummary {
    pub name: String,
    pub entity_count: u64,
    pub file_count: u64,
    pub languages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepoSummaryQueryResult {
    pub repo_name: String,
    pub entity_count: u64,
    // SurrealDB arrays may contain nulls if some entities have missing file/language.
    // Accept Option<String> here and filter out None values when constructing
    // the public `RepoSummary` to avoid deserialization errors like
    // "expected a string, found None".
    pub files: Vec<Option<String>>,
    pub languages: Vec<Option<String>>,
}

#[derive(Clone)]
pub struct Database {
    db: Arc<SurrealConnection>,
}

enum SurrealConnection {
    Local(Surreal<surrealdb::engine::local::Db>),
    RemoteHttp(Surreal<surrealdb::engine::remote::http::Client>),
    RemoteWs(Surreal<surrealdb::engine::remote::http::Client>),
}

impl Clone for SurrealConnection {
    fn clone(&self) -> Self {
        // Since we're using Arc<SurrealConnection>, we don't actually clone the connection
        // Instead, we return a reference to the same connection
        // This is safe because Arc handles the reference counting
        match self {
            SurrealConnection::Local(db) => SurrealConnection::Local(db.clone()),
            SurrealConnection::RemoteHttp(db) => SurrealConnection::RemoteHttp(db.clone()),
            SurrealConnection::RemoteWs(db) => SurrealConnection::RemoteWs(db.clone()),
        }
    }
}

impl SurrealConnection {
    #[allow(dead_code)]
    async fn use_ns(&self, namespace: &str) -> Result<(), surrealdb::Error> {
        match self {
            SurrealConnection::Local(db) => db.use_ns(namespace).await,
            SurrealConnection::RemoteHttp(db) => db.use_ns(namespace).await,
            SurrealConnection::RemoteWs(db) => db.use_ns(namespace).await,
        }
    }

    #[allow(dead_code)]
    async fn use_db(&self, database: &str) -> Result<(), surrealdb::Error> {
        match self {
            SurrealConnection::Local(db) => db.use_db(database).await,
            SurrealConnection::RemoteHttp(db) => db.use_db(database).await,
            SurrealConnection::RemoteWs(db) => db.use_db(database).await,
        }
    }

    async fn query(&self, sql: &str) -> Result<surrealdb::Response, surrealdb::Error> {
        // Centralized logging for all SurrealDB queries: log SQL and duration
        log::debug!("Executing SurrealDB query: {}", sql);
        let start = Instant::now();
        let res = match self {
            SurrealConnection::Local(db) => db.query(sql).await,
            SurrealConnection::RemoteHttp(db) => db.query(sql).await,
            SurrealConnection::RemoteWs(db) => db.query(sql).await,
        };
        let elapsed = start.elapsed();
        match &res {
            Ok(_) => log::debug!("SurrealDB query succeeded in {:?}: {}", elapsed, sql),
            Err(e) => log::debug!("SurrealDB query failed in {:?}: {} -> {}", elapsed, sql, e),
        }
        res
    }
}

impl Database {
    pub async fn new(
        url: Option<&str>,
        ns: &str,
        db_name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        use hyperzoekt::db_writer::connection::{connect, SurrealConnection as HZConn};

        let conn = connect(&url.map(|s| s.to_string()), &None, &None, ns, db_name).await?;
        let db = match conn {
            HZConn::Local(arc) => SurrealConnection::Local(arc.as_ref().clone()),
            HZConn::RemoteHttp(c) => SurrealConnection::RemoteHttp(c),
            HZConn::RemoteWs(c) => SurrealConnection::RemoteWs(c),
        };
        Ok(Self { db: Arc::new(db) })
    }

    pub async fn get_repo_summaries(&self) -> Result<Vec<RepoSummary>, Box<dyn std::error::Error>> {
        // Use the stored repo_name field instead of parsing from file paths
        // Handle cases where repo_name might be null for existing data
        let query = r#"
            SELECT
                repo_name ?? 'unknown' as repo_name,
                count() as entity_count,
                array::distinct(file) as files,
                array::distinct(language) as languages
            FROM entity
            GROUP BY repo_name
            ORDER BY entity_count DESC
        "#;

        let mut response = self.db.query(query).await?;
        let query_results: Vec<RepoSummaryQueryResult> = response.take(0)?;

        Ok(query_results
            .into_iter()
            .map(|s| {
                // Filter out any nulls returned by the DB and produce stable Vec<String> types
                let files: Vec<String> = s.files.into_iter().flatten().collect();
                let languages: Vec<String> = s.languages.into_iter().flatten().collect();
                let file_count = files.len() as u64; // Count distinct files
                RepoSummary {
                    name: s.repo_name,
                    entity_count: s.entity_count,
                    file_count,
                    languages,
                }
            })
            .collect())
    }

    pub async fn get_entities_for_repo(
        &self,
        repo_name: &str,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        // Prefer filtering on the explicit `repo_name` field (safer and more direct).
        // Fall back to matching file path prefixes for older/imported records that
        // don't have `repo_name` populated.
        let entity_fields = "file, language, kind, name, parent, signature, start_line, end_line, doc, rank, imports, unresolved_imports, stable_id, repo_name, source_url, source_display";
        let entities: Vec<EntityPayload> = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                let mut resp = db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY file, start_line",
                        entity_fields
                    ))
                    .bind(("repo", repo_name.to_string()))
                    .await?;
                match resp.take::<Vec<EntityPayload>>(0) {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!(
                                "get_entities_for_repo: failed to deserialize response for repo='{}': {}",
                                repo_name,
                                e
                            );
                        return Err(Box::new(e));
                    }
                }
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                let mut resp = db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY file, start_line",
                        entity_fields
                    ))
                    .bind(("repo", repo_name.to_string()))
                    .await?;
                match resp.take::<Vec<EntityPayload>>(0) {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!(
                            "get_entities_for_repo: failed to deserialize response for repo='{}': {}",
                            repo_name,
                            e
                        );
                        return Err(Box::new(e));
                    }
                }
            }
            SurrealConnection::RemoteWs(db_conn) => {
                let mut resp = db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY file, start_line",
                        entity_fields
                    ))
                    .bind(("repo", repo_name.to_string()))
                    .await?;
                match resp.take::<Vec<EntityPayload>>(0) {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!(
                            "get_entities_for_repo: failed to deserialize response for repo='{}': {}",
                            repo_name,
                            e
                        );
                        return Err(Box::new(e));
                    }
                }
            }
        };

        if !entities.is_empty() {
            return Ok(entities);
        }

        // Fallback: look for file paths that start with the provided repo_name.
        // Use a parameterized query to avoid injection.
        let entity_fields = "file, language, kind, name, parent, signature, start_line, end_line, doc, rank, imports, unresolved_imports, stable_id, repo_name, source_url, source_display";
        let entities2: Vec<EntityPayload> = match &*self.db {
            SurrealConnection::Local(db_conn) => db_conn
                .query(format!(
                    "SELECT {} FROM entity WHERE string::starts_with(file ?? '', $repo) ORDER BY file, start_line",
                    entity_fields
                ))
                .bind(("repo", repo_name.to_string()))
                .await?
                .take(0)?,
            SurrealConnection::RemoteHttp(db_conn) => db_conn
                .query(format!(
                    "SELECT {} FROM entity WHERE string::starts_with(file ?? '', $repo) ORDER BY file, start_line",
                    entity_fields
                ))
                .bind(("repo", repo_name.to_string()))
                .await?
                .take(0)?,
            SurrealConnection::RemoteWs(db_conn) => db_conn
                .query(format!(
                    "SELECT {} FROM entity WHERE string::starts_with(file ?? '', $repo) ORDER BY file, start_line",
                    entity_fields
                ))
                .bind(("repo", repo_name.to_string()))
                .await?
                .take(0)?,
        };

        Ok(entities2)
    }

    pub async fn get_all_entities(&self) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        let entity_fields = "file, language, kind, name, parent, signature, start_line, end_line, doc, rank, imports, unresolved_imports, stable_id, repo_name, source_url, source_display";
        let query_sql = format!(
            "SELECT {} FROM entity ORDER BY file, start_line",
            entity_fields
        );
        let mut response = self.db.query(&query_sql).await?;
        // Defensive deserialization with logging to help debug malformed DB rows
        match response.take::<Vec<EntityPayload>>(0) {
            Ok(ents) => Ok(ents),
            Err(e) => {
                log::error!(
                    "get_all_entities: failed to deserialize response for query='{}': {}",
                    query_sql,
                    e
                );
                Err(Box::new(e))
            }
        }
    }

    pub async fn get_entity_by_id(
        &self,
        stable_id: &str,
    ) -> Result<Option<EntityPayload>, Box<dyn std::error::Error>> {
        let entity_fields = "file, language, kind, name, parent, signature, start_line, end_line, doc, rank, imports, unresolved_imports, stable_id, repo_name, source_url, source_display";
        let mut response = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE stable_id = $stable_id",
                        entity_fields
                    ))
                    .bind(("stable_id", stable_id.to_string()))
                    .await?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE stable_id = $stable_id",
                        entity_fields
                    ))
                    .bind(("stable_id", stable_id.to_string()))
                    .await?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE stable_id = $stable_id",
                        entity_fields
                    ))
                    .bind(("stable_id", stable_id.to_string()))
                    .await?
            }
        };
        let entities: Vec<EntityPayload> = response.take(0)?;

        Ok(entities.into_iter().next())
    }

    /// Fetch methods for a given class/struct/interface/trait entity by matching on parent name within the same repo.
    /// Note: For Rust, impl methods are exported as top-level functions with parent = NULL, so this will return empty.
    pub async fn get_methods_for_parent(
        &self,
        repo_name: &str,
        parent_name: &str,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        let fields = "file, language, kind, name, parent, signature, start_line, end_line, doc, rank, imports, unresolved_imports, stable_id, repo_name, source_url, source_display";

        let mut response = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE repo_name = $repo AND kind = 'function' AND parent = $parent ORDER BY start_line, name",
                        fields
                    ))
                    .bind(("repo", repo_name.to_string()))
                    .bind(("parent", parent_name.to_string()))
                    .await?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE repo_name = $repo AND kind = 'function' AND parent = $parent ORDER BY start_line, name",
                        fields
                    ))
                    .bind(("repo", repo_name.to_string()))
                    .bind(("parent", parent_name.to_string()))
                    .await?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE repo_name = $repo AND kind = 'function' AND parent = $parent ORDER BY start_line, name",
                        fields
                    ))
                    .bind(("repo", repo_name.to_string()))
                    .bind(("parent", parent_name.to_string()))
                    .await?
            }
        };

        let methods: Vec<EntityPayload> = response.take(0)?;
        Ok(methods)
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
        // Delegate to the shared graph API which already handles SurrealDB Thing/enum
        // forms and falls back to tolerant raw JSON parsing when needed.
        let cfg = hyperzoekt::graph_api::GraphDbConfig::from_env();
        match hyperzoekt::graph_api::fetch_entity_graph(&cfg, stable_id, limit).await {
            Ok(g) => {
                let callers = g
                    .callers
                    .into_iter()
                    .map(|r| (r.name, r.stable_id))
                    .collect::<Vec<(String, String)>>();
                let callees = g
                    .callees
                    .into_iter()
                    .map(|r| (r.name, r.stable_id))
                    .collect::<Vec<(String, String)>>();
                let imports = g
                    .imports
                    .into_iter()
                    .map(|r| (r.name, r.stable_id))
                    .collect::<Vec<(String, String)>>();
                Ok((callers, callees, imports))
            }
            Err(e) => {
                log::error!(
                    "fetch_entity_graph: delegated graph fetch failed for {}: {}",
                    stable_id,
                    e
                );
                Err(Box::<dyn std::error::Error>::from(e))
            }
        }
    }

    pub async fn search_entities(
        &self,
        query: &str,
        repo_filter: Option<&str>,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        // Use a fixed set of fields and parameterized binds to avoid SQL injection and SELECT *
        let fields = "file, language, kind, name, parent, signature, start_line, end_line, doc, rank, imports, unresolved_imports, stable_id, repo_name, source_url, source_display";

        let mut response = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                let q0 = db_conn.query(format!(
                    // Coalesce NULLs to empty string before applying string functions so
                    // SurrealDB doesn't error when fields are missing.
                    "SELECT {} FROM entity WHERE (string::matches(string::lowercase(name ?? ''), $q) OR string::matches(string::lowercase(signature ?? ''), $q) OR string::matches(string::lowercase(file ?? ''), $q)) ORDER BY rank DESC LIMIT 100",
                    fields
                ));
                let q = q0.bind(("q", query.to_lowercase()));
                if let Some(repo) = repo_filter {
                    // append repo filter by building a new query with starts_with
                    let sql = format!(
                        "SELECT {} FROM entity WHERE (string::matches(string::lowercase(name ?? ''), $q) OR string::matches(string::lowercase(signature ?? ''), $q) OR string::matches(string::lowercase(file ?? ''), $q)) AND string::starts_with(file ?? '', $repo) ORDER BY rank DESC LIMIT 100",
                        fields
                    );
                    let q2 = db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("repo", repo.to_string()));
                    q2.await?
                } else {
                    q.await?
                }
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                let q0 = db_conn.query(format!(
                    "SELECT {} FROM entity WHERE (string::matches(string::lowercase(name ?? ''), $q) OR string::matches(string::lowercase(signature ?? ''), $q) OR string::matches(string::lowercase(file ?? ''), $q)) ORDER BY rank DESC LIMIT 100",
                    fields
                ));
                let q = q0.bind(("q", query.to_lowercase()));
                if let Some(repo) = repo_filter {
                    let sql = format!(
                        "SELECT {} FROM entity WHERE (string::matches(string::lowercase(name ?? ''), $q) OR string::matches(string::lowercase(signature ?? ''), $q) OR string::matches(string::lowercase(file ?? ''), $q)) AND string::starts_with(file ?? '', $repo) ORDER BY rank DESC LIMIT 100",
                        fields
                    );
                    let q2 = db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("repo", repo.to_string()));
                    q2.await?
                } else {
                    q.await?
                }
            }
            SurrealConnection::RemoteWs(db_conn) => {
                let q0 = db_conn.query(format!(
                    "SELECT {} FROM entity WHERE (string::matches(string::lowercase(name ?? ''), $q) OR string::matches(string::lowercase(signature ?? ''), $q) OR string::matches(string::lowercase(file ?? ''), $q)) ORDER BY rank DESC LIMIT 100",
                    fields
                ));
                let q = q0.bind(("q", query.to_lowercase()));
                if let Some(repo) = repo_filter {
                    let sql = format!(
                        "SELECT {} FROM entity WHERE (string::matches(string::lowercase(name ?? ''), $q) OR string::matches(string::lowercase(signature ?? ''), $q) OR string::matches(string::lowercase(file ?? ''), $q)) AND string::starts_with(file ?? '', $repo) ORDER BY rank DESC LIMIT 100",
                        fields
                    );
                    let q2 = db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("repo", repo.to_string()));
                    q2.await?
                } else {
                    q.await?
                }
            }
        };

        let entities: Vec<EntityPayload> = match response.take::<Vec<EntityPayload>>(0) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "search_entities: failed to deserialize response for q='{}' repo={:?}: {}",
                    query,
                    repo_filter,
                    e
                );
                return Err(Box::new(e));
            }
        };

        Ok(entities)
    }
}

/// Render markdown to sanitized HTML using pulldown-cmark -> ammonia.
fn render_markdown_to_html(md: &str) -> String {
    if md.is_empty() {
        return String::new();
    }
    let mut opts = MdOptions::empty();
    opts.insert(MdOptions::ENABLE_STRIKETHROUGH);
    opts.insert(MdOptions::ENABLE_TABLES);
    opts.insert(MdOptions::ENABLE_FOOTNOTES);
    let parser = MdParser::new_ext(md, opts);
    let mut html_out = String::new();
    pulldown_cmark::html::push_html(&mut html_out, parser);

    // Sanitize the generated HTML to avoid XSS
    ammonia::Builder::default().clean(&html_out).to_string()
}

#[derive(Clone)]
struct AppState {
    db: Database,
    templates: Environment<'static>,
}

/// Clean up file paths to show repository-relative paths instead of absolute filesystem paths
/// Converts paths like "/tmp/hyperzoekt-clones/repo-uuid/path/to/file.rs" to "repo/path/to/file.rs"
fn clean_file_path(file_path: &str) -> String {
    // Robust cleaning of file paths to handle many variants. Strategy:
    //  - Split into path segments
    //  - Remove any embedded /tmp/hyperzoekt-clones/<uuid>/ sequences
    //  - For any segment that ends with a UUID-like suffix (repo-name-<uuid>), strip the suffix
    //  - Rejoin segments
    let mut parts: Vec<String> = file_path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    // Remove sequences like tmp -> hyperzoekt-clones -> <uuid>
    let mut i = 0;
    while i + 2 < parts.len() {
        if parts[i] == "tmp" && parts[i + 1] == "hyperzoekt-clones" {
            // Remove tmp and hyperzoekt-clones and the following uuid-like segment
            parts.drain(i..=i + 2);
            // continue without incrementing i
            continue;
        }
        i += 1;
    }

    // Also handle the variant where only "hyperzoekt-clones" appears
    let mut j = 0;
    while j < parts.len() {
        if parts[j] == "hyperzoekt-clones" {
            // remove the hyperzoekt-clones and possibly next uuid segment
            if j + 1 < parts.len() {
                parts.drain(j..=j + 1);
            } else {
                parts.remove(j);
            }
            continue;
        }
        j += 1;
    }

    // Strip UUID suffixes from any segment like name-8e9834cb-6abb-...-deadbeef
    for seg in parts.iter_mut() {
        if let Some(last_dash) = seg.rfind('-') {
            let potential_uuid = &seg[last_dash + 1..];
            let cleaned: String = potential_uuid.chars().filter(|c| *c != '-').collect();
            if cleaned.len() >= 32 && cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
                *seg = seg[..last_dash].to_string();
            }
        }
    }

    // Reconstruct path
    if parts.is_empty() {
        return file_path.to_string();
    }
    parts.join("/")
}

/// Remove a trailing UUID-style suffix from names like "repo-name-8e9834cb-6abb-4381-90f4-deadbeefdead"
fn remove_uuid_suffix(name: &str) -> String {
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() >= 6 {
        let tail = &parts[parts.len() - 5..];
        let lens: [usize; 5] = [8, 4, 4, 4, 12];
        let mut ok = true;
        for (i, seg) in tail.iter().enumerate() {
            if seg.len() != lens[i] || !seg.chars().all(|c| c.is_ascii_hexdigit()) {
                ok = false;
                break;
            }
        }
        if ok {
            return parts[..parts.len() - 5].join("-");
        }
    }
    name.to_string()
}

/// Extract the repo name with UUID suffix from a file path
/// E.g., "/tmp/hyperzoekt-clones/repo-uuid/file" -> "repo-uuid"
fn extract_uuid_repo_name(file_path: &str) -> Option<String> {
    let parts: Vec<&str> = file_path.split('/').filter(|s| !s.is_empty()).collect();
    for (i, part) in parts.iter().enumerate() {
        if *part == "hyperzoekt-clones" && i + 1 < parts.len() {
            let next = parts[i + 1];
            // Check if it looks like name-uuid
            if let Some(last_dash) = next.rfind('-') {
                let potential_uuid = &next[last_dash + 1..];
                let cleaned: String = potential_uuid.chars().filter(|c| *c != '-').collect();
                if cleaned.len() >= 32 && cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(next.to_string());
                }
            }
        }
    }
    None
}

/// Compute a repository-relative path from a (cleaned) file path and a clean repo name.
/// Attempts to find the repo-name segment and return the remaining path. If not found,
/// strips known clone roots and returns a reasonable relative path.
fn repo_relative_from_file(file_path: &str, clean_repo_name: &str) -> String {
    let normalized = clean_file_path(file_path);
    let parts: Vec<&str> = normalized.split('/').filter(|p| !p.is_empty()).collect();

    // Try to find a segment that equals the repo name or a variant
    for (i, p) in parts.iter().enumerate() {
        if *p == clean_repo_name
            || p.starts_with(&format!("{}-", clean_repo_name))
            || *p == clean_repo_name.replace('-', "_")
        {
            if i + 1 < parts.len() {
                return parts[i + 1..].join("/");
            } else {
                return String::new();
            }
        }
    }

    // If the path begins with known clone roots, strip them
    let mut start = 0usize;
    while start < parts.len() {
        let p = parts[start];
        if p == "tmp" || p == "hyperzoekt-clones" {
            start += 1;
            continue;
        }
        // UUID-like segment (hex groups)
        if p.len() >= 8 && p.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
            start += 1;
            continue;
        }
        break;
    }

    if start < parts.len() {
        return parts[start..].join("/");
    }

    // Fallback: if more than one segment, drop the first and return the rest
    if parts.len() > 1 {
        return parts[1..].join("/");
    }

    normalized
}

/// Construct a proper source URL using repository metadata from the database
async fn construct_source_url(
    state: &AppState,
    repo_name: &str,
    file_path: &str,
    uuid_repo_name: Option<&str>,
) -> Option<String> {
    // Clean the repo_name to remove UUID suffix if present
    let clean_repo_name = remove_uuid_suffix(repo_name);

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

    if let Ok(mut res) = match &*state.db.db {
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
                if let Some(normalized_base) = normalize_git_url(&r.git_url) {
                    log::debug!(
                        "construct_source_url: Normalized git_url to '{}'",
                        normalized_base
                    );
                    let branch = r
                        .branch
                        .or_else(|| std::env::var("SOURCE_BRANCH").ok())
                        .unwrap_or_else(|| "main".to_string());

                    // Use helper to compute repo-relative path
                    let repo_relative = repo_relative_from_file(file_path, &clean_repo_name);
                    log::debug!(
                        "construct_source_url: Repo-relative path: '{}'",
                        repo_relative
                    );

                    let url = format!(
                        "{}/blob/{}/{}",
                        normalized_base.trim_end_matches('/'),
                        branch,
                        repo_relative
                    );
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
        "construct_source_url: Using fallback for repo '{}',",
        clean_repo_name
    );
    let source_base = "https://github.com".to_string();
    let source_branch = std::env::var("SOURCE_BRANCH").unwrap_or_else(|_| "main".to_string());

    // Clean and compute repo-relative path similar to the DB-backed branch above
    let normalized_file = clean_file_path(file_path);
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

/// Normalize various git URL forms into an https base URL without a trailing `.git`.
/// Examples:
/// - git@github.com:owner/repo.git -> https://github.com/owner/repo
/// - https://github.com/owner/repo.git -> https://github.com/owner/repo
/// - http://... -> http://...
///   Returns None for local paths (file:// or absolute filesystem paths) where a web URL
///   cannot be constructed.
fn normalize_git_url(git_url: &str) -> Option<String> {
    if git_url.is_empty() {
        return None;
    }

    // Local paths are not convertible to web URLs
    if git_url.starts_with("file://") || std::path::Path::new(git_url).is_absolute() {
        return None;
    }

    // SSH style: git@host:owner/repo.git -> https://host/owner/repo
    if git_url.starts_with("git@") {
        if let Some(colon) = git_url.find(':') {
            let host = &git_url[4..colon]; // strip "git@"
            let rest = &git_url[colon + 1..];
            let s = format!("https://{}/{}", host, rest.trim_end_matches(".git"));
            return Some(s);
        }
    }

    // HTTP/HTTPS style: strip trailing .git
    if git_url.starts_with("http://") || git_url.starts_with("https://") {
        return Some(git_url.trim_end_matches(".git").to_string());
    }

    // Fallback: try to treat as https host/path
    Some(git_url.trim_end_matches(".git").to_string())
}

/// Derive a short display string like "owner/repo/path/to/file" from a full source URL
fn short_display_from_source_url(source_url: &str, repo_name: &str) -> String {
    // Expected forms:
    //  - https://github.com/owner/repo/blob/branch/path/to/file
    //  - https://gitlab.com/owner/repo/-/blob/branch/path/to/file
    // Fallback: if we can't parse, return repo_name + "/" + trailing path from source_url
    if source_url.is_empty() {
        return repo_name.to_string();
    }

    if let Ok(u) = url::Url::parse(source_url) {
        let segments: Vec<String> = u
            .path()
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        // Look for 'blob' or '-/blob' markers and take the pieces after branch
        if let Some(pos) = segments.iter().position(|s| s == "blob") {
            // segments: owner, repo, blob, branch, rest... (GitHub style)
            // Avoid matching the GitLab variant where the segment before 'blob' is '-' (owner/repo/-/blob/...)
            if pos >= 2 && segments[pos - 1] != "-" {
                let owner = &segments[pos - 2];
                let repo = &segments[pos - 1];
                let rest = if pos + 2 <= segments.len() - 1 {
                    segments[pos + 2..].join("/")
                } else {
                    String::new()
                };
                if rest.is_empty() {
                    return format!("{}/{}", owner, repo);
                }
                return format!("{}/{}/{}", owner, repo, rest);
            }
        }
        // GitLab variant: owner/repo/-/blob/branch/path
        if let Some(pos) = segments.iter().position(|s| s == "-") {
            if pos + 2 < segments.len() && segments[pos + 1] == "blob" && pos >= 2 {
                let owner = &segments[pos - 2];
                let repo = &segments[pos - 1];
                let rest = segments[pos + 3..].join("/");
                return format!("{}/{}/{}", owner, repo, rest);
            }
        }
    }

    // fallback: try to strip the repo_name from the path
    if let Some(pos) = source_url.find(repo_name) {
        let tail = &source_url[pos + repo_name.len()..];
        let tail = tail.trim_start_matches('/');
        if tail.is_empty() {
            return repo_name.to_string();
        }
        return format!("{}/{}", repo_name, tail);
    }

    // last resort: return the hostname + path
    source_url.to_string()
}

/// Compute a file-like hint for an entity. Prefer `source_display`, fall back to
/// deriving from `source_url`, otherwise return empty string.
fn file_hint_for_entity(entity: &EntityPayload) -> String {
    if let Some(sd) = &entity.source_display {
        if !sd.is_empty() {
            return sd.clone();
        }
    }
    if let Some(url) = &entity.source_url {
        let short = short_display_from_source_url(url, &entity.repo_name);
        if !short.is_empty() {
            return short;
        }
    }
    String::new()
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

    // Initialize logging with the configured level
    std::env::set_var("RUST_LOG", &config.log_level);
    let mut builder = env_logger::Builder::from_env(env_logger::Env::default());
    builder
        .filter_module("hyper_util", log::LevelFilter::Warn)
        .filter_module("hyper", log::LevelFilter::Warn)
        .filter_module("h2", log::LevelFilter::Warn)
        .filter_module("reqwest", log::LevelFilter::Warn)
        .filter_module("tower_http", log::LevelFilter::Warn);
    builder.init();

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
    log::info!("  Log Level: {}", config.log_level);
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
        .route(
            "/api/entity_graph/{stable_id}",
            get(entity_graph_api_handler),
        )
        .route("/repo/{repo_name}", get(repo_handler))
        .route("/entity/{stable_id}", get(entity_handler))
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
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": "HyperZoekt WebUI is running"
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
            let doc_html = entity.doc.as_ref().map(|d| render_markdown_to_html(d));
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
    // (for example: "gpt-researcher-<uuid>/gpt_researcher/agent.py"), normalize it to
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
        let doc_html = entity.doc.as_ref().map(|d| render_markdown_to_html(d));
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
    repo: Option<String>,
}

async fn search_api_handler(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    log::debug!("search_api_handler: q='{}' repo={:?}", query.q, query.repo);
    let started = std::time::Instant::now();
    let results = state
        .db
        .search_entities(&query.q, query.repo.as_deref())
        .await
        .map_err(|e| {
            log::error!("search_entities failed for q='{}': {}", query.q, e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Deduplicate entities: it's possible for the DB query to produce logically
    // identical rows (e.g., overlapping name/signature matches). Use a stable key.
    // Prefer stable_id when present; fall back to (repo_name, file, name, start_line).
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut unique_results: Vec<EntityPayload> = Vec::with_capacity(results.len());
    for ent in results.into_iter() {
        let key = if !ent.stable_id.is_empty() {
            format!("sid:{}", ent.stable_id)
        } else {
            // Fall back to a composite key of repo/name/signature/line span
            format!(
                "rnsse:{}|{}|{}|{}|{}",
                ent.repo_name,
                ent.name,
                ent.signature,
                ent.start_line.unwrap_or(0),
                ent.end_line.unwrap_or(0)
            )
        };
        if seen.insert(key) {
            unique_results.push(ent);
        }
    }

    let mut results = unique_results;

    // Clean up file paths and prefer stored source URLs; compute only if missing
    for entity in &mut results {
        // Ensure rank present for JSON consumers
        if entity.rank.is_none() {
            entity.rank = Some(0.0);
        }
        // Compute a local file hint (do not mutate EntityPayload)
        let raw_file_hint = file_hint_for_entity(entity);
        let file_hint = clean_file_path(&raw_file_hint);

        // Debug: log DB-provided source_url for search API
        log::debug!(
            "webui: search_api db source_url stable_id={} file='{}' repo='{}' source_url={:?}",
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

    let elapsed_ms = started.elapsed().as_millis();
    Ok(Json(serde_json::json!({
        "results": results,
        "elapsed_ms": elapsed_ms
    })))
}

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

async fn repos_api_handler(
    State(state): State<AppState>,
) -> Result<Json<Vec<RepoSummary>>, StatusCode> {
    let repos = state.db.get_repo_summaries().await.map_err(|e| {
        log::error!("Failed to fetch repo summaries: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(repos))
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
            let doc_html = ent.doc.as_ref().map(|d| render_markdown_to_html(d));
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
    let entity_fields = "file, language, kind, name, parent, signature, start_line, end_line, doc, rank, imports, unresolved_imports, stable_id, repo_name, source_url, source_display";
    let mut response = match &*state.db.db {
        SurrealConnection::Local(db_conn) => {
            db_conn
                .query(format!(
                    "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY rank DESC LIMIT $limit",
                    entity_fields
                ))
                .bind(("repo", query.repo.clone()))
                .bind(("limit", limit as i64))
                .await
        }
        SurrealConnection::RemoteHttp(db_conn) => {
            db_conn
                .query(format!(
                    "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY rank DESC LIMIT $limit",
                    entity_fields
                ))
                .bind(("repo", query.repo.clone()))
                .bind(("limit", limit as i64))
                .await
        }
        SurrealConnection::RemoteWs(db_conn) => {
            db_conn
                .query(format!(
                    "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY rank DESC LIMIT $limit",
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

    let repo_row: Option<RepoRow> = match &*state.db.db {
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
        if let Some(norm) = normalize_git_url(&r.git_url) {
            (Some(norm), branch)
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
                &source_branch,
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
            let doc_html = ent.doc.as_ref().map(|d| render_markdown_to_html(d));
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
        match &*state.db.db {
            SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteWs(db_conn) => {
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
        match &*state.db.db {
            SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteWs(db_conn) => {
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
        match &*state.db.db {
            SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteWs(db_conn) => {
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
        match &*state.db.db {
            SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteWs(db_conn) => {
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
        match &*state.db.db {
            SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteWs(db_conn) => {
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
        match &*state.db.db {
            SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
        }

        std::env::remove_var("SOURCE_BASE_URL");
        std::env::remove_var("SOURCE_BRANCH");
        std::env::set_var("SOURCE_BASE_URL", "https://fallback.example");

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
        match &*state.db.db {
            SurrealConnection::Local(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(insert_q).await.expect("insert repo");
            }
            SurrealConnection::RemoteWs(db_conn) => {
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
