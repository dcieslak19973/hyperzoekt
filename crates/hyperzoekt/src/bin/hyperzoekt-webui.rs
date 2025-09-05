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
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use surrealdb::engine::local::Mem;
use surrealdb::engine::remote::http::Http;
use surrealdb::engine::remote::ws::Ws;
use surrealdb::Surreal;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use hyperzoekt::repo_index::indexer::payload::EntityPayload;

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
                .or_else(|| std::env::var("SURREALDB_URL").ok()),
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
    pub files: Vec<String>,
    pub languages: Vec<String>,
}

#[derive(Clone)]
pub struct Database {
    db: Arc<SurrealConnection>,
}

enum SurrealConnection {
    Local(Surreal<surrealdb::engine::local::Db>),
    RemoteHttp(Surreal<surrealdb::engine::remote::http::Client>),
    RemoteWs(Surreal<surrealdb::engine::remote::ws::Client>),
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
        let db = if let Some(url) = url {
            // Parse the URL to determine connection type
            if url.starts_with("http://") || url.starts_with("https://") {
                // HTTP connection to SurrealDB - extract host and port
                let url_str = url
                    .trim_start_matches("http://")
                    .trim_start_matches("https://");
                log::info!(
                    "Connecting to SurrealDB via HTTP: {} (parsed from {})",
                    url_str,
                    url
                );
                let db = Surreal::new::<Http>(url_str).await?;

                // Authenticate if credentials are provided
                if let (Ok(username), Ok(password)) = (
                    std::env::var("SURREALDB_USERNAME"),
                    std::env::var("SURREALDB_PASSWORD"),
                ) {
                    db.signin(surrealdb::opt::auth::Root {
                        username: &username,
                        password: &password,
                    })
                    .await?;
                    log::info!("Authenticated with SurrealDB as user: {}", username);
                }

                db.use_ns(ns).use_db(db_name).await?;
                SurrealConnection::RemoteHttp(db)
            } else if url.starts_with("ws://") || url.starts_with("wss://") {
                // WebSocket connection to SurrealDB
                log::info!("Connecting to SurrealDB via WebSocket: {}", url);
                let db = Surreal::new::<Ws>(url).await?;

                // Authenticate if credentials are provided
                if let (Ok(username), Ok(password)) = (
                    std::env::var("SURREALDB_USERNAME"),
                    std::env::var("SURREALDB_PASSWORD"),
                ) {
                    db.signin(surrealdb::opt::auth::Root {
                        username: &username,
                        password: &password,
                    })
                    .await?;
                    log::info!("Authenticated with SurrealDB as user: {}", username);
                }

                db.use_ns(ns).use_db(db_name).await?;
                SurrealConnection::RemoteWs(db)
            } else {
                // Assume HTTP connection for URLs without protocol prefix
                let http_url = format!("http://{}", url);
                log::info!(
                    "Connecting to SurrealDB via HTTP: {} (inferred from {})",
                    http_url,
                    url
                );
                let db = Surreal::new::<Http>(&http_url).await?;

                // Authenticate if credentials are provided
                if let (Ok(username), Ok(password)) = (
                    std::env::var("SURREALDB_USERNAME"),
                    std::env::var("SURREALDB_PASSWORD"),
                ) {
                    db.signin(surrealdb::opt::auth::Root {
                        username: &username,
                        password: &password,
                    })
                    .await?;
                    log::info!("Authenticated with SurrealDB as user: {}", username);
                }

                db.use_ns(ns).use_db(db_name).await?;
                SurrealConnection::RemoteHttp(db)
            }
        } else {
            log::info!("No SurrealDB URL provided, using embedded database");
            let db = Surreal::new::<Mem>(()).await?;
            db.use_ns(ns).use_db(db_name).await?;
            SurrealConnection::Local(db)
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
                let file_count = s.files.len() as u64; // Count distinct files
                RepoSummary {
                    name: s.repo_name,
                    entity_count: s.entity_count,
                    file_count,
                    languages: s.languages,
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
        let entities: Vec<EntityPayload> = match &*self.db {
            SurrealConnection::Local(db_conn) => db_conn
                .query("SELECT * FROM entity WHERE repo_name = $repo ORDER BY file, start_line")
                .bind(("repo", repo_name.to_string()))
                .await?
                .take(0)?,
            SurrealConnection::RemoteHttp(db_conn) => db_conn
                .query("SELECT * FROM entity WHERE repo_name = $repo ORDER BY file, start_line")
                .bind(("repo", repo_name.to_string()))
                .await?
                .take(0)?,
            SurrealConnection::RemoteWs(db_conn) => db_conn
                .query("SELECT * FROM entity WHERE repo_name = $repo ORDER BY file, start_line")
                .bind(("repo", repo_name.to_string()))
                .await?
                .take(0)?,
        };

        if !entities.is_empty() {
            return Ok(entities);
        }

        // Fallback: look for file paths that start with the provided repo_name.
        // Use a parameterized query to avoid injection.
        let entities2: Vec<EntityPayload> = match &*self.db {
            SurrealConnection::Local(db_conn) => db_conn
                .query(
                    "SELECT * FROM entity WHERE string::starts_with(file, $repo) ORDER BY file, start_line",
                )
                .bind(("repo", repo_name.to_string()))
                .await?
                .take(0)?,
            SurrealConnection::RemoteHttp(db_conn) => db_conn
                .query(
                    "SELECT * FROM entity WHERE string::starts_with(file, $repo) ORDER BY file, start_line",
                )
                .bind(("repo", repo_name.to_string()))
                .await?
                .take(0)?,
            SurrealConnection::RemoteWs(db_conn) => db_conn
                .query(
                    "SELECT * FROM entity WHERE string::starts_with(file, $repo) ORDER BY file, start_line",
                )
                .bind(("repo", repo_name.to_string()))
                .await?
                .take(0)?,
        };

        Ok(entities2)
    }

    pub async fn get_all_entities(&self) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        let mut response = self
            .db
            .query("SELECT * FROM entity ORDER BY file, start_line")
            .await?;
        let entities: Vec<EntityPayload> = response.take(0)?;

        Ok(entities)
    }

    pub async fn get_entity_by_id(
        &self,
        stable_id: &str,
    ) -> Result<Option<EntityPayload>, Box<dyn std::error::Error>> {
        let query = format!(
            r#"
            SELECT * FROM entity
            WHERE stable_id = '{}'
        "#,
            stable_id.replace("'", "\\'")
        );

        let mut response = self.db.query(&query).await?;
        let entities: Vec<EntityPayload> = response.take(0)?;

        Ok(entities.into_iter().next())
    }

    /// Query the database for callers and callees of an entity using SurrealDB queries.
    /// Returns (callers, callees) where each is a Vec<(name, Option<stable_id>)>
    pub async fn get_entity_relations(
        &self,
        stable_id: &str,
        name: &str,
        limit: usize,
    ) -> Result<
        (Vec<(String, Option<String>)>, Vec<(String, Option<String>)>),
        Box<dyn std::error::Error>,
    > {
        // Callees: resolve entries in this entity's `calls` array to entities by stable_id or name
        // We'll perform two queries: one matching stable_id in calls, and one matching name in calls.
        let mut callers: Vec<(String, Option<String>)> = Vec::new();
        let mut callees: Vec<(String, Option<String>)> = Vec::new();

        // Query callers: find entities where this stable_id or name appears in their calls array
        let callers_query = r#"
            SELECT name, stable_id, rank FROM entity WHERE $id IN calls OR $name IN calls ORDER BY rank DESC LIMIT $limit
        "#;

        // Query callees by matching calls array elements to existing entities' stable_id or name
        // We use a two-step approach: fetch distinct call strings from this entity, then lookup entities
        let callees_distinct_query = r#"
            SELECT array::distinct(calls) as calls FROM entity WHERE stable_id = $id LIMIT 1
        "#;

        // Execute callers query
        let mut response = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(callers_query)
                    .bind(("id", stable_id.to_string()))
                    .bind(("name", name.to_string()))
                    .bind(("limit", limit as i64))
                    .await?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn
                    .query(callers_query)
                    .bind(("id", stable_id.to_string()))
                    .bind(("name", name.to_string()))
                    .bind(("limit", limit as i64))
                    .await?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn
                    .query(callers_query)
                    .bind(("id", stable_id.to_string()))
                    .bind(("name", name.to_string()))
                    .bind(("limit", limit as i64))
                    .await?
            }
        };

        // Extract callers
        if let Ok(rows) = response.take::<Vec<serde_json::Value>>(0) {
            for row in rows.into_iter() {
                if let (Some(n), Some(id)) = (row.get("name"), row.get("stable_id")) {
                    let n = n.as_str().unwrap_or_default().to_string();
                    let id = id.as_str().map(|s| s.to_string());
                    callers.push((n, id));
                }
            }
        }

        // Get distinct calls from this entity
        let mut resp2 = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(callees_distinct_query)
                    .bind(("id", stable_id.to_string()))
                    .await?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn
                    .query(callees_distinct_query)
                    .bind(("id", stable_id.to_string()))
                    .await?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn
                    .query(callees_distinct_query)
                    .bind(("id", stable_id.to_string()))
                    .await?
            }
        };

        let calls_list: Vec<String> = if let Ok(vals) = resp2.take::<Vec<serde_json::Value>>(0) {
            if let Some(first) = vals.into_iter().next() {
                if let Some(calls_val) = first.get("calls") {
                    if calls_val.is_array() {
                        calls_val
                            .as_array()
                            .unwrap()
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        if !calls_list.is_empty() {
            // Lookup entities whose stable_id or name is in calls_list
            // Build an IN-list safely by binding each as a separate parameter is tedious; instead use array::contains in Surreal
            // We'll perform two queries: match stable_id in array, then name in array
            let lookup_by_id = r#"
                SELECT name, stable_id, rank FROM entity WHERE stable_id IN $calls ORDER BY rank DESC LIMIT $limit
            "#;

            let lookup_by_name = r#"
                SELECT name, stable_id, rank FROM entity WHERE name IN $calls ORDER BY rank DESC LIMIT $limit
            "#;

            // Bind calls as a JSON array string
            let calls_json = serde_json::to_value(&calls_list)?;

            let mut resp_id = match &*self.db {
                SurrealConnection::Local(db_conn) => {
                    db_conn
                        .query(lookup_by_id)
                        .bind(("calls", calls_json.clone()))
                        .bind(("limit", limit as i64))
                        .await?
                }
                SurrealConnection::RemoteHttp(db_conn) => {
                    db_conn
                        .query(lookup_by_id)
                        .bind(("calls", calls_json.clone()))
                        .await?
                }
                SurrealConnection::RemoteWs(db_conn) => {
                    db_conn
                        .query(lookup_by_id)
                        .bind(("calls", calls_json.clone()))
                        .bind(("limit", limit as i64))
                        .await?
                }
            };

            if let Ok(rows) = resp_id.take::<Vec<serde_json::Value>>(0) {
                for row in rows.into_iter() {
                    let n = row
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let id = row
                        .get("stable_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    callees.push((n, id));
                }
            }

            let mut resp_name = match &*self.db {
                SurrealConnection::Local(db_conn) => {
                    db_conn
                        .query(lookup_by_name)
                        .bind(("calls", calls_json.clone()))
                        .bind(("limit", limit as i64))
                        .await?
                }
                SurrealConnection::RemoteHttp(db_conn) => {
                    db_conn
                        .query(lookup_by_name)
                        .bind(("calls", calls_json.clone()))
                        .await?
                }
                SurrealConnection::RemoteWs(db_conn) => {
                    db_conn
                        .query(lookup_by_name)
                        .bind(("calls", calls_json.clone()))
                        .bind(("limit", limit as i64))
                        .await?
                }
            };

            if let Ok(rows) = resp_name.take::<Vec<serde_json::Value>>(0) {
                for row in rows.into_iter() {
                    let n = row
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string();
                    let id = row
                        .get("stable_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    // Avoid duplicates
                    if !callees.iter().any(|(cn, cid)| cn == &n && cid == &id) {
                        callees.push((n, id));
                    }
                }
            }
            // Include unresolved call strings as (call, None) entries so callers/callees page shows unknown targets
            for call in calls_list.iter() {
                if !callees.iter().any(|(cn, _)| cn == call) {
                    callees.push((call.clone(), None));
                }
            }
        }

        Ok((callers, callees))
    }

    pub async fn search_entities(
        &self,
        query: &str,
        repo_filter: Option<&str>,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        // Escape single quotes in the query for SQL safety
        let escaped_query = query.replace("'", "\\'");

        let mut sql_query = format!(
            r#"
            SELECT * FROM entity
            WHERE (string::matches(string::lowercase(name), '{}') 
                   OR string::matches(string::lowercase(signature), '{}') 
                   OR string::matches(string::lowercase(file), '{}'))
        "#,
            escaped_query.to_lowercase(),
            escaped_query.to_lowercase(),
            escaped_query.to_lowercase()
        );

        if let Some(repo) = repo_filter {
            sql_query.push_str(&format!(
                " AND string::starts_with(file, '{}')",
                repo.replace("'", "\\'")
            ));
        }

        sql_query.push_str(" ORDER BY rank DESC LIMIT 100");

        let mut response = self.db.query(&sql_query).await?;
        let entities: Vec<EntityPayload> = response.take(0)?;

        Ok(entities)
    }

    pub async fn insert_test_data(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Insert test entities using direct SQL with all required fields
        let queries = vec![
            r#"CREATE entity CONTENT {
                stable_id: "test1",
                name: "getUserData",
                repo_name: "hyperzoekt-abc123",
                signature: "def getUserData() -> User",
                file: "/tmp/hyperzoekt-clones/hyperzoekt-abc123/src/user.py",
                language: "python",
                kind: "function",
                parent: null,
                start_line: 10,
                end_line: 15,
                calls: [],
                doc: null,
                rank: 0.8,
                imports: [],
                unresolved_imports: []
            }"#,
            r#"CREATE entity CONTENT {
                stable_id: "test2",
                name: "processData",
                repo_name: "hyperzoekt-abc123",
                signature: "def processData(data: Data) -> None",
                file: "/tmp/hyperzoekt-clones/hyperzoekt-abc123/src/data.py",
                language: "python",
                kind: "function",
                parent: null,
                start_line: 20,
                end_line: 25,
                calls: [],
                doc: null,
                rank: 0.7,
                imports: [],
                unresolved_imports: []
            }"#,
            r#"CREATE entity CONTENT {
                stable_id: "test3",
                name: "UserService",
                repo_name: "gpt-researcher-def456",
                signature: "class UserService",
                file: "/tmp/hyperzoekt-clones/gpt-researcher-def456/src/service.ts",
                language: "typescript",
                kind: "class",
                parent: null,
                start_line: 5,
                end_line: 30,
                calls: [],
                doc: null,
                rank: 0.9,
                imports: [],
                unresolved_imports: []
            }"#,
            r#"CREATE entity CONTENT {
                stable_id: "test4",
                name: "calculateTotal",
                repo_name: "gpt-researcher-def456",
                signature: "function calculateTotal(items: Item[]): number",
                file: "/tmp/hyperzoekt-clones/gpt-researcher-def456/src/math.ts",
                language: "typescript",
                kind: "function",
                parent: null,
                start_line: 12,
                end_line: 18,
                calls: [],
                doc: null,
                rank: 0.6,
                imports: [],
                unresolved_imports: []
            }"#,
            r#"CREATE entity CONTENT {
                stable_id: "test5",
                name: "DatabaseConnection",
                repo_name: "gpt-researcher-def456",
                signature: "class DatabaseConnection",
                file: "/tmp/hyperzoekt-clones/gpt-researcher-def456/src/db.tsx",
                language: "tsx",
                kind: "class",
                parent: null,
                start_line: 1,
                end_line: 50,
                calls: [],
                doc: null,
                rank: 0.85,
                imports: [],
                unresolved_imports: []
            }"#,
            r#"CREATE entity CONTENT {
                stable_id: "test6",
                name: "validateInput",
                repo_name: "gpt-researcher-def456",
                signature: "function validateInput(input: string): boolean",
                file: "/tmp/hyperzoekt-clones/gpt-researcher-def456/src/validation.js",
                language: "javascript",
                kind: "function",
                parent: null,
                start_line: 5,
                end_line: 20,
                calls: [],
                doc: null,
                rank: 0.75,
                imports: [],
                unresolved_imports: []
            }"#,
        ];

        for query in queries {
            self.db.query(query).await?;
        }

        Ok(())
    }
}

#[derive(Clone)]
struct AppState {
    db: Database,
    templates: Environment<'static>,
}

/// Clean up file paths to show repository-relative paths instead of absolute filesystem paths
/// Converts paths like "/tmp/hyperzoekt-clones/repo-uuid/path/to/file.rs" to "repo/path/to/file.rs"
fn clean_file_path(file_path: &str) -> String {
    // Pattern: /tmp/hyperzoekt-clones/{repo-name}-{uuid}/{repo-relative-path}
    if let Some(clones_pos) = file_path.find("/tmp/hyperzoekt-clones/") {
        let after_clones = &file_path[clones_pos + "/tmp/hyperzoekt-clones/".len()..];

        // Find the first '/' after the repo name and UUID
        if let Some(slash_pos) = after_clones.find('/') {
            // Extract repo name (everything before the first '/')
            let repo_part = &after_clones[..slash_pos];

            // Extract repo-relative path (everything after the first '/')
            let relative_path = &after_clones[slash_pos + 1..];

            // If repo_part contains a UUID pattern (like "repo-uuid"), extract just the repo name
            if let Some(uuid_start) = repo_part.find('-') {
                // Look for UUID pattern (32 hex chars or similar)
                let potential_uuid = &repo_part[uuid_start + 1..];
                if potential_uuid.len() >= 32 && potential_uuid.chars().all(|c| c.is_alphanumeric())
                {
                    // This looks like a UUID, extract just the repo name
                    let repo_name = &repo_part[..uuid_start];
                    return format!("{}/{}", repo_name, relative_path);
                }
            }

            // Fallback: use the whole repo part
            return format!("{}/{}", repo_part, relative_path);
        }
    }

    // If we can't parse the path, return it as-is but try to make it more readable
    // Remove the /tmp/hyperzoekt-clones/ prefix if present
    if let Some(clones_pos) = file_path.find("/tmp/hyperzoekt-clones/") {
        file_path[clones_pos + "/tmp/hyperzoekt-clones/".len()..].to_string()
    } else {
        file_path.to_string()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = Config::from_args(args);

    // Initialize logging with the configured level
    std::env::set_var("RUST_LOG", &config.log_level);
    env_logger::init();

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

    // Insert test data for demo purposes
    if let Err(e) = db.insert_test_data().await {
        log::warn!("Failed to insert test data: {}", e);
        // Don't fail the startup if test data insertion fails
    } else {
        log::info!("Test data inserted successfully");
    }

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
        .route("/repo/:repo_name", get(repo_handler))
        .route("/entity/:stable_id", get(entity_handler))
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

    // Compute source base URL and branch from environment (defaults chosen for convenience)
    let source_base =
        std::env::var("SOURCE_BASE_URL").unwrap_or_else(|_| "https://github.com".to_string());
    let source_branch = std::env::var("SOURCE_BRANCH").unwrap_or_else(|_| "main".to_string());

    // Clean up file paths and populate a direct source URL only when DB did not provide one
    for entity in &mut entities {
        // Clean the displayed file path
        entity.file = clean_file_path(&entity.file);

        if entity.source_url.is_none() && !entity.repo_name.is_empty() {
            // entity.file may already be repo-relative 'repo/path/to/file', remove leading repo if present
            let repo = &entity.repo_name;
            let file_path = entity.file.replace(&format!("{}/", repo), "");
            let url = format!(
                "{}/{}/blob/{}/{}",
                source_base.trim_end_matches('/'),
                repo,
                source_branch,
                file_path
            );
            entity.source_url = Some(url);
        }
    }

    let template = state.templates.get_template("repo").unwrap();
    let html = template
        .render(context! {
            repo_name => repo_name,
            entities => entities,
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

    // Clean up the file path
    entity.file = clean_file_path(&entity.file);
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
    let (callers, callees) = state
        .db
        .get_entity_relations(&entity.stable_id, &entity.name, relations_limit)
        .await
        .map_err(|e| {
            log::error!(
                "Failed to resolve callers/callees from DB for {}: {}",
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

    let template = state.templates.get_template("entity").unwrap();
    // Ensure repo_name is available to the template (entity.html references `repo_name`)
    let repo_name = entity.repo_name.clone();
    let html = template
        .render(context! {
            entity => entity,
            repo_name => repo_name,
            callers => callers,
            callees => callees,
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
) -> Result<Json<Vec<EntityPayload>>, StatusCode> {
    log::debug!("search_api_handler: q='{}' repo={:?}", query.q, query.repo);
    let mut results = state
        .db
        .search_entities(&query.q, query.repo.as_deref())
        .await
        .map_err(|e| {
            log::error!("search_entities failed for q='{}': {}", query.q, e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Clean up file paths to show repository-relative paths instead of absolute filesystem paths
    for entity in &mut results {
        entity.file = clean_file_path(&entity.file);
    }

    Ok(Json(results))
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
) -> Result<Json<Vec<EntityPayload>>, StatusCode> {
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

    // Clean up file paths for all entities
    for entity in &mut entities {
        entity.file = clean_file_path(&entity.file);
    }

    Ok(Json(entities))
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
}

async fn pagerank_api_handler(
    State(state): State<AppState>,
    Query(query): Query<PageRankQuery>,
) -> Result<Json<Vec<EntityPayload>>, StatusCode> {
    log::debug!("pagerank_api_handler: repo={}", &query.repo);
    let mut entities = state
        .db
        .get_entities_for_repo(&query.repo)
        .await
        .map_err(|e| {
            log::error!(
                "pagerank get_entities_for_repo failed for {}: {}",
                &query.repo,
                e
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    log::debug!(
        "pagerank_api_handler: fetched {} entities for repo={}",
        entities.len(),
        &query.repo
    );

    // Clean up file paths for all entities
    for entity in &mut entities {
        entity.file = clean_file_path(&entity.file);
    }

    Ok(Json(entities))
}
