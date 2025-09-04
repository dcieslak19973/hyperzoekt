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
use axum::extract::Json as ReqJson;
use axum::extract::Query;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Json;
use axum::Extension;
use clap::Parser;
use parking_lot::RwLock;
use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing_subscriber::EnvFilter;
use zoekt_rs::query::{QueryPlan, Searcher};

use zoekt_distributed::{
    InMemoryIndex, Indexer, LeaseManager, Node, NodeConfig, NodeType, RemoteRepo,
};

type RemoteList = std::sync::Arc<RwLock<Vec<RemoteRepo>>>;

fn make_snippet(
    text: &str,
    _path: &str,
    context: usize,
    needle: &str,
) -> (Option<String>, Option<zoekt_rs::types::Symbol>) {
    let lines: Vec<&str> = text.lines().collect();
    if needle.trim().is_empty() {
        return (None, None);
    }

    // Determine matching strategy:
    // - If needle is wrapped in /slashes/, treat as a regex: /foo.*bar/
    // - Else if needle contains quoted phrases, match those exact phrases
    // - Otherwise split on whitespace and match any term (case-insensitive substring)

    let mut matches: Vec<usize> = Vec::new();

    // regex case: starts and ends with '/'
    if needle.starts_with('/') && needle.rfind('/').map(|i| i > 0).unwrap_or(false) {
        if let Some(end) = needle.rfind('/') {
            let pat = &needle[1..end];
            if let Ok(re) = Regex::new(pat) {
                for (i, line) in lines.iter().enumerate() {
                    if re.is_match(line) {
                        matches.push(i);
                    }
                }
            }
        }
    } else {
        // extract quoted phrases first
        let mut terms: Vec<String> = Vec::new();
        let mut rest = needle.to_string();
        let quote_re = Regex::new(r#""([^"]*)""#).unwrap();
        for cap in quote_re.captures_iter(&rest) {
            if let Some(m) = cap.get(1) {
                terms.push(m.as_str().to_string());
            }
        }
        // remove quoted parts from rest
        rest = quote_re.replace_all(&rest, " ").to_string();
        // remaining whitespace-split terms
        for t in rest.split_whitespace() {
            if !t.is_empty() {
                terms.push(t.to_string());
            }
        }

        if !terms.is_empty() {
            for (i, line) in lines.iter().enumerate() {
                let line_l = line.to_lowercase();
                if terms.iter().any(|t| line_l.contains(&t.to_lowercase())) {
                    matches.push(i);
                }
            }
        }
    }

    if matches.is_empty() {
        return (None, None);
    }

    // Extract symbols for potential fallback
    let ext = Path::new(_path)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    let all_symbols = extract_symbols_local(text, ext);

    // Find symbol near first match
    let first_match_line = matches[0];
    let mut pos = 0usize;
    for (i, line) in lines.iter().enumerate() {
        if i == first_match_line {
            break;
        }
        pos += line.len() + 1;
    }
    let pos_u32 = pos as u32;
    let mut chosen: Option<zoekt_rs::types::Symbol> = None;
    for sym in &all_symbols {
        if let Some(sstart) = sym.start {
            if sstart <= pos_u32 {
                match &chosen {
                    Some(c) => {
                        if sstart > c.start.unwrap_or(0) {
                            chosen = Some(sym.clone());
                        }
                    }
                    None => chosen = Some(sym.clone()),
                }
            }
        }
    }

    // merge matches into context windows
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    for &m in &matches {
        let start = m.saturating_sub(context);
        let end = std::cmp::min(lines.len(), m + context + 1);
        if let Some(last) = ranges.last_mut() {
            // if windows overlap or touch, merge them
            if start <= last.1 {
                last.1 = std::cmp::max(last.1, end);
                continue;
            }
        }
        ranges.push((start, end));
    }

    let match_set: std::collections::HashSet<usize> = matches.into_iter().collect();

    let mut out = String::new();
    for (ri, (start, end)) in ranges.into_iter().enumerate() {
        // separate multiple windows with a divider for readability
        if ri > 0 {
            out.push_str(
                "...
",
            );
        }
        for (j, l) in lines[start..end].iter().enumerate() {
            let ln = start + j + 1;
            if match_set.contains(&(ln - 1)) {
                out.push_str(&format!(
                    "{:6} > {}
",
                    ln, l
                ));
            } else {
                out.push_str(&format!(
                    "{:6}   {}
",
                    ln, l
                ));
            }
        }
    }

    (Some(out), chosen)
}
// so this binary can populate `symbol` and `symbol_loc` without relying on
// internal indexing crate APIs.
fn line_for_offset(content: &str, pos: u32) -> usize {
    let bytes = content.as_bytes();
    let mut idx = 0usize;
    let mut line = 0usize;
    while idx < bytes.len() && (idx as u32) < pos {
        if bytes[idx] == b'\n' {
            line += 1;
        }
        idx += 1;
    }
    line
}

fn extract_symbols_local(content: &str, ext: &str) -> Vec<zoekt_rs::types::Symbol> {
    let mut out = Vec::new();
    if ext == "rs" {
        let re_fn = Regex::new(r"\bfn\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap();
        let re_struct = Regex::new(r"\bstruct\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap();
        for cap in re_fn.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                out.push(zoekt_rs::types::Symbol {
                    name: m.as_str().to_string(),
                    start: Some(m.start() as u32),
                    line: Some(line_for_offset(content, m.start() as u32) as u32 + 1),
                });
            }
        }
        for cap in re_struct.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                out.push(zoekt_rs::types::Symbol {
                    name: m.as_str().to_string(),
                    start: Some(m.start() as u32),
                    line: Some(line_for_offset(content, m.start() as u32) as u32 + 1),
                });
            }
        }
    } else if ext == "py" {
        let re = Regex::new(r"^(?:\s*)(?:def|class)\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap();
        let mut offset = 0usize;
        for line in content.lines() {
            if let Some(cap) = re.captures(line) {
                if let Some(m) = cap.get(1) {
                    out.push(zoekt_rs::types::Symbol {
                        name: m.as_str().to_string(),
                        start: Some((offset + m.start()) as u32),
                        line: Some(
                            (line_for_offset(content, (offset + m.start()) as u32) as u32) + 1,
                        ),
                    });
                }
            }
            offset += line.len() + 1;
        }
    } else if ext == "go" {
        let re = Regex::new(r"\bfunc(?:\s*\(.*?\))?\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap();
        for cap in re.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                out.push(zoekt_rs::types::Symbol {
                    name: m.as_str().to_string(),
                    start: Some(m.start() as u32),
                    line: Some(line_for_offset(content, m.start() as u32) as u32 + 1),
                });
            }
        }
    }
    out
}

// Extract user ID from request headers
// Supports JWT tokens in Authorization header or user ID in X-User-ID header
fn extract_user_id(headers: &HeaderMap) -> Option<String> {
    // Try Authorization header first (Bearer token)
    if let Some(auth_header) = headers.get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                // Remove "Bearer " prefix
                // For now, we'll use the token as-is. In production, you'd validate the JWT
                // and extract the user ID from the claims
                return Some(token.to_string());
            }
        }
    }

    // Fallback to X-User-ID header
    if let Some(user_header) = headers.get("x-user-id") {
        if let Ok(user_str) = user_header.to_str() {
            return Some(user_str.to_string());
        }
    }

    None
}

#[derive(Parser)]
struct Opts {
    /// Path to a TOML config file (optional)
    #[arg(long)]
    config: Option<PathBuf>,

    /// Node id (overrides env/config)
    #[arg(long)]
    id: Option<String>,

    /// Lease TTL in seconds (overrides env/config)
    #[arg(long)]
    lease_ttl_seconds: Option<u64>,

    /// Poll interval in seconds (overrides env/config)
    #[arg(long)]
    poll_interval_seconds: Option<u64>,

    /// Remote repo name
    /// Remote repo name (can be supplied multiple times to match multiple --remote-url)
    #[arg(long)]
    remote_name: Vec<String>,

    /// Remote repo URL/path (can be supplied multiple times)
    #[arg(long)]
    remote_url: Vec<String>,
    /// Address to listen on for HTTP search (env: ZOEKTD_BIND_ADDR)
    #[arg(long)]
    listen: Option<String>,
    /// Disable periodic reindexing (useful for dev when you don't want frequent re-index)
    #[arg(long)]
    disable_reindex: bool,
    /// Index each repo only once and then stop reindexing it.
    /// When enabled, successfully indexed repos are recorded in-memory and
    /// skipped on future polling cycles. Use `ZOEKTD_INDEX_ONCE=true` to set
    /// this behavior via environment instead.
    #[arg(long)]
    index_once: bool,
}

/// Shared in-memory store for indexes keyed by the repo url/path string.
type IndexStore = Arc<RwLock<HashMap<String, InMemoryIndex>>>;

struct SimpleIndexer {
    store: IndexStore,
}

impl SimpleIndexer {
    fn new(store: IndexStore) -> Self {
        Self { store }
    }
}

impl Indexer for SimpleIndexer {
    fn index_repo(&self, repo_path: PathBuf) -> Result<InMemoryIndex> {
        let repo_path_str = repo_path.to_string_lossy().to_string();
        tracing::debug!(repo_path=%repo_path_str, "starting repository indexing");

        // Check if this is a git URL that needs to be cloned
        let is_git_url = repo_path_str.starts_with("http://")
            || repo_path_str.starts_with("https://")
            || repo_path_str.starts_with("git@")
            || repo_path_str.starts_with("ssh://");

        let actual_repo_path = if is_git_url {
            // Check if this git URL is the current repository
            if is_current_repo_sync(&repo_path_str) {
                tracing::info!(url=%repo_path_str, "detected current repository, using current directory");
                std::env::current_dir()
                    .map_err(|e| anyhow::anyhow!("Failed to get current directory: {}", e))?
            } else {
                // For git URLs, clone to a temporary directory
                let temp_dir = std::env::temp_dir().join(format!(
                    "hyperzoekt-{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                ));

                tracing::info!(url=%repo_path_str, temp_dir=%temp_dir.display(), "cloning remote repository");

                // Clone the repository
                let clone_result = std::process::Command::new("git")
                    .args([
                        "clone",
                        "--depth",
                        "1",
                        &repo_path_str,
                        &temp_dir.to_string_lossy(),
                    ])
                    .output();

                match clone_result {
                    Ok(output) if output.status.success() => {
                        tracing::info!(url=%repo_path_str, "successfully cloned repository");
                        temp_dir
                    }
                    Ok(output) => {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        return Err(anyhow::anyhow!("Failed to clone repository: {}", stderr));
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("Failed to run git clone: {}", e));
                    }
                }
            }
        } else {
            // For local paths, use as-is
            repo_path
        };

        // Ensure directory exists; do not attempt to create files. Indexer will only read
        // existing readable files to avoid write attempts that can fail under restricted perms.
        if !actual_repo_path.exists() {
            std::fs::create_dir_all(&actual_repo_path)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        }

        tracing::info!(path=%actual_repo_path.display(), "starting index build");
        // Build a real in-memory index for the repo dir using the IndexBuilder
        // Explicitly enable symbol extraction at index time.
        let builder = zoekt_rs::IndexBuilder::new(actual_repo_path.clone()).enable_symbols(true);
        let idx = builder
            .build()
            .map_err(|e| anyhow::anyhow!(format!("index build error: {}", e)))?;

        // Store the built index under the original repo URL/path string so search handlers can find it
        // For git URLs, use the original URL as the key; for local paths, use the path
        let key = if is_git_url {
            repo_path_str
        } else {
            actual_repo_path.to_string_lossy().into_owned()
        };
        self.store.write().insert(key.clone(), idx.clone());
        tracing::info!(repo=%key, "index build complete and stored");
        tracing::debug!(repo=%key, "repository indexing completed successfully");
        Ok(idx)
    }

    fn remove_index(&self, repo_key: &str) {
        let removed = self.store.write().remove(repo_key);
        if removed.is_some() {
            tracing::info!(repo=%repo_key, "removed index from memory");
        } else {
            tracing::debug!(repo=%repo_key, "index not found in memory, nothing to remove");
        }
    }

    fn get_indexed_repos(&self) -> Vec<String> {
        self.store.read().keys().cloned().collect()
    }
}

/// Synchronous version of is_current_repo for use in blocking indexer
fn is_current_repo_sync(git_url: &str) -> bool {
    // Try to get the remote URL of the current directory
    match std::process::Command::new("git")
        .args(["remote", "get-url", "origin"])
        .current_dir(".")
        .output()
    {
        Ok(output) if output.status.success() => {
            let remote_url = String::from_utf8_lossy(&output.stdout).trim().to_string();
            // Normalize URLs by removing trailing .git and comparing
            let normalized_git_url = git_url.trim_end_matches(".git");
            let normalized_remote = remote_url.trim_end_matches(".git");
            tracing::debug!(git_url=%git_url, remote_url=%remote_url, normalized_git_url=%normalized_git_url, normalized_remote=%normalized_remote, "checking if current repo");
            if normalized_git_url == normalized_remote {
                tracing::info!(git_url=%git_url, remote_url=%remote_url, "detected current repository, using current directory");
                return true;
            } else {
                tracing::debug!(git_url=%git_url, remote_url=%remote_url, "not current repository");
            }
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::debug!(git_url=%git_url, status=%output.status, stderr=%stderr, "git remote get-url failed");
        }
        Err(e) => {
            tracing::debug!(git_url=%git_url, error=%e, "failed to run git remote get-url");
        }
    }
    false
}

#[derive(Deserialize)]
struct SearchParams {
    repo: String,
    q: String,
    /// number of context lines to include before/after the match (optional)
    context_lines: Option<usize>,
}
// handler will be registered inline in main using an async closure so axum can infer the handler

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing using the RUST_LOG env var when present, default to `info`
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let opts = Opts::parse();

    // determine bind address from CLI flag, env var, or default
    let _bind_addr = opts
        .listen
        .clone()
        .or_else(|| std::env::var("ZOEKTD_BIND_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:3000".into());

    // Determine endpoint for registration: prefer ZOEKT_ENDPOINT env var, fallback to listen addr
    let endpoint = if let Ok(e) = std::env::var("ZOEKT_ENDPOINT") {
        Some(e)
    } else {
        opts.listen.as_ref().map(|addr| format!("http://{}", addr))
    };

    let cfg = zoekt_distributed::load_node_config(
        NodeConfig {
            node_type: NodeType::Indexer,
            endpoint: None, // Let NodeConfig discover the endpoint automatically
            ..Default::default()
        },
        zoekt_distributed::MergeOpts {
            config_path: opts.config,
            cli_id: opts.id,
            cli_lease_ttl_seconds: opts.lease_ttl_seconds,
            cli_poll_interval_seconds: opts.poll_interval_seconds,
            cli_endpoint: endpoint, // Only override if explicitly provided
            cli_enable_reindex: Some(!opts.disable_reindex),
            cli_index_once: Some(opts.index_once),
        },
    )?;

    let lease_mgr = LeaseManager::new().await;
    // shared store of indexes that the HTTP handler will read from
    let store: IndexStore = Arc::new(RwLock::new(HashMap::new()));
    let indexer = SimpleIndexer::new(store.clone());
    // Create the node (it holds the authoritative list of registered repos)
    let node = Node::new(cfg, lease_mgr.clone(), indexer);
    // Use the node's internal registered repos list for the HTTP handlers so
    // runtime additions by the node are visible to /status and /search.
    let registered_repos = node.registered_repos();

    // Collect remote URLs and optional names from CLI or env. Support multiple values.
    let mut urls: Vec<String> = opts.remote_url.clone();
    if urls.is_empty() {
        if let Ok(s) = std::env::var("REMOTE_REPO_URL") {
            urls.push(s);
        }
        // Don't add default demo repo when no remote URLs are configured
        // The indexer will still pick up repos from Redis via get_repo_branches()
    }

    // remote_name may be provided multiple times; if not present, try env or use basename/demo
    let names = if !opts.remote_name.is_empty() {
        opts.remote_name.clone()
    } else if let Ok(s) = std::env::var("REMOTE_REPO_NAME") {
        vec![s]
    } else {
        Vec::new()
    };

    for (i, url) in urls.into_iter().enumerate() {
        let name = if let Some(n) = names.get(i) {
            n.clone()
        } else if let Some(b) = Path::new(&url).file_name().and_then(|s| s.to_str()) {
            b.to_string()
        } else {
            format!("repo-{}", i)
        };

        tracing::info!(remote = %name, url = %url, "registering remote repo");
        let repo = RemoteRepo {
            name,
            git_url: url,
            branch: Some("main".into()),
            visibility: zoekt_rs::types::RepoVisibility::Public, // Default to public
            owner: None,
            allowed_users: Vec::new(),
            last_commit_sha: None,
        };
        // record in local registered list and register with the node
        registered_repos.write().push(repo.clone());
        node.add_remote(repo.clone());

        // Update repository metadata in SurrealDB
        lease_mgr.update_repo_metadata(&repo, None).await;
    }

    // Before spawning the node, also register repositories from Redis
    let redis_repos = lease_mgr.get_repo_branches().await;
    for (name, branch, url) in redis_repos {
        let repo = RemoteRepo {
            name: name.clone(),
            git_url: url.clone(),
            branch: Some(branch.clone()),
            visibility: zoekt_rs::types::RepoVisibility::Public, // Default to public
            owner: None,
            allowed_users: Vec::new(),
            last_commit_sha: None,
        };
        // Add to the registered repos list and register with the node
        registered_repos.write().push(repo.clone());
        node.add_remote(repo.clone());
        // Update repository metadata in SurrealDB
        lease_mgr.update_repo_metadata(&repo, None).await;
        tracing::info!(repo=%name, branch=%branch, url=%url, "registered Redis repo with search handlers and node");
    }

    // spawn the node loop in the background (indexing/lease loop)
    let node_handle = tokio::spawn(async move {
        // run for a long time for demo/dev; in production this would be indefinite
        let _ = node.run_for(Duration::from_secs(60 * 60)).await;
    }); // start a small HTTP server to answer search requests against the in-memory indexes
    let app = axum::Router::new()
        .route(
            "/search",
            axum::routing::get(|Query(params): Query<SearchParams>, Extension(store): Extension<IndexStore>, Extension(reg): Extension<RemoteList>, Extension(lease_mgr): Extension<LeaseManager>, headers: HeaderMap| async move {
                tracing::info!(repo=%params.repo, q=%params.q, "received search request");

                // Extract user ID from headers
                let user_id_binding = extract_user_id(&headers);
                let user_id = user_id_binding.as_deref();

                // Resolve repo name to git_url if it's a registered name
                let mut resolved_repos = Vec::new();
                let registered_repos_list = reg.read().clone(); // Clone the list to avoid holding lock across await
                if params.repo == "*" {
                    // Wildcard search: search all indexed repos
                    for repo in &registered_repos_list {
                        // Check permissions for each repo
                        if lease_mgr.can_user_access_repo(&repo.git_url, user_id).await {
                            resolved_repos.push(repo.git_url.clone());
                        } else {
                            tracing::debug!(repo=%repo.git_url, "access denied for user");
                        }
                    }
                } else {
                    // Single repo search
                    let mut resolved_repo = params.repo.clone();
                    for repo in &registered_repos_list {
                        if repo.name == params.repo {
                            resolved_repo = repo.git_url.clone();
                            tracing::debug!(original=%params.repo, resolved=%resolved_repo, "resolved repo name to git_url");
                            break;
                        }
                    }

                    // Check permissions for the resolved repo
                    if !lease_mgr.can_user_access_repo(&resolved_repo, user_id).await {
                        tracing::warn!(repo=%resolved_repo, "access denied for user");
                        return (
                            axum::http::StatusCode::FORBIDDEN,
                            Json(json!({"error": "access denied"})),
                        );
                    }

                    resolved_repos.push(resolved_repo);
                }
                                // Search across all resolved repos
                let mut all_results = Vec::new();
                for resolved_repo in resolved_repos {
                    // clone the index Arc out of the store and capture the repo root (map key)
                    let idx_pair = {
                        let map = store.read();
                        tracing::debug!(resolved_repo=%resolved_repo, available_keys=?map.keys().collect::<Vec<_>>(), "searching for repo in index store");

                        // try exact match by key
                        if let Some((k, v)) = map.get_key_value(&resolved_repo) {
                            tracing::debug!(resolved_repo=%resolved_repo, matched_key=%k, "found exact match in index store");
                            Some((k.clone(), v.clone()))
                        } else {
                            // fallback: try matching by basename or path suffix (so 'demo' matches '/tmp/demo')
                            let fallback_match = map.iter().find_map(|(k, v)| {
                                if k == &resolved_repo
                                    || k.ends_with(&format!("/{}", resolved_repo))
                                    || Path::new(k)
                                        .file_name()
                                        .and_then(|s| s.to_str())
                                        .map(|s| s == resolved_repo)
                                        .unwrap_or(false)
                                {
                                    tracing::debug!(resolved_repo=%resolved_repo, matched_key=%k, "found fallback match in index store");
                                    Some((k.clone(), v.clone()))
                                } else {
                                    None
                                }
                            });
                            if fallback_match.is_none() {
                                tracing::debug!(resolved_repo=%resolved_repo, "no match found in index store");
                            }
                            fallback_match
                        }
                    };

                    if let Some((_, idx)) = idx_pair {
                        // Parse query plan
                        let plan = match QueryPlan::parse(&params.q) {
                            Ok(p) => p,
                            Err(e) => {
                                tracing::warn!(repo=%resolved_repo, q=%params.q, error=%e, "query parse error");
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    Json(json!({"error": format!("parse error: {}", e)})),
                                )
                            }
                        };

                        let idx_clone = idx.clone();
                        let actual_repo_root = idx.repo_root(); // Use the actual repo root from index metadata
                        let resolved_repo_clone = resolved_repo.clone();
                        let query_clone = params.q.clone();
                        let context_lines_clone = params.context_lines;
                        // Find the branch for this repo
                        let branch = registered_repos_list.iter()
                            .find(|r| r.git_url == resolved_repo)
                            .and_then(|r| r.branch.clone())
                            .unwrap_or_else(|| "main".to_string());
                        let branch_clone = branch.clone();
                        let _fut = tokio::task::spawn_blocking(move || {
                            // Run the plan search and then try to enrich file results with
                            // symbol information when available. We prefer any symbols
                            // that were extracted at index time (meta.symbols). If a
                            // document has no symbols in the index, we fall back to
                            // extracting symbols from the file content on-the-fly.
                            let s = Searcher::new(&idx_clone);
                            let mut results = s.search_plan(&plan);

                            // Only attempt enrichment if we have a pattern to match
                            if let Some(pat) = &plan.pattern {
                                for r in results.iter_mut() {
                                    // skip if already has symbol information
                                    if r.symbol.is_some() {
                                        continue;
                                    }
                                    // guard: doc index within range
                                    if let Some(meta) = idx_clone.doc_meta(r.doc as usize) {
                                        // try to obtain file text: prefer in-memory doc_contents
                                        let mut text_opt: Option<String> = idx_clone.doc_content(r.doc as usize);
                                        if text_opt.is_none() {
                                            let full = idx_clone.repo_root().join(&meta.path);
                                            text_opt = std::fs::read_to_string(&full).ok();
                                        }
                                        if let Some(text) = text_opt {
                                            // find first match position (byte offset)
                                            let pos_opt: Option<u32> = if plan.regex {
                                                let mut pat_s = pat.clone();
                                                if !plan.case_sensitive && !pat_s.starts_with("(?i)") {
                                                    pat_s = format!("(?i){}", pat_s);
                                                }
                                                if let Ok(re) = Regex::new(&pat_s) {
                                                    re.find(&text).map(|m| m.start() as u32)
                                                } else {
                                                    None
                                                }
                                            } else if plan.case_sensitive {
                                                text.find(pat).map(|p| p as u32)
                                            } else {
                                                text.to_lowercase()
                                                    .find(&pat.to_lowercase())
                                                    .map(|p| p as u32)
                                            };

                                            if let Some(pos) = pos_opt {
                                                // prefer indexed symbols if present
                                                let mut chosen: Option<zoekt_rs::types::Symbol> = None;
                                                for sym in &meta.symbols {
                                                    if let Some(sstart) = sym.start {
                                                        if sstart <= pos {
                                                            match &chosen {
                                                                Some(c) => {
                                                                    if sstart > c.start.unwrap_or(0) {
                                                                        chosen = Some(sym.clone());
                                                                    }
                                                                }
                                                                None => chosen = Some(sym.clone()),
                                                            }
                                                        }
                                                    }
                                                }

                                                // fallback: match by line number if start offsets unavailable
                                                if chosen.is_none() {
                                                    let match_line = line_for_offset(&text, pos);
                                                    for sym in &meta.symbols {
                                                        if let Some(sline) = sym.line {
                                                            if (sline as usize) <= (match_line + 1) {
                                                                chosen = Some(sym.clone());
                                                            }
                                                        }
                                                    }
                                                }

                                                // if still none, attempt on-the-fly extraction and repeat
                                                if chosen.is_none() {
                                                    let ext = meta
                                                        .path
                                                        .extension()
                                                        .and_then(|s| s.to_str())
                                                        .unwrap_or("");
                                                    let extra = extract_symbols_local(&text, ext);
                                                    for sym in &extra {
                                                        if let Some(sstart) = sym.start {
                                                            if sstart <= pos {
                                                                match &chosen {
                                                                    Some(c) => {
                                                                        if sstart > c.start.unwrap_or(0) {
                                                                            chosen = Some(sym.clone());
                                                                        }
                                                                    }
                                                                    None => chosen = Some(sym.clone()),
                                                                }
                                                            }
                                                        }
                                                    }
                                                }

                                                if let Some(sym) = chosen {
                                                    r.symbol = Some(sym.name.clone());
                                                    r.symbol_loc = Some(sym.clone());
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            // Generate snippets here inside the blocking task
                            let context = context_lines_clone.unwrap_or(2);
                            let repo_results: Vec<_> = results
                                .into_iter()
                                .take(100)
                                .map(|mut r| {
                                    // try to build a snippet: prefer in-memory contents if present
                                    // Try to read file from disk under repo root. We don't access
                                    // in-memory doc_contents here to avoid touching private APIs.
                                    // We don't have the repo root here; attempt to read path as-is
                                    // join repo_root with relative path so we read the correct file
                                    let full_path = Path::new(&actual_repo_root).join(&r.path);
                                    let text_opt = idx_clone.doc_content(r.doc as usize)
                                        .or_else(|| std::fs::read_to_string(&full_path).ok());
                                    match text_opt {
                                        Some(text) => {
                                            let (snippet, snippet_symbol) = make_snippet(&text, &full_path.display().to_string(), context, &query_clone);
                                            if snippet.is_some() {
                                                tracing::info!(repo=%actual_repo_root.display(), path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, "snippet produced for result");
                                            } else {
                                                tracing::info!(repo=%actual_repo_root.display(), path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, "no snippet available for result");
                                            }
                                            // Fallback: if no symbol from enrichment, use from snippet
                                            if r.symbol_loc.is_none() && snippet_symbol.is_some() {
                                                r.symbol_loc = snippet_symbol.clone();
                                                r.symbol = snippet_symbol.map(|s| s.name);
                                            }
                                            json!({"doc": r.doc, "path": r.path, "symbol": r.symbol, "symbol_loc": r.symbol_loc, "snippet": snippet, "repo": resolved_repo_clone, "branch": branch_clone})
                                        }
                                        None => {
                                            tracing::info!(repo=%actual_repo_root.display(), path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, "failed to read file for snippet");
                                            json!({"doc": r.doc, "path": r.path, "symbol": r.symbol, "symbol_loc": r.symbol_loc, "snippet": serde_json::Value::Null, "repo": resolved_repo_clone, "branch": branch_clone})
                                        }
                                    }
                                })
                                .collect();
                            repo_results
                        });

                        // Wait for the search task to complete and collect results
                        match tokio::time::timeout(std::time::Duration::from_secs(30), _fut).await {
                            Ok(Ok(repo_results)) => {
                                all_results.extend(repo_results);
                            }
                            Ok(Err(e)) => {
                                tracing::error!(repo=%resolved_repo, error=%e, "search task failed");
                            }
                            Err(_) => {
                                tracing::warn!(repo=%resolved_repo, q=%params.q, "search timed out");
                            }
                        }
                    } else {
                        tracing::warn!(repo=%resolved_repo, "repo not found in index store");
                        // For wildcard searches, we don't fail if a repo isn't found, just skip it
                        if params.repo != "*" {
                            return (
                                axum::http::StatusCode::NOT_FOUND,
                                Json(json!({"error": "repo not found"})),
                            );
                        }
                    }
                }

                // Sort results by score (highest first) and limit total results
                all_results.sort_by(|a: &serde_json::Value, b: &serde_json::Value| {
                    let score_a = a.get("score").and_then(|s| s.as_f64()).unwrap_or(0.0);
                    let score_b = b.get("score").and_then(|s| s.as_f64()).unwrap_or(0.0);
                    score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
                });
                let final_results = all_results.into_iter().take(100).collect::<Vec<_>>();

                tracing::info!(repo=%params.repo, total_results=%final_results.len(), "search completed");
                (axum::http::StatusCode::OK, Json(json!({"results": final_results})))
            }),
        )
        .route(
            "/search",
            axum::routing::post(|Extension(store): Extension<IndexStore>, Extension(reg): Extension<RemoteList>, Extension(lease_mgr): Extension<LeaseManager>, headers: HeaderMap, ReqJson(body): ReqJson<serde_json::Value>| async move {
                // Accept either {"repo":..., "q":..., ...} or {"data": {"repo":..., "q":...}}
                let inner = if let Some(d) = body.get("data") { d.clone() } else { body.clone() };
                let params: SearchParams = match serde_json::from_value(inner) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(error=%e, "failed to parse JSON search parameters");
                        return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("parse error: {}", e)})));
                    }
                };
                tracing::info!(repo=%params.repo, q=%params.q, "received POST search request");

                // Extract user ID from headers
                let user_id_binding = extract_user_id(&headers);
                let user_id = user_id_binding.as_deref();

                // Resolve repo name to git_url if it's a registered name
                let mut resolved_repo = params.repo.clone();
                let registered_repos_list = reg.read().clone(); // Clone the list to avoid holding lock across await
                for repo in &registered_repos_list {
                    if repo.name == params.repo {
                        resolved_repo = repo.git_url.clone();
                        tracing::debug!(original=%params.repo, resolved=%resolved_repo, "resolved repo name to git_url");
                        break;
                    }
                }

                // Check permissions for the resolved repo
                if !lease_mgr.can_user_access_repo(&resolved_repo, user_id).await {
                    tracing::warn!(repo=%resolved_repo, "access denied for user");
                    return (
                        StatusCode::FORBIDDEN,
                        Json(json!({"error": "access denied"})),
                    );
                }

                // clone the index Arc and repo root key out of the store without holding
                // non-Send guards across await. We capture the store key (repo_root)
                // so we can join relative result paths against the repo root when
                // reading files for snippets (same behavior as GET /search).
                let idx_pair = {
                    let map = store.read();
                    tracing::debug!(resolved_repo=%resolved_repo, available_keys=?map.keys().collect::<Vec<_>>(), "POST search: searching for repo in index store");

                    // try exact match by key
                    if let Some((k, v)) = map.get_key_value(&resolved_repo) {
                        tracing::debug!(resolved_repo=%resolved_repo, matched_key=%k, "POST search: found exact match in index store");
                        Some((k.clone(), v.clone()))
                    } else {
                        // fallback: try matching by basename or path suffix
                        let fallback_match = map.iter().find_map(|(k, v)| {
                            if k == &resolved_repo
                                || k.ends_with(&format!("/{}", resolved_repo))
                                || Path::new(k)
                                    .file_name()
                                    .and_then(|s| s.to_str())
                                    .map(|s| s == resolved_repo)
                                    .unwrap_or(false)
                            {
                                tracing::debug!(resolved_repo=%resolved_repo, matched_key=%k, "POST search: found fallback match in index store");
                                Some((k.clone(), v.clone()))
                            } else {
                                None
                            }
                        });
                        if fallback_match.is_none() {
                            tracing::debug!(resolved_repo=%resolved_repo, "POST search: no match found in index store");
                        }
                        fallback_match
                    }
                };

                if let Some((_, idx)) = idx_pair {
                    let plan = match QueryPlan::parse(&params.q) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!(repo=%params.repo, q=%params.q, error=%e, "query parse error");
                            return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("parse error: {}", e)})));
                        }
                    };

                    let idx_clone = idx.clone();
                    let actual_repo_root = idx.repo_root(); // Use the actual repo root from index metadata
                    let resolved_repo_clone = resolved_repo.clone();
                    let query_clone = params.q.clone();
                    let context_lines_clone = params.context_lines;
                    // Find the branch for this repo
                    let branch = registered_repos_list.iter()
                        .find(|r| r.git_url == resolved_repo)
                        .and_then(|r| r.branch.clone())
                        .unwrap_or_else(|| "main".to_string());
                    let branch_clone = branch.clone();
                    let fut = tokio::task::spawn_blocking(move || {
                        // Same enrichment as the GET handler: attach symbol metadata
                        // to results when possible by scanning content and using
                        // indexed symbols or extracting them on-the-fly.
                        let s = Searcher::new(&idx_clone);
                        let mut results = s.search_plan(&plan);

                        if let Some(pat) = &plan.pattern {
                            for r in results.iter_mut() {
                                if r.symbol.is_some() {
                                    continue;
                                }
                                if let Some(meta) = idx_clone.doc_meta(r.doc as usize) {
                                    let mut text_opt: Option<String> = idx_clone.doc_content(r.doc as usize);
                                    if text_opt.is_none() {
                                        let full = idx_clone.repo_root().join(&meta.path);
                                        text_opt = std::fs::read_to_string(&full).ok();
                                    }
                                    if let Some(text) = text_opt {
                                        let pos_opt: Option<u32> = if plan.regex {
                                            let mut pat_s = pat.clone();
                                            if !plan.case_sensitive && !pat_s.starts_with("(?i)") {
                                                pat_s = format!("(?i){}", pat_s);
                                            }
                                            if let Ok(re) = Regex::new(&pat_s) {
                                                re.find(&text).map(|m| m.start() as u32)
                                            } else {
                                                None
                                            }
                                        } else if plan.case_sensitive {
                                            text.find(pat).map(|p| p as u32)
                                        } else {
                                            text.to_lowercase()
                                                .find(&pat.to_lowercase())
                                                .map(|p| p as u32)
                                        };

                                        if let Some(pos) = pos_opt {
                                            let mut chosen: Option<zoekt_rs::types::Symbol> = None;
                                            for sym in &meta.symbols {
                                                if let Some(sstart) = sym.start {
                                                    if sstart <= pos {
                                                        match &chosen {
                                                            Some(c) => {
                                                                if sstart > c.start.unwrap_or(0) {
                                                                    chosen = Some(sym.clone());
                                                                }
                                                            }
                                                            None => chosen = Some(sym.clone()),
                                                        }
                                                    }
                                                }
                                            }
                                            if chosen.is_none() {
                                                let match_line = line_for_offset(&text, pos);
                                                for sym in &meta.symbols {
                                                    if let Some(sline) = sym.line {
                                                        if (sline as usize) <= (match_line + 1) {
                                                            chosen = Some(sym.clone());
                                                        }
                                                    }
                                                }
                                            }
                                            if chosen.is_none() {
                                                let ext = meta
                                                    .path
                                                    .extension()
                                                    .and_then(|s| s.to_str())
                                                    .unwrap_or("");
                                                let extra = extract_symbols_local(&text, ext);
                                                for sym in &extra {
                                                    if let Some(sstart) = sym.start {
                                                        if sstart <= pos {
                                                            match &chosen {
                                                                Some(c) => {
                                                                    if sstart > c.start.unwrap_or(0) {
                                                                        chosen = Some(sym.clone());
                                                                    }
                                                                }
                                                                None => chosen = Some(sym.clone()),
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            if let Some(sym) = chosen {
                                                r.symbol = Some(sym.name.clone());
                                                r.symbol_loc = Some(sym.clone());
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Generate snippets here inside the blocking task
                        let cap = 100usize;
                        let context = context_lines_clone.unwrap_or(2);
                        let out: Vec<_> = results
                            .into_iter()
                            .take(cap)
                            .map(|mut r| {
                                // join the repo root key with the relative path to read the
                                // correct file on disk for snippet generation
                                let full_path = Path::new(&actual_repo_root).join(&r.path);
                                let text_opt = idx_clone.doc_content(r.doc as usize)
                                    .or_else(|| std::fs::read_to_string(&full_path).ok());
                                match text_opt {
                                    Some(text) => {
                                        let (snippet, snippet_symbol) = make_snippet(&text, &full_path.display().to_string(), context, &query_clone);
                                        if snippet.is_some() {
                                            tracing::info!(repo=%actual_repo_root.display(), path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, "snippet produced for result");
                                        } else {
                                            tracing::info!(repo=%actual_repo_root.display(), path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, "no snippet available for result");
                                        }
                                        // Fallback: if no symbol from enrichment, use from snippet
                                        if r.symbol_loc.is_none() && snippet_symbol.is_some() {
                                            r.symbol_loc = snippet_symbol.clone();
                                            r.symbol = snippet_symbol.map(|s| s.name);
                                        }
                                        json!({"doc": r.doc, "path": r.path, "symbol": r.symbol, "symbol_loc": r.symbol_loc, "snippet": snippet, "repo": resolved_repo_clone, "branch": branch_clone})
                                    }
                                    None => {
                                        tracing::info!(repo=%actual_repo_root.display(), path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, "failed to read file for snippet");
                                        json!({"doc": r.doc, "path": r.path, "symbol": r.symbol, "symbol_loc": r.symbol_loc, "snippet": serde_json::Value::Null, "repo": resolved_repo_clone, "branch": branch_clone})
                                    }
                                }
                            })
                            .collect();
                        out
                    });

                    match timeout(Duration::from_secs(5), fut).await {
                        Ok(join_res) => match join_res {
                            Ok(out) => {
                                tracing::info!(repo=%params.repo, results=%out.len(), "POST search completed");
                                (StatusCode::OK, Json(json!({"results": out})))
                            }
                            Err(e) => {
                                tracing::error!(repo=%params.repo, error=%e, "search task failed");
                                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("search task failed: {}", e)})))
                            }
                        },
                        Err(_) => {
                            tracing::warn!(repo=%params.repo, q=%params.q, "search timed out");
                            (StatusCode::REQUEST_TIMEOUT, Json(json!({"error": "search timed out"})))
                        }
                    }
                } else {
                    tracing::warn!(repo=%params.repo, "repo not found in index store");
                    (StatusCode::NOT_FOUND, Json(json!({"error": "repo not found"})))
                }
            }),
        )
        .route(
            "/status",
            axum::routing::get(|Extension(store): Extension<IndexStore>, Extension(reg): Extension<RemoteList>| async move {
                let repos = reg.read().clone();
                let mut out = Vec::new();
                for repo in repos {
                    // try to find a matching index in the store
                    let idx_pair = {
                        let map = store.read();
                        if let Some((k, v)) = map.get_key_value(&repo.git_url) {
                            Some((k.clone(), v.clone()))
                        } else {
                            map.iter().find_map(|(k, v)| {
                                if k == &repo.git_url || k.ends_with(&format!("/{}", repo.git_url)) || std::path::Path::new(k)
                                    .file_name()
                                    .and_then(|s| s.to_str())
                                    .map(|s| s == repo.git_url)
                                    .unwrap_or(false)
                                {
                                    Some((k.clone(), v.clone()))
                                } else {
                                    None
                                }
                            })
                        }
                    };

                    let (indexed, docs) = if let Some((_k, idx)) = idx_pair {
                        (true, Some(idx.doc_count()))
                    } else {
                        (false, None)
                    };

                    out.push(json!({
                        "name": repo.name,
                        "git_url": repo.git_url,
                        "branch": repo.branch,
                        "indexed": indexed,
                        "docs": docs,
                    }));
                }
                (axum::http::StatusCode::OK, Json(json!({"status": "ok", "repos": out})))
            }),
        )
    .layer(Extension(store))
    .layer(Extension(registered_repos))
    .layer(Extension(lease_mgr));

    // determine bind address from CLI flag, env var, or default
    let bind_addr = opts
        .listen
        .or_else(|| std::env::var("ZOEKTD_BIND_ADDR").ok())
        .unwrap_or_else(|| "127.0.0.1:3000".into());
    tracing::info!(binding = %bind_addr, "starting http search server");
    let addr: std::net::SocketAddr = bind_addr.parse().expect("invalid bind address");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let server = axum::serve(listener, app);

    // run server until interrupted; if it returns, propagate error
    tokio::select! {
        res = server => {
            res.map_err(|e| anyhow::anyhow!(e.to_string()))?;
            Ok(())
        }
        _ = node_handle => {
            // node loop finished; return OK for demo
            Ok(())
        }
    }
}
