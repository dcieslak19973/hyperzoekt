use anyhow::Result;
use axum::extract::Json as ReqJson;
use axum::extract::Query;
use axum::http::StatusCode;
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
        // Ensure directory exists; do not attempt to create files. Indexer will only read
        // existing readable files to avoid write attempts that can fail under restricted perms.
        if !repo_path.exists() {
            std::fs::create_dir_all(&repo_path).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        }

        tracing::info!(path=%repo_path.display(), "starting index build");
        // Build a real in-memory index for the repo dir using the IndexBuilder
        // Explicitly enable symbol extraction at index time.
        let builder = zoekt_rs::IndexBuilder::new(repo_path.clone()).enable_symbols(true);
        let idx = builder
            .build()
            .map_err(|e| anyhow::anyhow!(format!("index build error: {}", e)))?;

        // store the built index under the repo_path string so search handlers can find it
        let key = repo_path.to_string_lossy().into_owned();
        self.store.write().insert(key.clone(), idx.clone());
        tracing::info!(repo=%key, "index build complete and stored");
        Ok(idx)
    }
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
            cli_endpoint: opts.listen.as_ref().map(|addr| format!("http://{}", addr)), // Only override if explicitly provided
        },
    )?;

    let lease_mgr = LeaseManager::new().await;
    // shared store of indexes that the HTTP handler will read from
    let store: IndexStore = Arc::new(RwLock::new(HashMap::new()));
    let indexer = SimpleIndexer::new(store.clone());
    let node = Node::new(cfg, lease_mgr, indexer);

    // Collect remote URLs and optional names from CLI or env. Support multiple values.
    let mut urls: Vec<String> = opts.remote_url.clone();
    if urls.is_empty() {
        if let Ok(s) = std::env::var("REMOTE_REPO_URL") {
            urls.push(s);
        } else {
            urls.push("/tmp/demo".into());
        }
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
        let repo = RemoteRepo { name, git_url: url };
        node.add_remote(repo.clone());
    }

    // spawn the node loop in the background (indexing/lease loop)
    let node_handle = tokio::spawn(async move {
        // run for a long time for demo/dev; in production this would be indefinite
        let _ = node.run_for(Duration::from_secs(60 * 60)).await;
    });

    // start a small HTTP server to answer search requests against the in-memory indexes
    let app = axum::Router::new()
        .route(
            "/search",
            axum::routing::get(|Query(params): Query<SearchParams>, Extension(store): Extension<IndexStore>| async move {
                tracing::info!(repo=%params.repo, q=%params.q, "received search request");
                // clone the index Arc out of the store and capture the repo root (map key)
                let idx_pair = {
                    let map = store.read();
                    // try exact match by key
                    if let Some((k, v)) = map.get_key_value(&params.repo) {
                        Some((k.clone(), v.clone()))
                    } else {
                        // fallback: try matching by basename or path suffix (so 'demo' matches '/tmp/demo')
                        map.iter().find_map(|(k, v)| {
                            if k == &params.repo
                                || k.ends_with(&format!("/{}", params.repo))
                                || Path::new(k)
                                    .file_name()
                                    .and_then(|s| s.to_str())
                                    .map(|s| s == params.repo)
                                    .unwrap_or(false)
                            {
                                Some((k.clone(), v.clone()))
                            } else {
                                None
                            }
                        })
                    }
                };

                if let Some((repo_root, idx)) = idx_pair {
                    // Parse query plan
                    let plan = match QueryPlan::parse(&params.q) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!(repo=%params.repo, q=%params.q, error=%e, "query parse error");
                            return (
                                axum::http::StatusCode::BAD_REQUEST,
                                Json(json!({"error": format!("parse error: {}", e)})),
                            )
                        }
                    };

                    // Run the query in a blocking task with timeout to avoid blocking the reactor.
                    let idx_clone = idx.clone();
                    let fut = tokio::task::spawn_blocking(move || {
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
                        results
                    });

                    // 5s timeout for demo
                    match timeout(Duration::from_secs(5), fut).await {
                        Ok(join_res) => match join_res {
                            Ok(results) => {
                                // cap results to first 100
                                let cap = 100usize;
                                let context = params.context_lines.unwrap_or(2);
                                let out: Vec<_> = results
                                    .into_iter()
                                    .take(cap)
                                    .map(|mut r| {
                                        // try to build a snippet: prefer in-memory contents if present
                                        // Try to read file from disk under repo root. We don't access
                                        // in-memory doc_contents here to avoid touching private APIs.
                                        // We don't have the repo root here; attempt to read path as-is
                                        // join repo_root with relative path so we read the correct file
                                        let full_path = Path::new(&repo_root).join(&r.path);
                                        match std::fs::read_to_string(&full_path) {
                                            Ok(text) => {
                                                let (snippet, snippet_symbol) = make_snippet(&text, &full_path.display().to_string(), context, &params.q);
                                                if snippet.is_some() {
                                                    tracing::info!(repo=%repo_root, path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, "snippet produced for result");
                                                } else {
                                                    tracing::info!(repo=%repo_root, path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, "no snippet available for result");
                                                }
                                                // Fallback: if no symbol from enrichment, use from snippet
                                                if r.symbol_loc.is_none() && snippet_symbol.is_some() {
                                                    r.symbol_loc = snippet_symbol.clone();
                                                    r.symbol = snippet_symbol.map(|s| s.name);
                                                }
                                                json!({"doc": r.doc, "path": r.path, "symbol": r.symbol, "symbol_loc": r.symbol_loc, "snippet": snippet})
                                            }
                                            Err(e) => {
                                                tracing::info!(repo=%repo_root, path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, error=%e, "failed to read file for snippet");
                                                json!({"doc": r.doc, "path": r.path, "symbol": r.symbol, "symbol_loc": r.symbol_loc, "snippet": serde_json::Value::Null})
                                            }
                                        }
                                    })
                                    .collect();
                                tracing::info!(repo=%params.repo, results=%out.len(), "search completed");
                                (axum::http::StatusCode::OK, Json(json!({"results": out})))
                            }
                            Err(e) => (
                                {
                                    tracing::error!(repo=%params.repo, error=%e, "search task failed");
                                    axum::http::StatusCode::INTERNAL_SERVER_ERROR
                                },
                                Json(json!({"error": format!("search task failed: {}", e)})),
                            ),
                        },
                        Err(_) => (
                            {
                                tracing::warn!(repo=%params.repo, q=%params.q, "search timed out");
                                axum::http::StatusCode::REQUEST_TIMEOUT
                            },
                            Json(json!({"error": "search timed out"})),
                        ),
                    }
                } else {
                    tracing::warn!(repo=%params.repo, "repo not found in index store");
                    (
                        axum::http::StatusCode::NOT_FOUND,
                        Json(json!({"error": "repo not found"})),
                    )
                }
            }),
        )
        .route(
            "/search",
            axum::routing::post(|Extension(store): Extension<IndexStore>, ReqJson(body): ReqJson<serde_json::Value>| async move {
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

                // clone the index Arc and repo root key out of the store without holding
                // non-Send guards across await. We capture the store key (repo_root)
                // so we can join relative result paths against the repo root when
                // reading files for snippets (same behavior as GET /search).
                let idx_pair = {
                    let map = store.read();
                    // try exact match by key
                    if let Some((k, v)) = map.get_key_value(&params.repo) {
                        Some((k.clone(), v.clone()))
                    } else {
                        // fallback: try matching by basename or path suffix
                        map.iter().find_map(|(k, v)| {
                            if k == &params.repo
                                || k.ends_with(&format!("/{}", params.repo))
                                || Path::new(k)
                                    .file_name()
                                    .and_then(|s| s.to_str())
                                    .map(|s| s == params.repo)
                                    .unwrap_or(false)
                            {
                                Some((k.clone(), v.clone()))
                            } else {
                                None
                            }
                        })
                    }
                };

                if let Some((repo_root, idx)) = idx_pair {
                    let plan = match QueryPlan::parse(&params.q) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!(repo=%params.repo, q=%params.q, error=%e, "query parse error");
                            return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("parse error: {}", e)})));
                        }
                    };

                    let idx_clone = idx.clone();
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
                        results
                    });

                    match timeout(Duration::from_secs(5), fut).await {
                        Ok(join_res) => match join_res {
                            Ok(results) => {
                                let cap = 100usize;
                                let context = params.context_lines.unwrap_or(2);
                                let out: Vec<_> = results
                                    .into_iter()
                                    .take(cap)
                                    .map(|mut r| {
                                        // join the repo root key with the relative path to read the
                                        // correct file on disk for snippet generation
                                        let full_path = Path::new(&repo_root).join(&r.path);
                                        match std::fs::read_to_string(&full_path) {
                                            Ok(text) => {
                                                let (snippet, snippet_symbol) = make_snippet(&text, &full_path.display().to_string(), context, &params.q);
                                                if snippet.is_some() {
                                                    tracing::info!(repo=%repo_root, path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, "snippet produced for result");
                                                } else {
                                                    tracing::info!(repo=%repo_root, path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, "no snippet available for result");
                                                }
                                                // Fallback: if no symbol from enrichment, use from snippet
                                                if r.symbol_loc.is_none() && snippet_symbol.is_some() {
                                                    r.symbol_loc = snippet_symbol.clone();
                                                    r.symbol = snippet_symbol.map(|s| s.name);
                                                }
                                                json!({"doc": r.doc, "path": r.path, "symbol": r.symbol, "symbol_loc": r.symbol_loc, "snippet": snippet})
                                            }
                                            Err(e) => {
                                                tracing::info!(repo=%repo_root, path=%r.path, doc=r.doc, has_symbol=%r.symbol.is_some(), symbol_loc=?r.symbol_loc, error=%e, "failed to read file for snippet");
                                                json!({"doc": r.doc, "path": r.path, "symbol": r.symbol, "symbol_loc": r.symbol_loc, "snippet": serde_json::Value::Null})
                                            }
                                        }
                                    })
                                    .collect();
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
            axum::routing::get(|| async move {
                // the handler closes over nothing; we'll read the store via Extension below
                (axum::http::StatusCode::OK, Json(json!({"status": "ok"})))
            }),
        )
    .layer(Extension(store));

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
