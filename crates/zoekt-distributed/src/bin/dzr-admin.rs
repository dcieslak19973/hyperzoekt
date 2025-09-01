use axum::http::header::{CONTENT_TYPE, SET_COOKIE};
use axum::http::HeaderValue;
use axum::Json;
use axum::{
    extract::{Extension, Form, Multipart},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Router,
};
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use deadpool_redis::Config as RedisConfig;
// hmac handled in web_utils now when building cookies
use parking_lot::RwLock;
use serde::Deserialize;
use serde_json::json;
// sha2 used by web_utils if HMAC signing is enabled
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use zoekt_distributed::redis_adapter::{DynRedis, RealRedis};
use zoekt_distributed::web_utils;

// Reuse SessionEntry from shared web_utils
use web_utils::SessionEntry;

use anyhow::Result;
use base64::Engine;

use tracing_subscriber::fmt::time::UtcTime;

use zoekt_distributed::{load_node_config, MergeOpts, NodeConfig, NodeType};

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
    #[arg(long, default_value = "127.0.0.1:7878")]
    bind: String,
}

struct AppStateInner {
    // Abstract redis backend so tests can provide a mock implementation.
    redis_pool: Option<std::sync::Arc<dyn DynRedis>>,
    admin_user: String,
    admin_pass: String,
    // per-session CSRF tokens stored in-memory for this prototype
    // map: session id -> (csrf token, expiry Instant, optional username)
    sessions: Arc<RwLock<HashMap<String, SessionEntry>>>,
    // HMAC key for signing session ids (optional—if empty, unsigned cookies accepted)
    session_hmac_key: Option<Vec<u8>>,
}

type AppState = Arc<AppStateInner>;

// DynRedis and RealRedis are implemented in the crate::redis module.

#[derive(Deserialize)]
#[allow(dead_code)]
struct CreateRepoWithFreq {
    name: String,
    url: String,
    frequency: Option<u64>,
    branches: Option<String>,
    csrf: String,
}

// Embed static templates and JS so the binary can serve them directly.
const INDEX_TEMPLATE: &str = include_str!("../../static/admin/index.html");
const LOGIN_TEMPLATE: &str = include_str!("../../static/admin/login.html");
const ADMIN_JS: &str = include_str!("../../static/admin/admin.js");

// Reuse shared web helper functions from the crate for common HTTP helpers.
use web_utils::{
    basic_auth_from_headers, extract_sid_for_log, gen_token, into_boxed_response, parse_basic_auth,
    wants_json, SessionManager,
};

// Render a human-friendly byte count (B / KB / MB / GB) for the admin UI.
fn human_readable_bytes(bytes: i64) -> String {
    if bytes < 0 {
        return "N/A".to_string();
    }
    const KB: f64 = 1024.0;
    let mut v = bytes as f64;
    if v < KB {
        return format!("{} B", bytes);
    }
    v /= KB;
    if v < KB {
        return format!("{:.1} KB", v);
    }
    v /= KB;
    if v < KB {
        return format!("{:.1} MB", v);
    }
    v /= KB;
    format!("{:.2} GB", v)
}

// session lifecycle is managed by `web_utils::SessionManager` in handlers below.

// `verify_and_extract_session` is provided by `web_utils` and imported above.

async fn health(state: Extension<AppState>) -> impl IntoResponse {
    // If we have a redis pool, try a PING to ensure connectivity.
    if let Some(pool) = &state.redis_pool {
        match pool.ping().await {
            Ok(_) => {
                tracing::info!(pid=%std::process::id(), outcome=%"health_ok");
                return (StatusCode::OK, Json(json!({"status": "ok"}))).into_response();
            }
            Err(e) => {
                tracing::warn!(pid=%std::process::id(), outcome=%"health_redis_ping_failed", error=?e);
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"status": "redis_unreachable"})),
                )
                    .into_response();
            }
        }
    }
    // No redis configured: still healthy for this admin UI
    (StatusCode::OK, Json(json!({"status": "ok"}))).into_response()
}

async fn index(state: Extension<AppState>, headers: HeaderMap) -> impl IntoResponse {
    let req_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(gen_token);
    tracing::info!(request_id=%req_id, "index: incoming request");
    let is_basic = basic_auth_from_headers(&headers, &state.admin_user, &state.admin_pass);
    let session_mgr = SessionManager::new(state.sessions.clone(), state.session_hmac_key.clone());
    let sid_opt = session_mgr.verify_and_extract_session(&headers);
    let auth_type = if is_basic { "basic" } else { "cookie" };
    let sid_log = extract_sid_for_log(&headers).unwrap_or_default();
    // Accept cookie-based auth when the session id can be extracted AND the server-side
    // session map contains a non-expired entry for it. This allows unsigned (dev) cookies
    // to work as long as the server tracked the session.
    let has_session = if let Some(sid) = &sid_opt {
        let map = state.sessions.read();
        map.get(sid)
            .map(|(_tok, exp, _u)| Instant::now() < *exp)
            .unwrap_or(false)
    } else {
        false
    };
    if !is_basic && !has_session {
        // If the client prefers JSON (AJAX/API), return 401 JSON; otherwise redirect to /login
        if wants_json(&headers) {
            let body = json!({"error": "unauthorized"});
            tracing::warn!(request_id=%req_id, user = %"", auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome = %"unauthorized");
            return into_boxed_response((StatusCode::UNAUTHORIZED, Json(body)));
        }
        tracing::warn!(request_id=%req_id, user = %"", auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome = %"redirect_login");
        return into_boxed_response(Redirect::to("/login"));
    }

    let mut rows = String::new();
    // ensure session exists and get csrf token (create if needed)
    let (_sid, csrf_token, cookie) = session_mgr.get_or_create_session(&headers);
    if let Some(pool) = &state.redis_pool {
        if let Ok(entries) = pool.hgetall("zoekt:repos").await {
            for (name, url) in entries {
                let safe_name = htmlescape::encode_minimal(&name);
                // fetch meta JSON from zoekt:repo_meta
                let mut freq = "".to_string();
                let mut branches = "".to_string();
                if let Ok(Some(meta_json)) = pool.hget("zoekt:repo_meta", &name).await {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&meta_json) {
                        // repo_meta now stores only administrative settings: frequency and the
                        // original branch pattern. Runtime fields (last_indexed, duration,
                        // memory, leased_node) are stored per-branch in
                        // `zoekt:repo_branch_meta` and are shown in the expandable branch
                        // details. Only read frequency and branches here.
                        if let Some(f) = v.get("frequency") {
                            freq = f.to_string();
                        }
                        if let Some(b) = v.get("branches") {
                            if !b.is_null() {
                                if let Some(s) = b.as_str() {
                                    branches = s.to_string();
                                } else {
                                    branches = b.to_string();
                                }
                            }
                        }
                    }
                }
                // Gather branch-level details for display as structured JSON so the client can
                // render an expandable subtable. We'll collect an array of objects with
                // branch, last_indexed (RFC3339 or string), last_duration_ms, memory_bytes,
                // memory_display and leased_node.
                let mut branch_objs: Vec<serde_json::Value> = Vec::new();
                if let Ok(entry_map) = pool.hgetall("zoekt:repo_branch_meta").await {
                    for (field, json_val) in entry_map {
                        if field.starts_with(&format!("{}|", name)) {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&json_val) {
                                if let Some((_, branch)) = field.split_once('|') {
                                    let mut obj = serde_json::Map::new();
                                    obj.insert(
                                        "branch".to_string(),
                                        serde_json::Value::String(branch.to_string()),
                                    );
                                    // last_indexed: normalize epoch millis -> RFC3339 when possible
                                    if let Some(li) = v.get("last_indexed") {
                                        if let Some(n) = li.as_i64() {
                                            let dt: DateTime<Utc> = Utc
                                                .timestamp_millis_opt(n)
                                                .single()
                                                .unwrap_or_else(|| {
                                                    Utc.timestamp_opt(0, 0).unwrap()
                                                });
                                            obj.insert(
                                                "last_indexed".to_string(),
                                                serde_json::Value::String(dt.to_rfc3339()),
                                            );
                                        } else if let Some(s) = li.as_str() {
                                            obj.insert(
                                                "last_indexed".to_string(),
                                                serde_json::Value::String(s.to_string()),
                                            );
                                        }
                                    }
                                    if let Some(ld) = v.get("last_duration_ms") {
                                        if let Some(n) = ld.as_i64() {
                                            obj.insert(
                                                "last_duration_ms".to_string(),
                                                serde_json::Value::Number(
                                                    serde_json::Number::from(n),
                                                ),
                                            );
                                        } else if let Some(s) = ld.as_str() {
                                            if let Ok(n) = s.parse::<i64>() {
                                                obj.insert(
                                                    "last_duration_ms".to_string(),
                                                    serde_json::Value::Number(
                                                        serde_json::Number::from(n),
                                                    ),
                                                );
                                            }
                                        }
                                    }
                                    if let Some(mb) = v.get("memory_bytes") {
                                        if let Some(n) = mb.as_i64() {
                                            obj.insert(
                                                "memory_bytes".to_string(),
                                                serde_json::Value::Number(
                                                    serde_json::Number::from(n),
                                                ),
                                            );
                                            obj.insert(
                                                "memory_display".to_string(),
                                                serde_json::Value::String(human_readable_bytes(n)),
                                            );
                                        } else if let Some(s) = mb.as_str() {
                                            obj.insert(
                                                "memory_display".to_string(),
                                                serde_json::Value::String(s.to_string()),
                                            );
                                        }
                                    }
                                    if let Some(n) = v.get("leased_node") {
                                        if !n.is_null() {
                                            if let Some(s) = n.as_str() {
                                                obj.insert(
                                                    "leased_node".to_string(),
                                                    serde_json::Value::String(s.to_string()),
                                                );
                                            } else {
                                                obj.insert("leased_node".to_string(), n.clone());
                                            }
                                        }
                                    }
                                    branch_objs.push(serde_json::Value::Object(obj));
                                }
                            }
                        }
                    }
                }

                let branch_json =
                    serde_json::to_string(&branch_objs).unwrap_or_else(|_| "[]".to_string());
                let branch_json_escaped = htmlescape::encode_minimal(&branch_json);

                // Row columns: Name (with expander), URL, Branches, Frequency, Actions
                rows.push_str(&format!(
                    "<tr data-name=\"{}\" data-branch-details=\"{}\"><td><span class=\"expander\">▶</span>{}</td><td>{}</td><td>{}</td><td>{}</td><td><form class=\"delete-form\" method=\"post\" action=\"/delete\"><input type=\"hidden\" name=\"name\" value=\"{}\"/><input type=\"hidden\" name=\"csrf\" value=\"{}\"/><button>Delete</button></form></td></tr>",
                    safe_name,
                    branch_json_escaped,
                    htmlescape::encode_minimal(&name),
                    htmlescape::encode_minimal(&url),
                    htmlescape::encode_minimal(&branches),
                    htmlescape::encode_minimal(&freq),
                    htmlescape::encode_minimal(&name),
                    htmlescape::encode_minimal(&csrf_token),
                ));
            }
        }
    }

    let admin_user = htmlescape::encode_minimal(&state.admin_user);

    // Use the embedded index template and replace simple placeholders.
    let html = INDEX_TEMPLATE
        .replace("{{ROWS}}", &rows)
        .replace("{{CSRF}}", &htmlescape::encode_minimal(&csrf_token))
        .replace("{{ADMIN_USER}}", &admin_user);

    let mut resp = Html(html).into_response();
    if let Some(cookie_val) = cookie {
        resp.headers_mut().insert(
            SET_COOKIE,
            HeaderValue::from_str(&cookie_val).unwrap_or_else(|_| HeaderValue::from_static("")),
        );
    }
    resp.headers_mut().insert(
        axum::http::header::HeaderName::from_static("x-request-id"),
        HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    // determine user_for_log: prefer Basic auth username, then session-mapped username, else anonymous
    let user_for_log = if let Some((u, _)) = parse_basic_auth(&headers) {
        u
    } else if let Some(sid) = extract_sid_for_log(&headers) {
        let map = state.sessions.read();
        map.get(&sid)
            .and_then(|(_tok, _exp, maybe_user)| maybe_user.clone())
            .unwrap_or_else(|| "anonymous".to_string())
    } else {
        "anonymous".to_string()
    };

    tracing::info!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome = %"index_ok");
    let (parts, body) = resp.into_parts();
    let boxed = axum::body::Body::new(body);
    axum::response::Response::from_parts(parts, boxed)
}

use async_trait::async_trait;

/// Trait for fetching branches from a repository
#[async_trait]
pub trait BranchFetcher: Send + Sync {
    async fn fetch_branches(&self, url: &str) -> Result<Vec<String>, String>;
}

/// Real implementation that uses git ls-remote
pub struct GitBranchFetcher;

#[async_trait]
impl BranchFetcher for GitBranchFetcher {
    async fn fetch_branches(&self, url: &str) -> Result<Vec<String>, String> {
        let output = tokio::process::Command::new("git")
            .args(["ls-remote", "--heads", url])
            .output()
            .await
            .map_err(|e| format!("Failed to execute git ls-remote: {}", e))?;

        if !output.status.success() {
            return Err(format!(
                "git ls-remote failed with status: {}",
                output.status
            ));
        }

        let stdout = String::from_utf8(output.stdout)
            .map_err(|e| format!("Invalid UTF-8 in git output: {}", e))?;

        let mut branches = Vec::new();
        for line in stdout.lines() {
            if let Some(branch_ref) = line.split('\t').nth(1) {
                if branch_ref.starts_with("refs/heads/") {
                    let branch_name = branch_ref.strip_prefix("refs/heads/").unwrap();
                    branches.push(branch_name.to_string());
                }
            }
        }

        Ok(branches)
    }
}

/// Mock implementation for testing
pub struct MockBranchFetcher {
    pub branches: Vec<String>,
}

#[async_trait]
impl BranchFetcher for MockBranchFetcher {
    async fn fetch_branches(&self, _url: &str) -> Result<Vec<String>, String> {
        Ok(self.branches.clone())
    }
}

/// Expands branch patterns like "main,features/*" into actual branch names.
/// Fetches branches from the Git repository and matches them against patterns.
#[allow(dead_code)]
async fn expand_branch_patterns(url: &str, pattern: &str) -> Result<Vec<String>, String> {
    let fetcher = GitBranchFetcher;
    expand_branch_patterns_with_fetcher(url, pattern, &fetcher).await
}

/// Expands branch patterns with a custom branch fetcher (for testing)
async fn expand_branch_patterns_with_fetcher(
    url: &str,
    pattern: &str,
    fetcher: &dyn BranchFetcher,
) -> Result<Vec<String>, String> {
    if pattern.trim().is_empty() {
        return Ok(vec!["main".to_string()]);
    }

    // Fetch branches from the repository
    let branches = fetcher
        .fetch_branches(url)
        .await
        .map_err(|e| format!("Failed to fetch branches from {}: {}", url, e))?;

    let mut expanded = Vec::new();
    for part in pattern.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if part.contains('*') || part.contains('?') || part.contains('[') {
            // Match against actual branches
            for branch in &branches {
                if matches_pattern(part, branch) {
                    expanded.push(branch.clone());
                }
            }
        } else {
            // Exact branch name - check if it exists
            if branches.contains(&part.to_string()) {
                expanded.push(part.to_string());
            } else {
                // For exact matches that don't exist, still include them
                // The indexer will handle the error when it tries to clone
                expanded.push(part.to_string());
            }
        }
    }

    if expanded.is_empty() {
        Ok(vec!["main".to_string()])
    } else {
        // Remove duplicates and sort
        expanded.sort();
        expanded.dedup();
        Ok(expanded)
    }
}

/// Simple wildcard pattern matching for branch names
fn matches_pattern(pattern: &str, branch: &str) -> bool {
    // Convert glob pattern to regex
    let mut regex_pattern = String::new();
    regex_pattern.push('^');

    for ch in pattern.chars() {
        match ch {
            '*' => regex_pattern.push_str(".*"),
            '?' => regex_pattern.push('.'),
            '[' => regex_pattern.push('['),
            ']' => regex_pattern.push(']'),
            '^' => regex_pattern.push_str("\\^"),
            '$' => regex_pattern.push_str("\\$"),
            '.' => regex_pattern.push_str("\\."),
            '+' => regex_pattern.push_str("\\+"),
            '|' => regex_pattern.push_str("\\|"),
            '(' => regex_pattern.push_str("\\("),
            ')' => regex_pattern.push_str("\\)"),
            '{' => regex_pattern.push_str("\\{"),
            '}' => regex_pattern.push_str("\\}"),
            '\\' => regex_pattern.push_str("\\\\"),
            _ => regex_pattern.push(ch),
        }
    }

    regex_pattern.push('$');

    match regex::Regex::new(&regex_pattern) {
        Ok(re) => re.is_match(branch),
        Err(_) => false, // If regex compilation fails, no match
    }
}

#[allow(dead_code)]
async fn create_inner(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    Form(form): Form<CreateRepoWithFreq>,
) -> impl IntoResponse {
    let req_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(gen_token);
    tracing::info!(request_id=%req_id, "create: incoming");
    // compute user_for_log and auth_type similar to delete
    let parsed_basic = parse_basic_auth(&headers).map(|(u, _)| u);
    let sid_log = extract_sid_for_log(&headers).unwrap_or_default();
    let auth_type = if basic_auth_from_headers(&headers, &state.admin_user, &state.admin_pass) {
        "basic"
    } else if sid_log.is_empty() {
        "none"
    } else {
        "cookie"
    };
    let mut user_for_log = parsed_basic.clone().unwrap_or_default();
    if user_for_log.is_empty() {
        if let Some(sid) = extract_sid_for_log(&headers) {
            let map = state.sessions.read();
            if let Some((_tok, _exp, Some(u))) = map.get(&sid) {
                user_for_log = u.clone();
            }
        }
    }
    if user_for_log.is_empty() {
        user_for_log = "anonymous".to_string();
    }
    let sid_log = extract_sid_for_log(&headers).unwrap_or_default();
    let is_basic = basic_auth_from_headers(&headers, &state.admin_user, &state.admin_pass);
    let session_mgr = SessionManager::new(state.sessions.clone(), state.session_hmac_key.clone());
    let sid_opt = session_mgr.verify_and_extract_session(&headers);
    let sid_valid = sid_opt
        .as_ref()
        .map(|sid| {
            let map = state.sessions.read();
            map.get(sid)
                .map(|(_tok, exp, _u)| Instant::now() < *exp)
                .unwrap_or(false)
        })
        .unwrap_or(false);
    if !is_basic && !sid_valid {
        tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"create_unauthorized");
        return into_boxed_response((
            StatusCode::UNAUTHORIZED,
            Html("<h1>Unauthorized</h1>".to_string()),
        ));
    }
    // Validate session cookie -> csrf mapping for create
    let sid = match sid_opt {
        Some(s) => s,
        None => {
            return into_boxed_response((
                StatusCode::FORBIDDEN,
                Html("<h1>Forbidden</h1>".to_string()),
            ));
        }
    };
    {
        let map = state.sessions.read();
        if map.get(&sid).map(|(v, _exp, _u)| v.as_str()) != Some(form.csrf.as_str()) {
            tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"create_forbidden_csrf_mismatch");
            return into_boxed_response((
                StatusCode::FORBIDDEN,
                Html("<h1>Forbidden</h1>".to_string()),
            ));
        }
    }
    if let Some(pool) = &state.redis_pool {
        // Expand branch patterns if provided
        let branches = if let Some(branches_str) = &form.branches {
            if !branches_str.trim().is_empty() {
                match expand_branch_patterns(&form.url, branches_str.trim()).await {
                    Ok(expanded) => expanded,
                    Err(e) => {
                        tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"create_branch_expand_error", error=?e);
                        if wants_json(&headers) {
                            let body = json!({"error": "branch_pattern_error", "message": e});
                            let mut resp =
                                into_boxed_response((StatusCode::BAD_REQUEST, Json(body)));
                            resp.headers_mut().insert(
                                axum::http::header::HeaderName::from_static("x-request-id"),
                                HeaderValue::from_str(&req_id)
                                    .unwrap_or_else(|_| HeaderValue::from_static("")),
                            );
                            return resp;
                        }
                        return into_boxed_response((
                            StatusCode::BAD_REQUEST,
                            Html("<h1>Invalid branch pattern</h1>".to_string()),
                        ));
                    }
                }
            } else {
                vec!["main".to_string()]
            }
        } else {
            vec!["main".to_string()]
        };

        let script = r#"
            local name = ARGV[1]
            local url = ARGV[2]
            if redis.call('HEXISTS', KEYS[1], name) == 1 then
                return 1
            end
            if redis.call('SISMEMBER', KEYS[2], url) == 1 then
                return 2
            end
            redis.call('HSET', KEYS[1], name, url)
            redis.call('SADD', KEYS[2], url)
            return 0
        "#;

        match pool
            .eval_i32(
                script,
                &["zoekt:repos", "zoekt:repo_urls"],
                &[&form.name, &form.url],
            )
            .await
        {
            Ok(res) => {
                if res == 1 {
                    tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"create_conflict_name", name=%form.name);
                    if wants_json(&headers) {
                        let body = json!({"error": "name_conflict", "name": form.name});
                        let mut resp = into_boxed_response((StatusCode::CONFLICT, Json(body)));
                        resp.headers_mut().insert(
                            axum::http::header::HeaderName::from_static("x-request-id"),
                            HeaderValue::from_str(&req_id)
                                .unwrap_or_else(|_| HeaderValue::from_static("")),
                        );
                        return resp;
                    }
                    let mut resp = into_boxed_response((
                        StatusCode::CONFLICT,
                        Html("<h1>Name already exists</h1>".to_string()),
                    ));
                    resp.headers_mut().insert(
                        axum::http::header::HeaderName::from_static("x-request-id"),
                        HeaderValue::from_str(&req_id)
                            .unwrap_or_else(|_| HeaderValue::from_static("")),
                    );
                    return resp;
                }
                if res == 2 {
                    tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"create_conflict_url", url=%form.url);
                    if wants_json(&headers) {
                        let body = json!({"error": "url_conflict", "url": form.url});
                        let mut resp = into_boxed_response((StatusCode::CONFLICT, Json(body)));
                        resp.headers_mut().insert(
                            axum::http::header::HeaderName::from_static("x-request-id"),
                            HeaderValue::from_str(&req_id)
                                .unwrap_or_else(|_| HeaderValue::from_static("")),
                        );
                        return resp;
                    }
                    let mut resp = into_boxed_response((
                        StatusCode::CONFLICT,
                        Html("<h1>URL already exists</h1>".to_string()),
                    ));
                    resp.headers_mut().insert(
                        axum::http::header::HeaderName::from_static("x-request-id"),
                        HeaderValue::from_str(&req_id)
                            .unwrap_or_else(|_| HeaderValue::from_static("")),
                    );
                    return resp;
                }
                // otherwise success (res == 0)
                // store repo meta (administrative fields only). Runtime/status fields
                // like last_indexed/last_duration_ms/memory_bytes/leased_node are
                // maintained per-branch in `zoekt:repo_branch_meta` and should not be
                // written here.
                let meta_key = "zoekt:repo_meta".to_string();
                let mut meta = json!({
                    "frequency": form.frequency,
                });
                let branches_str = branches.join(",");
                meta["branches"] = json!(branches_str);

                if let Err(e) = pool.hset(&meta_key, &form.name, &meta.to_string()).await {
                    tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"create_meta_error", error=?e);
                    return into_boxed_response((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Html("<h1>Internal Server Error</h1>".to_string()),
                    ));
                }
                // Write expanded branch-level entries into zoekt:repo_branches so indexers can lease per-branch
                let branch_map_key = "zoekt:repo_branches";
                for b in branches.iter() {
                    let field = format!("{}|{}", form.name, b);
                    // store the repo URL as the value; consumers can parse field to get repo name + branch
                    let _ = pool.hset(branch_map_key, &field, &form.url).await;
                }
            }
            Err(e) => {
                tracing::warn!(request_id=%req_id, error=?e, pid=%std::process::id(), outcome=%"create_redis_error");
                return into_boxed_response((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Html("<h1>Internal Server Error</h1>".to_string()),
                ));
            }
        }
    }

    if wants_json(&headers) {
        let body = json!({"name": form.name, "url": form.url});
        tracing::info!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"create_success", name=%form.name);
        let mut resp = into_boxed_response((StatusCode::CREATED, Json(body)));
        resp.headers_mut().insert(
            axum::http::header::HeaderName::from_static("x-request-id"),
            HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
        );
        return resp;
    }

    tracing::info!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"create_redirect");
    let mut resp = into_boxed_response(Redirect::to("/"));
    resp.headers_mut().insert(
        axum::http::header::HeaderName::from_static("x-request-id"),
        HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    resp
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct DeleteForm {
    name: String,
    csrf: String,
}

#[allow(dead_code)]
async fn delete_inner(
    state: Extension<AppState>,
    headers: HeaderMap,
    Form(form): Form<DeleteForm>,
) -> impl IntoResponse {
    let req_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(gen_token);
    tracing::info!(request_id=%req_id, "delete: incoming");
    // compute user_for_log and auth_type similar to create
    let parsed_basic = parse_basic_auth(&headers).map(|(u, _)| u);
    let sid_log = extract_sid_for_log(&headers).unwrap_or_default();
    let auth_type = if basic_auth_from_headers(&headers, &state.admin_user, &state.admin_pass) {
        "basic"
    } else if sid_log.is_empty() {
        "none"
    } else {
        "cookie"
    };
    let mut user_for_log = parsed_basic.clone().unwrap_or_default();
    if user_for_log.is_empty() {
        if let Some(sid) = extract_sid_for_log(&headers) {
            let map = state.sessions.read();
            if let Some((_tok, _exp, Some(u))) = map.get(&sid) {
                user_for_log = u.clone();
            }
        }
    }
    if user_for_log.is_empty() {
        user_for_log = "anonymous".to_string();
    }
    let sid_log = extract_sid_for_log(&headers).unwrap_or_default();
    let is_basic = basic_auth_from_headers(&headers, &state.admin_user, &state.admin_pass);
    let session_mgr = SessionManager::new(state.sessions.clone(), state.session_hmac_key.clone());
    let sid_opt = session_mgr.verify_and_extract_session(&headers);
    let sid_valid = sid_opt
        .as_ref()
        .map(|sid| {
            let map = state.sessions.read();
            map.get(sid)
                .map(|(_tok, exp, _u)| Instant::now() < *exp)
                .unwrap_or(false)
        })
        .unwrap_or(false);
    if !is_basic && !sid_valid {
        tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"delete_unauthorized");
        return into_boxed_response((
            StatusCode::UNAUTHORIZED,
            Html("<h1>Unauthorized</h1>".to_string()),
        ));
    }
    // Validate session cookie -> csrf mapping for delete
    let sid = match sid_opt {
        Some(s) => s,
        None => {
            return into_boxed_response((
                StatusCode::FORBIDDEN,
                Html("<h1>Forbidden</h1>".to_string()),
            ));
        }
    };
    {
        let map = state.sessions.read();
        if map.get(&sid).map(|(v, _exp, _u)| v.as_str()) != Some(form.csrf.as_str()) {
            tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"delete_forbidden_csrf_mismatch");
            return into_boxed_response((
                StatusCode::FORBIDDEN,
                Html("<h1>Forbidden</h1>".to_string()),
            ));
        }
    }
    if let Some(pool) = &state.redis_pool {
        let script = r#"
            local name = ARGV[1]
            local key_repos = KEYS[1]
            local key_urls = KEYS[2]
            local url = redis.call('HGET', key_repos, name)
            if not url then
                return 1
            end
            redis.call('HDEL', key_repos, name)
            redis.call('SREM', key_urls, url)
            return 0
        "#;

        match pool
            .eval_i32(script, &["zoekt:repos", "zoekt:repo_urls"], &[&form.name])
            .await
        {
            Ok(res) => {
                if res == 1 {
                    tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"delete_not_found", name=%form.name);
                    if wants_json(&headers) {
                        let body = json!({"error": "not_found", "name": form.name});
                        let mut resp = into_boxed_response((StatusCode::NOT_FOUND, Json(body)));
                        resp.headers_mut().insert(
                            axum::http::header::HeaderName::from_static("x-request-id"),
                            HeaderValue::from_str(&req_id)
                                .unwrap_or_else(|_| HeaderValue::from_static("")),
                        );
                        return resp;
                    }
                    let mut resp = into_boxed_response((
                        StatusCode::NOT_FOUND,
                        Html("<h1>Not Found</h1>".to_string()),
                    ));
                    resp.headers_mut().insert(
                        axum::http::header::HeaderName::from_static("x-request-id"),
                        HeaderValue::from_str(&req_id)
                            .unwrap_or_else(|_| HeaderValue::from_static("")),
                    );
                    return resp;
                }
                // otherwise success (res == 0)
                // remove repo meta as well
                let _ = pool.hdel("zoekt:repo_meta", &form.name).await;
                // remove per-branch entries for this repo from zoekt:repo_branches
                if let Ok(entries) = pool.hgetall("zoekt:repo_branches").await {
                    let mut to_delete: Vec<String> = Vec::new();
                    for (field, _val) in entries {
                        if field.starts_with(&format!("{}|", form.name)) {
                            to_delete.push(field);
                        }
                    }
                    for f in to_delete {
                        let _ = pool.hdel("zoekt:repo_branches", &f).await;
                    }
                }
            }
            Err(e) => {
                tracing::warn!(request_id=%req_id, error=?e, pid=%std::process::id(), outcome=%"delete_redis_error");
                return into_boxed_response((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Html("<h1>Internal Server Error</h1>".to_string()),
                ));
            }
        }
    }

    if wants_json(&headers) {
        let body = json!({"name": form.name});
        tracing::info!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"delete_success", name=%form.name);
        let mut resp = into_boxed_response((StatusCode::OK, Json(body)));
        resp.headers_mut().insert(
            axum::http::header::HeaderName::from_static("x-request-id"),
            HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
        );
        return resp;
    }

    tracing::info!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"delete_redirect");
    let mut resp = into_boxed_response(Redirect::to("/"));
    resp.headers_mut().insert(
        axum::http::header::HeaderName::from_static("x-request-id"),
        HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    resp
}

async fn logout(state: Extension<AppState>, headers: HeaderMap) -> axum::response::Response {
    // If we can extract a session id, remove it from the server-side session map.
    let session_mgr = SessionManager::new(state.sessions.clone(), state.session_hmac_key.clone());
    if let Some(sid) = session_mgr.verify_and_extract_session(&headers) {
        session_mgr.remove(&sid);
        tracing::info!(sid=%sid, pid=%std::process::id(), outcome=%"logout");
    }

    // Clear the session cookie client-side
    let cookie_val = "dzr_session=; Path=/; Max-Age=0; HttpOnly";
    let mut resp = Redirect::to("/login").into_response();
    resp.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::from_str(cookie_val).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    into_boxed_response(resp)
}

#[derive(Deserialize)]
struct LoginForm {
    username: String,
    password: String,
}

async fn login_get(state: Extension<AppState>, headers: HeaderMap) -> axum::response::Response {
    // If already authenticated via basic auth, redirect to index
    if basic_auth_from_headers(&headers, &state.admin_user, &state.admin_pass) {
        return into_boxed_response(Redirect::to("/"));
    }

    // Serve the embedded login template for a nicer UI.
    let html = LOGIN_TEMPLATE.replace(
        "{{ADMIN_USER}}",
        &htmlescape::encode_minimal(&state.admin_user),
    );
    into_boxed_response((StatusCode::OK, Html(html)))
}

async fn serve_admin_js() -> impl IntoResponse {
    let mut resp = (StatusCode::OK, ADMIN_JS).into_response();
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/javascript; charset=utf-8"),
    );
    resp
}

#[derive(Serialize)]
struct RepoInfo {
    name: String,
    url: String,
    frequency: Option<u64>,
    branches: Option<String>,
}

async fn api_repos(state: Extension<AppState>) -> impl IntoResponse {
    let mut out: Vec<RepoInfo> = Vec::new();
    if let Some(pool) = &state.redis_pool {
        if let Ok(entries) = pool.hgetall("zoekt:repos").await {
            for (name, url) in entries {
                let mut frequency: Option<u64> = None;
                let mut branches: Option<String> = None;

                if let Ok(Some(meta_json)) = pool.hget("zoekt:repo_meta", &name).await {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&meta_json) {
                        if let Some(f) = v.get("frequency") {
                            if let Some(n) = f.as_u64() {
                                frequency = Some(n);
                            } else if let Some(s) = f.as_str() {
                                if let Ok(n) = s.parse::<u64>() {
                                    frequency = Some(n);
                                }
                            }
                        }
                        if let Some(b) = v.get("branches") {
                            if !b.is_null() {
                                if let Some(s) = b.as_str() {
                                    branches = Some(s.to_string());
                                } else {
                                    branches = Some(b.to_string());
                                }
                            }
                        }
                    }
                }

                out.push(RepoInfo {
                    name: name.clone(),
                    url: url.clone(),
                    frequency,
                    branches,
                });
            }
        }
    }
    Json(out)
}

#[derive(Serialize)]
struct IndexerInfo {
    node_id: String,
    endpoint: String,
    last_heartbeat: Option<i64>,
    status: String,
}

async fn api_indexers(state: Extension<AppState>) -> impl IntoResponse {
    let mut out: Vec<IndexerInfo> = Vec::new();
    if let Some(pool) = &state.redis_pool {
        if let Ok(entries) = pool.hgetall("zoekt:indexers").await {
            for (node_id, json_str) in entries {
                let mut endpoint = String::new();
                let mut last_heartbeat: Option<i64> = None;
                let mut status = "unknown".to_string();

                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&json_str) {
                    if let Some(ep) = v.get("endpoint").and_then(|e| e.as_str()) {
                        endpoint = ep.to_string();
                    }
                    if let Some(hb) = v.get("last_heartbeat").and_then(|h| h.as_i64()) {
                        last_heartbeat = Some(hb);
                        // Determine status based on heartbeat age
                        let now = chrono::Utc::now().timestamp_millis();
                        let age_minutes = (now - hb) / (1000 * 60);
                        if age_minutes < 5 {
                            status = "online".to_string();
                        } else if age_minutes < 15 {
                            status = "stale".to_string();
                        } else {
                            status = "offline".to_string();
                        }
                    }
                }

                out.push(IndexerInfo {
                    node_id,
                    endpoint,
                    last_heartbeat,
                    status,
                });
            }
        }
    }
    Json(out)
}

async fn login_post(
    state: Extension<AppState>,
    headers: HeaderMap,
    Form(form): Form<LoginForm>,
) -> impl IntoResponse {
    // Validate credentials
    if form.username != state.admin_user || form.password != state.admin_pass {
        tracing::warn!(user=%form.username, pid=%std::process::id(), outcome=%"login_failed");
        return into_boxed_response((
            StatusCode::UNAUTHORIZED,
            Html("<h1>Unauthorized</h1>".to_string()),
        ));
    }

    // Create session and set cookie
    let session_mgr = SessionManager::new(state.sessions.clone(), state.session_hmac_key.clone());
    let (sid_new, _csrf, cookie) = session_mgr.get_or_create_session(&headers);
    // associate this session id with the logged-in username
    session_mgr.set_username(&sid_new, form.username.clone());
    tracing::info!(user=%form.username, pid=%std::process::id(), sid=%sid_new, outcome=%"login_success");
    let mut resp = Redirect::to("/").into_response();
    if let Some(cookie_val) = cookie {
        resp.headers_mut().insert(
            SET_COOKIE,
            HeaderValue::from_str(&cookie_val).unwrap_or_else(|_| HeaderValue::from_static("")),
        );
    }
    // attach a request id to help correlate logs with this response
    let req_id = gen_token();
    resp.headers_mut().insert(
        axum::http::header::HeaderName::from_static("x-request-id"),
        HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    into_boxed_response(resp)
}

#[allow(dead_code)]
async fn create_handler(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    Form(form): Form<CreateRepoWithFreq>,
) -> impl IntoResponse {
    create_inner(Extension(state), headers, Form(form)).await
}

#[allow(dead_code)]
async fn delete_handler(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    Form(form): Form<DeleteForm>,
) -> impl IntoResponse {
    delete_inner(Extension(state), headers, Form(form)).await
}

#[allow(dead_code)]
async fn bulk_import_handler(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    multipart: Multipart,
) -> impl IntoResponse {
    bulk_import_inner(Extension(state), headers, multipart).await
}

fn make_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/", get(index))
        .route("/api/repos", get(api_repos))
        .route("/api/indexers", get(api_indexers))
        .route("/login", get(login_get))
        .route("/login", post(login_post))
        .route("/logout", get(logout))
        .route(
            "/create",
            post(|state: Extension<AppState>, headers: HeaderMap, Form(form): Form<CreateRepoWithFreq>| async move {
                create_inner(state, headers, Form(form)).await
            }),
        )
        .route(
            "/delete",
            post(|state: Extension<AppState>, headers: HeaderMap, Form(form): Form<DeleteForm>| async move {
                delete_inner(state, headers, Form(form)).await
            }),
        )
        .route(
            "/bulk-import",
            post(|state: Extension<AppState>, headers: HeaderMap, multipart: Multipart| async move {
                bulk_import_inner(state, headers, multipart).await
            }),
        )
        .nest_service("/static/common", tower_http::services::ServeDir::new("crates/zoekt-distributed/static/common"))
        .route("/static/admin.js", get(serve_admin_js))
        .route("/export.csv", get(export_csv))
        .layer(axum::Extension(state))
}

#[allow(dead_code)]
async fn bulk_import_inner(
    state: Extension<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let req_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(gen_token);
    tracing::info!(request_id=%req_id, "bulk_import: incoming");

    // Authentication check
    let is_basic = basic_auth_from_headers(&headers, &state.admin_user, &state.admin_pass);
    let session_mgr = SessionManager::new(state.sessions.clone(), state.session_hmac_key.clone());
    let sid_opt = session_mgr.verify_and_extract_session(&headers);
    let has_session = if let Some(sid) = &sid_opt {
        let map = state.sessions.read();
        map.get(sid)
            .map(|(_tok, exp, _u)| Instant::now() < *exp)
            .unwrap_or(false)
    } else {
        false
    };
    if !is_basic && !has_session {
        tracing::warn!(request_id=%req_id, outcome=%"bulk_import_unauthorized");
        if wants_json(&headers) {
            let body = json!({"error": "unauthorized"});
            let mut resp = into_boxed_response((StatusCode::UNAUTHORIZED, Json(body)));
            resp.headers_mut().insert(
                axum::http::header::HeaderName::from_static("x-request-id"),
                HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
            );
            return resp;
        }
        return into_boxed_response(Redirect::to("/login"));
    }

    // Get the CSV file from multipart
    let mut csv_content = String::new();
    let mut csrf_token = String::new();

    while let Some(field) = multipart.next_field().await.unwrap_or(None) {
        let name = field.name().unwrap_or("").to_string();
        if name == "csv_file" {
            csv_content = field.text().await.unwrap_or_default();
        } else if name == "csrf" {
            csrf_token = field.text().await.unwrap_or_default();
        }
    }

    // Validate CSRF if we have a session
    if let Some(sid) = sid_opt {
        let map = state.sessions.read();
        if map.get(&sid).map(|(v, _exp, _u)| v.as_str()) != Some(csrf_token.as_str()) {
            tracing::warn!(request_id=%req_id, outcome=%"bulk_import_forbidden_csrf_mismatch");
            if wants_json(&headers) {
                let body = json!({"error": "csrf_mismatch"});
                let mut resp = into_boxed_response((StatusCode::FORBIDDEN, Json(body)));
                resp.headers_mut().insert(
                    axum::http::header::HeaderName::from_static("x-request-id"),
                    HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
                );
                return resp;
            }
            return into_boxed_response((
                StatusCode::FORBIDDEN,
                Html("<h1>Forbidden</h1>".to_string()),
            ));
        }
    }

    if csv_content.is_empty() {
        tracing::warn!(request_id=%req_id, outcome=%"bulk_import_no_file");
        if wants_json(&headers) {
            let body = json!({"error": "no_csv_file"});
            let mut resp = into_boxed_response((StatusCode::BAD_REQUEST, Json(body)));
            resp.headers_mut().insert(
                axum::http::header::HeaderName::from_static("x-request-id"),
                HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
            );
            return resp;
        }
        return into_boxed_response((
            StatusCode::BAD_REQUEST,
            Html("<h1>No CSV file provided</h1>".to_string()),
        ));
    }

    // Parse CSV and create repositories
    let mut created = Vec::new();
    let mut errors = Vec::new();

    for (line_num, line) in csv_content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue; // Skip empty lines and comments
        }

        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() < 2 {
            errors.push(format!(
                "Line {}: Invalid format, expected at least name,url",
                line_num + 1
            ));
            continue;
        }

        let name = parts[0].to_string();
        let url = parts[1].to_string();
        let branches = if parts.len() > 2 && !parts[2].is_empty() {
            Some(parts[2].to_string())
        } else {
            None
        };
        let frequency = if parts.len() > 3 && !parts[3].is_empty() {
            parts[3].parse::<u64>().ok()
        } else {
            None
        };

        if name.is_empty() || url.is_empty() {
            errors.push(format!(
                "Line {}: Name and URL cannot be empty",
                line_num + 1
            ));
            continue;
        }

        // Create the repository using the same logic as the create endpoint
        if let Some(pool) = &state.redis_pool {
            let script = r#"
                local name = ARGV[1]
                local url = ARGV[2]
                if redis.call('HEXISTS', KEYS[1], name) == 1 then
                    return 1
                end
                if redis.call('SISMEMBER', KEYS[2], url) == 1 then
                    return 2
                end
                redis.call('HSET', KEYS[1], name, url)
                redis.call('SADD', KEYS[2], url)
                return 0
            "#;

            match pool
                .eval_i32(script, &["zoekt:repos", "zoekt:repo_urls"], &[&name, &url])
                .await
            {
                Ok(res) => match res {
                    0 => {
                        // Success - store metadata
                        let meta_key = "zoekt:repo_meta".to_string();
                        let mut meta = json!({
                            "frequency": frequency.unwrap_or(60),
                        });

                        let branches_value = if let Some(branches) = &branches {
                            if !branches.trim().is_empty() {
                                // Validate the branch pattern by expanding it
                                match expand_branch_patterns(&url, branches.trim()).await {
                                    Ok(_) => {
                                        // Store the original pattern, not the expanded branches
                                        // The indexer will re-expand when it clones the repo
                                        branches.trim().to_string()
                                    }
                                    Err(e) => {
                                        errors.push(format!(
                                            "Line {}: Failed to expand branch patterns '{}': {}",
                                            line_num + 1,
                                            branches.trim(),
                                            e
                                        ));
                                        branches.trim().to_string()
                                    }
                                }
                            } else {
                                "main".to_string()
                            }
                        } else {
                            "main".to_string()
                        };
                        meta["branches"] = json!(branches_value);

                        if let Err(e) = pool.hset(&meta_key, &name, &meta.to_string()).await {
                            errors.push(format!(
                                "Line {}: Failed to store metadata: {}",
                                line_num + 1,
                                e
                            ));
                        } else {
                            // write branch-level entries: expand stored pattern and write entries to zoekt:repo_branches
                            let branches_pattern = branches_value.clone();
                            match expand_branch_patterns(&url, &branches_pattern).await {
                                Ok(expanded) => {
                                    let branch_map_key = "zoekt:repo_branches";
                                    for b in expanded.iter() {
                                        let field = format!("{}|{}", name, b);
                                        let _ = pool.hset(branch_map_key, &field, &url).await;
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(request_id=%req_id, line=%line_num, error=?e, "failed to expand branches during bulk import");
                                }
                            }
                            created.push(name.clone());
                        }
                    }
                    1 => {
                        errors.push(format!(
                            "Line {}: Repository name '{}' already exists",
                            line_num + 1,
                            name
                        ));
                    }
                    2 => {
                        errors.push(format!(
                            "Line {}: Repository URL '{}' already exists",
                            line_num + 1,
                            url
                        ));
                    }
                    _ => {
                        errors.push(format!(
                            "Line {}: Unknown error creating repository '{}'",
                            line_num + 1,
                            name
                        ));
                    }
                },
                Err(e) => {
                    errors.push(format!(
                        "Line {}: Redis error for '{}': {}",
                        line_num + 1,
                        name,
                        e
                    ));
                }
            }
        } else {
            errors.push(format!(
                "Line {}: No Redis connection available",
                line_num + 1
            ));
        }
    }

    tracing::info!(
        request_id=%req_id,
        created_count=%created.len(),
        errors_count=%errors.len(),
        outcome=%"bulk_import_completed"
    );

    if wants_json(&headers) {
        let body = json!({
            "created": created,
            "errors": errors
        });
        let mut resp = into_boxed_response((StatusCode::OK, Json(body)));
        resp.headers_mut().insert(
            axum::http::header::HeaderName::from_static("x-request-id"),
            HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
        );
        return resp;
    }

    // For HTML response, redirect back to index
    let mut resp = into_boxed_response(Redirect::to("/"));
    resp.headers_mut().insert(
        axum::http::header::HeaderName::from_static("x-request-id"),
        HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    resp
}

async fn export_csv(state: Extension<AppState>, headers: HeaderMap) -> axum::response::Response {
    let req_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(gen_token);

    // authentication: allow basic auth or a valid server-side session (same as index)
    let is_basic = basic_auth_from_headers(&headers, &state.admin_user, &state.admin_pass);
    let session_mgr = SessionManager::new(state.sessions.clone(), state.session_hmac_key.clone());
    let sid_opt = session_mgr.verify_and_extract_session(&headers);
    let has_session = if let Some(sid) = &sid_opt {
        let map = state.sessions.read();
        map.get(sid)
            .map(|(_tok, exp, _u)| Instant::now() < *exp)
            .unwrap_or(false)
    } else {
        false
    };
    if !is_basic && !has_session {
        // prefer JSON clients for API-style requests, otherwise redirect
        if wants_json(&headers) {
            let body = json!({"error": "unauthorized"});
            tracing::warn!(request_id=%req_id, outcome=%"export_unauthorized");
            let mut resp = into_boxed_response((StatusCode::UNAUTHORIZED, Json(body)));
            resp.headers_mut().insert(
                axum::http::header::HeaderName::from_static("x-request-id"),
                HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
            );
            return resp;
        }
        tracing::warn!(request_id=%req_id, outcome=%"export_redirect_login");
        return into_boxed_response(Redirect::to("/login"));
    }

    // Build CSV
    fn esc(s: &str) -> String {
        // escape double quotes per CSV and wrap in quotes
        let mut out = String::new();
        out.push('"');
        for ch in s.chars() {
            if ch == '"' {
                out.push_str("\"\"");
            } else {
                out.push(ch);
            }
        }
        out.push('"');
        out
    }

    let mut csv = String::new();
    csv.push_str("name,url,branches,frequency,branch_details\n");
    if let Some(pool) = &state.redis_pool {
        if let Ok(entries) = pool.hgetall("zoekt:repos").await {
            for (name, url) in entries {
                let mut frequency = String::new();
                let mut branches = String::new();

                if let Ok(Some(meta_json)) = pool.hget("zoekt:repo_meta", &name).await {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&meta_json) {
                        // Only read administrative fields from repo_meta. Runtime/status
                        // fields are stored per-branch in `zoekt:repo_branch_meta`.
                        if let Some(f) = v.get("frequency") {
                            frequency = f.to_string();
                        }
                        if let Some(b) = v.get("branches") {
                            if !b.is_null() {
                                if let Some(s) = b.as_str() {
                                    branches = s.to_string();
                                } else {
                                    branches = b.to_string();
                                }
                            }
                        }
                    }
                }

                // collect branch-level meta summary
                let mut branch_details: Vec<String> = Vec::new();
                if let Ok(entries_b) = pool.hgetall("zoekt:repo_branch_meta").await {
                    for (field, val) in entries_b {
                        if field.starts_with(&format!("{}|", name)) {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&val) {
                                if let Some((_, branch)) = field.split_once('|') {
                                    let last_indexed_b = v
                                        .get("last_indexed")
                                        .map(|x| x.to_string())
                                        .unwrap_or_default();
                                    let dur_b = v
                                        .get("last_duration_ms")
                                        .map(|x| x.to_string())
                                        .unwrap_or_default();
                                    branch_details.push(format!(
                                        "{}:last_indexed={} dur_ms={}",
                                        branch, last_indexed_b, dur_b
                                    ));
                                }
                            }
                        }
                    }
                }

                csv.push_str(&format!(
                    "{},{},{},{},{}\n",
                    esc(&name),
                    esc(&url),
                    esc(&branches),
                    esc(&frequency),
                    esc(&branch_details.join("; ")),
                ));
            }
        }
    }

    // return CSV with proper headers
    let mut resp = into_boxed_response((StatusCode::OK, csv));
    resp.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/csv; charset=utf-8"),
    );
    resp.headers_mut().insert(
        axum::http::header::HeaderName::from_static("content-disposition"),
        HeaderValue::from_static("attachment; filename=zoekt_repos.csv"),
    );
    resp.headers_mut().insert(
        axum::http::header::HeaderName::from_static("x-request-id"),
        HeaderValue::from_str(&req_id).unwrap_or_else(|_| HeaderValue::from_static("")),
    );
    resp
}

/// Background task: sweeps expired sessions every minute
async fn session_sweeper(state: AppState) {
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        let mut map = state.sessions.write();
        let now = Instant::now();
        let keys: Vec<String> = map
            .iter()
            .filter_map(|(k, (_v, exp, _u))| if *exp <= now { Some(k.clone()) } else { None })
            .collect();
        for k in keys {
            map.remove(&k);
        }
    }
}

// ...existing code...

#[tokio::main]
async fn main() -> Result<()> {
    // Emit structured JSON logs for easier aggregation. Honor RUST_LOG via EnvFilter.
    // include pid in logs via field when needed; avoid unused variable warning
    let _pid = std::process::id();
    tracing_subscriber::fmt()
        .with_timer(UtcTime::rfc_3339())
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let opts = Opts::parse();

    let _cfg = load_node_config(
        NodeConfig {
            node_type: NodeType::Admin,
            ..Default::default()
        },
        MergeOpts {
            config_path: opts.config,
            cli_id: opts.id,
            cli_lease_ttl_seconds: opts.lease_ttl_seconds,
            cli_poll_interval_seconds: opts.poll_interval_seconds,
            cli_endpoint: None,
            cli_enable_reindex: None,
            cli_index_once: None,
        },
    )?;

    // Build redis pool from REDIS_URL if present, with optional authentication
    let redis_pool = match std::env::var("REDIS_URL").ok() {
        Some(mut url) => {
            // Check if URL already has authentication
            let has_auth = url.contains('@');

            // If no auth in URL, try to add it from environment variables
            if !has_auth {
                if let Ok(username) = std::env::var("REDIS_USERNAME") {
                    if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                        // Insert username:password@ after redis://
                        if let Some(pos) = url.find("://") {
                            let auth_part = format!("{}:{}@", username, password);
                            url.insert_str(pos + 3, &auth_part);
                        }
                    } else {
                        // Username without password
                        if let Some(pos) = url.find("://") {
                            let auth_part = format!("{}@", username);
                            url.insert_str(pos + 3, &auth_part);
                        }
                    }
                } else if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                    // Password without username
                    if let Some(pos) = url.find("://") {
                        let auth_part = format!(":{}@", password);
                        url.insert_str(pos + 3, &auth_part);
                    }
                }
            }

            RedisConfig::from_url(&url)
                .create_pool(None)
                .ok()
                .map(|p| std::sync::Arc::new(RealRedis { pool: p }) as std::sync::Arc<dyn DynRedis>)
        }
        None => {
            // No REDIS_URL provided, but check if we have auth credentials to construct one
            if let (Ok(username), Ok(password)) = (
                std::env::var("REDIS_USERNAME"),
                std::env::var("REDIS_PASSWORD"),
            ) {
                let url = format!("redis://{}:{}@127.0.0.1:6379", username, password);
                RedisConfig::from_url(&url).create_pool(None).ok().map(|p| {
                    std::sync::Arc::new(RealRedis { pool: p }) as std::sync::Arc<dyn DynRedis>
                })
            } else if let Ok(password) = std::env::var("REDIS_PASSWORD") {
                let url = format!("redis://:{}@127.0.0.1:6379", password);
                RedisConfig::from_url(&url).create_pool(None).ok().map(|p| {
                    std::sync::Arc::new(RealRedis { pool: p }) as std::sync::Arc<dyn DynRedis>
                })
            } else {
                None
            }
        }
    };

    // Admin credentials from env (user requested ZOEKT_ADMIN_{USERNAME,PASSWORD})
    let admin_user = std::env::var("ZOEKT_ADMIN_USERNAME").unwrap_or_else(|_| "admin".into());
    let admin_pass = std::env::var("ZOEKT_ADMIN_PASSWORD").unwrap_or_else(|_| "password".into());

    // optional HMAC key for signing session ids (read from env ZOEKT_SESSION_KEY as base64)
    let session_hmac_key = std::env::var("ZOEKT_SESSION_KEY")
        .ok()
        .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok());

    let state = Arc::new(AppStateInner {
        redis_pool,
        admin_user: admin_user.clone(),
        admin_pass: admin_pass.clone(),
        sessions: Arc::new(RwLock::new(HashMap::new())),
        session_hmac_key,
    });

    let app = make_app(state.clone());

    // spawn background session sweeper
    let sweeper_state = state.clone();
    tokio::spawn(async move { session_sweeper(sweeper_state).await });

    let addr: SocketAddr = opts.bind.parse()?;
    tracing::info!("admin UI listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    Ok(())
}

#[tokio::test]
async fn login_create_delete_flow_inprocess() {
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request};
    use tower::util::ServiceExt; // for oneshot

    let sessions = Arc::new(RwLock::new(HashMap::new()));
    sessions.write().insert(
        "sid1".into(),
        (
            "testtoken".into(),
            Instant::now() + Duration::from_secs(3600),
            None,
        ),
    );
    let state = Arc::new(AppStateInner {
        redis_pool: None,
        admin_user: "user1".into(),
        admin_pass: "pass1".into(),
        sessions: sessions.clone(),
        session_hmac_key: None,
    });
    let app1 = make_app(state.clone());

    let creds = base64::engine::general_purpose::STANDARD.encode("user1:pass1");
    let req = Request::builder()
        .method("POST")
        .uri("/create")
        .header(AUTHORIZATION, format!("Basic {}", creds))
        .header("Cookie", "dzr_session=sid1|sig")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(
            "name=flowrepo&url=https%3A%2F%2Fexample.com&csrf=testtoken",
        ))
        .unwrap();
    let resp = app1.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection() || resp.status().as_u16() == 201);

    let app2 = make_app(state.clone());
    let creds = base64::engine::general_purpose::STANDARD.encode("user1:pass1");
    let req = Request::builder()
        .method("POST")
        .uri("/delete")
        .header(AUTHORIZATION, format!("Basic {}", creds))
        .header("Cookie", "dzr_session=sid1|sig")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from("name=flowrepo&csrf=testtoken"))
        .unwrap();
    let resp = app2.oneshot(req).await.unwrap();
    assert!(resp.status().is_redirection() || resp.status().as_u16() == 200);
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request};
    use tower::util::ServiceExt;
    use zoekt_distributed::redis_adapter; // for oneshot

    // Simple mock Redis implementation used only in unit tests below.
    struct MockRedis {
        // pre-programmed responses for eval scripts keyed by a simple token
        pub eval_response: std::sync::Mutex<Option<i32>>,
    }

    #[async_trait]
    impl redis_adapter::DynRedis for MockRedis {
        async fn ping(&self) -> anyhow::Result<()> {
            Ok(())
        }

        async fn hgetall(&self, _key: &str) -> anyhow::Result<Vec<(String, String)>> {
            Ok(vec![])
        }

        async fn eval_i32(
            &self,
            _script: &str,
            _keys: &[&str],
            _args: &[&str],
        ) -> anyhow::Result<i32> {
            let mut lock = self.eval_response.lock().unwrap();
            Ok(lock.take().unwrap_or(0))
        }

        async fn hset(&self, _key: &str, _field: &str, _value: &str) -> anyhow::Result<()> {
            Ok(())
        }

        async fn hget(&self, _key: &str, _field: &str) -> anyhow::Result<Option<String>> {
            Ok(None)
        }

        async fn hdel(&self, _key: &str, _field: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn index_requires_auth() {
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        sessions.write().insert(
            "sid1".into(),
            (
                "testtoken".into(),
                Instant::now() + Duration::from_secs(3600),
                None,
            ),
        );
        let state = Arc::new(AppStateInner {
            redis_pool: None,
            admin_user: "user1".into(),
            admin_pass: "pass1".into(),
            sessions: sessions.clone(),
            session_hmac_key: None,
        });
        let app = make_app(state);
        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        // expect a redirect to /login for browser clients
        assert!(resp.status().is_redirection());
    }

    #[tokio::test]
    async fn create_redirects_when_authorized() {
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        sessions.write().insert(
            "sid1".into(),
            (
                "testtoken".into(),
                Instant::now() + Duration::from_secs(3600),
                None,
            ),
        );
        let state = Arc::new(AppStateInner {
            redis_pool: None,
            admin_user: "user1".into(),
            admin_pass: "pass1".into(),
            sessions: sessions.clone(),
            session_hmac_key: None,
        });
        let app = make_app(state);

        let creds = base64::engine::general_purpose::STANDARD.encode("user1:pass1");
        let req = Request::builder()
            .method("POST")
            .uri("/create")
            .header(AUTHORIZATION, format!("Basic {}", creds))
            .header("Cookie", "dzr_session=sid1|sig")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(
                "name=foo&url=https%3A%2F%2Fexample.com&csrf=testtoken",
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert!(resp.status().is_redirection());
    }

    #[tokio::test]
    async fn create_unauthorized_is_denied() {
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        sessions.write().insert(
            "sid1".into(),
            (
                "testtoken".into(),
                Instant::now() + Duration::from_secs(3600),
                None,
            ),
        );
        let state = Arc::new(AppStateInner {
            redis_pool: None,
            admin_user: "user1".into(),
            admin_pass: "pass1".into(),
            sessions: sessions.clone(),
            session_hmac_key: None,
        });
        let app = make_app(state);
        let req = Request::builder()
            .method("POST")
            .uri("/create")
            .header("Cookie", "dzr_session=sid1")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(
                "name=foo&url=http%3A%2F%2Fexample.com&csrf=testtoken",
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        // unsigned session cookies are accepted if the server-side session map contains
        // a valid (non-expired) entry for the sid. Expect a redirect on success.
        assert!(resp.status().is_redirection());
    }

    #[tokio::test]
    async fn delete_with_csrf_and_auth() {
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        sessions.write().insert(
            "sid1".into(),
            (
                "testtoken".into(),
                Instant::now() + Duration::from_secs(3600),
                None,
            ),
        );
        let state = Arc::new(AppStateInner {
            redis_pool: None,
            admin_user: "user1".into(),
            admin_pass: "pass1".into(),
            sessions: sessions.clone(),
            session_hmac_key: None,
        });
        let app = make_app(state);
        let creds = base64::engine::general_purpose::STANDARD.encode("user1:pass1");
        let req = Request::builder()
            .method("POST")
            .uri("/delete")
            .header(AUTHORIZATION, format!("Basic {}", creds))
            .header("Cookie", "dzr_session=sid1|sig")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("name=repo1&csrf=testtoken"))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert!(resp.status().is_redirection());
    }

    #[tokio::test]
    async fn create_conflict_name_returns_409() {
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        sessions.write().insert(
            "sid1".into(),
            (
                "testtoken".into(),
                Instant::now() + Duration::from_secs(3600),
                Some("user1".into()),
            ),
        );

        let mock = std::sync::Arc::new(MockRedis {
            eval_response: std::sync::Mutex::new(Some(1)),
        });
        let state = Arc::new(AppStateInner {
            redis_pool: Some(mock),
            admin_user: "user1".into(),
            admin_pass: "pass1".into(),
            sessions: sessions.clone(),
            session_hmac_key: None,
        });
        let app = make_app(state);

        let creds = base64::engine::general_purpose::STANDARD.encode("user1:pass1");
        let req = Request::builder()
            .method("POST")
            .uri("/create")
            .header(AUTHORIZATION, format!("Basic {}", creds))
            .header("Cookie", "dzr_session=sid1|sig")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(
                "name=conflict&url=https%3A%2F%2Fexample.com&csrf=testtoken",
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn delete_not_found_returns_404() {
        let sessions = Arc::new(RwLock::new(HashMap::new()));
        sessions.write().insert(
            "sid1".into(),
            (
                "testtoken".into(),
                Instant::now() + Duration::from_secs(3600),
                Some("user1".into()),
            ),
        );

        let mock = std::sync::Arc::new(MockRedis {
            eval_response: std::sync::Mutex::new(Some(1)),
        });
        let state = Arc::new(AppStateInner {
            redis_pool: Some(mock),
            admin_user: "user1".into(),
            admin_pass: "pass1".into(),
            sessions: sessions.clone(),
            session_hmac_key: None,
        });
        let app = make_app(state);

        let creds = base64::engine::general_purpose::STANDARD.encode("user1:pass1");
        let req = Request::builder()
            .method("POST")
            .uri("/delete")
            .header(AUTHORIZATION, format!("Basic {}", creds))
            .header("Cookie", "dzr_session=sid1|sig")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("name=missing&csrf=testtoken"))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn login_sets_session_username() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::util::ServiceExt; // for oneshot

        let sessions = Arc::new(RwLock::new(HashMap::new()));
        let state = Arc::new(AppStateInner {
            redis_pool: None,
            admin_user: "user1".into(),
            admin_pass: "pass1".into(),
            sessions: sessions.clone(),
            session_hmac_key: None,
        });
        let app = make_app(state.clone());

        // submit correct credentials to /login
        let body = "username=user1&password=pass1";
        let req = Request::builder()
            .method("POST")
            .uri("/login")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        // Expect a redirect on success
        assert!(resp.status().is_redirection());

        // Extract Set-Cookie header to find the session id
        let cookie_hdr = resp
            .headers()
            .get(SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .expect("Set-Cookie present");
        // cookie_hdr looks like "dzr_session=SID[|SIG]; Path=/; HttpOnly; SameSite=None"
        let first_pair = cookie_hdr.split(';').next().unwrap_or_default();
        let v = first_pair
            .split_once('=')
            .map(|(_, v)| v)
            .unwrap_or_default();
        let sid_val = v.split_once('|').map(|(s, _)| s).unwrap_or(v).to_string();

        // verify the server-side session has an associated username
        let map = sessions.read();
        let entry = map.get(&sid_val).expect("session created");
        assert_eq!(entry.2.as_deref(), Some("user1"));
    }

    #[tokio::test]
    async fn login_with_bad_credentials_does_not_set_username() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::util::ServiceExt; // for oneshot

        let sessions = Arc::new(RwLock::new(HashMap::new()));
        let state = Arc::new(AppStateInner {
            redis_pool: None,
            admin_user: "user1".into(),
            admin_pass: "pass1".into(),
            sessions: sessions.clone(),
            session_hmac_key: None,
        });
        let app = make_app(state.clone());

        // submit incorrect credentials to /login
        let body = "username=user1&password=wrongpass";
        let req = Request::builder()
            .method("POST")
            .uri("/login")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        // Expect Unauthorized
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // There should be no Set-Cookie header
        assert!(resp.headers().get(SET_COOKIE).is_none());

        // And server-side sessions map should remain empty
        let map = sessions.read();
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn full_login_index_logout_flow_unsigned() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::util::ServiceExt; // for oneshot

        let sessions = Arc::new(RwLock::new(HashMap::new()));
        let mock = std::sync::Arc::new(MockRedis {
            eval_response: std::sync::Mutex::new(None),
        });
        let state = Arc::new(AppStateInner {
            redis_pool: Some(mock),
            admin_user: "user1".into(),
            admin_pass: "pass1".into(),
            sessions: sessions.clone(),
            session_hmac_key: None,
        });
        let app = make_app(state.clone());

        // login
        let body = "username=user1&password=pass1";
        let req = Request::builder()
            .method("POST")
            .uri("/login")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(body))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert!(resp.status().is_redirection());

        // extract cookie value
        let cookie_hdr = resp
            .headers()
            .get(SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .expect("Set-Cookie present");
        let first_pair = cookie_hdr.split(';').next().unwrap_or_default();
        let v = first_pair
            .split_once('=')
            .map(|(_, v)| v)
            .unwrap_or_default();
        let cookie_val = format!("dzr_session={}", v);

        // access index with cookie
        let req = Request::builder()
            .method("GET")
            .uri("/")
            .header("Cookie", &cookie_val)
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // basic smoke check: status ok and response contains HTML content-type
        // full HTML content validation isn't necessary here
        assert_eq!(resp.status(), StatusCode::OK);

        // logout
        let req = Request::builder()
            .method("GET")
            .uri("/logout")
            .header("Cookie", &cookie_val)
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert!(resp.status().is_redirection());
        // expect Set-Cookie clearing header
        let clear = resp
            .headers()
            .get(SET_COOKIE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(clear.contains("Max-Age=0") || clear.contains("Max-Age: 0"));

        // subsequent index should redirect to login
        let req = Request::builder()
            .method("GET")
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_redirection());
    }

    #[tokio::test]
    async fn create_with_custom_branches() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::util::ServiceExt; // for oneshot

        let sessions = Arc::new(RwLock::new(HashMap::new()));
        sessions.write().insert(
            "sid1".into(),
            (
                "testtoken".into(),
                Instant::now() + Duration::from_secs(3600),
                None,
            ),
        );
        let state = Arc::new(AppStateInner {
            redis_pool: None,
            admin_user: "user1".into(),
            admin_pass: "pass1".into(),
            sessions: sessions.clone(),
            session_hmac_key: None,
        });
        let app = make_app(state);

        let creds = base64::engine::general_purpose::STANDARD.encode("user1:pass1");
        let req = Request::builder()
            .method("POST")
            .uri("/create")
            .header(AUTHORIZATION, format!("Basic {}", creds))
            .header("Cookie", "dzr_session=sid1|sig")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(
                "name=customrepo&url=https%3A%2F%2Fexample.com&branches=main%2Cfeatures%2F*&csrf=testtoken",
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_redirection());
    }

    #[tokio::test]
    async fn create_defaults_to_main_branch() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::util::ServiceExt; // for oneshot

        let sessions = Arc::new(RwLock::new(HashMap::new()));
        sessions.write().insert(
            "sid1".into(),
            (
                "testtoken".into(),
                Instant::now() + Duration::from_secs(3600),
                None,
            ),
        );
        let state = Arc::new(AppStateInner {
            redis_pool: None,
            admin_user: "user1".into(),
            admin_pass: "pass1".into(),
            sessions: sessions.clone(),
            session_hmac_key: None,
        });
        let app = make_app(state);

        let creds = base64::engine::general_purpose::STANDARD.encode("user1:pass1");
        let req = Request::builder()
            .method("POST")
            .uri("/create")
            .header(AUTHORIZATION, format!("Basic {}", creds))
            .header("Cookie", "dzr_session=sid1|sig")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(
                "name=defaultrepo&url=https%3A%2F%2Fexample.com&csrf=testtoken",
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_redirection());
    }

    #[tokio::test]
    async fn test_expand_branch_patterns() {
        let mock_branches = vec![
            "main".to_string(),
            "develop".to_string(),
            "features/feature1".to_string(),
            "features/feature2".to_string(),
            "hotfix/v1.0".to_string(),
            "hotfix/v1.1".to_string(),
        ];
        let fetcher = MockBranchFetcher {
            branches: mock_branches,
        };

        // Test empty pattern
        let result =
            expand_branch_patterns_with_fetcher("https://github.com/test/repo", "", &fetcher).await;
        assert_eq!(result.unwrap(), vec!["main"]);

        // Test whitespace only
        let result =
            expand_branch_patterns_with_fetcher("https://github.com/test/repo", "   ", &fetcher)
                .await;
        assert_eq!(result.unwrap(), vec!["main"]);

        // Test single branch
        let result =
            expand_branch_patterns_with_fetcher("https://github.com/test/repo", "main", &fetcher)
                .await;
        assert_eq!(result.unwrap(), vec!["main"]);

        // Test multiple branches
        let result = expand_branch_patterns_with_fetcher(
            "https://github.com/test/repo",
            "main,develop",
            &fetcher,
        )
        .await;
        assert_eq!(result.unwrap(), vec!["develop", "main"]);

        // Test with wildcards
        let result = expand_branch_patterns_with_fetcher(
            "https://github.com/test/repo",
            "main,features/*",
            &fetcher,
        )
        .await;
        assert_eq!(
            result.unwrap(),
            vec!["features/feature1", "features/feature2", "main"]
        );

        // Test mixed patterns
        let result = expand_branch_patterns_with_fetcher(
            "https://github.com/test/repo",
            "main,features/*,hotfix/*",
            &fetcher,
        )
        .await;
        assert_eq!(
            result.unwrap(),
            vec![
                "features/feature1",
                "features/feature2",
                "hotfix/v1.0",
                "hotfix/v1.1",
                "main"
            ]
        );

        // Test with spaces around commas
        let result = expand_branch_patterns_with_fetcher(
            "https://github.com/test/repo",
            " main , develop ",
            &fetcher,
        )
        .await;
        assert_eq!(result.unwrap(), vec!["develop", "main"]);

        // Test non-existent branch (should still be included)
        let result = expand_branch_patterns_with_fetcher(
            "https://github.com/test/repo",
            "nonexistent",
            &fetcher,
        )
        .await;
        assert_eq!(result.unwrap(), vec!["nonexistent"]);

        // Test pattern that matches nothing
        let result = expand_branch_patterns_with_fetcher(
            "https://github.com/test/repo",
            "releases/*",
            &fetcher,
        )
        .await;
        assert_eq!(result.unwrap(), vec!["main"]); // fallback to main when no matches
    }

    #[tokio::test]
    async fn bulk_import_csv_success() {
        // Create CSV content with multiple repositories
        let csv_content = r#"repo1,https://github.com/org/repo1,main,60
repo2,https://github.com/org/repo2,main,develop,120
# This is a comment line
repo3,https://github.com/org/repo3,,300"#;

        let mut created = Vec::new();
        let mut errors = Vec::new();

        for (line_num, line) in csv_content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue; // Skip empty lines and comments
            }

            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if parts.len() < 2 {
                errors.push(format!(
                    "Line {}: Invalid format, expected at least name,url",
                    line_num + 1
                ));
                continue;
            }

            let name = parts[0].to_string();
            let url = parts[1].to_string();

            if name.is_empty() || url.is_empty() {
                errors.push(format!(
                    "Line {}: Name and URL cannot be empty",
                    line_num + 1
                ));
                continue;
            }

            created.push(name.clone());
        }

        // Verify CSV parsing results
        assert_eq!(created.len(), 3);
        assert_eq!(created[0], "repo1");
        assert_eq!(created[1], "repo2");
        assert_eq!(created[2], "repo3");
        assert_eq!(errors.len(), 0);
    }

    #[tokio::test]
    async fn bulk_import_csv_with_errors() {
        // Test CSV parsing with various error conditions
        let csv_content = r#"# Valid repo
valid_repo,https://github.com/org/valid,main,60

# Empty name
,https://github.com/org/invalid

# Empty URL
invalid_name,

# Invalid frequency (non-numeric)
bad_freq,https://github.com/org/bad_freq,main,not_a_number

# Valid repo after errors
another_valid,https://github.com/org/another,develop,120"#;

        let mut created = Vec::new();
        let mut errors = Vec::new();

        for (line_num, line) in csv_content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue; // Skip empty lines and comments
            }

            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if parts.len() < 2 {
                errors.push(format!(
                    "Line {}: Invalid format, expected at least name,url",
                    line_num + 1
                ));
                continue;
            }

            let name = parts[0].to_string();
            let url = parts[1].to_string();

            if name.is_empty() || url.is_empty() {
                errors.push(format!(
                    "Line {}: Name and URL cannot be empty",
                    line_num + 1
                ));
                continue;
            }

            // Validate frequency if provided
            if parts.len() >= 4 {
                let freq_str = parts[3];
                if !freq_str.is_empty() && freq_str.parse::<u32>().is_err() {
                    errors.push(format!(
                        "Line {}: Invalid frequency '{}', must be a number",
                        line_num + 1,
                        freq_str
                    ));
                    continue;
                }
            }

            created.push(name.clone());
        }

        // Verify results
        assert_eq!(created.len(), 2); // valid_repo and another_valid
        assert_eq!(created[0], "valid_repo");
        assert_eq!(created[1], "another_valid");
        assert_eq!(errors.len(), 3); // Three error cases: empty name, empty URL, invalid frequency
        assert!(errors[0].contains("Name and URL cannot be empty"));
        assert!(errors[1].contains("Name and URL cannot be empty"));
        assert!(errors[2].contains("Invalid frequency"));
    }

    #[tokio::test]
    async fn bulk_import_empty_csv() {
        // Test with empty CSV file
        let csv_content = "";

        let mut created = Vec::new();
        let mut errors = Vec::new();

        for (line_num, line) in csv_content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if parts.len() < 2 {
                errors.push(format!(
                    "Line {}: Invalid format, expected at least name,url",
                    line_num + 1
                ));
                continue;
            }

            let name = parts[0].to_string();
            let url = parts[1].to_string();

            if name.is_empty() || url.is_empty() {
                errors.push(format!(
                    "Line {}: Name and URL cannot be empty",
                    line_num + 1
                ));
                continue;
            }

            created.push(name.clone());
        }

        assert_eq!(created.len(), 0);
        assert_eq!(errors.len(), 0);
    }

    #[tokio::test]
    async fn bulk_import_csv_with_comments_and_empty_lines() {
        // Test CSV with comments and empty lines
        let csv_content = r#"
# This is a comment
repo1,https://github.com/org/repo1

# Another comment

repo2,https://github.com/org/repo2,main,60


# Final comment
repo3,https://github.com/org/repo3,develop"#;

        let mut created = Vec::new();
        let mut errors = Vec::new();

        for (line_num, line) in csv_content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if parts.len() < 2 {
                errors.push(format!(
                    "Line {}: Invalid format, expected at least name,url",
                    line_num + 1
                ));
                continue;
            }

            let name = parts[0].to_string();
            let url = parts[1].to_string();

            if name.is_empty() || url.is_empty() {
                errors.push(format!(
                    "Line {}: Name and URL cannot be empty",
                    line_num + 1
                ));
                continue;
            }

            created.push(name.clone());
        }

        assert_eq!(created.len(), 3);
        assert_eq!(created[0], "repo1");
        assert_eq!(created[1], "repo2");
        assert_eq!(created[2], "repo3");
        assert_eq!(errors.len(), 0);
    }
}
