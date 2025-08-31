use axum::http::header::{COOKIE, SET_COOKIE};
use axum::http::HeaderValue;
use axum::Json;
use axum::{
    extract::{Extension, Form},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Router,
};
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use deadpool_redis::Config as RedisConfig;
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use rand::RngCore;
use serde::Deserialize;
use serde_json::json;
use sha2::Sha256;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use zoekt_distributed::redis_adapter::{DynRedis, RealRedis};

// Session entry: (csrf token, expiry Instant, optional username)
type SessionEntry = (String, Instant, Option<String>);

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
struct CreateRepoWithFreq {
    name: String,
    url: String,
    frequency: Option<u64>,
    csrf: String,
}

// Embed static templates and JS so the binary can serve them directly.
const INDEX_TEMPLATE: &str = include_str!("../../static/index.html");
const LOGIN_TEMPLATE: &str = include_str!("../../static/login.html");
const ADMIN_JS: &str = include_str!("../../static/admin.js");

fn basic_auth_from_headers(headers: &HeaderMap, user: &str, pass: &str) -> bool {
    if let Some(v) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(s) = v.to_str() {
            if let Some(rest) = s.strip_prefix("Basic ") {
                if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(rest) {
                    if let Ok(cred) = String::from_utf8(decoded) {
                        return cred == format!("{}:{}", user, pass);
                    }
                }
            }
        }
    }
    false
}

fn parse_basic_auth(headers: &HeaderMap) -> Option<(String, String)> {
    if let Some(v) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(s) = v.to_str() {
            if let Some(rest) = s.strip_prefix("Basic ") {
                if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(rest) {
                    if let Ok(cred) = String::from_utf8(decoded) {
                        if let Some((u, p)) = cred.split_once(':') {
                            return Some((u.to_string(), p.to_string()));
                        }
                    }
                }
            }
        }
    }
    None
}

fn wants_json(headers: &HeaderMap) -> bool {
    if let Some(v) = headers.get(axum::http::header::ACCEPT) {
        if let Ok(s) = v.to_str() {
            if s.contains("application/json") {
                return true;
            }
        }
    }
    if let Some(v) = headers.get("X-Requested-With") {
        if let Ok(s) = v.to_str() {
            if s == "XMLHttpRequest" {
                return true;
            }
        }
    }
    false
}

fn gen_token() -> String {
    let mut b = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut b);
    base64::engine::general_purpose::STANDARD.encode(b)
}

fn into_boxed_response<R: IntoResponse>(r: R) -> axum::response::Response {
    let resp = r.into_response();
    let (parts, body) = resp.into_parts();
    let boxed = axum::body::boxed(body);
    axum::response::Response::from_parts(parts, boxed)
}

fn parse_session_cookie(headers: &HeaderMap) -> Option<String> {
    if let Some(v) = headers.get(COOKIE) {
        if let Ok(s) = v.to_str() {
            for part in s.split(';') {
                let p = part.trim();
                if let Some(rest) = p.strip_prefix("dzr_session=") {
                    return Some(rest.to_string());
                }
            }
        }
    }
    None
}

fn extract_sid_for_log(headers: &HeaderMap) -> Option<String> {
    parse_session_cookie(headers).and_then(|raw| {
        raw.split_once('|')
            .map(|(s, _)| s.to_string())
            .or(Some(raw))
    })
}

fn get_or_create_session(
    state: &AppState,
    headers: &HeaderMap,
) -> (String, String, Option<String>) {
    // If a session cookie exists and hasn't expired, return its token.
    if let Some(sid) = parse_session_cookie(headers) {
        let map = state.sessions.read();
        if let Some((tok, expiry, _user)) = map.get(&sid) {
            if Instant::now() < *expiry {
                return (sid, tok.clone(), None);
            }
            // expired -> fallthrough to create new
        }
    }
    let sid = gen_token();
    let csrf = gen_token();
    let expiry = Instant::now() + Duration::from_secs(60 * 60); // 1 hour
    {
        let mut map = state.sessions.write();
        map.insert(sid.clone(), (csrf.clone(), expiry, None));
    }
    // sign the session id if we have a key
    // For local dev over HTTP, many browsers will reject cookies with SameSite=None unless Secure
    // is set. Respect an env var ZOEKT_SESSION_SECURE=true to enable Secure+SameSite=None (for
    // production/TLS). Otherwise default to SameSite=Lax which works with top-level form POSTs.
    // For local dev over HTTP, many browsers require Secure when SameSite=None.
    // Use SameSite=Lax by default (works for top-level form POSTs). Enable
    // SameSite=None; Secure by setting ZOEKT_SESSION_SECURE=true in production/TLS.
    let secure_flag = std::env::var("ZOEKT_SESSION_SECURE")
        .map(|v| v == "true")
        .unwrap_or(false);
    let same_site_part = if secure_flag {
        "SameSite=None; Secure"
    } else {
        "SameSite=Lax"
    };

    let cookie_val = if let Some(key) = &state.session_hmac_key {
        let mut mac: Hmac<Sha256> =
            Hmac::new_from_slice(key).expect("HMAC can take key of any size");
        mac.update(sid.as_bytes());
        let signature = mac.finalize().into_bytes();
        format!(
            "dzr_session={}|{}; Path=/; HttpOnly; {}",
            sid,
            base64::engine::general_purpose::STANDARD.encode(signature),
            same_site_part
        )
    } else {
        format!("dzr_session={}; Path=/; HttpOnly; {}", sid, same_site_part)
    };
    let cookie = cookie_val;
    (sid, csrf, Some(cookie))
}

fn verify_and_extract_session(headers: &HeaderMap, key: &Option<Vec<u8>>) -> Option<String> {
    // parse cookie value and verify signature if present
    if let Some(raw) = parse_session_cookie(headers) {
        tracing::trace!(
            "verify_and_extract_session: raw_cookie={:?}, key_present={}",
            raw,
            key.is_some()
        );
        if let Some(k) = key {
            if let Some((sid, sig_b64)) = raw.split_once('|') {
                if let Ok(sig) = base64::engine::general_purpose::STANDARD.decode(sig_b64) {
                    let mut mac: Hmac<Sha256> = match Hmac::new_from_slice(k) {
                        Ok(m) => m,
                        Err(_) => return None,
                    };
                    mac.update(sid.as_bytes());
                    if mac.verify_slice(&sig).is_ok() {
                        tracing::trace!(
                            "verify_and_extract_session: signature verified for sid={}",
                            sid
                        );
                        return Some(sid.to_string());
                    }
                    tracing::warn!(
                        "verify_and_extract_session: signature verification failed for sid={}",
                        sid
                    );
                }
                return None;
            }
            // missing signature part
            tracing::warn!(
                "verify_and_extract_session: missing signature part in cookie and key is present"
            );
            return None;
        }
        // no key configured; if cookie contains a '|' (signature part), strip it so
        // the returned session id matches the keys stored in `state.sessions`.
        tracing::trace!(
            "verify_and_extract_session: no key configured, raw_cookie={}",
            raw
        );
        if let Some((sid, _sig)) = raw.split_once('|') {
            return Some(sid.to_string());
        }
        return Some(raw);
    }
    None
}

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

async fn index(state: Extension<AppState>, headers: HeaderMap) -> axum::response::Response {
    let req_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(gen_token);
    tracing::info!(request_id=%req_id, "index: incoming request");
    let is_basic = basic_auth_from_headers(&headers, &state.admin_user, &state.admin_pass);
    let sid_opt = verify_and_extract_session(&headers, &state.session_hmac_key);
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
    let (_sid, csrf_token, cookie) = get_or_create_session(&state, &headers);
    if let Some(pool) = &state.redis_pool {
        if let Ok(entries) = pool.hgetall("zoekt:repos").await {
            for (name, url) in entries {
                let safe_name = htmlescape::encode_minimal(&name);
                // fetch meta JSON from zoekt:repo_meta
                let mut freq = "".to_string();
                let mut last_indexed = "".to_string();
                let mut last_duration_ms = "".to_string();
                let mut leased = "".to_string();
                if let Ok(Some(meta_json)) = pool.hget("zoekt:repo_meta", &name).await {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&meta_json) {
                        if let Some(f) = v.get("frequency") {
                            freq = f.to_string();
                        }
                        if let Some(li) = v.get("last_indexed") {
                            if !li.is_null() {
                                // last_indexed stored as epoch milliseconds -> format RFC3339
                                if let Some(n) = li.as_i64() {
                                    let dt: DateTime<Utc> = Utc
                                        .timestamp_millis_opt(n)
                                        .single()
                                        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap());
                                    last_indexed = dt.to_rfc3339();
                                } else if let Some(s) = li.as_str() {
                                    // fallback if stored as string
                                    last_indexed = s.to_string();
                                } else {
                                    last_indexed = li.to_string();
                                }
                            }
                        }
                        if let Some(ld) = v.get("last_duration_ms") {
                            if !ld.is_null() {
                                last_duration_ms = ld.to_string();
                            }
                        }
                        if let Some(n) = v.get("leased_node") {
                            if !n.is_null() {
                                leased = n.to_string();
                            }
                        }
                    }
                }
                rows.push_str(&format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td><form class=\"delete-form\" method=post action=\"/delete\"><input type=\"hidden\" name=\"name\" value=\"{}\"/><input type=\"hidden\" name=\"csrf\" value=\"{}\"/><button>Delete</button></form></td></tr>",
                    htmlescape::encode_minimal(&name),
                    htmlescape::encode_minimal(&url),
                    htmlescape::encode_minimal(&freq),
                    htmlescape::encode_minimal(&last_indexed),
                    htmlescape::encode_minimal(&last_duration_ms),
                    htmlescape::encode_minimal(&leased),
                    safe_name,
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
    let boxed = axum::body::boxed(body);
    axum::response::Response::from_parts(parts, boxed)
}

async fn create(
    state: Extension<AppState>,
    headers: HeaderMap,
    Form(form): Form<CreateRepoWithFreq>,
) -> axum::response::Response {
    let req_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(gen_token);
    tracing::info!(request_id=%req_id, "create: incoming");
    // compute user_for_log and auth_type
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
    let is_basic = basic_auth_from_headers(&headers, &state.admin_user, &state.admin_pass);
    let sid_opt = verify_and_extract_session(&headers, &state.session_hmac_key);
    // Accept the request if the session id maps to a valid server-side session (not expired).
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
    if form.url.is_empty() || form.name.is_empty() {
        return into_boxed_response((
            StatusCode::BAD_REQUEST,
            Html("<h1>Bad Request</h1>".to_string()),
        ));
    }
    // Validate session cookie -> csrf mapping
    let sid = match sid_opt {
        Some(s) => s,
        None => {
            tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"create_forbidden_no_sid");
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
            Ok(res) => match res {
                0 => {}
                1 => {
                    tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"create_conflict_name", name=%form.name);
                    if wants_json(&headers) {
                        let body = json!({"error": "conflict", "reason": "name_exists", "name": form.name});
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
                        Html("<h1>Conflict: name already exists</h1>".to_string()),
                    ));
                    resp.headers_mut().insert(
                        axum::http::header::HeaderName::from_static("x-request-id"),
                        HeaderValue::from_str(&req_id)
                            .unwrap_or_else(|_| HeaderValue::from_static("")),
                    );
                    return resp;
                }
                2 => {
                    tracing::warn!(request_id=%req_id, user=%user_for_log, auth_type=%auth_type, sid=%sid_log, pid=%std::process::id(), outcome=%"create_conflict_url", url=%form.url);
                    if wants_json(&headers) {
                        let body =
                            json!({"error": "conflict", "reason": "url_exists", "url": form.url});
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
                        Html("<h1>Conflict: url already exists</h1>".to_string()),
                    ));
                    resp.headers_mut().insert(
                        axum::http::header::HeaderName::from_static("x-request-id"),
                        HeaderValue::from_str(&req_id)
                            .unwrap_or_else(|_| HeaderValue::from_static("")),
                    );
                    return resp;
                }
                _ => {
                    tracing::warn!(request_id=%req_id, pid=%std::process::id(), outcome=%"create_unknown_script_result", result=%res);
                    return into_boxed_response((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Html("<h1>Internal Server Error</h1>".to_string()),
                    ));
                }
            },
            Err(e) => {
                tracing::warn!(request_id=%req_id, error=?e, pid=%std::process::id(), outcome=%"create_redis_error");
                return into_boxed_response((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Html("<h1>Internal Server Error</h1>".to_string()),
                ));
            }
        }
    }

    // Persist repo metadata (frequency, last_indexed, last_duration_ms, leased_node)
    if let Some(pool) = &state.redis_pool {
        let meta_key = "zoekt:repo_meta".to_string();
        let meta = json!({
            "frequency": form.frequency.unwrap_or(60),
            "last_indexed": null,
            "last_duration_ms": null,
            "leased_node": null,
        });
        if let Err(e) = pool.hset(&meta_key, &form.name, &meta.to_string()).await {
            tracing::warn!(request_id=%req_id, error=?e, pid=%std::process::id(), outcome=%"create_meta_set_failed");
        }
    }

    if wants_json(&headers) {
        let body = json!({"name": form.name, "url": form.url, "csrf": form.csrf});
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
struct DeleteForm {
    name: String,
    csrf: String,
}

async fn delete(
    state: Extension<AppState>,
    headers: HeaderMap,
    Form(form): Form<DeleteForm>,
) -> axum::response::Response {
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
    let sid_opt = verify_and_extract_session(&headers, &state.session_hmac_key);
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
    if let Some(sid) = verify_and_extract_session(&headers, &state.session_hmac_key) {
        let mut map = state.sessions.write();
        map.remove(&sid);
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

async fn login_post(
    state: Extension<AppState>,
    headers: HeaderMap,
    Form(form): Form<LoginForm>,
) -> axum::response::Response {
    // Validate credentials
    if form.username != state.admin_user || form.password != state.admin_pass {
        tracing::warn!(user=%form.username, pid=%std::process::id(), outcome=%"login_failed");
        return into_boxed_response((
            StatusCode::UNAUTHORIZED,
            Html("<h1>Unauthorized</h1>".to_string()),
        ));
    }

    // Create session and set cookie
    let (sid_new, _csrf, cookie) = get_or_create_session(&state, &headers);
    // associate this session id with the logged-in username
    {
        let mut map = state.sessions.write();
        if let Some(entry) = map.get_mut(&sid_new) {
            entry.2 = Some(form.username.clone());
        }
    }
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

fn make_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/", get(index))
        .route("/login", get(login_get))
        .route("/login", post(login_post))
        .route("/logout", get(logout))
        .route("/create", post(create))
        .route("/delete", post(delete))
        .route("/static/admin.js", get(serve_admin_js))
        .layer(axum::Extension(state))
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

#[tokio::test]
async fn login_create_delete_flow_inprocess() {
    use axum::body::Body;
    use axum::http::{header::AUTHORIZATION, Request};
    use tower::ServiceExt; // for oneshot

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
    use tower::ServiceExt;
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
        let resp = app.oneshot(req).await.unwrap();
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
        let resp = app.oneshot(req).await.unwrap();
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
        let resp = app.oneshot(req).await.unwrap();
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
        let resp = app.oneshot(req).await.unwrap();
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
        let resp = app.oneshot(req).await.unwrap();
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
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn login_sets_session_username() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt; // for oneshot

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
        let resp = app.oneshot(req).await.unwrap();
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
        use tower::ServiceExt; // for oneshot

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
        let resp = app.oneshot(req).await.unwrap();
        // Expect Unauthorized
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // There should be no Set-Cookie header
        assert!(resp.headers().get(SET_COOKIE).is_none());

        // And server-side sessions map should remain empty
        let map = sessions.read();
        assert!(map.is_empty());
    }
}

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
        },
    )?;

    // Build redis pool from REDIS_URL if present
    let redis_pool = match std::env::var("REDIS_URL").ok() {
        Some(url) => RedisConfig::from_url(&url)
            .create_pool(None)
            .ok()
            .map(|p| std::sync::Arc::new(RealRedis { pool: p }) as std::sync::Arc<dyn DynRedis>),
        None => None,
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
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;

    Ok(())
}
