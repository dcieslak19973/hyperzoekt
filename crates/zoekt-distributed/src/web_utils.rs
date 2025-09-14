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

//! Lightweight shared HTTP helper utilities for zoekt-distributed binaries.
//!
//! This module contains small, stateless helpers that are reused by multiple
//! binaries in this crate (for example `dzr-admin` and `dzr-http-search`). The
//! goal is to keep common parsing, cookie handling, and response helpers in one
//! place so behavior is consistent across binaries.
//!
//! Safety and security notes:
//! - `verify_and_extract_session` can optionally validate an HMAC signature when
//!   a key is provided; callers are responsible for keeping the key secret and
//!   for configuring secure cookie flags (SameSite/Secure) when running behind
//!   TLS.
//! - `parse_session_cookie` is intentionally simple and only extracts the
//!   `dzr_session` cookie value; production systems may want a more robust
//!   cookie parser.
//! - `into_boxed_response` boxes response bodies to make returning different
//!   response types convenient from handlers.
//!
//! Keep these helpers minimal — complex logic (session stores, auth backends)
//! should live in higher-level modules so tests can mock or replace them.

use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::response::Response;
use base64::Engine;
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use rand::RngCore;
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Session entry stored in the sessions map: (csrf token, expiry, optional username)
pub type SessionEntry = (String, Instant, Option<String>);

/// Small session manager that owns (or reuses) a sessions map and optional HMAC key.
///
/// Responsibilities:
/// - create or return an existing session id and CSRF token
/// - generate Set-Cookie header values (with optional HMAC signing)
/// - verify session cookies (signature + existence)
pub struct SessionManager {
    sessions: Arc<RwLock<HashMap<String, SessionEntry>>>,
    key: Option<Vec<u8>>,
}

impl SessionManager {
    /// Create a new SessionManager backed by the provided sessions map and optional key.
    pub fn new(sessions: Arc<RwLock<HashMap<String, SessionEntry>>>, key: Option<Vec<u8>>) -> Self {
        Self { sessions, key }
    }

    /// If a session cookie exists and maps to a non-expired entry, return it. Otherwise
    /// create a fresh session id and csrf token, store them and return a Set-Cookie value.
    pub fn get_or_create_session(&self, headers: &HeaderMap) -> (String, String, Option<String>) {
        if let Some(sid) = parse_session_cookie(headers) {
            let map = self.sessions.read();
            if let Some((tok, expiry, _user)) = map.get(&sid) {
                if Instant::now() < *expiry {
                    return (sid, tok.clone(), None);
                }
            }
        }
        let sid = gen_token();
        let csrf = gen_token();
        let expiry = Instant::now() + std::time::Duration::from_secs(60 * 60);
        {
            let mut map = self.sessions.write();
            map.insert(sid.clone(), (csrf.clone(), expiry, None));
        }
        let secure_flag = env_session_secure_flag();
        let cookie = session_cookie_header_value(&sid, self.key.as_deref(), secure_flag);
        (sid, csrf, Some(cookie))
    }

    /// Verify cookie signature (if key configured) and return the raw session id when valid.
    pub fn verify_and_extract_session(&self, headers: &HeaderMap) -> Option<String> {
        if let Some(raw) = parse_session_cookie(headers) {
            if let Some(k) = &self.key {
                if let Some((sid, sig_b64)) = raw.split_once('|') {
                    if let Ok(sig) = base64::engine::general_purpose::STANDARD.decode(sig_b64) {
                        let mut mac: Hmac<Sha256> = match Hmac::new_from_slice(k) {
                            Ok(m) => m,
                            Err(_) => return None,
                        };
                        mac.update(sid.as_bytes());
                        if mac.verify_slice(&sig).is_ok() {
                            return Some(sid.to_string());
                        }
                    }
                    return None;
                }
                return None;
            }
            // no key configured; strip optional signature part if present
            if let Some((sid, _)) = raw.split_once('|') {
                return Some(sid.to_string());
            }
            return Some(raw);
        }
        None
    }

    /// Associate a username with a session id in the sessions map.
    pub fn set_username(&self, sid: &str, username: String) {
        let mut map = self.sessions.write();
        if let Some(entry) = map.get_mut(sid) {
            entry.2 = Some(username);
        }
    }

    /// Remove a session from the sessions map.
    pub fn remove(&self, sid: &str) {
        let mut map = self.sessions.write();
        map.remove(sid);
    }
}

/// Inspect headers for Basic auth matching a username/password.
pub fn basic_auth_from_headers(headers: &HeaderMap, user: &str, pass: &str) -> bool {
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

/// Parse Basic auth headers into (user, pass) if present.
pub fn parse_basic_auth(headers: &HeaderMap) -> Option<(String, String)> {
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

/// Detect if the client prefers JSON responses (Accept header or X-Requested-With).
pub fn wants_json(headers: &HeaderMap) -> bool {
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

/// Generate a random token (base64-encoded 16 bytes).
pub fn gen_token() -> String {
    let mut b = [0u8; 16];
    rand::rng().fill_bytes(&mut b);
    base64::engine::general_purpose::STANDARD.encode(b)
}

/// Return whether cookies should be marked Secure + SameSite=None based on
/// the `ZOEKT_SESSION_SECURE` environment variable. This is useful to toggle
/// behavior between local HTTP dev and production TLS.
pub fn env_session_secure_flag() -> bool {
    std::env::var("ZOEKT_SESSION_SECURE")
        .map(|v| v == "true")
        .unwrap_or(false)
}

/// Build the `Set-Cookie` value for the session cookie. If `key` is provided
/// it will HMAC-sign the session id and append the signature after a `|`.
/// `secure_flag` controls SameSite/Secure attributes.
pub fn session_cookie_header_value(sid: &str, key: Option<&[u8]>, secure_flag: bool) -> String {
    let same_site_part = if secure_flag {
        "SameSite=None; Secure"
    } else {
        "SameSite=Lax"
    };
    if let Some(k) = key {
        let mut mac: Hmac<Sha256> = Hmac::new_from_slice(k).expect("HMAC can take key of any size");
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
    }
}

/// Convert any IntoResponse into a boxed Response (helps returning from handlers).
pub fn into_boxed_response<R: IntoResponse>(r: R) -> Response {
    r.into_response()
}

/// Parse raw Cookie header and return the value of `dzr_session` if present.
pub fn parse_session_cookie(headers: &HeaderMap) -> Option<String> {
    if let Some(v) = headers.get(axum::http::header::COOKIE) {
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

/// Extract session id portion for logging (strip signature part after '|').
pub fn extract_sid_for_log(headers: &HeaderMap) -> Option<String> {
    parse_session_cookie(headers).and_then(|raw| {
        raw.split_once('|')
            .map(|(s, _)| s.to_string())
            .or(Some(raw))
    })
}

/// Verify a session cookie and optionally validate an HMAC signature if a key is provided.
/// Returns Some(session_id) when the cookie is present and valid (or no key configured).
pub fn verify_and_extract_session(headers: &HeaderMap, key: &Option<Vec<u8>>) -> Option<String> {
    if let Some(raw) = parse_session_cookie(headers) {
        if let Some(k) = key {
            if let Some((sid, sig_b64)) = raw.split_once('|') {
                if let Ok(sig) = base64::engine::general_purpose::STANDARD.decode(sig_b64) {
                    let mut mac: Hmac<Sha256> = match Hmac::new_from_slice(k) {
                        Ok(m) => m,
                        Err(_) => return None,
                    };
                    mac.update(sid.as_bytes());
                    if mac.verify_slice(&sig).is_ok() {
                        return Some(sid.to_string());
                    }
                }
                return None;
            }
            return None;
        }
        if let Some((sid, _sig)) = raw.split_once('|') {
            return Some(sid.to_string());
        }
        return Some(raw);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};
    use tracing_subscriber::EnvFilter;

    fn init_test_logging() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // Default: app info, quiet noisy deps
                EnvFilter::new("info,hyper_util=warn,hyper=warn,h2=warn,reqwest=warn,tower_http=warn,ignore=warn")
            });
            let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
        });
    }

    #[test]
    fn test_session_cookie_without_key() {
        init_test_logging();
        tracing::info!("TEST START: web_utils::tests::test_session_cookie_without_key");
        let sid = "session123";
        let cookie = session_cookie_header_value(sid, None, false);
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&cookie).unwrap(),
        );

        // No key configured should return the bare sid
        let got = verify_and_extract_session(&headers, &None);
        assert_eq!(got.as_deref(), Some(sid));
        tracing::info!("TEST END: web_utils::tests::test_session_cookie_without_key");
    }

    #[test]
    fn test_session_cookie_with_key_and_verify() {
        init_test_logging();
        tracing::info!("TEST START: web_utils::tests::test_session_cookie_with_key_and_verify");
        let sid = "sessionABC";
        let key = b"super-secret-key".to_vec();
        let cookie = session_cookie_header_value(sid, Some(key.as_slice()), true);
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&cookie).unwrap(),
        );

        // Correct key should validate and return sid
        let got = verify_and_extract_session(&headers, &Some(key.clone()));
        assert_eq!(got.as_deref(), Some(sid));

        // Tamper the sid but keep signature -> verification should fail
        let tampered = cookie.replacen(sid, "other", 1);
        let mut th = HeaderMap::new();
        th.insert(
            axum::http::header::COOKIE,
            HeaderValue::from_str(&tampered).unwrap(),
        );
        let got2 = verify_and_extract_session(&th, &Some(key));
        assert!(got2.is_none());
        tracing::info!("TEST END: web_utils::tests::test_session_cookie_with_key_and_verify");
    }
}
