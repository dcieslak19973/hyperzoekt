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

// Shared test utilities exposed from the library so integration tests can reuse them.
use serde_json::Value;

/// Try to extract a stable identifier string from a Surreal JSON value.
/// Handles: string ids, objects with `id` string, nested `in`/`out` Thing objects.
pub fn normalize_thing_id(v: &Value) -> Option<String> {
    if v.is_null() {
        return None;
    }
    if let Some(s) = v.as_str() {
        return Some(s.to_string());
    }
    if let Some(obj) = v.as_object() {
        // Direct id field as string
        if let Some(idv) = obj.get("id") {
            if let Some(s) = idv.as_str() {
                return Some(s.to_string());
            }
            // id may be nested object {tb:..., id: "..."}
            if let Some(inner) = idv.as_object() {
                if let Some(s2) = inner.get("id").and_then(|x| x.as_str()) {
                    return Some(s2.to_string());
                }
            }
        }
        // Some responses put the thing directly under `in`/`out`
        if let Some(inv) = obj.get("in") {
            if let Some(s) = normalize_thing_id(inv) {
                return Some(s);
            }
        }
        if let Some(outv) = obj.get("out") {
            if let Some(s) = normalize_thing_id(outv) {
                return Some(s);
            }
        }
        // Fallback: stringify and heuristically accept common id-like substrings
        let raw = v.to_string();
        if raw.contains("dependency:")
            || raw.contains("repo:")
            || raw.contains("entity:")
            || raw.contains("_rust_")
            || raw.contains("_id")
        {
            return Some(raw);
        }
    }
    None
}

/// Given a vector of rows (serde_json Values) and a field name, collect normalized ids
/// found in that field across all rows.
pub fn collect_field_ids(rows: &Vec<Value>, field: &str) -> Vec<String> {
    let mut out = Vec::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            if let Some(val) = obj.get(field) {
                if let Some(id) = normalize_thing_id(val) {
                    out.push(id);
                }
                // If the field is an array of things
                if val.is_array() {
                    for item in val.as_array().unwrap_or(&vec![]) {
                        if let Some(id2) = normalize_thing_id(item) {
                            out.push(id2);
                        }
                    }
                }
            }
        }
    }
    out
}

/// Normalize a SurrealDB URL/host string for tests.
/// Returns (schemeful_for_logging, host_no_scheme) where:
/// - schemeful_for_logging is guaranteed to start with http:// or https://
/// - host_no_scheme is the host:port (no scheme) form expected by some clients
pub fn normalize_surreal_host(u: &str) -> (String, String) {
    let mut s = u.trim().to_string();
    if s.starts_with("http//") {
        s = s.replacen("http//", "http://", 1);
    } else if s.starts_with("https//") {
        s = s.replacen("https//", "https://", 1);
    }
    let mut no_scheme = s.clone();
    if no_scheme.starts_with("http://") {
        no_scheme = no_scheme.replacen("http://", "", 1);
    } else if no_scheme.starts_with("https://") {
        no_scheme = no_scheme.replacen("https://", "", 1);
    }
    while s.ends_with('/') {
        s.pop();
    }
    while no_scheme.ends_with('/') {
        no_scheme.pop();
    }
    if !(s.starts_with("http://") || s.starts_with("https://")) {
        (format!("http://{}", s), no_scheme)
    } else {
        (s, no_scheme)
    }
}
