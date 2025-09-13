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

// Test helpers for normalizing SurrealDB response shapes in tests.
// Surreal client may return Thing values as strings or objects depending on engine/client.
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
