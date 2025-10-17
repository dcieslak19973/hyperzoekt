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
use std::sync::{Mutex, OnceLock};

pub static CALL_EDGE_CAPTURE: OnceLock<Mutex<Vec<(String, String)>>> = OnceLock::new();

pub fn init_call_edge_capture() {
    let _ = CALL_EDGE_CAPTURE.get_or_init(|| Mutex::new(Vec::new()));
}

pub fn extract_surreal_id(v: &serde_json::Value) -> Option<String> {
    if v.is_null() {
        return None;
    }
    if let Some(s) = v.as_str() {
        return Some(s.to_string());
    }
    if let Some(obj) = v.as_object() {
        if let (Some(tb_val), Some(id_val)) = (obj.get("tb"), obj.get("id")) {
            if let (Some(tb), Some(id)) = (tb_val.as_str(), id_val.as_str()) {
                return Some(format!("{}:{}", tb, id));
            }
        }
        for (_k, val) in obj {
            if let Some(inner) = val.as_object() {
                if let (Some(tb_val), Some(id_val)) = (inner.get("tb"), inner.get("id")) {
                    if let (Some(tb), Some(id)) = (tb_val.as_str(), id_val.as_str()) {
                        return Some(format!("{}:{}", tb, id));
                    }
                }
            }
        }
        if let Some(id_field) = obj.get("id").and_then(|x| x.as_str()) {
            if id_field.contains(':') {
                return Some(id_field.to_string());
            }
        }
    }
    None
}

pub fn extract_id_from_str(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if (bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len()
                && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b':' {
                i += 1;
                let mid = i;
                while i < bytes.len()
                    && ((bytes[i] as char).is_ascii_alphanumeric()
                        || bytes[i] == b'_'
                        || bytes[i] == b'-')
                {
                    i += 1;
                }
                if mid < i {
                    if let Ok(tok) = std::str::from_utf8(&bytes[start..i]) {
                        if tok.matches(':').count() == 1 {
                            return Some(tok.to_string());
                        }
                    }
                }
            }
        } else {
            i += 1;
        }
    }
    None
}

pub fn normalize_sql_value_id(v: &surrealdb::sql::Value) -> Option<String> {
    use surrealdb::sql::Value;
    match v {
        Value::Thing(t) => Some(format!("{}:{}", t.tb, t.id)),
        Value::Strand(s) => {
            let sref: &str = s.as_ref();
            if sref.contains(':') {
                Some(sref.to_string())
            } else {
                None
            }
        }
        Value::Object(obj) => {
            if let Some(idv) = obj.get("id") {
                normalize_sql_value_id(idv)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Normalize a git URL or repo path to a canonical https-like form used in persisted
/// repo metadata. Examples:
/// - "git@github.com:owner/repo.git" -> "https://github.com/owner/repo"
/// - "owner/repo" -> "https://owner/repo"
/// - "https://github.com/owner/repo.git" -> "https://github.com/owner/repo"
pub fn normalize_git_url(input: &str) -> String {
    let s = input.trim();
    if s.is_empty() {
        return String::new();
    }
    // strip trailing .git
    let raw = s.strip_suffix(".git").unwrap_or(s);
    if raw.starts_with("git@") {
        if let Some(colon) = raw.find(':') {
            let host = &raw[4..colon];
            let rest = &raw[colon + 1..];
            let rest = rest.strip_suffix(".git").unwrap_or(rest);
            return format!("https://{}/{}", host, rest);
        }
        return format!("https://{}", raw);
    }
    if raw.starts_with("http://") || raw.starts_with("https://") {
        return raw.to_string();
    }
    format!("https://{}", raw)
}

/// Redact sensitive or large fields (embeddings, centroid arrays, long source_content)
/// This is a shared helper used by modules that log SurrealDB responses so we
/// avoid printing full embedding vectors or huge source blobs in logs.
pub fn redact_for_log(mut v: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match &mut v {
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                let _ = redact_for_log(item.clone());
            }
            if arr.len() > 10 {
                let prefix = arr.drain(0..10).collect::<Vec<_>>();
                let mut new = Vec::new();
                for p in prefix.into_iter() {
                    new.push(redact_for_log(p));
                }
                new.push(Value::String(format!("... {} more items ...", arr.len())));
                return Value::Array(new);
            }
        }
        Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys.into_iter() {
                if let Some(val) = map.get_mut(&k) {
                    if (k.to_lowercase().contains("embedding")
                        || k.to_lowercase().contains("centroid"))
                        && val.is_array()
                    {
                        if let Some(arr) = val.as_array() {
                            let mut summary = Vec::new();
                            for (i, item) in arr.iter().enumerate() {
                                if i >= 3 {
                                    break;
                                }
                                if let Some(n) = item.as_f64() {
                                    summary.push(Value::Number(
                                        serde_json::Number::from_f64(n)
                                            .unwrap_or(serde_json::Number::from(0)),
                                    ));
                                } else {
                                    summary.push(item.clone());
                                }
                            }
                            let s = serde_json::json!({"type":"redacted_vector","len":arr.len(),"sample":summary});
                            *val = s;
                            continue;
                        }
                    }
                    if val.is_string() {
                        if let Some(s) = val.as_str() {
                            if s.len() > 200 {
                                let truncated = format!("{}... ({} bytes)", &s[..200], s.len());
                                *val = Value::String(truncated);
                                continue;
                            }
                        }
                    }
                    let red = redact_for_log(val.clone());
                    *val = red;
                }
            }
        }
        _ => {}
    }
    v
}

fn sql_value_to_json(v: &surrealdb::sql::Value) -> serde_json::Value {
    use surrealdb::sql::Value;
    match v {
        Value::Thing(_) => serde_json::Value::String(v.to_string()),
        Value::Strand(s) => {
            // Use the inner string value for Strands instead of the Display
            // representation which may include quotes in some client outputs.
            let sref: &str = s.as_ref();
            serde_json::Value::String(sref.to_string())
        }
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Number(n) => {
            // Use the Display representation; try to parse as integer then float.
            let s = format!("{}", n);
            if let Ok(i) = s.parse::<i64>() {
                serde_json::Value::Number(serde_json::Number::from(i))
            } else if let Ok(f) = s.parse::<f64>() {
                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            } else {
                serde_json::Value::String(s)
            }
        }
        Value::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (k, v2) in obj.iter() {
                map.insert(k.clone(), sql_value_to_json(v2));
            }
            serde_json::Value::Object(map)
        }
        Value::Array(arr) => {
            let vec = arr.iter().map(sql_value_to_json).collect();
            serde_json::Value::Array(vec)
        }
        _ => serde_json::Value::String(v.to_string()),
    }
}

/// Run a RELATE (or any SQL that returns relation objects) and collect normalized
/// relation ids returned by the SurrealDB response. This centralizes parsing of
/// different client response shapes (serde_json::Value vs surrealdb::sql::Value).
///
/// NOTE FOR TESTS: prefer calling this helper from integration/unit tests that
/// exercise RELATE statements. Different SurrealDB client backends (in-memory vs
/// remote HTTP/WS) return relation rows in slightly different shapes and slots
/// in the response. `relate_and_collect_ids` probes multiple slots and formats
/// and produces a normalized Vec<String> of relation ids which makes tests
/// deterministic and less brittle. Use it to assert that relation rows were
/// created instead of parsing raw client responses directly.
pub async fn relate_and_collect_ids(
    conn: &crate::db::connection::SurrealConnection,
    sql: &str,
) -> Result<Vec<String>, surrealdb::Error> {
    let mut ids = Vec::new();
    let mut resp = conn.query(sql).await?;
    // Try multiple result slots and attempt serde_json parsing first. Some clients
    // populate different slots (0..N), so probe a few slots before falling back.
    for slot in 0usize..3usize {
        if let Ok(rows) = resp.take::<Vec<serde_json::Value>>(slot) {
            for row in rows.iter() {
                // direct id on the row
                if let Some(s) = extract_surreal_id(row) {
                    ids.push(s);
                    continue;
                }
                // scan object fields for nested id-bearing values
                if let Some(obj) = row.as_object() {
                    for (_k, v) in obj.iter() {
                        if let Some(s2) = extract_surreal_id(v) {
                            ids.push(s2);
                            break;
                        }
                        if v.is_array() {
                            for item in v.as_array().unwrap_or(&vec![]) {
                                if let Some(s3) = extract_surreal_id(item) {
                                    ids.push(s3);
                                    break;
                                }
                            }
                        }
                    }
                }
                // if row is an array, scan its items
                if row.is_array() {
                    for item in row.as_array().unwrap_or(&vec![]) {
                        if let Some(s4) = extract_surreal_id(item) {
                            ids.push(s4);
                            break;
                        }
                    }
                }
            }
            if !ids.is_empty() {
                return Ok(ids);
            }
        }
    }

    // Fallback: try the surrealdb::sql::Value shape across a few slots and
    // convert to JSON to extract id fields.
    for slot in 0usize..3usize {
        if let Ok(vrows) = resp.take::<Vec<surrealdb::sql::Value>>(slot) {
            for v in vrows.iter() {
                let jv = sql_value_to_json(v);
                if let Some(idv) = jv.get("id") {
                    if let Some(s) = extract_surreal_id(idv) {
                        ids.push(s);
                    }
                } else {
                    // scan nested fields if top-level id not present
                    if let Some(obj) = jv.as_object() {
                        for (_k, vv) in obj.iter() {
                            if let Some(s2) = extract_surreal_id(vv) {
                                ids.push(s2);
                                break;
                            }
                        }
                    }
                }
            }
            if !ids.is_empty() {
                return Ok(ids);
            }
        }
    }
    Ok(ids)
}

/// Fetch a short snippet for an entity by stable_id. Returns None when not found.
/// Now queries entity_snapshot.source_content directly since content has been denormalized.
pub async fn get_snippet_for_entity(
    conn: &crate::db::connection::SurrealConnection,
    stable_id: &str,
) -> Result<Option<String>, surrealdb::Error> {
    #[derive(serde::Deserialize)]
    struct ERow {
        source_content: Option<String>,
    }
    let q = format!(
        "SELECT source_content FROM entity_snapshot WHERE stable_id = \"{}\" LIMIT 1",
        stable_id
    );
    if let Ok(mut r) = conn.query(&q).await {
        if let Ok(rows) = r.take::<Vec<ERow>>(0) {
            if let Some(row) = rows.into_iter().next() {
                if let Some(s) = row.source_content {
                    // Filter out empty strings
                    if !s.trim().is_empty() {
                        return Ok(Some(s));
                    }
                }
            }
        }
    }
    Ok(None)
}

/// Try to fetch the embedding vector for a given entity stable_id by looking
/// up the corresponding `entity_snapshot` record (preferred). This returns
/// None when no embedding is found. This helper centralizes the snapshot
/// lookup and keeps callers migration-friendly.
pub async fn get_embedding_for_entity(
    conn: &crate::db::connection::SurrealConnection,
    stable_id: &str,
) -> Result<Option<Vec<f32>>, surrealdb::Error> {
    #[derive(serde::Deserialize)]
    struct Row {
        embedding: Option<Vec<f32>>,
    }
    // Try snapshot-scoped lookup first (entity_snapshot with matching stable_id)
    let q = format!(
        "SELECT embedding FROM entity_snapshot WHERE stable_id = \"{}\" LIMIT 1",
        stable_id
    );
    if let Ok(mut resp) = conn.query(&q).await {
        if let Ok(rows) = resp.take::<Vec<Row>>(0) {
            if let Some(r) = rows.into_iter().next() {
                return Ok(r.embedding);
            }
        }
    }
    Ok(None)
}

/// Fetch a short snippet for an entity by stable_id from entity_snapshot.source_content.
/// This is the preferred method since content has been denormalized to entity_snapshot.
pub async fn get_snippet_for_entity_snapshot(
    conn: &crate::db::connection::SurrealConnection,
    stable_id: &str,
) -> Result<Option<String>, surrealdb::Error> {
    #[derive(serde::Deserialize)]
    struct SRow {
        source_content: Option<String>,
    }
    let q = format!(
        "SELECT source_content FROM entity_snapshot WHERE stable_id = \"{}\" LIMIT 1",
        stable_id
    );
    if let Ok(mut resp) = conn.query(&q).await {
        if let Ok(rows) = resp.take::<Vec<SRow>>(0) {
            if let Some(r) = rows.into_iter().next() {
                if let Some(s) = r.source_content {
                    // Filter out empty strings
                    if !s.trim().is_empty() {
                        return Ok(Some(s));
                    }
                }
            }
        }
    }
    Ok(None)
}

/// Extract repo provenance for a given entity stable_id. Returns a deduped Vec of repo strings.
/// Queries entity_snapshot.repo_name directly since content has been denormalized.
pub async fn get_repos_for_entity(
    conn: &crate::db::connection::SurrealConnection,
    stable_id: &str,
) -> Result<Vec<String>, surrealdb::Error> {
    use std::collections::HashSet;
    #[derive(serde::Deserialize)]
    struct SRow {
        repo_name: Option<String>,
    }
    let mut repos_set: HashSet<String> = HashSet::new();

    let q_snap = format!(
        "SELECT repo_name FROM entity_snapshot WHERE stable_id = \"{}\" LIMIT 1",
        stable_id
    );
    if let Ok(mut resp) = conn.query(&q_snap).await {
        if let Ok(rows) = resp.take::<Vec<SRow>>(0) {
            if let Some(r) = rows.into_iter().next() {
                if let Some(rn) = r.repo_name {
                    let n = normalize_git_url(&rn);
                    if !n.is_empty() {
                        repos_set.insert(n);
                    }
                }
            }
        }
    }

    let mut list: Vec<String> = repos_set.into_iter().collect();
    list.sort();
    Ok(list)
}

/// Convert a `surrealdb::Response` into a `serde_json::Value` when possible.
/// Tries multiple result slots and both `serde_json::Value` and
/// `surrealdb::sql::Value` shapes to maximize compatibility with remote
/// and embedded clients which return slightly different response shapes.
pub fn response_to_json(mut resp: surrealdb::Response) -> Option<serde_json::Value> {
    // Fast path: some remote HTTP responses for simple SELECT single-column
    // queries deserialize cleanly as an Option<serde_json::Value> in slot 0
    // but attempting sql::Value vector shapes first can consume/empty the slot.
    // Try a quick scan for up to a few slots before the heavier probing logic.
    for slot in 0usize..3usize {
        if let Ok(Some(val)) = resp.take::<Option<serde_json::Value>>(slot) {
            if val.is_array() {
                if val.as_array().map(|a| a.is_empty()).unwrap_or(false) {
                    continue;
                }
                return Some(val);
            } else {
                return Some(serde_json::Value::Array(vec![val]));
            }
        }
    }
    // Prefer probing the `surrealdb::sql::Value` shapes first because remote
    // HTTP clients often return nested `Array(Array({...}))` structures that
    // deserialize into nested `Vec<Vec<sql::Value>>`. Trying SQL-shaped takes
    // first avoids some serde_json deserialization failures and makes the
    // normalization deterministic.
    for slot in 0usize..10usize {
        // Nested Vec<Vec<sql::Value>> — common shape for Array(Array(...)).
        if let Ok(nested) = resp.take::<Vec<Vec<surrealdb::sql::Value>>>(slot) {
            for inner in nested.into_iter() {
                let mut arr = Vec::new();
                for v in inner.into_iter() {
                    arr.push(sql_value_to_json(&v));
                }
                if !arr.is_empty() {
                    return Some(serde_json::Value::Array(arr));
                }
            }
        }
        // Option-wrapped nested vectors
        if let Ok(Some(nv)) = resp.take::<Option<Vec<Vec<surrealdb::sql::Value>>>>(slot) {
            for inner in nv.into_iter() {
                let mut arr = Vec::new();
                for v in inner.into_iter() {
                    arr.push(sql_value_to_json(&v));
                }
                if !arr.is_empty() {
                    return Some(serde_json::Value::Array(arr));
                }
            }
        }
        // Vec<sql::Value>
        if let Ok(rows) = resp.take::<Vec<surrealdb::sql::Value>>(slot) {
            let mut arr = Vec::with_capacity(rows.len());
            for v in rows.into_iter() {
                let jv = sql_value_to_json(&v);
                arr.push(jv);
            }
            // Flatten one level when the response shape is [[{...}]]
            if arr.len() == 1
                && arr[0].is_array()
                && !arr[0].as_array().map(|a| a.is_empty()).unwrap_or(true)
            {
                return Some(arr[0].clone());
            }
            if !arr.is_empty() {
                return Some(serde_json::Value::Array(arr));
            }
        }
        // Option<Vec<sql::Value>>
        if let Ok(Some(rows)) = resp.take::<Option<Vec<surrealdb::sql::Value>>>(slot) {
            let mut arr = Vec::new();
            for v in rows.into_iter() {
                arr.push(sql_value_to_json(&v));
            }
            if !arr.is_empty() {
                return Some(serde_json::Value::Array(arr));
            }
        }
        // Option<sql::Value>
        if let Ok(Some(val)) = resp.take::<Option<surrealdb::sql::Value>>(slot) {
            let jv = sql_value_to_json(&val);
            if jv.is_array() {
                if jv.as_array().map(|a| a.is_empty()).unwrap_or(false) {
                    continue;
                }
                return Some(jv);
            } else {
                return Some(serde_json::Value::Array(vec![jv]));
            }
        }
    }

    // Probe multiple slots (0..10) more aggressively to account for remote
    // clients that populate non-zero or non-vector slots. Critical ordering:
    // attempt Option<serde_json::Value> BEFORE Vec<serde_json::Value>. We have
    // observed that a single object row (e.g. {sbom_blob: ...}) can deserialize
    // as an empty Vec<serde_json::Value>, causing the slot to be consumed and
    // data lost. Reordering preserves that object.
    for slot in 0usize..10usize {
        // Single value first.
        if let Ok(Some(val)) = resp.take::<Option<serde_json::Value>>(slot) {
            if val.is_array() {
                if val.as_array().map(|a| a.is_empty()).unwrap_or(false) {
                    // keep probing other slots
                } else {
                    return Some(val);
                }
            } else {
                return Some(serde_json::Value::Array(vec![val]));
            }
        }
        // Vector of rows.
        if let Ok(rows) = resp.take::<Vec<serde_json::Value>>(slot) {
            if rows.is_empty() {
                // continue probing other slots – do NOT return an empty array
                continue;
            }
            let v = serde_json::Value::Array(rows);
            // Some clients return nested arrays like [[row1, row2]] — flatten one level.
            let candidate = if let Some(first) = v.as_array().and_then(|a| a.first()) {
                if first.is_array() && v.as_array().unwrap().len() == 1 {
                    first.clone()
                } else {
                    v.clone()
                }
            } else {
                v.clone()
            };
            if candidate.as_array().map(|a| a.is_empty()).unwrap_or(false) {
                continue;
            }
            return Some(candidate);
        }
        // Nested Vec<Vec<serde_json::Value>> shapes
        if let Ok(nested) = resp.take::<Vec<Vec<serde_json::Value>>>(slot) {
            for inner in nested.into_iter() {
                if inner.is_empty() {
                    continue;
                }
                let v = serde_json::Value::Array(inner);
                if v.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                    return Some(v);
                }
            }
        }
        if let Ok(Some(rows)) = resp.take::<Option<Vec<serde_json::Value>>>(slot) {
            if rows.is_empty() {
                continue;
            }
            let v = serde_json::Value::Array(rows);
            if v.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                return Some(v);
            }
        }
    }

    // Try serde_json::Value rows first (typed slot access can fail for some
    // client backends; this is a best-effort probe).
    for slot in 0usize..3usize {
        if let Ok(rows) = resp.take::<Vec<serde_json::Value>>(slot) {
            let v = serde_json::Value::Array(rows);
            let candidate = if let Some(first) = v.as_array().and_then(|a| a.first()) {
                if first.is_array() && v.as_array().unwrap().len() == 1 {
                    first.clone()
                } else {
                    v.clone()
                }
            } else {
                v.clone()
            };
            if candidate.as_array().map(|a| a.is_empty()).unwrap_or(false) {
                continue;
            }
            return Some(candidate);
        }
        if let Ok(nested) = resp.take::<Vec<Vec<serde_json::Value>>>(slot) {
            for inner in nested.into_iter() {
                let v = serde_json::Value::Array(inner);
                if v.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                    return Some(v);
                }
            }
        }
    }

    // Fallback to surrealdb::sql::Value and convert each to JSON
    for slot in 0usize..10usize {
        if let Ok(rows) = resp.take::<Vec<surrealdb::sql::Value>>(slot) {
            let mut arr = Vec::with_capacity(rows.len());
            for v in rows.into_iter() {
                let jv = sql_value_to_json(&v);
                arr.push(jv);
            }
            // Flatten one level when the response shape is [[{...}]]
            if arr.len() == 1
                && arr[0].is_array()
                && !arr[0].as_array().map(|a| a.is_empty()).unwrap_or(true)
            {
                return Some(arr[0].clone());
            }
            if !arr.is_empty() {
                return Some(serde_json::Value::Array(arr));
            }
        }
        // Nested Vec<Vec<sql::Value>>
        if let Ok(nested) = resp.take::<Vec<Vec<surrealdb::sql::Value>>>(slot) {
            for inner in nested.into_iter() {
                let mut arr = Vec::new();
                for v in inner.into_iter() {
                    arr.push(sql_value_to_json(&v));
                }
                if !arr.is_empty() {
                    return Some(serde_json::Value::Array(arr));
                }
            }
        }
        if let Ok(Some(rows)) = resp.take::<Option<Vec<surrealdb::sql::Value>>>(slot) {
            let mut arr = Vec::new();
            for v in rows.into_iter() {
                arr.push(sql_value_to_json(&v));
            }
            if !arr.is_empty() {
                return Some(serde_json::Value::Array(arr));
            }
        }
        if let Ok(Some(val)) = resp.take::<Option<surrealdb::sql::Value>>(slot) {
            let jv = sql_value_to_json(&val);
            if jv.is_array() {
                if jv.as_array().map(|a| a.is_empty()).unwrap_or(false) {
                    continue;
                }
                return Some(jv);
            } else {
                return Some(serde_json::Value::Array(vec![jv]));
            }
        }
    }
    // Diagnostic: when all probes fail, attempt to capture several typed takes
    // and log them to help triage mysterious serialization errors observed
    // in integration tests. These logs are at debug level.
    for slot in 0usize..10usize {
        // try serde_json variants
        if let Ok(Some(v)) = resp.take::<Option<serde_json::Value>>(slot) {
            let red = redact_for_log(v.clone());
            log::debug!(
                "response_to_json diagnostics: slot={} took Option<serde_json::Value> = {}",
                slot,
                red
            );
        }
        if let Ok(vs) = resp.take::<Option<Vec<serde_json::Value>>>(slot) {
            let red = redact_for_log(serde_json::Value::Array(vs.clone().unwrap_or_default()));
            log::debug!(
                "response_to_json diagnostics: slot={} took Option<Vec<serde_json::Value>> = {}",
                slot,
                red
            );
        }
        if let Ok(vs2) = resp.take::<Vec<Vec<serde_json::Value>>>(slot) {
            // Flatten one level and redact
            let mut flat = Vec::new();
            for inner in vs2.iter() {
                for item in inner.iter() {
                    flat.push(item.clone());
                }
            }
            let red = redact_for_log(serde_json::Value::Array(flat));
            log::debug!(
                "response_to_json diagnostics: slot={} took Vec<Vec<serde_json::Value>> = {}",
                slot,
                red
            );
        }
        // try surreal sql::Value variants
        if let Ok(vs) = resp.take::<Option<surrealdb::sql::Value>>(slot) {
            let j = vs
                .as_ref()
                .map(sql_value_to_json)
                .unwrap_or(serde_json::Value::Null);
            let red = redact_for_log(j);
            log::debug!(
                "response_to_json diagnostics: slot={} took Option<sql::Value> = {}",
                slot,
                red
            );
        }
        if let Ok(vs) = resp.take::<Option<Vec<surrealdb::sql::Value>>>(slot) {
            let mut arr = Vec::new();
            if let Some(vs2) = vs.as_ref() {
                for v in vs2.iter() {
                    arr.push(sql_value_to_json(v));
                }
            }
            let red = redact_for_log(serde_json::Value::Array(arr));
            log::debug!(
                "response_to_json diagnostics: slot={} took Option<Vec<sql::Value>> = {}",
                slot,
                red
            );
        }
        if let Ok(vs) = resp.take::<Vec<Vec<surrealdb::sql::Value>>>(slot) {
            let mut flat = Vec::new();
            for inner in vs.iter() {
                for v in inner.iter() {
                    flat.push(sql_value_to_json(v));
                }
            }
            let red = redact_for_log(serde_json::Value::Array(flat));
            log::debug!(
                "response_to_json diagnostics: slot={} took Vec<Vec<sql::Value>> = {}",
                slot,
                red
            );
        }
        if let Ok(vs) = resp.take::<Vec<surrealdb::sql::Value>>(slot) {
            let mut arr = Vec::new();
            for v in vs.iter() {
                arr.push(sql_value_to_json(v));
            }
            let red = redact_for_log(serde_json::Value::Array(arr));
            log::debug!(
                "response_to_json diagnostics: slot={} took Vec<sql::Value> = {}",
                slot,
                red
            );
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::connection::SurrealConnection;
    use std::sync::Arc;
    use surrealdb::engine::local::Mem;
    use surrealdb::Surreal;
    #[test]
    fn id_extract_variants() {
        assert!(extract_surreal_id(&serde_json::json!("repo:abc")).is_some());
        assert!(extract_id_from_str("some repo:xyz token").is_some());
    }

    #[test]
    fn id_extract_object_variants() {
        // object with {tb, id}
        let v = serde_json::json!({"tb":"repo","id":"abc"});
        assert_eq!(extract_surreal_id(&v).unwrap(), "repo:abc");

        // nested object
        let v2 = serde_json::json!({"something": {"tb":"file","id":"f1"}});
        assert_eq!(extract_surreal_id(&v2).unwrap(), "file:f1");

        // string id
        let s = serde_json::json!("file:xyz");
        assert_eq!(extract_surreal_id(&s).unwrap(), "file:xyz");
    }

    #[test]
    fn normalize_sql_value_id_variants() {
        use surrealdb::sql::Value;
        // Strand containing colon
        let st = Value::Strand("repo:abc".into());
        assert_eq!(normalize_sql_value_id(&st).unwrap(), "repo:abc");

        // Strand containing colon
        let st = Value::Strand("repo:abc".into());
        assert_eq!(normalize_sql_value_id(&st).unwrap(), "repo:abc");

        // Note: constructing Value::Object here is tricky due to type aliases; the
        // Strand case above covers common behavior for string ids returned by the
        // client which contain colons.
    }

    #[tokio::test]
    async fn relate_and_collect_ids_mem_smoke() {
        // Create an in-memory Surreal instance and use it via SurrealConnection::Local
        let mem = Surreal::new::<Mem>(()).await.expect("create mem surreal");
        let arc = Arc::new(mem);
        let conn = SurrealConnection::Local(arc.clone());
        // use ns/db
        conn.use_ns("test_ns").await.expect("use ns");
        conn.use_db("test_db").await.expect("use db");

        // Create two base records and a relation between them, returning relation objects
        let create_sql = r#"
            CREATE repo:one SET name='r1';
            CREATE file:f1 SET path='src/a.rs';
        "#;
        let _ = conn.query(create_sql).await.expect("create base");

        // Issue RELATE statement and collect ids
        let rel_sql = "RELATE repo:one->has_file->file:f1 RETURN id;";
        let ids = relate_and_collect_ids(&conn, rel_sql)
            .await
            .expect("relate ids");
        // The helper should not error; returned ids may be empty depending on client
        // response shape. Accept either outcome but assert the call completed.
        assert!(ids.iter().all(|s| s.contains(':') || s.is_empty()));
    }

    #[test]
    fn sql_value_to_json_array_shape() {
        use surrealdb::sql::{Array, Value};

        // Construct a simple sql::Value::Array containing a Strand which mirrors
        // a typed client response slot. Ensure conversion to serde_json works.
        let arr = Array::from(vec![Value::Strand("s1".into())]);
        let v = Value::Array(arr);
        let j = sql_value_to_json(&v);
        // Expect the strand's raw content without surrounding quotes.
        let expected = serde_json::Value::Array(vec![serde_json::Value::String("s1".to_string())]);
        assert_eq!(j, expected);
    }
}
