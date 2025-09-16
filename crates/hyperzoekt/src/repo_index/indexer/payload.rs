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

use serde::Deserializer;
use serde::{Deserialize, Serialize};

// Helper to allow SurrealDB rows that contain `null` for array fields to deserialize
// into empty Vecs instead of failing with: expected a sequence, found null.
fn empty_vec_if_null<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    // First deserialize as an Option<Vec<T>>; map None (or null) to default empty vec.
    let opt = Option::<Vec<T>>::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

/// Typed in-library payloads reused by the binary to avoid JSON string churn.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportItem {
    pub path: String,
    pub line: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnresolvedImport {
    pub module: String,
    pub line: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MethodItem {
    pub name: String,
    pub visibility: Option<String>,
    pub signature: String,
    pub start_line: Option<u32>,
    pub end_line: Option<u32>,
    // Full text of this method's body (computed at index time when possible)
    pub source_content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityPayload {
    pub language: String,
    pub kind: String,
    pub name: String,
    pub parent: Option<String>,
    pub signature: String,
    pub start_line: Option<u32>,
    pub end_line: Option<u32>,
    pub doc: Option<String>,
    pub rank: Option<f32>,
    #[serde(default, deserialize_with = "empty_vec_if_null")]
    pub imports: Vec<ImportItem>,
    #[serde(default, deserialize_with = "empty_vec_if_null")]
    pub unresolved_imports: Vec<UnresolvedImport>,
    #[serde(default, deserialize_with = "empty_vec_if_null")]
    pub methods: Vec<MethodItem>,
    pub stable_id: String,
    pub repo_name: String,
    // Optional URL pointing to the source hosting (e.g. GitHub) for quick navigation from the UI
    pub source_url: Option<String>,
    /// Optional precomputed short display like "owner/repo/path" for UI link text
    pub source_display: Option<String>,
    // Raw callee names or stable_ids (best-effort) used by db_writer to create call edges.
    #[serde(default, deserialize_with = "empty_vec_if_null")]
    pub calls: Vec<String>,
    /// Full text used for embeddings. For functions/methods this is the function body text.
    pub source_content: Option<String>,
}

/// Compute a repository-relative path from a file path and repo name.
/// Shared helper used by the indexer to normalize paths before persistence.
pub fn compute_repo_relative(file_path: &str, repo_name: &str) -> String {
    // Normalize: remove leading / and split
    let mut parts: Vec<&str> = file_path.split('/').filter(|p| !p.is_empty()).collect();

    // Remove common clone roots like tmp/hyperzoekt-clones/<uuid>
    let mut i = 0usize;
    while i + 2 < parts.len() {
        if parts[i] == "tmp" && parts[i + 1] == "hyperzoekt-clones" {
            parts.drain(i..=i + 2);
            continue;
        }
        i += 1;
    }
    // Also remove standalone hyperzoekt-clones
    parts.retain(|p| *p != "hyperzoekt-clones");

    // Strip trailing uuid suffix from the leading segment if present
    if !parts.is_empty() {
        if let Some(first) = parts.first() {
            let leading = *first;
            if let Some(last_dash) = leading.rfind('-') {
                let potential_uuid = &leading[last_dash + 1..];
                let cleaned: String = potential_uuid.chars().filter(|c| *c != '-').collect();
                if cleaned.len() >= 32 && cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
                    // replace leading with portion before uuid
                    let repo_root = &leading[..last_dash];
                    parts[0] = repo_root;
                }
            }
        }
    }

    // Try to locate the repo_name segment
    let clean_repo = repo_name;
    for (idx, seg) in parts.iter().enumerate() {
        if *seg == clean_repo || seg.starts_with(&format!("{}-", clean_repo)) {
            if idx + 1 < parts.len() {
                return parts[idx + 1..].join("/");
            } else {
                return String::new();
            }
        }
    }

    // Strip common leading segments (uuid-like or tmp) until reasonable
    let mut start = 0usize;
    while start < parts.len() {
        let p = parts[start];
        if p == "tmp" || p == "hyperzoekt-clones" {
            start += 1;
            continue;
        }
        if p.len() >= 8 && p.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
            start += 1;
            continue;
        }
        break;
    }
    if start < parts.len() {
        return parts[start..].join("/");
    }

    // Fallback: drop first segment if multiple
    if parts.len() > 1 {
        return parts[1..].join("/");
    }

    file_path.to_string()
}
