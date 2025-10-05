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

use ammonia;
use pulldown_cmark;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url;

/// Summary information for a repository
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoSummary {
    pub name: String,
    pub entity_count: u64,
    pub file_count: u64,
    pub languages: Vec<String>,
}

/// Query result for repository summaries from SurrealDB
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoSummaryQueryResult {
    pub repo_name: String,
    pub entity_count: u64,
    // SurrealDB arrays may contain nulls if some entities have missing file/language.
    // Accept Option<String> here and filter out None values when constructing
    // the public `RepoSummary` to avoid deserialization errors like
    // "expected a string, found None".
    pub files: Vec<Option<String>>,
    pub languages: Vec<Option<String>>,
}

/// Generate a stable ID for an entity based on its identifying characteristics.
/// This ensures consistent entity identification across different indexing runs.
///
/// The stable ID is generated using SHA256 hash of the following format:
/// `{project}|{repo}|{branch}|{commit}|{file_path}|{entity_name}|{entity_signature}`
pub fn generate_stable_id(
    project: &str,
    repo: &str,
    branch: &str,
    commit: &str,
    file_path: &str,
    entity_name: &str,
    entity_signature: &str,
) -> String {
    let key = format!(
        "{}|{}|{}|{}|{}|{}|{}",
        project, repo, branch, commit, file_path, entity_name, entity_signature
    );
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Render markdown to sanitized HTML using pulldown-cmark -> ammonia.
pub fn render_markdown_to_html(md: &str) -> String {
    if md.is_empty() {
        return String::new();
    }
    let mut opts = pulldown_cmark::Options::empty();
    opts.insert(pulldown_cmark::Options::ENABLE_STRIKETHROUGH);
    opts.insert(pulldown_cmark::Options::ENABLE_TABLES);
    opts.insert(pulldown_cmark::Options::ENABLE_FOOTNOTES);
    let parser = pulldown_cmark::Parser::new_ext(md, opts);
    let mut html_out = String::new();
    pulldown_cmark::html::push_html(&mut html_out, parser);

    // Sanitize the generated HTML to avoid XSS
    ammonia::Builder::default().clean(&html_out).to_string()
}

/// Clean up file paths to show repository-relative paths instead of absolute filesystem paths
/// Converts paths like "/tmp/hyperzoekt-clones/repo-uuid/path/to/file.rs" to "repo/path/to/file.rs"
pub fn clean_file_path(file_path: &str) -> String {
    // Robust cleaning of file paths to handle many variants. Strategy:
    //  - Split into path segments
    //  - Remove any embedded /tmp/hyperzoekt-clones/<uuid>/ sequences
    //  - For any segment that ends with a UUID-like suffix (repo-name-<uuid>), strip the suffix
    //  - Rejoin segments
    let mut parts: Vec<String> = file_path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    // Remove sequences like tmp -> hyperzoekt-clones -> <uuid>
    let mut i = 0;
    while i + 2 < parts.len() {
        if parts[i] == "tmp" && parts[i + 1] == "hyperzoekt-clones" {
            // Remove tmp and hyperzoekt-clones and the following uuid-like segment
            parts.drain(i..=i + 2);
            // continue without incrementing i
            continue;
        }
        i += 1;
    }

    // Also handle the variant where only "hyperzoekt-clones" appears
    let mut j = 0;
    while j < parts.len() {
        if parts[j] == "hyperzoekt-clones" {
            // remove the hyperzoekt-clones and possibly next uuid segment
            if j + 1 < parts.len() {
                parts.drain(j..=j + 1);
            } else {
                parts.remove(j);
            }
            continue;
        }
        j += 1;
    }

    // Strip UUID suffixes from any segment like name-8e9834cb-6abb-...-deadbeef
    for seg in parts.iter_mut() {
        if let Some(last_dash) = seg.rfind('-') {
            let potential_uuid = &seg[last_dash + 1..];
            let cleaned: String = potential_uuid.chars().filter(|c| *c != '-').collect();
            if cleaned.len() >= 32 && cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
                *seg = seg[..last_dash].to_string();
            }
        }
    }

    // Reconstruct path
    if parts.is_empty() {
        return file_path.to_string();
    }
    parts.join("/")
}

/// Remove a trailing UUID-style suffix from names like "repo-name-8e9834cb-6abb-4381-90f4-deadbeefdead"
pub fn remove_uuid_suffix(name: &str) -> String {
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() >= 6 {
        let tail = &parts[parts.len() - 5..];
        let lens: [usize; 5] = [8, 4, 4, 4, 12];
        let mut ok = true;
        for (i, seg) in tail.iter().enumerate() {
            if seg.len() != lens[i] || !seg.chars().all(|c| c.is_ascii_hexdigit()) {
                ok = false;
                break;
            }
        }
        if ok {
            return parts[..parts.len() - 5].join("-");
        }
    }
    name.to_string()
}

/// Extract the repo name with UUID suffix from a file path
/// E.g., "/tmp/hyperzoekt-clones/repo-uuid/file" -> "repo-uuid"
pub fn extract_uuid_repo_name(file_path: &str) -> Option<String> {
    let parts: Vec<&str> = file_path.split('/').filter(|s| !s.is_empty()).collect();
    for (i, part) in parts.iter().enumerate() {
        if *part == "hyperzoekt-clones" && i + 1 < parts.len() {
            let next = parts[i + 1];
            // Check if it looks like name-uuid
            if let Some(last_dash) = next.rfind('-') {
                let potential_uuid = &next[last_dash + 1..];
                let cleaned: String = potential_uuid.chars().filter(|c| *c != '-').collect();
                if cleaned.len() >= 32 && cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
                    return Some(next.to_string());
                }
            }
        }
    }
    None
}

/// Compute a repository-relative path from a (cleaned) file path and a clean repo name.
/// Attempts to find the repo-name segment and return the remaining path. If not found,
/// strips known clone roots and returns a reasonable relative path.
pub fn repo_relative_from_file(file_path: &str, clean_repo_name: &str) -> String {
    let normalized = clean_file_path(file_path);
    let parts: Vec<&str> = normalized.split('/').filter(|p| !p.is_empty()).collect();

    // Try to find a segment that equals the repo name or a variant
    for (i, p) in parts.iter().enumerate() {
        if *p == clean_repo_name
            || p.starts_with(&format!("{}-", clean_repo_name))
            || *p == clean_repo_name.replace('-', "_")
        {
            if i + 1 < parts.len() {
                return parts[i + 1..].join("/");
            } else {
                return String::new();
            }
        }
    }

    // If the path begins with known clone roots, strip them
    let mut start = 0usize;
    while start < parts.len() {
        let p = parts[start];
        if p == "tmp" || p == "hyperzoekt-clones" {
            start += 1;
            continue;
        }
        // UUID-like segment (hex groups)
        if p.len() >= 8 && p.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
            start += 1;
            continue;
        }
        break;
    }

    if start < parts.len() {
        return parts[start..].join("/");
    }

    // Fallback: if more than one segment, drop the first and return the rest
    if parts.len() > 1 {
        return parts[1..].join("/");
    }

    normalized
}

/// Normalize various git URL forms into an https base URL without a trailing `.git`.
/// Examples:
/// - git@github.com:owner/repo.git -> https://github.com/owner/repo
/// - https://github.com/owner/repo.git -> https://github.com/owner/repo
/// - http://... -> http://...
///   Returns None for local paths (file:// or absolute filesystem paths) where a web URL
///   cannot be constructed.
pub fn normalize_git_url(git_url: &str) -> Option<String> {
    if git_url.is_empty() {
        return None;
    }

    // Local paths are not convertible to web URLs
    if git_url.starts_with("file://") || std::path::Path::new(git_url).is_absolute() {
        return None;
    }

    // SSH style: git@host:owner/repo.git -> https://host/owner/repo
    if git_url.starts_with("git@") {
        if let Some(colon) = git_url.find(':') {
            let host = &git_url[4..colon]; // strip "git@"
            let rest = &git_url[colon + 1..];
            let s = format!("https://{}/{}", host, rest.trim_end_matches(".git"));
            return Some(s);
        }
    }

    // HTTP/HTTPS style: strip trailing .git
    if git_url.starts_with("http://") || git_url.starts_with("https://") {
        return Some(git_url.trim_end_matches(".git").to_string());
    }

    // Fallback: try to treat as https host/path
    Some(git_url.trim_end_matches(".git").to_string())
}

/// Derive a short display string like "owner/repo/path/to/file" from a full source URL
pub fn short_display_from_source_url(source_url: &str, repo_name: &str) -> String {
    // Expected forms:
    //  - https://github.com/owner/repo/blob/branch/path/to/file
    //  - https://gitlab.com/owner/repo/-/blob/branch/path/to/file
    // Fallback: if we can't parse, return repo_name + "/" + trailing path from source_url
    if source_url.is_empty() {
        return repo_name.to_string();
    }

    if let Ok(u) = url::Url::parse(source_url) {
        let segments: Vec<String> = u
            .path()
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        // Look for 'blob' or '-/blob' markers and take the pieces after branch
        if let Some(pos) = segments.iter().position(|s| s == "blob") {
            // segments: owner, repo, blob, branch, rest... (GitHub style)
            // Avoid matching the GitLab variant where the segment before 'blob' is '-' (owner/repo/-/blob/...)
            if pos >= 2 && segments[pos - 1] != "-" {
                let owner = &segments[pos - 2];
                let repo = &segments[pos - 1];
                let rest = if pos + 2 <= segments.len() - 1 {
                    segments[pos + 2..].join("/")
                } else {
                    String::new()
                };
                if rest.is_empty() {
                    return format!("{}/{}", owner, repo);
                }
                return format!("{}/{}/{}", owner, repo, rest);
            }
        }
        // GitLab variant: owner/repo/-/blob/branch/path
        if let Some(pos) = segments.iter().position(|s| s == "-") {
            if pos + 2 < segments.len() && segments[pos + 1] == "blob" && pos >= 2 {
                let owner = &segments[pos - 2];
                let repo = &segments[pos - 1];
                let rest = segments[pos + 3..].join("/");
                return format!("{}/{}/{}", owner, repo, rest);
            }
        }
    }

    // fallback: try to strip the repo_name from the path
    if let Some(pos) = source_url.find(repo_name) {
        let tail = &source_url[pos + repo_name.len()..];
        let tail = tail.trim_start_matches('/');
        if tail.is_empty() {
            return repo_name.to_string();
        }
        return format!("{}/{}", repo_name, tail);
    }

    // last resort: return the hostname + path
    source_url.to_string()
}

/// Compute a file-like hint for an entity. Prefer `source_display`, fall back to
/// deriving from `source_url`, otherwise return empty string.
pub fn file_hint_for_entity(entity: &crate::repo_index::indexer::payload::EntityPayload) -> String {
    if let Some(sd) = &entity.source_display {
        if !sd.is_empty() {
            return sd.clone();
        }
    }
    if let Some(url) = &entity.source_url {
        let short = short_display_from_source_url(url, &entity.repo_name);
        if !short.is_empty() {
            return short;
        }
    }
    String::new()
}

/// Extract SurrealDB record ID from a JSON value, handling various formats.
/// Returns None if the value is null or cannot be parsed as an ID.
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
        if let Some(id_field) = obj.get("id").and_then(|x| x.as_str()) {
            if id_field.contains(':') {
                return Some(id_field.to_string());
            }
        }
    }
    None
}

/// Resolve a human ref name (e.g. "main" or "refs/heads/main") to a commit id
/// and attempt to find a matching `snapshot_meta` row. Returns (repo_filter, snapshot_id_opt).
/// If raw_ref is None, attempts to resolve the repository's default branch.
/// repo_filter is always Some(repo), snapshot_id_opt is resolved if possible.
pub async fn resolve_ref_to_snapshot(
    db: &crate::db::connection::SurrealConnection,
    repo: &str,
    raw_ref: Option<&str>,
) -> Result<
    (Option<String>, Option<String>, Option<String>),
    Box<dyn std::error::Error + Send + Sync>,
> {
    // Determine what ref(s) to try resolving. If the caller provided a raw_ref,
    // only try that. Otherwise try the repo's configured default branch; if
    // that's missing, fall back to some common default branch names.
    let repo_filter = repo.to_string();
    let mut initial_refs: Vec<String> = Vec::new();

    if let Some(raw) = raw_ref {
        initial_refs.push(raw.to_string());
    } else {
        // No explicit ref, try to resolve default branch from the repo table
        #[derive(Deserialize)]
        struct BranchRow {
            branch: Option<String>,
        }
        let sql = "SELECT branch FROM repo WHERE name = $name LIMIT 1";
        let mut res = match db {
            crate::db::connection::SurrealConnection::Local(db_conn) => {
                db_conn.query(sql).bind(("name", repo.to_string())).await?
            }
            crate::db::connection::SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(sql).bind(("name", repo.to_string())).await?
            }
            crate::db::connection::SurrealConnection::RemoteWs(db_conn) => {
                db_conn.query(sql).bind(("name", repo.to_string())).await?
            }
        };
        let rows: Vec<BranchRow> = res.take(0)?;
        if let Some(br) = rows.into_iter().next().and_then(|r| r.branch) {
            initial_refs.push(br);
        } else {
            // No configured default branch; try a shortlist of common defaults.
            initial_refs.extend(["main", "master", "trunk"].iter().map(|s| s.to_string()));
        }
    }

    #[derive(Deserialize)]
    struct RefRow {
        name: String,
        target: String,
    }

    // Expand initial_refs into the forms we store in the refs table: raw,
    // refs/heads/<name>, and refs/tags/<name>. Preserve order and avoid
    // duplicates.
    let mut candidates: Vec<String> = Vec::new();
    for r in initial_refs.iter() {
        let forms = [
            r.clone(),
            format!("refs/heads/{}", r),
            format!("refs/tags/{}", r),
        ];
        for f in forms.iter() {
            if !candidates.contains(f) {
                candidates.push(f.clone());
            }
        }
    }
    // Query refs by repo and candidate names
    let sql = format!(
        "SELECT name, target FROM refs WHERE repo = $repo AND name IN [{}]",
        candidates
            .iter()
            .enumerate()
            .map(|(i, _)| format!("$n{}", i))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut res = match db {
        crate::db::connection::SurrealConnection::Local(db_conn) => {
            let mut q = db_conn.query(&sql);
            q = q.bind(("repo", repo.to_string()));
            for (i, v) in candidates.iter().enumerate() {
                q = q.bind((format!("n{}", i), v.to_string()));
            }
            q.await?
        }
        crate::db::connection::SurrealConnection::RemoteHttp(db_conn) => {
            let mut q = db_conn.query(&sql);
            q = q.bind(("repo", repo.to_string()));
            for (i, v) in candidates.iter().enumerate() {
                q = q.bind((format!("n{}", i), v.to_string()));
            }
            q.await?
        }
        crate::db::connection::SurrealConnection::RemoteWs(db_conn) => {
            let mut q = db_conn.query(&sql);
            q = q.bind(("repo", repo.to_string()));
            for (i, v) in candidates.iter().enumerate() {
                q = q.bind((format!("n{}", i), v.to_string()));
            }
            q.await?
        }
    };
    let rows: Vec<RefRow> = res.take(0)?;
    if rows.is_empty() {
        return Ok((Some(repo_filter), None, None));
    }

    // prefer the order of initial_refs; for each candidate ref name try exact,
    // then refs/heads/<name>, then refs/tags/<name>
    let mut commit_opt: Option<String> = None;
    'outer: for name in initial_refs.iter() {
        // exact match
        if let Some(r) = rows.iter().find(|r| r.name == *name) {
            commit_opt = Some(r.target.clone());
            break 'outer;
        }
        let heads = format!("refs/heads/{}", name);
        if let Some(r) = rows.iter().find(|r| r.name == heads) {
            commit_opt = Some(r.target.clone());
            break 'outer;
        }
        let tags = format!("refs/tags/{}", name);
        if let Some(r) = rows.iter().find(|r| r.name == tags) {
            commit_opt = Some(r.target.clone());
            break 'outer;
        }
    }

    if let Some(commit) = commit_opt {
        // lookup snapshot_meta
        let snap_sql =
            "SELECT id FROM snapshot_meta WHERE repo = $repo AND commit = $commit LIMIT 1";
        let mut r2 = match db {
            crate::db::connection::SurrealConnection::Local(db_conn) => {
                let mut q2 = db_conn.query(snap_sql);
                q2 = q2
                    .bind(("repo", repo.to_string()))
                    .bind(("commit", commit.clone()));
                q2.await?
            }
            crate::db::connection::SurrealConnection::RemoteHttp(db_conn) => {
                let mut q2 = db_conn.query(snap_sql);
                q2 = q2
                    .bind(("repo", repo.to_string()))
                    .bind(("commit", commit.clone()));
                q2.await?
            }
            crate::db::connection::SurrealConnection::RemoteWs(db_conn) => {
                let mut q2 = db_conn.query(snap_sql);
                q2 = q2
                    .bind(("repo", repo.to_string()))
                    .bind(("commit", commit.clone()));
                q2.await?
            }
        };
        #[derive(Deserialize)]
        struct SnapRow {
            id: Option<String>,
        }
        if let Ok(rows2) = r2.take::<Vec<SnapRow>>(0) {
            if let Some(s) = rows2.into_iter().next() {
                // Return repo filter, commit, and snapshot id
                return Ok((Some(repo_filter), Some(commit.clone()), s.id));
            }
        }
        // Commit found but no snapshot
        return Ok((Some(repo_filter), Some(commit.clone()), None));
    }

    Ok((Some(repo_filter), None, None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_stable_id_consistency() {
        let id1 = generate_stable_id(
            "proj",
            "repo",
            "main",
            "abc123",
            "/path/file.rs",
            "func",
            "fn func()",
        );
        let id2 = generate_stable_id(
            "proj",
            "repo",
            "main",
            "abc123",
            "/path/file.rs",
            "func",
            "fn func()",
        );
        assert_eq!(
            id1, id2,
            "Stable IDs should be consistent for identical inputs"
        );
    }

    #[test]
    fn test_generate_stable_id_uniqueness() {
        let id1 = generate_stable_id(
            "proj",
            "repo",
            "main",
            "abc123",
            "/path/file.rs",
            "func",
            "fn func()",
        );
        let id2 = generate_stable_id(
            "proj",
            "repo",
            "main",
            "abc123",
            "/path/file.rs",
            "func",
            "fn func2()",
        );
        assert_ne!(
            id1, id2,
            "Stable IDs should be unique for different signatures"
        );
    }

    #[test]
    fn test_url_parsing_fix() {
        // Test the fixed URL parsing logic
        let test_cases = vec![
            ("https://example.com:8080", "example.com:8080"),
            ("http://example.com", "example.com"),
            ("https://localhost:8000/db", "localhost:8000/db"),
            ("http://surrealdb:8000", "surrealdb:8000"),
        ];

        for (input, expected) in test_cases {
            let result = if let Some(stripped) = input.strip_prefix("https://") {
                stripped
            } else if let Some(stripped) = input.strip_prefix("http://") {
                stripped
            } else {
                input
            };
            assert_eq!(result, expected, "Failed to parse URL: {}", input);
        }
    }
}
