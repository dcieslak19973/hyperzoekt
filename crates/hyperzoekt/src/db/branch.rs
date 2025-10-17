use crate::db::connection::SurrealConnection;
use anyhow::Result;
use log::debug;
use serde::Deserialize;
use serde_json::Value as JsonValue;

#[derive(Clone, Debug, Default)]
pub struct DefaultBranchInfo {
    pub branch: Option<String>,
    pub candidates: Vec<String>,
}

impl DefaultBranchInfo {
    pub fn new_exact(branch: String) -> Self {
        Self {
            branch: Some(branch.clone()),
            candidates: vec![branch],
        }
    }

    pub fn new_candidates<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        use std::collections::BTreeSet;
        let mut set: BTreeSet<String> = BTreeSet::new();
        for v in values {
            let s = v.into();
            if !s.trim().is_empty() {
                set.insert(s);
            }
        }
        Self {
            branch: None,
            candidates: set.into_iter().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.branch.is_none() && self.candidates.is_empty()
    }

    pub fn matches(&self, branch_name: &str) -> bool {
        if let Some(ref exact) = self.branch {
            if exact == branch_name {
                return true;
            }
        }
        self.candidates.iter().any(|c| c == branch_name)
    }
}

#[derive(Deserialize)]
struct BranchRow {
    name: Option<String>,
}

#[derive(Deserialize)]
struct RepoBranchRow {
    branch: Option<String>,
}

fn normalize_commit_ref(commit_ref: &str) -> &str {
    commit_ref.strip_prefix("commits:").unwrap_or(commit_ref)
}

/// Resolve the branch name (without `refs/heads/` prefix) associated with a commit.
pub async fn resolve_commit_branch(
    conn: &SurrealConnection,
    repo_name: &str,
    commit_ref: &str,
) -> Result<Option<String>> {
    if repo_name.is_empty() || commit_ref.is_empty() {
        return Ok(None);
    }

    let commit_id = normalize_commit_ref(commit_ref);
    let mut repo_candidates: Vec<JsonValue> = Vec::new();
    repo_candidates.push(JsonValue::String(repo_name.to_string()));
    let prefixed = format!("repo:{}", repo_name);
    repo_candidates.push(JsonValue::String(prefixed));

    let mut resp = conn
        .query_with_binds(
            "SELECT name FROM refs WHERE repo IN $repos AND kind = 'branch' AND target = $target LIMIT 1",
            vec![
                ("repos", JsonValue::Array(repo_candidates)),
                ("target", JsonValue::String(commit_id.to_string())),
            ],
        )
        .await?;

    let rows: Vec<BranchRow> = resp.take(0)?;
    if let Some(row) = rows.into_iter().next() {
        if let Some(name) = row.name {
            // Expected format: refs/heads/<branch>
            if let Some(stripped) = name.strip_prefix("refs/heads/") {
                return Ok(Some(stripped.to_string()));
            }
            return Ok(Some(name));
        }
    }
    Ok(None)
}

/// Resolve the configured default branch for a repository (if any).
pub async fn resolve_default_branch(
    conn: &SurrealConnection,
    repo_name: &str,
) -> Result<Option<DefaultBranchInfo>> {
    if repo_name.is_empty() {
        return Ok(None);
    }

    let mut resp = conn
        .query_with_binds(
            "SELECT branch FROM repo WHERE name = $name LIMIT 1",
            vec![("name", JsonValue::String(repo_name.to_string()))],
        )
        .await?;

    let rows: Vec<RepoBranchRow> = resp.take(0)?;
    if let Some(row) = rows.into_iter().next() {
        if let Some(branch) = row.branch {
            if !branch.trim().is_empty() {
                return Ok(Some(DefaultBranchInfo::new_exact(branch)));
            }
        }
    }

    debug!(
        "resolve_default_branch: no configured branch found for repo '{}'",
        repo_name
    );

    // Fallback: inspect refs table for common default branch names.
    let mut repo_candidates: Vec<JsonValue> = Vec::new();
    repo_candidates.push(JsonValue::String(repo_name.to_string()));
    let prefixed = format!("repo:{}", repo_name);
    repo_candidates.push(JsonValue::String(prefixed));

    let branch_candidates = vec!["refs/heads/main", "refs/heads/master", "refs/heads/trunk"]
        .into_iter()
        .map(|s| JsonValue::String(s.to_string()))
        .collect::<Vec<_>>();

    let mut refs_resp = conn
        .query_with_binds(
            "SELECT VALUE name FROM refs WHERE repo IN $repos AND name IN $names LIMIT 1",
            vec![
                ("repos", JsonValue::Array(repo_candidates)),
                ("names", JsonValue::Array(branch_candidates)),
            ],
        )
        .await?;

    let refs_rows: Vec<String> = refs_resp.take(0)?;
    if let Some(ref_name) = refs_rows.into_iter().next() {
        let branch = ref_name
            .strip_prefix("refs/heads/")
            .map(|s| s.to_string())
            .unwrap_or(ref_name);
        return Ok(Some(DefaultBranchInfo::new_exact(branch)));
    }

    if let Ok(env_branch) = std::env::var("SOURCE_BRANCH") {
        if !env_branch.trim().is_empty() {
            return Ok(Some(DefaultBranchInfo::new_exact(env_branch)));
        }
    }

    Ok(Some(DefaultBranchInfo::new_candidates([
        "main", "master", "trunk",
    ])))
}
