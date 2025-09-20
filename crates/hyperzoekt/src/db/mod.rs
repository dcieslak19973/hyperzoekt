pub mod models;

use async_trait::async_trait;
use models::{Commit, RefKind, RefRecord};
use std::collections::HashMap;

/// Minimal DB client abstraction used by higher layers to resolve refs and read commits.
#[async_trait]
pub trait DbClient: Send + Sync {
    async fn get_ref_by_repo_and_name(&self, repo: &str, name: &str) -> Option<RefRecord>;
    async fn get_refs_by_repo_and_names(&self, repo: &str, names: &[String]) -> Vec<RefRecord>;
    async fn get_commit(&self, commit_id: &str) -> Option<Commit>;
}

/// Simple in-memory DB implementation for tests and local use.
pub struct InMemoryDb {
    refs: HashMap<String, RefRecord>,
    commits: HashMap<String, Commit>,
}

impl InMemoryDb {
    pub fn new() -> Self {
        Self {
            refs: HashMap::new(),
            commits: HashMap::new(),
        }
    }

    pub fn insert_ref(&mut self, r: RefRecord) {
        self.refs.insert(r.id.clone(), r);
    }

    pub fn insert_commit(&mut self, c: Commit) {
        self.commits.insert(c.id.clone(), c);
    }
}

#[async_trait]
impl DbClient for InMemoryDb {
    async fn get_ref_by_repo_and_name(&self, repo: &str, name: &str) -> Option<RefRecord> {
        self.refs
            .values()
            .find(|r| r.repo == repo && r.name == name)
            .cloned()
    }

    async fn get_refs_by_repo_and_names(&self, repo: &str, names: &[String]) -> Vec<RefRecord> {
        let set: std::collections::HashSet<_> = names.iter().collect();
        self.refs
            .values()
            .filter(|r| r.repo == repo && set.contains(&r.name))
            .cloned()
            .collect()
    }

    async fn get_commit(&self, commit_id: &str) -> Option<Commit> {
        self.commits.get(commit_id).cloned()
    }
}

/// Resolve a ref name to a commit id using simple disambiguation rules.
/// Tries exact name first, then `refs/heads/<name>`, then `refs/tags/<name>`.
pub async fn resolve_ref<C: DbClient>(db: &C, repo: &str, raw: &str) -> Result<String, String> {
    // candidates
    let candidates = vec![
        raw.to_string(),
        format!("refs/heads/{}", raw),
        format!("refs/tags/{}", raw),
    ];
    let rows = db.get_refs_by_repo_and_names(repo, &candidates).await;

    // prefer exact match
    if let Some(r) = rows.iter().find(|r| r.name == raw) {
        return Ok(r.target.clone());
    }
    if let Some(r) = rows
        .iter()
        .find(|r| r.name == format!("refs/heads/{}", raw))
    {
        return Ok(r.target.clone());
    }
    if let Some(r) = rows.iter().find(|r| r.name == format!("refs/tags/{}", raw)) {
        return Ok(r.target.clone());
    }

    Err(format!("ref not found: {}", raw))
}
