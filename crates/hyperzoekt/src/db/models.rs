use serde::{Deserialize, Serialize};

/// Immutable commit record
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Commit {
    pub id: String,
    pub repo: String,
    pub parents: Vec<String>,
    pub tree: Option<String>,
    pub author: Option<String>,
    pub committer: Option<String>,
    pub message: Option<String>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub indexed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Kind of ref
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RefKind {
    Branch,
    Tag,
}

/// Ref pointer to a commit
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RefRecord {
    pub id: String,
    pub repo: String,
    pub name: String,
    pub kind: RefKind,
    pub target: String,
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_by: Option<String>,
}
