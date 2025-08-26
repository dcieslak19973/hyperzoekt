use serde::{Deserialize, Serialize};

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
pub struct EntityPayload {
    pub file: String,
    pub language: String,
    pub kind: String,
    pub name: String,
    pub parent: Option<String>,
    pub signature: String,
    pub start_line: Option<u32>,
    pub end_line: Option<u32>,
    pub calls: Vec<String>,
    pub doc: Option<String>,
    pub rank: f32,
    pub imports: Vec<ImportItem>,
    pub unresolved_imports: Vec<UnresolvedImport>,
    pub stable_id: String,
}
