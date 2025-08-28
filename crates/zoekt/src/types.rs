use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RepoMeta {
    pub name: String,
    pub root: PathBuf,
    // For future multi-branch support; for now, single-repo indexing will default to ["HEAD"].
    pub branches: Vec<String>,
}

/// A discovered symbol inside a document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Symbol {
    pub name: String,
    /// byte offset where the symbol name starts in the file (if available)
    pub start: Option<u32>,
    /// 1-based line number containing the symbol (if available)
    pub line: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMeta {
    pub path: PathBuf,
    pub lang: Option<String>,
    pub size: u64,
    /// Branches this document is present in. For in-memory index builder we default to ["HEAD"].
    pub branches: Vec<String>,
    /// Extracted top-level symbols (name + offset) for select=symbol
    pub symbols: Vec<Symbol>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Posting {
    pub doc: u32,
    pub line: u32,
    pub start: u32,
    pub end: u32,
}
