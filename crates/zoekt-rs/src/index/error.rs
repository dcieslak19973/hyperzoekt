use std::error::Error;
use std::fmt::{self, Display};

/// Typed errors returned by index building operations.
#[derive(Debug)]
pub enum IndexError {
    /// Clone failed due to authentication/permission issues.
    CloneDenied(String),
    /// Clone failed because the repository was not found (404).
    RepoNotFound(String),
    /// Generic clone / network error.
    CloneError(String),
    /// Fallback for other textual errors.
    Other(String),
}

impl Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexError::CloneDenied(s) => write!(f, "clone denied: {}", s),
            IndexError::RepoNotFound(s) => write!(f, "repo not found: {}", s),
            IndexError::CloneError(s) => write!(f, "clone error: {}", s),
            IndexError::Other(s) => write!(f, "error: {}", s),
        }
    }
}

impl Error for IndexError {}

// Conversions from common error types into IndexError for easier propagation in binaries.
impl From<std::io::Error> for IndexError {
    fn from(e: std::io::Error) -> Self {
        IndexError::Other(e.to_string())
    }
}

impl From<anyhow::Error> for IndexError {
    fn from(e: anyhow::Error) -> Self {
        IndexError::Other(e.to_string())
    }
}

impl From<regex::Error> for IndexError {
    fn from(e: regex::Error) -> Self {
        IndexError::Other(e.to_string())
    }
}

impl From<serde_json::Error> for IndexError {
    fn from(e: serde_json::Error) -> Self {
        IndexError::Other(e.to_string())
    }
}
