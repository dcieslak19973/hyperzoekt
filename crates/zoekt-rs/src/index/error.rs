// Copyright 2025 HyperZoekt Project
// Derived from sourcegraph/zoekt (https://github.com/sourcegraph/zoekt)
// Copyright 2016 Google Inc. All rights reserved.
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
