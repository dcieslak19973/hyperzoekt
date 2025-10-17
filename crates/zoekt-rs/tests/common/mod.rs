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

use std::path::Path;
pub use tempfile;

use std::result::Result as StdResult;
use zoekt_rs::build_in_memory_index;
use zoekt_rs::index::IndexError;
use zoekt_rs::InMemoryIndex;

/// Create a temporary directory and return its path and a guard.
pub fn new_repo() -> tempfile::TempDir {
    tempfile::tempdir().expect("create tempdir")
}

/// Write a file relative to the repo root.
pub fn write_file(repo: &Path, rel: &str, contents: &[u8]) {
    let p = repo.join(rel);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).expect("create parent dirs");
    }
    std::fs::write(p, contents).expect("write file");
}

/// Build an in-memory index for the repo and return a Searcher.
pub fn build_index(repo: &Path) -> StdResult<InMemoryIndex, IndexError> {
    let idx = build_in_memory_index(repo)?;
    Ok(idx)
}
