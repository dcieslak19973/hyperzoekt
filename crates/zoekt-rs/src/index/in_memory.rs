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

use crate::types::{DocumentMeta, RepoMeta};
use parking_lot::RwLock;
use std::collections::HashMap;

pub type RepoDocId = u32;

/// Minimal in-memory inverted index (token -> doc ids)
#[derive(Debug)]
pub struct InMemoryIndexInner {
    pub repo: RepoMeta,
    pub docs: Vec<DocumentMeta>,
    pub terms: HashMap<String, Vec<RepoDocId>>, // unsorted, no positions (yet)
    /// symbol term map (lowercased symbol -> doc ids)
    pub symbol_terms: HashMap<String, Vec<RepoDocId>>,
    /// trigram -> doc ids for symbols (used as a prefilter)
    pub symbol_trigrams: HashMap<[u8; 3], Vec<RepoDocId>>,
    /// Optional in-memory per-doc content for branch-extracted documents
    pub doc_contents: Vec<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct InMemoryIndex {
    inner: std::sync::Arc<RwLock<InMemoryIndexInner>>,
}

impl InMemoryIndex {
    /// crate-local constructor used by the index builder
    pub(crate) fn from_inner(inner: InMemoryIndexInner) -> Self {
        InMemoryIndex {
            inner: std::sync::Arc::new(RwLock::new(inner)),
        }
    }

    pub fn search_literal(&self, needle: &str) -> Vec<(RepoDocId, String)> {
        let inner = self.inner.read();
        inner
            .docs
            .iter()
            .enumerate()
            .filter_map(|(i, meta)| {
                if let Some(opt) = inner.doc_contents.get(i) {
                    if let Some(text) = opt.as_ref() {
                        if text.contains(needle) {
                            return Some((i as RepoDocId, meta.path.display().to_string()));
                        }
                        return None;
                    }
                }
                let path = inner.repo.root.join(&meta.path);
                if let Ok(text) = std::fs::read_to_string(&path) {
                    if text.contains(needle) {
                        return Some((i as RepoDocId, meta.path.display().to_string()));
                    }
                }
                None
            })
            .collect()
    }

    pub(crate) fn read_inner(&self) -> parking_lot::RwLockReadGuard<'_, InMemoryIndexInner> {
        self.inner.read()
    }

    pub fn doc_count(&self) -> usize {
        self.inner.read().docs.len()
    }

    /// Return total bytes (sum of per-doc sizes) in the in-memory index
    pub fn total_scanned_bytes(&self) -> u64 {
        let inner = self.inner.read();
        inner.docs.iter().map(|d| d.size).sum()
    }

    /// Public accessor to retrieve a cloned DocumentMeta for a given doc index.
    pub fn doc_meta(&self, doc_idx: usize) -> Option<crate::types::DocumentMeta> {
        let inner = self.inner.read();
        inner.docs.get(doc_idx).cloned()
    }

    /// Public accessor to retrieve in-memory doc content for a given doc index, if present.
    pub fn doc_content(&self, doc_idx: usize) -> Option<String> {
        let inner = self.inner.read();
        inner.doc_contents.get(doc_idx).and_then(|o| o.clone())
    }

    /// Public accessor to retrieve the repo root path for this index.
    pub fn repo_root(&self) -> std::path::PathBuf {
        let inner = self.inner.read();
        inner.repo.root.clone()
    }
}
