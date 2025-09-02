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

use crate::index::{InMemoryIndex, RepoDocId};
use crate::query::ast::Query;
use regex::Regex;
use std::fs;

pub(super) fn eval_literal(idx: &InMemoryIndex, needle: &str) -> Vec<RepoDocId> {
    let inner = idx.read_inner();
    // case-insensitive by default (we'll add case handling via QueryPlan)
    if let Some(list) = inner.terms.get(&needle.to_lowercase()) {
        let mut v: Vec<RepoDocId> = list.to_vec();
        v.sort_unstable();
        v.dedup();
        return v;
    }
    // Fallback scan: prefer in-memory doc_contents for branch-indexed docs
    let mut v: Vec<RepoDocId> = Vec::new();
    for (i, meta) in inner.docs.iter().enumerate() {
        // prefer in-memory content
        if let Some(opt) = inner.doc_contents.get(i) {
            if let Some(text) = opt.as_ref() {
                let hay = text.to_lowercase();
                let nee = needle.to_lowercase();
                if hay.contains(&nee) {
                    v.push(i as RepoDocId);
                }
                continue;
            }
        }
        // fallback to reading from disk
        let path = inner.repo.root.join(&meta.path);
        if let Ok(text) = fs::read_to_string(&path) {
            let hay = text.to_lowercase();
            if hay.contains(&needle.to_lowercase()) {
                v.push(i as RepoDocId);
            }
        }
    }
    v.sort_unstable();
    v
}

pub(super) fn eval_regex(idx: &InMemoryIndex, pattern: &str) -> Vec<RepoDocId> {
    let re = match Regex::new(pattern) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let inner = idx.read_inner();
    let mut v: Vec<RepoDocId> = Vec::new();
    for (i, meta) in inner.docs.iter().enumerate() {
        // prefer in-memory content
        if let Some(opt) = inner.doc_contents.get(i) {
            if let Some(text) = opt.as_ref() {
                if re.is_match(text) {
                    v.push(i as RepoDocId);
                }
                continue;
            }
        }
        let path = inner.repo.root.join(&meta.path);
        if let Ok(text) = fs::read_to_string(&path) {
            if re.is_match(&text) {
                v.push(i as RepoDocId);
            }
        }
    }
    v.sort_unstable();
    v
}

pub(super) fn estimate_cost(idx: &InMemoryIndex, q: &Query) -> usize {
    match q {
        Query::Literal(s) => {
            let inner = idx.read_inner();
            inner
                .terms
                .get(&s.to_lowercase())
                .map(|l| l.len())
                .unwrap_or(inner.docs.len())
        }
        Query::Regex(_) => idx.doc_count(),
        Query::And(a, b) | Query::Or(a, b) => {
            let ca = estimate_cost(idx, a);
            let cb = estimate_cost(idx, b);
            std::cmp::min(ca, cb)
        }
        Query::Not(inner_q) => estimate_cost(idx, inner_q),
    }
}

pub(super) fn eval_literal_case_sensitive(idx: &InMemoryIndex, needle: &str) -> Vec<RepoDocId> {
    let inner = idx.read_inner();
    let mut v: Vec<RepoDocId> = Vec::new();
    for (i, meta) in inner.docs.iter().enumerate() {
        if let Some(opt) = inner.doc_contents.get(i) {
            if let Some(text) = opt.as_ref() {
                if text.contains(needle) {
                    v.push(i as RepoDocId);
                }
                continue;
            }
        }
        let path = inner.repo.root.join(&meta.path);
        if let Ok(text) = fs::read_to_string(&path) {
            if text.contains(needle) {
                v.push(i as RepoDocId);
            }
        }
    }
    v.sort_unstable();
    v
}
