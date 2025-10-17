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

use crate::index::RepoDocId;

pub(crate) fn intersect_sorted(left: &[RepoDocId], right: &[RepoDocId]) -> Vec<RepoDocId> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < left.len() && j < right.len() {
        match left[i].cmp(&right[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(left[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

pub(crate) fn union_sorted(left: &[RepoDocId], right: &[RepoDocId]) -> Vec<RepoDocId> {
    let mut out = Vec::with_capacity(left.len() + right.len());
    let mut i = 0usize;
    let mut j = 0usize;
    while i < left.len() && j < right.len() {
        match left[i].cmp(&right[j]) {
            std::cmp::Ordering::Less => {
                out.push(left[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(right[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(left[i]);
                i += 1;
                j += 1;
            }
        }
    }
    while i < left.len() {
        out.push(left[i]);
        i += 1;
    }
    while j < right.len() {
        out.push(right[j]);
        j += 1;
    }
    out
}

pub(crate) fn difference_sorted(left: &[RepoDocId], right: &[RepoDocId]) -> Vec<RepoDocId> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < left.len() && j < right.len() {
        match left[i].cmp(&right[j]) {
            std::cmp::Ordering::Less => {
                out.push(left[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    while i < left.len() {
        out.push(left[i]);
        i += 1;
    }
    out
}

// Very small helper: split on whitespace, honoring single/double quotes.
pub(crate) fn shell_split(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_s = false;
    let mut in_d = false;
    for ch in input.chars() {
        match ch {
            '\'' if !in_d => {
                in_s = !in_s;
            }
            '"' if !in_s => {
                in_d = !in_d;
            }
            c if c.is_whitespace() && !in_s && !in_d => {
                if !buf.is_empty() {
                    out.push(std::mem::take(&mut buf));
                }
            }
            c => buf.push(c),
        }
    }
    if !buf.is_empty() {
        out.push(buf);
    }
    out
}
