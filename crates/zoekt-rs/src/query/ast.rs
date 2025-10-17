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
use std::fmt;

#[derive(Debug, Clone)]
pub enum Query {
    Literal(String),
    Regex(String),
    And(Box<Query>, Box<Query>),
    Or(Box<Query>, Box<Query>),
    Not(Box<Query>),
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub doc: RepoDocId,
    pub path: String,
    /// symbol name (if this result is a symbol-select)
    pub symbol: Option<String>,
    /// optional symbol location with offsets/line when available
    pub symbol_loc: Option<crate::types::Symbol>,
    /// relevance score for ranking (higher is better)
    pub score: f32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SelectKind {
    #[default]
    File,
    Repo,
    Symbol,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Query::Literal(s) => write!(f, "L({})", s),
            Query::Regex(r) => write!(f, "R({})", r),
            Query::And(a, b) => write!(f, "(AND {} {})", a, b),
            Query::Or(a, b) => write!(f, "(OR {} {})", a, b),
            Query::Not(inner) => write!(f, "(NOT {})", inner),
        }
    }
}
