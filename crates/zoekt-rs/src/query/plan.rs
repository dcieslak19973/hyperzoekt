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

use regex::Regex;

use super::helpers::shell_split;

#[derive(Debug, Clone, Default)]
pub struct QueryPlan {
    // core query text (literal or regex). If regex=true, treat as regex.
    pub pattern: Option<String>,
    pub regex: bool,
    // filters
    pub repos: Vec<String>,
    pub repo_globs: Vec<String>,
    pub repo_regexes: Vec<Regex>,
    pub file_globs: Vec<String>,
    pub file_regexes: Vec<Regex>,
    pub langs: Vec<String>,
    pub branches: Vec<String>,
    pub case_sensitive: bool,
    pub content_only: bool,
    pub path_only: bool,
    pub select: super::ast::SelectKind,
}

fn looks_like_regex(s: &str) -> bool {
    // Heuristic: presence of typical regex metacharacters
    s.contains('[')
        || s.contains(']')
        || s.contains('(')
        || s.contains(')')
        || s.contains('|')
        || s.contains('?')
        || s.contains('+')
        || s.contains('*')
        || s.contains('^')
        || s.contains('$')
        || s.contains('\\')
}

impl QueryPlan {
    pub fn parse(input: &str) -> anyhow::Result<Self> {
        // Extremely small subset of Zoekt syntax:
        // tokens split by whitespace, filters of form key:value, select=..., case:yes|no
        // If a token is not key:value or select=, it contributes to the pattern (joined by space).
        let mut plan = QueryPlan::default();
        let mut pats: Vec<String> = Vec::new();
        let mut is_regex = false;
        for tok in shell_split(input).into_iter() {
            if let Some((k, v)) = tok.split_once(':') {
                match k {
                    "repo" => {
                        if looks_like_regex(v) {
                            if let Ok(re) = Regex::new(v) {
                                plan.repo_regexes.push(re);
                            } else {
                                plan.repos.push(v.to_string());
                            }
                        } else if v.contains('*') || v.contains('?') || v.contains('[') {
                            plan.repo_globs.push(v.to_string());
                        } else {
                            plan.repos.push(v.to_string());
                        }
                    }
                    "file" => {
                        // treat as a regex if it looks like one, otherwise glob-ish
                        if looks_like_regex(v) {
                            if let Ok(re) = Regex::new(v) {
                                plan.file_regexes.push(re);
                            }
                        } else {
                            plan.file_globs.push(v.to_string());
                        }
                    }
                    "lang" => plan.langs.push(v.to_lowercase()),
                    "branch" => plan.branches.push(v.to_string()),
                    "case" => {
                        plan.case_sensitive = matches!(v, "yes" | "true" | "sensitive");
                    }
                    "select" => {
                        plan.select = match v {
                            "repo" => super::ast::SelectKind::Repo,
                            "symbol" => super::ast::SelectKind::Symbol,
                            _ => super::ast::SelectKind::File,
                        }
                    }
                    "content" => {
                        plan.content_only = matches!(v, "only" | "yes" | "true");
                    }
                    "path" => {
                        plan.path_only = matches!(v, "only" | "yes" | "true");
                    }
                    "re" | "regex" => {
                        is_regex = matches!(v, "1" | "yes" | "true");
                    }
                    _ => pats.push(tok),
                }
            } else if let Some((k, v)) = tok.split_once('=') {
                if k == "select" {
                    plan.select = match v {
                        "repo" => super::ast::SelectKind::Repo,
                        "symbol" => super::ast::SelectKind::Symbol,
                        _ => super::ast::SelectKind::File,
                    };
                } else {
                    pats.push(tok);
                }
            } else {
                pats.push(tok);
            }
        }
        if !pats.is_empty() {
            plan.pattern = Some(pats.join(" "));
        }
        plan.regex = is_regex;
        Ok(plan)
    }
}
