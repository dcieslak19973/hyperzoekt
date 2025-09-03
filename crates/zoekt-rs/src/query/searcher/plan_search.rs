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
use regex::Regex;

use crate::query::ast::{Query, QueryResult, SelectKind};
use crate::query::helpers::union_sorted;
use crate::query::plan::QueryPlan;

pub(crate) fn search_plan(
    s: &crate::query::searcher::Searcher<'_>,
    plan: &QueryPlan,
) -> Vec<QueryResult> {
    // repo filter
    {
        let inner = s.idx.read_inner();
        if !crate::query::searcher::plan_helpers::repo_matches(
            &inner.repo.name,
            &inner.repo.branches,
            plan,
        ) {
            return vec![];
        }
    }

    // Build a base doc set either by content/path pattern or by filters only
    let mut base_docs: Vec<RepoDocId> = (0..s.idx.doc_count() as RepoDocId).collect();
    if let Some(mut pat) = plan.pattern.clone() {
        if plan.regex {
            if !plan.case_sensitive && !pat.starts_with("(?i)") {
                pat = format!("(?i){}", pat);
            }
            base_docs = s.eval(&Query::Regex(pat));
        } else if plan.case_sensitive {
            base_docs = s.eval_literal_case_sensitive(&pat);
        } else {
            base_docs = s.eval(&Query::Literal(pat));
        }
    }

    // Apply path-only/content-only
    if !plan.content_only {
        if let Some(pat) = &plan.pattern {
            let inner = s.idx.read_inner();
            let path_docs = crate::query::searcher::plan_helpers::build_path_docs(
                &inner.docs,
                pat,
                plan.case_sensitive,
            );
            if plan.path_only {
                base_docs = path_docs;
            } else {
                base_docs = union_sorted(&base_docs, &path_docs);
            }
        }
    }

    let filtered = crate::query::searcher::plan_helpers::apply_file_filters(s.idx, base_docs, plan);

    let inner = s.idx.read_inner();

    match plan.select {
        SelectKind::Repo => {
            if filtered.is_empty() {
                vec![]
            } else {
                let inner = s.idx.read_inner();
                vec![QueryResult {
                    doc: 0,
                    path: inner.repo.name.clone(),
                    symbol: None,
                    symbol_loc: None,
                    score: 1.0,
                }]
            }
        }
        SelectKind::Symbol => {
            let mut out: Vec<QueryResult> = Vec::new();

            if let Some(pat) = &plan.pattern {
                if let Some(cdocs) =
                    crate::query::searcher::plan_helpers::symbol_prefilter(s.idx, plan)
                {
                    let filtered_set: std::collections::HashSet<RepoDocId> =
                        filtered.iter().copied().collect();
                    for d in cdocs.into_iter().filter(|d| filtered_set.contains(d)) {
                        if let Some(meta) = inner.docs.get(d as usize) {
                            if plan.regex {
                                let mut pat_s = pat.clone();
                                if !plan.case_sensitive && !pat_s.starts_with("(?i)") {
                                    pat_s = format!("(?i){}", pat_s);
                                }
                                if let Ok(re) = Regex::new(&pat_s) {
                                    for sym in &meta.symbols {
                                        if re.is_match(&sym.name) {
                                            out.push(QueryResult {
                                                doc: d,
                                                path: meta.path.display().to_string(),
                                                symbol: Some(sym.name.clone()),
                                                symbol_loc: Some(sym.clone()),
                                                score: 2.0, // Higher score for symbol matches
                                            });
                                        }
                                    }
                                }
                            } else {
                                let nee = if plan.case_sensitive {
                                    pat.clone()
                                } else {
                                    pat.to_lowercase()
                                };
                                for sym in &meta.symbols {
                                    let hay = if plan.case_sensitive {
                                        sym.name.clone()
                                    } else {
                                        sym.name.to_lowercase()
                                    };
                                    if hay.contains(&nee) {
                                        out.push(QueryResult {
                                            doc: d,
                                            path: meta.path.display().to_string(),
                                            symbol: Some(sym.name.clone()),
                                            symbol_loc: Some(sym.clone()),
                                            score: 2.0, // Higher score for symbol matches
                                        });
                                    }
                                }
                            }
                        }
                    }
                    return out;
                }
            }

            for d in filtered {
                if let Some(meta) = inner.docs.get(d as usize) {
                    if let Some(pat) = &plan.pattern {
                        if plan.regex {
                            let mut pat_s = pat.clone();
                            if !plan.case_sensitive && !pat_s.starts_with("(?i)") {
                                pat_s = format!("(?i){}", pat_s);
                            }
                            if let Ok(re) = Regex::new(&pat_s) {
                                for sym in &meta.symbols {
                                    if re.is_match(&sym.name) {
                                        out.push(QueryResult {
                                            doc: d,
                                            path: meta.path.display().to_string(),
                                            symbol: Some(sym.name.clone()),
                                            symbol_loc: Some(sym.clone()),
                                            score: 2.0, // Higher score for symbol matches
                                        });
                                    }
                                }
                            }
                        } else {
                            let nee = if plan.case_sensitive {
                                pat.clone()
                            } else {
                                pat.to_lowercase()
                            };
                            for sym in &meta.symbols {
                                let hay = if plan.case_sensitive {
                                    sym.name.clone()
                                } else {
                                    sym.name.to_lowercase()
                                };
                                if hay.contains(&nee) {
                                    out.push(QueryResult {
                                        doc: d,
                                        path: meta.path.display().to_string(),
                                        symbol: Some(sym.name.clone()),
                                        symbol_loc: Some(sym.clone()),
                                        score: 2.0, // Higher score for symbol matches
                                    });
                                }
                            }
                        }
                    } else {
                        for sym in &meta.symbols {
                            out.push(QueryResult {
                                doc: d,
                                path: meta.path.display().to_string(),
                                symbol: Some(sym.name.clone()),
                                symbol_loc: Some(sym.clone()),
                                score: 2.0, // Higher score for symbol matches
                            });
                        }
                    }
                }
            }

            out
        }
        SelectKind::File => filtered
            .into_iter()
            .filter_map(|d| {
                inner.docs.get(d as usize).map(|meta| QueryResult {
                    doc: d,
                    path: meta.path.display().to_string(),
                    symbol: None,
                    symbol_loc: None,
                    score: 1.0, // Lower score for file matches
                })
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::InMemoryIndexInner;
    use crate::types::{DocumentMeta, RepoMeta};
    // ...existing code...

    fn make_index(
        docs: Vec<DocumentMeta>,
        repo_name: &str,
        branches: Vec<String>,
    ) -> crate::index::InMemoryIndex {
        let inner = InMemoryIndexInner {
            repo: RepoMeta {
                name: repo_name.to_string(),
                root: std::path::PathBuf::from("/tmp"),
                branches,
                visibility: crate::types::RepoVisibility::Public,
                owner: None,
                allowed_users: vec![],
                last_commit_sha: None,
            },
            docs,
            terms: std::collections::HashMap::new(),
            symbol_terms: std::collections::HashMap::new(),
            symbol_trigrams: std::collections::HashMap::new(),
            doc_contents: vec![],
        };
        crate::index::InMemoryIndex::from_inner(inner)
    }

    #[test]
    fn test_repo_glob_filtering() {
        let docs = vec![DocumentMeta {
            path: std::path::PathBuf::from("a.txt"),
            lang: None,
            size: 10,
            branches: vec!["HEAD".to_string()],
            symbols: vec![],
        }];
        let idx = make_index(docs, "owner/repo-main", vec!["HEAD".to_string()]);
        let s = crate::query::searcher::Searcher::new(&idx);
        let mut plan = QueryPlan::default();
        plan.repo_globs.push("owner/*".to_string());
        let res = search_plan(&s, &plan);
        assert!(!res.is_empty());
    }

    #[test]
    fn test_file_regex_filtering() {
        let docs = vec![DocumentMeta {
            path: std::path::PathBuf::from("src/lib.rs"),
            lang: None,
            size: 10,
            branches: vec!["HEAD".to_string()],
            symbols: vec![],
        }];
        let idx = make_index(docs, "repo", vec!["HEAD".to_string()]);
        let s = crate::query::searcher::Searcher::new(&idx);
        let mut plan = QueryPlan::default();
        plan.file_regexes
            .push(regex::Regex::new(".*lib\\.rs$").unwrap());
        let res = search_plan(&s, &plan);
        assert_eq!(res.len(), 1);
    }

    #[test]
    fn test_branch_filtering() {
        let docs = vec![DocumentMeta {
            path: std::path::PathBuf::from("one.txt"),
            lang: None,
            size: 10,
            branches: vec!["feature".to_string()],
            symbols: vec![],
        }];
        let idx = make_index(
            docs,
            "repo",
            vec!["HEAD".to_string(), "feature".to_string()],
        );
        let s = crate::query::searcher::Searcher::new(&idx);
        let mut plan = QueryPlan::default();
        plan.branches.push("feature".to_string());
        let res = search_plan(&s, &plan);
        assert_eq!(res.len(), 1);
    }
}
