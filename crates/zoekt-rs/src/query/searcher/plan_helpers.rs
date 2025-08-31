use crate::query::helpers::intersect_sorted;
use crate::QueryPlan;
use crate::RepoDocId;
use globset::{GlobBuilder, GlobSetBuilder};
use regex::Regex;

/// Check whether a repository matches the QueryPlan's repo filters (explicit
/// repo list, globs, regexes) and whether its branches satisfy the plan's
/// branch constraints.
pub(crate) fn repo_matches(repo_name: &str, repo_branches: &[String], plan: &QueryPlan) -> bool {
    if !plan.repos.is_empty() {
        if plan.repos.iter().any(|r| r == repo_name) {
            return true;
        }
        return false;
    }
    if !plan.repo_globs.is_empty() {
        for g in &plan.repo_globs {
            // simple glob substring match for now
            if globset::Glob::new(g)
                .ok()
                .is_some_and(|gl| gl.compile_matcher().is_match(repo_name))
            {
                return true;
            }
        }
    }
    if !plan.repo_regexes.is_empty() {
        for re in &plan.repo_regexes {
            if re.is_match(repo_name) {
                return true;
            }
        }
    }
    if !plan.branches.is_empty() {
        // require at least one branch overlap
        for b in &plan.branches {
            if repo_branches.iter().any(|rb| rb == b) {
                return true;
            }
        }
        return false;
    }
    true
}

/// Build a list of document ids whose path matches the given pattern.
pub(crate) fn build_path_docs(
    docs: &[crate::types::DocumentMeta],
    pattern: &str,
    case_sensitive: bool,
) -> Vec<RepoDocId> {
    let mut out = Vec::new();
    for (i, d) in docs.iter().enumerate() {
        let path = d.path.display().to_string();
        if case_sensitive {
            if path.contains(pattern) {
                out.push(i as RepoDocId);
            }
        } else if path.to_lowercase().contains(&pattern.to_lowercase()) {
            out.push(i as RepoDocId);
        }
    }
    out
}

/// Apply file/glob/lang/branch filters and return the filtered doc ids.
pub(crate) fn apply_file_filters(
    idx: &crate::index::InMemoryIndex,
    base_docs: Vec<RepoDocId>,
    plan: &QueryPlan,
) -> Vec<RepoDocId> {
    let inner = idx.read_inner();
    let globset = if !plan.file_globs.is_empty() {
        let mut b = GlobSetBuilder::new();
        for g in &plan.file_globs {
            let mut gb = GlobBuilder::new(g);
            gb.case_insensitive(!plan.case_sensitive);
            if let Ok(gl) = gb.build() {
                b.add(gl);
            }
        }
        b.build().ok()
    } else {
        None
    };
    let mut filtered: Vec<RepoDocId> = Vec::new();
    'doc: for d in base_docs {
        let meta = match inner.docs.get(d as usize) {
            Some(m) => m,
            None => continue,
        };
        if !plan.branches.is_empty() {
            let mut ok = false;
            for b in &plan.branches {
                if meta.branches.iter().any(|mb| mb == b) {
                    ok = true;
                    break;
                }
            }
            if !ok {
                continue 'doc;
            }
        }
        let path_str = meta.path.display().to_string();
        if !plan.langs.is_empty() {
            let l = meta.lang.as_deref().unwrap_or("");
            if !plan.langs.iter().any(|x| x == l) {
                continue 'doc;
            }
        }
        if let Some(gs) = &globset {
            if !gs.is_match(&path_str) {
                continue 'doc;
            }
        } else {
            for g in &plan.file_globs {
                let hay = if plan.case_sensitive {
                    path_str.clone()
                } else {
                    path_str.to_lowercase()
                };
                let nee = if plan.case_sensitive {
                    g.clone()
                } else {
                    g.to_lowercase()
                };
                if !hay.contains(&nee) {
                    continue 'doc;
                }
            }
        }
        for re in &plan.file_regexes {
            if plan.case_sensitive {
                if !re.is_match(&path_str) {
                    continue 'doc;
                }
            } else {
                let pat = format!("(?i){}", re.as_str());
                match Regex::new(&pat) {
                    Ok(rr) => {
                        if !rr.is_match(&path_str) {
                            continue 'doc;
                        }
                    }
                    Err(_) => continue 'doc,
                }
            }
        }
        filtered.push(d);
    }
    filtered
}

/// Prefilter symbol candidate docs using trigrams when possible. Returns Some(doclist)
/// when able to build a trigram-based candidate list (possibly empty), otherwise None.
pub(crate) fn symbol_prefilter(
    idx: &crate::index::InMemoryIndex,
    plan: &QueryPlan,
) -> Option<Vec<RepoDocId>> {
    let inner = idx.read_inner();
    let mut cand_docs: Option<Vec<RepoDocId>> = None;
    if let Some(pat) = &plan.pattern {
        if plan.regex {
            let mut subs: Vec<String> = Vec::new();
            let mut cur = String::new();
            for ch in pat.chars() {
                if ch.is_alphanumeric() {
                    cur.push(ch);
                } else {
                    if cur.len() >= 3 {
                        subs.push(cur.clone());
                    }
                    cur.clear();
                }
            }
            if cur.len() >= 3 {
                subs.push(cur);
            }
            if let Some(sub) = subs.first() {
                let tris: Vec<[u8; 3]> = crate::trigram::trigrams(sub).collect();
                if !tris.is_empty() {
                    for t in tris {
                        if let Some(list) = inner.symbol_trigrams.get(&t) {
                            let mut docs: Vec<RepoDocId> = list.clone();
                            docs.sort_unstable();
                            docs.dedup();
                            cand_docs = Some(match cand_docs {
                                None => docs,
                                Some(prev) => intersect_sorted(&prev, &docs),
                            });
                        } else {
                            cand_docs = Some(Vec::new());
                            break;
                        }
                    }
                }
            }
        } else {
            let tris: Vec<[u8; 3]> = crate::trigram::trigrams(pat).collect();
            if !tris.is_empty() {
                for t in tris {
                    if let Some(list) = inner.symbol_trigrams.get(&t) {
                        let mut docs: Vec<RepoDocId> = list.clone();
                        docs.sort_unstable();
                        docs.dedup();
                        cand_docs = Some(match cand_docs {
                            None => docs,
                            Some(prev) => intersect_sorted(&prev, &docs),
                        });
                    } else {
                        cand_docs = Some(Vec::new());
                        break;
                    }
                }
            }
        }
    }
    cand_docs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DocumentMeta;

    #[test]
    fn repo_matches_substring_glob_regex_and_branch() {
        let mut plan = crate::QueryPlan::default();
        plan.repos.push("owner/repo".to_string());
        assert!(repo_matches("owner/repo", &["HEAD".to_string()], &plan));

        plan.repos.clear();
        plan.repo_globs.push("owner/*".to_string());
        assert!(repo_matches("owner/repo", &["HEAD".to_string()], &plan));

        plan.repo_globs.clear();
        plan.repo_regexes
            .push(regex::Regex::new("^owner/.*$").unwrap());
        assert!(repo_matches("owner/repo", &["HEAD".to_string()], &plan));

        plan.repo_regexes.clear();
        plan.branches.push("feature".to_string());
        // repo has only HEAD
        assert!(!repo_matches("owner/repo", &["HEAD".to_string()], &plan));
        // when branches include feature it's ok
        assert!(repo_matches(
            "owner/repo",
            &["HEAD".to_string(), "feature".to_string()],
            &plan
        ));
    }

    #[test]
    fn build_path_docs_detects_paths_case_insensitive() {
        let docs = vec![
            DocumentMeta {
                path: std::path::PathBuf::from("src/lib.rs"),
                lang: None,
                size: 1,
                branches: vec!["HEAD".to_string()],
                symbols: vec![],
            },
            DocumentMeta {
                path: std::path::PathBuf::from("README.md"),
                lang: None,
                size: 1,
                branches: vec!["HEAD".to_string()],
                symbols: vec![],
            },
        ];
        let res = build_path_docs(&docs, "lib.rs", false);
        assert_eq!(res, vec![0u32]);
    }
}
