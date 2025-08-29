use crate::index::{InMemoryIndex, RepoDocId};
use globset::{GlobBuilder, GlobSetBuilder};
use lru::LruCache;
use regex::Regex;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SelectKind {
    #[default]
    File,
    Repo,
    Symbol,
}

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
    pub select: SelectKind,
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
                            "repo" => SelectKind::Repo,
                            "symbol" => SelectKind::Symbol,
                            _ => SelectKind::File,
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
                        "repo" => SelectKind::Repo,
                        "symbol" => SelectKind::Symbol,
                        _ => SelectKind::File,
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

pub struct Searcher<'a> {
    idx: &'a InMemoryIndex,
    // simple per-searcher cache for subquery results (stringified query -> doc ids)
    cache: RefCell<HashMap<String, Vec<RepoDocId>>>,
    // small LRU for repeated expensive subqueries
    lru: RefCell<LruCache<String, Vec<RepoDocId>>>,
}

impl<'a> Searcher<'a> {
    pub fn new(idx: &'a InMemoryIndex) -> Self {
        Self {
            idx,
            cache: RefCell::new(HashMap::new()),
            lru: RefCell::new(LruCache::new(std::num::NonZeroUsize::new(64).unwrap())),
        }
    }

    pub fn search(&self, q: &Query) -> Vec<QueryResult> {
        let docs = self.eval(q);
        let inner = self.idx.read_inner();
        docs.into_iter()
            .filter_map(|d| {
                inner.docs.get(d as usize).map(|meta| QueryResult {
                    doc: d,
                    path: meta.path.display().to_string(),
                    symbol: None,
                    symbol_loc: None,
                })
            })
            .collect()
    }

    pub fn search_plan(&self, plan: &QueryPlan) -> Vec<QueryResult> {
        // repo filter
        {
            let inner = self.idx.read_inner();
            if !plan.repos.is_empty() || !plan.repo_regexes.is_empty() {
                let name = &inner.repo.name;
                let hay = if plan.case_sensitive {
                    name.clone()
                } else {
                    name.to_lowercase()
                };
                let ok_sub = plan
                    .repos
                    .iter()
                    .map(|r| {
                        if plan.case_sensitive {
                            r.clone()
                        } else {
                            r.to_lowercase()
                        }
                    })
                    .any(|r| hay.contains(&r));
                let ok_re = plan.repo_regexes.iter().any(|re| {
                    if plan.case_sensitive {
                        re.is_match(name)
                    } else {
                        let pat = format!("(?i){}", re.as_str());
                        Regex::new(&pat)
                            .map(|rr| rr.is_match(name))
                            .unwrap_or(false)
                    }
                });
                // repo globs
                let ok_glob = if !plan.repo_globs.is_empty() {
                    let mut gb = GlobSetBuilder::new();
                    for g in &plan.repo_globs {
                        let mut bldr = GlobBuilder::new(g);
                        bldr.case_insensitive(!plan.case_sensitive);
                        if let Ok(gl) = bldr.build() {
                            gb.add(gl);
                        }
                    }
                    gb.build().map(|s| s.is_match(name)).unwrap_or(false)
                } else {
                    false
                };
                if !(ok_sub || ok_re || ok_glob) {
                    return vec![];
                }
            }
            if !plan.branches.is_empty() {
                // Very early: InMemoryIndex only has HEAD
                let branches: HashSet<_> = inner.repo.branches.iter().collect();
                if !plan
                    .branches
                    .iter()
                    .any(|b| branches.contains(&b.to_string()))
                {
                    return vec![];
                }
            }
        }

        // Build a base doc set either by content/path pattern or by filters only
        let mut base_docs: Vec<RepoDocId> = (0..self.idx.doc_count() as RepoDocId).collect();
        if let Some(mut pat) = plan.pattern.clone() {
            if plan.regex {
                // maintain case choice: default regex is case-sensitive; add (?i) when not sensitive
                if !plan.case_sensitive && !pat.starts_with("(?i)") {
                    pat = format!("(?i){}", pat);
                }
                base_docs = self.eval(&Query::Regex(pat));
            } else if plan.case_sensitive {
                // do a scan to respect case
                base_docs = self.eval_literal_case_sensitive(&pat);
            } else {
                base_docs = self.eval(&Query::Literal(pat));
            }
        }

        // debug prints removed

        // Apply path-only/content-only: when a pattern is present and the user did not
        // request content-only semantics, also consider filename (path) matches. If
        // `path_only` is set we replace the candidate set with path matches. Otherwise
        // we union path matches with the existing content-based `base_docs` so the
        // default behavior matches both path and content hits.
        if !plan.content_only {
            if let Some(pat) = &plan.pattern {
                let inner = self.idx.read_inner();
                let needle = if plan.case_sensitive {
                    pat.clone()
                } else {
                    pat.to_lowercase()
                };
                let mut path_docs = Vec::new();
                for (i, meta) in inner.docs.iter().enumerate() {
                    let p = meta.path.display().to_string();
                    let hay = if plan.case_sensitive {
                        p
                    } else {
                        p.to_lowercase()
                    };
                    if hay.contains(&needle) {
                        path_docs.push(i as RepoDocId);
                    }
                }
                if plan.path_only {
                    base_docs = path_docs;
                } else {
                    base_docs = union_sorted(&base_docs, &path_docs);
                }
            }
        }

        // Now apply file filters: prefix/regex (we only have regex list and globs here)
        let inner = self.idx.read_inner();
        // Build globset from file_globs with case handling
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
            // branch filter: if the plan requests branches, require the doc to be present in at least one.
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
            // lang filter
            if !plan.langs.is_empty() {
                let l = meta.lang.as_deref().unwrap_or("");
                if !plan.langs.iter().any(|x| x == l) {
                    continue 'doc;
                }
            }
            // file globs via globset
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
            // file regex (honor case via (?i))
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

        // handle select modes
        match plan.select {
            SelectKind::Repo => {
                if filtered.is_empty() {
                    vec![]
                } else {
                    let inner = self.idx.read_inner();
                    vec![QueryResult {
                        doc: 0,
                        path: inner.repo.name.clone(),
                        symbol: None,
                        symbol_loc: None,
                    }]
                }
            }
            SelectKind::Symbol => {
                let mut out: Vec<QueryResult> = Vec::new();

                if let Some(pat) = &plan.pattern {
                    // prefilter candidate docs using symbol trigrams when possible
                    let mut cand_docs: Option<Vec<RepoDocId>> = None;
                    if plan.regex {
                        // heuristic: extract alnum substrings >=3 from regex
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

                    if let Some(cdocs) = cand_docs {
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
                                            });
                                        }
                                    }
                                }
                            }
                        }
                        return out;
                    }
                    // fallthrough to full scan if cand_docs was None or empty
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
                    })
                })
                .collect(),
        }
    }

    fn eval(&self, q: &Query) -> Vec<RepoDocId> {
        // cache key
        let key = q.to_string();
        if let Some(cached) = self.cache.borrow().get(&key) {
            return cached.clone();
        }
        if let Some(cached) = self.lru.borrow_mut().get(&key) {
            return cached.clone();
        }

        let out: Vec<RepoDocId> = match q {
            Query::Literal(s) => self.eval_literal(s),
            Query::Regex(r) => self.eval_regex(r),
            Query::And(a, b) => {
                // Evaluate cheaper side first and short-circuit if empty
                let (left_q, right_q) = if self.estimate_cost(a) <= self.estimate_cost(b) {
                    (a, b)
                } else {
                    (b, a)
                };
                let left = self.eval(left_q);
                if left.is_empty() {
                    Vec::new()
                } else {
                    let right = self.eval(right_q);
                    intersect_sorted(&left, &right)
                }
            }
            Query::Or(a, b) => {
                // evaluate both sides and try some short-circuits
                let left = self.eval(a);
                if left.is_empty() {
                    return self.eval(b);
                }
                let right = self.eval(b);
                if right.is_empty() {
                    left
                } else {
                    // if one side already covers all docs, union is that side
                    let doc_count = self.idx.doc_count();
                    if left.len() == doc_count {
                        left
                    } else if right.len() == doc_count {
                        right
                    } else {
                        union_sorted(&left, &right)
                    }
                }
            }
            Query::Not(inner_q) => {
                let all: Vec<RepoDocId> = (0..self.idx.doc_count() as RepoDocId).collect();
                let sub = self.eval(inner_q);
                difference_sorted(&all, &sub)
            }
        };

        // store in simple per-search cache
        self.cache.borrow_mut().insert(key.clone(), out.clone());
        // if this is an expensive/compound query or large result, also put into LRU
        match q {
            Query::Literal(_) => {}
            _ => {
                if !out.is_empty() {
                    self.lru.borrow_mut().put(key.clone(), out.clone());
                }
            }
        }
        out
    }

    fn eval_literal(&self, needle: &str) -> Vec<RepoDocId> {
        let inner = self.idx.read_inner();
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
            if let Ok(text) = std::fs::read_to_string(&path) {
                let hay = text.to_lowercase();
                if hay.contains(&needle.to_lowercase()) {
                    v.push(i as RepoDocId);
                }
            }
        }
        v.sort_unstable();
        v
    }

    fn eval_regex(&self, pattern: &str) -> Vec<RepoDocId> {
        let re = match Regex::new(pattern) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        let inner = self.idx.read_inner();
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
            if let Ok(text) = std::fs::read_to_string(&path) {
                if re.is_match(&text) {
                    v.push(i as RepoDocId);
                }
            }
        }
        v.sort_unstable();
        v
    }

    fn estimate_cost(&self, q: &Query) -> usize {
        match q {
            Query::Literal(s) => {
                let inner = self.idx.read_inner();
                inner
                    .terms
                    .get(&s.to_lowercase())
                    .map(|l| l.len())
                    .unwrap_or(inner.docs.len())
            }
            Query::Regex(_) => self.idx.doc_count(),
            Query::And(a, b) | Query::Or(a, b) => {
                let ca = self.estimate_cost(a);
                let cb = self.estimate_cost(b);
                std::cmp::min(ca, cb)
            }
            Query::Not(inner_q) => self.estimate_cost(inner_q),
        }
    }

    fn eval_literal_case_sensitive(&self, needle: &str) -> Vec<RepoDocId> {
        let inner = self.idx.read_inner();
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
            if let Ok(text) = std::fs::read_to_string(&path) {
                if text.contains(needle) {
                    v.push(i as RepoDocId);
                }
            }
        }
        v.sort_unstable();
        v
    }
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

// Helpers
fn intersect_sorted(left: &[RepoDocId], right: &[RepoDocId]) -> Vec<RepoDocId> {
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

fn union_sorted(left: &[RepoDocId], right: &[RepoDocId]) -> Vec<RepoDocId> {
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

fn difference_sorted(left: &[RepoDocId], right: &[RepoDocId]) -> Vec<RepoDocId> {
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
fn shell_split(input: &str) -> Vec<String> {
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
