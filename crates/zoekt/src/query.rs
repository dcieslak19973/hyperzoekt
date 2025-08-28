use crate::index::{InMemoryIndex, RepoDocId};
use lru::LruCache;
use regex::Regex;
use std::cell::RefCell;
use std::collections::HashMap;
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
                })
            })
            .collect()
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
        if let Some(list) = inner.terms.get(&needle.to_lowercase()) {
            let mut v: Vec<RepoDocId> = list.to_vec();
            v.sort_unstable();
            v.dedup();
            return v;
        }
        // Fallback scan
        let mut v: Vec<RepoDocId> = inner
            .docs
            .iter()
            .enumerate()
            .filter_map(|(i, meta)| {
                let path = inner.repo.root.join(&meta.path);
                if let Ok(text) = std::fs::read_to_string(&path) {
                    if text.contains(needle) {
                        return Some(i as RepoDocId);
                    }
                }
                None
            })
            .collect();
        v.sort_unstable();
        v
    }

    fn eval_regex(&self, pattern: &str) -> Vec<RepoDocId> {
        let re = match Regex::new(pattern) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        let inner = self.idx.read_inner();
        let mut v: Vec<RepoDocId> = inner
            .docs
            .iter()
            .enumerate()
            .filter_map(|(i, meta)| {
                let path = inner.repo.root.join(&meta.path);
                let text = std::fs::read_to_string(&path).ok()?;
                if re.is_match(&text) {
                    Some(i as RepoDocId)
                } else {
                    None
                }
            })
            .collect();
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
