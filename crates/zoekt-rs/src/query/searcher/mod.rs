mod eval;
pub(crate) mod plan_helpers;
mod plan_search;

use crate::index::{InMemoryIndex, RepoDocId};
use lru::LruCache;
use std::cell::RefCell;
use std::collections::HashMap;

use super::ast::{Query, QueryResult};
use super::helpers::{difference_sorted, intersect_sorted, union_sorted};
use super::plan::QueryPlan;
use eval::{estimate_cost, eval_literal, eval_literal_case_sensitive, eval_regex};

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
        plan_search::search_plan(self, plan)
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
        eval_literal(self.idx, needle)
    }

    fn eval_regex(&self, pattern: &str) -> Vec<RepoDocId> {
        eval_regex(self.idx, pattern)
    }

    fn estimate_cost(&self, q: &Query) -> usize {
        estimate_cost(self.idx, q)
    }

    fn eval_literal_case_sensitive(&self, needle: &str) -> Vec<RepoDocId> {
        eval_literal_case_sensitive(self.idx, needle)
    }
}
