use crate::regex_analyze::prefilter_from_regex;
use crate::trigram::trigrams;
use lru::LruCache;
use memmap2::Mmap;
use parking_lot::Mutex;
use regex::Regex;
use std::fs::File;
use std::num::NonZeroUsize;
use std::sync::Arc;

use super::intersect_sorted;

pub struct ShardSearcher<'a> {
    rdr: &'a super::ShardReader,
    file_cache: Mutex<LruCache<u32, Arc<Mmap>>>,
}

impl<'a> ShardSearcher<'a> {
    pub fn new(rdr: &'a super::ShardReader) -> Self {
        let cap = NonZeroUsize::new(256).unwrap();
        Self {
            rdr,
            file_cache: Mutex::new(LruCache::new(cap)),
        }
    }

    /// Access underlying reader for bridge code in parent module.
    pub fn rdr(&self) -> &super::ShardReader {
        self.rdr
    }

    fn get_doc_mmap(&self, doc: u32) -> Option<Arc<Mmap>> {
        if let Some(mm) = self.file_cache.lock().get(&doc).cloned() {
            return Some(mm);
        }
        let path = self.rdr.paths().get(doc as usize)?.clone();
        let full = std::path::Path::new(self.rdr.repo_root()).join(path);
        let f = File::open(&full).ok()?;
        let mmap = unsafe { Mmap::map(&f).ok()? };
        let arc = Arc::new(mmap);
        self.file_cache.lock().put(doc, arc.clone());
        Some(arc)
    }

    pub fn search_literal(&self, needle: &str) -> Vec<(u32, String)> {
        let tris: Vec<_> = trigrams(needle).collect();
        if tris.is_empty() {
            return vec![];
        }
        let map = match self.rdr.postings_map() {
            Ok(m) => m,
            Err(_) => return vec![],
        };
        let mut cand: Option<Vec<u32>> = None;
        for t in tris {
            if let Some(v) = map.get(&t) {
                let docs: Vec<u32> = v.keys().copied().collect();
                cand = Some(match cand {
                    None => docs,
                    Some(prev) => intersect_sorted(&prev, &docs),
                });
            } else {
                return vec![];
            }
        }
        let paths: Vec<_> = self.rdr.paths().to_vec();
        cand.unwrap_or_default()
            .into_iter()
            .map(|d| (d, paths[d as usize].clone()))
            .collect()
    }

    pub fn search_regex_prefiltered(&self, pattern: &str) -> Vec<(u32, String)> {
        let _re = match Regex::new(pattern) {
            Ok(r) => r,
            Err(_) => return vec![],
        };
        let pf = prefilter_from_regex(pattern);
        let paths = self.rdr.paths();
        let mut cand: Option<Vec<u32>> = None;
        match pf {
            crate::regex_analyze::Prefilter::Conj(tris) => {
                for t in tris {
                    if let Some(docs) = self.rdr.content_term_docs(&t) {
                        cand = Some(match cand {
                            None => docs,
                            Some(prev) => intersect_sorted(&prev, &docs),
                        });
                    } else {
                        return vec![];
                    }
                }
            }
            crate::regex_analyze::Prefilter::Disj(disj) => {
                use std::collections::BTreeSet;
                let mut set: BTreeSet<u32> = BTreeSet::new();
                for tris in disj {
                    let mut branch_cand: Option<Vec<u32>> = None;
                    let mut branch_ok = true;
                    for t in tris {
                        if let Some(docs) = self.rdr.content_term_docs(&t) {
                            branch_cand = Some(match branch_cand {
                                None => docs,
                                Some(prev) => intersect_sorted(&prev, &docs),
                            });
                        } else {
                            branch_ok = false;
                            break;
                        }
                    }
                    if branch_ok {
                        if let Some(bc) = branch_cand {
                            for d in bc {
                                set.insert(d);
                            }
                        }
                    }
                }
                cand = Some(set.into_iter().collect());
            }
            _ => {
                cand = Some((0..paths.len() as u32).collect());
            }
        }
        cand.unwrap_or_default()
            .into_iter()
            .map(|d| (d, paths[d as usize].clone()))
            .collect()
    }

    // The larger, context-aware searchers remain unchanged from the original
    // implementation; they call helpers in the parent `shard` module.
    pub fn search_literal_with_context(&self, needle: &str) -> Vec<super::SearchMatch> {
        self.search_literal_with_context_opts(needle, &super::SearchOpts::default())
    }

    pub fn search_literal_with_context_opts(
        &self,
        needle: &str,
        opts: &super::SearchOpts,
    ) -> Vec<super::SearchMatch> {
        if needle.is_empty() {
            return Vec::new();
        }
        let tris: Vec<_> = trigrams(needle).collect();
        let mut cand: Option<Vec<u32>> = None;
        if !tris.is_empty() {
            if let Ok(pm) = self.rdr.postings_map() {
                for t in tris {
                    if let Some(v) = pm.get(&t) {
                        let docs: Vec<u32> = v.keys().copied().collect();
                        cand = Some(match cand {
                            None => docs,
                            Some(prev) => super::intersect_sorted(&prev, &docs),
                        });
                    } else {
                        return Vec::new();
                    }
                }
            } else {
                return Vec::new();
            }
        } else {
            cand = Some((0..self.rdr.paths().len() as u32).collect());
        }

        let mut out: Vec<super::SearchMatch> = Vec::new();
        let file_len_cache = |m: &Mmap| m.len();

        for d in cand.unwrap_or_default() {
            if let Some(mm) = self.get_doc_mmap(d) {
                let s = match std::str::from_utf8(&mm[..]) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let mut start_pos = 0usize;
                while let Some(found) = s[start_pos..].find(needle) {
                    let abs = start_pos + found;
                    let end = abs + needle.len();
                    let starts = self.rdr.load_line_starts(d).unwrap_or_default();
                    let (line_idx, line_start) = if !starts.is_empty() {
                        super::line_for_offset(&starts, abs as u32)
                    } else {
                        (0usize, 0u32)
                    };
                    let (lb, le) = super::line_bounds(&starts, line_idx, file_len_cache(&mm));
                    let line_text = s[lb..le].to_string();
                    let before_raw = &s[line_start as usize..abs];
                    let after_raw = &s[end..le];
                    let before = super::trim_last_n_chars(before_raw, opts.context, true);
                    let after = super::trim_first_n_chars(after_raw, opts.context, true);
                    out.push(super::SearchMatch {
                        doc: d,
                        path: self.rdr.paths()[d as usize].clone(),
                        line: (line_idx + 1) as u32,
                        start: abs as u32,
                        end: end as u32,
                        before,
                        line_text,
                        after,
                        score: 0.0,
                    });
                    start_pos = end;
                }
            }
        }
        out
    }

    pub fn search_regex_confirmed(
        &self,
        pattern: &str,
        opts: &super::SearchOpts,
    ) -> Vec<super::SearchMatch> {
        let re = match Regex::new(pattern) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        // Use prefilter to produce candidate docs when possible
        let pf = prefilter_from_regex(pattern);
        let paths = self.rdr.paths();
        let mut cand: Option<Vec<u32>> = None;
        match pf {
            crate::regex_analyze::Prefilter::Conj(tris) => {
                for t in tris {
                    if let Some(docs) = self.rdr.content_term_docs(&t) {
                        cand = Some(match cand {
                            None => docs,
                            Some(prev) => super::intersect_sorted(&prev, &docs),
                        });
                    } else {
                        return Vec::new();
                    }
                }
            }
            crate::regex_analyze::Prefilter::Disj(disj) => {
                use std::collections::BTreeSet;
                let mut set: BTreeSet<u32> = BTreeSet::new();
                for tris in disj {
                    let mut branch_cand: Option<Vec<u32>> = None;
                    let mut branch_ok = true;
                    for t in tris {
                        if let Some(docs) = self.rdr.content_term_docs(&t) {
                            branch_cand = Some(match branch_cand {
                                None => docs,
                                Some(prev) => super::intersect_sorted(&prev, &docs),
                            });
                        } else {
                            branch_ok = false;
                            break;
                        }
                    }
                    if branch_ok {
                        if let Some(bc) = branch_cand {
                            for d in bc {
                                set.insert(d);
                            }
                        }
                    }
                }
                cand = Some(set.into_iter().collect());
            }
            _ => {
                cand = Some((0..paths.len() as u32).collect());
            }
        }

        let mut out: Vec<super::SearchMatch> = Vec::new();
        for d in cand.unwrap_or_default() {
            if let Some(mm) = self.get_doc_mmap(d) {
                let s = match std::str::from_utf8(&mm[..]) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                for m in re.find_iter(s) {
                    let abs = m.start();
                    let end = m.end();
                    let starts = self.rdr.load_line_starts(d).unwrap_or_default();
                    let (line_idx, line_start) = if !starts.is_empty() {
                        super::line_for_offset(&starts, abs as u32)
                    } else {
                        (0usize, 0u32)
                    };
                    let (lb, le) = super::line_bounds(&starts, line_idx, mm.len());
                    let line_text = s[lb..le].to_string();
                    let before_raw = &s[line_start as usize..abs];
                    let after_raw = &s[end..le];
                    let before = super::trim_last_n_chars(before_raw, opts.context, true);
                    let after = super::trim_first_n_chars(after_raw, opts.context, true);
                    out.push(super::SearchMatch {
                        doc: d,
                        path: self.rdr.paths()[d as usize].clone(),
                        line: (line_idx + 1) as u32,
                        start: abs as u32,
                        end: end as u32,
                        before,
                        line_text,
                        after,
                        score: 0.0,
                    });
                }
            }
        }
        out
    }

    pub fn search_symbols_prefiltered(
        &self,
        pattern: Option<&str>,
        is_regex: bool,
        case_sensitive: bool,
    ) -> Vec<crate::query::QueryResult> {
        let mut out: Vec<crate::query::QueryResult> = Vec::new();
        let paths = self.rdr.paths();
        let re = if let Some(pat) = pattern {
            if is_regex {
                if case_sensitive {
                    Regex::new(pat)
                } else {
                    Regex::new(&format!("(?i){}", pat))
                }
                .ok()
            } else {
                None
            }
        } else {
            None
        };

        let needle_lower = pattern.map(|p| p.to_lowercase());

        for d in 0..paths.len() as u32 {
            if let Some(syms) = self.rdr.symbols_for_doc(d) {
                for (name, start, line) in syms.iter() {
                    let ok = match (pattern, is_regex) {
                        (None, _) => true,
                        (Some(_), true) => {
                            if let Some(r) = &re {
                                r.is_match(name)
                            } else {
                                false
                            }
                        }
                        (Some(pat), false) => {
                            if case_sensitive {
                                name.contains(pat)
                            } else {
                                name.to_lowercase().contains(needle_lower.as_ref().unwrap())
                            }
                        }
                    };
                    if ok {
                        out.push(crate::query::QueryResult {
                            doc: d as crate::index::RepoDocId,
                            path: paths[d as usize].clone(),
                            symbol: Some(name.clone()),
                            symbol_loc: Some(crate::types::Symbol {
                                name: name.clone(),
                                start: *start,
                                line: *line,
                            }),
                            score: 2.0, // Higher score for symbol matches
                        });
                    }
                }
            }
        }
        out
    }
}
