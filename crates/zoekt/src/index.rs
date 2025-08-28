use crate::types::{DocumentMeta, RepoMeta};
use parking_lot::RwLock;
use regex::Regex;
use std::{collections::HashMap, fs, path::PathBuf};

pub type RepoDocId = u32;

/// Minimal in-memory inverted index (token -> doc ids)
#[derive(Debug)]
pub struct InMemoryIndexInner {
    pub repo: RepoMeta,
    pub docs: Vec<DocumentMeta>,
    pub terms: HashMap<String, Vec<RepoDocId>>, // unsorted, no positions (yet)
}

#[derive(Debug, Clone)]
pub struct InMemoryIndex {
    inner: std::sync::Arc<RwLock<InMemoryIndexInner>>,
}

impl InMemoryIndex {
    pub fn search_literal(&self, needle: &str) -> Vec<(RepoDocId, String)> {
        let inner = self.inner.read();
        // naive: return docs that contain the needle anywhere
        inner
            .docs
            .iter()
            .enumerate()
            .filter_map(|(i, meta)| {
                let path = inner.repo.root.join(&meta.path);
                if let Ok(text) = fs::read_to_string(&path) {
                    if text.contains(needle) {
                        return Some((i as RepoDocId, meta.path.display().to_string()));
                    }
                }
                None
            })
            .collect()
    }

    pub(crate) fn read_inner(&self) -> parking_lot::RwLockReadGuard<'_, InMemoryIndexInner> {
        self.inner.read()
    }

    pub fn doc_count(&self) -> usize {
        self.inner.read().docs.len()
    }

    /// Return total bytes (sum of per-doc sizes) in the in-memory index
    pub fn total_scanned_bytes(&self) -> u64 {
        let inner = self.inner.read();
        inner.docs.iter().map(|d| d.size).sum()
    }
}

pub struct IndexBuilder {
    root: PathBuf,
    include: Option<Regex>,
    exclude: Option<Regex>,
    max_file_size: usize,
    follow_symlinks: bool,
    include_hidden: bool,
}

impl IndexBuilder {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            include: None,
            exclude: None,
            max_file_size: 1_000_000,
            follow_symlinks: false,
            include_hidden: false,
        }
    }

    pub fn max_file_size(mut self, sz: usize) -> Self {
        self.max_file_size = sz;
        self
    }

    pub fn follow_symlinks(mut self, follow: bool) -> Self {
        self.follow_symlinks = follow;
        self
    }

    pub fn include_hidden(mut self, include: bool) -> Self {
        self.include_hidden = include;
        self
    }

    pub fn include_regex(mut self, re: Regex) -> Self {
        self.include = Some(re);
        self
    }
    pub fn exclude_regex(mut self, re: Regex) -> Self {
        self.exclude = Some(re);
        self
    }

    pub fn build(self) -> anyhow::Result<InMemoryIndex> {
        let repo = RepoMeta {
            name: self
                .root
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            root: self.root.clone(),
        };
        let mut docs = Vec::new();

        let mut builder = ignore::WalkBuilder::new(&self.root);
        // WalkBuilder.hidden(true) makes hidden files be ignored. We expose include_hidden on the builder
        // so invert the flag: when include_hidden==true, set hidden(false) to include them.
        builder.hidden(!self.include_hidden);
        builder.follow_links(self.follow_symlinks);
        builder.git_ignore(true);
        // If there's a .gitignore in the repo root, read simple patterns for quick basename matching
        let mut ignore_patterns: Vec<String> = Vec::new();
        if let Ok(gitignore_content) = std::fs::read_to_string(self.root.join(".gitignore")) {
            for line in gitignore_content.lines() {
                let pat = line.trim();
                if pat.is_empty() || pat.starts_with('#') {
                    continue;
                }
                ignore_patterns.push(pat.to_string());
            }
        }
        let walker = builder.build();
        for result in walker
            .filter_map(Result::ok)
            .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        {
            let rel = pathdiff::diff_paths(result.path(), &self.root)
                .unwrap_or_else(|| PathBuf::from(result.file_name()));
            // apply simple .gitignore basename/leading-slash matching
            if !ignore_patterns.is_empty() {
                let rel_s = rel.to_string_lossy();
                let base = result
                    .path()
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string());
                let mut skipped = false;
                for pat in &ignore_patterns {
                    if pat.starts_with('/') {
                        let p = pat.trim_start_matches('/');
                        if rel_s == p {
                            skipped = true;
                            break;
                        }
                    } else if let Some(b) = &base {
                        if b == pat {
                            skipped = true;
                            break;
                        }
                    }
                }
                if skipped {
                    continue;
                }
            }
            if let Some(inc) = &self.include {
                if !inc.is_match(rel.to_string_lossy().as_ref()) {
                    continue;
                }
            }
            if let Some(exc) = &self.exclude {
                if exc.is_match(rel.to_string_lossy().as_ref()) {
                    continue;
                }
            }
            let size = result.metadata().map(|m| m.len()).unwrap_or(0) as usize;
            if size > self.max_file_size {
                continue;
            }
            docs.push(DocumentMeta {
                path: rel,
                lang: None,
                size: size as u64,
            });
        }

        // Very naive tokenization: split on non-word, build term->docids map
        let mut acc: HashMap<String, Vec<RepoDocId>> = HashMap::new();
        // Tokenize only text-like files. Use a small heuristic to skip binary files.
        for (i, meta) in docs.iter().enumerate() {
            let path = self.root.join(&meta.path);
            if let Ok(bytes) = fs::read(&path) {
                if !is_text(&bytes) {
                    continue;
                }
                let text = String::from_utf8_lossy(&bytes);
                let mut seen: fnv::FnvHashSet<String> = fnv::FnvHashSet::default();
                for tok in text.split(|c: char| !c.is_alphanumeric() && c != '_') {
                    if tok.is_empty() {
                        continue;
                    }
                    let tok = tok.to_lowercase();
                    seen.insert(tok);
                }
                for tok in seen.into_iter() {
                    acc.entry(tok).or_default().push(i as RepoDocId);
                }
            }
        }

        fn is_text(buf: &[u8]) -> bool {
            // Reject if NUL present
            if buf.contains(&0) {
                return false;
            }
            // Heuristic: if a high fraction of bytes are non-printable (excluding common UTF-8), treat as binary
            let sample = &buf[..std::cmp::min(buf.len(), 4096)];
            let mut non_print = 0usize;
            for &b in sample {
                if b >= 0x20 || b == b'\n' || b == b'\r' || b == b'\t' {
                    // printable or whitespace
                } else {
                    non_print += 1;
                }
            }
            let ratio = non_print as f64 / sample.len() as f64;
            ratio < 0.30
        }
        let inner = InMemoryIndexInner {
            repo,
            docs,
            terms: acc,
        };
        Ok(InMemoryIndex {
            inner: std::sync::Arc::new(RwLock::new(inner)),
        })
    }
}
