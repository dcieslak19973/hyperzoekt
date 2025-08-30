use crate::types::{DocumentMeta, RepoMeta};
use std::collections::HashMap;
use std::path::PathBuf;

use super::in_memory::{InMemoryIndex, InMemoryIndexInner, RepoDocId};
use super::process::{process_pending, PendingFile};
use super::utils::*;

pub struct IndexBuilder {
    root: PathBuf,
    include: Option<regex::Regex>,
    exclude: Option<regex::Regex>,
    max_file_size: usize,
    follow_symlinks: bool,
    include_hidden: bool,
    branches: Option<Vec<String>>,
    max_branches: usize,
    enable_symbols: bool,
    thread_cap: Option<usize>,
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
            branches: None,
            max_branches: 1,
            enable_symbols: true,
            thread_cap: None,
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

    pub fn include_regex(mut self, re: regex::Regex) -> Self {
        self.include = Some(re);
        self
    }

    pub fn branches(mut self, bs: Vec<String>) -> Self {
        self.branches = Some(bs);
        self
    }

    pub fn max_branches(mut self, n: usize) -> Self {
        self.max_branches = n.max(1);
        self
    }

    pub fn exclude_regex(mut self, re: regex::Regex) -> Self {
        self.exclude = Some(re);
        self
    }

    pub fn enable_symbols(mut self, enable: bool) -> Self {
        self.enable_symbols = enable;
        self
    }

    pub fn index_threads(mut self, n: usize) -> Self {
        self.thread_cap = Some(n.max(1));
        self
    }

    pub fn build(self) -> anyhow::Result<InMemoryIndex> {
        // Repo meta will reflect the list of branches we indexed (or HEAD by default)
        let mut repo_branches: Vec<String> = vec!["HEAD".to_string()];
        if let Some(bs) = &self.branches {
            repo_branches = bs.clone();
        } else if let Some(chosen) =
            crate::index::git::choose_branches(&self.root, self.max_branches)
        {
            repo_branches = chosen;
        }
        let repo = RepoMeta {
            name: self
                .root
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            root: self.root.clone(),
            branches: repo_branches.clone(),
        };

        let mut pending: Vec<PendingFile> = Vec::new();
        let mut _branch_tempdirs: Vec<tempfile::TempDir> = Vec::new();

        if let Some(bs) = &self.branches {
            for b in bs {
                let td = crate::index::git::extract_branch_to_tempdir(&self.root, b)?;

                let mut builder = ignore::WalkBuilder::new(td.path());
                builder.hidden(!self.include_hidden);
                builder.follow_links(self.follow_symlinks);
                builder.git_ignore(true);
                let walker = builder.build();
                for result in walker
                    .filter_map(Result::ok)
                    .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
                {
                    let rel = pathdiff::diff_paths(result.path(), td.path())
                        .unwrap_or_else(|| PathBuf::from(result.file_name()));
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
                    let lang = detect_lang_from_ext(&rel);
                    pending.push(PendingFile {
                        rel,
                        size,
                        lang,
                        branches: vec![b.clone()],
                        base: Some(td.path().to_path_buf()),
                    });
                }
                _branch_tempdirs.push(td);
            }
        } else {
            let mut builder = ignore::WalkBuilder::new(&self.root);
            builder.hidden(!self.include_hidden);
            builder.follow_links(self.follow_symlinks);
            builder.git_ignore(true);
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
                let lang = detect_lang_from_ext(&rel);
                pending.push(PendingFile {
                    rel,
                    size,
                    lang,
                    branches: vec!["HEAD".to_string()],
                    base: None,
                });
            }
        }

        let enable_symbols = self.enable_symbols;
        let root = self.root.clone();
        let processed = process_pending(pending, root.clone(), enable_symbols, self.thread_cap);

        let mut processed = processed;
        processed.sort_by_key(|p| p.idx);

        let mut docs: Vec<DocumentMeta> = Vec::with_capacity(processed.len());
        let mut doc_contents: Vec<Option<String>> = Vec::with_capacity(processed.len());
        let mut terms: HashMap<String, Vec<RepoDocId>> = HashMap::new();
        let mut symbol_terms: HashMap<String, Vec<RepoDocId>> = HashMap::new();
        let mut symbol_trigrams: HashMap<[u8; 3], Vec<RepoDocId>> = HashMap::new();

        for (i, p) in processed.into_iter().enumerate() {
            let doc_id = i as RepoDocId;
            if let Some(ref text) = p.content {
                let mut seen: fnv::FnvHashSet<String> = fnv::FnvHashSet::default();
                for tok in text.split(|c: char| !c.is_alphanumeric() && c != '_') {
                    if tok.is_empty() {
                        continue;
                    }
                    seen.insert(tok.to_lowercase());
                }
                for tok in seen.into_iter() {
                    terms.entry(tok).or_default().push(doc_id);
                }
            }

            if enable_symbols {
                for key in p.sym_names {
                    symbol_terms.entry(key).or_default().push(doc_id);
                }
                for sym in &p.doc.symbols {
                    for tri in crate::trigram::trigrams(&sym.name) {
                        symbol_trigrams.entry(tri).or_default().push(doc_id);
                    }
                }
            }
            docs.push(p.doc);
            if p.keep_content {
                doc_contents.push(p.content);
            } else {
                doc_contents.push(None);
            }
        }

        for (_k, v) in symbol_terms.iter_mut() {
            v.sort_unstable();
            v.dedup();
        }
        for (_k, v) in symbol_trigrams.iter_mut() {
            v.sort_unstable();
            v.dedup();
        }

        let inner = InMemoryIndexInner {
            repo,
            docs,
            terms,
            symbol_terms,
            symbol_trigrams,
            doc_contents,
        };
        Ok(InMemoryIndex::from_inner(inner))
    }
}
