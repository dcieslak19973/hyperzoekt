use crate::types::{DocumentMeta, RepoMeta};
use std::collections::HashMap;
use std::path::PathBuf;

use super::in_memory::{InMemoryIndex, InMemoryIndexInner, RepoDocId};
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
        } else if let Ok(repo) = git2::Repository::open(&self.root) {
            let mut chosen: Vec<String> = Vec::new();
            if let Ok(head) = repo.head() {
                if let Some(name) = head.shorthand() {
                    chosen.push(name.to_string());
                }
            }
            let mut tips: Vec<(String, i64)> = Vec::new();
            if let Ok(mut refs) = repo.references() {
                while let Some(Ok(r)) = refs.next() {
                    if let Some(name) = r.shorthand() {
                        if let Some(rname) = r.name() {
                            if rname.starts_with("refs/heads/") {
                                if let Ok(resolved) = r.resolve() {
                                    if let Ok(target) = resolved.peel_to_commit() {
                                        let time = target.time().seconds();
                                        tips.push((name.to_string(), time));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            tips.sort_by(|a, b| b.1.cmp(&a.1));
            for (n, _) in tips.into_iter() {
                if chosen.contains(&n) {
                    continue;
                }
                if chosen.len() >= self.max_branches {
                    break;
                }
                chosen.push(n);
            }
            if !chosen.is_empty() {
                repo_branches = chosen;
            }
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

        struct PendingFile {
            rel: PathBuf,
            size: usize,
            lang: Option<String>,
            branches: Vec<String>,
            base: Option<PathBuf>,
        }
        let mut pending: Vec<PendingFile> = Vec::new();
        let mut _branch_tempdirs: Vec<tempfile::TempDir> = Vec::new();

        if let Some(bs) = &self.branches {
            for b in bs {
                let td = tempfile::tempdir()?;
                let libgit2_ok = extract_branch_tree_libgit2(&self.root, b, td.path());
                if let Err(_e) = libgit2_ok {
                    let mut git = std::process::Command::new("git")
                        .arg("-C")
                        .arg(&self.root)
                        .arg("archive")
                        .arg("--format=tar")
                        .arg(b)
                        .stdout(std::process::Stdio::piped())
                        .spawn()?;
                    let git_stdout = git.stdout.take().unwrap();
                    let mut tar = std::process::Command::new("tar")
                        .arg("-x")
                        .arg("-C")
                        .arg(td.path())
                        .stdin(std::process::Stdio::piped())
                        .spawn()?;
                    if let Some(mut tar_stdin) = tar.stdin.take() {
                        std::io::copy(&mut std::io::BufReader::new(git_stdout), &mut tar_stdin)?;
                    }
                    let git_status = git.wait()?;
                    let tar_status = tar.wait()?;
                    if !git_status.success() || !tar_status.success() {
                        continue;
                    }
                }

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

        use rayon::prelude::*;
        use rayon::ThreadPoolBuilder;
        let enable_symbols = self.enable_symbols;
        let root = self.root.clone();
        #[derive(Debug)]
        struct ProcessedDoc {
            idx: usize,
            doc: DocumentMeta,
            content: Option<String>,
            sym_names: Vec<String>,
            keep_content: bool,
        }
        let avail = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let default_cap = std::cmp::min(avail, 8).max(1);
        let env_cap = std::env::var("ZOEKT_INDEX_THREADS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|n| n.max(1));
        let cap = self
            .thread_cap
            .or(env_cap)
            .unwrap_or(default_cap)
            .min(avail)
            .max(1);
        let processed: Vec<ProcessedDoc> =
            if let Ok(pool) = ThreadPoolBuilder::new().num_threads(cap).build() {
                pool.install(|| {
                    pending
                        .par_iter()
                        .enumerate()
                        .map(|(idx, pf)| {
                            let fullp = match &pf.base {
                                Some(base) => base.join(&pf.rel),
                                None => root.join(&pf.rel),
                            };
                            let bytes = std::fs::read(&fullp).ok();
                            let mut doc = DocumentMeta {
                                path: pf.rel.clone(),
                                lang: pf.lang.clone(),
                                size: bytes.as_ref().map(|b| b.len()).unwrap_or(pf.size) as u64,
                                branches: pf.branches.clone(),
                                symbols: Vec::new(),
                            };
                            let mut symbol_terms: Vec<String> = Vec::new();
                            let mut content: Option<String> = None;
                            if let Some(b) = &bytes {
                                let text = String::from_utf8_lossy(b).into_owned();
                                if is_text(b) {
                                    content = Some(text.clone());
                                }
                                if enable_symbols {
                                    doc.symbols = extract_symbols(
                                        &text,
                                        doc.path.extension().and_then(|s| s.to_str()).unwrap_or(""),
                                    );
                                    symbol_terms =
                                        doc.symbols.iter().map(|s| s.name.to_lowercase()).collect();
                                }
                            }
                            let keep = pf.base.is_some();
                            ProcessedDoc {
                                idx,
                                doc,
                                content,
                                sym_names: symbol_terms,
                                keep_content: keep,
                            }
                        })
                        .collect()
                })
            } else {
                pending
                    .par_iter()
                    .enumerate()
                    .map(|(idx, pf)| {
                        let fullp = match &pf.base {
                            Some(base) => base.join(&pf.rel),
                            None => root.join(&pf.rel),
                        };
                        let bytes = std::fs::read(&fullp).ok();
                        let mut doc = DocumentMeta {
                            path: pf.rel.clone(),
                            lang: pf.lang.clone(),
                            size: bytes.as_ref().map(|b| b.len()).unwrap_or(pf.size) as u64,
                            branches: pf.branches.clone(),
                            symbols: Vec::new(),
                        };
                        let mut symbol_terms: Vec<String> = Vec::new();
                        let mut content: Option<String> = None;
                        if let Some(b) = &bytes {
                            let text = String::from_utf8_lossy(b).into_owned();
                            if is_text(b) {
                                content = Some(text.clone());
                            }
                            if enable_symbols {
                                doc.symbols = extract_symbols(
                                    &text,
                                    doc.path.extension().and_then(|s| s.to_str()).unwrap_or(""),
                                );
                                symbol_terms =
                                    doc.symbols.iter().map(|s| s.name.to_lowercase()).collect();
                            }
                        }
                        let keep = pf.base.is_some();
                        ProcessedDoc {
                            idx,
                            doc,
                            content,
                            sym_names: symbol_terms,
                            keep_content: keep,
                        }
                    })
                    .collect()
            };

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
