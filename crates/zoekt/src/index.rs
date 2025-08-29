use crate::types::{DocumentMeta, RepoMeta};
use git2::Repository;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use regex::Regex;
use std::io::Write;
use std::{collections::HashMap, fs, path::PathBuf};

pub type RepoDocId = u32;

/// Minimal in-memory inverted index (token -> doc ids)
#[derive(Debug)]
pub struct InMemoryIndexInner {
    pub repo: RepoMeta,
    pub docs: Vec<DocumentMeta>,
    pub terms: HashMap<String, Vec<RepoDocId>>, // unsorted, no positions (yet)
    /// symbol term map (lowercased symbol -> doc ids)
    pub symbol_terms: HashMap<String, Vec<RepoDocId>>,
    /// trigram -> doc ids for symbols (used as a prefilter)
    pub symbol_trigrams: HashMap<[u8; 3], Vec<RepoDocId>>,
    /// Optional in-memory per-doc content for branch-extracted documents
    pub doc_contents: Vec<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct InMemoryIndex {
    inner: std::sync::Arc<RwLock<InMemoryIndexInner>>,
}

impl InMemoryIndex {
    pub fn search_literal(&self, needle: &str) -> Vec<(RepoDocId, String)> {
        let inner = self.inner.read();
        // Prefer in-memory per-doc contents (from branch extraction) when available
        // to avoid re-reading files from disk.
        inner
            .docs
            .iter()
            .enumerate()
            .filter_map(|(i, meta)| {
                if let Some(opt) = inner.doc_contents.get(i) {
                    if let Some(text) = opt.as_ref() {
                        if text.contains(needle) {
                            return Some((i as RepoDocId, meta.path.display().to_string()));
                        }
                        return None;
                    }
                }
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
    branches: Option<Vec<String>>,
    /// When `branches` is not explicitly set, select at most this many branches
    /// from the repository (default: 1). If the repository cannot be opened
    /// as a git repo we fall back to indexing the working tree as before.
    max_branches: usize,
    /// Enable symbol extraction (can be disabled to speed indexing)
    enable_symbols: bool,
    /// Optional cap on indexing threads (defaults to min(avail_cpus, 8));
    /// can also be provided via env ZOEKT_INDEX_THREADS when not set here.
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

    pub fn include_regex(mut self, re: Regex) -> Self {
        self.include = Some(re);
        self
    }
    /// Index specific branches. When set, the indexer will extract each branch's
    /// tree via `git archive` into a temporary directory and index the files as
    /// separate documents tagged with that branch name. This produces per-branch
    /// documents (duplicate paths across branches are indexed separately).
    pub fn branches(mut self, bs: Vec<String>) -> Self {
        self.branches = Some(bs);
        self
    }

    /// Limit the number of branches to index when `branches` was not set.
    /// Defaults to 1 (only the repository's default branch).
    pub fn max_branches(mut self, n: usize) -> Self {
        self.max_branches = n.max(1);
        self
    }
    pub fn exclude_regex(mut self, re: Regex) -> Self {
        self.exclude = Some(re);
        self
    }

    /// Enable or disable symbol extraction during indexing (default: enabled).
    pub fn enable_symbols(mut self, enable: bool) -> Self {
        self.enable_symbols = enable;
        self
    }

    /// Set a cap on indexing threads. When not provided, we'll use the
    /// environment variable ZOEKT_INDEX_THREADS if set, otherwise
    /// min(available_parallelism, 8).
    pub fn index_threads(mut self, n: usize) -> Self {
        self.thread_cap = Some(n.max(1));
        self
    }

    pub fn build(self) -> anyhow::Result<InMemoryIndex> {
        // Repo meta will reflect the list of branches we indexed (or HEAD by default)
        let mut repo_branches: Vec<String> = vec!["HEAD".to_string()];
        // If branches were not explicitly provided, try to discover them from git
        // and pick up to `max_branches` tips (default branch + most recently updated).
        if let Some(bs) = &self.branches {
            repo_branches = bs.clone();
        } else {
            // Attempt to open repository and enumerate refs; if anything fails,
            // fall back to HEAD-only behavior.
            if let Ok(repo) = Repository::open(&self.root) {
                // Determine default branch (symbolic-ref HEAD), fall back to "HEAD"
                let mut chosen: Vec<String> = Vec::new();
                if let Ok(head) = repo.head() {
                    if let Some(name) = head.shorthand() {
                        chosen.push(name.to_string());
                    }
                }

                // Collect local branch tips with their commit times
                let mut tips: Vec<(String, i64)> = Vec::new();
                if let Ok(mut refs) = repo.references() {
                    while let Some(Ok(r)) = refs.next() {
                        if let Some(name) = r.shorthand() {
                            // consider only local heads (refs/heads/*)
                            if let Some(rname) = r.name() {
                                if rname.starts_with("refs/heads/") {
                                    // resolve the ref to a commit
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
                // sort by commit time desc (most recent first)
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

        // We'll gather file descriptors first, then process them in parallel with a single
        // read per file (tokenization + optional symbols), then assemble docs and maps.
        struct PendingFile {
            rel: PathBuf,
            size: usize,
            lang: Option<String>,
            branches: Vec<String>,
            // Source of file content: either from an extracted branch tree (Some base path)
            // or None meaning read from repo working tree.
            base: Option<PathBuf>,
        }
        let mut pending: Vec<PendingFile> = Vec::new();
        // Hold branch extraction tempdirs alive until processing completes
        let mut _branch_tempdirs: Vec<tempfile::TempDir> = Vec::new();

        // If branches were specified, create a per-branch working tree via git archive
        // and index files from each extracted tree, tagging DocumentMeta.branches accordingly.
        if let Some(bs) = &self.branches {
            for b in bs {
                // Create a tempdir for the branch tree
                let td = tempfile::tempdir()?;
                // First try to extract using libgit2 to avoid shelling out to `tar` and `git`.
                // If libgit2 extraction fails, fall back to the existing `git archive | tar -x` flow.
                let libgit2_ok = extract_branch_tree_libgit2(&self.root, b, td.path());
                if let Err(_e) = libgit2_ok {
                    // fallback to external git|tar pipeline
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
                    // pipe data
                    if let Some(mut tar_stdin) = tar.stdin.take() {
                        std::io::copy(&mut std::io::BufReader::new(git_stdout), &mut tar_stdin)?;
                    }
                    let git_status = git.wait()?;
                    let tar_status = tar.wait()?;
                    if !git_status.success() || !tar_status.success() {
                        // extraction failed; skip this branch
                        continue;
                    }
                }

                // Walk the extracted tree similarly to the main walker but rooted at td.path()
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
                // end branch walker
                // keep the extracted tree alive
                _branch_tempdirs.push(td);
            }
        } else {
            // No branches specified: index working tree at self.root as before
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

        // Parallel processing of files with a bounded rayon pool
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
        // Cap the rayon pool to avoid unbounded parallel IO.
        // Priority: explicit builder setting -> env ZOEKT_INDEX_THREADS -> min(avail, 8)
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
                            // compute full path
                            let fullp = match &pf.base {
                                Some(base) => base.join(&pf.rel),
                                None => root.join(&pf.rel),
                            };
                            // single read (avoid reading twice)
                            let bytes = std::fs::read(&fullp).ok();
                            let mut doc = DocumentMeta {
                                path: pf.rel.clone(),
                                lang: pf.lang.clone(),
                                size: bytes.as_ref().map(|b| b.len()).unwrap_or(pf.size) as u64,
                                branches: pf.branches.clone(),
                                symbols: Vec::new(),
                            };
                            let mut symbol_terms: Vec<String> = Vec::new(); // names only; merge later
                            let mut content: Option<String> = None;
                            if let Some(b) = &bytes {
                                // Decode to UTF-8 lossy once
                                let text = String::from_utf8_lossy(b).into_owned();
                                // Tokenization only for text-like files
                                if is_text(b) {
                                    content = Some(text.clone());
                                }
                                if enable_symbols {
                                    // Extract symbols even if the file contains NULs; parser can handle/skip
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
                // Fallback to default rayon pool
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

        // Merge processed outputs in original order to keep doc ids stable
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
                // build term set per doc, then add
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

        // Ensure symbol term/posting lists are sorted and deduplicated to
        // provide deterministic serialization and correct prefilter semantics.
        for (_k, v) in symbol_terms.iter_mut() {
            v.sort_unstable();
            v.dedup();
        }
        for (_k, v) in symbol_trigrams.iter_mut() {
            v.sort_unstable();
            v.dedup();
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
        // debug prints removed

        let inner = InMemoryIndexInner {
            repo,
            docs,
            terms,
            symbol_terms,
            symbol_trigrams,
            doc_contents,
        };
        Ok(InMemoryIndex {
            inner: std::sync::Arc::new(RwLock::new(inner)),
        })
    }
}

fn extract_symbols(content: &str, ext: &str) -> Vec<crate::types::Symbol> {
    // Prefer a Tree-sitter based extractor when available; fall back to
    // the simpler regex-based extractor implemented below.
    let out = crate::typesitter::extract_symbols_typesitter(content, ext);
    if !out.is_empty() {
        return out;
    }

    // Fallback to regex-based extraction for languages not supported by
    // the Tree-sitter extractor or when parsing fails.
    let mut out = Vec::new();
    if ext == "rs" {
        // allow Unicode identifier characters using XID properties (cached regexes)
        static RE_RS_FN: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"\bfn\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap());
        static RE_RS_STRUCT: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"\bstruct\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap());
        for cap in RE_RS_FN.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                out.push(crate::types::Symbol {
                    name: m.as_str().to_string(),
                    start: Some(m.start() as u32),
                    line: Some(line_for_offset(content, m.start() as u32) as u32 + 1),
                });
            }
        }
        for cap in RE_RS_STRUCT.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                out.push(crate::types::Symbol {
                    name: m.as_str().to_string(),
                    start: Some(m.start() as u32),
                    line: Some(line_for_offset(content, m.start() as u32) as u32 + 1),
                });
            }
        }
    } else if ext == "py" {
        static RE_PY: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"^(?:\s*)(?:def|class)\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap()
        });
        let mut offset = 0usize;
        for line in content.lines() {
            if let Some(cap) = RE_PY.captures(line) {
                if let Some(m) = cap.get(1) {
                    out.push(crate::types::Symbol {
                        name: m.as_str().to_string(),
                        start: Some((offset + m.start()) as u32),
                        line: Some((out.len() as u32) + 1),
                    });
                }
            }
            offset += line.len() + 1; // approximate line length + newline
        }
    } else if ext == "go" {
        static RE_GO_FN: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"\bfunc(?:\s*\(.*?\))?\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap()
        });
        for cap in RE_GO_FN.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                out.push(crate::types::Symbol {
                    name: m.as_str().to_string(),
                    start: Some(m.start() as u32),
                    line: Some(line_for_offset(content, m.start() as u32) as u32 + 1),
                });
            }
        }
    }
    out
}

// Helper to compute line index (0-based) for a byte offset within content
fn line_for_offset(content: &str, pos: u32) -> usize {
    let bytes = content.as_bytes();
    let mut idx = 0usize;
    let mut line = 0usize;
    while idx < bytes.len() && (idx as u32) < pos {
        if bytes[idx] == b'\n' {
            line += 1;
        }
        idx += 1;
    }
    line
}

fn detect_lang_from_ext(path: &std::path::Path) -> Option<String> {
    let ext = path.extension()?.to_string_lossy().to_lowercase();
    let lang = match ext.as_str() {
        "rs" => "rust",
        "go" => "go",
        "ts" | "tsx" => "typescript",
        "js" | "jsx" => "javascript",
        "py" => "python",
        "java" => "java",
        "cs" => "csharp",
        "c" => "c",
        "h" => "c",
        "cpp" | "cc" | "cxx" | "hpp" | "hxx" => "cpp",
        "rb" => "ruby",
        "php" => "php",
        "sh" | "bash" => "shell",
        "md" => "markdown",
        "yml" | "yaml" => "yaml",
        "toml" => "toml",
        "json" => "json",
        _ => return None,
    };
    Some(lang.to_string())
}

// Extract the tree for `branch` from a repo at `repo_path` into `dst`.
// This is a best-effort helper using libgit2; it intentionally errs instead of
// panicking so callers can fall back to the external `git archive | tar` flow.
fn extract_branch_tree_libgit2(
    repo_path: &std::path::Path,
    branch: &str,
    dst: &std::path::Path,
) -> anyhow::Result<()> {
    let repo = Repository::open(repo_path)?;
    // Resolve the reference for the branch name (allow tags, refs, simple names)
    let obj = repo.revparse_single(branch)?;
    let commit = obj.peel_to_commit()?;
    let tree = commit.tree()?;

    // Walk the tree entries and write blobs to dst preserving paths
    let walk_res = tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        if let Some(name) = entry.name() {
            // compute full path relative to dst
            let rel = std::path::Path::new(root).join(name);
            let full = dst.join(&rel);
            if let Ok(obj) = entry.to_object(&repo) {
                if obj.as_blob().is_some() {
                    if let Some(blob) = obj.as_blob() {
                        if let Some(parent) = full.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        match std::fs::File::create(&full) {
                            Ok(mut f) => {
                                let _ = f.write_all(blob.content());
                            }
                            Err(_) => {
                                // ignore write failures for best-effort extraction
                            }
                        }
                    }
                } else if obj.as_tree().is_some() {
                    let _ = std::fs::create_dir_all(&full);
                }
            }
        }
        0
    });
    walk_res?;
    Ok(())
}
