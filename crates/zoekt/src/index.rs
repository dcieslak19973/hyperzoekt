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
    /// symbol term map (lowercased symbol -> doc ids)
    pub symbol_terms: HashMap<String, Vec<RepoDocId>>,
    /// trigram -> doc ids for symbols (used as a prefilter)
    pub symbol_trigrams: HashMap<[u8; 3], Vec<RepoDocId>>,
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
            branches: vec!["HEAD".to_string()],
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
            let lang = detect_lang_from_ext(&rel);
            docs.push(DocumentMeta {
                path: rel,
                lang,
                size: size as u64,
                branches: vec!["HEAD".to_string()],
                symbols: Vec::new(),
            });
        }

        // Very naive tokenization: split on non-word, build term->docids map
        let mut acc: HashMap<String, Vec<RepoDocId>> = HashMap::new();
        let mut symbol_terms: HashMap<String, Vec<RepoDocId>> = HashMap::new();
        let mut symbol_trigrams: HashMap<[u8; 3], Vec<RepoDocId>> = HashMap::new();
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
                // naive symbol extraction will be attached after the main loop
            }
        }

        // Re-open files to populate per-doc branches (HEAD) and symbols properly
        for meta in docs.iter_mut() {
            meta.branches = vec!["HEAD".to_string()];
            let path = self.root.join(&meta.path);
            if let Ok(s) = fs::read_to_string(&path) {
                meta.symbols = extract_symbols(
                    &s,
                    meta.path.extension().and_then(|s| s.to_str()).unwrap_or(""),
                );
            } else {
                meta.symbols = Vec::new();
            }
        }

        // After symbols are populated in `docs`, build symbol term/trigram maps
        for (i, meta) in docs.iter().enumerate() {
            for sym in &meta.symbols {
                let key = sym.name.to_lowercase();
                symbol_terms
                    .entry(key.clone())
                    .or_default()
                    .push(i as RepoDocId);
                // trigram prefilter on symbol name
                for tri in crate::trigram::trigrams(&sym.name) {
                    symbol_trigrams.entry(tri).or_default().push(i as RepoDocId);
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
            symbol_terms,
            symbol_trigrams,
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
        // allow Unicode identifier characters using XID properties
        let re_fn = Regex::new(r"\bfn\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap();
        let re_struct = Regex::new(r"\bstruct\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap();
        for cap in re_fn.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                out.push(crate::types::Symbol {
                    name: m.as_str().to_string(),
                    start: Some(m.start() as u32),
                    line: Some(line_for_offset(content, m.start() as u32) as u32 + 1),
                });
            }
        }
        for cap in re_struct.captures_iter(content) {
            if let Some(m) = cap.get(1) {
                out.push(crate::types::Symbol {
                    name: m.as_str().to_string(),
                    start: Some(m.start() as u32),
                    line: Some(line_for_offset(content, m.start() as u32) as u32 + 1),
                });
            }
        }
    } else if ext == "py" {
        let re = Regex::new(r"^(?:\s*)(?:def|class)\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap();
        let mut offset = 0usize;
        for line in content.lines() {
            if let Some(cap) = re.captures(line) {
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
        let re = Regex::new(r"\bfunc(?:\s*\(.*?\))?\s+(\p{XID_Start}\p{XID_Continue}*)").unwrap();
        for cap in re.captures_iter(content) {
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
