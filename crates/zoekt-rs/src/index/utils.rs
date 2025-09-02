// Copyright 2025 HyperZoekt Project
// Derived from sourcegraph/zoekt (https://github.com/sourcegraph/zoekt)
// Copyright 2016 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use git2::Repository;
use once_cell::sync::Lazy;
use regex::Regex;
use std::io::Write;

/// Heuristic: decide whether a byte buffer should be considered text.
pub(crate) fn is_text(buf: &[u8]) -> bool {
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

/// Helper to compute line index (0-based) for a byte offset within content
pub(crate) fn line_for_offset(content: &str, pos: u32) -> usize {
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

pub(crate) fn detect_lang_from_ext(path: &std::path::Path) -> Option<String> {
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

/// Extract the tree for `branch` from a repo at `repo_path` into `dst` using libgit2.
/// This helper intentionally errs instead of panicking so callers can fall back
/// to the external `git archive | tar` flow.
pub(crate) fn extract_branch_tree_libgit2(
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

/// Extract symbols using typesitter when available, otherwise fall back to regex-based extraction.
pub(crate) fn extract_symbols(content: &str, ext: &str) -> Vec<crate::types::Symbol> {
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

// (line_for_offset is defined above and is used by the regex extractors)
