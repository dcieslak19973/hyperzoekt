// Copyright 2025 HyperZoekt Project
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

// intentionally no global imports required; keep helpers self-contained

pub fn extract_quoted(s: &str) -> Option<String> {
    let mut current = None;
    for (i, ch) in s.char_indices() {
        if ch == '"' || ch == '\'' {
            if current.is_none() {
                current = Some((ch, i));
            } else if let Some((qc, start)) = current {
                if qc == ch {
                    return Some(s[start + 1..i].to_string());
                }
            }
        }
    }
    None
}

pub fn between<'a>(s: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let a = s.find(start)? + start.len();
    let rest = &s[a..];
    let b = rest.find(end)?;
    Some(&rest[..b])
}

pub fn normalize_module_basename(module: &str) -> String {
    let last = module.rsplit('/').next().unwrap_or(module);
    let stem = last.split('.').next().unwrap_or(last);
    stem.to_string()
}

/// Extract import module basenames and their source line numbers from a file's source text.
/// Returns a Vec of (module_basename, line_number) pairs. Line numbers are 0-based.
pub fn extract_import_modules(lang: &str, source: &str) -> Vec<(String, usize)> {
    let mut vec: Vec<(String, usize)> = Vec::new();
    match lang {
        "python" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if let Some(rest) = t.strip_prefix("import ") {
                    for part in rest.split(',') {
                        let mut token = part.trim();
                        if let Some(idx) = token.find(" as ") {
                            token = &token[..idx];
                        }
                        token = token.split_whitespace().next().unwrap_or("");
                        if !token.is_empty() {
                            vec.push((token.to_string(), lineno));
                        }
                    }
                } else if t.starts_with("from ") {
                    if let Some(after_from) = t.strip_prefix("from ") {
                        if let Some((module, _imports)) = after_from.split_once(" import ") {
                            let mut mod_token = module.trim();
                            // strip leading relative dots (e.g. from .subpackage import x)
                            mod_token = mod_token.trim_start_matches('.');
                            // keep the full dotted module name (e.g. pkg.subpkg.module)
                            let mod_token = mod_token.split_whitespace().next().unwrap_or("");
                            if !mod_token.is_empty() {
                                vec.push((mod_token.to_string(), lineno));
                            }
                        }
                    }
                }
            }
        }
        "cpp" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if t.starts_with("#include \"") {
                    if let Some(start) = t.find('"') {
                        if let Some(end_rel) = t[start + 1..].find('"') {
                            let name = &t[start + 1..start + 1 + end_rel];
                            if !name.is_empty() {
                                vec.push((normalize_module_basename(name), lineno));
                            }
                        }
                    }
                }
            }
        }
        "java" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if let Some(mut rest) = t.strip_prefix("import ") {
                    if let Some(r) = rest.strip_prefix("static ") {
                        rest = r;
                    }
                    if let Some(semi) = rest.find(';') {
                        rest = &rest[..semi];
                    }
                    let last = rest.rsplit('.').next().unwrap_or(rest).trim();
                    if !last.is_empty() && last != "*" {
                        vec.push((last.to_string(), lineno));
                    }
                }
            }
        }
        "javascript" | "typescript" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if t.starts_with("import ") {
                    if let Some(idx) = t.find(" from ") {
                        let rest = &t[idx + 6..];
                        if let Some(m) = extract_quoted(rest) {
                            vec.push((normalize_module_basename(&m), lineno));
                        }
                    } else if t.starts_with("import ") && (t.contains('"') || t.contains('\'')) {
                        if let Some(m) = extract_quoted(t) {
                            vec.push((normalize_module_basename(&m), lineno));
                        }
                    }
                } else if t.contains("require(") {
                    if let Some(m) = between(t, "require(", ")") {
                        let m2 = m.trim_matches(&['"', '\''] as &[_]);
                        vec.push((normalize_module_basename(m2), lineno));
                    }
                }
            }
        }
        "go" => {
            let mut in_block = false;
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if t.starts_with("import (") {
                    in_block = true;
                    continue;
                }
                if t.starts_with("import ") {
                    if let Some(m) = extract_quoted(t) {
                        vec.push((normalize_module_basename(&m), lineno));
                    }
                    continue;
                }
                if in_block {
                    if t.starts_with(')') {
                        in_block = false;
                        continue;
                    }
                    if let Some(m) = extract_quoted(t) {
                        vec.push((normalize_module_basename(&m), lineno));
                    }
                }
            }
        }
        "rust" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if let Some(rest) = t.strip_prefix("mod ") {
                    let token = rest
                        .split(|c: char| c == ';' || c == '{' || c.is_whitespace())
                        .next()
                        .unwrap_or("");
                    if !token.is_empty() {
                        vec.push((token.to_string(), lineno));
                    }
                } else if let Some(after) = t.strip_prefix("use ") {
                    let mut first = after
                        .split(|c: char| c == ':' || c == ';' || c == '{' || c.is_whitespace())
                        .next()
                        .unwrap_or("");
                    if ["crate", "super", "self"].contains(&first) {
                        let remainder = after
                            .trim_start_matches(first)
                            .trim_start_matches(':')
                            .trim_start_matches(':');
                        first = remainder
                            .split(|c: char| c == ':' || c == ';' || c == '{' || c.is_whitespace())
                            .next()
                            .unwrap_or("");
                    }
                    if !first.is_empty() && first != "*" {
                        vec.push((first.to_string(), lineno));
                    }
                }
            }
        }
        "c_sharp" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if let Some(after) = t.strip_prefix("using ") {
                    let head = after.split('=').next().unwrap_or(after);
                    let first = head
                        .split(|c: char| c == '.' || c == ';' || c.is_whitespace())
                        .next()
                        .unwrap_or("");
                    if !first.is_empty() {
                        vec.push((first.to_string(), lineno));
                    }
                }
            }
        }
        "swift" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if let Some(rest) = t.strip_prefix("@testable import ") {
                    let tok = rest.split_whitespace().next().unwrap_or("");
                    if !tok.is_empty() {
                        vec.push((tok.to_string(), lineno));
                    }
                } else if let Some(rest) = t.strip_prefix("import ") {
                    let tok = rest.split_whitespace().next().unwrap_or("");
                    if !tok.is_empty() {
                        vec.push((tok.to_string(), lineno));
                    }
                }
            }
        }
        _ => {}
    }
    vec
}

/// Extract import tokens with optional alias information for languages that support aliases (Python, JS, etc).
/// Returns a Vec of (module_token, optional_alias, line_number). Line numbers are 0-based.
pub fn extract_import_aliases(lang: &str, source: &str) -> Vec<(String, Option<String>, usize)> {
    let mut out: Vec<(String, Option<String>, usize)> = Vec::new();
    match lang {
        "python" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if let Some(rest) = t.strip_prefix("import ") {
                    for part in rest.split(',') {
                        let token = part.trim();
                        if token.is_empty() {
                            continue;
                        }
                        // support: import pkg.a as x
                        if let Some(idx) = token.find(" as ") {
                            let module = token[..idx].trim().to_string();
                            let alias = token[idx + 4..].trim().to_string();
                            out.push((module, Some(alias), lineno));
                        } else {
                            let module = token.split_whitespace().next().unwrap_or("").to_string();
                            if !module.is_empty() {
                                out.push((module, None, lineno));
                            }
                        }
                    }
                } else if t.starts_with("from ") {
                    if let Some(after_from) = t.strip_prefix("from ") {
                        if let Some((module, imports_part)) = after_from.split_once(" import ") {
                            let mut mod_token = module.trim();
                            // strip leading relative dots
                            mod_token = mod_token.trim_start_matches('.');
                            let mod_token = mod_token.split_whitespace().next().unwrap_or("");
                            if !mod_token.is_empty() {
                                // Parse individual imported symbols: could be 'a', 'a as b', '(a, b)'
                                let imports = imports_part.trim();
                                let imports = imports.trim_start_matches('(').trim_end_matches(')');
                                for part in imports.split(',') {
                                    let token = part.trim();
                                    if token.is_empty() || token == "*" {
                                        continue;
                                    }
                                    if let Some(idx) = token.find(" as ") {
                                        let name = token[..idx].trim();
                                        let alias = token[idx + 4..].trim();
                                        let full = format!("{}.{}", mod_token, name);
                                        out.push((full, Some(alias.to_string()), lineno));
                                    } else {
                                        let name = token.split_whitespace().next().unwrap_or("");
                                        if !name.is_empty() {
                                            let full = format!("{}.{}", mod_token, name);
                                            out.push((full, None, lineno));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "javascript" | "typescript" => {
            // simple line-based fallback remains available, but prefer a
            // tree-sitter based extractor when callers provide a parsed tree.
            // This branch is kept for the fallback extractor used when no parse
            // is available; more robust extraction for parsed files is handled
            // by `extract_import_aliases_from_tree` (invoked by the builder).
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if t.starts_with("import ") {
                    // import defaultExport from 'mod'
                    if let Some(idx) = t.find(" from ") {
                        let left = t[7..idx].trim();
                        if let Some(m) = extract_quoted(&t[idx + 6..]) {
                            let mod_basename = normalize_module_basename(&m).to_string();
                            if left.starts_with('{') {
                                // named imports: import { a as b, c } from 'mod'
                                let inner = left.trim_start_matches('{').trim_end_matches('}');
                                for part in inner.split(',') {
                                    let token = part.trim();
                                    if token.is_empty() {
                                        continue;
                                    }
                                    if let Some(pos) = token.find(":") {
                                        // destructured with colon e.g. {a: b}
                                        let name = token[..pos].trim();
                                        let alias = token[pos + 1..].trim();
                                        let full = format!("{}.{}", mod_basename, name);
                                        out.push((full, Some(alias.to_string()), lineno));
                                    } else if let Some(idx2) = token.find(" as ") {
                                        let name = token[..idx2].trim();
                                        let alias = token[idx2 + 4..].trim();
                                        let full = format!("{}.{}", mod_basename, name);
                                        out.push((full, Some(alias.to_string()), lineno));
                                    } else {
                                        let name = token.split_whitespace().next().unwrap_or("");
                                        if !name.is_empty() {
                                            let full = format!("{}.{}", mod_basename, name);
                                            out.push((full, None, lineno));
                                        }
                                    }
                                }
                            } else if left.starts_with("* as ") {
                                // import * as alias from 'mod'
                                if let Some(alias) = left.split_whitespace().last() {
                                    out.push((
                                        mod_basename.clone(),
                                        Some(alias.to_string()),
                                        lineno,
                                    ));
                                }
                            } else if !left.is_empty() {
                                // default import: import defaultExport from 'mod'
                                let alias = left.split_whitespace().next().unwrap_or("");
                                if !alias.is_empty() {
                                    out.push((
                                        mod_basename.clone(),
                                        Some(alias.to_string()),
                                        lineno,
                                    ));
                                }
                            }
                        }
                    } else {
                        // side-effect import: import 'mod'
                        if let Some(m) = extract_quoted(t) {
                            out.push((normalize_module_basename(&m), None, lineno));
                        }
                    }
                } else if t.contains("require(") {
                    // handle common patterns: const x = require('mod'); or const {a} = require('mod');
                    if let Some(eq) = t.find('=') {
                        let left = t[..eq].trim();
                        if let Some(start) = t.find("require(") {
                            if let Some(end_rel) = t[start + 8..].find(')') {
                                let m = &t[start + 8..start + 8 + end_rel];
                                let m2 = m.trim_matches(&['\'', '"'] as &[_]);
                                let mod_basename = normalize_module_basename(m2).to_string();
                                if left.starts_with("const ")
                                    || left.starts_with("let ")
                                    || left.starts_with("var ")
                                {
                                    let left = left.split_whitespace().nth(1).unwrap_or("");
                                    if left.starts_with('{') {
                                        // destructured require: const {a: b} = require('mod')
                                        let inner =
                                            left.trim_start_matches('{').trim_end_matches('}');
                                        for part in inner.split(',') {
                                            let token = part.trim();
                                            if token.is_empty() {
                                                continue;
                                            }
                                            if let Some(col) = token.find(':') {
                                                let name = token[..col].trim();
                                                let alias = token[col + 1..].trim();
                                                let full = format!("{}.{}", mod_basename, name);
                                                out.push((full, Some(alias.to_string()), lineno));
                                            } else {
                                                let name =
                                                    token.split_whitespace().next().unwrap_or("");
                                                let full = format!("{}.{}", mod_basename, name);
                                                out.push((full, None, lineno));
                                            }
                                        }
                                    } else if !left.is_empty() {
                                        out.push((
                                            mod_basename.clone(),
                                            Some(left.to_string()),
                                            lineno,
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        "rust" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim().trim_end_matches(';');
                if let Some(after) = t.strip_prefix("use ") {
                    let s = after.trim();
                    // handle grouped imports: foo::{a as b, c}
                    if let Some(idx) = s.find("::{") {
                        let base = &s[..idx];
                        if let Some(start) = s.find("{") {
                            if let Some(end) = s.find('}') {
                                let inner = &s[start + 1..end];
                                for part in inner.split(',') {
                                    let token = part.trim();
                                    if token.is_empty() {
                                        continue;
                                    }
                                    if let Some(pos) = token.find(" as ") {
                                        let name = token[..pos].trim();
                                        let alias = token[pos + 4..].trim();
                                        let full = format!("{}.{}", base.replace("::", "."), name);
                                        out.push((full, Some(alias.to_string()), lineno));
                                    } else {
                                        let name = token.split_whitespace().next().unwrap_or("");
                                        let full = format!("{}.{}", base.replace("::", "."), name);
                                        out.push((full, None, lineno));
                                    }
                                }
                            }
                        }
                    } else if s.contains(" as ") {
                        if let Some(pos) = s.find(" as ") {
                            let module = s[..pos].trim();
                            let alias = s[pos + 4..].trim();
                            let module_token = module.replace("::", ".");
                            out.push((module_token, Some(alias.to_string()), lineno));
                        }
                    } else {
                        // no alias; we can still emit module tokens for use statements
                        let module_token = s.replace("::", ".");
                        out.push((module_token, None, lineno));
                    }
                }
            }
        }
        "go" => {
            let mut in_block = false;
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if t.starts_with("import (") {
                    in_block = true;
                    continue;
                }
                if in_block {
                    if t.starts_with(')') {
                        in_block = false;
                        continue;
                    }
                    // line could be: alias "module/path" or "module/path"
                    if let Some(start) = t.find('"') {
                        if let Some(end_rel) = t[start + 1..].find('"') {
                            let module = &t[start + 1..start + 1 + end_rel];
                            let before = t[..start].trim();
                            if !before.is_empty() {
                                let alias = before.split_whitespace().next().unwrap_or("");
                                out.push((
                                    normalize_module_basename(module),
                                    Some(alias.to_string()),
                                    lineno,
                                ));
                            } else {
                                out.push((normalize_module_basename(module), None, lineno));
                            }
                        }
                    }
                } else if t.starts_with("import ") {
                    if let Some(start) = t.find('"') {
                        if let Some(end_rel) = t[start + 1..].find('"') {
                            let module = &t[start + 1..start + 1 + end_rel];
                            let before = t[..start].trim();
                            // before might be: import alias "module/path" or import "module/path"
                            let pre = before.strip_prefix("import ").unwrap_or(before).trim();
                            if !pre.is_empty() {
                                let alias = pre.split_whitespace().next().unwrap_or("");
                                out.push((
                                    normalize_module_basename(module),
                                    Some(alias.to_string()),
                                    lineno,
                                ));
                            } else {
                                out.push((normalize_module_basename(module), None, lineno));
                            }
                        }
                    }
                }
            }
        }
        _ => {
            // fallback: reuse extract_import_modules tokens without alias info
            for (m, ln) in extract_import_modules(lang, source) {
                out.push((m, None, ln));
            }
        }
    }
    out
}

/// Tree-sitter based alias extraction for JavaScript/TypeScript.
/// Returns the same Vec<(module_token, optional_alias, lineno)> shape.
pub fn extract_import_aliases_from_tree(
    lang: &str,
    tree: &tree_sitter::Tree,
    source: &str,
) -> Vec<(String, Option<String>, usize)> {
    // Delegate to zoekt-rs typesitter implementation which centralizes
    // AST-based extraction for multiple languages. Map `lang` to an
    // extension string expected by the zoekt-rs helper.
    let ext = match lang {
        "javascript" => "js",
        "typescript" => "ts",
        "tsx" => "tsx",
        _ => return Vec::new(),
    };
    // Use the zoekt-rs helper with the provided pre-parsed `tree` to avoid reparsing.
    zoekt_rs::typesitter::extract_import_aliases_typesitter_with_tree(source, ext, Some(tree))
}
