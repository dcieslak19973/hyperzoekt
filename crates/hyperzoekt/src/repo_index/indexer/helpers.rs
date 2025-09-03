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
                            mod_token = mod_token.trim_start_matches('.');
                            mod_token = mod_token.split('.').next().unwrap_or("");
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
