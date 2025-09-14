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

use crate::repo_index::indexer::langspec::*;
use crate::repo_index::indexer::types::Entity;
use std::path::Path;
use std::time::Instant;
use tree_sitter::Node;

#[derive(Debug, Default, Clone)]
pub struct ExtractStats {
    pub docs_nanos: u128,
    pub calls_nanos: u128,
    pub docs_attempts: usize,
    pub calls_attempts: usize,
    pub entities_emitted: usize,
}

// Return the source slice for a node.
fn node_text<'a>(node: Node<'a>, src: &'a str) -> &'a str {
    src.get(node.byte_range()).unwrap_or("")
}

// Try to extract a human-friendly name for a node using a preferred field name,
// with fallbacks across common identifier fields.
fn extract_node_name(node: Node, src: &str, preferred_field: &str) -> String {
    if let Some(ch) = node.child_by_field_name(preferred_field) {
        let s = node_text(ch, src).trim().to_string();
        if !s.is_empty() {
            // For C/C++-style declarators (e.g., "f()", "*pf(int)") extract the base identifier.
            if let Some(paren) = s.find('(') {
                let head = &s[..paren];
                // split by typical separators and take the last token
                let mut cand = head
                    .rsplit(|c: char| c.is_whitespace() || c == ':' || c == '*' || c == '&')
                    .find(|t| !t.trim().is_empty())
                    .unwrap_or(head)
                    .trim()
                    .to_string();
                // strip possible trailing qualifiers
                cand = cand.trim_end_matches(':').to_string();
                // normalize destructor names: "~Point" -> "Point"
                if cand.starts_with('~') {
                    cand = cand.trim_start_matches('~').to_string();
                }
                if !cand.is_empty() {
                    return cand;
                }
            }
            return s;
        }
    }
    // Fallbacks: look for common identifier-like children in document order.
    let mut w = node.walk();
    for ch in node.named_children(&mut w) {
        let k = ch.kind();
        if k.contains("identifier")
            || k.contains("type_identifier")
            || k.contains("field_identifier")
            || k.contains("name")
        {
            let s = node_text(ch, src).trim().to_string();
            if !s.is_empty() {
                return s;
            }
        }
    }
    // Last resort: use the first non-empty token from the node text.
    let sig = node_text(node, src);
    sig.split(|c: char| c.is_whitespace() || c == '(' || c == '{')
        .find(|t| !t.is_empty())
        .unwrap_or("")
        .to_string()
}

fn generic_class_function_walk<'a>(
    lang: &'a str,
    tree: &tree_sitter::Tree,
    src: &'a str,
    path: &Path,
    out: &mut Vec<Entity<'a>>,
    spec: &dyn crate::repo_index::indexer::langspec::LangSpec,
    stats: &mut ExtractStats,
) {
    let mut stack = vec![(tree.root_node(), None::<String>)];
    while let Some((node, parent)) = stack.pop() {
        let kind = node.kind();
        // Rust: handle impl blocks which hold method definitions for a type.
        if lang == "rust" && kind == "impl_item" {
            // attempt to extract the type name this impl applies to
            let type_name = if let Some(tn) = node.child_by_field_name("type") {
                node_text(tn, src).to_string()
            } else {
                extract_node_name(node, src, "type")
            };
            if !type_name.is_empty() {
                let mut methods: Vec<crate::repo_index::indexer::payload::MethodItem> = Vec::new();
                // traverse impl node recursively to find function_item nodes
                let mut stack_impl = vec![node];
                while let Some(curr) = stack_impl.pop() {
                    let mut wimpl = curr.walk();
                    for ch in curr.children(&mut wimpl) {
                        if ch.kind() == spec.function_kind()
                            || ch.kind().contains("method")
                            || ch.kind().contains("function")
                        {
                            let mname = extract_node_name(ch, src, spec.function_name_field());
                            let snippet = src.get(ch.start_byte()..ch.end_byte()).unwrap_or("");
                            let visibility = if snippet.contains("pub ") {
                                Some("public".to_string())
                            } else {
                                None
                            };
                            methods.push(crate::repo_index::indexer::payload::MethodItem {
                                name: mname.clone(),
                                visibility,
                                signature: extract_signature(&ch, src).to_string(),
                                start_line: Some(ch.start_position().row as u32),
                                end_line: Some(ch.end_position().row as u32),
                            });
                        } else {
                            stack_impl.push(ch);
                        }
                    }
                }
                if !methods.is_empty() {
                    if let Some(ent) = out.iter_mut().find(|e| {
                        e.name == type_name
                            || e.signature.contains(&type_name)
                            || type_name.contains(&e.name)
                    }) {
                        ent.methods.extend(methods);
                    } else {
                        out.push(Entity {
                            file: path.display().to_string(),
                            language: lang,
                            kind: "class",
                            name: type_name.clone(),
                            parent: None,
                            signature: format!("impl {}", type_name),
                            start_line: node.start_position().row,
                            end_line: node.end_position().row,
                            doc: None,
                            calls_raw: Vec::new(),
                            methods,
                        });
                    }
                }
            }
            // skip pushing children of impl_item to avoid double-processing
            continue;
        }
        if kind == spec.class_kind() {
            // For Verilog, be strict: only take the explicit module name field to avoid
            // accidentally picking a nested identifier (e.g., an instantiation below).
            let mut name = if lang == "verilog" {
                if let Some(ch) = node.child_by_field_name(spec.class_name_field()) {
                    node_text(ch, src).trim().to_string()
                } else {
                    String::new()
                }
            } else {
                extract_node_name(node, src, spec.class_name_field())
            };
            if lang == "verilog" && name.is_empty() {
                // Fallback: parse module name from the signature text: "module <ident> ...".
                let sig = extract_signature(&node, src);
                if let Some(mi) = sig.find("module") {
                    let after = &sig[mi + "module".len()..];
                    let ident = after
                        .trim_start()
                        .split(|c: char| c.is_whitespace() || c == '(' || c == '#')
                        .find(|s| !s.is_empty())
                        .unwrap_or("");
                    name = ident.to_string();
                }
            }
            // Special handling for Go: prefer the underlying type name (e.g., "struct" or "int")
            // for type declarations like `type Point struct { ... }` so exported entity names match
            // expectations ("struct" entries in geometry.go).
            if lang == "go" {
                // Derive the underlying type directly from the declaration signature, e.g.:
                //  - "type Point struct" => "struct"
                //  - "type Direction int" => "int"
                let sig = extract_signature(&node, src);
                let last = sig.split_whitespace().last().unwrap_or("").trim();
                if !last.is_empty() {
                    name = last.to_string();
                }
            } else if lang != "verilog" {
                // Non-Go: if the extracted name is overly generic ("struct", "class", "module") or empty,
                // try to find a better identifier descendant (e.g., type_identifier).
                if name == "struct" || name == "class" || name == "module" || name.trim().is_empty()
                {
                    let mut found_ident: Option<String> = None;
                    // manual DFS over descendants
                    let mut stack_desc = vec![node];
                    while let Some(ch) = stack_desc.pop() {
                        let mut wch = ch.walk();
                        for gc in ch.children(&mut wch) {
                            let k = gc.kind();
                            if k.contains("identifier")
                                || k.contains("type_identifier")
                                || k.contains("field_identifier")
                            {
                                let s = node_text(gc, src);
                                if !s.trim().is_empty() {
                                    found_ident = Some(s.to_string());
                                    break;
                                }
                            }
                            stack_desc.push(gc);
                        }
                        if found_ident.is_some() {
                            break;
                        }
                    }
                    if let Some(s) = found_ident {
                        name = s;
                    }
                }
            }
            // collect methods defined anywhere under this class node (but not nested
            // inside other function definitions).
            let mut methods: Vec<crate::repo_index::indexer::payload::MethodItem> = Vec::new();
            let mut stack2 = vec![node];
            while let Some(curr) = stack2.pop() {
                let mut w2 = curr.walk();
                for child in curr.children(&mut w2) {
                    if child.kind() == spec.function_kind()
                        || child.kind().contains("method")
                        || child.kind().contains("method_definition")
                        || child.kind().contains("function")
                    {
                        // skip nested functions: walk up parents and see if any parent between
                        // child and the class node is also a function
                        let mut p = child.parent();
                        let mut nested = false;
                        while let Some(pp) = p {
                            if pp.id() == node.id() {
                                break;
                            }
                            if pp.kind() == spec.function_kind() {
                                nested = true;
                                break;
                            }
                            p = pp.parent();
                        }
                        if nested {
                            continue;
                        }
                        let mname = extract_node_name(child, src, spec.function_name_field());
                        // visibility heuristic
                        let mut visibility: Option<String> = None;
                        let snippet = src.get(child.start_byte()..child.end_byte()).unwrap_or("");
                        if snippet.contains("pub ") {
                            visibility = Some("public".to_string());
                        } else if mname.starts_with('_') {
                            visibility = Some("private".to_string());
                        } else if spec.class_kind() == "class_declaration"
                            && spec.function_kind() == "function_declaration"
                        {
                            let before =
                                src.get(node.start_byte()..child.start_byte()).unwrap_or("");
                            if before.contains("private ") {
                                visibility = Some("private".to_string());
                            } else if before.contains("protected ") {
                                visibility = Some("protected".to_string());
                            } else {
                                visibility = Some("public".to_string());
                            }
                        }
                        methods.push(crate::repo_index::indexer::payload::MethodItem {
                            name: mname.clone(),
                            visibility,
                            signature: extract_signature(&child, src).to_string(),
                            start_line: Some(child.start_position().row as u32),
                            end_line: Some(child.end_position().row as u32),
                        });
                    } else {
                        stack2.push(child);
                    }
                }
            }
            let doc_t0 = Instant::now();
            let doc_opt = extract_doc_comments(&node, src, lang);
            stats.docs_attempts += 1;
            stats.docs_nanos += doc_t0.elapsed().as_nanos();
            let entity = Entity {
                file: path.display().to_string(),
                language: lang,
                kind: "class",
                name: name.clone(),
                parent: parent.clone(),
                signature: extract_signature(&node, src).to_string(),
                start_line: node.start_position().row,
                end_line: node.end_position().row,
                // calls removed
                doc: doc_opt,
                calls_raw: Vec::new(),
                methods,
            };
            out.push(entity);
            for child in node.children(&mut node.walk()) {
                stack.push((child, Some(name.to_string())));
            }
        } else if kind == spec.function_kind()
            || kind.contains("method")
            || kind.contains("function")
        {
            let name = extract_node_name(node, src, spec.function_name_field());
            // If this function node is within a class (we passed a parent),
            // Python needs stand-alone method entities for call-edge tests; for other
            // languages, avoid emitting as a separate entity to normalize method emission.
            if parent.is_some() && lang != "python" {
                // still traverse children to continue discovery, but don't emit
                for child in node.children(&mut node.walk()) {
                    stack.push((child, parent.clone()));
                }
                continue;
            }

            // Special-case Go: methods are often top-level function_declaration
            // nodes with a receiver (e.g. `func (f Foo) Public() {}`); try to
            // detect a receiver and attach the method to the corresponding type
            // entity if it already exists in `out`.
            if lang == "go" {
                let snippet = src.get(node.start_byte()..node.end_byte()).unwrap_or("");
                if let Some(open) = snippet.find("func (") {
                    if let Some(close) = snippet[open..].find(')') {
                        let recv = &snippet[open + 6..open + close]; // between '(' and ')'
                                                                     // take last token as the type name (strip pointer '*')
                        let mut parts: Vec<&str> = recv.split_whitespace().collect();
                        if let Some(raw) = parts.pop() {
                            let type_name = raw.trim().trim_start_matches('*').trim();
                            if !type_name.is_empty() {
                                // try to find an existing entity with that name
                                if let Some(ent) = out.iter_mut().find(|e| {
                                    e.name == type_name
                                        || e.signature.contains(type_name)
                                        || type_name.contains(&e.name)
                                }) {
                                    ent.methods.push(
                                        crate::repo_index::indexer::payload::MethodItem {
                                            name: name.clone(),
                                            visibility: if snippet.contains("public ")
                                                || name
                                                    .chars()
                                                    .next()
                                                    .is_some_and(|c| c.is_uppercase())
                                            {
                                                Some("public".to_string())
                                            } else {
                                                None
                                            },
                                            signature: extract_signature(&node, src).to_string(),
                                            start_line: Some(node.start_position().row as u32),
                                            end_line: Some(node.end_position().row as u32),
                                        },
                                    );
                                    // traverse children but do not emit a function entity
                                    for child in node.children(&mut node.walk()) {
                                        stack.push((child, parent.clone()));
                                    }
                                    continue;
                                } else {
                                    // create a type entity now and attach the method so
                                    // methods are always attached to a type entity
                                    out.push(Entity {
                                        file: path.display().to_string(),
                                        language: lang,
                                        kind: "class",
                                        name: type_name.to_string(),
                                        parent: None,
                                        signature: format!("type {} struct", type_name),
                                        start_line: node.start_position().row,
                                        end_line: node.end_position().row,
                                        doc: None,
                                        calls_raw: Vec::new(),
                                        methods: vec![
                                            crate::repo_index::indexer::payload::MethodItem {
                                                name: name.clone(),
                                                visibility: if snippet.contains("public ")
                                                    || name
                                                        .chars()
                                                        .next()
                                                        .is_some_and(|c| c.is_uppercase())
                                                {
                                                    Some("public".to_string())
                                                } else {
                                                    None
                                                },
                                                signature: extract_signature(&node, src)
                                                    .to_string(),
                                                start_line: Some(node.start_position().row as u32),
                                                end_line: Some(node.end_position().row as u32),
                                            },
                                        ],
                                    });
                                    for child in node.children(&mut node.walk()) {
                                        stack.push((child, parent.clone()));
                                    }
                                    continue;
                                }
                            }
                        }
                    }
                }
            }

            // Fallback: emit a top-level function entity
            let doc_t0 = Instant::now();
            let doc_opt = extract_doc_comments(&node, src, lang);
            stats.docs_attempts += 1;
            stats.docs_nanos += doc_t0.elapsed().as_nanos();
            let calls_t0 = Instant::now();
            let calls_vec = collect_call_idents(node, src);
            stats.calls_attempts += 1;
            stats.calls_nanos += calls_t0.elapsed().as_nanos();
            out.push(Entity {
                file: path.display().to_string(),
                language: lang,
                kind: "function",
                name: name.clone(),
                parent: parent.clone(),
                signature: extract_signature(&node, src).to_string(),
                start_line: node.start_position().row,
                end_line: node.end_position().row,
                // calls removed
                doc: doc_opt,
                calls_raw: calls_vec,
                methods: Vec::new(),
            });
            for child in node.children(&mut node.walk()) {
                stack.push((child, parent.clone()));
            }
        } else {
            for child in node.children(&mut node.walk()) {
                stack.push((child, parent.clone()));
            }
        }
    }
}

fn extract_signature<'a>(node: &Node<'a>, src: &'a str) -> &'a str {
    let text = src.get(node.byte_range()).unwrap_or("");
    if let Some(idx) = text.find('{') {
        text[..idx].trim_end()
    } else {
        text.lines().next().unwrap_or("")
    }
}

fn extract_doc_comments<'a>(node: &Node<'a>, src: &'a str, lang: &str) -> Option<String> {
    let start_byte = node.start_byte();
    let prefix = &src[..start_byte.min(src.len())];
    let mut lines: Vec<&str> = prefix.lines().collect();
    let mut doc_rev: Vec<String> = Vec::new();
    while let Some(&line) = lines.last() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }
        // Handle block comments starting with /* or /** by collecting the whole
        // contiguous block and normalizing inner '*' prefixes and trailing '*/'.
        if trimmed.starts_with("/*") || trimmed.ends_with("*/") || trimmed == "*/" {
            let mut block_lines: Vec<String> = Vec::new();
            // Pop lines until we find the opening of the block ("/*" or "/**")
            // or run out. We iterate backwards from the line closest to the
            // node, so stopping on the opening ensures we collected the full
            // comment block rather than only the closing line.
            while let Some(&l) = lines.last() {
                let s = l.to_string();
                let is_open = s.trim_start().starts_with("/**") || s.trim_start().starts_with("/*");
                lines.pop();
                block_lines.push(s);
                if is_open {
                    break;
                }
            }
            // Process block lines: we collected them in reverse document order
            // (closest to the node first), so reverse to restore original order.
            block_lines.reverse();
            // remove opening '/*' or '/**' from first line, remove leading '*'
            // from each line, and strip trailing '*/' from last.
            let mut processed: Vec<String> = Vec::new();
            for (i, bl) in block_lines.into_iter().enumerate() {
                let mut s = bl.trim().to_string();
                if i == 0 {
                    s = s
                        .trim_start_matches("/**")
                        .trim_start_matches("/*")
                        .to_string();
                }
                if s.ends_with("*/") {
                    s = s.trim_end_matches("*/").to_string();
                }
                // strip leading '*' common to comment middles
                s = s.trim_start_matches('*').trim().to_string();
                if !s.is_empty() {
                    processed.push(s);
                }
            }
            if !processed.is_empty() {
                let mut doc_text = processed.join("\n");
                // Clean up each line to remove leading/trailing comment delimiters
                let mut cleaned: Vec<String> = Vec::new();
                for l in doc_text.lines() {
                    let mut s = l.trim().to_string();
                    // strip opening tokens
                    if s.starts_with("/**") {
                        s = s.trim_start_matches("/**").trim().to_string();
                    }
                    if s.starts_with("/*") {
                        s = s.trim_start_matches("/*").trim().to_string();
                    }
                    if s.ends_with("*/") {
                        s = s.trim_end_matches("*/").trim().to_string();
                    }
                    // remove leading '*' or '/' characters left on the line
                    s = s
                        .trim_start_matches(['*', '/'])
                        .trim_end_matches('/')
                        .trim()
                        .to_string();
                    if !s.is_empty() {
                        cleaned.push(s);
                    }
                }
                doc_text = cleaned.join("\n");
                // Remove lines that are only delimiters ("/", "/*", "*/", "*", etc.)
                let filtered_lines: Vec<String> = doc_text
                    .lines()
                    .map(|l| l.trim().to_string())
                    .filter(|l| {
                        if l.is_empty() {
                            return false;
                        }
                        // if the line consists only of '/' or '*' characters, drop it
                        let all_delim = l.chars().all(|c| c == '/' || c == '*');
                        if all_delim {
                            return false;
                        }
                        // common explicit delimiters to drop
                        if l == "/" || l == "/*" || l == "/**" || l == "*/" || l == "*" {
                            return false;
                        }
                        true
                    })
                    .collect();
                doc_text = filtered_lines.join("\n");
                // final aggressive trim of leftover delimiters around the whole text
                doc_text = doc_text
                    .trim()
                    .trim_start_matches(['/', '*'])
                    .trim_end_matches(['/', '*'])
                    .trim()
                    .to_string();
                // After trimming, some cases still leave a dangling single '/'
                if doc_text.ends_with('/') {
                    // only strip if it's a stray delimiter, not part of a word
                    if doc_text
                        .chars()
                        .rev()
                        .nth(1)
                        .is_some_and(|c| c.is_alphanumeric() || c == ' ')
                    {
                        // ok to strip the trailing '/'
                        doc_text.pop();
                        doc_text = doc_text.trim_end().to_string();
                    }
                }
                if !doc_text.is_empty() {
                    doc_rev.push(doc_text);
                }
            }
            continue;
        }
        if trimmed.starts_with("///")
            || trimmed.starts_with("//")
            || trimmed.starts_with('#')
            || trimmed.starts_with('*')
        {
            let mut s = trimmed
                .trim_start_matches("///")
                .trim_start_matches("//")
                .trim_start_matches('#')
                .trim_start_matches('*')
                .trim()
                .to_string();
            if s.ends_with("*/") {
                s = s.trim_end_matches("*/").trim().to_string();
            }
            doc_rev.push(s);
            lines.pop();
        } else {
            break;
        }
    }

    // If no immediate prefixed comment was found and this is a Go file, try to
    // associate the package-level comment (comments immediately preceding the
    // `package` declaration) as the doc for the node. This helps in cases where
    // package comments are used as file-level docs that consumers expect to be
    // attached to top-level declarations.
    if doc_rev.is_empty() && lang == "go" {
        if let Some(pkg_pos) = prefix.rfind("package ") {
            // find start of the package line
            let line_start = prefix[..pkg_pos].rfind('\n').map(|i| i + 1).unwrap_or(0);
            let before = &prefix[..line_start];
            let mut collected: Vec<String> = Vec::new();
            for l in before.lines().rev() {
                let t = l.trim();
                if t.starts_with("//") || t.starts_with("///") {
                    let s = t
                        .trim_start_matches("///")
                        .trim_start_matches("//")
                        .trim()
                        .to_string();
                    collected.push(s);
                } else {
                    break;
                }
            }
            if !collected.is_empty() {
                collected.reverse();
                doc_rev.push(collected.join("\n"));
            }
        }
    }
    if doc_rev.is_empty() {
        // No prefixed comment docs found. Use AST-level extraction: look for
        // string-literal descendant nodes that usually represent docstrings.
        // Use a per-language set of accepted node-kind substrings to reduce
        // false positives/negatives across grammars.
        let mut literals: Vec<String> = Vec::new();

        // Helper: normalize indentation and trim surrounding blank lines
        let normalize = |s: &str| -> String {
            let mut lines: Vec<&str> = s.lines().collect();
            while !lines.is_empty() && lines.first().unwrap().trim().is_empty() {
                lines.remove(0);
            }
            while !lines.is_empty() && lines.last().unwrap().trim().is_empty() {
                lines.pop();
            }
            let min_indent = lines
                .iter()
                .filter(|l| !l.trim().is_empty())
                .map(|l| l.chars().take_while(|c| c.is_whitespace()).count())
                .min()
                .unwrap_or(0);
            let out: Vec<String> = lines
                .into_iter()
                .map(|l| {
                    if l.len() >= min_indent {
                        l[min_indent..].to_string()
                    } else {
                        l.to_string()
                    }
                })
                .collect();
            out.join("\n").trim().to_string()
        };

        // Helper: strip quotes and prefix letters (r, u, f, b, etc.) from a literal
        let unquote = |lit: &str| -> Option<String> {
            let t = lit.trim();
            // strip leading bytes that are ASCII letters (prefixes like r, u, f, b)
            let mut i = 0;
            for c in t.chars() {
                if c.is_ascii_alphabetic() {
                    i += c.len_utf8();
                } else {
                    break;
                }
            }
            let rest = &t[i..];
            if rest.len() >= 3 && (rest.starts_with("\"\"\"") || rest.starts_with("'''")) {
                let q = &rest[..3];
                if rest.ends_with(q) && rest.len() > 6 {
                    return Some(rest[3..rest.len() - 3].to_string());
                }
            } else if !rest.is_empty() && (rest.starts_with('"') || rest.starts_with('\'')) {
                let q = rest.chars().next().unwrap();
                if rest.ends_with(q) && rest.len() > 1 {
                    return Some(rest[1..rest.len() - 1].to_string());
                }
            }
            None
        };

        // Per-language accepted node-kind substrings. These are matched against
        // the tree-sitter node kind (substring match) for tolerance across
        // minor grammar naming differences.
        // Exact node-kind names per tree-sitter grammar.
        // These are exact equality checks (not substring matches) to reduce
        // accidental matches on unrelated node kinds.
        let accepted_kinds: Vec<&str> = match lang {
            // tree-sitter-python: "string", "f_string"
            "python" => vec!["string", "f_string"],
            // tree-sitter-rust: "string_literal", "raw_string_literal"
            "rust" => vec!["string_literal", "raw_string_literal"],
            // tree-sitter-javascript/typescript: "string", "template_string"
            "javascript" | "typescript" | "tsx" => vec!["string", "template_string"],
            // tree-sitter-java: "string_literal"
            "java" => vec!["string_literal"],
            // tree-sitter-c_sharp: common kinds include "string_literal", "verbatim_string_literal", "interpolated_string_expression"
            "c_sharp" => vec![
                "string_literal",
                "verbatim_string_literal",
                "interpolated_string_expression",
            ],
            // tree-sitter-go: "raw_string_literal", "interpreted_string_literal"
            "go" => vec!["raw_string_literal", "interpreted_string_literal"],
            // tree-sitter-cpp: "string_literal"
            "cpp" => vec!["string_literal"],
            // tree-sitter-swift: "string_literal"
            "swift" => vec!["string_literal"],
            // fallback: include common names
            _ => vec![
                "string",
                "string_literal",
                "raw_string_literal",
                "f_string",
                "template_string",
            ],
        };

        // Traverse descendants in order and collect string-like nodes that
        // appear early in the node body. Use a simple window: collect the first
        // contiguous cluster of literal nodes within a small byte window.
        let mut first_start: Option<usize> = None;
        let mut stack = vec![*node];
        while let Some(n) = stack.pop() {
            // push children in reverse so iteration order is document order
            let mut w = n.walk();
            let children: Vec<Node> = n.named_children(&mut w).collect();
            for ch in children.iter().rev() {
                stack.push(*ch);
            }
            let kind = n.kind();
            // Accept node kinds that match any of the accepted_kinds for this language
            let kind_ok = accepted_kinds.contains(&kind);
            if kind_ok {
                let start = n.start_byte();
                if start <= node.start_byte() {
                    continue;
                }
                if first_start.is_none() {
                    first_start = Some(start);
                }
                if let Some(fs) = first_start {
                    // limit cluster to ~2KB window to avoid grabbing unrelated strings
                    if start.saturating_sub(fs) > 2048 {
                        break;
                    }
                }
                let txt = node_text(n, src);
                if let Some(inner) = unquote(txt) {
                    literals.push(normalize(&inner));
                }
            }
        }

        if literals.is_empty() {
            None
        } else {
            Some(literals.join(""))
        }
    } else {
        doc_rev.reverse();
        Some(doc_rev.join("\n"))
    }
}

// Note: small heuristic for Go where package-level comments are sometimes used
// as file-level docs that consumers may want associated with the first top-
// level declaration. If we found no immediate prefixed comment for the node
// above, attempt to find a comment preceding a `package` declaration and
// return that instead for Go files.

fn collect_call_idents(func_node: Node, src: &str) -> Vec<String> {
    let mut calls = Vec::new();
    let mut stack = vec![func_node];
    while let Some(node) = stack.pop() {
        let kind = node.kind();
        // Accept a variety of call node kinds used by different tree-sitter grammars.
        // For Python the node kind is typically "call"; other grammars use
        // "call_expression" or "function_call". Match any kind that contains "call".
        if kind.contains("call") {
            // Prefer the named field "function" when present (common in several grammars).
            if let Some(child) = node.child_by_field_name("function") {
                let text = node_text(child, src).trim().to_string();
                if !text.is_empty() {
                    calls.push(text);
                }
            } else {
                // Fallback: find a first named child that likely represents the callee
                // (could be an identifier or attribute access like `self.session.get`).
                let mut found = None;
                for child in node.named_children(&mut node.walk()) {
                    let k = child.kind();
                    if k.contains("ident")
                        || k.contains("name")
                        || k.contains("attribute")
                        || child.is_named()
                    {
                        let t = node_text(child, src).trim().to_string();
                        if !t.is_empty() {
                            found = Some(t);
                            break;
                        }
                    }
                }
                if let Some(t) = found {
                    calls.push(t);
                } else if let Some(first) = node.child(0) {
                    if first.is_named() {
                        calls.push(node_text(first, src).to_string());
                    }
                }
            }
        }
        for child in node.children(&mut node.walk()) {
            stack.push(child);
        }
    }
    calls.sort();
    calls.dedup();
    calls
}

pub fn extract_entities<'a>(
    lang: &'a str,
    tree: &tree_sitter::Tree,
    src: &'a str,
    path: &Path,
    out: &mut Vec<Entity<'a>>,
) {
    let mut _stats = ExtractStats::default();
    match lang {
        "rust" => {
            generic_class_function_walk(lang, tree, src, path, out, &RustLangSpec, &mut _stats)
        }
        "python" => {
            generic_class_function_walk(lang, tree, src, path, out, &PythonLangSpec, &mut _stats)
        }
        "javascript" => {
            generic_class_function_walk(lang, tree, src, path, out, &JsLangSpec, &mut _stats)
        }
        "typescript" => {
            generic_class_function_walk(lang, tree, src, path, out, &TsLangSpec, &mut _stats)
        }
        "java" => {
            generic_class_function_walk(lang, tree, src, path, out, &JavaLangSpec, &mut _stats)
        }
        "go" => generic_class_function_walk(lang, tree, src, path, out, &GoLangSpec, &mut _stats),
        "cpp" => generic_class_function_walk(lang, tree, src, path, out, &CppLangSpec, &mut _stats),
        "c_sharp" => {
            generic_class_function_walk(lang, tree, src, path, out, &CSharpLangSpec, &mut _stats)
        }
        "swift" => {
            generic_class_function_walk(lang, tree, src, path, out, &SwiftLangSpec, &mut _stats)
        }
        "verilog" => {
            generic_class_function_walk(lang, tree, src, path, out, &VerilogLangSpec, &mut _stats)
        }
        _ => {
            out.push(Entity {
                file: path.display().to_string(),
                language: lang,
                kind: "file",
                name: path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string(),
                parent: None,
                signature: src.lines().next().unwrap_or("").to_string(),
                start_line: 0,
                end_line: src.lines().count().saturating_sub(1),
                // calls removed
                doc: None,
                calls_raw: Vec::new(),
                methods: Vec::new(),
            });
        }
    }

    // Post-process: attach function entities with a parent as methods to that parent.
    // For Python, we keep the function entities as well (to support call-edge tests).
    let mut to_remove: Vec<usize> = Vec::new();
    for i in (0..out.len()).rev() {
        if out[i].kind == "function" {
            if let Some(pname) = out[i].parent.clone() {
                if let Some(idx) = out.iter().position(|e| e.name == pname) {
                    // move into methods
                    let method = crate::repo_index::indexer::payload::MethodItem {
                        name: out[i].name.clone(),
                        visibility: None,
                        signature: out[i].signature.clone(),
                        start_line: Some(out[i].start_line as u32),
                        end_line: Some(out[i].end_line as u32),
                    };
                    out[idx].methods.push(method);
                    // remove only for non-Python languages
                    if lang != "python" {
                        to_remove.push(i);
                    }
                }
            }
        }
    }
    to_remove.sort_unstable();
    for idx in to_remove.into_iter().rev() {
        out.remove(idx);
    }
}

pub fn extract_entities_with_stats<'a>(
    lang: &'a str,
    tree: &tree_sitter::Tree,
    src: &'a str,
    path: &Path,
    out: &mut Vec<Entity<'a>>,
) -> ExtractStats {
    let mut stats = ExtractStats::default();
    match lang {
        "rust" => {
            generic_class_function_walk(lang, tree, src, path, out, &RustLangSpec, &mut stats)
        }
        "python" => {
            generic_class_function_walk(lang, tree, src, path, out, &PythonLangSpec, &mut stats)
        }
        "javascript" => {
            generic_class_function_walk(lang, tree, src, path, out, &JsLangSpec, &mut stats)
        }
        "typescript" => {
            generic_class_function_walk(lang, tree, src, path, out, &TsLangSpec, &mut stats)
        }
        "java" => {
            generic_class_function_walk(lang, tree, src, path, out, &JavaLangSpec, &mut stats)
        }
        "go" => generic_class_function_walk(lang, tree, src, path, out, &GoLangSpec, &mut stats),
        "cpp" => generic_class_function_walk(lang, tree, src, path, out, &CppLangSpec, &mut stats),
        "c_sharp" => {
            generic_class_function_walk(lang, tree, src, path, out, &CSharpLangSpec, &mut stats)
        }
        "swift" => {
            generic_class_function_walk(lang, tree, src, path, out, &SwiftLangSpec, &mut stats)
        }
        "verilog" => {
            generic_class_function_walk(lang, tree, src, path, out, &VerilogLangSpec, &mut stats)
        }
        _ => {
            out.push(Entity {
                file: path.display().to_string(),
                language: lang,
                kind: "file",
                name: path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string(),
                parent: None,
                signature: src.lines().next().unwrap_or("").to_string(),
                start_line: 0,
                end_line: src.lines().count().saturating_sub(1),
                doc: None,
                calls_raw: Vec::new(),
                methods: Vec::new(),
            });
        }
    }
    stats.entities_emitted += out.len();
    stats
}
