use crate::repo_index::indexer::langspec::*;
use crate::repo_index::indexer::types::Entity;
use std::path::Path;
use tree_sitter::Node;

fn node_text<'a>(node: Node<'a>, src: &'a str) -> &'a str {
    node.utf8_text(src.as_bytes()).unwrap_or("")
}

fn extract_node_name<'a>(node: Node<'a>, src: &'a str, preferred_field: &str) -> String {
    let mut found: Option<&str> = None;
    if let Some(n) = node.child_by_field_name(preferred_field) {
        let s = node_text(n, src);
        if !s.is_empty() {
            found = Some(s);
        }
    }
    if found.is_none() {
        for alt in ["name", "identifier", "module_identifier", "declarator"].iter() {
            if let Some(n) = node.child_by_field_name(alt) {
                let s = node_text(n, src);
                if !s.is_empty() {
                    found = Some(s);
                    break;
                }
            }
        }
    }
    if found.is_none() {
        let mut w = node.walk();
        for child in node.children(&mut w) {
            let k = child.kind();
            if k.contains("ident") || k.contains("name") || k.contains("module") {
                let s = node_text(child, src);
                if !s.is_empty() {
                    found = Some(s);
                    break;
                }
            }
        }
    }
    let mut candidate = if let Some(s) = found {
        s.to_string()
    } else {
        extract_signature(&node, src).to_string()
    };
    if candidate.trim().len() <= 2 {
        let sig = extract_signature(&node, src).to_string();
        if !sig.is_empty() {
            candidate = sig;
        }
    }
    if let Some(pidx) = candidate.find('(') {
        let before = &candidate[..pidx];
        let mut tokens: Vec<String> = Vec::new();
        let mut cur = String::new();
        for ch in before.chars() {
            if ch == '_' || ch.is_ascii_alphanumeric() {
                cur.push(ch);
            } else if !cur.is_empty() {
                tokens.push(cur.clone());
                cur.clear();
            }
        }
        if !cur.is_empty() {
            tokens.push(cur);
        }
        if let Some(last) = tokens.last() {
            return last.clone();
        }
    }
    let mut tokens: Vec<String> = Vec::new();
    let mut cur = String::new();
    for ch in candidate.chars() {
        if ch == '_' || ch.is_ascii_alphanumeric() {
            cur.push(ch);
        } else if !cur.is_empty() {
            tokens.push(cur.clone());
            cur.clear();
        }
    }
    if !cur.is_empty() {
        tokens.push(cur);
    }
    if let Some(last) = tokens.last() {
        return last.clone();
    }
    candidate.trim().to_string()
}

fn generic_class_function_walk<'a>(
    lang: &'a str,
    tree: &tree_sitter::Tree,
    src: &'a str,
    path: &Path,
    out: &mut Vec<Entity<'a>>,
    spec: &dyn crate::repo_index::indexer::langspec::LangSpec,
) {
    let mut stack = vec![(tree.root_node(), None::<String>)];
    while let Some((node, parent)) = stack.pop() {
        let kind = node.kind();
        if kind == spec.class_kind() {
            let name = extract_node_name(node, src, spec.class_name_field());
            let entity = Entity {
                file: path.display().to_string(),
                language: lang,
                kind: "class",
                name: name.clone(),
                parent: parent.clone(),
                signature: extract_signature(&node, src).to_string(),
                start_line: node.start_position().row,
                end_line: node.end_position().row,
                calls: None,
                doc: extract_doc_comments(&node, src),
            };
            out.push(entity);
            for child in node.children(&mut node.walk()) {
                stack.push((child, Some(name.to_string())));
            }
        } else if kind == spec.function_kind() {
            let name = extract_node_name(node, src, spec.function_name_field());
            let calls = collect_call_idents(node, src);
            out.push(Entity {
                file: path.display().to_string(),
                language: lang,
                kind: "function",
                name: name.clone(),
                parent: parent.clone(),
                signature: extract_signature(&node, src).to_string(),
                start_line: node.start_position().row,
                end_line: node.end_position().row,
                calls: if calls.is_empty() { None } else { Some(calls) },
                doc: extract_doc_comments(&node, src),
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

fn extract_doc_comments<'a>(node: &Node<'a>, src: &'a str) -> Option<String> {
    let start_byte = node.start_byte();
    let prefix = &src[..start_byte.min(src.len())];
    let mut lines: Vec<&str> = prefix.lines().collect();
    let mut doc_rev: Vec<String> = Vec::new();
    while let Some(&line) = lines.last() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }
        if trimmed.starts_with("///")
            || trimmed.starts_with("//")
            || trimmed.starts_with('#')
            || trimmed.starts_with("/*")
            || trimmed.starts_with('*')
        {
            doc_rev.push(
                trimmed
                    .trim_start_matches("///")
                    .trim_start_matches("//")
                    .trim_start_matches('#')
                    .trim_start_matches('*')
                    .trim()
                    .to_string(),
            );
            lines.pop();
        } else {
            break;
        }
    }
    if doc_rev.is_empty() {
        None
    } else {
        doc_rev.reverse();
        Some(doc_rev.join("\n"))
    }
}

fn collect_call_idents(func_node: Node, src: &str) -> Vec<String> {
    let mut calls = Vec::new();
    let mut stack = vec![func_node];
    while let Some(node) = stack.pop() {
        let kind = node.kind();
        if kind == "call_expression" || kind == "function_call" {
            if let Some(child) = node.child_by_field_name("function") {
                calls.push(node_text(child, src).to_string());
            } else if let Some(first) = node.child(0) {
                if first.is_named() {
                    calls.push(node_text(first, src).to_string());
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
    match lang {
        "rust" => generic_class_function_walk(lang, tree, src, path, out, &RustLangSpec),
        "python" => generic_class_function_walk(lang, tree, src, path, out, &PythonLangSpec),
        "javascript" => generic_class_function_walk(lang, tree, src, path, out, &JsLangSpec),
        "typescript" => generic_class_function_walk(lang, tree, src, path, out, &TsLangSpec),
        "java" => generic_class_function_walk(lang, tree, src, path, out, &JavaLangSpec),
        "go" => generic_class_function_walk(lang, tree, src, path, out, &GoLangSpec),
        "cpp" => generic_class_function_walk(lang, tree, src, path, out, &CppLangSpec),
        "c_sharp" => generic_class_function_walk(lang, tree, src, path, out, &CSharpLangSpec),
        "swift" => generic_class_function_walk(lang, tree, src, path, out, &SwiftLangSpec),
        "verilog" => generic_class_function_walk(lang, tree, src, path, out, &VerilogLangSpec),
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
                calls: None,
                doc: None,
            });
        }
    }
}
