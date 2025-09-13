// Copyright 2025 HyperZoekt Project
// Derived from sourcegraph/zoekt (https://github.com/sourcegraph/zoekt)
// Copyright 2016 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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

//! A small Tree-sitter based symbol extractor used by the indexer as an
//! opt-in higher-fidelity extractor. It attempts to parse the file and
//! extract common top-level symbols (functions, methods, classes, structs)
//! for a few languages (rust, go, python). If the language isn't supported
//! or parsing fails, it returns an empty Vec so callers can fall back to the
//! regex-based extractor.

// std helpers kept intentionally minimal for this module
use tree_sitter::{Language, Node, Parser};
// use the language bindings with short aliases to match other code in workspace
use tree_sitter_c_sharp as ts_c_sharp;
use tree_sitter_cpp as ts_cpp;
use tree_sitter_go as ts_go;
use tree_sitter_java as ts_java;
use tree_sitter_javascript as ts_js;
use tree_sitter_ocaml as ts_ocaml;
use tree_sitter_python as ts_python;
use tree_sitter_rust as ts_rust;
use tree_sitter_swift as ts_swift;
use tree_sitter_typescript as ts_typescript;
use tree_sitter_verilog as ts_verilog;

pub fn extract_symbols_typesitter(content: &str, ext: &str) -> Vec<crate::types::Symbol> {
    // Map extension to language. Keep this mapping conservative and explicit.
    let lang: Language = match ext {
        "rs" => ts_rust::LANGUAGE.into(),
        "go" => ts_go::LANGUAGE.into(),
        "py" => ts_python::LANGUAGE.into(),
        "js" | "jsx" => ts_js::LANGUAGE.into(),
        "java" => ts_java::LANGUAGE.into(),
        "ts" => ts_typescript::LANGUAGE_TYPESCRIPT.into(),
        "tsx" => ts_typescript::LANGUAGE_TSX.into(),
        "c" | "h" | "cpp" | "cc" | "cxx" | "hpp" | "hxx" => ts_cpp::LANGUAGE.into(),
        "cs" => ts_c_sharp::LANGUAGE.into(),
        "swift" => ts_swift::LANGUAGE.into(),
        "v" | "sv" => ts_verilog::LANGUAGE.into(),
        "ml" | "mli" | "mll" => ts_ocaml::LANGUAGE_OCAML.into(),
        _ => return Vec::new(),
    };

    let mut parser = Parser::new();
    if parser.set_language(&lang).is_err() {
        return Vec::new();
    }

    let tree = match parser.parse(content, None) {
        Some(t) => t,
        None => return Vec::new(),
    };

    let root = tree.root_node();
    let mut out: Vec<crate::types::Symbol> = Vec::new();

    // Centralized descendant search for identifier-like kinds used by OCaml and others
    fn find_identifier_descendant(n: Node) -> Option<Node> {
        const KINDS: &[&str] = &[
            "identifier",
            "name",
            "value_name",
            "pattern",
            "constructor",
            "variant_constructor",
            "type_identifier",
            "type_name",
            "module_name",
            "lower_identifier",
            "upper_identifier",
            "class_name",
        ];
        for i in 0..n.child_count() {
            if let Some(c) = n.child(i) {
                let k = c.kind();
                if KINDS.contains(&k) {
                    return Some(c);
                }
                if let Some(found) = find_identifier_descendant(c) {
                    return Some(found);
                }
            }
        }
        None
    }

    // (helper removed: extract_from_pattern was unused)

    // Top-level traversal. We keep this generic but include OCaml-specific kinds
    // when deciding whether to attempt extraction from a node.
    fn visit(n: Node, content: &str, out: &mut Vec<crate::types::Symbol>, _ext: &str) {
        let kind = n.kind();

        // Kinds that are likely to declare a named symbol across languages
        // Include Swift-specific node kinds (protocol/extension/enum) so
        // those top-level declarations are discovered by the extractor.
        const TOP_KINDS: &[&str] = &[
            "function_item",
            "function_definition",
            "function_declaration",
            "value_binding",
            "let_binding",
            "value_description",
            "value_spec",
            "type_declaration",
            "interface_declaration",
            "interface_definition",
            "interface",
            "type_item",
            "module",
            "module_declaration",
            "module_definition",
            "module_binding",
            "class_declaration",
            "class_definition",
            "struct_item",
            "enum_item",
            "trait_item",
            "impl_item",
            "enum_declaration",
            "method_declaration",
            "method_definition",
            "protocol_declaration",
            "protocol",
            "extension_declaration",
            "extension",
            "macro_definition",
            "macro_rules",
        ];

        let want = TOP_KINDS.contains(&kind)
            || kind == "module" // be permissive for module-like nodes
            || kind.starts_with("module")
            || kind.starts_with("class")
            || kind.starts_with("interface")
            || kind.starts_with("trait")
            || kind.starts_with("impl")
            || kind.starts_with("protocol")
            || kind.starts_with("extension")
            || kind.starts_with("enum")
            || kind.starts_with("macro")
            || kind == "exception_definition";

        if want {
            // Special-case OCaml binding patterns and classes to get best name
            if let Some(nm) = find_identifier_descendant(n) {
                if let Ok(name) = nm.utf8_text(content.as_bytes()) {
                    let start = nm.start_byte();
                    out.push(crate::types::Symbol {
                        name: name.to_string(),
                        start: Some(start as u32),
                        line: Some(line_for_offset(content, start as u32)),
                    });
                }
            }
        }
    }

    // Simple DFS visit over the tree nodes
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        visit(node, content, &mut out, ext);
        // Push children in reverse so that traversal visits left-to-right.
        for i in (0..node.child_count()).rev() {
            if let Some(c) = node.child(i) {
                stack.push(c);
            }
        }
    }

    out
}

/// Backwards-compatible wrapper that reparses the content.
pub fn extract_import_aliases_typesitter(
    content: &str,
    ext: &str,
) -> Vec<(String, Option<String>, usize)> {
    // Forward to the with-tree implementation with no pre-parsed tree.
    extract_import_aliases_typesitter_with_tree(content, ext, None)
}

/// Extract import aliases using an optional pre-parsed tree to avoid reparsing.
pub fn extract_import_aliases_typesitter_with_tree(
    content: &str,
    ext: &str,
    tree_opt: Option<&tree_sitter::Tree>,
) -> Vec<(String, Option<String>, usize)> {
    // helper: strip surrounding quotes from a string literal node text
    fn extract_quoted(s: &str) -> Option<String> {
        let s = s.trim();
        if s.len() >= 2 {
            let first = s.chars().next().unwrap();
            let last = s.chars().next_back().unwrap();
            if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
                return Some(s[1..s.len() - 1].to_string());
            }
        }
        None
    }

    fn normalize_basename(path: &str) -> String {
        let p = path.trim();
        let p = p.trim_end_matches("/index");
        if let Some(seg) = p.rsplit('/').next() {
            let seg = seg.trim_end_matches(".js").trim_end_matches(".ts");
            return seg.to_string();
        }
        p.to_string()
    }

    // Acquire a tree: use provided or parse one for supported langs
    let owned_tree: Option<tree_sitter::Tree>;
    let lang: Language = match ext {
        "js" | "jsx" => ts_js::LANGUAGE.into(),
        "ts" => ts_typescript::LANGUAGE_TYPESCRIPT.into(),
        "tsx" => ts_typescript::LANGUAGE_TSX.into(),
        "py" => ts_python::LANGUAGE.into(),
        "rs" => ts_rust::LANGUAGE.into(),
        "go" => ts_go::LANGUAGE.into(),
        _ => return Vec::new(),
    };

    let root = if let Some(t) = tree_opt {
        t.root_node()
    } else {
        let mut parser = Parser::new();
        if parser.set_language(&lang).is_err() {
            return Vec::new();
        }
        owned_tree = parser.parse(content, None);
        if owned_tree.is_none() {
            return Vec::new();
        }
        owned_tree.as_ref().unwrap().root_node()
    };

    let mut out: Vec<(String, Option<String>, usize)> = Vec::new();

    // DFS over nodes
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        let kind = node.kind();
        match ext {
            "js" | "ts" | "tsx" | "jsx" => {
                if kind == "import_statement" || kind == "import_declaration" {
                    // find module string child
                    let mut module: Option<String> = None;
                    let mut mod_start = 0u32;
                    for i in 0..node.child_count() {
                        if let Some(c) = node.child(i) {
                            if c.kind() == "string" {
                                if let Ok(s) = c.utf8_text(content.as_bytes()) {
                                    if let Some(q) = extract_quoted(s) {
                                        module = Some(normalize_basename(&q));
                                        mod_start = c.start_byte() as u32;
                                    }
                                }
                            }
                        }
                    }
                    if let Some(modname) = module {
                        let lineno = line_for_offset(content, mod_start) as usize;
                        // (diagnostics removed)
                        // scan child nodes for imports
                        for i in 0..node.child_count() {
                            if let Some(c) = node.child(i) {
                                let ck = c.kind();
                                // (diagnostics removed)
                                if ck == "namespace_import" || ck == "import_namespace_specifier" {
                                    if let Some(alias_node) = c.child_by_field_name("name") {
                                        if let Ok(alias) = alias_node.utf8_text(content.as_bytes())
                                        {
                                            out.push((
                                                modname.clone(),
                                                Some(alias.to_string()),
                                                lineno,
                                            ));
                                        }
                                    }
                                } else if ck == "named_imports"
                                    || ck == "import_clause"
                                    || ck == "import_specifier"
                                {
                                    for j in 0..c.child_count() {
                                        if let Some(n2) = c.child(j) {
                                            // (diagnostics removed)
                                            let nk = n2.kind();
                                            if nk == "named_imports" {
                                                // descend into named_imports (contains import_specifier nodes)
                                                for k in 0..n2.child_count() {
                                                    if let Some(n3) = n2.child(k) {
                                                        let nk3 = n3.kind();
                                                        if nk3 == "import_specifier" {
                                                            if let Some(name_node) =
                                                                n3.child_by_field_name("name")
                                                            {
                                                                if let Ok(name_txt) = name_node
                                                                    .utf8_text(content.as_bytes())
                                                                {
                                                                    let alias = n3
                                                                        .child_by_field_name(
                                                                            "alias",
                                                                        )
                                                                        .and_then(|a| {
                                                                            a.utf8_text(
                                                                                content.as_bytes(),
                                                                            )
                                                                            .ok()
                                                                        })
                                                                        .map(|s| s.to_string());
                                                                    // (diagnostics removed)
                                                                    out.push((
                                                                        format!(
                                                                            "{}.{}",
                                                                            modname, name_txt
                                                                        ),
                                                                        alias,
                                                                        lineno,
                                                                    ));
                                                                }
                                                            } else {
                                                                // fallback: if import_specifier without fields, collect identifier child
                                                                for kk in 0..n3.child_count() {
                                                                    if let Some(n4) = n3.child(kk) {
                                                                        let nk4 = n4.kind();
                                                                        if nk4 == "identifier" || nk4 == "property_identifier" {
                                                                                if let Ok(name_str) = n4.utf8_text(content.as_bytes()) {
                                                                                    out.push((format!("{}.{}", modname, name_str), None, lineno));
                                                                                }
                                                                            }
                                                                    }
                                                                }
                                                            }
                                                        } else if nk3 == "identifier"
                                                            || nk3 == "property_identifier"
                                                        {
                                                            if let Ok(name_str) =
                                                                n3.utf8_text(content.as_bytes())
                                                            {
                                                                out.push((
                                                                    format!(
                                                                        "{}.{}",
                                                                        modname, name_str
                                                                    ),
                                                                    None,
                                                                    lineno,
                                                                ));
                                                            }
                                                        }
                                                    }
                                                }
                                                // continue to next sibling
                                                continue;
                                            }
                                            if nk == "identifier" || nk == "property_identifier" {
                                                if let Ok(name_str) =
                                                    n2.utf8_text(content.as_bytes())
                                                {
                                                    out.push((
                                                        format!("{}.{}", modname, name_str),
                                                        None,
                                                        lineno,
                                                    ));
                                                }
                                            } else if nk == "import_specifier" {
                                                // Prefer field-based extraction when the parser provides
                                                // named fields for import specifiers (name / alias).
                                                if let Some(name_node) =
                                                    n2.child_by_field_name("name")
                                                {
                                                    if let Ok(name_txt) =
                                                        name_node.utf8_text(content.as_bytes())
                                                    {
                                                        // Debug: print extracted name/alias candidates
                                                        let alias = n2
                                                            .child_by_field_name("alias")
                                                            .and_then(|a| {
                                                                a.utf8_text(content.as_bytes()).ok()
                                                            })
                                                            .map(|s| s.to_string());
                                                        // (diagnostics removed)
                                                        out.push((
                                                            format!("{}.{}", modname, name_txt),
                                                            alias,
                                                            lineno,
                                                        ));
                                                        continue;
                                                    }
                                                }
                                                // Fallback: collect child texts and heuristically find alias
                                                let mut spec_children: Vec<String> = Vec::new();
                                                for k in 0..n2.child_count() {
                                                    if let Some(n3) = n2.child(k) {
                                                        if let Ok(txt) =
                                                            n3.utf8_text(content.as_bytes())
                                                        {
                                                            let t = txt.trim();
                                                            if !t.is_empty() && t != "," {
                                                                spec_children.push(t.to_string());
                                                            }
                                                        }
                                                    }
                                                }
                                                // spec_children may contain ["a"] or ["a", "as", "b"] or ["a", "b"]
                                                if !spec_children.is_empty() {
                                                    let name = spec_children[0].clone();
                                                    // find alias if present as last element and not the token "as"
                                                    let alias = if spec_children.len() >= 3
                                                        && spec_children[1] == "as"
                                                    {
                                                        Some(spec_children[2].clone())
                                                    } else if spec_children.len() == 2
                                                        && spec_children[0] != "as"
                                                    {
                                                        // sometimes parser presents two idents
                                                        Some(spec_children[1].clone())
                                                    } else {
                                                        None
                                                    };
                                                    out.push((
                                                        format!("{}.{}", modname, name),
                                                        alias,
                                                        lineno,
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        // default import detection: look for identifier child at top level
                        for i in 0..node.child_count() {
                            if let Some(d) = node.child(i) {
                                if d.kind() == "identifier" || d.kind() == "property_identifier" {
                                    if let Ok(alias) = d.utf8_text(content.as_bytes()) {
                                        out.push((
                                            modname.clone(),
                                            Some(alias.to_string()),
                                            lineno,
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "py" => {
                if kind.contains("import") {
                    if let Some(snippet) = content.get(node.start_byte()..node.end_byte()) {
                        let s = snippet.replace('\n', " ");
                        if let Some(pos) = s.find("from ") {
                            if let Some(impos) = s[pos..].find(" import ") {
                                let module = s[pos + 5..pos + impos]
                                    .trim()
                                    .trim_start_matches('.')
                                    .to_string();
                                let rest = s[pos + impos + 8..]
                                    .trim()
                                    .trim_start_matches('(')
                                    .trim_end_matches(')');
                                for part in rest.split(',') {
                                    let token = part.trim();
                                    if token.is_empty() || token == "*" {
                                        continue;
                                    }
                                    if let Some(idx) = token.find(" as ") {
                                        let name = token[..idx].trim();
                                        let alias = token[idx + 4..].trim();
                                        out.push((
                                            format!("{}.{}", module, name),
                                            Some(alias.to_string()),
                                            node.start_position().row,
                                        ));
                                    } else {
                                        let name = token.split_whitespace().next().unwrap_or("");
                                        if !name.is_empty() {
                                            out.push((
                                                format!("{}.{}", module, name),
                                                None,
                                                node.start_position().row,
                                            ));
                                        }
                                    }
                                }
                            }
                        } else if let Some(pos) = s.find("import ") {
                            let rest = s[pos + 7..].trim();
                            for part in rest.split(',') {
                                let token = part.trim();
                                if token.is_empty() {
                                    continue;
                                }
                                if let Some(idx) = token.find(" as ") {
                                    let module = token[..idx].trim().to_string();
                                    let alias = token[idx + 4..].trim();
                                    out.push((
                                        module,
                                        Some(alias.to_string()),
                                        node.start_position().row,
                                    ));
                                } else {
                                    let module =
                                        token.split_whitespace().next().unwrap_or("").to_string();
                                    if !module.is_empty() {
                                        out.push((module, None, node.start_position().row));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "rs" => {
                if kind.starts_with("use") || kind == "use_declaration" {
                    if let Some(snippet) = content.get(node.start_byte()..node.end_byte()) {
                        let s = snippet.replace('\n', " ");
                        if let Some(idx) = s.find("::{") {
                            let base = s[..idx].trim().trim_start_matches("use ").to_string();
                            if let Some(start) = s.find('{') {
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
                                            out.push((
                                                format!("{}.{}", base.replace("::", "."), name),
                                                Some(alias.to_string()),
                                                line_for_offset(content, node.start_byte() as u32)
                                                    as usize,
                                            ));
                                        } else {
                                            let name =
                                                token.split_whitespace().next().unwrap_or("");
                                            out.push((
                                                format!("{}.{}", base.replace("::", "."), name),
                                                None,
                                                line_for_offset(content, node.start_byte() as u32)
                                                    as usize,
                                            ));
                                        }
                                    }
                                }
                            }
                        } else if s.contains(" as ") {
                            if let Some(pos) = s.find(" as ") {
                                let module = s[..pos].trim().trim_start_matches("use ").to_string();
                                let alias = s[pos + 4..].trim().trim_end_matches(';');
                                out.push((
                                    module.replace("::", "."),
                                    Some(alias.to_string()),
                                    line_for_offset(content, node.start_byte() as u32) as usize,
                                ));
                            }
                        } else {
                            let module = s
                                .trim()
                                .trim_start_matches("use ")
                                .trim_end_matches(';')
                                .to_string();
                            out.push((
                                module.replace("::", "."),
                                None,
                                line_for_offset(content, node.start_byte() as u32) as usize,
                            ));
                        }
                    }
                }
            }
            "go" => {
                if kind.contains("import") {
                    if let Some(snippet) = content.get(node.start_byte()..node.end_byte()) {
                        let s = snippet.replace('\n', " ");
                        let parts: Vec<&str> = s.split('"').collect();
                        for i in (1..parts.len()).step_by(2) {
                            let module = parts[i];
                            let before = parts[i - 1];
                            let alias = before
                                .split_whitespace()
                                .next_back()
                                .map(|s| s.trim())
                                .filter(|s| !s.is_empty());
                            if let Some(a) = alias {
                                out.push((
                                    normalize_basename(module),
                                    Some(a.to_string()),
                                    line_for_offset(content, node.start_byte() as u32) as usize,
                                ));
                            } else {
                                out.push((
                                    normalize_basename(module),
                                    None,
                                    line_for_offset(content, node.start_byte() as u32) as usize,
                                ));
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        // Push children in reverse so that traversal visits left-to-right.
        for i in (0..node.child_count()).rev() {
            if let Some(c) = node.child(i) {
                stack.push(c);
            }
        }
    }

    out
}

// local helper: compute 0-based line index for a byte offset
fn line_for_offset(content: &str, pos: u32) -> u32 {
    let bytes = content.as_bytes();
    let mut idx = 0usize;
    let mut line = 1u32; // 1-based
    while idx < bytes.len() && (idx as u32) < pos {
        if bytes[idx] == b'\n' {
            line += 1;
        }
        idx += 1;
    }
    line
}

// (No duplicate stubs here) The real implementations appear earlier in the file
// and should be used. Deleted duplicate placeholder definitions.

#[cfg(test)]
mod tests {
    use super::*;

    fn find_symbol<'a>(
        syms: &'a [crate::types::Symbol],
        name: &str,
    ) -> Option<&'a crate::types::Symbol> {
        syms.iter().find(|s| s.name == name)
    }

    fn line_for_offset_test(content: &str, off: usize) -> u32 {
        let mut line = 1u32;
        for (i, b) in content.as_bytes().iter().enumerate() {
            if i >= off {
                break;
            }
            if *b == b'\n' {
                line += 1;
            }
        }
        line
    }

    #[test]
    fn rust_symbols_are_extracted_with_offsets() {
        let src = r#"
pub struct Foo {
    a: i32,
}

impl Foo {
    pub fn new() -> Self { Foo { a: 0 } }
}

fn helper() {}
"#;
        let syms = extract_symbols_typesitter(src, "rs");
        // debug removed
        let foo_off = src.find("Foo").expect("Foo in src");
        let foo = find_symbol(&syms, "Foo").expect("Foo symbol");
        assert_eq!(foo.start, Some(foo_off as u32));
        assert_eq!(foo.line, Some(line_for_offset_test(src, foo_off)));

        let helper_off = src.find("helper").expect("helper in src");
        let helper = find_symbol(&syms, "helper").expect("helper symbol");
        assert_eq!(helper.start, Some(helper_off as u32));
        assert_eq!(helper.line, Some(line_for_offset_test(src, helper_off)));
    }

    // (other tests omitted for brevity in this restore — kept minimal)
}

#[cfg(test)]
mod alias_tests {
    use super::*;
    use tree_sitter::Parser;

    fn parse_and_extract(src: &str, ext: &str) -> Vec<(String, Option<String>, usize)> {
        let lang = match ext {
            "py" => ts_python::LANGUAGE.into(),
            "rs" => ts_rust::LANGUAGE.into(),
            "go" => ts_go::LANGUAGE.into(),
            "js" => ts_js::LANGUAGE.into(),
            "ts" => ts_typescript::LANGUAGE_TYPESCRIPT.into(),
            _ => return Vec::new(),
        };
        let mut parser = Parser::new();
        parser.set_language(&lang).unwrap();
        let tree = parser.parse(src, None).unwrap();
        extract_import_aliases_typesitter_with_tree(src, ext, Some(&tree))
    }

    #[test]
    fn python_from_imports_with_aliases() {
        let src = r#"
from pkg.subpkg import (a as a_alias, b, c as c_alias)
import x as x_alias, y
"#;
        let out = parse_and_extract(src, "py");
        // debug removed
        assert!(
            out.contains(&("pkg.subpkg.a".to_string(), Some("a_alias".to_string()), 1))
                || out.contains(&("pkg.subpkg.a".to_string(), Some("a_alias".to_string()), 0))
        );
    }
}
