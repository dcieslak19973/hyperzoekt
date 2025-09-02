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

use std::cell::RefCell;
use std::collections::HashMap;
use tree_sitter::{Language, Node, Parser};
// use the language bindings with short aliases to match other code in workspace
use tree_sitter_c_sharp as ts_c_sharp;
use tree_sitter_cpp as ts_cpp;
use tree_sitter_go as ts_go;
use tree_sitter_java as ts_java;
use tree_sitter_javascript as ts_js;
use tree_sitter_python as ts_python;
use tree_sitter_rust as ts_rust;
use tree_sitter_swift as ts_swift;
use tree_sitter_typescript as ts_typescript;
use tree_sitter_verilog as ts_verilog;

pub fn extract_symbols_typesitter(content: &str, ext: &str) -> Vec<crate::types::Symbol> {
    // Map extension to a static language reference and a static key used by the cache.
    let (lang, key): (Language, &'static str) = match ext {
        "rs" => (ts_rust::LANGUAGE.into(), "rs"),
        "go" => (ts_go::LANGUAGE.into(), "go"),
        "py" => (ts_python::LANGUAGE.into(), "py"),
        "js" | "jsx" => (ts_js::LANGUAGE.into(), "js"),
        "ts" => (ts_typescript::LANGUAGE_TYPESCRIPT.into(), "ts"),
        "tsx" => (ts_typescript::LANGUAGE_TSX.into(), "tsx"),
        "c" | "h" | "cpp" | "cc" | "cxx" | "hpp" | "hxx" => (ts_cpp::LANGUAGE.into(), "cpp"),
        "java" => (ts_java::LANGUAGE.into(), "java"),
        "cs" => (ts_c_sharp::LANGUAGE.into(), "cs"),
        "swift" => (ts_swift::LANGUAGE.into(), "swift"),
        "sv" | "v" => (ts_verilog::LANGUAGE.into(), "verilog"),
        _ => return Vec::new(),
    };

    // Thread-local cache of Parser instances keyed by language short name.
    thread_local! {
        static PARSERS: RefCell<HashMap<&'static str, Parser>> = RefCell::new(HashMap::new());
    }

    let tree = PARSERS.with(|cell| {
        let mut map = cell.borrow_mut();
        let parser = map.entry(key).or_insert_with(|| {
            let mut p = Parser::new();
            // set_language should succeed for known-good bindings; if it fails we return empty
            let _ = p.set_language(&lang);
            p
        });
        parser.parse(content, None)
    });

    let tree = match tree {
        Some(t) => t,
        None => return Vec::new(),
    };

    let root = tree.root_node();
    let mut out = Vec::new();

    // Recursive walk to find nodes that represent top-level symbols.
    fn visit(n: Node, content: &str, out: &mut Vec<crate::types::Symbol>, ext: &str) {
        // Helpful kinds to look for per-language. These are intentionally
        // broad; we then try to find an identifier/name child.
        let kind = n.kind();

        let want = match ext {
            "rs" => matches!(
                kind,
                "function_item" | "struct_item" | "enum_item" | "trait_item"
            ),
            "go" => matches!(
                kind,
                "function_declaration" | "method_declaration" | "method_spec"
            ),
            "py" => matches!(kind, "function_definition" | "class_definition"),
            "js" | "jsx" | "ts" | "tsx" => matches!(
                kind,
                "function_declaration" | "method_definition" | "class_declaration"
            ),
            "java" => matches!(kind, "class_declaration" | "method_declaration"),
            "c" | "cpp" | "cc" | "cxx" | "hpp" | "hxx" | "h" => {
                matches!(
                    kind,
                    "function_definition" | "class_specifier" | "struct_specifier"
                )
            }
            "cs" => matches!(kind, "class_declaration" | "method_declaration"),
            "swift" => matches!(kind, "function_declaration" | "class_declaration"),
            "sv" | "v" => matches!(kind, "module_declaration" | "interface_declaration"),
            _ => false,
        };

        if want {
            // Prefer a field named "name" or "identifier". Fall back to
            // searching children for the first identifier-like node.
            let name_node = n
                .child_by_field_name("name")
                .or_else(|| n.child_by_field_name("identifier"))
                .or_else(|| {
                    // Search children for an "identifier"-kind node.
                    for i in 0..n.child_count() {
                        if let Some(c) = n.child(i) {
                            let k = c.kind();
                            if k == "identifier" || k == "name" {
                                return Some(c);
                            }
                        }
                    }
                    None
                });

            if let Some(idn) = name_node {
                let start = idn.start_byte();
                let end = idn.end_byte();
                if start < end && end <= content.len() {
                    if let Some(name) = content.get(start..end) {
                        out.push(crate::types::Symbol {
                            name: name.to_string(),
                            start: Some(start as u32),
                            line: Some(line_for_offset(content, start as u32) as u32 + 1),
                        });
                    }
                }
            }
        }

        for i in 0..n.child_count() {
            if let Some(child) = n.child(i) {
                visit(child, content, out, ext);
            }
        }
    }

    visit(root, content, &mut out, ext);
    out
}

// local helper: compute 0-based line index for a byte offset
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
        let foo_off = src.find("Foo").expect("Foo in src");
        let foo = find_symbol(&syms, "Foo").expect("Foo symbol");
        assert_eq!(foo.start, Some(foo_off as u32));
        assert_eq!(foo.line, Some(line_for_offset_test(src, foo_off)));

        let helper_off = src.find("helper").expect("helper in src");
        let helper = find_symbol(&syms, "helper").expect("helper symbol");
        assert_eq!(helper.start, Some(helper_off as u32));
        assert_eq!(helper.line, Some(line_for_offset_test(src, helper_off)));
    }

    #[test]
    fn go_symbols_are_extracted_with_offsets() {
        let src = r#"
package main

func Add(x int, y int) int { return x + y }

func (s *Server) Start() {}
"#;
        let syms = extract_symbols_typesitter(src, "go");
        let add_off = src.find("Add").expect("Add in src");
        let add = find_symbol(&syms, "Add").expect("Add symbol");
        assert_eq!(add.start, Some(add_off as u32));
        assert_eq!(add.line, Some(line_for_offset_test(src, add_off)));
        assert!(!syms.is_empty());
    }

    #[test]
    fn python_symbols_are_extracted_with_offsets() {
        let src = r#"
class C:
    def method(self):
        pass

def free_func():
    return 1
"#;
        let syms = extract_symbols_typesitter(src, "py");
        let c_off = src.find("C:").expect("C in src");
        let c_sym = find_symbol(&syms, "C").expect("C symbol");
        assert_eq!(c_sym.start, Some(src.find("C").unwrap() as u32));
        assert_eq!(c_sym.line, Some(line_for_offset_test(src, c_off)));

        let f_off = src.find("free_func").expect("free_func in src");
        let f_sym = find_symbol(&syms, "free_func").expect("free_func symbol");
        assert_eq!(f_sym.start, Some(f_off as u32));
        assert_eq!(f_sym.line, Some(line_for_offset_test(src, f_off)));
    }
}
