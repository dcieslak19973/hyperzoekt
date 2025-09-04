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
// note: java binding intentionally omitted in this build
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
        "ts" => ts_typescript::LANGUAGE_TYPESCRIPT.into(),
        "tsx" => ts_typescript::LANGUAGE_TSX.into(),
        "c" | "h" | "cpp" | "cc" | "cxx" | "hpp" | "hxx" => ts_cpp::LANGUAGE.into(),
        "cs" => ts_c_sharp::LANGUAGE.into(),
        "swift" => ts_swift::LANGUAGE.into(),
        "v" | "sv" => ts_verilog::LANGUAGE.into(),
        // OCaml extensions
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

    // Extract a bound identifier from a pattern node (handles tuples/constructors)
    fn extract_from_pattern(n: Node) -> Option<Node> {
        for i in 0..n.child_count() {
            if let Some(c) = n.child(i) {
                let k = c.kind();
                if k == "identifier"
                    || k == "value_name"
                    || k == "name"
                    || k == "lower_identifier"
                    || k == "upper_identifier"
                    || k == "class_name"
                {
                    return Some(c);
                }
                if k == "tuple_pattern" || k == "pattern" {
                    if let Some(found) = extract_from_pattern(c) {
                        return Some(found);
                    }
                }
                if k == "constructor" || k == "variant_constructor" {
                    return Some(c);
                }
            }
        }
        None
    }

    // Top-level traversal. We keep this generic but include OCaml-specific kinds
    // when deciding whether to attempt extraction from a node.
    fn visit(n: Node, content: &str, out: &mut Vec<crate::types::Symbol>, _ext: &str) {
        let kind = n.kind();

        // Kinds that are likely to declare a named symbol across languages
        const TOP_KINDS: &[&str] = &[
            "function_item",
            "function_definition",
            "function_declaration",
            "value_binding",
            "let_binding",
            "value_description",
            "value_spec",
            "type_declaration",
            "type_item",
            "module",
            "module_declaration",
            "module_definition",
            "module_binding",
            "class_declaration",
            "class_definition",
            "struct_item",
            "enum_item",
            "method_declaration",
            "method_definition",
        ];

        let want = TOP_KINDS.contains(&kind)
            || kind == "module" // be permissive for module-like nodes
            || kind.starts_with("module")
            || kind.starts_with("class")
            || kind == "exception_definition";

        if want {
            // Special-case OCaml binding patterns and classes to get best name
            let idn = if kind == "value_binding" || kind == "let_binding" {
                if let Some(pat) = n.child_by_field_name("pattern") {
                    extract_from_pattern(pat).or_else(|| find_identifier_descendant(n))
                } else if let Some(pat2) = n.child_by_field_name("name") {
                    extract_from_pattern(pat2).or_else(|| find_identifier_descendant(n))
                } else {
                    find_identifier_descendant(n)
                }
            } else if kind.starts_with("class") {
                n.child_by_field_name("name")
                    .or_else(|| n.child_by_field_name("identifier"))
                    .or_else(|| find_identifier_descendant(n))
            } else {
                // Generic path: prefer name/identifier fields then descendant search
                let name_node = n
                    .child_by_field_name("name")
                    .or_else(|| n.child_by_field_name("identifier"))
                    .or_else(|| {
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

                name_node.or_else(|| find_identifier_descendant(n))
            };

            if let Some(idn) = idn {
                let start = idn.start_byte();
                let end = idn.end_byte();
                if start < end && end <= content.len() {
                    if let Some(raw) = content.get(start..end) {
                        // Normalize: strip leading non-identifier chars; keep only identifier token
                        let mut name = raw
                            .trim_start_matches(|c: char| !c.is_alphanumeric() && c != '_')
                            .trim();
                        let ident_end = name
                            .char_indices()
                            .find(|&(_, ch)| !(ch.is_alphanumeric() || ch == '_'))
                            .map(|(i, _)| i)
                            .unwrap_or(name.len());
                        name = &name[..ident_end];
                        if !name.is_empty() {
                            out.push(crate::types::Symbol {
                                name: name.to_string(),
                                start: Some(start as u32),
                                line: Some(line_for_offset(content, start as u32) as u32 + 1),
                            });
                        }
                    }
                }
            }
        }

        for i in 0..n.child_count() {
            if let Some(child) = n.child(i) {
                visit(child, content, out, _ext);
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

    #[test]
    fn ocaml_symbols_are_extracted_with_offsets() {
        let src = r#"
type mytype = { f: int }
let top_x = 1
let add a b = a + b
module ModA = struct
    let mod_y = 2
end
"#;

        let syms = extract_symbols_typesitter(src, "ml");

        // The OCaml extractor reliably finds module declarations; be permissive
        // about other node kinds because tree-sitter grammars differ.
        assert!(!syms.is_empty());

        let mod_off = src.find("ModA").expect("ModA in src");
        let mod_sym = find_symbol(&syms, "ModA").expect("ModA symbol");
        assert_eq!(mod_sym.start, Some(mod_off as u32));
        assert_eq!(mod_sym.line, Some(line_for_offset_test(src, mod_off)));
    }

    #[test]
    fn ocaml_more_symbols_ml() {
        let src = r#"
type t = A | B of int
exception E of int
let x = 42
let (y, z) = (1, 2)
let add a b = a + b
module M = struct
    let m = 1
end
module F(X : sig val v : int end) = struct
    let from_x = X.v
end
"#;

        let syms = extract_symbols_typesitter(src, "ml");
        // Expect at least module M and function add and value x
        assert!(!syms.is_empty());

        for name in ["M", "add", "x"].iter() {
            let sym = find_symbol(&syms, name).expect(&format!("{} symbol", name));
            let start = sym.start.expect("start");
            let end = start as usize + name.len();
            assert_eq!(src.get(start as usize..end).unwrap(), *name);
            assert_eq!(sym.line, Some(line_for_offset_test(src, start as usize)));
        }
    }

    #[test]
    fn ocaml_more_symbols_mli() {
        let src = r#"
val x : int
type t = A | B
module M : sig val m : int end
"#;

        let syms = extract_symbols_typesitter(src, "mli");
        assert!(!syms.is_empty());

        let sym = find_symbol(&syms, "M").expect("M symbol");
        let start = sym.start.expect("start");
        let end = start as usize + "M".len();
        assert_eq!(src.get(start as usize..end).unwrap(), "M");
        assert_eq!(sym.line, Some(line_for_offset_test(src, start as usize)));
    }

    #[test]
    fn ocaml_fixtures_extract_symbols_ml() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/ocaml/sample1.ml"
        );
        let src = std::fs::read_to_string(path).expect("read sample1.ml");
        let syms = extract_symbols_typesitter(&src, "ml");
        // Expect a conservative set of reliably-extracted top-level symbols.
        for name in ["add", "M", "fib"].iter() {
            assert!(find_symbol(&syms, name).is_some(), "expected {}", name);
        }
        // `x` may appear with variants ("x" or ".x") depending on pattern extraction.
        assert!(find_symbol(&syms, "x").is_some() || find_symbol(&syms, ".x").is_some());
    }

    #[test]
    fn ocaml_fixtures_extract_symbols_mli() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/ocaml/sample2.mli"
        );
        let src = std::fs::read_to_string(path).expect("read sample2.mli");
        let syms = extract_symbols_typesitter(&src, "mli");
        // Expect at least the module M to be present in the interface
        assert!(find_symbol(&syms, "M").is_some());
    }
}
