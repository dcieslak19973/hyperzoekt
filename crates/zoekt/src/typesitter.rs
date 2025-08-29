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
