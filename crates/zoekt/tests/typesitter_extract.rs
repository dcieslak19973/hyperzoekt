use zoekt_rs::types;
use zoekt_rs::typesitter;

fn find_symbol<'a>(syms: &'a [types::Symbol], name: &str) -> Option<&'a types::Symbol> {
    syms.iter().find(|s| s.name == name)
}

fn line_for_offset(content: &str, off: usize) -> u32 {
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
    let syms = typesitter::extract_symbols_typesitter(src, "rs");
    // Foo
    let foo_off = src.find("Foo").expect("Foo in src");
    let foo = find_symbol(&syms, "Foo").expect("Foo symbol");
    assert_eq!(foo.start, Some(foo_off as u32));
    assert_eq!(foo.line, Some(line_for_offset(src, foo_off)));

    // helper
    let helper_off = src.find("helper").expect("helper in src");
    let helper = find_symbol(&syms, "helper").expect("helper symbol");
    assert_eq!(helper.start, Some(helper_off as u32));
    assert_eq!(helper.line, Some(line_for_offset(src, helper_off)));
}

#[test]
fn go_symbols_are_extracted_with_offsets() {
    let src = r#"
package main

func Add(x int, y int) int { return x + y }

func (s *Server) Start() {}
"#;
    let syms = typesitter::extract_symbols_typesitter(src, "go");
    let add_off = src.find("Add").expect("Add in src");
    let add = find_symbol(&syms, "Add").expect("Add symbol");
    assert_eq!(add.start, Some(add_off as u32));
    assert_eq!(add.line, Some(line_for_offset(src, add_off)));
    // ensure at least one symbol present
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
    let syms = typesitter::extract_symbols_typesitter(src, "py");
    let c_off = src.find("C:").expect("C in src");
    // extractor records name without trailing ':'
    let c_sym = find_symbol(&syms, "C").expect("C symbol");
    assert_eq!(c_sym.start, Some(src.find("C").unwrap() as u32));
    assert_eq!(c_sym.line, Some(line_for_offset(src, c_off)));

    let f_off = src.find("free_func").expect("free_func in src");
    let f_sym = find_symbol(&syms, "free_func").expect("free_func symbol");
    assert_eq!(f_sym.start, Some(f_off as u32));
    assert_eq!(f_sym.line, Some(line_for_offset(src, f_off)));
}
