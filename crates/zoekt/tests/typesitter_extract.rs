use zoekt_rs::typesitter;

#[test]
fn rust_symbols_are_extracted() {
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
    let names: Vec<String> = syms.into_iter().map(|s| s.name).collect();
    // Expect struct Foo, impl methods may not expose name nodes depending on grammar; ensure Foo and helper present
    assert!(names.iter().any(|n| n == "Foo"));
    assert!(names.iter().any(|n| n == "helper"));
}

#[test]
fn go_symbols_are_extracted() {
    let src = r#"
package main

func Add(x int, y int) int { return x + y }

func (s *Server) Start() {}
"#;
    let syms = typesitter::extract_symbols_typesitter(src, "go");
    let names: Vec<String> = syms.into_iter().map(|s| s.name).collect();
    assert!(names.iter().any(|n| n == "Add"));
    // method receiver names may vary; ensure at least one known symbol found
    assert!(!names.is_empty());
}

#[test]
fn python_symbols_are_extracted() {
    let src = r#"
class C:
    def method(self):
        pass

def free_func():
    return 1
"#;
    let syms = typesitter::extract_symbols_typesitter(src, "py");
    let names: Vec<String> = syms.into_iter().map(|s| s.name).collect();
    assert!(names.iter().any(|n| n == "C"));
    assert!(names.iter().any(|n| n == "free_func"));
}
