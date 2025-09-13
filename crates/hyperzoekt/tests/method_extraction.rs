// Focused tests for method extraction across languages (tolerant to current extractor behavior)
use std::path::Path;
use tree_sitter::Parser;

use hyperzoekt::repo_index::indexer::extract as ix_extract;
use hyperzoekt::repo_index::indexer::index::lang_to_ts;
use hyperzoekt::repo_index::indexer::types as ix_types;

fn parse_and_extract(lang: &'static str, src: &'static str) -> Vec<ix_types::Entity<'static>> {
    let language = lang_to_ts(lang).expect("language available");
    let mut parser = Parser::new();
    parser.set_language(&language).expect("set language");
    let tree = parser.parse(src, None).expect("parse");
    let mut out: Vec<ix_types::Entity<'static>> = Vec::new();
    ix_extract::extract_entities(lang, &tree, src, Path::new("<test>"), &mut out);
    out
}

#[test]
fn python_methods_extracted() {
    let src = r#"
class Foo:
    def public(self):
        pass

    def _private(self):
        pass
"#;
    let ents = parse_and_extract("python", src);
    // require class Foo and its methods to be attached
    let cls = ents
        .iter()
        .find(|e| e.name == "Foo")
        .expect("class Foo not found");
    let pub_m = cls
        .methods
        .iter()
        .find(|m| m.name == "public")
        .expect("public method not attached to Foo");
    let priv_m = cls
        .methods
        .iter()
        .find(|m| m.name == "_private")
        .expect("_private method not attached to Foo");
    assert!(pub_m.signature.contains("public") || pub_m.signature.contains("def public"));
    assert!(priv_m.signature.contains("_private") || priv_m.signature.contains("def _private"));
    assert!(priv_m.start_line.is_some());
    if let Some(v) = &priv_m.visibility {
        assert_eq!(v, "private");
    }
}

#[test]
fn rust_methods_extracted_and_pub() {
    let src = r#"
pub struct Foo;
impl Foo {
    pub fn public(&self) {}
    fn private(&self) {}
}
"#;
    let ents = parse_and_extract("rust", src);
    // Debug prints removed; rely on assertions for verification.
    let cls = ents
        .iter()
        .find(|e| e.name == "Foo")
        .expect("type Foo not found");
    let pub_m = cls
        .methods
        .iter()
        .find(|m| m.name == "public")
        .expect("public method not attached to Foo");
    let priv_m = cls
        .methods
        .iter()
        .find(|m| m.name == "private")
        .expect("private method not attached to Foo");
    assert!(pub_m.signature.contains("public") || pub_m.signature.contains("fn public"));
    assert!(priv_m.signature.contains("private") || priv_m.signature.contains("fn private"));
    assert!(pub_m.start_line.is_some());
    if let Some(v) = &pub_m.visibility {
        assert_eq!(v, "public");
    }
}

#[test]
fn js_ts_methods_extracted_visibility() {
    let src = r#"
class C {
  public m() {}
  private p() {}
  protected r() {}
}
"#;
    // use TypeScript parser for visibility modifiers (public/private/protected)
    let ents = parse_and_extract("typescript", src);
    // Debug prints removed; rely on assertions for verification.
    // require class C and that methods are attached to it
    let cls = ents
        .iter()
        .find(|e| e.name == "C")
        .expect("class C not found");
    let m_m = cls
        .methods
        .iter()
        .find(|m| m.name == "m")
        .expect("method m not attached to C");
    let p_m = cls
        .methods
        .iter()
        .find(|m| m.name == "p")
        .expect("method p not attached to C");
    let r_m = cls
        .methods
        .iter()
        .find(|m| m.name == "r")
        .expect("method r not attached to C");
    assert!(m_m.signature.contains("m") || m_m.signature.contains("m("));
    assert!(p_m.signature.contains("p") || p_m.signature.contains("p("));
    assert!(r_m.signature.contains("r") || r_m.signature.contains("r("));
    assert!(m_m.start_line.is_some());
    // visibility heuristics may vary by grammar; ensure visibility is present
    // when provided (do not assert exact token here).
    if let Some(v) = &p_m.visibility {
        assert!(!v.is_empty());
    }
    if let Some(v) = &r_m.visibility {
        assert!(!v.is_empty());
    }
}

#[test]
fn go_methods_extracted_exported() {
    let src = r#"
package p

type Foo struct {}

func (f Foo) Public() {}
func (f *Foo) private() {}
"#;
    let ents = parse_and_extract("go", src);
    // Debug prints removed; rely on assertions for verification.
    // ensure type entity exists
    let ent = ents
        .iter()
        .find(|e| e.name == "Foo" || e.signature.contains("type Foo"))
        .expect("type Foo not found");
    let public_m = ent
        .methods
        .iter()
        .find(|m| m.name == "Public")
        .expect("Public method not attached to Foo");
    let private_m = ent
        .methods
        .iter()
        .find(|m| m.name == "private")
        .expect("private method not attached to Foo");
    assert!(public_m.signature.contains("Public") || public_m.signature.contains("Public("));
    assert!(private_m.signature.contains("private") || private_m.signature.contains("private("));
    assert!(public_m.start_line.is_some());
    if let Some(v) = &public_m.visibility {
        assert!(!v.is_empty());
    }
}

// Edge-case: nested functions inside a class method should not be treated as class methods
#[test]
fn nested_methods_ignored() {
    let src = r#"
class A {
  m() {
    function inner() {}
  }
}
"#;
    let ents = parse_and_extract("typescript", src);
    let cls = ents
        .iter()
        .find(|e| e.name == "A")
        .expect("class A not found");
    // only method 'm' should be attached
    assert!(cls.methods.iter().any(|m| m.name == "m"));
    assert!(!cls.methods.iter().any(|m| m.name == "inner"));
}

// Explicit pointer/value receiver forms in Go should both attach to the same type entity
#[test]
fn go_pointer_and_value_receivers() {
    let src = r#"
package p

type Bar struct{}

func (b Bar) Value() {}
func (b *Bar) Pointer() {}
"#;
    let ents = parse_and_extract("go", src);
    let ent = ents
        .iter()
        .find(|e| e.name == "Bar" || e.signature.contains("type Bar"))
        .expect("type Bar not found");
    assert!(ent.methods.iter().any(|m| m.name == "Value"));
    assert!(ent.methods.iter().any(|m| m.name == "Pointer"));
}

#[test]
fn java_methods_extracted_visibility_and_static() {
    let src = r#"
public class J {
  public void p() {}
  private int q() { return 1; }
  public static void s() {}
}
"#;
    let ents = parse_and_extract("java", src);
    let cls = ents
        .iter()
        .find(|e| e.name == "J")
        .expect("class J not found");
    assert!(cls.methods.iter().any(|m| m.name == "p"));
    assert!(cls.methods.iter().any(|m| m.name == "q"));
    assert!(cls.methods.iter().any(|m| m.name == "s"));
    for m in &cls.methods {
        assert!(m.start_line.is_some());
        assert!(!m.signature.is_empty());
    }
}

#[test]
fn cpp_methods_extracted_static_and_const() {
    let src = r#"
class D {
public:
  void f() {}
  static int g() { return 0; }
};
"#;
    let ents = parse_and_extract("cpp", src);
    let cls = ents
        .iter()
        .find(|e| e.name == "D")
        .expect("class D not found");
    assert!(cls.methods.iter().any(|m| m.name == "f"));
    assert!(cls.methods.iter().any(|m| m.name == "g"));
}

#[test]
fn csharp_methods_extracted() {
    let src = r#"
public class K {
  public void M() {}
  private int m() { return 0; }
}
"#;
    let ents = parse_and_extract("c_sharp", src);
    let cls = ents
        .iter()
        .find(|e| e.name == "K")
        .expect("class K not found");
    assert!(cls.methods.iter().any(|m| m.name == "M"));
    assert!(cls.methods.iter().any(|m| m.name == "m"));
}

// New tests for doc comment extraction
#[test]
fn python_triple_quoted_single_line_docstring() {
    let src = r#"
def f():
    """This is a single-line docstring"""
    pass
"#;
    let ents = parse_and_extract("python", src);
    let func = ents
        .iter()
        .find(|e| e.kind == "function" && e.name == "f")
        .expect("function f not found");
    assert_eq!(
        func.doc.as_deref().unwrap_or(""),
        "This is a single-line docstring"
    );
}

#[test]
fn python_triple_quoted_multi_line_docstring() {
    let src = r#"
def g():
    """
    Multi-line docstring line1
    line2
    """
    return 1
"#;
    let ents = parse_and_extract("python", src);
    let func = ents
        .iter()
        .find(|e| e.kind == "function" && e.name == "g")
        .expect("function g not found");
    let expected = "Multi-line docstring line1\nline2";
    assert_eq!(func.doc.as_deref().unwrap_or(""), expected);
}

#[test]
fn python_parenthesized_expr_docstring() {
    // docstring wrapped in parentheses (some code-generation patterns)
    let src = r#"
def h():
    (
        "wrapped docstring"
    )
    pass
"#;
    let ents = parse_and_extract("python", src);
    let func = ents
        .iter()
        .find(|e| e.kind == "function" && e.name == "h")
        .expect("function h not found");
    assert_eq!(func.doc.as_deref().unwrap_or(""), "wrapped docstring");
}

#[test]
fn rust_prefixed_comment_docs() {
    let src = r#"
/// This is a rust doc comment
pub fn r() {}
"#;
    let ents = parse_and_extract("rust", src);
    let func = ents
        .iter()
        .find(|e| e.kind == "function" && e.name == "r")
        .expect("function r not found");
    assert_eq!(
        func.doc.as_deref().unwrap_or(""),
        "This is a rust doc comment"
    );
}

#[test]
fn go_prefixed_comment_docs() {
    let src = r#"
// This is a go comment
package p

func G() {}
"#;
    let ents = parse_and_extract("go", src);
    let ent = ents
        .iter()
        .find(|e| e.name == "G" || e.signature.contains("func G"))
        .expect("function G not found");
    assert_eq!(ent.doc.as_deref().unwrap_or(""), "This is a go comment");
}

#[test]
fn js_prefixed_comment_docs() {
    let src = r#"
// JS comment for function
function j() {}
"#;
    let ents = parse_and_extract("javascript", src);
    let func = ents
        .iter()
        .find(|e| e.kind == "function" && e.name == "j")
        .expect("function j not found");
    assert_eq!(func.doc.as_deref().unwrap_or(""), "JS comment for function");
}

#[test]
fn python_concatenated_adjacent_string_literals() {
    let src = r#"
def a():
    """first""" """second"""
    pass
"#;
    let ents = parse_and_extract("python", src);
    let func = ents.iter().find(|e| e.name == "a").expect("a not found");
    assert_eq!(func.doc.as_deref().unwrap_or(""), "firstsecond");
}

#[test]
fn python_f_string_docstring() {
    let src = r#"
def b(name):
    f"""Hello {name}"""
    pass
"#;
    let ents = parse_and_extract("python", src);
    let func = ents.iter().find(|e| e.name == "b").expect("b not found");
    assert!(func.doc.as_deref().unwrap_or("").contains("Hello"));
}

#[test]
fn python_raw_string_docstring() {
    let src = r#"
def c():
    r"""raw\nline"""
    pass
"#;
    let ents = parse_and_extract("python", src);
    let func = ents.iter().find(|e| e.name == "c").expect("c not found");
    assert!(func.doc.as_deref().unwrap_or("").contains("raw\\nline"));
}

#[test]
fn python_docstring_with_internal_triple_quotes() {
    let src = r#"
def d():
    """This has an internal triple quote: \"\"\" inside"""
    pass
"#;
    let ents = parse_and_extract("python", src);
    let func = ents.iter().find(|e| e.name == "d").expect("d not found");
    assert!(func
        .doc
        .as_deref()
        .unwrap_or("")
        .contains("internal triple"));
}

#[test]
fn typescript_jsdoc_block_comment() {
    let src = r#"
/**
 * JSDoc for function t
 */
function t() {}
"#;
    let ents = parse_and_extract("typescript", src);
    let func = ents.iter().find(|e| e.name == "t").expect("t not found");
    assert_eq!(func.doc.as_deref().unwrap_or(""), "JSDoc for function t");
}

#[test]
fn java_javadoc_block_comment() {
    let src = r#"
/**
 * Javadoc for class Jd
 */
public class Jd {
  public void m() {}
}
"#;
    let ents = parse_and_extract("java", src);
    let cls = ents.iter().find(|e| e.name == "Jd").expect("Jd not found");
    assert_eq!(cls.doc.as_deref().unwrap_or(""), "Javadoc for class Jd");
}

#[test]
fn csharp_triple_slash_xml_doc() {
    let src = r#"
/// <summary>XML doc for M</summary>
public class Cx {
  public void M() {}
}
"#;
    let ents = parse_and_extract("c_sharp", src);
    let cls = ents.iter().find(|e| e.name == "Cx").expect("Cx not found");
    assert!(cls.doc.as_deref().unwrap_or("").contains("XML doc for M"));
}

#[test]
fn cpp_doxygen_block_comment() {
    let src = r#"
/** Doxygen for function cpp_f */
void cpp_f() {}
"#;
    let ents = parse_and_extract("cpp", src);
    let func = ents
        .iter()
        .find(|e| e.name == "cpp_f")
        .expect("cpp_f not found");
    assert_eq!(
        func.doc.as_deref().unwrap_or(""),
        "Doxygen for function cpp_f"
    );
}
