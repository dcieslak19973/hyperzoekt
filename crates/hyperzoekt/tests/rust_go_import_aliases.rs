use std::fs::{create_dir_all, write};
use tempfile::TempDir;

#[test]
fn rust_use_alias_resolves() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo_rust");
    create_dir_all(repo_root.join("src")).expect("mkdir");
    let a_rs = repo_root.join("src").join("lib.rs");
    let b_rs = repo_root.join("src").join("app.rs");
    let a_src = r#"
pub fn foo() {}
pub fn bar() {}
"#;
    let b_src = r#"
use crate::lib::foo as foo_alias;

fn caller_alias() { foo_alias(); }
"#;
    write(&a_rs, a_src).expect("write a");
    write(&b_rs, b_src).expect("write b");

    let (svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build(repo_root).expect("build");
    let mut foo_id = None;
    let mut caller_id = None;
    for e in &svc.entities {
        if e.name == "foo" {
            foo_id = Some(e.id);
        }
        if e.name == "caller_alias" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(calls.contains(&foo_id.unwrap()));
}

#[test]
fn go_import_alias_resolves() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo_go");
    create_dir_all(repo_root.join("pkg")).expect("mkdir");
    let a_go = repo_root.join("pkg").join("a.go");
    let b_go = repo_root.join("main.go");
    let a_src = r#"
package pkg

func Foo() {}
"#;
    let b_src = r#"
package main

import alias "repo/pkg"

func main() { alias.Foo() }
"#;
    write(&a_go, a_src).expect("write a");
    write(&b_go, b_src).expect("write b");

    let (svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build(repo_root).expect("build");
    let mut foo_id = None;
    let mut caller_id = None;
    for e in &svc.entities {
        if e.name == "Foo" {
            foo_id = Some(e.id);
        }
        if e.name == "main" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(calls.contains(&foo_id.unwrap()));
}
