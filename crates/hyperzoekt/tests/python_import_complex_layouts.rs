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
// limitations under the License.

use std::fs::{create_dir_all, write};
use tempfile::TempDir;

#[test]
fn multi_level_relative_imports_resolve() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    // pkg/inner/sub where sub imports from ..a
    create_dir_all(repo_root.join("pkg").join("inner").join("sub")).expect("mkdir");
    write(repo_root.join("pkg").join("__init__.py"), "").expect("init");
    write(repo_root.join("pkg").join("inner").join("__init__.py"), "").expect("init");
    write(
        repo_root
            .join("pkg")
            .join("inner")
            .join("sub")
            .join("__init__.py"),
        "",
    )
    .expect("init");

    let a_py = repo_root.join("pkg").join("inner").join("a.py");
    let b_py = repo_root
        .join("pkg")
        .join("inner")
        .join("sub")
        .join("b_rel2.py");
    let src_a = r#"
def foo():
    return 1
"#;
    let src_b = r#"
from ..a import foo

def caller_multi_relative():
    foo()
"#;
    write(&a_py, src_a).expect("write a");
    write(&b_py, src_b).expect("write b");

    let (svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build(repo_root).expect("build");
    let mut foo_id = None;
    let mut caller_id = None;
    for e in &svc.entities {
        if e.name == "foo" && e.kind.as_str() != "file" {
            foo_id = Some(e.id);
        }
        if e.name == "caller_multi_relative" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(
        calls.contains(&foo_id.unwrap()),
        "multi-level relative import should resolve"
    );
}

#[test]
fn package_aliasing_with_nested_layout_resolves() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    // outer/lib/a.py and app that imports as alias
    create_dir_all(repo_root.join("outer").join("lib")).expect("mkdir");
    write(repo_root.join("outer").join("__init__.py"), "").expect("init");
    write(repo_root.join("outer").join("lib").join("__init__.py"), "").expect("init");

    let a_py = repo_root.join("outer").join("lib").join("a.py");
    let app_py = repo_root.join("app_alias.py");
    let src_a = r#"
def foo():
    return 1
"#;
    let src_app = r#"
import outer.lib.a as aliased
import outer as o

def caller_alias_complex():
    aliased.foo()

def caller_package_alias():
    o.lib.a.foo()
"#;

    write(&a_py, src_a).expect("write a");
    write(&app_py, src_app).expect("write app");

    let (svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build(repo_root).expect("build");
    let mut foo_id = None;
    let mut caller_alias_id = None;
    let mut caller_pkg_alias_id = None;
    for e in &svc.entities {
        if e.name == "foo" && e.kind.as_str() != "file" {
            foo_id = Some(e.id);
        }
        if e.name == "caller_alias_complex" {
            caller_alias_id = Some(e.id);
        }
        if e.name == "caller_package_alias" {
            caller_pkg_alias_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_alias_id.is_some() && caller_pkg_alias_id.is_some());
    let calls_alias = &svc.call_edges[caller_alias_id.unwrap() as usize];
    let calls_pkg = &svc.call_edges[caller_pkg_alias_id.unwrap() as usize];
    assert!(
        calls_alias.contains(&foo_id.unwrap()),
        "aliased import should resolve"
    );
    assert!(
        calls_pkg.contains(&foo_id.unwrap()),
        "package alias access should resolve"
    );
}

#[test]
fn mixed_absolute_and_relative_imports_in_same_file_resolve() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    // pkg/inner with b.py and a.py where subfile imports both absolute and relative
    create_dir_all(repo_root.join("pkg").join("inner")).expect("mkdir");
    write(repo_root.join("pkg").join("__init__.py"), "").expect("init");
    write(repo_root.join("pkg").join("inner").join("__init__.py"), "").expect("init");

    let a_py = repo_root.join("pkg").join("a.py");
    let b_py = repo_root.join("pkg").join("inner").join("b.py");
    let c_py = repo_root.join("pkg").join("inner").join("c_mixed.py");
    let src_a = r#"
def foo():
    return 1
"#;
    let src_b = r#"
def bar():
    return 2
"#;
    let src_c = r#"
import pkg.a as x
from .b import bar

def caller_mixed():
    x.foo()
    bar()
"#;
    write(&a_py, src_a).expect("write a");
    write(&b_py, src_b).expect("write b");
    write(&c_py, src_c).expect("write c");

    let (svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build(repo_root).expect("build");
    let mut foo_id = None;
    let mut bar_id = None;
    let mut caller_id = None;
    for e in &svc.entities {
        if e.name == "foo" && e.kind.as_str() != "file" {
            foo_id = Some(e.id);
        }
        if e.name == "bar" && e.kind.as_str() != "file" {
            bar_id = Some(e.id);
        }
        if e.name == "caller_mixed" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && bar_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(
        calls.contains(&foo_id.unwrap()),
        "mixed absolute import should resolve"
    );
    assert!(
        calls.contains(&bar_id.unwrap()),
        "mixed relative import should resolve"
    );
}

#[test]
fn aliased_star_chain_resolves() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    create_dir_all(repo_root.join("pkg")).expect("mkdir");
    write(repo_root.join("pkg").join("__init__.py"), "").expect("init");
    let a_py = repo_root.join("pkg").join("a.py");
    let b_py = repo_root.join("pkg").join("b_alias_star.py");
    let src_a = r#"
def foo():
    return 1

__all__ = ['foo']
"#;
    let src_b = r#"
import pkg.a as mod_alias
from mod_alias import *

def caller_alias_star():
    foo()
"#;
    write(&a_py, src_a).expect("write a");
    write(&b_py, src_b).expect("write b");

    let (svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build(repo_root).expect("build");
    let mut foo_id = None;
    let mut caller_id = None;
    for e in &svc.entities {
        if e.name == "foo" && e.kind.as_str() != "file" {
            foo_id = Some(e.id);
        }
        if e.name == "caller_alias_star" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(
        calls.contains(&foo_id.unwrap()),
        "aliased-star chain should resolve"
    );
}

#[test]
fn conditional_import_true_resolves() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    create_dir_all(repo_root.join("pkg")).expect("mkdir");
    write(repo_root.join("pkg").join("__init__.py"), "").expect("init");
    let a_py = repo_root.join("pkg").join("a.py");
    let b_py = repo_root.join("pkg").join("b_cond.py");
    let src_a = r#"
def foo():
    return 1
"#;
    let src_b = r#"
if True:
    from pkg.a import foo as foo_cond

def caller_conditional():
    foo_cond()
"#;
    write(&a_py, src_a).expect("write a");
    write(&b_py, src_b).expect("write b");

    let (svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build(repo_root).expect("build");
    let mut foo_id = None;
    let mut caller_id = None;
    for e in &svc.entities {
        if e.name == "foo" && e.kind.as_str() != "file" {
            foo_id = Some(e.id);
        }
        if e.name == "caller_conditional" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(
        calls.contains(&foo_id.unwrap()),
        "conditional import (True) should resolve"
    );
}
