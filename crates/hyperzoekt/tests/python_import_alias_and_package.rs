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
fn import_alias_and_attribute_calls_resolve() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    create_dir_all(repo_root.join("pkg")).expect("mkdir");
    // create package __init__ to make pkg a package
    write(repo_root.join("pkg").join("__init__.py"), "").expect("init");
    let a_py = repo_root.join("pkg").join("a.py");
    let b_py = repo_root.join("pkg").join("b.py");
    let src_a = r#"
def foo():
    return 1
"#;
    // Test alias import: import pkg.a as x; x.foo()
    let src_b = r#"
import pkg.a as x

def caller_alias():
    x.foo()
"#;
    write(&a_py, src_a).expect("write a");
    write(&b_py, src_b).expect("write b");

    let (svc, _stats) =
        hyperzoekt::repo_index::RepoIndexService::build(repo_root.clone()).expect("build");
    let mut foo_id = None;
    let mut caller_id = None;
    for e in &svc.entities {
        if e.name == "foo" && e.kind.as_str() != "file" {
            foo_id = Some(e.id);
        }
        if e.name == "caller_alias" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(
        calls.contains(&foo_id.unwrap()),
        "alias call should resolve"
    );
}

#[test]
fn package_attribute_call_resolves() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    create_dir_all(repo_root.join("pkg")).expect("mkdir");
    write(repo_root.join("pkg").join("__init__.py"), "").expect("init");
    let a_py = repo_root.join("pkg").join("a.py");
    let b_py = repo_root.join("pkg").join("b_attr.py");
    let src_a = r#"
def foo():
    return 1
"#;
    let src_b = r#"
import pkg

def caller_pkg():
    pkg.a.foo()
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
        if e.name == "caller_pkg" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(
        calls.contains(&foo_id.unwrap()),
        "pkg.a.foo() should resolve"
    );
}

#[test]
fn from_import_symbol_alias_resolves() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    create_dir_all(repo_root.join("pkg")).expect("mkdir");
    write(repo_root.join("pkg").join("__init__.py"), "").expect("init");
    let a_py = repo_root.join("pkg").join("a.py");
    let b_py = repo_root.join("pkg").join("b_from_symbol.py");
    let src_a = r#"
def foo():
    return 1
"#;
    let src_b = r#"
from pkg import a as x

def caller_from_symbol_alias():
    x.foo()
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
        if e.name == "caller_from_symbol_alias" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(
        calls.contains(&foo_id.unwrap()),
        "from pkg import a as x; x.foo() should resolve"
    );
}
