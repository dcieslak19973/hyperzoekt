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
fn from_import_symbol_resolves_without_alias() {
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
from pkg.a import foo

def caller_from_symbol():
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
        if e.name == "caller_from_symbol" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(
        calls.contains(&foo_id.unwrap()),
        "from pkg.a import foo; foo() should resolve"
    );
}

#[test]
fn relative_import_in_package_resolves() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    create_dir_all(repo_root.join("pkg")).expect("mkdir");
    write(repo_root.join("pkg").join("__init__.py"), "").expect("init");
    let a_py = repo_root.join("pkg").join("a.py");
    let b_py = repo_root.join("pkg").join("b_relative.py");
    let src_a = r#"
def foo():
    return 1
"#;
    let src_b = r#"
from .a import foo

def caller_relative():
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
        if e.name == "caller_relative" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(
        calls.contains(&foo_id.unwrap()),
        "from .a import foo; foo() should resolve"
    );
}

#[test]
fn from_import_star_resolves_when_symbol_present() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    create_dir_all(repo_root.join("pkg")).expect("mkdir");
    write(repo_root.join("pkg").join("__init__.py"), "").expect("init");
    let a_py = repo_root.join("pkg").join("a.py");
    let b_py = repo_root.join("pkg").join("b_star.py");
    let src_a = r#"
def foo():
    return 1

__all__ = ['foo']
"#;
    let src_b = r#"
from pkg.a import *

def caller_star():
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
        if e.name == "caller_star" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some());
    let calls = &svc.call_edges[caller_id.unwrap() as usize];
    assert!(
        calls.contains(&foo_id.unwrap()),
        "from pkg.a import *; foo() should resolve when __all__ exposes foo"
    );
}
