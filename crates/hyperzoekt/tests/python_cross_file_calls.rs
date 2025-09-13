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
fn cross_file_import_call_resolves() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    create_dir_all(repo_root.join("pkg")).expect("mkdir");
    let a_py = repo_root.join("pkg").join("a.py");
    let b_py = repo_root.join("pkg").join("b.py");
    let src_a = r#"
def foo():
    return 1
"#;
    let src_b = r#"
from pkg.a import foo

def caller():
    foo()
"#;
    write(&a_py, src_a).expect("write a");
    write(&b_py, src_b).expect("write b");

    let (svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build(repo_root).expect("build");
    let mut foo_id = None;
    let mut caller_id = None;
    for e in &svc.entities {
        if e.name == "foo" {
            // prefer the function defined in a.py (it will be present twice: file entity and function), pick function
            if e.kind.as_str() != "file" {
                foo_id = Some(e.id);
            }
        }
        if e.name == "caller" {
            caller_id = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && caller_id.is_some(), "entities present");
    let f = foo_id.unwrap();
    let c = caller_id.unwrap();
    let calls = &svc.call_edges[c as usize];
    assert!(
        calls.contains(&f),
        "caller should call foo defined in a.py, got {:?}",
        calls
    );
}
