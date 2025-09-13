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
fn python_method_calls_resolve_to_method_in_same_class() {
    // Create temporary repo layout
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    create_dir_all(repo_root.join("pkg")).expect("mkdir");
    let py_file = repo_root.join("pkg").join("mod.py");
    let src = r#"
class C:
    def get(self):
        return 42

    def scrape(self):
        self.get()
"#;
    write(&py_file, src).expect("write py");

    // Build index
    let (svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build(repo_root).expect("build");
    // Find the two function entities and the call edge
    let mut get_id = None;
    let mut scrape_id = None;
    for e in &svc.entities {
        if e.name == "get" {
            get_id = Some(e.id);
        }
        if e.name == "scrape" {
            scrape_id = Some(e.id);
        }
    }
    assert!(get_id.is_some() && scrape_id.is_some(), "entities present");
    let g = get_id.unwrap();
    let s = scrape_id.unwrap();
    let calls = &svc.call_edges[s as usize];
    assert!(
        calls.contains(&g),
        "scrape should call get, got {:?}",
        calls
    );
}
