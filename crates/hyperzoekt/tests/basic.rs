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

use hyperzoekt::repo_index::RepoIndexService;
use std::fs;
use tempfile::tempdir;

#[test]
fn smoke_index_and_search() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("foo.rs");
    fs::write(&file, "pub fn test_fn() { let x = 1; }\nstruct Foo { }\n").unwrap();

    let (svc, stats) = RepoIndexService::build(dir.path()).unwrap();
    assert!(stats.files_indexed >= 1);

    let results = svc.search("test_fn", 10);
    assert!(results.iter().any(|r| r.name == "test_fn"));
}

#[test]
fn fixture_index_and_search() {
    let repo = std::path::Path::new("tests/fixtures/example-treesitter-repo");
    let (svc, stats) = RepoIndexService::build(repo).unwrap();
    assert!(stats.files_indexed >= 1);
    let expect = vec![
        "new",
        "distance_to",
        "heading_to",
        "distance_and_heading_to",
    ];
    for e in expect {
        let results = svc.search(e, 10);
        assert!(results.iter().any(|r| r.name == e), "missing {}", e);
    }
}
