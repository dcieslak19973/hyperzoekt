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

#[test]
fn usages_integration() {
    // use the example-treesitter-repo fixtures included in tests
    let root = std::path::PathBuf::from("tests/fixtures/example-treesitter-repo");
    let (svc, stats) = RepoIndexService::build(&root).unwrap();
    // Sanity: ensure the fixture repo was actually indexed
    assert!(
        stats.files_indexed > 0,
        "expected fixture files to be indexed"
    );
    assert!(
        stats.entities_indexed > 0,
        "expected fixture entities to be indexed"
    );

    // errors_01.rs defines `may_fail` and `caller` where `caller` calls `may_fail`.
    let ids = svc.symbol_ids_exact("may_fail");
    assert!(!ids.is_empty(), "may_fail should be found in fixtures");

    // Inspect the parsed `calls` strings on the `caller` entity directly. This
    // is a more conservative check than resolved callee edges and matches how
    // the tree-sitter extractor records call sites in fixtures.
    // Find the `caller` entity that came from a Rust source file
    let caller_entity = svc
        .entities
        .iter()
        .find(|e| e.name == "caller" && svc.files[e.file_id as usize].language == "rust")
        .expect("rust caller entity present");
    assert!(
        caller_entity.calls.iter().any(|c| c == "may_fail"),
        "caller should have may_fail in calls: {:?}",
        caller_entity.calls
    );
}
