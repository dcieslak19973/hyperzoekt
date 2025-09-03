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
use hyperzoekt::RepoIndexService;

// Ensure we can call the sync handler helper without launching stdio/HTTP.
#[test]
fn test_handle_call_tool_sync_search() {
    // build a service for the current repo (uses test fixtures/helpers already
    // present in the crate tests). We will reuse the existing RepoIndexService
    // constructor to build an in-memory index.
    let (svc, _stats) = RepoIndexService::build(std::path::Path::new("./")).expect("build index");

    // simple search query that should return results for common symbols like `main`.
    let params = Some({
        let mut m = serde_json::Map::new();
        m.insert(
            "q".to_string(),
            serde_json::Value::String("main".to_string()),
        );
        m
    });

    let res = hyperzoekt::mcp::handle_call_tool_sync(&svc, "search", params.as_ref())
        .expect("search response");
    // expect an object with `results` array
    assert!(res.get("results").is_some(), "results key present");
}
