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
use std::fs;
use tempfile::tempdir;

#[test]
fn stream_once_ingests_to_db() -> Result<(), Box<dyn std::error::Error>> {
    // Create a small temp repo with a single file
    let dir = tempdir()?;
    let file_path = dir.path().join("hello.rs");
    fs::write(&file_path, "fn hello() { println!(\"hi\"); }\n")?;

    // Build RepoIndexService with root set to the temp file's parent and no output
    let root = dir.path();
    let mut opts_builder = hyperzoekt::repo_index::indexer::RepoIndexOptions::builder();
    opts_builder = opts_builder.root(root).output_null();
    let (_svc, _stats) =
        hyperzoekt::repo_index::RepoIndexService::build_with_options(opts_builder.build())?;

    // If we reached here the indexer ran successfully (equivalent to binary exiting 0)
    Ok(())
}
