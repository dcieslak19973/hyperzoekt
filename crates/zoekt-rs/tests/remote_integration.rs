// Copyright 2025 HyperZoekt Project
// Derived from sourcegraph/zoekt (https://github.com/sourcegraph/zoekt)
// Copyright 2016 Google Inc. All rights reserved.
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

// gated integration test: only run when ZOEKT_TEST_REMOTE is set
// This test clones and indexes a remote repository. It's intentionally
// opt-in because it requires network access and may be slower.

#[test]
fn remote_index_integration() -> anyhow::Result<()> {
    use std::env;
    use std::path::PathBuf;
    let url = "https://github.com/dcieslak19973/hyperzoekt";
    if env::var("ZOEKT_TEST_REMOTE").is_err() {
        eprintln!("skipping remote_integration (set ZOEKT_TEST_REMOTE=1 to run)");
        return Ok(());
    }
    let idx = zoekt_rs::IndexBuilder::new(PathBuf::from(url)).build()?;
    // basic sanity: there should be at least one document
    assert!(idx.doc_count() > 0, "expected indexed docs");
    Ok(())
}
