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

use std::fs::{self, File};
use std::io::Write;
use tempfile::tempdir;
use zoekt_rs::index::IndexBuilder;
use zoekt_rs::query::QueryPlan;

#[test]
fn test_default_unions_path_and_content() -> anyhow::Result<()> {
    let td = tempdir()?;
    let repo = td.path().join("repo");
    fs::create_dir_all(&repo)?;

    // file1: name contains 'needle' but content does not
    let f1 = repo.join("needle_name.txt");
    let mut fh = File::create(&f1)?;
    writeln!(fh, "no matching content here")?;

    // file2: name does not contain 'needle' but content does
    let f2 = repo.join("other.txt");
    let mut fh2 = File::create(&f2)?;
    writeln!(fh2, "this content contains needle inside the text")?;

    let idx = IndexBuilder::new(repo.clone()).build()?;
    let s = zoekt_rs::query::Searcher::new(&idx);

    // default (neither path-only nor content-only) should match both
    let plan = QueryPlan {
        pattern: Some("needle".to_string()),
        ..Default::default()
    };
    let res = s.search_plan(&plan);
    let paths: Vec<String> = res.into_iter().map(|r| r.path).collect();
    assert!(
        paths.iter().any(|p| p.ends_with("needle_name.txt")),
        "expected path match"
    );
    assert!(
        paths.iter().any(|p| p.ends_with("other.txt")),
        "expected content match"
    );
    assert_eq!(
        paths.len(),
        2,
        "expected exactly two matches (union of path+content)"
    );

    Ok(())
}
