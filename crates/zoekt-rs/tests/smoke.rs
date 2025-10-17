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

mod common;
use zoekt_rs::query::Query;

#[test]
fn builds_and_finds_literal() {
    let dir = common::new_repo();
    common::write_file(dir.path(), "a.txt", b"hello zoekt");

    let idx = common::build_index(dir.path()).unwrap();
    let searcher = zoekt_rs::Searcher::new(&idx);
    let results = searcher.search(&Query::Literal("zoekt".into()));

    assert_eq!(results.len(), 1);
    assert!(results[0].path.ends_with("a.txt"));
}
