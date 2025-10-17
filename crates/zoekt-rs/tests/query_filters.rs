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

use zoekt_rs::query::QueryPlan;

#[test]
fn path_only_and_case() {
    let dir = common::new_repo();
    common::write_file(dir.path(), "src/Hello.rs", b"fn main() {}\n");
    common::write_file(dir.path(), "README.md", b"hello world\n");
    let idx = common::build_index(dir.path()).unwrap();
    let s = zoekt_rs::Searcher::new(&idx);

    let plan = QueryPlan::parse("path:only hello").unwrap();
    let files: Vec<_> = s.search_plan(&plan).into_iter().map(|r| r.path).collect();
    assert!(files.iter().any(|p| p.ends_with("src/Hello.rs")));

    let plan = QueryPlan::parse("path:only case:yes hello").unwrap();
    let files: Vec<_> = s.search_plan(&plan).into_iter().map(|r| r.path).collect();
    assert!(files.iter().all(|p| !p.ends_with("src/Hello.rs")));
}

#[test]
fn lang_filter() {
    let dir = common::new_repo();
    common::write_file(dir.path(), "src/lib.rs", b"pub fn foo() {}\n");
    common::write_file(dir.path(), "script.py", b"print('x')\n");
    let idx = common::build_index(dir.path()).unwrap();
    let s = zoekt_rs::Searcher::new(&idx);

    let plan = QueryPlan::parse("lang:rust foo").unwrap();
    let files: Vec<_> = s.search_plan(&plan).into_iter().map(|r| r.path).collect();
    assert!(files.iter().any(|p| p.ends_with("src/lib.rs")));
    assert!(!files.iter().any(|p| p.ends_with("script.py")));
}

#[test]
fn select_repo() {
    let dir = common::new_repo();
    common::write_file(dir.path(), "a.txt", b"hello world\n");
    let idx = common::build_index(dir.path()).unwrap();
    let s = zoekt_rs::Searcher::new(&idx);
    let plan = QueryPlan::parse("select=repo hello").unwrap();
    let res = s.search_plan(&plan);
    assert_eq!(res.len(), 1);
}

#[test]
fn select_symbol_emits_symbols() {
    let dir = common::new_repo();
    common::write_file(dir.path(), "lib.rs", b"pub fn foo() {}\nfn bar() {}\n");
    let idx = common::build_index(dir.path()).unwrap();
    let s = zoekt_rs::Searcher::new(&idx);
    let plan = QueryPlan::parse("select=symbol").unwrap();
    let res = s.search_plan(&plan);
    // both symbols should be present
    let syms: Vec<_> = res.into_iter().filter_map(|r| r.symbol).collect();
    assert!(syms.contains(&"foo".to_string()));
    assert!(syms.contains(&"bar".to_string()));
}
