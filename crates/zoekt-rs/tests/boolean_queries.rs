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

use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

use zoekt_rs::{IndexBuilder, Query, Searcher};

/// Helper: create files with given contents under a temp dir and return the index
fn make_index(files: &[(&str, &str)]) -> zoekt_rs::InMemoryIndex {
    let dir = tempdir().expect("tmpdir");
    for (name, contents) in files {
        let p = dir.path().join(name);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut f = File::create(&p).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
    }
    IndexBuilder::new(dir.path().to_path_buf())
        .max_file_size(10_000_000)
        .build()
        .unwrap()
}

#[test]
fn boolean_query_happy_paths() {
    let idx = make_index(&[
        ("a.txt", "apple banana"),
        ("b.txt", "banana cherry"),
        ("c.txt", "cherry date"),
    ]);

    let s = Searcher::new(&idx);

    // Literal
    let res = s.search(&Query::Literal("banana".to_string()));
    assert_eq!(res.len(), 2);

    // AND: banana AND cherry -> only b.txt
    let res = s.search(&Query::And(
        Box::new(Query::Literal("banana".to_string())),
        Box::new(Query::Literal("cherry".to_string())),
    ));
    assert_eq!(res.len(), 1);

    // OR: apple OR date -> a.txt and c.txt
    let res = s.search(&Query::Or(
        Box::new(Query::Literal("apple".to_string())),
        Box::new(Query::Literal("date".to_string())),
    ));
    assert_eq!(res.len(), 2);
}

#[test]
fn boolean_query_empty_and_not() {
    // Files with unrelated content
    let idx = make_index(&[("x.txt", "foo"), ("y.txt", "bar")]);
    let s = Searcher::new(&idx);

    // AND with an absent term -> empty
    let res = s.search(&Query::And(
        Box::new(Query::Literal("foo".to_string())),
        Box::new(Query::Literal("missing".to_string())),
    ));
    assert!(res.is_empty());

    // NOT: return docs that do NOT contain 'foo' -> only y.txt
    let res = s.search(&Query::Not(Box::new(Query::Literal("foo".to_string()))));
    assert_eq!(res.len(), 1);
}

#[test]
fn boolean_query_nested_not() {
    let idx = make_index(&[
        ("a.txt", "one two three"),
        ("b.txt", "two three"),
        ("c.txt", "three"),
    ]);
    let s = Searcher::new(&idx);

    // NOT (two) -> docs that don't have 'two' -> only a.txt? actually a,b,c: a and b and c contain two? a contains two yes so only c
    let res = s.search(&Query::Not(Box::new(Query::Literal("two".to_string()))));
    assert_eq!(res.len(), 1);

    // Nested NOT: NOT (NOT three) -> should be same as 'three' (all docs that contain three)
    let res = s.search(&Query::Not(Box::new(Query::Not(Box::new(Query::Literal(
        "three".to_string(),
    ))))));
    // every document contains 'three' in this fixture -> 3
    assert_eq!(res.len(), 3);
}
