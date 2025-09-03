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

use std::time::{Duration, Instant};

use tempfile::tempdir;

use zoekt_rs::IndexBuilder;
use zoekt_rs::{Query, Searcher};

#[test]
fn microbench_searcher_search() {
    // Build a modest index in a tempdir using current workspace files
    let td = tempdir().unwrap();
    let root = td.path().to_path_buf();
    let idx = IndexBuilder::new(root.clone()).build().expect("build");
    let searcher = Searcher::new(&idx);

    let queries = vec![
        Query::Literal("malloc".into()),
        Query::Literal("kmalloc".into()),
        Query::Literal("pthread_create".into()),
        Query::Literal("EXPORT_SYMBOL".into()),
        Query::Literal("nonexistent_token_abcdefg".into()),
        Query::And(
            Box::new(Query::Literal("malloc".into())),
            Box::new(Query::Literal("nonexistent_token_abcdefg".into())),
        ),
        Query::Regex("\\bint\\s+main\\b".into()),
        Query::Regex("\\bstatic\\s+inline\\b".into()),
        Query::Regex("\\(\\s*\\*\\s*\\w+\\s*\\)".into()),
    ];

    let iters = 100usize;
    for q in queries.iter() {
        // warmup
        for _ in 0..10 {
            let _ = searcher.search(q);
        }
        let mut times = Vec::new();
        for _ in 0..iters {
            let start = Instant::now();
            let _ = searcher.search(q);
            times.push(start.elapsed());
        }
        times.sort();
        let sum: Duration = times.iter().sum();
        let mean = sum / (times.len() as u32);
        let median = times[times.len() / 2];
        println!(
            "query={} mean_ms={} median_ms={} p95_ms={}",
            q,
            mean.as_millis(),
            median.as_millis(),
            times[(times.len() * 95) / 100].as_millis()
        );
    }
}
