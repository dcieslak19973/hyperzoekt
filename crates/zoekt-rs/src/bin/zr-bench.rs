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

use anyhow::Result;
use clap::Parser;
use serde_json::json;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use zoekt_rs::{IndexBuilder, ShardWriter};

fn get_max_rss_kb() -> Option<u64> {
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if line.starts_with("VmHWM:") || line.starts_with("VmRSS:") {
                let parts: Vec<_> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<u64>() {
                        return Some(kb);
                    }
                }
            }
        }
    }
    None
}
#[derive(Parser)]
struct Opts {
    /// Path to repo root to index
    path: PathBuf,
    /// Write a shard file after indexing to <path>/.data/index.shard
    #[clap(long)]
    write_shard: bool,
    /// Optional description to include in output JSON
    #[clap(long)]
    description: Option<String>,
}

fn main() -> Result<()> {
    let opts = Opts::parse();
    let repo = opts.path.clone();

    let before_rss = get_max_rss_kb();
    let start = Instant::now();
    let idx = IndexBuilder::new(repo.clone())
        .build()
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let dur = start.elapsed();
    let after_rss = get_max_rss_kb();

    let scanned_bytes = idx.total_scanned_bytes();

    println!("docs: {}", idx.doc_count());
    println!("scanned_bytes: {}", scanned_bytes);
    println!("elapsed_ms: {}", dur.as_millis());
    if let Some(b) = before_rss {
        if let Some(a) = after_rss {
            println!("rss_kb_before: {} rss_kb_after: {}", b, a);
        }
    }

    // Optionally write shard and stat its size
    let mut index_path: Option<String> = None;
    let mut index_size: Option<u64> = None;
    if opts.write_shard {
        let dir = repo.join(".data");
        std::fs::create_dir_all(&dir)?;
        let shard = dir.join("index.shard");
        ShardWriter::new(&shard).write_from_index(&idx)?;
        if let Ok(meta) = std::fs::metadata(&shard) {
            index_size = Some(meta.len());
            index_path = Some(shard.display().to_string());
            println!("wrote shard: {} ({} bytes)", shard.display(), meta.len());
        }
    }

    // Emit JSON summary to .bench_results/index-bench-<timestamp>.json
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let out = json!({
        "timestamp_ms": now,
        "path": repo.display().to_string(),
    "description": opts.description,
        "docs": idx.doc_count(),
        "scanned_bytes": scanned_bytes,
        "elapsed_ms": dur.as_millis(),
    // include an empty results array so compare tools that expect query-bench style
    // JSON won't panic when index-only runs are compared
    "results": [],
        "rss_kb_before": before_rss,
        "rss_kb_after": after_rss,
        "wrote_shard": opts.write_shard,
        "index_path": index_path,
        "index_size_bytes": index_size,
    });
    std::fs::create_dir_all(".bench_results")?;
    let fname = format!(".bench_results/index-bench-{}.json", now);
    let mut f = File::create(&fname)?;
    let s = serde_json::to_string_pretty(&out)?;
    f.write_all(s.as_bytes())?;
    println!("wrote {}", fname);

    Ok(())
}
