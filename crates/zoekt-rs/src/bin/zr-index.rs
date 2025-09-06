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

use clap::Parser;
use std::result::Result as StdResult;
use zoekt_rs::index::IndexError;
use zoekt_rs::{IndexBuilder, ShardWriter};

#[derive(Parser, Debug)]
#[command(name = "zr-index", about = "Build a simple in-memory index (demo)")]
struct Args {
    /// Path to repository root
    repo: std::path::PathBuf,
    /// Optional output shard path (defaults to <repo>/.data/index.shard)
    #[arg(long)]
    out: Option<std::path::PathBuf>,
    /// Do not write the shard to disk; build index in-memory only
    #[arg(long)]
    no_write: bool,
    /// Maximum file size in bytes to index (skip larger files). Default: 1000000
    #[arg(long)]
    max_file_size: Option<usize>,
}

fn main() -> StdResult<(), IndexError> {
    let args = Args::parse();
    let max_file_size = args.max_file_size.unwrap_or(1_000_000usize);
    let idx = IndexBuilder::new(args.repo.clone())
        .max_file_size(max_file_size)
        .build()?;
    // Determine shard path but only create directories /write when not in --no-write mode.
    let shard = if let Some(o) = args.out.as_ref() {
        o.clone()
    } else {
        let dir = args.repo.join(".data");
        dir.join("index.shard")
    };

    if !args.no_write {
        if let Some(o) = args.out.as_ref() {
            // ensure parent directory exists when user provided a path
            if let Some(p) = o.parent() {
                std::fs::create_dir_all(p)?;
            }
        } else {
            let dir = args.repo.join(".data");
            std::fs::create_dir_all(&dir)?;
        }

        ShardWriter::new(&shard).write_from_index(&idx)?;
        println!(
            "wrote shard: {} ({} docs)",
            shard.display(),
            idx.doc_count()
        );
    } else {
        println!(
            "built index (no-write): {} docs, shard path: {}",
            idx.doc_count(),
            shard.display()
        );
    }
    Ok(())
}
