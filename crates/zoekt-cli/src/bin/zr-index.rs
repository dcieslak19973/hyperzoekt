use anyhow::Result;
use clap::Parser;
use zoekt_rs::{IndexBuilder, ShardWriter};

#[derive(Parser, Debug)]
#[command(name = "zr-index", about = "Build a simple in-memory index (demo)")]
struct Args {
    /// Path to repository root
    repo: std::path::PathBuf,
    /// Optional output shard path (defaults to <repo>/.data/index.shard)
    #[arg(long)]
    out: Option<std::path::PathBuf>,
    /// Maximum file size in bytes to index (skip larger files). Default: 1000000
    #[arg(long)]
    max_file_size: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let max_file_size = args.max_file_size.unwrap_or(1_000_000usize);
    let idx = IndexBuilder::new(args.repo.clone())
        .max_file_size(max_file_size)
        .build()?;
    let shard = if let Some(o) = args.out.as_ref() {
        // ensure parent directory exists when user provided a path
        if let Some(p) = o.parent() {
            std::fs::create_dir_all(p)?;
        }
        o.clone()
    } else {
        let dir = args.repo.join(".data");
        std::fs::create_dir_all(&dir)?;
        dir.join("index.shard")
    };
    ShardWriter::new(&shard).write_from_index(&idx)?;
    println!(
        "wrote shard: {} ({} docs)",
        shard.display(),
        idx.doc_count()
    );
    Ok(())
}
