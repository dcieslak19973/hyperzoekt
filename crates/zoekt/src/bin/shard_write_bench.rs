use std::time::Instant;
use zoekt_rs::{build_in_memory_index, ShardWriter};

fn main() -> anyhow::Result<()> {
    // Small synthetic repo: many small files to stress writer IO and postings assembly.
    let dir = tempfile::tempdir()?;
    let repo = dir.path();
    // Larger synthetic repo to expose IO-bound behavior
    let file_count = 4000usize;
    let lines_per_file = 200usize;
    eprintln!("creating {} files in {}", file_count, repo.display());
    for i in 0..file_count {
        let mut s = String::new();
        for j in 0..lines_per_file {
            // include a mix of words and a symbol-like function name
            s.push_str(&format!("// file {} line {}\n", i, j));
            s.push_str(&format!("fn sym_{:04}_{}() {{}}\n", i, j));
            s.push_str("let x = 42;\n");
        }
        std::fs::write(repo.join(format!("file_{:04}.rs", i)), s)?;
    }

    eprintln!("building in-memory index...");
    let t0 = Instant::now();
    // crate exports build_in_memory_index
    let idx = build_in_memory_index(repo)?;
    let t1 = Instant::now();
    eprintln!("index build took {} ms", (t1 - t0).as_millis());

    let shard_path = repo.join("index.shard");
    eprintln!("writing shard to {}", shard_path.display());
    let sw = ShardWriter::new(&shard_path);
    let t2 = Instant::now();
    sw.write_from_index(&idx)?;
    let t3 = Instant::now();
    eprintln!("shard write took {} ms", (t3 - t2).as_millis());

    // Print shard file size
    if let Ok(meta) = std::fs::metadata(&shard_path) {
        eprintln!("shard size bytes: {}", meta.len());
    }

    Ok(())
}
