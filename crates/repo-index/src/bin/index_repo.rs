use clap::Parser;
use repo_index::service::RepoIndexService;
use serde_json::json;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// Simple repo indexer that writes one JSON object per line (JSONL).
#[derive(Parser)]
struct Args {
    /// Root path of the repository to index
    root: PathBuf,

    /// Output JSONL file
    out: PathBuf,

    /// Write output incrementally as files are processed
    #[arg(long)]
    incremental: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Index all detected languages by default

    if args.incremental {
        // Stream results by passing a writer to options
        let mut file_writer = BufWriter::new(File::create(&args.out)?);
        let mut opts_builder = repo_index::internal::RepoIndexOptions::builder();
        opts_builder = opts_builder.root(&args.root);
        let opts = opts_builder.output_writer(&mut file_writer).build();
        let (_svc, stats) = repo_index::service::RepoIndexService::build_with_options(opts)?;
        // flush just in case
        file_writer.flush()?;
        println!(
            "Indexed {} files, {} entities",
            stats.files_indexed, stats.entities_indexed
        );
        return Ok(());
    }
    // Removed include_langs processing, indexing all detected languages by default

    // Non-incremental: build in-memory and then write
    let opts = repo_index::internal::RepoIndexOptions::builder()
        .root(&args.root)
        .output_null()
        .build();
    let (svc, stats) = RepoIndexService::build_with_options(opts)?;

    let mut writer = BufWriter::new(File::create(&args.out)?);

    for ent in &svc.entities {
        let file = &svc.files[ent.file_id as usize];
        let obj = json!({
            "file": file.path,
            "language": file.language,
            "kind": ent.kind.as_str(),
            "name": ent.name,
            "parent": ent.parent,
            "signature": ent.signature,
            "start_line": ent.start_line,
            "end_line": ent.end_line,
            "calls": ent.calls,
            "doc": ent.doc,
            "rank": ent.rank,
        });
        writeln!(writer, "{}", obj.to_string())?;
    }

    writer.flush()?;
    println!(
        "Wrote {} entities to {}",
        svc.entities.len(),
        args.out.display()
    );
    println!(
        "Indexed {} files, {} entities",
        stats.files_indexed, stats.entities_indexed
    );
    Ok(())
}
