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
    out: Option<PathBuf>,

    /// Write output incrementally as files are processed
    #[arg(long)]
    incremental: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let out_dir = PathBuf::from(".data");
    let _ = std::fs::create_dir_all(&out_dir);
    // canonical output filename when the user doesn't provide one
    let default_out = out_dir.join("bin_integration_out.jsonl");

    // Index all detected languages by default

    if args.incremental {
        // Stream results by passing a writer to options
        let out_path = args.out.as_deref().unwrap_or(default_out.as_path());
        let mut file_writer = BufWriter::new(File::create(out_path)?);
        let mut opts_builder = repo_index::internal::RepoIndexOptions::builder();
        opts_builder = opts_builder.root(&args.root);
        let opts = opts_builder.output_writer(&mut file_writer).build();
        let (_svc, stats) = repo_index::service::RepoIndexService::build_with_options(opts)?;
        // flush just in case
        file_writer.flush()?;
        println!(
            "Indexed {} files, {} entities in {:.3}s",
            stats.files_indexed,
            stats.entities_indexed,
            stats.duration.as_secs_f64()
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
    let out_path = args.out.as_deref().unwrap_or(default_out.as_path());
    let mut writer = BufWriter::new(File::create(out_path)?);

    for ent in &svc.entities {
        let file = &svc.files[ent.file_id as usize];
        // Attach file-level imports for File pseudo-entities
        let mut imports: Vec<serde_json::Value> = Vec::new();
        let mut unresolved_imports: Vec<serde_json::Value> = Vec::new();
        if matches!(ent.kind, repo_index::internal::EntityKind::File) {
            // import_edges stores target entity ids (file pseudo-entity ids)
            if let Some(edge_list) = svc.import_edges.get(ent.id as usize) {
                let lines = svc.import_lines.get(ent.id as usize);
                for (i, &target_eid) in edge_list.iter().enumerate() {
                    if let Some(target_ent) = svc.entities.get(target_eid as usize) {
                        let target_file_idx = target_ent.file_id as usize;
                        if let Some(target_file) = svc.files.get(target_file_idx) {
                            let line_no = lines
                                .and_then(|l| l.get(i))
                                .cloned()
                                .unwrap_or(0)
                                .saturating_add(1);
                            imports.push(json!({"path": target_file.path, "line": line_no}));
                        }
                    }
                }
            }
            // unresolved imports are stored per file index as (module,line)
            if let Some(unres) = svc.unresolved_imports.get(ent.file_id as usize) {
                for (m, lineno) in unres {
                    unresolved_imports.push(json!({"module": m, "line": lineno.saturating_add(1)}));
                }
            }
        }

        // decide whether to emit start/end lines. For file pseudo-entities we only
        // emit numeric 1-based start/end if there are import lines or unresolved
        // imports recorded; otherwise emit null to avoid misleading 1/1 values.
        let (start_field, end_field) = if matches!(ent.kind, repo_index::internal::EntityKind::File)
        {
            let has_imports = !imports.is_empty();
            let has_unresolved = !unresolved_imports.is_empty();
            if has_imports || has_unresolved {
                (
                    serde_json::Value::from(ent.start_line.saturating_add(1)),
                    serde_json::Value::from(ent.end_line.saturating_add(1)),
                )
            } else {
                (serde_json::Value::Null, serde_json::Value::Null)
            }
        } else {
            (
                serde_json::Value::from(ent.start_line.saturating_add(1)),
                serde_json::Value::from(ent.end_line.saturating_add(1)),
            )
        };

        let obj = json!({
            "file": file.path,
            "language": file.language,
            "kind": ent.kind.as_str(),
            "name": ent.name,
            "parent": ent.parent,
            "signature": ent.signature,
            "start_line": start_field,
            "end_line": end_field,
            "calls": ent.calls,
            "doc": ent.doc,
            "rank": ent.rank,
            "imports": imports,
            "unresolved_imports": unresolved_imports,
        });
        writeln!(writer, "{}", obj)?;
    }

    writer.flush()?;
    let out_display = args.out.as_ref().unwrap_or(&default_out);
    println!(
        "Wrote {} entities to {}",
        svc.entities.len(),
        out_display.display()
    );
    println!(
        "Indexed {} files, {} entities in {:.3}s",
        stats.files_indexed,
        stats.entities_indexed,
        stats.duration.as_secs_f64()
    );
    Ok(())
}
