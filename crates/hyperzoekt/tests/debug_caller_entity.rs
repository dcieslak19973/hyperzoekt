use hyperzoekt::repo_index::RepoIndexService;

#[test]
fn debug_print_caller_entity() {
    let root = std::path::PathBuf::from("tests/fixtures/example-treesitter-repo");
    let (svc, stats) = RepoIndexService::build(&root).expect("build index");
    eprintln!(
        "indexed files={}, entities={}",
        stats.files_indexed, stats.entities_indexed
    );

    // find the `caller` entity
    let maybe = svc
        .entities
        .iter()
        .enumerate()
        .find(|(_i, e)| e.name == "caller");
    let (idx, ent) = match maybe {
        Some(x) => x,
        None => panic!(
            "caller entity not found; available names: {:?}",
            svc.entities
                .iter()
                .map(|e| e.name.clone())
                .take(50)
                .collect::<Vec<_>>()
        ),
    };

    let file_rec = &svc.files[ent.file_id as usize];
    eprintln!(
        "found caller entity idx={} file={} start_line={} end_line={} signature=<{:?}>",
        idx, file_rec.path, ent.start_line, ent.end_line, ent.signature
    );
    eprintln!("calls recorded: {:?}", ent.calls);

    // Print the source snippet for the function (0-based internal, show a few context lines)
    let src = std::fs::read_to_string(&file_rec.path).expect("read source file");
    let lines: Vec<&str> = src.lines().collect();
    let start = ent.start_line.saturating_sub(3) as usize;
    let end = std::cmp::min(lines.len(), (ent.end_line + 3) as usize);
    eprintln!("--- source snippet (lines {}..{}): ---", start + 1, end);
    for (i, l) in lines[start..end].iter().enumerate() {
        eprintln!("{:4} | {}", start + i + 1, l);
    }
}
