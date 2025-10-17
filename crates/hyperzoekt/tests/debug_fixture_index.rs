#[test]
fn debug_fixture_index_contents() {
    use hyperzoekt::RepoIndexService;
    use std::path::Path;
    let repo = Path::new("tests/fixtures/example-treesitter-repo");
    let (svc, stats) = RepoIndexService::build(repo).expect("build index");
    println!(
        "files_indexed={} entities_indexed={}",
        stats.files_indexed,
        svc.entities.len()
    );
    let mut cnt_new = 0usize;
    for e in svc.entities.iter() {
        println!("{}: {} (kind={})", e.id, e.name, e.kind.as_str());
        if e.name == "new" {
            cnt_new += 1;
        }
        for m in &e.methods {
            if m.name == "new" {
                println!("  has method new on entity {}", e.id);
            }
        }
    }
    println!("found 'new' occurrences: {}", cnt_new);
    // Inspect name_index for the key "new"
    match svc.name_index.get("new") {
        Some(ids) => println!("name_index['new'] -> {} ids", ids.len()),
        None => println!("name_index['new'] -> none"),
    }
}
