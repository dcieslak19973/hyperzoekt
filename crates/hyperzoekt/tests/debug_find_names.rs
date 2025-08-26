use hyperzoekt::service::RepoIndexService;

#[test]
fn debug_find_may_fail_and_caller() {
    let root = std::path::PathBuf::from("tests/fixtures/example-treesitter-repo");
    let (svc, stats) = RepoIndexService::build(&root).expect("build index");
    eprintln!(
        "indexed files={}, entities={}",
        stats.files_indexed, stats.entities_indexed
    );

    for (i, e) in svc.entities.iter().enumerate() {
        if e.name == "may_fail" || e.name == "caller" {
            let fr = &svc.files[e.file_id as usize];
            eprintln!(
                "entity[{}] name={} lang={} file={} start={} end={} calls={:?}",
                i, e.name, fr.language, fr.path, e.start_line, e.end_line, e.calls
            );
        }
    }
}
