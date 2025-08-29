use zoekt_rs::{
    build_in_memory_index,
    query::{Query, Searcher},
};

#[test]
fn builds_and_finds_literal() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.txt"), b"hello zoekt").unwrap();

    let idx = build_in_memory_index(dir.path()).unwrap();
    let searcher = Searcher::new(&idx);
    let results = searcher.search(&Query::Literal("zoekt".into()));

    assert_eq!(results.len(), 1);
    assert!(results[0].path.ends_with("a.txt"));
}
