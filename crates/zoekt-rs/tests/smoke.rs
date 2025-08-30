mod common;
use zoekt_rs::query::Query;

#[test]
fn builds_and_finds_literal() {
    let dir = common::new_repo();
    common::write_file(dir.path(), "a.txt", b"hello zoekt");

    let idx = common::build_index(dir.path()).unwrap();
    let searcher = zoekt_rs::Searcher::new(&idx);
    let results = searcher.search(&Query::Literal("zoekt".into()));

    assert_eq!(results.len(), 1);
    assert!(results[0].path.ends_with("a.txt"));
}
