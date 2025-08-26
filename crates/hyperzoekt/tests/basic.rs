use hyperzoekt::repo_index::RepoIndexService;
use std::fs;
use tempfile::tempdir;

#[test]
fn smoke_index_and_search() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("foo.rs");
    fs::write(&file, "pub fn test_fn() { let x = 1; }\nstruct Foo { }\n").unwrap();

    let (svc, stats) = RepoIndexService::build(dir.path()).unwrap();
    assert!(stats.files_indexed >= 1);

    let results = svc.search("test_fn", 10);
    assert!(results.iter().any(|r| r.name == "test_fn"));
}

#[test]
fn fixture_index_and_search() {
    let repo = std::path::Path::new("tests/fixtures/example-treesitter-repo");
    let (svc, stats) = RepoIndexService::build(repo).unwrap();
    assert!(stats.files_indexed >= 1);
    let expect = vec![
        "new",
        "distance_to",
        "heading_to",
        "distance_and_heading_to",
    ];
    for e in expect {
        let results = svc.search(e, 10);
        assert!(results.iter().any(|r| r.name == e), "missing {}", e);
    }
}
