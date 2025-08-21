use assert_cmd::Command;
use predicates::str::contains;
use predicates::Predicate;
use std::fs;
use std::path::PathBuf;

#[test]
fn index_fixture_repo_writes_jsonl() {
    let mut cmd = Command::cargo_bin("index_repo").unwrap();
    let fixture = PathBuf::from("tests/fixture_repo");
    let out = PathBuf::from("tests/fixture_out.jsonl");
    if out.exists() {
        let _ = fs::remove_file(&out);
    }
    cmd.arg(fixture).arg(&out);
    cmd.assert().success();
    let s = fs::read_to_string(&out).expect("read out");
    assert!(contains("hello").eval(&s));
    let _ = fs::remove_file(&out);
}
