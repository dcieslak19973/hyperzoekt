use regex::Regex;
use zoekt_rs::test_helpers as th;
use zoekt_rs::types::DocumentMeta;
use zoekt_rs::QueryPlan;

#[test]
fn repo_matches_substring_glob_regex_and_branch() {
    let mut plan = QueryPlan::default();
    plan.repos.push("owner/repo".to_string());
    assert!(th::repo_matches_wrapper(
        "owner/repo",
        &vec!["HEAD".to_string()],
        &plan
    ));

    plan.repos.clear();
    plan.repo_globs.push("owner/*".to_string());
    assert!(th::repo_matches_wrapper(
        "owner/repo",
        &vec!["HEAD".to_string()],
        &plan
    ));

    plan.repo_globs.clear();
    plan.repo_regexes.push(Regex::new("^owner/.*$").unwrap());
    assert!(th::repo_matches_wrapper(
        "owner/repo",
        &vec!["HEAD".to_string()],
        &plan
    ));

    plan.repo_regexes.clear();
    plan.branches.push("feature".to_string());
    // repo has only HEAD
    assert!(!th::repo_matches_wrapper(
        "owner/repo",
        &vec!["HEAD".to_string()],
        &plan
    ));
    // when branches include feature it's ok
    assert!(th::repo_matches_wrapper(
        "owner/repo",
        &vec!["HEAD".to_string(), "feature".to_string()],
        &plan
    ));
}

#[test]
fn build_path_docs_detects_paths_case_insensitive() {
    let docs = vec![
        DocumentMeta {
            path: std::path::PathBuf::from("src/lib.rs"),
            lang: None,
            size: 1,
            branches: vec!["HEAD".to_string()],
            symbols: vec![],
        },
        DocumentMeta {
            path: std::path::PathBuf::from("README.md"),
            lang: None,
            size: 1,
            branches: vec!["HEAD".to_string()],
            symbols: vec![],
        },
    ];
    let res = th::build_path_docs_wrapper(&docs, "lib.rs", false);
    assert_eq!(res, vec![0u32]);
}
