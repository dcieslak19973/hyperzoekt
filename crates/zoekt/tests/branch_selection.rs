use std::fs;
use std::process::Command;
use tempfile::tempdir;
use zoekt_rs::index::IndexBuilder;
use zoekt_rs::query::QueryPlan;

/// Create a small git repo with two branches and differing file contents, index both
/// branches via IndexBuilder::branches and verify branch-scoped queries.
#[test]
fn test_branch_selection_with_git_archive() -> anyhow::Result<()> {
    let td = tempdir()?;
    let repo = td.path().join("repo");
    fs::create_dir_all(&repo)?;

    // init git repo
    Command::new("git").arg("init").arg(&repo).status()?;
    // commit file on main
    let f = repo.join("file.txt");
    fs::write(&f, "content on main")?;
    Command::new("git")
        .arg("-C")
        .arg(&repo)
        .arg("add")
        .arg(".")
        .status()?;
    Command::new("git")
        .arg("-C")
        .arg(&repo)
        .arg("commit")
        .arg("-m")
        .arg("main")
        .status()?;
    // Robustly detect the repository's initial branch name. Try several fallbacks
    // to handle different git versions and environment defaults.
    // 1) preferred: rev-parse --abbrev-ref HEAD
    let out = Command::new("git")
        .arg("-C")
        .arg(&repo)
        .arg("rev-parse")
        .arg("--abbrev-ref")
        .arg("HEAD")
        .output()?;
    let mut initial_branch = String::from_utf8_lossy(&out.stdout).trim().to_string();
    // If rev-parse returned HEAD or empty, try symbolic-ref
    if initial_branch.is_empty() || initial_branch == "HEAD" {
        let out2 = Command::new("git")
            .arg("-C")
            .arg(&repo)
            .arg("symbolic-ref")
            .arg("--short")
            .arg("HEAD")
            .output()?;
        initial_branch = String::from_utf8_lossy(&out2.stdout).trim().to_string();
    }
    // If still empty, try reading user's configured default branch
    if initial_branch.is_empty() {
        let out3 = Command::new("git")
            .arg("-C")
            .arg(&repo)
            .arg("config")
            .arg("--get")
            .arg("init.defaultBranch")
            .output()?;
        initial_branch = String::from_utf8_lossy(&out3.stdout).trim().to_string();
    }
    // Final fallback
    if initial_branch.is_empty() {
        initial_branch = "main".to_string();
    }

    // create branch 'feature' and change file
    Command::new("git")
        .arg("-C")
        .arg(&repo)
        .arg("checkout")
        .arg("-b")
        .arg("feature")
        .status()?;
    fs::write(&f, "content on feature")?;
    Command::new("git")
        .arg("-C")
        .arg(&repo)
        .arg("commit")
        .arg("-am")
        .arg("feature")
        .status()?;

    // Now use IndexBuilder to index both branches
    let idx = IndexBuilder::new(repo.clone())
        .branches(vec![initial_branch.clone(), "feature".to_string()])
        .build()?;
    let s = zoekt_rs::query::Searcher::new(&idx);

    // query branch main should match 'content on main'
    let mut plan = QueryPlan::default();
    plan.pattern = Some("content on main".to_string());
    plan.branches = vec![initial_branch.clone()];
    let res = s.search_plan(&plan);
    assert_eq!(res.len(), 1);
    assert!(res[0].path.ends_with("file.txt"));

    // query branch feature should match 'content on feature'
    let mut plan2 = QueryPlan::default();
    plan2.pattern = Some("content on feature".to_string());
    plan2.branches = vec!["feature".to_string()];
    let res2 = s.search_plan(&plan2);
    assert_eq!(res2.len(), 1);
    assert!(res2[0].path.ends_with("file.txt"));

    Ok(())
}
