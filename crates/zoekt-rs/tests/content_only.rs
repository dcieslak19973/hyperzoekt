use std::fs::{self, File};
use std::io::Write;
use tempfile::tempdir;
use zoekt_rs::index::IndexBuilder;
use zoekt_rs::query::QueryPlan;

#[test]
fn test_path_only_vs_content_only() -> anyhow::Result<()> {
    let td = tempdir()?;
    let repo = td.path().join("repo");
    fs::create_dir_all(&repo)?;

    // file1: name contains 'needle' but content does not
    let f1 = repo.join("needle_name.txt");
    let mut fh = File::create(&f1)?;
    writeln!(fh, "this file has no match")?;

    // file2: name does not contain 'needle' but content does
    let f2 = repo.join("other.txt");
    let mut fh2 = File::create(&f2)?;
    writeln!(fh2, "this content contains needle inside the text")?;

    let idx = IndexBuilder::new(repo.clone()).build()?;
    let s = zoekt_rs::query::Searcher::new(&idx);

    // path-only should match file1 only
    let plan = QueryPlan {
        pattern: Some("needle".to_string()),
        path_only: true,
        content_only: false,
        ..Default::default()
    };
    let res = s.search_plan(&plan);
    assert_eq!(res.len(), 1);
    assert!(res.iter().any(|r| r.path.ends_with("needle_name.txt")));

    // content-only should match file2 only
    let plan2 = QueryPlan {
        pattern: Some("needle".to_string()),
        content_only: true,
        path_only: false,
        ..Default::default()
    };
    let res2 = s.search_plan(&plan2);
    assert_eq!(res2.len(), 1);
    assert!(res2.iter().any(|r| r.path.ends_with("other.txt")));

    // default (neither) should match both
    let plan3 = QueryPlan {
        pattern: Some("needle".to_string()),
        ..Default::default()
    };
    let res3 = s.search_plan(&plan3);
    assert_eq!(res3.len(), 2);

    Ok(())
}
