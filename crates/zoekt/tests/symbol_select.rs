use anyhow::Result;
use std::fs;
use tempfile::tempdir;
use zoekt_rs::build_in_memory_index;
use zoekt_rs::QueryPlan;
use zoekt_rs::Searcher;

#[test]
fn select_symbol_filters_by_pattern_literal_and_regex() -> Result<()> {
    let dir = tempdir()?;
    fs::create_dir_all(dir.path())?;
    // write a Rust file with a couple of functions and a struct
    fs::write(
        dir.path().join("lib.rs"),
        r#"
        pub fn Foo() {}
        fn internal_helper() {}
        pub struct Bar {}
        "#,
    )?;
    // write a Python file with symbols
    fs::write(
        dir.path().join("script.py"),
        r#"
        def do_work():
            pass
        class Worker:
            def run(self):
                pass
        "#,
    )?;

    let idx = build_in_memory_index(dir.path())?;
    let s = Searcher::new(&idx);

    // literal symbol filter (case-insensitive by default)
    let plan = QueryPlan::parse("select=symbol Foo")?;
    let res = s.search_plan(&plan);
    // Should find Foo symbol from lib.rs
    assert!(res.iter().any(|r| r.symbol.as_deref() == Some("Foo")));

    // case-sensitive literal should not match 'foo' if case differs
    let plan_cs = QueryPlan::parse("select=symbol case:yes Foo")?;
    let res_cs = s.search_plan(&plan_cs);
    assert!(res_cs.iter().any(|r| r.symbol.as_deref() == Some("Foo")));

    // regex symbol filter: match symbols starting with 'do_' or 'run'
    let plan_re = QueryPlan::parse("select=symbol re:1 'do_.*'")?;
    let res_re = s.search_plan(&plan_re);
    assert!(res_re
        .iter()
        .any(|r| r.symbol.as_deref() == Some("do_work")));

    Ok(())
}
