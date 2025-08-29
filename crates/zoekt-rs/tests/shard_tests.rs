use anyhow::Result;
use tempfile;

use zoekt_rs::build_in_memory_index;
use zoekt_rs::shard::{ShardReader, ShardSearcher, ShardWriter};

#[test]
fn write_read_search_roundtrip() -> Result<()> {
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("a.txt"), b"hello zoekt shard")?;
    let idx = build_in_memory_index(dir.path())?;
    let shard_path = dir.path().join("index.shard");
    ShardWriter::new(&shard_path).write_from_index(&idx)?;

    let reader = ShardReader::open(&shard_path)?;
    let searcher = ShardSearcher::new(&reader);
    let hits = searcher.search_literal_with_context("zoekt");
    assert!(hits
        .iter()
        .any(|m| m.path.ends_with("a.txt") && m.line >= 1));
    Ok(())
}

#[test]
fn corrupt_shard_metadata_returns_error() -> Result<()> {
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("a.txt"), b"hello zoekt shard")?;
    let idx = build_in_memory_index(dir.path())?;
    let shard_path = dir.path().join("index.shard");
    ShardWriter::new(&shard_path).write_from_index(&idx)?;

    // Read shard bytes and corrupt the repo_name bytes to invalid UTF-8
    let mut bytes = std::fs::read(&shard_path)?;
    // Find meta_off at header offset 28..36
    let meta_off = u64::from_le_bytes(bytes[28..36].try_into().unwrap()) as usize;
    // repo_name length is first u16 at meta_off
    let n1 = u16::from_le_bytes(bytes[meta_off..meta_off + 2].try_into().unwrap()) as usize;
    let name_start = meta_off + 2;
    if n1 > 0 {
        // Flip first byte to 0xFF to make invalid UTF-8
        bytes[name_start] = 0xFF;
    }
    let corrupt_path = dir.path().join("index-corrupt.shard");
    std::fs::write(&corrupt_path, &bytes)?;

    let res = ShardReader::open(&corrupt_path);
    assert!(res.is_err());
    let msg = res.err().unwrap().to_string();
    assert!(msg.contains("repo_name not valid UTF-8") || msg.contains("shard truncated"));
    Ok(())
}

#[test]
fn symbol_trigram_prefilter_roundtrip() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let content = r#"
        // top-level functions
        fn Foo_bar() {}
        fn Other() {}
        "#;
    std::fs::write(dir.path().join("a.rs"), content)?;
    let idx = build_in_memory_index(dir.path())?;
    let shard_path = dir.path().join("index.shard");
    ShardWriter::new(&shard_path).write_from_index(&idx)?;

    let reader = ShardReader::open(&shard_path)?;
    let searcher = ShardSearcher::new(&reader);
    let res = searcher.search_symbols_prefiltered(Some("Foo"), false, false);
    assert!(res.iter().any(|r| r.symbol.as_deref() == Some("Foo_bar")));
    Ok(())
}

#[test]
fn symbol_regex_case_sensitive_and_insensitive() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let content = r#"
        // top-level functions
        fn DoThing() {}
        fn dothing() {}
        "#;
    std::fs::write(dir.path().join("a.rs"), content)?;
    let idx = build_in_memory_index(dir.path())?;
    let shard_path = dir.path().join("index.shard");
    ShardWriter::new(&shard_path).write_from_index(&idx)?;

    let reader = ShardReader::open(&shard_path)?;
    let searcher = ShardSearcher::new(&reader);

    let res_cs = searcher.search_symbols_prefiltered(Some("^DoThing$"), true, true);
    assert!(res_cs
        .iter()
        .any(|r| r.symbol.as_deref() == Some("DoThing")));
    assert!(!res_cs
        .iter()
        .any(|r| r.symbol.as_deref() == Some("dothing")));

    let res_ci = searcher.search_symbols_prefiltered(Some("^dothing$"), true, false);
    assert!(res_ci
        .iter()
        .any(|r| r.symbol.as_deref() == Some("DoThing")));
    assert!(res_ci
        .iter()
        .any(|r| r.symbol.as_deref() == Some("dothing")));

    Ok(())
}

#[test]
fn symbol_unicode_and_multibyte_names() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let content = "fn caf\u{e9}() {}\nfn 漢字_func() {}\n";
    std::fs::write(dir.path().join("a.rs"), content)?;
    let idx = build_in_memory_index(dir.path())?;
    let shard_path = dir.path().join("index.shard");
    ShardWriter::new(&shard_path).write_from_index(&idx)?;

    let reader = ShardReader::open(&shard_path)?;
    let searcher = ShardSearcher::new(&reader);

    let res = searcher.search_symbols_prefiltered(Some("caf\u{e9}"), false, true);
    assert!(res.iter().any(|r| r.symbol.as_deref() == Some("caf\u{e9}")));

    let res2 = searcher.search_symbols_prefiltered(Some("漢字_func"), false, true);
    assert!(res2
        .iter()
        .any(|r| r.symbol.as_deref() == Some("漢字_func")));

    let res3 = searcher.search_symbols_prefiltered(Some("caf."), true, false);
    assert!(res3
        .iter()
        .any(|r| r.symbol.as_deref() == Some("caf\u{e9}")));

    Ok(())
}

#[test]
fn many_symbols_in_single_doc() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut content = String::new();
    let count = 200usize;
    for i in 0..count {
        content.push_str(&format!("fn sym_{:04}() {{}}\n", i));
    }
    std::fs::write(dir.path().join("big.rs"), content)?;
    let idx = build_in_memory_index(dir.path())?;
    let shard_path = dir.path().join("index.shard");
    ShardWriter::new(&shard_path).write_from_index(&idx)?;

    let reader = ShardReader::open(&shard_path)?;
    let searcher = ShardSearcher::new(&reader);

    let res = searcher.search_symbols_prefiltered(Some("sym_00"), false, false);
    assert!(res.len() >= 11, "expected many symbols, got {}", res.len());

    let res2 = searcher.search_symbols_prefiltered(Some("sym_0199"), false, false);
    assert!(res2.iter().any(|r| r.symbol.as_deref() == Some("sym_0199")));

    Ok(())
}

#[test]
fn symbol_postings_deduped_roundtrip() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let content1 = "fn Dup() {}\n";
    let content2 = "fn Dup() {}\n";
    std::fs::write(dir.path().join("a.rs"), content1)?;
    std::fs::write(dir.path().join("b.rs"), content2)?;
    let idx = build_in_memory_index(dir.path())?;
    let shard_path = dir.path().join("index.shard");
    ShardWriter::new(&shard_path).write_from_index(&idx)?;

    let reader = ShardReader::open(&shard_path)?;
    let sym_map = zoekt_rs::test_helpers::symbol_postings_map(&reader)?;
    for (_tri, docs) in sym_map.iter() {
        let mut sorted = docs.clone();
        sorted.sort_unstable();
        let mut dedup = sorted.clone();
        dedup.dedup();
        assert_eq!(sorted, dedup);
    }
    Ok(())
}
