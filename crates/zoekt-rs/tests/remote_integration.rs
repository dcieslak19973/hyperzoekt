// gated integration test: only run when ZOEKT_TEST_REMOTE is set
// This test clones and indexes a remote repository. It's intentionally
// opt-in because it requires network access and may be slower.

#[test]
fn remote_index_integration() -> anyhow::Result<()> {
    use std::env;
    use std::path::PathBuf;
    let url = "https://github.com/dcieslak19973/hyperzoekt";
    if env::var("ZOEKT_TEST_REMOTE").is_err() {
        eprintln!("skipping remote_integration (set ZOEKT_TEST_REMOTE=1 to run)");
        return Ok(());
    }
    let idx = zoekt_rs::IndexBuilder::new(PathBuf::from(url)).build()?;
    // basic sanity: there should be at least one document
    assert!(idx.doc_count() > 0, "expected indexed docs");
    Ok(())
}
