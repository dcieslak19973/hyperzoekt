use hyperzoekt::RepoIndexService;

// Ensure we can call the sync handler helper without launching stdio/HTTP.
#[test]
fn test_handle_call_tool_sync_search() {
    // build a service for the current repo (uses test fixtures/helpers already
    // present in the crate tests). We will reuse the existing RepoIndexService
    // constructor to build an in-memory index.
    let svc = RepoIndexService::build(std::path::Path::new("./")).expect("build index");

    // simple search query that should return results for common symbols like `main`.
    let params = Some({
        let mut m = serde_json::Map::new();
        m.insert(
            "q".to_string(),
            serde_json::Value::String("main".to_string()),
        );
        m
    });

    let res = hyperzoekt::mcp::handle_call_tool_sync(&svc, "search", params.as_ref())
        .expect("search response");
    // expect an object with `results` array
    assert!(res.get("results").is_some(), "results key present");
}
