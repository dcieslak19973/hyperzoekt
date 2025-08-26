use sha2::Digest;
use surrealdb::engine::local::Mem;
use surrealdb::Surreal;

#[tokio::test]
async fn surreal_initial_batch_inserts_entities() -> Result<(), Box<dyn std::error::Error>> {
    // Start an embedded Mem surreal instance
    let db = Surreal::new::<Mem>(()).await?;
    db.use_ns("test").use_db("test").await?;

    // Build index service using library API, target the example fixture
    let root = std::path::PathBuf::from("tests/fixtures/example-treesitter-repo");
    let mut opts_builder = hyperzoekt::repo_index::indexer::RepoIndexOptions::builder();
    opts_builder = opts_builder.root(&root).output_null();
    let (svc, _stats) =
        hyperzoekt::repo_index::RepoIndexService::build_with_options(opts_builder.build())?;

    // Build payloads similar to binary
    let mut payloads = Vec::new();
    for ent in svc.entities.iter() {
        // minimal payload for test: stable_id + name
        let file = &svc.files[ent.file_id as usize];
        let key = format!(
            "local|repo|branch|commit|{}|{}|{}",
            file.path, ent.name, ent.signature
        );
        let mut hasher = sha2::Sha256::new();
        hasher.update(key.as_bytes());
        let stable_id = format!("{:x}", hasher.finalize());
        let v = serde_json::json!({"stable_id": stable_id, "name": ent.name, "file": file.path});
        payloads.push(v);
    }

    // Insert all records. Use parameterized CREATE to avoid SurrealQL parsing issues
    for v in payloads.into_iter() {
        let q = "CREATE entity CONTENT $e";
        db.query(q).bind(("e", v)).await?;
    }

    // Verify approximate count (inspect debug output for a 'count' field)
    let res = db.query("SELECT count() FROM entity").await?;
    let out_debug = format!("{:?}", res);
    assert!(out_debug.contains("count"));
    Ok(())
}
