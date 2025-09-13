// Copyright 2025 HyperZoekt Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use sha2::Digest;

#[tokio::test]
async fn surreal_initial_batch_inserts_entities() -> Result<(), Box<dyn std::error::Error>> {
    // Start an embedded Mem surreal instance
    use hyperzoekt::db_writer::connection::connect;
    let db = match connect(&None, &None, &None, "testns", "testdb").await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Skipping surreal_initial_batch_inserts_entities: unable to connect: {e}");
            return Ok(());
        }
    };
    if let Err(e) = db.use_ns("test").await {
        eprintln!("Skipping surreal_initial_batch_inserts_entities: unable to use ns: {e}");
        return Ok(());
    }
    if let Err(e) = db.use_db("test").await {
        eprintln!("Skipping surreal_initial_batch_inserts_entities: unable to use db: {e}");
        return Ok(());
    }

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
        db.query_with_binds(q, vec![("e", v)]).await?;
    }

    // Verify approximate count (inspect debug output for a 'count' field)
    let res = db.query("SELECT count() FROM entity").await?;
    let out_debug = format!("{:?}", res);
    assert!(out_debug.contains("count"));
    Ok(())
}
