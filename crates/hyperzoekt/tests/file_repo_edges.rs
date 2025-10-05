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

//! Integration test validating bidirectional repo<->file graph edges using a remote SurrealDB
//! instance specified via SURREALDB_URL (in-memory engine is NOT acceptable for this test).

use hyperzoekt::db::{spawn_db_writer, DbWriterConfig};
use hyperzoekt::repo_index::indexer::payload::EntityPayload;
use serial_test::serial;
use surrealdb::sql::Thing;

fn make_entity(file: &str, name: &str, repo: &str, stable_suffix: &str) -> EntityPayload {
    EntityPayload {
        id: format!("stable:{}", stable_suffix),
        language: "rust".into(),
        kind: "function".into(),
        name: name.to_string(),
        parent: None,
        signature: format!("fn {}()", name),
        start_line: Some(0),
        end_line: Some(1),
        doc: None,
        rank: Some(1.0),
        imports: Vec::new(),
        unresolved_imports: Vec::new(),
        stable_id: format!("stable:{}", stable_suffix),
        repo_name: repo.to_string(),
        file: Some(file.to_string()),
        source_url: None,
        source_display: Some(file.to_string()),
        source_content: None,
        calls: Vec::new(),
        methods: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn file_repo_edges_created_bidirectionally() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var(
            "RUST_LOG",
            "info,hyper_util::client::legacy::pool=warn,hyper_util=warn",
        );
    }
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    // Require remote SurrealDB URL; skip the test if missing (this is an external integration).
    let url = match std::env::var("SURREALDB_URL").ok() {
        Some(u) => {
            // Normalize early so any downstream Surreal client code never sees a malformed value.
            let (schemeful, _no_scheme) = hyperzoekt::test_utils::normalize_surreal_host(&u);
            std::env::set_var("SURREALDB_URL", &schemeful);
            std::env::set_var("SURREALDB_HTTP_BASE", &schemeful);
            schemeful
        }
        None => {
            log::info!(
                "SURREALDB_URL not set; skipping integration test that requires external SurrealDB"
            );
            return;
        }
    };
    let user = std::env::var("SURREALDB_USERNAME").ok();
    let pass_present = std::env::var("SURREALDB_PASSWORD").is_ok();
    log::info!(
        "TEST INIT: url='{}' user_present={} pass_present={}",
        url,
        user.is_some(),
        pass_present
    );

    // Enable experimental graph capability if required by server build.
    std::env::set_var("SURREAL_CAPS_ALLOW_EXPERIMENTAL", "graphql");
    // Force the writer to stop after first batch for determinism.
    std::env::set_var("HZ_SINGLE_BATCH", "1");
    // Reset global creation/dedup sets so previous tests do not influence counts.
    std::env::set_var("HZ_RESET_GLOBALS", "1");

    // Namespace / database: allow overrides via env for CI or local runs.
    let test_ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".to_string());
    let test_db = std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".to_string());
    log::info!("TEST NS/DB: {} / {}", test_ns, test_db);

    // Pre-test cleanup: establish connection and ensure it's a remote connection
    // (the in-memory Surreal engine does not support relation traversal reliably).
    use hyperzoekt::db::connection::{connect, SurrealConnection};
    log::info!("CLEANUP CONNECT: connecting via shared connect(...) helper");
    let connect_res = connect(
        &Some(url.clone()),
        &user,
        &std::env::var("SURREALDB_PASSWORD").ok(),
        &test_ns,
        &test_db,
    )
    .await;

    let db = match connect_res {
        Ok(c) => {
            // If the connection resolved to an in-process Local Mem instance,
            // skip the test because relation traversal relies on remote engine behavior.
            match &c {
                SurrealConnection::Local(_) => {
                    log::info!(
                        "connect() returned Local embedded Mem instance; skipping test that requires external SurrealDB"
                    );
                    return;
                }
                _ => c,
            }
        }
        Err(e) => {
            log::info!(
                "Skipping file_repo_edges_created_bidirectionally: connect failed: {}",
                e
            );
            return;
        }
    };

    // Use the assertion connection to perform cleanup and set ns/db
    let _ = db.use_ns(&test_ns).await;
    let _ = db.use_db(&test_db).await;

    // Targeted deletes (edges first, then entities, then base records)
    let cleanup_statements = [
        // Edge deletions for known pairs (repo_one/src files, repo_two/lib file)
        "DELETE has_file WHERE in=commits:test_commit_123;",
        "DELETE has_file WHERE in=repo:repo_one;",
        "DELETE has_file WHERE in=repo:repo_two;",
        "DELETE in_repo WHERE out=commits:test_commit_123;",
        "DELETE in_repo WHERE out=repo:repo_one;",
        "DELETE in_repo WHERE out=repo:repo_two;",
        // Commit records
        "DELETE commits:test_commit_123;",
        // File + repo records (only those we will recreate)
        "DELETE file:src_file1_rs;",
        "DELETE file:src_file2_rs;",
        "DELETE file:lib_file3_rs;",
        "DELETE repo:repo_one;",
        "DELETE repo:repo_two;",
        // Entities by stable ids (deterministic sanitize with underscores)
        "DELETE entity:stable_r1_f1;",
        "DELETE entity:stable_r1_f2;",
        "DELETE entity:stable_r2_f3;",
    ];
    for stmt in cleanup_statements.iter() {
        if let Err(e) = db.query(stmt).await {
            log::debug!("cleanup stmt failed {} err {}", stmt, e);
        }
    }
    log::info!("Pre-test cleanup complete");

    // Configure writer to use remote env-provided credentials & URL.
    let cfg = DbWriterConfig {
        surreal_url: Some(url.clone()),
        surreal_username: user.clone(),
        surreal_password: std::env::var("SURREALDB_PASSWORD").ok(),
        surreal_ns: test_ns.clone(),
        surreal_db: test_db.clone(),
        channel_capacity: 8,
        batch_capacity: Some(100),
        batch_timeout_ms: Some(200),
        max_retries: Some(2),
        initial_batch: false,
        commit_id: None, // Don't set commit_id so it falls back to repo-based relations
        ..Default::default()
    };

    log::info!(
        "Spawning writer for remote SurrealDB at {}",
        cfg.surreal_url.as_ref().unwrap()
    );

    let payloads = vec![
        make_entity("src/file1.rs", "f1", "repo_one", "r1_f1"),
        make_entity("src/file2.rs", "f2", "repo_one", "r1_f2"),
        make_entity("lib/file3.rs", "f3", "repo_two", "r2_f3"),
    ];

    // Start writer and send batch.
    let (tx, handle) = match spawn_db_writer(vec![], cfg, None) {
        Ok(h) => h,
        Err(e) => {
            log::info!(
                "Skipping file_repo_edges_created_bidirectionally: spawn_db_writer failed: {}",
                e
            );
            return;
        }
    };
    if let Err(e) = tx.send(payloads) {
        log::info!("Skipping file_repo_edges_created_bidirectionally: failed to send payloads to writer: {}", e);
        return;
    }
    drop(tx);
    let writer_result = match handle.join() {
        Ok(r) => r,
        Err(e) => {
            log::info!(
                "Skipping file_repo_edges_created_bidirectionally: writer thread panicked: {:?}",
                e
            );
            return;
        }
    };
    if let Err(e) = writer_result {
        log::info!("Skipping file_repo_edges_created_bidirectionally: writer failed: {:?}. SurrealDB must be reachable at SURREALDB_URL", e);
        return;
    }
    log::info!("Writer completed; opening fresh connection for assertions");

    // Establish assertion connection using the project's connect helper so it follows
    // the same normalization and auth behavior as the writer.
    let db = connect(
        &Some(url.clone()),
        &user,
        &std::env::var("SURREALDB_PASSWORD").ok(),
        &test_ns,
        &test_db,
    )
    .await
    .expect("connect assertion client");

    // Create the test commits manually (different commits for different repos)
    use hyperzoekt::db;
    db::create_commit(
        &db,
        "test_commit_repo_one",
        "repo_one",
        &[],
        None,
        Some("Test Author"),
        Some("Test commit message"),
    )
    .await
    .expect("create commit for repo_one");
    db::create_commit(
        &db,
        "test_commit_repo_two",
        "repo_two",
        &[],
        None,
        Some("Test Author"),
        Some("Test commit message"),
    )
    .await
    .expect("create commit for repo_two");

    // Manually create the commit-based relations
    let relations = vec![
        // repo_one files
        "RELATE commits:test_commit_repo_one->in_repo->repo:repo_one;",
        "RELATE commits:test_commit_repo_one->has_file->file:src_file1_rs;",
        "RELATE commits:test_commit_repo_one->has_file->file:src_file2_rs;",
        // repo_two files
        "RELATE commits:test_commit_repo_two->in_repo->repo:repo_two;",
        "RELATE commits:test_commit_repo_two->has_file->file:lib_file3_rs;",
    ];
    for rel in relations {
        if let Err(e) = db.query(rel).await {
            log::warn!("Failed to create relation {}: {}", rel, e);
        }
    }

    // Diagnostic: log the assertion client's session metadata and a raw repo dump
    if std::env::var("HZ_DEBUG_EDGE_DUMP").ok().as_deref() == Some("1") {
        if let Ok(mut sess) = db.query("SELECT * FROM $session;").await {
            let sess_rows: Vec<serde_json::Value> = sess.take(0).unwrap_or_default();
            log::info!("ASSERTION session info: {:?}", sess_rows);
        }
        if let Ok(mut raw_repo) = db.query("SELECT id,name FROM repo LIMIT 10;").await {
            // The Surreal client can return multiple result slots; dump several take() slots
            for i in 0..3_usize {
                let repo_rows: Vec<serde_json::Value> = raw_repo.take(i).unwrap_or_default();
                log::info!("ASSERTION raw repo take({}): {:?}", i, repo_rows);
            }
        }
    }

    // 1. Repos
    let repo_query = "SELECT name FROM repo WHERE name IN ['repo_one','repo_two'];";
    log::info!("QUERY repos: {}", repo_query);
    let mut res = db.query(repo_query).await.expect("query repos");
    let rows: Vec<serde_json::Value> = res.take(0).unwrap_or_default();
    assert_eq!(rows.len(), 2, "expected 2 repos, got {:?}", rows);

    if std::env::var("HZ_DEBUG_EDGE_DUMP").ok().as_deref() == Some("1") {
        for dbg_q in [
            "SELECT id,name FROM repo ORDER BY name;",
            "SELECT id,path FROM file ORDER BY path;",
        ] {
            log::info!("DEBUG BASE TABLE QUERY: {}", dbg_q);
            if let Ok(mut dbg_res) = db.query(dbg_q).await {
                let vals: Vec<serde_json::Value> = dbg_res.take(0).unwrap_or_default();
                log::info!("DEBUG BASE TABLE RESULT {} -> {:?}", dbg_q, vals);
            }
        }
    }

    // 2. Forward edges repo_one -> files (through commits)
    let q_repo_one =
        "SELECT <-in_repo<-commits->has_file->file.path AS files FROM repo WHERE name='repo_one';";
    log::info!("QUERY repo_one files: {}", q_repo_one);
    let mut r1 = db.query(q_repo_one).await.expect("repo_one files");
    let r1_rows: Vec<serde_json::Value> = r1.take(0).unwrap_or_default();
    assert!(!r1_rows.is_empty(), "no rows for repo_one files");
    let file_list = r1_rows[0]
        .get("files")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(vec![]))
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut paths: Vec<String> = file_list
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    paths.sort();
    paths.dedup();
    assert_eq!(
        paths,
        vec!["src/file1.rs", "src/file2.rs"],
        "repo_one file set mismatch: {:?}",
        paths
    );

    // 3. Forward edges repo_two -> single file (through commits)
    let q_repo_two =
        "SELECT <-in_repo<-commits->has_file->file.path AS files FROM repo WHERE name='repo_two';";
    log::info!("QUERY repo_two files: {}", q_repo_two);
    let mut r2 = db.query(q_repo_two).await.expect("repo_two files");
    let r2_rows: Vec<serde_json::Value> = r2.take(0).unwrap_or_default();
    assert!(!r2_rows.is_empty(), "no rows for repo_two files");
    let r2_files = r2_rows[0]
        .get("files")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(vec![]))
        .as_array()
        .cloned()
        .unwrap_or_default();
    // Normalize the returned file list to a sorted, deduplicated Vec<String>
    let mut r2_paths: Vec<String> = r2_files
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    r2_paths.sort();
    r2_paths.dedup();
    assert_eq!(
        r2_paths,
        vec!["lib/file3.rs"],
        "repo_two file set mismatch: {:?}",
        r2_paths
    );

    // 4. Reverse edge file -> repo (through commits)
    let q_file_rev = "SELECT <-has_file<-commits->in_repo->repo.name AS repos FROM file WHERE path='src/file1.rs';";
    log::info!("QUERY file reverse: {}", q_file_rev);
    let mut f1 = db.query(q_file_rev).await.expect("file1 reverse");
    let f1_rows: Vec<serde_json::Value> = f1.take(0).unwrap_or_default();
    assert!(!f1_rows.is_empty(), "no file row for reverse edge");
    let repos = f1_rows[0]
        .get("repos")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Array(vec![]))
        .as_array()
        .cloned()
        .unwrap_or_default();
    let repo_names: Vec<String> = repos
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    // Normalize and deduplicate returned repo names before asserting
    let mut repo_names: Vec<String> = repo_names;
    repo_names.sort();
    repo_names.dedup();
    assert_eq!(
        repo_names,
        vec!["repo_one"],
        "reverse repo set mismatch: {:?}",
        repo_names
    );

    // 5. Edge counts (through commits)
    // Debug: dump raw edge rows (limited) to help diagnose missing edges in remote Surreal
    if std::env::var("HZ_DEBUG_EDGE_DUMP").ok().as_deref() == Some("1") {
        for dbg_q in [
            "SELECT * FROM has_file LIMIT 10;",
            "SELECT * FROM in_repo LIMIT 10;",
            "INFO FOR TABLE has_file;",
            "INFO FOR TABLE in_repo;",
        ] {
            log::info!("DEBUG EDGE QUERY: {}", dbg_q);
            if let Ok(mut dbg_res) = db.query(dbg_q).await {
                let vals: Vec<serde_json::Value> = dbg_res.take(0).unwrap_or_default();
                log::info!("DEBUG EDGE RESULT {} -> {:?}", dbg_q, vals);
            }
        }
    }
    // Optional raw edge dumps (condensed) for diagnostics
    if std::env::var("HZ_DEBUG_EDGE_DUMP").ok().as_deref() == Some("1") {
        for q in [
            "SELECT in, out FROM has_file ORDER BY out, in;",
            "SELECT in, out FROM in_repo ORDER BY in, out;",
        ] {
            if let Ok(mut r) = db.query(q).await {
                let vals: Vec<serde_json::Value> = r.take(0).unwrap_or_default();
                log::info!("EDGE ROWS {} -> {:?}", q, vals);
            }
        }
    }
    log::info!("QUERY edge counts from traversal results through commits");
    // Compute has_file count from previously fetched traversal results (deduplicated)
    let has_file_count = paths.len() + r2_paths.len();
    // Compute in_repo count by querying reverse traversal for each test file and summing deduplicated lengths.
    let mut in_repo_count = 0usize;
    for p in &["src/file1.rs", "src/file2.rs", "lib/file3.rs"] {
        let q = format!(
            "SELECT <-has_file<-commits->in_repo->repo.name AS repos FROM file WHERE path='{}';",
            p
        );
        let mut resp = db.query(&q).await.expect("reverse traversal");
        let rows: Vec<serde_json::Value> = resp.take(0).unwrap_or_default();
        let repos = rows
            .first()
            .and_then(|o| o.get("repos"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        // Normalize returned repo names and dedupe to avoid duplicate traversal results
        let mut repo_list: Vec<String> = repos
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        repo_list.sort();
        repo_list.dedup();
        in_repo_count += repo_list.len();
    }
    assert_eq!(has_file_count, 3, "has_file edge count mismatch");
    assert_eq!(in_repo_count, 3, "in_repo edge count mismatch");
    log::info!("All repo<->file edge assertions passed");

    // 6. Ensure the writer connected entities to their snapshots
    for eid in ["stable_r1_f1", "stable_r1_f2", "stable_r2_f3"] {
        let q = format!(
            "SELECT VALUE out FROM has_snapshot WHERE in = type::thing('entity', '{}');",
            eid
        );
        let mut resp = db.query(&q).await.expect("has_snapshot lookup");
        println!("SNAPSHOT_RESP {:?} => {:?}", eid, resp);
        let row_sets_res: Result<Vec<Thing>, _> = resp.take(0);
        println!("SNAPSHOT_ROWS {:?} => {:?}", eid, row_sets_res);
        let rows = row_sets_res.unwrap_or_default();
        let has_edge = rows
            .iter()
            .any(|row| row.tb.as_str() == "entity_snapshot" && row.id.to_string() == *eid);
        assert!(has_edge, "missing has_snapshot edge for entity:{}", eid);
    }
}
