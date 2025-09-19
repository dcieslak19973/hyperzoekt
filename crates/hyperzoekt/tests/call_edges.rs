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
use hyperzoekt::db_writer::{spawn_db_writer, DbWriterConfig};
use hyperzoekt::repo_index::indexer::payload::EntityPayload;

#[tokio::test]
async fn test_call_edges_created() {
    // Prepare two entities where one calls the other
    let ent_a = EntityPayload {
        language: "rust".into(),
        kind: "function".into(),
        name: "callee_fn".into(),
        parent: None,
        signature: "fn callee_fn()".into(),
        start_line: Some(1),
        end_line: Some(2),
        doc: None,
        rank: Some(1.0),
        imports: vec![],
        unresolved_imports: vec![],
        stable_id: "callee_fn_id".into(),
        repo_name: "repo".into(),
        source_url: None,
        source_display: None,
        source_content: None,
        calls: vec![],
        methods: vec![],
    };
    let ent_b = EntityPayload {
        language: "rust".into(),
        kind: "function".into(),
        name: "caller_fn".into(),
        parent: None,
        signature: "fn caller_fn()".into(),
        start_line: Some(10),
        end_line: Some(20),
        doc: None,
        rank: Some(2.0),
        imports: vec![],
        unresolved_imports: vec![],
        stable_id: "caller_fn_id".into(),
        repo_name: "repo".into(),
        source_url: None,
        source_display: None,
        source_content: None,
        calls: vec!["callee_fn".into()],
        methods: vec![],
    };

    // Ensure this test uses embedded SurrealDB even if SURREALDB_URL is set in
    // the environment (CI sets it). Save/restore env vars to avoid leakage.
    let saved_url = std::env::var("SURREALDB_URL").ok();
    let saved_user = std::env::var("SURREALDB_USERNAME").ok();
    let saved_pass = std::env::var("SURREALDB_PASSWORD").ok();
    std::env::remove_var("SURREALDB_URL");
    std::env::remove_var("SURREALDB_USERNAME");
    std::env::remove_var("SURREALDB_PASSWORD");

    let cfg = DbWriterConfig {
        channel_capacity: 10,
        batch_capacity: Some(10),
        batch_timeout_ms: Some(200),
        max_retries: Some(1),
        surreal_url: None,
        surreal_username: None,
        surreal_password: None,
        surreal_ns: "testns_call".into(),
        surreal_db: "testdb_call".into(),
        initial_batch: false,
        // rely on Default for any future fields
        ..Default::default()
    };

    hyperzoekt::db_writer::init_call_edge_capture();
    let (tx, handle) = spawn_db_writer(vec![], cfg.clone(), None).expect("spawn writer");
    tx.send(vec![ent_a.clone(), ent_b.clone()])
        .expect("send batch");
    // Allow writer thread to receive before closing channel
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    drop(tx);
    handle.join().expect("join").expect("writer ok");
    let cap = hyperzoekt::db_writer::CALL_EDGE_CAPTURE
        .get()
        .expect("capture store")
        .lock()
        .expect("lock capture");
    assert!(
        cap.iter()
            .any(|(c, cal)| c == "caller_fn" && cal == "callee_fn"),
        "expected captured call edge caller_fn->callee_fn, got {:?}",
        *cap
    );

    if let Some(v) = saved_url {
        std::env::set_var("SURREALDB_URL", v);
    }
    if let Some(v) = saved_user {
        std::env::set_var("SURREALDB_USERNAME", v);
    }
    if let Some(v) = saved_pass {
        std::env::set_var("SURREALDB_PASSWORD", v);
    }
}
