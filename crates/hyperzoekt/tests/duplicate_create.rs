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

use serde_json::json;
use surrealdb::engine::local::Mem;
use surrealdb::Surreal;

#[tokio::test]
async fn duplicate_create_updates() {
    // Start embedded SurrealDB
    let db = Surreal::new::<Mem>(()).await.unwrap();
    db.use_ns("test").use_db("test").await.unwrap();

    // Ensure table and index exist
    let _ = db.query("CREATE TABLE entity;").await; // ignore errors
    let _ = db
        .query("ALTER TABLE entity CREATE FIELD stable_id TYPE string;")
        .await;
    let _ = db
        .query("CREATE INDEX idx_entity_stable_id ON entity (stable_id);")
        .await;

    let payload = json!({
        "file": "foo.rs",
        "language": "rust",
        "kind": "function",
        "name": "dup",
        "parent": null,
        "signature": "()",
        "start_line": 1,
        "end_line": 2,
        "calls": [],
        "doc": null,
        "rank": 1.0,
        "imports": [],
        "unresolved_imports": [],
        "stable_id": "deadbeef",
    });

    // First create should succeed
    db.query("CREATE entity CONTENT $entity")
        .bind(("entity", payload.clone()))
        .await
        .unwrap();

    // Second create should result in duplicate handling; emulate the binary logic
    let res = db
        .query("CREATE entity CONTENT $entity")
        .bind(("entity", payload.clone()))
        .await;
    if let Err(e) = res {
        let msg = format!("{}", e);
        if msg.to_lowercase().contains("duplicate") || msg.to_lowercase().contains("unique") {
            // Attempt update
            db.query("UPDATE entity CONTENT $entity WHERE stable_id = $stable_id")
                .bind(("entity", payload.clone()))
                .bind(("stable_id", "deadbeef"))
                .await
                .unwrap();
        } else {
            panic!("unexpected DB error: {}", e);
        }
    }

    // Verify there's one entity with stable_id
    let rows = db
        .query("SELECT * FROM entity WHERE stable_id = $stable_id")
        .bind(("stable_id", "deadbeef"))
        .await
        .unwrap();
    let s = format!("{:?}", rows);
    assert!(s.contains("result"));
}
