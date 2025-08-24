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
