use serde_json;
use surrealdb::engine::local::Mem;
use surrealdb::Surreal;

#[tokio::test]
async fn test_entity_relations_basic_sql() {
    // Setup an embedded SurrealDB instance
    let db = Surreal::new::<Mem>(()).await.expect("start surreal");
    db.use_ns("zoekt").use_db("repos").await.expect("use ns/db");

    // Ensure clean state
    let _ = db.query("REMOVE entity").await;

    // Insert three entities: A calls B and "unknown", C calls A
    let create_a = r#"CREATE entity CONTENT {
         stable_id: "A",
         name: "Alpha",
         repo_name: "test-repo",
         signature: "fn alpha()",
         file: "src/a.rs",
         language: "rust",
         kind: "function",
         parent: null,
         start_line: 1,
         end_line: 10,
         calls: ["B", "unknown"],
         doc: null,
         rank: 0.5,
         imports: [],
         unresolved_imports: []
     }"#;

    let create_b = r#"CREATE entity CONTENT {
         stable_id: "B",
         name: "Beta",
         repo_name: "test-repo",
         signature: "fn beta()",
         file: "src/b.rs",
         language: "rust",
         kind: "function",
         parent: null,
         start_line: 1,
         end_line: 8,
         calls: [],
         doc: null,
         rank: 0.6,
         imports: [],
         unresolved_imports: []
     }"#;

    let create_c = r#"CREATE entity CONTENT {
         stable_id: "C",
         name: "Gamma",
         repo_name: "test-repo",
         signature: "fn gamma()",
         file: "src/c.rs",
         language: "rust",
         kind: "function",
         parent: null,
         start_line: 1,
         end_line: 12,
         calls: ["A"],
         doc: null,
         rank: 0.7,
         imports: [],
         unresolved_imports: []
     }"#;

    db.query(create_a).await.expect("create a");
    db.query(create_b).await.expect("create b");
    db.query(create_c).await.expect("create c");

    // Run callers query (with rank in select) to ensure ORDER BY rank works
    let callers_q = r#"SELECT name, stable_id, rank FROM entity WHERE $id IN calls OR $name IN calls ORDER BY rank DESC LIMIT $limit"#;
    let mut resp = db
        .query(callers_q)
        .bind(("id", "A"))
        .bind(("name", "Alpha"))
        .bind(("limit", 10i64))
        .await
        .expect("callers q");
    let rows: Vec<serde_json::Value> = resp.take(0).expect("rows");
    // Should find one caller (Gamma)
    assert!(rows
        .iter()
        .any(|r| r.get("name").and_then(|v| v.as_str()) == Some("Gamma")));

    // Get distinct calls and lookup callees by stable_id and name
    let calls_q =
        r#"SELECT array::distinct(calls) as calls FROM entity WHERE stable_id = $id LIMIT 1"#;
    let mut resp2 = db.query(calls_q).bind(("id", "A")).await.expect("calls q");
    let vals: Vec<serde_json::Value> = resp2.take(0).expect("vals");
    let calls_list = vals
        .get(0)
        .and_then(|v| v.get("calls"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    // Now lookup by stable_id IN calls
    let lookup_id = r#"SELECT name, stable_id, rank FROM entity WHERE stable_id IN $calls ORDER BY rank DESC LIMIT $limit"#;
    let calls_json = serde_json::to_value(calls_list.clone()).unwrap();
    let mut resp3 = db
        .query(lookup_id)
        .bind(("calls", calls_json))
        .bind(("limit", 10i64))
        .await
        .expect("lookup id");
    let rows3: Vec<serde_json::Value> = resp3.take(0).expect("rows3");
    assert!(rows3
        .iter()
        .any(|r| r.get("name").and_then(|v| v.as_str()) == Some("Beta")));

    // For unresolved calls ensure at least the 'unknown' appears in calls_list
    assert!(calls_list.iter().any(|v| v.as_str() == Some("unknown")));
}
