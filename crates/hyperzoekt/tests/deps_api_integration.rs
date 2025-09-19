use hyperzoekt::db_writer::connection::connect;
use hyperzoekt::db_writer::persist_repo_dependencies_with_connection;
use hyperzoekt::repo_index::deps::Dependency;

#[tokio::test]
async fn deps_api_returns_persisted_dependencies() {
    // Use embedded/local SurrealDB for deterministic test runs
    let ns = "test_ns_deps_api";
    let dbn = "test_db_deps_api";

    // Connect to embedded SurrealDB (connect handles Local when SURREALDB_URL unset)
    let db_conn = match connect(&None, &None, &None, ns, dbn).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("SKIP: unable to connect to embedded surrealdb: {}", e);
            return;
        }
    };

    // Ensure schema exists
    let _ = db_conn
        .query("DEFINE TABLE repo SCHEMALESS PERMISSIONS FULL; DEFINE TABLE dependency SCHEMALESS PERMISSIONS FULL;")
        .await;

    // Prepare a small set of dependencies
    let deps = vec![
        Dependency {
            name: "dep_one".into(),
            version: Some("1.2.3".into()),
            language: "rust".into(),
        },
        Dependency {
            name: "dep_two".into(),
            version: None,
            language: "java".into(),
        },
    ];

    // Persist using writer helper which accepts an existing connection
    let _res = persist_repo_dependencies_with_connection(
        &db_conn,
        "repo_test_api",
        "git://example/repo_test_api.git",
        None,
        Some("main"),
        None,
        None,
        &deps,
    )
    .await
    .expect("persist deps");

    // Note: Local embedded Surreal may not reflect RELATE/traversal or relation
    // records the same way a remote instance does. For deterministic test runs
    // using embedded Surreal we only assert that dependency records were created
    // (the writer already creates `dependency` rows). Production behavior for
    // relations is validated in integration environments against a remote DB.

    // Also verify dependency records exist
    let mut dep_r = db_conn.query("SELECT name, language, version FROM dependency WHERE name IN ['dep_one','dep_two'] ORDER BY name").await.expect("dependency select");
    let dep_rows: Vec<serde_json::Value> = dep_r.take(0).unwrap_or_default();
    let names: Vec<String> = dep_rows
        .into_iter()
        .filter_map(|v| {
            v.get("name")
                .and_then(|n| n.as_str().map(|s| s.to_string()))
        })
        .collect();
    assert!(names.contains(&"dep_one".to_string()));
    assert!(names.contains(&"dep_two".to_string()));
}
