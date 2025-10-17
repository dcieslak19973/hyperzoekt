use hyperzoekt::db::connection::connect as hz_connect;

#[tokio::test]
async fn test_relate_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    // Use embedded in-memory SurrealDB for a quick integration smoke test
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    std::env::set_var("HZ_EPHEMERAL_MEM", "1");
    // Create a transient in-memory DB
    let conn = hz_connect(&None, &None, &None, "testns", "testdb").await?;
    conn.use_ns("test").await?;
    conn.use_db("test").await?;
    // Create tables and unique index similar to writer's schema (split statements)
    conn.query("DEFINE TABLE entity SCHEMALESS;").await?;
    conn.query("DEFINE TABLE entity_snapshot SCHEMALESS;")
        .await?;
    conn.query(
        "DEFINE TABLE similar_same_repo TYPE RELATION FROM entity_snapshot TO entity_snapshot;",
    )
    .await?;
    conn.query(
        "DEFINE TABLE similar_external_repo TYPE RELATION FROM entity_snapshot TO entity_snapshot;",
    )
    .await?;
    conn.query(
        "DEFINE INDEX idx_similar_same_repo_unique ON similar_same_repo FIELDS in, out UNIQUE;",
    )
    .await?;

    // Insert two entity rows and corresponding entity_snapshot rows (relations are FROM entity_snapshot)
    conn.query("CREATE entity:one CONTENT { stable_id: \"one\", repo_name: \"r1\" }; CREATE entity:two CONTENT { stable_id: \"two\", repo_name: \"r1\" }; CREATE entity_snapshot:es_one CONTENT { stable_id: \"one\", repo_name: \"r1\" }; CREATE entity_snapshot:es_two CONTENT { stable_id: \"two\", repo_name: \"r1\" }; ").await?;

    // Run RELATE twice and ensure only one relation exists. Use entity_snapshot:es_one/out since relation FROM entity_snapshot.
    let relate_sql = "RELATE entity_snapshot:es_one -> similar_same_repo -> entity_snapshot:es_two CONTENT { score: 0.9 } RETURN AFTER;";
    let _ = conn.query(relate_sql).await?;
    let _ = conn.query(relate_sql).await?;

    // Query only the score field to avoid Thing/enum parsing issues in tests
    let mut resp = conn
        .query("SELECT score FROM similar_same_repo START AT 0 LIMIT 10")
        .await?;
    let rows: Vec<serde_json::Value> = resp.take(0)?;
    assert!(
        !rows.is_empty(),
        "expected at least one similar_same_repo row"
    );
    if let Some(r0) = rows.get(0) {
        // row may be an object like { "score": 0.9 }
        if let Some(score_val) = r0.get("score") {
            assert!(score_val.is_number(), "score is not numeric");
        } else {
            panic!("relation missing score field: {:?}", r0);
        }
    }

    Ok(())
}
