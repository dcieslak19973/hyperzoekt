// Test the similarity worker query patterns with != NONE filter
// CRITICAL: In SurrealDB, "IS NOT NULL" does NOT filter out NONE values!
// You MUST use "!= NONE" to properly filter.
use hyperzoekt::db::connection::connect as hz_connect;

#[tokio::test]
#[ignore = "Requires manual SurrealDB setup on port 8000"]
async fn test_similarity_worker_none_filter() -> Result<(), Box<dyn std::error::Error>> {
    let conn = hz_connect(
        &Some("http://127.0.0.1:8000".to_string()),
        &Some("root".to_string()),
        &Some("root".to_string()),
        "test_ns",
        "test_db",
    )
    .await?;

    let repo_name = "test_none_filter";
    let commit_hash = "abc123def456789";
    let commit_ref = format!("commits:{}", commit_hash);

    // Clean up from any previous runs
    conn.query(&format!(
        "DELETE entity_snapshot WHERE repo_name = '{}';",
        repo_name
    ))
    .await?;
    conn.query("DELETE entity_snapshot WHERE repo_name = 'other_repo';")
        .await?;

    // Create test data: mix of NONE and non-NONE source_content
    conn.query(&format!(
        r#"CREATE entity_snapshot:none_filter_1 CONTENT {{
            stable_id: "none_filter_1",
            repo_name: "{}",
            sourcecontrol_commit: "{}",
            embedding_len: 768,
            embedding: [0.1, 0.2, 0.3],
            source_content: "function example() {{ return 42; }}"
        }};"#,
        repo_name, commit_ref
    ))
    .await?;

    conn.query(&format!(
        r#"CREATE entity_snapshot:none_filter_2 CONTENT {{
            stable_id: "none_filter_2",
            repo_name: "{}",
            sourcecontrol_commit: "{}",
            embedding_len: 768,
            embedding: [0.2, 0.3, 0.4],
            source_content: "short"
        }};"#,
        repo_name, commit_ref
    ))
    .await?;

    conn.query(&format!(
        r#"CREATE entity_snapshot:none_filter_3 CONTENT {{
            stable_id: "none_filter_3",
            repo_name: "{}",
            sourcecontrol_commit: "{}",
            embedding_len: 768,
            embedding: [0.3, 0.4, 0.5],
            source_content: NONE
        }};"#,
        repo_name, commit_ref
    ))
    .await?;

    conn.query(&format!(
        r#"CREATE entity_snapshot:none_filter_4 CONTENT {{
            stable_id: "none_filter_4",
            repo_name: "{}",
            sourcecontrol_commit: "{}",
            embedding_len: 768,
            embedding: [0.4, 0.5, 0.6]
        }};"#,
        repo_name, commit_ref
    ))
    .await?;

    // Test 1: Count query with != NONE (used by similarity worker)
    let count_query = format!(
        r#"SELECT COUNT() FROM entity_snapshot WHERE repo_name = "{}" AND sourcecontrol_commit = "{}" AND embedding_len > 0 AND source_content != NONE GROUP ALL"#,
        repo_name, commit_ref
    );

    let resp = conn.query(&count_query).await?;
    let rows_opt = hyperzoekt::db::helpers::response_to_json(resp);

    assert!(rows_opt.is_some(), "Count query should return results");

    let rows = rows_opt.unwrap();
    let rows_array = rows.as_array().expect("Should be array");
    assert_eq!(rows_array.len(), 1, "Should have one count result");

    let count = rows_array[0].get("count").and_then(|v| v.as_i64());
    assert_eq!(
        count,
        Some(2),
        "Should count only 2 entities with non-NONE source_content (none_filter_1 and none_filter_2)"
    );

    // Test 2: Document the SurrealDB IS NOT NULL quirk
    let is_not_null_query = format!(
        r#"SELECT COUNT() FROM entity_snapshot WHERE repo_name = "{}" AND sourcecontrol_commit = "{}" AND embedding_len > 0 AND source_content IS NOT NULL GROUP ALL"#,
        repo_name, commit_ref
    );

    let resp = conn.query(&is_not_null_query).await?;
    let rows_opt = hyperzoekt::db::helpers::response_to_json(resp);

    if let Some(rows) = rows_opt {
        if let Some(rows_array) = rows.as_array() {
            if !rows_array.is_empty() {
                if let Some(count) = rows_array[0].get("count").and_then(|v| v.as_i64()) {
                    // This documents the SurrealDB quirk: IS NOT NULL includes NONE values!
                    assert_eq!(
                        count, 4,
                        "SurrealDB quirk: IS NOT NULL incorrectly returns all 4 records including NONE"
                    );
                }
            }
        }
    }

    // Test 3: Batch query with != NONE
    let batch_query = format!(
        r#"SELECT meta::id(id) AS id, stable_id, embedding, sourcecontrol_commit, embedding_len FROM entity_snapshot WHERE repo_name = "{}" AND sourcecontrol_commit = "{}" AND embedding_len > 0 AND source_content != NONE LIMIT 1000 START 0"#,
        repo_name, commit_ref
    );

    let mut resp = conn.query(&batch_query).await?;

    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct SnapshotRow {
        id: String,
        stable_id: String,
        embedding: Option<Vec<f32>>,
        sourcecontrol_commit: Option<String>,
        embedding_len: Option<i64>,
    }

    let rows: Vec<SnapshotRow> = resp.take(0)?;

    assert_eq!(
        rows.len(),
        2,
        "Should retrieve only 2 entities with non-NONE source_content"
    );

    // Verify we got the correct entities
    let stable_ids: Vec<String> = rows.iter().map(|r| r.stable_id.clone()).collect();

    assert!(stable_ids.contains(&"none_filter_1".to_string()));
    assert!(stable_ids.contains(&"none_filter_2".to_string()));
    assert!(!stable_ids.contains(&"none_filter_3".to_string()));
    assert!(!stable_ids.contains(&"none_filter_4".to_string()));

    // Test 4: Same-repo similarity query with != NONE — fetch embedding first to avoid LET+SELECT
    let source_emb_opt = hyperzoekt::db::helpers::get_embedding_for_entity(&conn, "none_filter_1")
        .await
        .map_err(|e| anyhow::anyhow!("get_embedding_for_entity failed: {}", e))?;
    let source_emb = source_emb_opt.expect("source embedding present");
    let emb_literal = source_emb
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let sim_query = format!(
        r#"SELECT id, stable_id, sourcecontrol_commit, vector::similarity::cosine(embedding, [{}]) AS score FROM entity_snapshot WHERE embedding_len > 0 AND repo_name = "{}" AND sourcecontrol_commit = "{}" AND stable_id != "none_filter_1" AND source_content != NONE ORDER BY score DESC LIMIT 25"#,
        emb_literal, repo_name, commit_ref
    );

    let mut resp = conn.query(&sim_query).await?;
    // Use the canonical response_to_json helper to normalize remote and
    // embedded SurrealDB client response shapes into a serde_json::Value.
    // This avoids fragile typed takes and keeps parsing logic centralized.
    // Deserialize into typed rows using the Surreal client shapes (Thing id)
    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct SimilarRow {
        id: surrealdb::sql::Thing,
        stable_id: String,
        sourcecontrol_commit: String,
        score: f32,
    }

    let rows: Vec<SimilarRow> = resp.take(0)?;
    assert_eq!(
        rows.len(),
        1,
        "Should retrieve only none_filter_2 (none_filter_1 excluded, none_filter_3/4 have NONE source_content)"
    );

    let result = &rows[0];
    // Access typed fields directly on the deserialized row
    assert_eq!(result.stable_id, "none_filter_2");
    // Score should be a finite f32 value (not NaN/inf)
    assert!(result.score.is_finite(), "Should have similarity score");

    // Test 5: External-repo query pattern
    conn.query(
        r#"CREATE entity_snapshot:external_none_test CONTENT {
            stable_id: "external_none_test",
            repo_name: "other_repo",
            sourcecontrol_commit: "commits:xyz789",
            embedding_len: 768,
            embedding: [0.15, 0.25, 0.35],
            source_content: "function other() { return 99; }"
        };"#,
    )
    .await?;

    let external_emb_literal = source_emb
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let external_query = format!(
        r#"SELECT id, stable_id, repo_name, sourcecontrol_commit, vector::similarity::cosine(embedding, [{}]) AS score FROM entity_snapshot WHERE embedding_len > 0 AND repo_name != "{}" AND source_content != NONE ORDER BY score DESC LIMIT 50"#,
        external_emb_literal, repo_name
    );

    let mut resp = conn.query(&external_query).await?;
    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct ExternalRow {
        id: surrealdb::sql::Thing,
        stable_id: String,
        repo_name: String,
        sourcecontrol_commit: String,
        score: f32,
    }

    let rows: Vec<ExternalRow> = resp.take(0)?;
    assert!(
        rows.len() >= 1,
        "Should find at least external_none_test from other_repo"
    );

    let external_found = rows.iter().any(|r| r.stable_id == "external_none_test");
    assert!(
        external_found,
        "Should find external_none_test from other_repo"
    );

    // Clean up
    conn.query(&format!(
        "DELETE entity_snapshot WHERE repo_name = '{}';",
        repo_name
    ))
    .await?;
    conn.query("DELETE entity_snapshot WHERE repo_name = 'other_repo';")
        .await?;

    Ok(())
}
