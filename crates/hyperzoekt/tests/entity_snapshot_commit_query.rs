// Test that entity_snapshot queries correctly handle sourcecontrol_commit filtering
// Tests against real SurrealDB to verify type::string() cast behavior
use hyperzoekt::db::connection::connect as hz_connect;

#[tokio::test]
#[ignore = "Requires manual SurrealDB setup and CLI testing"]
async fn test_entity_snapshot_commit_filter_real_db() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the real SurrealDB instance on port 8000
    let conn = hz_connect(
        &Some("http://127.0.0.1:8000".to_string()),
        &Some("root".to_string()),
        &Some("root".to_string()),
        "test_ns",
        "test_db",
    )
    .await?;

    // Clean up any existing test data
    conn.query("DELETE entity_snapshot WHERE repo_name = 'test_commit_filter';")
        .await?;

    // Create test records
    let test_commit = "commits:abc123def456";
    let other_commit = "commits:xyz789ghi012";

    conn.query(&format!(
        r#"CREATE entity_snapshot:test_commit_1 CONTENT {{
            stable_id: "test_commit_1",
            repo_name: "test_commit_filter",
            sourcecontrol_commit: "{}",
            embedding_len: 768
        }};"#,
        test_commit
    ))
    .await?;

    conn.query(&format!(
        r#"CREATE entity_snapshot:test_commit_2 CONTENT {{
            stable_id: "test_commit_2",
            repo_name: "test_commit_filter",
            sourcecontrol_commit: "{}",
            embedding_len: 768
        }};"#,
        test_commit
    ))
    .await?;

    conn.query(&format!(
        r#"CREATE entity_snapshot:test_commit_3 CONTENT {{
            stable_id: "test_commit_3",
            repo_name: "test_commit_filter",
            sourcecontrol_commit: "{}",
            embedding_len: 512
        }};"#,
        other_commit
    ))
    .await?;

    // Test 1: Query WITHOUT type::string() cast (this may fail on real SurrealDB)
    let query_no_cast = format!(
        r#"SELECT id, stable_id FROM entity_snapshot WHERE repo_name = "test_commit_filter" AND sourcecontrol_commit = "{}""#,
        test_commit
    );

    let resp_no_cast = conn.query(&query_no_cast).await?;
    let rows_no_cast = hyperzoekt::db::helpers::response_to_json(resp_no_cast);

    println!(
        "Query without type::string() cast returned: {:?}",
        rows_no_cast
            .as_ref()
            .and_then(|v| v.as_array())
            .map(|a| a.len())
    );

    // Test 2: Query WITH type::string() cast (should work reliably)
    let query_with_cast = format!(
        r#"SELECT id, stable_id FROM entity_snapshot WHERE repo_name = "test_commit_filter" AND type::string(sourcecontrol_commit) = "{}""#,
        test_commit
    );

    let resp_with_cast = conn.query(&query_with_cast).await?;
    let rows_with_cast = hyperzoekt::db::helpers::response_to_json(resp_with_cast);

    assert!(
        rows_with_cast.is_some(),
        "Query with type::string() cast should return results"
    );

    let rows = rows_with_cast.unwrap();
    let rows_array = rows.as_array().expect("Should be array");

    assert_eq!(
        rows_array.len(),
        2,
        "Should get exactly 2 records with matching commit"
    );

    // Verify the records have correct stable_ids
    let stable_ids: Vec<String> = rows_array
        .iter()
        .filter_map(|v| v.get("stable_id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();

    assert!(stable_ids.contains(&"test_commit_1".to_string()));
    assert!(stable_ids.contains(&"test_commit_2".to_string()));

    // Test 3: Verify different commit filter works
    let query_other = format!(
        r#"SELECT id FROM entity_snapshot WHERE repo_name = "test_commit_filter" AND type::string(sourcecontrol_commit) = "{}""#,
        other_commit
    );

    let resp_other = conn.query(&query_other).await?;
    let rows_other = hyperzoekt::db::helpers::response_to_json(resp_other);

    assert!(rows_other.is_some());
    let rows_other_val = rows_other.unwrap();
    let rows_other_array = rows_other_val.as_array().expect("Should be array");
    assert_eq!(
        rows_other_array.len(),
        1,
        "Should get 1 record for other commit"
    );

    // Clean up
    conn.query("DELETE entity_snapshot WHERE repo_name = 'test_commit_filter';")
        .await?;

    Ok(())
}

#[tokio::test]
#[ignore = "Requires manual SurrealDB setup and CLI testing"]
async fn test_entity_snapshot_query_matches_similarity_worker(
) -> Result<(), Box<dyn std::error::Error>> {
    // Test the exact query pattern used by similarity worker
    let conn = hz_connect(
        &Some("http://127.0.0.1:8000".to_string()),
        &Some("root".to_string()),
        &Some("root".to_string()),
        "test_ns",
        "test_db",
    )
    .await?;

    // Clean up
    conn.query("DELETE entity_snapshot WHERE repo_name = 'test_similarity_worker';")
        .await?;

    let repo_name = "test_similarity_worker";
    let commit_hash = "abc123def456789";
    let commit_ref = format!("commits:{}", commit_hash);

    // Create test data with embeddings
    for i in 0..5 {
        conn.query(&format!(
            r#"CREATE entity_snapshot:sim_test_{} CONTENT {{
                stable_id: "sim_test_{}",
                repo_name: "{}",
                sourcecontrol_commit: "{}",
                embedding_len: 768,
                embedding: [0.1, 0.2, 0.3]
            }};"#,
            i, i, repo_name, commit_ref
        ))
        .await?;
    }

    // Use the exact query from similarity worker
    let query = format!(
        r#"SELECT id, stable_id, embedding, sourcecontrol_commit, embedding_len FROM entity_snapshot WHERE repo_name = "{}" AND type::string(sourcecontrol_commit) = "{}" AND embedding_len > 0 LIMIT 1000"#,
        repo_name, commit_ref
    );

    let resp = conn.query(&query).await?;
    let rows_opt = hyperzoekt::db::helpers::response_to_json(resp);

    assert!(
        rows_opt.is_some(),
        "Similarity worker query should return results"
    );

    let rows = rows_opt.unwrap();
    let rows_array = rows.as_array().expect("Should be array");

    assert_eq!(rows_array.len(), 5, "Should retrieve all 5 test snapshots");

    // Verify all expected fields are present
    for row in rows_array {
        assert!(row.get("id").is_some());
        assert!(row.get("stable_id").is_some());
        assert!(row.get("embedding").is_some());
        assert!(row.get("sourcecontrol_commit").is_some());
        assert!(row.get("embedding_len").is_some());

        let len = row.get("embedding_len").unwrap().as_i64();
        assert_eq!(len, Some(768));
    }

    // Clean up
    conn.query("DELETE entity_snapshot WHERE repo_name = 'test_similarity_worker';")
        .await?;

    Ok(())
}
#[tokio::test]
#[ignore = "Requires manual SurrealDB setup on port 8000"]
async fn test_similarity_worker_none_filter() -> Result<(), Box<dyn std::error::Error>> {
    // Test the != NONE query pattern used by similarity worker
    // CRITICAL: In SurrealDB, "IS NOT NULL" does NOT filter out NONE values!
    // You MUST use "!= NONE" to properly filter.
    let conn = hz_connect(
        &Some("http://127.0.0.1:8000".to_string()),
        &Some("root".to_string()),
        &Some("root".to_string()),
        "test_ns",
        "test_db",
    )
    .await?;

    // Clean up ALL test data from previous runs
    conn.query("DELETE entity_snapshot WHERE repo_name = 'test_is_not_null';")
        .await?;
    conn.query("DELETE entity_snapshot WHERE repo_name = 'test_similarity_worker';")
        .await?;
    conn.query("DELETE entity_snapshot WHERE stable_id ~ 'test_';")
        .await?;
    conn.query("DELETE entity_snapshot WHERE stable_id ~ 'null_test';")
        .await?;
    conn.query("DELETE entity_snapshot WHERE stable_id ~ 'sim_test';")
        .await?;
    conn.query("DELETE entity_snapshot WHERE stable_id = 'external_test_1';")
        .await?;

    let repo_name = "test_none_filter";
    let commit_hash = "abc123def456789";
    let commit_ref = format!("commits:{}", commit_hash);

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

    let resp = conn.query(&batch_query).await?;
    let rows_opt = hyperzoekt::db::helpers::response_to_json(resp);

    assert!(rows_opt.is_some(), "Batch query should return results");

    let rows = rows_opt.unwrap();
    let rows_array = rows.as_array().expect("Should be array");
    assert_eq!(
        rows_array.len(),
        2,
        "Should retrieve only 2 entities with non-NONE source_content"
    );

    // Verify we got the correct entities
    let stable_ids: Vec<String> = rows_array
        .iter()
        .filter_map(|v| v.get("stable_id"))
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();

    assert!(stable_ids.contains(&"none_filter_1".to_string()));
    assert!(stable_ids.contains(&"none_filter_2".to_string()));
    assert!(!stable_ids.contains(&"none_filter_3".to_string()));
    assert!(!stable_ids.contains(&"none_filter_4".to_string()));

    // Test 4: Same-repo similarity query with != NONE
    let sim_query = format!(
        r#"LET $source_embedding = (SELECT embedding FROM entity_snapshot WHERE stable_id = "none_filter_1" LIMIT 1)[0].embedding;
           SELECT id, stable_id, sourcecontrol_commit, vector::similarity::cosine(embedding, $source_embedding) AS score FROM entity_snapshot WHERE embedding_len > 0 AND repo_name = "{}" AND sourcecontrol_commit = "{}" AND stable_id != "none_filter_1" AND source_content != NONE ORDER BY score DESC LIMIT 25"#,
        repo_name, commit_ref
    );

    let mut resp = conn.query(&sim_query).await?;
    let _: Option<serde_json::Value> = resp.take(0)?; // LET binding
    let rows_opt: Option<Vec<serde_json::Value>> = resp.take(1)?; // SELECT

    assert!(rows_opt.is_some(), "Similarity query should return results");

    let rows = rows_opt.unwrap();
    assert_eq!(
        rows.len(),
        1,
        "Should retrieve only none_filter_2 (none_filter_1 excluded, none_filter_3/4 have NONE source_content)"
    );

    let result = &rows[0];
    assert_eq!(
        result.get("stable_id").and_then(|v| v.as_str()),
        Some("none_filter_2")
    );
    assert!(
        result.get("score").is_some(),
        "Should have similarity score"
    );

    // Test 5: External-repo query pattern
    conn.query(&format!(
        r#"CREATE entity_snapshot:external_none_test CONTENT {{
            stable_id: "external_none_test",
            repo_name: "other_repo",
            sourcecontrol_commit: "commits:xyz789",
            embedding_len: 768,
            embedding: [0.15, 0.25, 0.35],
            source_content: "function other() {{ return 99; }}"
        }};"#
    ))
    .await?;

    let external_query = format!(
        r#"LET $source_embedding = (SELECT embedding FROM entity_snapshot WHERE stable_id = "none_filter_1" LIMIT 1)[0].embedding;
           SELECT id, stable_id, repo_name, sourcecontrol_commit, vector::similarity::cosine(embedding, $source_embedding) AS score FROM entity_snapshot WHERE embedding_len > 0 AND repo_name != "{}" AND source_content != NONE ORDER BY score DESC LIMIT 50"#,
        repo_name
    );

    let mut resp = conn.query(&external_query).await?;
    let _: Option<serde_json::Value> = resp.take(0)?; // LET binding
    let rows_opt: Option<Vec<serde_json::Value>> = resp.take(1)?; // SELECT

    assert!(
        rows_opt.is_some(),
        "External-repo query should return results"
    );

    let rows = rows_opt.unwrap();
    assert!(
        rows.len() >= 1,
        "Should find at least external_none_test from other_repo"
    );

    let external_found = rows
        .iter()
        .any(|r| r.get("stable_id").and_then(|v| v.as_str()) == Some("external_none_test"));
    assert!(
        external_found,
        "Should find external_none_test from other_repo"
    );

    // Clean up
    conn.query("DELETE entity_snapshot WHERE repo_name = 'test_none_filter';")
        .await?;
    conn.query("DELETE entity_snapshot WHERE stable_id = 'external_none_test';")
        .await?;

    Ok(())
}
