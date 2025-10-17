// Integration test that uses EXACT queries from hyperzoekt-similarity-worker
// This ensures the test validates the actual production code paths
use hyperzoekt::db::connection::connect as hz_connect;

#[tokio::test]
#[ignore = "Requires manual SurrealDB setup on port 8000"]
async fn test_exact_similarity_worker_queries() -> Result<(), Box<dyn std::error::Error>> {
    let conn = hz_connect(
        &Some("http://127.0.0.1:8000".to_string()),
        &Some("root".to_string()),
        &Some("root".to_string()),
        "test_ns",
        "test_db",
    )
    .await?;

    let repo_name = "test_exact_queries";
    let commit_hash = "abc123def456789";
    let commit_ref = format!("commits:{}", commit_hash);

    // Clean up
    conn.query(&format!(
        "DELETE entity_snapshot WHERE repo_name = '{}';",
        repo_name
    ))
    .await?;
    conn.query("DELETE entity_snapshot WHERE repo_name = 'other_repo';")
        .await?;

    // Create test entities - exactly as they would exist in production
    for i in 0..5 {
        let has_content = i < 3; // First 3 have content, last 2 have NONE
        let content_field = if has_content {
            format!(
                r#", source_content: "function test_{}() {{ return {}; }}""#,
                i,
                i * 10
            )
        } else {
            String::new()
        };

        conn.query(&format!(
            r#"CREATE entity_snapshot:exact_test_{} CONTENT {{
                stable_id: "exact_test_{}",
                repo_name: "{}",
                sourcecontrol_commit: "{}",
                embedding_len: 768,
                embedding: [{}, {}, {}]{}
            }};"#,
            i,
            i,
            repo_name,
            commit_ref,
            0.1 + (i as f32 * 0.1),
            0.2 + (i as f32 * 0.1),
            0.3 + (i as f32 * 0.1),
            content_field
        ))
        .await?;
    }

    // TEST 1: EXACT Count Query from similarity worker (line 331-335)
    println!("\n=== TEST 1: Count Query ===");
    let count_sql = format!(
        "SELECT COUNT() FROM entity_snapshot \
         WHERE repo_name = \"{}\" AND sourcecontrol_commit = \"{}\" AND embedding_len > 0 AND source_content != NONE \
         GROUP ALL",
        repo_name.replace("\"", "\\\""),
        commit_ref.replace("\"", "\\\"")
    );

    let resp = conn.query(&count_sql).await?;
    let rows_opt = hyperzoekt::db::helpers::response_to_json(resp);
    assert!(rows_opt.is_some(), "Count query should return results");

    let rows = rows_opt.unwrap();
    let rows_array = rows.as_array().expect("Should be array");
    let count = rows_array[0]
        .get("count")
        .and_then(|v| v.as_i64())
        .expect("Should have count");

    println!("Count query returned: {}", count);
    assert_eq!(
        count, 3,
        "Should count only 3 entities with non-NONE source_content"
    );

    // TEST 2: EXACT Batch Query from similarity worker (line 378-386)
    println!("\n=== TEST 2: Batch Query ===");
    const BATCH_SIZE: usize = 1000;
    let offset = 0;

    let batch_sql = format!(
        "SELECT meta::id(id) AS id, stable_id, embedding, sourcecontrol_commit, embedding_len \
         FROM entity_snapshot \
         WHERE repo_name = \"{}\" AND sourcecontrol_commit = \"{}\" AND embedding_len > 0 AND source_content != NONE \
         LIMIT {} START {}",
        repo_name.replace("\"", "\\\""),
        commit_ref.replace("\"", "\\\""),
        BATCH_SIZE,
        offset
    );

    let mut resp = conn.query(&batch_sql).await?;

    // Use EXACT struct from similarity worker (line 397-403)
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
    println!("Batch query returned {} rows", rows.len());
    assert_eq!(
        rows.len(),
        3,
        "Should retrieve only 3 entities with non-NONE source_content"
    );

    // Verify all rows have expected structure
    for row in &rows {
        assert!(row.embedding.is_some());
        assert_eq!(row.embedding.as_ref().unwrap().len(), 3);
        assert!(row.sourcecontrol_commit.is_some());
        assert_eq!(row.embedding_len, Some(768));
        println!(
            "  - {}: embedding_len={}",
            row.stable_id,
            row.embedding_len.unwrap()
        );
    }

    // TEST 3: EXACT Same-Repo Query from similarity worker (line 546-551)
    println!("\n=== TEST 3: Same-Repo Similarity Query ===");
    let max_same = 25;

    let same_repo_sql = format!(
        "LET $source_embedding = (SELECT embedding FROM entity_snapshot WHERE stable_id = $source_stable_id LIMIT 1)[0].embedding; \
         SELECT id, stable_id, sourcecontrol_commit, vector::similarity::cosine(embedding, $source_embedding) AS score \
         FROM entity_snapshot \
         WHERE embedding_len > 0 AND repo_name = $repo AND sourcecontrol_commit = $commit AND stable_id != $source_stable_id AND source_content != NONE \
         ORDER BY score DESC \
         LIMIT {}",
        max_same
    );

    let same_resp = conn
        .query_with_binds(
            &same_repo_sql,
            vec![
                ("source_stable_id", serde_json::json!("exact_test_0")),
                ("repo", serde_json::json!(repo_name)),
                ("commit", serde_json::json!(&commit_ref)),
            ],
        )
        .await?;

    // Use EXACT struct from similarity worker (line 581-586)
    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct SimilarRow {
        id: surrealdb::sql::Thing, // Use Thing type instead of serde_json::Value
        stable_id: String,
        sourcecontrol_commit: String,
        score: f32,
    }

    let mut resp = same_resp;
    let same_results: Vec<SimilarRow> = resp.take(1)?; // LET is slot 0, SELECT is slot 1

    println!("Same-repo query returned {} results", same_results.len());
    assert_eq!(
        same_results.len(),
        2,
        "Should find 2 similar entities (exact_test_1, exact_test_2) excluding source and NONE entities"
    );

    for result in &same_results {
        println!("  - {}: score={:.3}", result.stable_id, result.score);
        assert!(
            result.score > 0.0 && result.score <= 1.0,
            "Score should be between 0 and 1"
        );
    }

    // TEST 4: EXACT External-Repo Query from similarity worker (line 746-752)
    println!("\n=== TEST 4: External-Repo Similarity Query ===");

    // Create external repo entities
    conn.query(
        r#"CREATE entity_snapshot:external_1 CONTENT {
            stable_id: "external_1",
            repo_name: "other_repo",
            sourcecontrol_commit: "commits:xyz789",
            embedding_len: 768,
            embedding: [0.15, 0.25, 0.35],
            source_content: "function external() { return 99; }"
        };"#,
    )
    .await?;

    conn.query(
        r#"CREATE entity_snapshot:external_2 CONTENT {
            stable_id: "external_2",
            repo_name: "other_repo",
            sourcecontrol_commit: "commits:xyz789",
            embedding_len: 768,
            embedding: [0.2, 0.3, 0.4]
        };"#,
    )
    .await?;

    let max_external = 50;

    let external_sql = format!(
        "LET $source_embedding = (SELECT embedding FROM entity_snapshot WHERE stable_id = $source_stable_id LIMIT 1)[0].embedding; \
         SELECT id, stable_id, repo_name, sourcecontrol_commit, vector::similarity::cosine(embedding, $source_embedding) AS score \
         FROM entity_snapshot \
         WHERE embedding_len > 0 AND repo_name != $repo AND source_content != NONE \
         ORDER BY score DESC \
         LIMIT {}",
        max_external
    );

    let external_resp = conn
        .query_with_binds(
            &external_sql,
            vec![
                ("source_stable_id", serde_json::json!("exact_test_0")),
                ("repo", serde_json::json!(repo_name)),
            ],
        )
        .await?;

    // Use EXACT struct from similarity worker (line 770-776)
    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct ExternalRow {
        id: surrealdb::sql::Thing, // Use Thing type instead of serde_json::Value
        stable_id: String,
        repo_name: Option<String>,
        sourcecontrol_commit: Option<String>,
        score: f32,
    }

    let mut resp = external_resp;
    let external_results: Vec<ExternalRow> = resp.take(1)?; // LET is slot 0, SELECT is slot 1

    println!(
        "External-repo query returned {} results",
        external_results.len()
    );
    assert!(
        external_results.len() >= 1,
        "Should find at least external_1 (external_2 has NONE source_content)"
    );

    let external_1_found = external_results.iter().any(|r| r.stable_id == "external_1");
    assert!(external_1_found, "Should find external_1");

    let external_2_found = external_results.iter().any(|r| r.stable_id == "external_2");
    assert!(
        !external_2_found,
        "Should NOT find external_2 (has NONE source_content)"
    );

    for result in &external_results {
        println!(
            "  - {} ({}): score={:.3}",
            result.stable_id,
            result.repo_name.as_deref().unwrap_or("?"),
            result.score
        );
    }

    // TEST 5: Document the IS NOT NULL quirk (prove it's wrong)
    println!("\n=== TEST 5: IS NOT NULL Quirk ===");
    let wrong_query = format!(
        "SELECT COUNT() FROM entity_snapshot \
         WHERE repo_name = \"{}\" AND sourcecontrol_commit = \"{}\" AND embedding_len > 0 AND source_content IS NOT NULL \
         GROUP ALL",
        repo_name.replace("\"", "\\\""),
        commit_ref.replace("\"", "\\\"")
    );

    let resp = conn.query(&wrong_query).await?;
    let rows_opt = hyperzoekt::db::helpers::response_to_json(resp);

    if let Some(rows) = rows_opt {
        if let Some(rows_array) = rows.as_array() {
            if !rows_array.is_empty() {
                if let Some(count) = rows_array[0].get("count").and_then(|v| v.as_i64()) {
                    println!(
                        "IS NOT NULL incorrectly returns: {} (should be 3, not 5)",
                        count
                    );
                    assert_eq!(
                        count, 5,
                        "SurrealDB quirk: IS NOT NULL incorrectly returns ALL 5 records including NONE"
                    );
                }
            }
        }
    }

    // Clean up
    conn.query(&format!(
        "DELETE entity_snapshot WHERE repo_name = '{}';",
        repo_name
    ))
    .await?;
    conn.query("DELETE entity_snapshot WHERE repo_name = 'other_repo';")
        .await?;

    println!("\n✅ All tests passed!");
    Ok(())
}

#[tokio::test]
#[ignore = "Requires manual SurrealDB setup on port 8000"]
async fn test_similarity_worker_min_store_logic() -> Result<(), Box<dyn std::error::Error>> {
    // Test the min_store feature that ensures at least N relations are stored
    let conn = hz_connect(
        &Some("http://127.0.0.1:8000".to_string()),
        &Some("root".to_string()),
        &Some("root".to_string()),
        "test_ns",
        "test_db",
    )
    .await?;

    let repo_name = "test_min_store";
    let commit_ref = "commits:min_store_test";

    // Clean up
    conn.query(&format!(
        "DELETE entity_snapshot WHERE repo_name = '{}';",
        repo_name
    ))
    .await?;

    // Create entities with varying similarity scores
    // Source entity
    conn.query(&format!(
        r#"CREATE entity_snapshot:min_store_source CONTENT {{
            stable_id: "min_store_source",
            repo_name: "{}",
            sourcecontrol_commit: "{}",
            embedding_len: 768,
            embedding: [1.0, 0.0, 0.0],
            source_content: "source entity"
        }};"#,
        repo_name, commit_ref
    ))
    .await?;

    // Create 3 similar entities with decreasing similarity
    // high_sim: very similar (score ~0.99)
    conn.query(&format!(
        r#"CREATE entity_snapshot:high_sim CONTENT {{
            stable_id: "high_sim",
            repo_name: "{}",
            sourcecontrol_commit: "{}",
            embedding_len: 768,
            embedding: [0.99, 0.01, 0.0],
            source_content: "high similarity"
        }};"#,
        repo_name, commit_ref
    ))
    .await?;

    // medium_sim: somewhat similar (score ~0.70)
    conn.query(&format!(
        r#"CREATE entity_snapshot:medium_sim CONTENT {{
            stable_id: "medium_sim",
            repo_name: "{}",
            sourcecontrol_commit: "{}",
            embedding_len: 768,
            embedding: [0.7, 0.7, 0.0],
            source_content: "medium similarity"
        }};"#,
        repo_name, commit_ref
    ))
    .await?;

    // low_sim: low similarity (score ~0.5)
    conn.query(&format!(
        r#"CREATE entity_snapshot:low_sim CONTENT {{
            stable_id: "low_sim",
            repo_name: "{}",
            sourcecontrol_commit: "{}",
            embedding_len: 768,
            embedding: [0.5, 0.5, 0.5],
            source_content: "low similarity"
        }};"#,
        repo_name, commit_ref
    ))
    .await?;

    // Query with high threshold (0.75) - should get sorted results
    let same_repo_sql = format!(
        "LET $source_embedding = (SELECT embedding FROM entity_snapshot WHERE stable_id = $source_stable_id LIMIT 1)[0].embedding; \
         SELECT id, stable_id, sourcecontrol_commit, vector::similarity::cosine(embedding, $source_embedding) AS score \
         FROM entity_snapshot \
         WHERE embedding_len > 0 AND repo_name = $repo AND sourcecontrol_commit = $commit AND stable_id != $source_stable_id AND source_content != NONE \
         ORDER BY score DESC \
         LIMIT 25"
    );

    let resp = conn
        .query_with_binds(
            &same_repo_sql,
            vec![
                ("source_stable_id", serde_json::json!("min_store_source")),
                ("repo", serde_json::json!(repo_name)),
                ("commit", serde_json::json!(commit_ref)),
            ],
        )
        .await?;

    #[derive(serde::Deserialize, Debug)]
    #[allow(dead_code)]
    struct SimilarRow {
        id: surrealdb::sql::Thing,
        stable_id: String,
        sourcecontrol_commit: String,
        score: f32,
    }

    let mut resp_mut = resp;
    let results: Vec<SimilarRow> = resp_mut.take(1)?;

    println!("\n=== Min Store Logic Test ===");
    println!("Retrieved {} candidates", results.len());

    assert_eq!(results.len(), 3, "Should find all 3 similar entities");

    // Verify scores are in descending order
    for (i, result) in results.iter().enumerate() {
        println!(
            "  {}. {}: score={:.3}",
            i + 1,
            result.stable_id,
            result.score
        );
        if i > 0 {
            assert!(
                result.score <= results[i - 1].score,
                "Results should be ordered by score DESC"
            );
        }
    }

    // Simulate min_store logic from similarity worker (line 657-660)
    let threshold_same = 0.75;
    let min_store = 1;

    let above_threshold: Vec<_> = results
        .iter()
        .filter(|r| r.score >= threshold_same)
        .collect();

    println!("\nWith threshold={:.2}:", threshold_same);
    println!("  - Above threshold: {}", above_threshold.len());
    println!("  - Min store: {}", min_store);

    // Count how many would be stored with min_store logic
    let mut stored_count = 0;
    for result in &results {
        if result.score < threshold_same && stored_count >= min_store {
            println!(
                "  - Skipping {} (score={:.3}, below threshold and min_store reached)",
                result.stable_id, result.score
            );
            continue;
        }
        stored_count += 1;
        println!(
            "  - Would store {} (score={:.3})",
            result.stable_id, result.score
        );
    }

    assert!(
        stored_count >= min_store,
        "Should store at least min_store={} relations",
        min_store
    );

    // With min_store=1, we should store: high_sim (above threshold) + medium_sim (to meet min_store)
    // But since high_sim is already above threshold, we get high_sim only
    // Actually with min_store=1 and one entity above threshold, we store just that one

    // Clean up
    conn.query(&format!(
        "DELETE entity_snapshot WHERE repo_name = '{}';",
        repo_name
    ))
    .await?;

    println!("\n✅ Min store logic test passed!");
    Ok(())
}
