use anyhow::Result;
/// Tests for similarity worker SurrealDB queries
/// These tests validate query syntax before deployment
use hyperzoekt::db::connection::connect;

#[tokio::test]
async fn test_same_repo_similarity_query_syntax() -> Result<()> {
    // Force using embedded in-memory SurrealDB for test isolation even when
    // SURREALDB_URL is set in the environment (e.g. local dev or CI).
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    // Create in-memory SurrealDB instance
    let conn = connect(&None, &None, &None, "testns", "testdb").await?;

    // Define the entity_snapshot table
    conn.query(
        "DEFINE TABLE entity_snapshot SCHEMAFULL;
         DEFINE FIELD stable_id ON entity_snapshot TYPE string;
         DEFINE FIELD embedding ON entity_snapshot TYPE array;
         DEFINE FIELD embedding_len ON entity_snapshot TYPE int;
         DEFINE FIELD repo_name ON entity_snapshot TYPE string;
         DEFINE FIELD sourcecontrol_commit ON entity_snapshot TYPE string;
         DEFINE INDEX idx_stable_id ON entity_snapshot FIELDS stable_id UNIQUE;
         DEFINE TABLE similar_to TYPE RELATION;
         DEFINE FIELD score ON similar_to TYPE float;
         DEFINE FIELD relation_type ON similar_to TYPE string;",
    )
    .await?;

    // Create test entities with embeddings
    let test_embedding = vec![0.1f32; 768];
    conn.query_with_binds(
        "CREATE entity_snapshot:test1 SET stable_id = 'test1', embedding = $emb, embedding_len = 768, repo_name = 'test-repo', sourcecontrol_commit = 'commits:abc123';
         CREATE entity_snapshot:test2 SET stable_id = 'test2', embedding = $emb, embedding_len = 768, repo_name = 'test-repo', sourcecontrol_commit = 'commits:abc123';
         CREATE entity_snapshot:test3 SET stable_id = 'test3', embedding = $emb, embedding_len = 768, repo_name = 'test-repo', sourcecontrol_commit = 'commits:abc123';",
        vec![("emb", serde_json::json!(test_embedding))]
    ).await?;

    // Test the same-repo similarity query (matches production code)
    let max_same = 25;
    let same_repo_sql = format!(
        "LET $source_embedding = (SELECT embedding FROM entity_snapshot WHERE stable_id = $source_stable_id LIMIT 1)[0].embedding; \
         LET $candidates = (SELECT id, stable_id, sourcecontrol_commit, vector::similarity::cosine(embedding, $source_embedding) AS score \
                            FROM entity_snapshot \
                            WHERE embedding_len > 0 AND repo_name = $repo AND sourcecontrol_commit = $commit AND stable_id != $source_stable_id \
                            ORDER BY score DESC \
                            LIMIT {}); \
         FOR $candidate IN $candidates {{ \
           IF $candidate.score >= $threshold {{ \
             LET $target_id = $candidate.id; \
             LET $target_score = $candidate.score; \
             RELATE $source_id->similar_to->$target_id SET score = $target_score, relation_type = 'same_repo'; \
           }} \
         }}; \
         RETURN $candidates;",
        max_same
    );

    // Execute the query - should not return parse error
    let result = conn
        .query_with_binds(
            &same_repo_sql,
            vec![
                ("source_stable_id", serde_json::json!("test1")),
                ("source_id", serde_json::json!("entity_snapshot:test1")),
                ("repo", serde_json::json!("test-repo")),
                ("commit", serde_json::json!("commits:abc123")),
                ("threshold", serde_json::json!(0.80)),
            ],
        )
        .await;

    // Should succeed (not return parse error)
    assert!(
        result.is_ok(),
        "Same-repo query should parse successfully: {:?}",
        result.err()
    );

    Ok(())
}

#[tokio::test]
async fn test_external_repo_similarity_query_syntax() -> Result<()> {
    // Force using embedded in-memory SurrealDB for test isolation even when
    // SURREALDB_URL is set in the environment (e.g. local dev or CI).
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    // Create in-memory SurrealDB instance
    let conn = connect(&None, &None, &None, "testns", "testdb").await?;

    // Define tables
    conn.query(
        "DEFINE TABLE entity_snapshot SCHEMAFULL;
         DEFINE FIELD stable_id ON entity_snapshot TYPE string;
         DEFINE FIELD embedding ON entity_snapshot TYPE array;
         DEFINE FIELD embedding_len ON entity_snapshot TYPE int;
         DEFINE FIELD repo_name ON entity_snapshot TYPE string;
         DEFINE FIELD sourcecontrol_commit ON entity_snapshot TYPE string;
         DEFINE INDEX idx_stable_id ON entity_snapshot FIELDS stable_id UNIQUE;
         DEFINE TABLE similar_to TYPE RELATION;
         DEFINE FIELD score ON similar_to TYPE float;
         DEFINE FIELD relation_type ON similar_to TYPE string;
         DEFINE TABLE sourcecontrol_commit SCHEMAFULL;
         DEFINE FIELD repo_name ON sourcecontrol_commit TYPE string;
         DEFINE FIELD commit_sha ON sourcecontrol_commit TYPE string;
         DEFINE FIELD is_default_branch ON sourcecontrol_commit TYPE bool;",
    )
    .await?;

    // Create test entities
    let test_embedding = vec![0.1f32; 768];
    conn.query_with_binds(
        "CREATE entity_snapshot:test1 SET stable_id = 'test1', embedding = $emb, embedding_len = 768, repo_name = 'repo1', sourcecontrol_commit = 'commits:abc123';
         CREATE entity_snapshot:test2 SET stable_id = 'test2', embedding = $emb, embedding_len = 768, repo_name = 'repo2', sourcecontrol_commit = 'commits:def456';
         CREATE sourcecontrol_commit:1 SET repo_name = 'repo2', commit_sha = 'commits:def456', is_default_branch = true;",
        vec![("emb", serde_json::json!(test_embedding))]
    ).await?;

    // Test the external-repo similarity query (matches production code)
    let max_external = 50;
    let external_sql = format!(
        r#"
        LET $source_embedding = (SELECT embedding FROM entity_snapshot WHERE stable_id = $source_stable_id LIMIT 1)[0].embedding;
        LET $candidates = (
            SELECT id, stable_id, repo_name, sourcecontrol_commit,
                   vector::similarity::cosine(embedding, $source_embedding) AS score
            FROM entity_snapshot
            WHERE embedding_len > 0 AND repo_name != $repo
            ORDER BY score DESC
            LIMIT {max_external}
        );
        FOR $candidate IN $candidates {{
            IF $candidate.score >= $threshold_external {{
                LET $is_default = (
                    SELECT VALUE is_default_branch 
                    FROM sourcecontrol_commit 
                    WHERE repo_name = $candidate.repo_name 
                      AND commit_sha = $candidate.sourcecontrol_commit 
                    LIMIT 1
                )[0] ?? false;
                IF $is_default {{
                    LET $target_id = $candidate.id;
                    LET $target_score = $candidate.score;
                    RELATE $source_id->similar_to->$target_id 
                        SET score = $target_score, 
                            relation_type = 'external_repo'
                }}
            }}
        }};
        RETURN $candidates;
        "#
    );

    // Execute the query - should not return parse error
    let result = conn
        .query_with_binds(
            &external_sql,
            vec![
                ("source_id", serde_json::json!("entity_snapshot:test1")),
                ("source_stable_id", serde_json::json!("test1")),
                ("repo", serde_json::json!("repo1")),
                ("threshold_external", serde_json::json!(0.85)),
            ],
        )
        .await;

    // Should succeed (not return parse error)
    assert!(
        result.is_ok(),
        "External-repo query should parse successfully: {:?}",
        result.err()
    );

    Ok(())
}

#[tokio::test]
#[ignore = "Deprecated - see similarity_worker_exact_queries.rs for comprehensive tests"]
async fn test_same_repo_similarity_creates_relations() -> Result<()> {
    // Create in-memory SurrealDB instance
    let conn = connect(&None, &None, &None, "testns", "testdb").await?;

    // Define tables
    conn.query(
        "DEFINE TABLE entity_snapshot SCHEMAFULL;
         DEFINE FIELD stable_id ON entity_snapshot TYPE string;
         DEFINE FIELD embedding ON entity_snapshot TYPE array;
         DEFINE FIELD embedding_len ON entity_snapshot TYPE int;
         DEFINE FIELD repo_name ON entity_snapshot TYPE string;
         DEFINE FIELD sourcecontrol_commit ON entity_snapshot TYPE string;
         DEFINE TABLE similar_to TYPE RELATION;
         DEFINE FIELD score ON similar_to TYPE float;
         DEFINE FIELD relation_type ON similar_to TYPE string;",
    )
    .await?;

    // Create test entities with similar embeddings (use small vectors to avoid NaN)
    let emb1 = vec![0.1f32, 0.2, 0.3];
    let emb2 = vec![0.11f32, 0.21, 0.31]; // Very similar

    conn.query_with_binds(
        "CREATE entity_snapshot:test1 SET stable_id = 'test1', embedding = $emb1, embedding_len = 3, repo_name = 'test-repo', sourcecontrol_commit = 'commits:abc123';
         CREATE entity_snapshot:test2 SET stable_id = 'test2', embedding = $emb2, embedding_len = 3, repo_name = 'test-repo', sourcecontrol_commit = 'commits:abc123';",
        vec![
            ("emb1", serde_json::json!(emb1)),
            ("emb2", serde_json::json!(emb2)),
        ]
    ).await?;

    // Execute same-repo query using the CORRECT approach (LET + SELECT, then Rust creates relations)
    let max_same = 25;
    let same_repo_sql = format!(
        "LET $source_embedding = (SELECT embedding FROM entity_snapshot WHERE stable_id = $source_stable_id LIMIT 1)[0].embedding; \
         SELECT id, stable_id, sourcecontrol_commit, vector::similarity::cosine(embedding, $source_embedding) AS score \
         FROM entity_snapshot \
         WHERE embedding_len > 0 AND embedding != NONE AND repo_name = $repo AND sourcecontrol_commit = $commit AND stable_id != $source_stable_id \
         ORDER BY score DESC \
         LIMIT {};",
        max_same
    );

    let mut same_resp = conn
        .query_with_binds(
            &same_repo_sql,
            vec![
                ("source_stable_id", serde_json::json!("test1")),
                ("repo", serde_json::json!("test-repo")),
                ("commit", serde_json::json!("commits:abc123")),
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

    // LET is slot 0, SELECT is slot 1
    let same_results: Vec<SimilarRow> = same_resp.take(1)?;

    // Now create relation in Rust (this is how the worker actually does it)
    for result in &same_results {
        if result.score >= 0.80 {
            let target_id_str = result.id.to_string();
            let relate_sql = format!(
                "RELATE entity_snapshot:test1->similar_to->{} SET score = $score, relation_type = 'same_repo'",
                target_id_str
            );
            conn.query_with_binds(
                &relate_sql,
                vec![("score", serde_json::json!(result.score))],
            )
            .await?;
        }
    }

    // Verify relation was created
    let mut check_resp = conn.query("SELECT * FROM similar_to").await?;

    #[derive(serde::Deserialize, Debug)]
    struct RelationRow {
        score: f32,
        relation_type: String,
    }

    let relations: Vec<RelationRow> = check_resp.take(0)?;

    assert!(!relations.is_empty(), "Should create at least one relation");
    assert_eq!(relations[0].relation_type, "same_repo");
    assert!(
        relations[0].score >= 0.80,
        "Score should be above threshold"
    );

    Ok(())
}
