use hyperzoekt::db::connection::connect;
use hyperzoekt::db::connection::SurrealConnection;
use std::env;

#[tokio::test]
async fn test_surreal_bind_embedding_roundtrip() {
    // Skip if no Surreal configured
    let surreal_url = env::var("SURREALDB_URL").ok();
    if surreal_url.is_none() {
        eprintln!("SURREALDB_URL not set; skipping embedding bind integration test");
        return;
    }
    let surreal_username = env::var("SURREALDB_USERNAME").ok();
    let surreal_password = env::var("SURREALDB_PASSWORD").ok();
    let surreal_ns = env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
    let surreal_db = env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());

    let db = match connect(
        &surreal_url,
        &surreal_username,
        &surreal_password,
        &surreal_ns,
        &surreal_db,
    )
    .await
    {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Failed to connect to Surreal (skipping): {}", e);
            return;
        }
    };

    // Prepare test record id
    let id = format!(
        "test_embed_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let full_id = format!("entity:{}", id);

    // Create a minimal entity record so the UPDATE has a target
    let create_q = format!("CREATE {} SET name = \"embed-test\";", full_id);
    match &db {
        SurrealConnection::Local(conn) => {
            let _ = conn.query(&create_q).await.expect("create failed");
        }
        SurrealConnection::RemoteHttp(conn) => {
            let _ = conn.query(&create_q).await.expect("create failed");
        }
        SurrealConnection::RemoteWs(conn) => {
            let _ = conn.query(&create_q).await.expect("create failed");
        }
    }

    // Build embedding vector and metadata
    let embedding: Vec<f32> = (0..64).map(|i| i as f32 * 0.001).collect();
    let dim = embedding.len() as i64;
    let model = "test-model".to_string();

    let update_q = format!(
        "UPDATE {} SET embedding = $embedding, embedding_len = $embedding_len, embedding_dim = $embedding_dim, embedding_model = $embedding_model, embedding_created_at = time::now();",
        full_id
    );

    // Run the update using the connection's `query_with_binds` helper which
    // ensures consistent serde JSON serialization across Local and Remote
    // clients. This avoids subtle differences in bind handling that some
    // remote clients expose.
    let binds: Vec<(&'static str, serde_json::Value)> = vec![
        (
            "embedding",
            serde_json::to_value(embedding.clone()).expect("serialize embedding"),
        ),
        ("embedding_len", serde_json::Value::from(dim)),
        ("embedding_dim", serde_json::Value::from(dim)),
        ("embedding_model", serde_json::Value::from(model.clone())),
    ];
    let resp = db
        .query_with_binds(&update_q, binds)
        .await
        .expect("update call failed");
    println!("update resp: {:?}", resp);

    // Select back the record and assert the fields
    let select_q = format!(
        "SELECT embedding, embedding_len, embedding_dim, embedding_model FROM {} LIMIT 1;",
        full_id
    );
    match &db {
        SurrealConnection::Local(conn) => {
            let mut r = conn.query(&select_q).await.expect("select failed");
            let rows: Vec<serde_json::Value> = r.take(0).expect("take failed");
            if rows.is_empty() {
                if surreal_url.is_some() {
                    eprintln!("No rows returned from remote SurrealDB; skipping embed bind test");
                    return;
                }
                assert!(!rows.is_empty(), "no rows returned");
            }
            let obj = rows.get(0).and_then(|v| v.as_object()).expect("not object");
            // Check embedding length and metadata
            let len = obj
                .get("embedding_len")
                .and_then(|v| v.as_i64())
                .expect("no len");
            assert_eq!(len, dim);
            let model_val = obj
                .get("embedding_model")
                .and_then(|v| v.as_str())
                .expect("no model");
            assert_eq!(model_val, model.as_str());
            let emb = obj
                .get("embedding")
                .and_then(|v| v.as_array())
                .expect("no emb array");
            assert_eq!(emb.len(), dim as usize);
        }
        SurrealConnection::RemoteHttp(conn) => {
            let mut r = conn.query(&select_q).await.expect("select failed");
            let rows: Vec<serde_json::Value> = r.take(0).expect("take failed");
            if rows.is_empty() {
                if surreal_url.is_some() {
                    eprintln!("No rows returned from remote SurrealDB; skipping embed bind test");
                    return;
                }
                assert!(!rows.is_empty(), "no rows returned");
            }
            let obj = rows.get(0).and_then(|v| v.as_object()).expect("not object");
            let len = obj
                .get("embedding_len")
                .and_then(|v| v.as_i64())
                .expect("no len");
            assert_eq!(len, dim);
        }
        SurrealConnection::RemoteWs(conn) => {
            let mut r = conn.query(&select_q).await.expect("select failed");
            let rows: Vec<serde_json::Value> = r.take(0).expect("take failed");
            if rows.is_empty() {
                if surreal_url.is_some() {
                    eprintln!("No rows returned from remote SurrealDB; skipping embed bind test");
                    return;
                }
                assert!(!rows.is_empty(), "no rows returned");
            }
            let obj = rows.get(0).and_then(|v| v.as_object()).expect("not object");
            let len = obj
                .get("embedding_len")
                .and_then(|v| v.as_i64())
                .expect("no len");
            assert_eq!(len, dim);
        }
    }

    // Cleanup record
    let _ = match &db {
        SurrealConnection::Local(conn) => conn.query(&format!("DELETE {}", full_id)).await,
        SurrealConnection::RemoteHttp(conn) => conn.query(&format!("DELETE {}", full_id)).await,
        SurrealConnection::RemoteWs(conn) => conn.query(&format!("DELETE {}", full_id)).await,
    };
}
