// Copyright 2025 HyperZoekt Project

#[tokio::test]
async fn persist_and_query_has_method_relation() -> Result<(), Box<dyn std::error::Error>> {
    use hyperzoekt::db_writer::connection::connect;
    use serde_json::json;

    // Connect directly (fallback in-memory). If a remote SURREALDB_URL is present but
    // unreachable, skip the test to avoid CI flakes.
    let db = match connect(&None, &None, &None, "testns", "testdb").await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Skipping persist_and_query_has_method_relation: unable to connect to SurrealDB: {}", e);
            return Ok(());
        }
    };
    db.use_ns("test").await.ok();
    db.use_db("test").await.ok();

    // Build a parent entity payload and a method payload. Use deterministic stable ids
    // so we can reference them predictably.
    let parent_stable = "parent-stable-123".to_string();
    let parent_id = parent_stable.to_string();
    let parent_e = json!({"stable_id": parent_stable.clone(), "name": "MyClass", "language": "rust", "kind": "type"});

    // Method stable id: sha256 of parent_stable + '|' + signature
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(parent_stable.as_bytes());
    hasher.update(b"|");
    hasher.update(b"fn foo(&self)");
    let method_hash = format!("{:x}", hasher.finalize());
    let method = json!({"stable_id": method_hash.clone(), "language": "rust", "kind": "function", "name": "foo", "signature": "fn foo(&self)", "repo_name": "testrepo"});

    // Add a second method to exercise multiple-method handling
    let mut hasher2 = sha2::Sha256::new();
    hasher2.update(parent_stable.as_bytes());
    hasher2.update(b"|");
    hasher2.update(b"fn bar(&self, x: i32)");
    let method_hash2 = format!("{:x}", hasher2.finalize());
    let method2 = json!({"stable_id": method_hash2.clone(), "language": "rust", "kind": "function", "name": "bar", "signature": "fn bar(&self, x: i32)", "repo_name": "testrepo"});

    // Insert parent and method using parameterized creates with deterministic ids
    let parent_eid = sanitize(&parent_id);
    let method_eid = sanitize(&format!("m_{}", method_hash));
    let method_eid2 = sanitize(&format!("m_{}", method_hash2));
    let q_parent = format!("CREATE entity:{} CONTENT $e", parent_eid);
    db.query_with_binds(&q_parent, vec![("e", parent_e)])
        .await?;
    let q_method = format!("CREATE entity:{} CONTENT $m", method_eid);
    db.query_with_binds(&q_method, vec![("m", method)]).await?;
    let q_method2 = format!("CREATE entity:{} CONTENT $m", method_eid2);
    db.query_with_binds(&q_method2, vec![("m", method2)])
        .await?;

    // Create the relation: RELATE entity:{parent_id} -> has_method -> entity:{method_mid}
    // In this test, entity ids are deterministically created by the writer using sanitized
    // stable ids; to reference them here we will sanitize similarly: replace non-alnum with '_'
    fn sanitize(raw: &str) -> String {
        let mut out = String::new();
        let mut last_us = false;
        for ch in raw.chars() {
            if ch.is_ascii_alphanumeric() {
                out.push(ch);
                last_us = false;
            } else {
                if !last_us {
                    out.push('_');
                }
                last_us = true;
            }
        }
        let t = out.trim_matches('_').to_string();
        if t.is_empty() {
            "_".to_string()
        } else {
            t
        }
    }
    // Ensure no pre-existing duplicate edges exist for these pairs (test DB may be shared)
    let _ = db
        .query(&format!(
            "DELETE FROM has_method WHERE in = entity:{} AND out = entity:{};",
            parent_eid, method_eid
        ))
        .await;
    let _ = db
        .query(&format!(
            "DELETE FROM has_method WHERE in = entity:{} AND out = entity:{};",
            parent_eid, method_eid2
        ))
        .await;

    // Relate both methods to the parent
    let rel_q1 = format!(
        "RELATE entity:{}->has_method->entity:{};",
        parent_eid, method_eid
    );
    db.query(&rel_q1).await?;
    let rel_q2 = format!(
        "RELATE entity:{}->has_method->entity:{};",
        parent_eid, method_eid2
    );
    db.query(&rel_q2).await?;

    // Query has_method table to ensure relation exists
    // Ensure at least two has_method relations exist. Use GROUP ALL for a single aggregate row
    let mut resp = db
        .query("SELECT count() AS c FROM has_method GROUP ALL;")
        .await?;
    let rows: Vec<serde_json::Value> = resp.take(0).unwrap_or_default();
    let total = rows
        .first()
        .and_then(|o| {
            o.get("c")
                .or_else(|| o.get("count"))
                .and_then(|v| v.as_i64())
        })
        .unwrap_or(0);
    assert!(
        total >= 2,
        "expected at least 2 has_method relations, got {}",
        total
    );

    // Verify each method row contains its stable_id field
    let sel1 = format!("SELECT stable_id FROM entity:{}", method_eid);
    let r1 = db.query(&sel1).await?;
    let r1s = format!("{:?}", r1);
    assert!(r1s.contains(&method_hash));

    let sel2 = format!("SELECT stable_id FROM entity:{}", method_eid2);
    let r2 = db.query(&sel2).await?;
    let r2s = format!("{:?}", r2);
    assert!(r2s.contains(&method_hash2));

    // Assert uniqueness of each parent->method relation (should be exactly 1)
    let q_e1 = format!(
        "SELECT count() AS c FROM has_method WHERE in = entity:{} AND out = entity:{} GROUP ALL;",
        parent_eid, method_eid
    );
    let mut resp_e1 = db.query(&q_e1).await?;
    let rows_e1: Vec<serde_json::Value> = resp_e1.take(0).unwrap_or_default();
    let cnt_e1 = rows_e1
        .first()
        .and_then(|o| {
            o.get("c")
                .or_else(|| o.get("count"))
                .and_then(|v| v.as_i64())
        })
        .unwrap_or(0);
    assert_eq!(
        cnt_e1, 1,
        "expected exactly one has_method edge for method1"
    );

    let q_e2 = format!(
        "SELECT count() AS c FROM has_method WHERE in = entity:{} AND out = entity:{} GROUP ALL;",
        parent_eid, method_eid2
    );
    let mut resp_e2 = db.query(&q_e2).await?;
    let rows_e2: Vec<serde_json::Value> = resp_e2.take(0).unwrap_or_default();
    let cnt_e2 = rows_e2
        .first()
        .and_then(|o| {
            o.get("c")
                .or_else(|| o.get("count"))
                .and_then(|v| v.as_i64())
        })
        .unwrap_or(0);
    assert_eq!(
        cnt_e2, 1,
        "expected exactly one has_method edge for method2"
    );

    // Deeper inspection: ensure method entity payloads contain expected fields
    let sel1_full = format!(
        "SELECT stable_id, name, signature, repo_name FROM entity:{}",
        method_eid
    );
    let rf1 = db.query(&sel1_full).await?;
    let rf1s = format!("{:?}", rf1);
    assert!(
        rf1s.contains(&method_hash),
        "method1 stable_id missing in payload"
    );
    assert!(rf1s.contains("name"), "method1 name missing in payload");
    assert!(
        rf1s.contains("signature"),
        "method1 signature missing in payload"
    );
    assert!(
        rf1s.contains("repo_name"),
        "method1 repo_name missing in payload"
    );

    let sel2_full = format!(
        "SELECT stable_id, name, signature, repo_name FROM entity:{}",
        method_eid2
    );
    let rf2 = db.query(&sel2_full).await?;
    let rf2s = format!("{:?}", rf2);
    assert!(
        rf2s.contains(&method_hash2),
        "method2 stable_id missing in payload"
    );
    assert!(rf2s.contains("name"), "method2 name missing in payload");
    assert!(
        rf2s.contains("signature"),
        "method2 signature missing in payload"
    );
    assert!(
        rf2s.contains("repo_name"),
        "method2 repo_name missing in payload"
    );
    Ok(())
}
