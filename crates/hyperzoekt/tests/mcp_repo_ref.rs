// Cleaned MCP-level integration tests for repo@ref and default-branch lookup
use serde::Deserialize;
use serde_json::Value;
// Tests use ephemeral Mem instances to avoid shared-state races in parallel tests.
use hyperzoekt::db::connection::connect;
use hyperzoekt::db::{create_branch, create_snapshot_meta};

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct SnapshotIdRow {
    snapshot_id: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct CommitRow {
    commit: String,
}

#[tokio::test]
async fn repo_at_ref_resolves_snapshot() {
    // Skip this test unless an external SurrealDB is configured and reachable
    if std::env::var("SURREALDB_URL").is_err() {
        println!("Skipping repo_at_ref_resolves_snapshot: no external SurrealDB configured (SURREALDB_URL not set)");
        return;
    }
    // Ensure this test gets an ephemeral Mem instance (no shared SHARED_MEM)
    std::env::set_var("HZ_EPHEMERAL_MEM", "1");
    // Use db_writer helpers via an explicit connection to mirror production code.
    let conn = match connect(&None, &None, &None, "testns", "testdb").await {
        Ok(c) => c,
        Err(e) => {
            println!(
                "Skipping repo_at_ref_resolves_snapshot: unable to connect to SURREALDB_URL: {}",
                e
            );
            return;
        }
    };

    // Create repo, a ref for 'main', and a snapshot_meta row for the target commit
    let _ = conn
        .query(r#"CREATE repo:myrepo CONTENT {"name":"myrepo","branch":"main","git_url":"https://example.com/myrepo.git"}"#)
        .await;
    // create_branch and create_snapshot_meta expect a &SurrealConnection
    create_branch(&conn, "myrepo", "main", "commit123")
        .await
        .expect("create_branch failed");
    create_snapshot_meta(
        &conn,
        "myrepo",
        "commit123",
        "snapshot:myrepo:commit123",
        None,
    )
    .await
    .expect("create_snapshot_meta failed");

    // Debug: inspect snapshot_meta for myrepo
    if let Ok(r) = conn
        .query("SELECT repo, commit, snapshot_id FROM snapshot_meta WHERE repo = 'myrepo'")
        .await
    {
        eprintln!("DEBUG raw response snapshot_meta select myrepo: {:?}", r);
    }

    // Ensure a snapshot_meta row exists for the commit we created
    match conn
        .query(&format!(
            "SELECT commit FROM snapshot_meta WHERE repo = 'myrepo' AND commit = '{}' LIMIT 1",
            "commit123"
        ))
        .await
    {
        Ok(mut r) => match r.take::<Vec<CommitRow>>(0) {
            Ok(rows) => {
                assert!(!rows.is_empty(), "snapshot_meta missing for myrepo commit");
            }
            Err(e) => panic!("failed to take snapshot_meta rows: {}", e),
        },
        Err(e) => panic!("query snapshot_meta failed: {}", e),
    }
    // (no otherrepo data is created in this test)

    // Simulate MCP ref resolution logic for repo@main
    let raw_ref = "main";
    let candidates = vec![
        raw_ref.to_string(),
        format!("refs/heads/{}", raw_ref),
        format!("refs/tags/{}", raw_ref),
    ];
    let sql = format!(
        "SELECT name, target FROM refs WHERE repo = 'myrepo' AND name IN ['{}','{}','{}']",
        candidates[0], candidates[1], candidates[2]
    );
    let mut resp = conn.query(&sql).await.expect("query refs failed");
    let mut dbg = conn.query(&sql).await.expect("query refs for debug");
    if let Ok(raw) = dbg.take::<Vec<Value>>(0) {
        eprintln!("DEBUG refs rows myrepo: {:?}", raw);
    }
    #[derive(Deserialize)]
    struct RefRow {
        name: String,
        target: String,
    }
    let rows: Vec<RefRow> = resp.take(0).unwrap_or_default();
    assert!(!rows.is_empty(), "expected ref rows");
    let chosen = rows
        .iter()
        .find(|r| r.name == raw_ref)
        .or_else(|| {
            rows.iter()
                .find(|r| r.name == format!("refs/heads/{}", raw_ref))
        })
        .or_else(|| {
            rows.iter()
                .find(|r| r.name == format!("refs/tags/{}", raw_ref))
        })
        .expect("no matching ref");

    let snap_sql = format!(
        "SELECT commit FROM snapshot_meta WHERE repo = 'myrepo' AND commit = '{}' LIMIT 1",
        chosen.target
    );
    let mut resp2 = conn
        .query(&snap_sql)
        .await
        .expect("query snapshot_meta failed");
    let srows: Vec<CommitRow> = resp2.take(0).unwrap_or_default();
    assert!(
        !srows.is_empty(),
        "expected snapshot_meta for myrepo commit"
    );
}

#[tokio::test]
async fn repo_default_branch_resolves_snapshot() {
    // Skip this test unless an external SurrealDB is configured and reachable
    if std::env::var("SURREALDB_URL").is_err() {
        println!("Skipping repo_default_branch_resolves_snapshot: no external SurrealDB configured (SURREALDB_URL not set)");
        return;
    }
    std::env::set_var("HZ_EPHEMERAL_MEM", "1");
    let conn = match connect(&None, &None, &None, "testns", "testdb").await {
        Ok(c) => c,
        Err(e) => {
            println!("Skipping repo_default_branch_resolves_snapshot: unable to connect to SURREALDB_URL: {}", e);
            return;
        }
    };
    let _ = conn
        .query(r#"CREATE repo:otherrepo CONTENT {"name":"otherrepo","branch":"main","git_url":"https://example.com/otherrepo.git"}"#)
        .await;
    create_branch(&conn, "otherrepo", "main", "commitABC")
        .await
        .expect("create_branch failed");
    create_snapshot_meta(
        &conn,
        "otherrepo",
        "commitABC",
        "snapshot:otherrepo:commitABC",
        None,
    )
    .await
    .expect("create_snapshot_meta failed");
    // Unconditional debug: inspect snapshot_meta and refs after create
    match conn.query("SELECT * FROM snapshot_meta").await {
        Ok(mut r) => match r.take::<Vec<serde_json::Value>>(0) {
            Ok(v) => eprintln!("DEBUG all snapshot_meta: {:?}", v),
            Err(e) => eprintln!("DEBUG snapshot_meta take error: {}", e),
        },
        Err(e) => eprintln!("DEBUG snapshot_meta query error: {}", e),
    }
    match conn.query("SELECT * FROM refs").await {
        Ok(mut r) => match r.take::<Vec<serde_json::Value>>(0) {
            Ok(v) => eprintln!("DEBUG all refs: {:?}", v),
            Err(e) => eprintln!("DEBUG refs take error: {}", e),
        },
        Err(e) => eprintln!("DEBUG refs query error: {}", e),
    }
    // Targeted debug: select specific fields and print raw Response
    if let Ok(r) = conn
        .query("SELECT repo, commit, snapshot_id FROM snapshot_meta WHERE repo = 'otherrepo'")
        .await
    {
        eprintln!("DEBUG raw response snapshot_meta select: {:?}", r);
    }
    match conn
        .query(&format!(
            "SELECT commit FROM snapshot_meta WHERE repo = 'otherrepo' AND commit = '{}' LIMIT 1",
            "commitABC"
        ))
        .await
    {
        Ok(mut r2) => match r2.take::<Vec<CommitRow>>(0) {
            Ok(rows2) => {
                assert!(
                    !rows2.is_empty(),
                    "snapshot_meta missing for otherrepo commit"
                );
            }
            Err(e) => panic!("failed to take snapshot_meta rows otherrepo: {}", e),
        },
        Err(e) => panic!("query snapshot_meta otherrepo failed: {}", e),
    }

    // Debug: print snapshot_meta and refs contents
    if let Ok(mut all_snap) = conn
        .query("SELECT * FROM snapshot_meta WHERE repo = 'otherrepo'")
        .await
    {
        if let Ok(v) = all_snap.take::<Vec<serde_json::Value>>(0) {
            eprintln!("DEBUG snapshot_meta otherrepo: {:?}", v);
        }
    }
    if let Ok(mut all_refs) = conn
        .query("SELECT * FROM refs WHERE repo = 'otherrepo'")
        .await
    {
        if let Ok(v2) = all_refs.take::<Vec<serde_json::Value>>(0) {
            eprintln!("DEBUG refs otherrepo: {:?}", v2);
        }
    }

    let sql = "SELECT branch FROM repo WHERE name = 'otherrepo' LIMIT 1".to_string();
    let mut resp = conn.query(&sql).await.expect("query repo failed");
    #[derive(Deserialize)]
    struct BranchRow {
        branch: Option<String>,
    }
    let bro: Vec<BranchRow> = resp.take(0).unwrap_or_default();
    let branch = bro
        .into_iter()
        .next()
        .and_then(|r| r.branch)
        .expect("branch missing");

    let candidates2 = vec![
        branch.clone(),
        format!("refs/heads/{}", branch),
        format!("refs/tags/{}", branch),
    ];
    let sql2 = format!(
        "SELECT name, target FROM refs WHERE repo = 'otherrepo' AND name IN ['{}','{}','{}']",
        candidates2[0], candidates2[1], candidates2[2]
    );
    let mut resp3 = conn.query(&sql2).await.expect("query refs2 failed");
    let mut dbg2 = conn.query(&sql2).await.expect("query refs2 for debug");
    if let Ok(raw2) = dbg2.take::<Vec<Value>>(0) {
        eprintln!("DEBUG refs rows otherrepo: {:?}", raw2);
    }
    #[derive(Deserialize)]
    struct RefRow2 {
        name: String,
        target: String,
    }
    let rr: Vec<RefRow2> = resp3.take(0).unwrap_or_default();
    assert!(!rr.is_empty(), "expected ref rows for branch");
    let chosen = rr
        .iter()
        .find(|r| r.name == branch)
        .or_else(|| {
            rr.iter()
                .find(|r| r.name == format!("refs/heads/{}", branch))
        })
        .or_else(|| {
            rr.iter()
                .find(|r| r.name == format!("refs/tags/{}", branch))
        })
        .expect("no matching ref for branch");

    let snap_sql2 = format!(
        "SELECT commit FROM snapshot_meta WHERE repo = 'otherrepo' AND commit = '{}' LIMIT 1",
        chosen.target
    );
    let mut resp4 = conn
        .query(&snap_sql2)
        .await
        .expect("query snapshot_meta2 failed");
    let srows2: Vec<CommitRow> = resp4.take(0).unwrap_or_default();
    assert!(
        !srows2.is_empty(),
        "expected snapshot_meta for otherrepo commit"
    );
}
