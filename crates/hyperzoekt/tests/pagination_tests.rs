// Copyright 2025 HyperZoekt Project

use hyperzoekt::db::connection::connect as hz_connect;
use hyperzoekt::db::connection::SurrealConnection;
use std::sync::Arc;

#[derive(Clone)]
struct TestDb {
    db: Arc<SurrealConnection>,
}

impl TestDb {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
        std::env::set_var("HZ_EPHEMERAL_MEM", "1");
        let conn = hz_connect(&None, &None, &None, "testns", "testdb").await?;
        conn.use_ns("test").await?;
        conn.use_db("test").await?;
        Ok(TestDb { db: Arc::new(conn) })
    }

    pub async fn insert_entities(&self, n: usize) -> Result<(), Box<dyn std::error::Error>> {
        for i in 0..n {
            let stable = format!("e{}", i);
            let name = format!("func_{}", i);
            let repo = if i % 2 == 0 { "r1" } else { "r2" };
            let file = format!("/{}//file{}.rs", repo, i);
            let q = format!(
                "CREATE entity CONTENT {{ stable_id: \"{}\", name: \"{}\", repo_name: \"{}\", signature: \"sig{}\", file: \"{}\", language: \"rust\", kind: \"function\", parent: null, start_line: 1, end_line: 2, calls: [], doc: null, rank: 0.5, imports: [], unresolved_imports: [] }}",
                stable, name, repo, i, file
            );
            self.db.query(&q).await?;
        }
        Ok(())
    }

    pub async fn insert_dupe_edges(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Create a few entities and add rows to similar_same_repo table
        // We'll create 6 edges so pagination of 2 items per page can be tested
        let create_entities = vec![
            r#"CREATE entity CONTENT { stable_id: "a", name: "A", repo_name: "r1", signature: "s", file: "/r1/a.rs", language: "rust", kind: "function", parent: null, start_line:1, end_line:2, calls: [], doc: null, rank: 0.1, imports: [], unresolved_imports: [] }"#,
            r#"CREATE entity CONTENT { stable_id: "b", name: "B", repo_name: "r1", signature: "s", file: "/r1/b.rs", language: "rust", kind: "function", parent: null, start_line:1, end_line:2, calls: [], doc: null, rank: 0.2, imports: [], unresolved_imports: [] }"#,
            r#"CREATE entity CONTENT { stable_id: "c", name: "C", repo_name: "r1", signature: "s", file: "/r1/c.rs", language: "rust", kind: "function", parent: null, start_line:1, end_line:2, calls: [], doc: null, rank: 0.3, imports: [], unresolved_imports: [] }"#,
        ];
        for q in create_entities.iter() {
            self.db.query(q).await?;
        }
        // Insert edges into similar_same_repo table directly as rows
        // SurrealDB allows CREATE similar_same_repo CONTENT { in: type::thing("entity:a"), out: type::thing("entity:b"), score: 0.9 }
        let edges = vec![
            ("a", "b", 0.9),
            ("a", "c", 0.8),
            ("b", "c", 0.7),
            ("c", "a", 0.6),
            ("b", "a", 0.5),
            ("c", "b", 0.4),
        ];
        for (a, b, score) in edges.iter() {
            let q = format!(
                "CREATE similar_same_repo CONTENT {{ in: type::thing(\"entity:{}\"), out: type::thing(\"entity:{}\"), score: {} }}",
                a, b, score
            );
            self.db.query(&q).await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_search_pagination() -> Result<(), Box<dyn std::error::Error>> {
    let t = TestDb::new().await?;
    t.insert_entities(10).await?;

    // Call the DB helper directly: use same SQL as production helper by doing a query
    // Page size 3
    let sql0 = format!(
        "SELECT file, start_line, name FROM entity START AT {} LIMIT {}",
        0, 3
    );
    println!("SQL page0: {}", sql0);
    let mut page0 = t.db.query_with_binds(&sql0, vec![]).await?;
    let rows0: Vec<serde_json::Value> = page0.take(0)?;
    assert_eq!(rows0.len(), 3);

    let sql1 = format!(
        "SELECT file, start_line, name FROM entity START AT {} LIMIT {}",
        3, 3
    );
    println!("SQL page1: {}", sql1);
    let mut page1 = t.db.query_with_binds(&sql1, vec![]).await?;
    let rows1: Vec<serde_json::Value> = page1.take(0)?;
    assert_eq!(rows1.len(), 3);

    let sql2 = format!(
        "SELECT file, start_line, name FROM entity START AT {} LIMIT {}",
        6, 3
    );
    println!("SQL page2: {}", sql2);
    let mut page2 = t.db.query_with_binds(&sql2, vec![]).await?;
    let rows2: Vec<serde_json::Value> = page2.take(0)?;
    // With 10 total and page size 3, pages at 0,3,6 return 3 items each; final page at 9 has 1
    assert_eq!(rows2.len(), 3);

    // Check final partial page at offset 9
    let sql3 = format!(
        "SELECT file, start_line, name FROM entity START AT {} LIMIT {}",
        9, 3
    );
    println!("SQL page3: {}", sql3);
    let mut page3 = t.db.query_with_binds(&sql3, vec![]).await?;
    let rows3: Vec<serde_json::Value> = page3.take(0)?;
    assert_eq!(rows3.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_dupes_pagination_helper() -> Result<(), Box<dyn std::error::Error>> {
    let t = TestDb::new().await?;
    t.insert_dupe_edges().await?;

    // Now query similar_same_repo with pagination
    let sp0 = format!(
        "SELECT score FROM similar_same_repo START AT {} LIMIT {}",
        0, 2
    );
    println!("SQL dupes p0: {}", sp0);
    let mut p0 = t.db.query_with_binds(&sp0, vec![]).await?;
    let r0: Vec<serde_json::Value> = p0.take(0)?;
    assert_eq!(r0.len(), 2);

    let sp1 = format!(
        "SELECT score FROM similar_same_repo START AT {} LIMIT {}",
        2, 2
    );
    println!("SQL dupes p1: {}", sp1);
    let mut p1 = t.db.query_with_binds(&sp1, vec![]).await?;
    let r1: Vec<serde_json::Value> = p1.take(0)?;
    assert_eq!(r1.len(), 2);

    let sp2 = format!(
        "SELECT score FROM similar_same_repo START AT {} LIMIT {}",
        4, 2
    );
    println!("SQL dupes p2: {}", sp2);
    let mut p2 = t.db.query_with_binds(&sp2, vec![]).await?;
    let r2: Vec<serde_json::Value> = p2.take(0)?;
    assert_eq!(r2.len(), 2);

    Ok(())
}
