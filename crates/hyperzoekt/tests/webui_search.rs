// Copyright 2025 HyperZoekt Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use hyperzoekt::db_writer::connection::{connect, SurrealConnection};
use hyperzoekt::repo_index::indexer::payload::EntityPayload;
use std::sync::Arc;

#[derive(Clone)]
struct TestDatabase {
    db: Arc<SurrealConnection>,
}

impl TestDatabase {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Force using embedded in-memory SurrealDB for tests even if
        // SURREALDB_URL is present in the environment. This avoids
        // accidental use of a remote DB with different schema/data.
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
        // Ensure each test gets an ephemeral in-memory instance created
        // on the current async runtime to avoid dropping a runtime that
        // services a shared Mem instance from a different test runtime.
        std::env::set_var("HZ_EPHEMERAL_MEM", "1");
        let db = connect(&None, &None, &None, "testns", "testdb").await?;
        // set a small local namespace/database for tests
        db.use_ns("test").await?;
        db.use_db("test").await?;
        Ok(Self { db: Arc::new(db) })
    }

    pub async fn search_entities(
        &self,
        query: &str,
        repo_filter: Option<&str>,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        let query_lower = query.to_lowercase();

        let fields = "file, language, kind, name, parent, signature, start_line, end_line, calls, doc, rank, imports, unresolved_imports, stable_id, repo_name, source_url, source_display";
        let sql = if let Some(_repo) = repo_filter {
            format!(
                r#"
          SELECT {fields} FROM entity
          WHERE (string::matches(string::lowercase(name ?? ''), $query)
              OR string::matches(string::lowercase(signature ?? ''), $query)
              OR string::matches(string::lowercase(file ?? ''), $query))
          AND string::starts_with(file ?? '', $repo)
                ORDER BY rank DESC LIMIT 100
            "#,
                fields = fields
            )
        } else {
            format!(
                r#"
          SELECT {fields} FROM entity
          WHERE (string::matches(string::lowercase(name ?? ''), $query)
              OR string::matches(string::lowercase(signature ?? ''), $query)
              OR string::matches(string::lowercase(file ?? ''), $query))
                ORDER BY rank DESC LIMIT 100
            "#,
                fields = fields
            )
        };

        // Build binds
        let mut binds = vec![("query", serde_json::Value::String(query_lower))];
        if let Some(repo) = repo_filter {
            binds.push(("repo", serde_json::Value::String(repo.to_string())));
        }

        let mut response = self.db.query_with_binds(&sql, binds).await?;
        let entities: Vec<EntityPayload> = response.take(0)?;
        Ok(entities)
    }

    pub async fn insert_test_data(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Insert test entities using direct SQL with all required fields
        let queries = vec![
            r#"CREATE entity CONTENT {
                stable_id: "test1",
                name: "getUserData",
                repo_name: "repo1",
                signature: "fn getUserData() -> User",
                file: "/repo1/user.rs",
                language: "rust",
                kind: "function",
                parent: null,
                start_line: 10,
                end_line: 15,
                calls: [],
                doc: null,
                rank: 0.8,
                imports: [],
                unresolved_imports: []
            }"#,
            r#"CREATE entity CONTENT {
                stable_id: "test2",
                name: "processData",
                repo_name: "repo1",
                signature: "fn processData(data: &Data)",
                file: "/repo1/data.rs",
                language: "rust",
                kind: "function",
                parent: null,
                start_line: 20,
                end_line: 25,
                calls: [],
                doc: null,
                rank: 0.7,
                imports: [],
                unresolved_imports: []
            }"#,
            r#"CREATE entity CONTENT {
                stable_id: "test3",
                name: "UserService",
                repo_name: "repo2",
                signature: "struct UserService { ... }",
                file: "/repo2/service.rs",
                language: "rust",
                kind: "struct",
                parent: null,
                start_line: 5,
                end_line: 30,
                calls: [],
                doc: null,
                rank: 0.9,
                imports: [],
                unresolved_imports: []
            }"#,
            r#"CREATE entity CONTENT {
                stable_id: "test4",
                name: "calculateTotal",
                repo_name: "repo2",
                signature: "fn calculateTotal(items: Vec<Item>) -> f64",
                file: "/repo2/math.rs",
                language: "rust",
                kind: "function",
                parent: null,
                start_line: 12,
                end_line: 18,
                calls: [],
                doc: null,
                rank: 0.6,
                imports: [],
                unresolved_imports: []
            }"#,
        ];

        for query in queries {
            self.db.query(query).await?;
        }

        Ok(())
    }
}

#[tokio::test]
async fn test_search_entities_basic() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;
    db.insert_test_data().await?;

    // Test basic search
    let results = db.search_entities("get", None).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "getUserData");

    Ok(())
}

#[tokio::test]
async fn test_search_entities_case_insensitive() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;
    db.insert_test_data().await?;

    // Test case insensitive search
    let results = db.search_entities("GETUSERDATA", None).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "getUserData");

    Ok(())
}

#[tokio::test]
async fn test_search_entities_multiple_results() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;
    db.insert_test_data().await?;

    // Test search that matches multiple entities
    let results = db.search_entities("data", None).await?;
    assert_eq!(results.len(), 2);

    let names: Vec<String> = results.iter().map(|e| e.name.clone()).collect();
    assert!(names.contains(&"getUserData".to_string()));
    assert!(names.contains(&"processData".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_search_entities_with_repo_filter() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;
    db.insert_test_data().await?;

    // Test search with repository filter
    let results = db.search_entities("User", Some("/repo2")).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "UserService");

    Ok(())
}

#[tokio::test]
async fn test_search_entities_no_matches() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;
    db.insert_test_data().await?;

    // Test search with no matches
    let results = db.search_entities("nonexistent", None).await?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_search_entities_in_signature() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;
    db.insert_test_data().await?;

    // Test search that matches in signature
    let results = db.search_entities("Vec", None).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "calculateTotal");

    Ok(())
}

#[tokio::test]
async fn test_search_entities_in_file_path() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;
    db.insert_test_data().await?;

    // Test search that matches in file path
    let results = db.search_entities("service.rs", None).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].name, "UserService");

    Ok(())
}

#[tokio::test]
async fn test_search_entities_sql_injection_protection() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;
    db.insert_test_data().await?;

    // Test that single quotes are properly escaped
    let results = db.search_entities("'", None).await?;
    // Should not crash and should return empty results
    assert_eq!(results.len(), 0);

    Ok(())
}
