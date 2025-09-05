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

use hyperzoekt::repo_index::indexer::payload::EntityPayload;
use surrealdb::engine::local::Mem;
use surrealdb::Surreal;

// Import the Database struct from the webui binary
// Since it's in a binary crate, we need to make it accessible for testing
// For now, we'll duplicate the relevant parts for testing

enum SurrealConnection {
    Local(Surreal<surrealdb::engine::local::Db>),
    RemoteHttp(Surreal<surrealdb::engine::remote::http::Client>),
    RemoteWs(Surreal<surrealdb::engine::remote::ws::Client>),
}

impl Clone for SurrealConnection {
    fn clone(&self) -> Self {
        match self {
            SurrealConnection::Local(db) => SurrealConnection::Local(db.clone()),
            SurrealConnection::RemoteHttp(db) => SurrealConnection::RemoteHttp(db.clone()),
            SurrealConnection::RemoteWs(db) => SurrealConnection::RemoteWs(db.clone()),
        }
    }
}

impl SurrealConnection {
    async fn query(&self, sql: &str) -> Result<surrealdb::Response, surrealdb::Error> {
        match self {
            SurrealConnection::Local(db) => db.query(sql).await,
            SurrealConnection::RemoteHttp(db) => db.query(sql).await,
            SurrealConnection::RemoteWs(db) => db.query(sql).await,
        }
    }
}

#[derive(Clone)]
struct TestDatabase {
    db: std::sync::Arc<SurrealConnection>,
}

impl TestDatabase {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let db = Surreal::new::<Mem>(()).await?;
        db.use_ns("test").use_db("test").await?;
        Ok(Self {
            db: std::sync::Arc::new(SurrealConnection::Local(db)),
        })
    }

    pub async fn search_entities(
        &self,
        query: &str,
        repo_filter: Option<&str>,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        // Escape single quotes in the query for SQL safety
        let escaped_query = query.replace("'", "\\'");

        let mut sql_query = format!(
            r#"
            SELECT * FROM entity
            WHERE (string::matches(string::lowercase(name), '{}')
                   OR string::matches(string::lowercase(signature), '{}')
                   OR string::matches(string::lowercase(file), '{}'))
        "#,
            escaped_query.to_lowercase(),
            escaped_query.to_lowercase(),
            escaped_query.to_lowercase()
        );

        if let Some(repo) = repo_filter {
            sql_query.push_str(&format!(
                " AND string::starts_with(file, '{}')",
                repo.replace("'", "\\'")
            ));
        }

        sql_query.push_str(" ORDER BY rank DESC LIMIT 100");

        let mut response = self.db.query(&sql_query).await?;
        let entities: Vec<EntityPayload> = response.take(0)?;

        Ok(entities)
    }

    pub async fn insert_test_data(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Insert test entities using direct SQL with all required fields
        let queries = vec![
            r#"CREATE entity CONTENT {
                stable_id: "test1",
                name: "getUserData",
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
