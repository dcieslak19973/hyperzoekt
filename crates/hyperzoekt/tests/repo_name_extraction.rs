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

use serde::{Deserialize, Serialize};
use serial_test::serial;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoSummary {
    pub name: String,
    pub entity_count: u64,
    pub file_count: u64,
    pub languages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepoSummaryQueryResult {
    pub repo_name: String,
    pub entity_count: u64,
    // Accept optional values returned by SurrealDB and filter None when mapping
    pub files: Vec<Option<String>>,
    pub languages: Vec<Option<String>>,
}

#[derive(Clone)]
struct TestDatabase {
    db: std::sync::Arc<hyperzoekt::db_writer::connection::SurrealConnection>,
}

impl TestDatabase {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        use hyperzoekt::db_writer::connection::connect;
        // Ensure tests use the embedded in-memory SurrealDB even if
        // SURREALDB_URL is set in the environment. Use an ephemeral Mem
        // instance per-test to avoid shared state across concurrently or
        // sequentially running tests.
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
        std::env::set_var("HZ_EPHEMERAL_MEM", "1");
        let conn = connect(&None, &None, &None, "testns", "testdb").await?;
        conn.use_ns("test").await?;
        conn.use_db("test").await?;
        Ok(Self {
            db: std::sync::Arc::new(conn),
        })
    }

    pub async fn get_repo_summaries(&self) -> Result<Vec<RepoSummary>, Box<dyn std::error::Error>> {
        // Use the stored repo_name field instead of parsing from file paths
        // Handle cases where repo_name might be null for existing data
        let query = r#"
            SELECT
                repo_name ?? 'unknown' as repo_name,
                count() as entity_count,
                array::distinct(source_display) as files,
                array::distinct(language) as languages
            FROM entity
            GROUP BY repo_name
            ORDER BY entity_count DESC
        "#;

        let mut response = self.db.query(query).await?;
        let query_results: Vec<RepoSummaryQueryResult> = response.take(0)?;

        Ok(query_results
            .into_iter()
            .map(|s| {
                let files: Vec<String> = s.files.into_iter().flatten().collect();
                let languages: Vec<String> = s.languages.into_iter().flatten().collect();
                let file_count = files.len() as u64; // Count distinct files
                RepoSummary {
                    name: s.repo_name,
                    entity_count: s.entity_count,
                    file_count,
                    languages,
                }
            })
            .collect())
    }

    pub async fn insert_test_entities(
        &self,
        entities: Vec<TestEntity>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for entity in entities {
            let query = format!(
                r#"
                CREATE entity CONTENT {{
                    stable_id: "{}",
                    name: "{}",
                    signature: "{}",
                        source_display: "{}",
                    language: "{}",
                    kind: "{}",
                    parent: null,
                    start_line: {},
                    end_line: {},
                    calls: [],
                    doc: null,
                    rank: {},
                    imports: [],
                    unresolved_imports: [],
                    repo_name: "{}"
                }}
            "#,
                entity.stable_id,
                entity.name,
                entity.signature,
                entity.source_display,
                entity.language,
                entity.kind,
                entity.start_line,
                entity.end_line,
                entity.rank,
                entity.repo_name
            );

            self.db.query(&query).await?;
        }

        Ok(())
    }
}

#[derive(Clone)]
struct TestEntity {
    stable_id: String,
    name: String,
    signature: String,
    source_display: String,
    language: String,
    kind: String,
    start_line: usize,
    end_line: usize,
    rank: f32,
    repo_name: String,
}

#[serial]
#[tokio::test]
async fn test_get_repo_summaries_handles_null_array_elements(
) -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;

    // Ensure clean state
    db.db.query("DELETE FROM entity").await?;

    // Insert entities, some with null language/source_display to simulate older/dirty data
    let q1 = r#"CREATE entity CONTENT {
        stable_id: "n1",
        name: "N1",
        signature: "fn n1()",
        source_display: null,
        language: null,
        kind: "function",
        parent: null,
        start_line: 1,
        end_line: 2,
        calls: [],
        doc: null,
        rank: 0.1,
        imports: [],
        unresolved_imports: [],
        repo_name: "null-repo"
    }"#;

    let q2 = r#"CREATE entity CONTENT {
        stable_id: "n2",
        name: "N2",
        signature: "fn n2()",
        source_display: "/tmp/hyperzoekt-clones/null-repo/src/lib.rs",
        language: "rust",
        kind: "function",
        parent: null,
        start_line: 10,
        end_line: 20,
        calls: [],
        doc: null,
        rank: 0.2,
        imports: [],
        unresolved_imports: [],
        repo_name: "null-repo"
    }"#;

    db.db.query(q1).await?;
    db.db.query(q2).await?;

    let summaries = db.get_repo_summaries().await?;
    let repo = summaries
        .iter()
        .find(|r| r.name == "null-repo")
        .expect("repo present");
    // languages should not contain nulls and should include "rust"
    assert!(repo.languages.contains(&"rust".to_string()));
    Ok(())
}

#[serial]
#[tokio::test]
async fn test_get_repo_summaries_hyperzoekt_clones_pattern(
) -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;

    let entities = vec![
        TestEntity {
            stable_id: "test1".to_string(),
            name: "getUserData".to_string(),
            signature: "def getUserData() -> User".to_string(),
            source_display: "/tmp/hyperzoekt-clones/gpt-abc123/src/user.py".to_string(),
            language: "python".to_string(),
            kind: "function".to_string(),
            start_line: 10,
            end_line: 15,
            rank: 0.8,
            repo_name: "gpt".to_string(),
        },
        TestEntity {
            stable_id: "test2".to_string(),
            name: "processData".to_string(),
            signature: "def processData(data: Data) -> None".to_string(),
            source_display: "/tmp/hyperzoekt-clones/gpt-abc123/src/data.py".to_string(),
            language: "python".to_string(),
            kind: "function".to_string(),
            start_line: 20,
            end_line: 25,
            rank: 0.7,
            repo_name: "gpt".to_string(),
        },
        TestEntity {
            stable_id: "test3".to_string(),
            name: "UserService".to_string(),
            signature: "class UserService".to_string(),
            source_display: "/tmp/hyperzoekt-clones/hyperzoekt-def456/src/service.ts".to_string(),
            language: "typescript".to_string(),
            kind: "class".to_string(),
            start_line: 5,
            end_line: 30,
            rank: 0.9,
            repo_name: "hyperzoekt".to_string(),
        },
    ];

    db.insert_test_entities(entities).await?;

    let repos = db.get_repo_summaries().await?;

    assert_eq!(repos.len(), 2);

    // Check gpt repository
    let gpt = repos.iter().find(|r| r.name == "gpt").unwrap();
    assert_eq!(gpt.entity_count, 2);
    assert_eq!(gpt.file_count, 2);
    assert!(gpt.languages.contains(&"python".to_string()));

    // Check hyperzoekt repository
    let hyperzoekt = repos.iter().find(|r| r.name == "hyperzoekt").unwrap();
    assert_eq!(hyperzoekt.entity_count, 1);
    assert_eq!(hyperzoekt.file_count, 1);
    assert!(hyperzoekt.languages.contains(&"typescript".to_string()));

    Ok(())
}

#[serial]
#[tokio::test]
async fn test_get_repo_summaries_empty_database() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;

    let repos = db.get_repo_summaries().await?;

    assert_eq!(repos.len(), 0);

    Ok(())
}

#[serial]
#[tokio::test]
async fn test_get_repo_summaries_with_null_repo_name() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;

    // Insert entities without repo_name field (simulating old data)
    let entities_without_repo_name = vec![TestEntity {
        stable_id: "old1".to_string(),
        name: "oldFunction".to_string(),
        signature: "def oldFunction() -> None".to_string(),
        source_display: "/tmp/hyperzoekt-clones/old-repo-abc123/src/old.py".to_string(),
        language: "python".to_string(),
        kind: "function".to_string(),
        start_line: 10,
        end_line: 15,
        rank: 0.8,
        repo_name: "".to_string(), // Empty string to simulate missing field
    }];

    // Insert using raw SQL to simulate entities without repo_name field
    for entity in entities_without_repo_name {
        let query = format!(
            r#"
            CREATE entity CONTENT {{
                stable_id: "{}",
                name: "{}",
                signature: "{}",
                    source_display: "{}",
                language: "{}",
                kind: "{}",
                parent: null,
                start_line: {},
                end_line: {},
                calls: [],
                doc: null,
                rank: {}
            }}
        "#,
            entity.stable_id,
            entity.name,
            entity.signature,
            entity.source_display,
            entity.language,
            entity.kind,
            entity.start_line,
            entity.end_line,
            entity.rank
        );

        db.db.query(&query).await?;
    }

    // This should handle null repo_name gracefully and return "unknown"
    let repos = db.get_repo_summaries().await?;

    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0].name, "unknown");
    assert_eq!(repos[0].entity_count, 1);

    Ok(())
}

#[serial]
#[tokio::test]
async fn test_get_repo_summaries_all_null_repo_names() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;

    // Insert multiple entities without repo_name field
    let entities_without_repo_name = vec![
        TestEntity {
            stable_id: "null1".to_string(),
            name: "func1".to_string(),
            signature: "def func1()".to_string(),
            source_display: "/tmp/hyperzoekt-clones/repo1-abc123/src/file1.py".to_string(),
            language: "python".to_string(),
            kind: "function".to_string(),
            start_line: 10,
            end_line: 15,
            rank: 0.8,
            repo_name: "".to_string(),
        },
        TestEntity {
            stable_id: "null2".to_string(),
            name: "func2".to_string(),
            signature: "def func2()".to_string(),
            source_display: "/tmp/hyperzoekt-clones/repo2-def456/src/file2.py".to_string(),
            language: "python".to_string(),
            kind: "function".to_string(),
            start_line: 20,
            end_line: 25,
            rank: 0.7,
            repo_name: "".to_string(),
        },
    ];

    // Insert using raw SQL to simulate entities without repo_name field
    for entity in entities_without_repo_name {
        let query = format!(
            r#"
            CREATE entity CONTENT {{
                stable_id: "{}",
                name: "{}",
                signature: "{}",
                source_display: "{}",
                language: "{}",
                kind: "{}",
                parent: null,
                start_line: {},
                end_line: {},
                calls: [],
                doc: null,
                rank: {}
            }}
        "#,
            entity.stable_id,
            entity.name,
            entity.signature,
            entity.source_display,
            entity.language,
            entity.kind,
            entity.start_line,
            entity.end_line,
            entity.rank
        );

        db.db.query(&query).await?;
    }

    // This should handle all null repo_names gracefully and return one "unknown" group
    let repos = db.get_repo_summaries().await?;

    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0].name, "unknown");
    assert_eq!(repos[0].entity_count, 2); // Both entities should be grouped together

    Ok(())
}
