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
use surrealdb::engine::local::Mem;
use surrealdb::Surreal;

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
    pub files: Vec<String>,
    pub languages: Vec<String>,
}

enum SurrealConnection {
    Local(Surreal<surrealdb::engine::local::Db>),
}

impl Clone for SurrealConnection {
    fn clone(&self) -> Self {
        match self {
            SurrealConnection::Local(db) => SurrealConnection::Local(db.clone()),
        }
    }
}

impl SurrealConnection {
    async fn query(&self, sql: &str) -> Result<surrealdb::Response, surrealdb::Error> {
        match self {
            SurrealConnection::Local(db) => db.query(sql).await,
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

    pub async fn get_repo_summaries(&self) -> Result<Vec<RepoSummary>, Box<dyn std::error::Error>> {
        // Use the stored repo_name field instead of parsing from file paths
        // Handle cases where repo_name might be null for existing data
        let query = r#"
            SELECT
                repo_name ?? 'unknown' as repo_name,
                count() as entity_count,
                array::distinct(file) as files,
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
                let file_count = s.files.len() as u64; // Count distinct files
                RepoSummary {
                    name: s.repo_name,
                    entity_count: s.entity_count,
                    file_count,
                    languages: s.languages,
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
                    file: "{}",
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
                entity.file,
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
    file: String,
    language: String,
    kind: String,
    start_line: usize,
    end_line: usize,
    rank: f32,
    repo_name: String,
}

#[tokio::test]
async fn test_get_repo_summaries_hyperzoekt_clones_pattern(
) -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;

    let entities = vec![
        TestEntity {
            stable_id: "test1".to_string(),
            name: "getUserData".to_string(),
            signature: "def getUserData() -> User".to_string(),
            file: "/tmp/hyperzoekt-clones/gpt-abc123/src/user.py".to_string(),
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
            file: "/tmp/hyperzoekt-clones/gpt-abc123/src/data.py".to_string(),
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
            file: "/tmp/hyperzoekt-clones/hyperzoekt-def456/src/service.ts".to_string(),
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

#[tokio::test]
async fn test_get_repo_summaries_empty_database() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;

    let repos = db.get_repo_summaries().await?;

    assert_eq!(repos.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_get_repo_summaries_with_null_repo_name() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;

    // Insert entities without repo_name field (simulating old data)
    let entities_without_repo_name = vec![TestEntity {
        stable_id: "old1".to_string(),
        name: "oldFunction".to_string(),
        signature: "def oldFunction() -> None".to_string(),
        file: "/tmp/hyperzoekt-clones/old-repo-abc123/src/old.py".to_string(),
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
                file: "{}",
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
            entity.file,
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

#[tokio::test]
async fn test_get_repo_summaries_all_null_repo_names() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;

    // Insert multiple entities without repo_name field
    let entities_without_repo_name = vec![
        TestEntity {
            stable_id: "null1".to_string(),
            name: "func1".to_string(),
            signature: "def func1()".to_string(),
            file: "/tmp/hyperzoekt-clones/repo1-abc123/src/file1.py".to_string(),
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
            file: "/tmp/hyperzoekt-clones/repo2-def456/src/file2.py".to_string(),
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
                file: "{}",
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
            entity.file,
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
