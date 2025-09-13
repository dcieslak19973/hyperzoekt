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

struct TestDatabase {
    db: Arc<SurrealConnection>,
}

impl TestDatabase {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Force using embedded in-memory SurrealDB for tests
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
        let prev = std::env::var("HZ_EPHEMERAL_MEM").ok();
        std::env::set_var("HZ_EPHEMERAL_MEM", "1");
        let conn = connect(&None, &None, &None, "testns", "testdb").await?;
        if let Some(v) = prev {
            std::env::set_var("HZ_EPHEMERAL_MEM", v);
        } else {
            std::env::remove_var("HZ_EPHEMERAL_MEM");
        }
        Ok(Self { db: Arc::new(conn) })
    }

    pub async fn insert_raw(&self, q: &str) -> Result<(), Box<dyn std::error::Error>> {
        match &*self.db {
            SurrealConnection::Local(db_conn) => {
                db_conn.query(q).await?;
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(q).await?;
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn.query(q).await?;
            }
        }
        Ok(())
    }

    pub async fn search_entities(
        &self,
        query: &str,
        repo_filter: Option<&str>,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        let query_lower = query.to_lowercase();

        let fields = "file, language, kind, name, parent, signature, start_line, end_line, doc, rank, imports, unresolved_imports, stable_id, repo_name, source_url, source_display";
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

        let mut binds = vec![("query", serde_json::Value::String(query_lower))];
        if let Some(repo) = repo_filter {
            binds.push(("repo", serde_json::Value::String(repo.to_string())));
        }

        let mut resp = self.db.query_with_binds(&sql, binds).await?;
        let entities: Vec<EntityPayload> = resp.take(0)?;
        Ok(entities)
    }
}

#[tokio::test]
async fn webui_search_smoke_handles_null_fields() -> Result<(), Box<dyn std::error::Error>> {
    let db = TestDatabase::new().await?;

    let insert1 = "CREATE entity CONTENT { stable_id: 'e-null-file', name: 'NullFileEntity', file: NONE, repo_name: 'r1', language: 'rust', kind: 'function', signature: '', start_line: NONE, end_line: NONE, doc: NONE, rank: 0.0, imports: [], unresolved_imports: [], calls: [] }";
    let insert2 = "CREATE entity CONTENT { stable_id: 'e-has-file', name: 'HasFileEntity', file: 'r1/path/to/file.rs', repo_name: 'r1', language: 'rust', kind: 'function', signature: '', start_line: NONE, end_line: NONE, doc: NONE, rank: 0.0, imports: [], unresolved_imports: [], calls: [] }";

    db.insert_raw(insert1).await?;
    db.insert_raw(insert2).await?;

    // Search without repo filter should find the entity by name even if file is NULL
    let res_no_repo = db.search_entities("NullFileEntity", None).await?;
    assert!(res_no_repo.iter().any(|e| e.stable_id == "e-null-file"));

    // Search with repo filter should find the file-bearing entity and not error
    let res_with_repo = db.search_entities("HasFileEntity", Some("r1")).await?;
    assert!(res_with_repo.iter().any(|e| e.stable_id == "e-has-file"));
    Ok(())
}
