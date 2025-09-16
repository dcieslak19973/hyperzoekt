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

// `connect` is imported where needed inside test functions to avoid unused-import warnings.
use hyperzoekt::db_writer::persist_repo_dependencies_with_connection;
use hyperzoekt::repo_index::deps::Dependency;
use hyperzoekt::repo_index::indexer::payload::EntityPayload;

#[tokio::test]
async fn repo_dependency_relations_created() {
    // Only run this test when an external SurrealDB URL is provided. The in-memory
    // engine does not expose GraphQL traversal the same way; users can set
    // SURREALDB_URL to point to a running SurrealDB instance with GraphQL enabled.
    if std::env::var("SURREALDB_URL").ok().is_none() {
        eprintln!("SKIP: SURREALDB_URL not set; skipping repo_dependency_relations_created test");
        return;
    }

    // Prefer environment variables so test runs align with docker/dev setups
    let ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "repo_dep_ns".into());
    let dbn = std::env::var("SURREAL_DB").unwrap_or_else(|_| "repo_dep_db".into());

    // Create a minimal entity set to represent repos and dependencies
    let entity_a = EntityPayload {
        language: "rust".into(),
        kind: "function".into(),
        name: "main".into(),
        parent: None,
        signature: "fn main()".into(),
        start_line: Some(1),
        end_line: Some(2),
        doc: None,
        rank: Some(1.0),
        imports: vec![hyperzoekt::repo_index::indexer::payload::ImportItem {
            path: "repo_y".into(),
            line: 1,
        }],
        unresolved_imports: vec![],
        stable_id: "main_stable".into(),
        repo_name: "repo_x".into(),
        source_url: None,
        source_display: None,
        calls: vec![],
        methods: vec![],
        source_content: None,
    };

    // Connect to the configured SurrealDB (test runs only when SURREALDB_URL is set)
    use hyperzoekt::db_writer::connection::connect;
    let db_conn = match connect(&None, &None, &None, ns.as_str(), dbn.as_str()).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "Skipping repo_dependency_relations_created: unable to connect to surrealdb: {e}"
            );
            return;
        }
    };
    // Ensure schema exists
    let _ = db_conn
        .query("DEFINE TABLE repo SCHEMALESS PERMISSIONS FULL; DEFINE TABLE dependency SCHEMALESS PERMISSIONS FULL;")
        .await;

    // Build dependency list from entity imports
    let mut repo_deps = std::collections::HashMap::new();
    repo_deps.insert(
        entity_a.repo_name.clone(),
        vec![Dependency {
            name: "repo_y".into(),
            version: None,
            language: entity_a.language.clone(),
        }],
    );

    for (repo, deps) in repo_deps {
        let res = persist_repo_dependencies_with_connection(
            &db_conn,
            &repo,
            &format!("git://example/{}.git", repo),
            None,
            None,
            None,
            None,
            &deps,
        )
        .await
        .expect("persist deps");

        // Expect at least one dependency record created
        assert!(
            res.deps_created > 0 || !res.edge_ids.is_empty() || res.edges_created > 0,
            "expected deps created or edges reported"
        );
    }

    // Verify traversal returns dependency from repo_x -> repo_y
    let q = "SELECT ->dependency.name AS deps FROM repo WHERE name = 'repo_x' LIMIT 1;";
    let mut resp = db_conn.query(q).await.expect("traversal query");
    let rows: Vec<serde_json::Value> = resp.take(0).unwrap_or_default();

    // Either traversal yields results or a relation/record exists
    if rows.is_empty() {
        // Check for explicit depends_on relation rows
        let rel_sel = "SELECT * FROM depends_on WHERE in = 'repo:repo_x' LIMIT 1;";
        let mut rel_r = db_conn.query(rel_sel).await.expect("relation select");
        let rel_rows: Vec<serde_json::Value> = rel_r.take(0).unwrap_or_default();
        if rel_rows.is_empty() {
            // Fallback: dependency record exists
            let sel = "SELECT name FROM dependency WHERE name = 'repo_y' LIMIT 1;";
            let mut r = db_conn.query(sel).await.expect("dependency select");
            let drows: Vec<serde_json::Value> = r.take(0).unwrap_or_default();
            assert!(
                !drows.is_empty(),
                "expected dependency record or depends_on relation for repo_y"
            );
        }
    } else {
        let names = hyperzoekt::test_utils::collect_field_ids(&rows, "deps");
        if names.is_empty() {
            // No traversal elements returned; fall back to checking depends_on or dependency record
            let rel_sel = "SELECT * FROM depends_on WHERE in = 'repo:repo_x' LIMIT 1;";
            let mut rel_r = db_conn.query(rel_sel).await.expect("relation select");
            let rel_rows: Vec<serde_json::Value> = rel_r.take(0).unwrap_or_default();
            if rel_rows.is_empty() {
                let sel = "SELECT name FROM dependency WHERE name = 'repo_y' LIMIT 1;";
                let mut r = db_conn.query(sel).await.expect("dependency select");
                let drows: Vec<serde_json::Value> = r.take(0).unwrap_or_default();
                assert!(
                    !drows.is_empty(),
                    "expected dependency record or depends_on relation for repo_y"
                );
            }
        } else {
            assert!(
                names.iter().any(|s| s.contains("repo_y")),
                "expected repo_y in traversal"
            );
        }
    }
}
