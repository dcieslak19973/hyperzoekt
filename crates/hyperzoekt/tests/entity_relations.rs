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
mod test_db;
use test_db::test_db;

#[tokio::test]
async fn test_entity_relations_basic_sql() {
    // Obtain a test DB (remote if SURREAL_TEST_HTTP_URL set, otherwise in-memory)
    let db = test_db().await;
    db.use_ns_db("zoekt", "repos").await;

    // Ensure clean state
    db.remove_all("entity").await;

    // Insert three entities: A calls B and "unknown", C calls A
    let create_a = r#"CREATE entity CONTENT {
         stable_id: "A",
         name: "Alpha",
         repo_name: "test-repo",
         signature: "fn alpha()",
         file: "src/a.rs",
         language: "rust",
         kind: "function",
         parent: null,
         start_line: 1,
         end_line: 10,
         calls: ["B", "unknown"],
         doc: null,
         rank: 0.5,
         imports: [],
         unresolved_imports: []
     }"#;

    let create_b = r#"CREATE entity CONTENT {
         stable_id: "B",
         name: "Beta",
         repo_name: "test-repo",
         signature: "fn beta()",
         file: "src/b.rs",
         language: "rust",
         kind: "function",
         parent: null,
         start_line: 1,
         end_line: 8,
         calls: [],
         doc: null,
         rank: 0.6,
         imports: [],
         unresolved_imports: []
     }"#;

    let create_c = r#"CREATE entity CONTENT {
         stable_id: "C",
         name: "Gamma",
         repo_name: "test-repo",
         signature: "fn gamma()",
         file: "src/c.rs",
         language: "rust",
         kind: "function",
         parent: null,
         start_line: 1,
         end_line: 12,
         calls: ["A"],
         doc: null,
         rank: 0.7,
         imports: [],
         unresolved_imports: []
     }"#;

    db.query(create_a).await.expect("create a");
    db.query(create_b).await.expect("create b");
    db.query(create_c).await.expect("create c");

    // Run callers query (with rank in select) to ensure ORDER BY rank works
    let callers_q = r#"SELECT name, stable_id, rank FROM entity WHERE 'A' IN calls OR 'Alpha' IN calls ORDER BY rank DESC LIMIT 10"#;
    let mut resp = db.query(callers_q).await.expect("callers q");
    let rows: Vec<serde_json::Value> = resp.take(0).expect("rows");
    // Should find one caller (Gamma)
    assert!(rows
        .iter()
        .any(|r| r.get("name").and_then(|v| v.as_str()) == Some("Gamma")));

    // Get distinct calls and lookup callees by stable_id and name
    let calls_q =
        r#"SELECT array::distinct(calls) as calls FROM entity WHERE stable_id = 'A' LIMIT 1"#;
    let mut resp2 = db.query(calls_q).await.expect("calls q");
    let vals: Vec<serde_json::Value> = resp2.take(0).expect("vals");
    let calls_list = vals
        .first()
        .and_then(|v| v.get("calls"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    // Ensure B appears among callees of A (calls_list) and then verify entity B exists.
    assert!(calls_list.iter().any(|v| v.as_str() == Some("B")));
    let mut check_b = db
        .query("SELECT name FROM entity WHERE stable_id = 'B' LIMIT 1")
        .await
        .expect("select b");
    let rows3: Vec<serde_json::Value> = check_b.take(0).expect("rows b");
    assert!(rows3
        .iter()
        .any(|r| r.get("name").and_then(|v| v.as_str()) == Some("Beta")));

    // For unresolved calls ensure at least the 'unknown' appears in calls_list
    assert!(calls_list.iter().any(|v| v.as_str() == Some("unknown")));
}
