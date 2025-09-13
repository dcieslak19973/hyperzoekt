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
use hyperzoekt::graph_api::{fetch_entity_graph, GraphDbConfig};
use std::env;

// Integration test: write two entities (caller + callee) via db_writer, then fetch graph via public API.
// This exercises: payload persistence -> edge creation -> public fetch_entity_graph() traversal.
#[tokio::test]
async fn graph_api_returns_call_edges_after_indexing() {
    // Only run this integration when a remote SurrealDB is configured.
    let ns = env::var("SURREAL_NS").unwrap_or_else(|_| "graph_call_ns".into());
    let dbn = env::var("SURREAL_DB").unwrap_or_else(|_| "graph_call_db".into());

    use hyperzoekt::db_writer::connection::connect;
    // Connect to remote SurrealDB; skip test if unreachable to avoid CI flakes.
    let surreal_url = match env::var("SURREALDB_URL") {
        Ok(u) => {
            let (schemeful, _no_scheme) = hyperzoekt::test_utils::normalize_surreal_host(&u);
            std::env::set_var("SURREALDB_URL", &schemeful);
            std::env::set_var("SURREALDB_HTTP_BASE", &schemeful);
            schemeful
        }
        Err(_) => {
            eprintln!(
                "Skipping graph_api_returns_call_edges_after_indexing: SURREALDB_URL not set"
            );
            return;
        }
    };
    let db = match connect(
        &Some(surreal_url.clone()),
        &None,
        &None,
        ns.as_str(),
        dbn.as_str(),
    )
    .await
    {
        Ok(d) => d,
        Err(e) => {
            eprintln!(
                "Skipping graph_api_returns_call_edges_after_indexing: unable to connect to SURREALDB_URL: {}",
                e
            );
            return;
        }
    };
    // Define schema and ensure a clean starting state for this test by deleting
    // any pre-existing entities/relations for these deterministic stable ids.
    db.query(
        "DEFINE TABLE entity SCHEMALESS; DEFINE TABLE calls TYPE RELATION FROM entity TO entity;",
    )
    .await
    .ok();
    // Remove any previous test data that may exist in a shared test DB
    db.query("DELETE FROM calls WHERE in = entity:caller_stable AND out = entity:callee_stable;")
        .await
        .ok();
    db.query("DELETE FROM entity:caller_stable;").await.ok();
    db.query("DELETE FROM entity:callee_stable;").await.ok();

    // Include required fields (repo_name, file, language, kind, start_line, end_line,
    // imports, unresolved_imports) so remote SurrealDB instances with existing
    // schemas accept the CREATE statements.
    db.query("CREATE entity:caller_stable SET stable_id='caller_stable', name='do_caller', file='r/f.rs', language='rust', kind='function', start_line=1, end_line=2, imports=[], unresolved_imports=[], repo_name='r'; CREATE entity:callee_stable SET stable_id='callee_stable', name='do_callee', file='r/f.rs', language='rust', kind='function', start_line=3, end_line=4, imports=[], unresolved_imports=[], repo_name='r';")
        .await
        .expect("create entities");
    db.query("RELATE entity:caller_stable->calls->entity:callee_stable;")
        .await
        .expect("relate call");

    // Do not set SHARED_MEM for remote connections
    let graph_cfg = GraphDbConfig {
        surreal_url: Some(surreal_url.clone()),
        surreal_ns: ns,
        surreal_db: dbn,
        surreal_username: None,
        surreal_password: None,
    };
    let g_caller = fetch_entity_graph(&graph_cfg, "caller_stable", 10)
        .await
        .expect("fetch caller graph");
    assert_eq!(
        g_caller.callees.len(),
        1,
        "expected 1 callee, got {:?}",
        g_caller
    );
    assert_eq!(g_caller.callees[0].name, "do_callee");
    let g_callee = fetch_entity_graph(&graph_cfg, "callee_stable", 10)
        .await
        .expect("fetch callee graph");
    assert_eq!(
        g_callee.callers.len(),
        1,
        "expected 1 caller, got {:?}",
        g_callee
    );
    assert_eq!(g_callee.callers[0].name, "do_caller");
}
