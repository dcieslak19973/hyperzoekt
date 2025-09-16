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
use hyperzoekt::repo_index::indexer::payload::{EntityPayload, ImportItem, UnresolvedImport};
use minijinja::Environment;
use std::fs;

#[tokio::test]
async fn render_entity_template_with_call_graph() {
    // Load templates from the static files on disk
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let base_path = format!("{}/static/webui/base.html", manifest_dir);
    let entity_path = format!("{}/static/webui/entity.html", manifest_dir);

    let base_t = fs::read_to_string(base_path).expect("read base template");
    let entity_t = fs::read_to_string(entity_path).expect("read entity template");

    let mut env = Environment::new();
    env.add_template("base", &base_t).unwrap();
    env.add_template("entity", &entity_t).unwrap();

    // Sample entity payload
    let entity = EntityPayload {
        language: "rust".to_string(),
        kind: "function".to_string(),
        name: "do_work".to_string(),
        parent: None,
        signature: "fn do_work()".to_string(),
        start_line: Some(10),
        end_line: Some(20),
        // calls removed
        doc: Some("Example docs".to_string()),
        rank: Some(0.123),
        imports: vec![ImportItem {
            path: "std::io".to_string(),
            line: 1,
        }],
        unresolved_imports: vec![UnresolvedImport {
            module: "mystery".to_string(),
            line: 2,
        }],
        stable_id: "stable123".to_string(),
        repo_name: "repo".to_string(),
        source_url: Some("https://example.com/repo/file.rs".to_string()),
        source_display: Some("repo/file.rs".to_string()),
        source_content: None,
        calls: vec![],
        methods: vec![],
    };

    // Sample callers/callees
    let callers = vec![("caller_fn".to_string(), Some("id1".to_string()))];
    let callees: Vec<(String, Option<String>)> = vec![("callee_fn".to_string(), None::<String>)];

    // Prepare call graph JSON (same as server-side)
    let call_graph_json = serde_json::to_string(&serde_json::json!({
        "callers": callers.iter().map(|(n,id)| serde_json::json!([n, id])).collect::<Vec<_>>(),
        "callees": callees.iter().map(|(n,id)| serde_json::json!([n, id])).collect::<Vec<_>>()
    }))
    .unwrap();

    let tmpl = env.get_template("entity").unwrap();
    let rendered = tmpl
        .render(minijinja::context! {
            entity => entity,
            repo_name => "repo",
            callers => callers,
            callees => callees,
            call_graph_json => call_graph_json,
            title => "Entity: do_work"
        })
        .expect("render should succeed");

    assert!(rendered.contains("do_work"));
    assert!(rendered.contains("call-graph-canvas"));
    assert!(rendered.contains("caller_fn"));
}

#[tokio::test]
async fn render_entity_template_with_empty_relations() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let base_t =
        std::fs::read_to_string(format!("{}/static/webui/base.html", manifest_dir)).unwrap();
    let entity_t =
        std::fs::read_to_string(format!("{}/static/webui/entity.html", manifest_dir)).unwrap();
    let mut env = Environment::new();
    env.add_template("base", &base_t).unwrap();
    env.add_template("entity", &entity_t).unwrap();

    let entity = EntityPayload {
        language: "rs".to_string(),
        kind: "function".to_string(),
        name: "empty_rel".to_string(),
        parent: None,
        signature: "".to_string(),
        start_line: None,
        end_line: None,
        // calls removed
        doc: None,
        rank: Some(0.0),
        imports: vec![],
        unresolved_imports: vec![],
        stable_id: "s0".to_string(),
        repo_name: "r".to_string(),
        source_url: None,
        source_display: None,
        source_content: None,
        calls: vec![],
        methods: vec![],
    };

    let callers: Vec<(String, Option<String>)> = vec![];
    let callees: Vec<(String, Option<String>)> = vec![];

    let call_graph_json = serde_json::to_string(&serde_json::json!({
        "callers": callers.iter().map(|(n,id)| serde_json::json!([n, id])).collect::<Vec<_>>(),
        "callees": callees.iter().map(|(n,id)| serde_json::json!([n, id])).collect::<Vec<_>>()
    }))
    .unwrap();

    let rendered = env
        .get_template("entity")
        .unwrap()
        .render(minijinja::context! {
            entity => entity,
            repo_name => "r",
            callers => callers,
            callees => callees,
            call_graph_json => call_graph_json,
            title => "Entity: empty_rel"
        })
        .unwrap();

    // JSON should be valid and include empty arrays
    let parsed: serde_json::Value = serde_json::from_str(&call_graph_json).unwrap();
    assert_eq!(parsed.get("callers").unwrap().as_array().unwrap().len(), 0);
    assert_eq!(parsed.get("callees").unwrap().as_array().unwrap().len(), 0);
    assert!(rendered.contains("empty_rel"));
}

#[tokio::test]
async fn render_entity_template_with_large_relations() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let base_t =
        std::fs::read_to_string(format!("{}/static/webui/base.html", manifest_dir)).unwrap();
    let entity_t =
        std::fs::read_to_string(format!("{}/static/webui/entity.html", manifest_dir)).unwrap();
    let mut env = Environment::new();
    env.add_template("base", &base_t).unwrap();
    env.add_template("entity", &entity_t).unwrap();

    let mut callers: Vec<(String, Option<String>)> = Vec::new();
    let mut callees: Vec<(String, Option<String>)> = Vec::new();
    for i in 0..500 {
        callers.push((format!("caller_{}", i), Some(format!("id_{}", i))));
        if i % 2 == 0 {
            callees.push((format!("callee_{}", i), None));
        } else {
            callees.push((format!("callee_{}", i), Some(format!("idc_{}", i))));
        }
    }

    let entity = EntityPayload {
        language: "rs".to_string(),
        kind: "function".to_string(),
        name: "big_rel".to_string(),
        parent: None,
        signature: "".to_string(),
        start_line: None,
        end_line: None,
        // calls removed
        doc: None,
        rank: Some(0.0),
        imports: vec![],
        unresolved_imports: vec![],
        stable_id: "sbig".to_string(),
        repo_name: "r".to_string(),
        source_url: None,
        source_display: None,
        source_content: None,
        calls: vec![],
        methods: vec![],
    };

    let call_graph_json = serde_json::to_string(&serde_json::json!({
        "callers": callers.iter().map(|(n,id)| serde_json::json!([n, id])).collect::<Vec<_>>(),
        "callees": callees.iter().map(|(n,id)| serde_json::json!([n, id])).collect::<Vec<_>>()
    }))
    .unwrap();

    // Ensure the JSON is parseable and contains expected nodes
    let parsed: serde_json::Value = serde_json::from_str(&call_graph_json).unwrap();
    assert_eq!(
        parsed.get("callers").unwrap().as_array().unwrap().len(),
        500
    );
    assert_eq!(
        parsed.get("callees").unwrap().as_array().unwrap().len(),
        500
    );

    let rendered = env
        .get_template("entity")
        .unwrap()
        .render(minijinja::context! {
            entity => entity,
            repo_name => "r",
            callers => callers.clone(),
            callees => callees.clone(),
            call_graph_json => call_graph_json,
            title => "Entity: big_rel"
        })
        .unwrap();

    assert!(rendered.contains("big_rel"));
    // spot-check that some caller/callee names are present in the rendered HTML
    assert!(rendered.contains("caller_0"));
    assert!(rendered.contains("callee_499"));
}
