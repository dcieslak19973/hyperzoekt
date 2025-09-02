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
use rust_mcp_sdk::mcp_client::client_runtime as sdk_client_runtime;
use rust_mcp_sdk::mcp_client::ClientHandler;
use rust_mcp_sdk::schema::{
    CallToolRequestParams, ClientCapabilities, Implementation, InitializeRequestParams,
    LATEST_PROTOCOL_VERSION,
};
use rust_mcp_sdk::McpClient; // bring trait methods into scope
use rust_mcp_sdk::{StdioTransport as SdkStdioTransport, TransportOptions};
use serde_json::{Map as JsonMap, Value as JsonValue};

// This test launches the hyperzoekt binary in MCP stdio mode using the official
// rust-mcp-sdk client runtime, then calls the `search` tool and validates a result.
#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_search_e2e() -> Result<(), Box<dyn std::error::Error>> {
    // 1) Create a client-side stdio transport that launches our server binary.
    // Resolve server binary path: prefer Cargo-provided env var; else derive from current_exe
    let server_cmd_path = std::env::var("CARGO_BIN_EXE_hyperzoekt")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            let mut path = std::env::current_exe().expect("current_exe");
            // target/debug/deps/<test> -> target/debug
            if path.pop() { /* deps */ }
            if path.pop() { /* debug */ }
            path.push("hyperzoekt");
            path
        });
    let server_cmd_path = server_cmd_path.canonicalize().unwrap_or(server_cmd_path);
    println!("Launching MCP server: {}", server_cmd_path.display());
    let server_cmd_str: String = server_cmd_path.to_string_lossy().into_owned();
    let transport = SdkStdioTransport::create_with_server_launch(
        &server_cmd_str,
        vec!["--mcp-stdio".to_string()],
        None,
        TransportOptions::default(),
    )?;

    // 2) Minimal client handler (use defaults).
    struct TestClientHandler;
    impl ClientHandler for TestClientHandler {}

    // 3) Initialize the MCP client runtime.
    let client_info = InitializeRequestParams {
        client_info: Implementation {
            name: "hyperzoekt-e2e-client".to_string(),
            version: "0.1.0".to_string(),
            title: None,
        },
        capabilities: ClientCapabilities::default(),
        protocol_version: LATEST_PROTOCOL_VERSION.to_string(),
    };
    let client = sdk_client_runtime::create_client(client_info, transport, TestClientHandler);

    // 4) Start the client (connects, performs initialize handshake).
    let client_arc = client.clone();
    // start() takes self: Arc<Self>, so use a clone to keep client_arc usable
    client_arc.clone().start().await?;

    // Sanity check initialize
    let server_ver = client_arc.server_version();
    assert!(
        server_ver.is_some(),
        "server did not report version after init"
    );
    assert_eq!(
        client_arc.server_has_tools(),
        Some(true),
        "server missing tools"
    );

    // 5) Call the `search` tool via the SDK.
    let mut args: JsonMap<String, JsonValue> = JsonMap::new();
    args.insert(
        "q".to_string(),
        JsonValue::String("nonexistent-symbol-xyz".to_string()),
    );
    let params = CallToolRequestParams {
        name: "search".to_string(),
        arguments: Some(args),
    };
    let res = client_arc
        .call_tool(params)
        .await
        .expect("call_tool(search) failed");

    // Validate structured_content shape: { "results": [ { name, path, line }, ... ] }
    let sc = res
        .structured_content
        .expect("missing structured_content from search result");
    let results = sc
        .get("results")
        .unwrap_or_else(|| panic!("structured_content missing 'results': {:?}", sc));
    assert!(
        results.is_array(),
        "'results' must be an array, got: {}",
        results
    );
    let arr = results.as_array().unwrap();
    // For the nonexistent query we expect zero results, but if there are any, validate shape.
    if let Some(first) = arr.first() {
        assert!(
            first.is_object(),
            "result item should be an object: {}",
            first
        );
        let obj = first.as_object().unwrap();
        assert!(
            obj.get("name").and_then(|v| v.as_str()).is_some(),
            "missing string 'name' field"
        );
        assert!(
            obj.get("path").and_then(|v| v.as_str()).is_some(),
            "missing string 'path' field"
        );
        assert!(
            obj.get("line").and_then(|v| v.as_u64()).is_some(),
            "missing numeric 'line' field"
        );
    }

    // Positive-case query: known symbol "search" should exist in this repository.
    let mut args2: JsonMap<String, JsonValue> = JsonMap::new();
    args2.insert("q".to_string(), JsonValue::String("search".to_string()));
    let params2 = CallToolRequestParams {
        name: "search".to_string(),
        arguments: Some(args2),
    };
    let res2 = client_arc
        .call_tool(params2)
        .await
        .expect("call_tool(search q=search) failed");
    let sc2 = res2
        .structured_content
        .expect("missing structured_content from search result (positive)");
    let results2 = sc2
        .get("results")
        .unwrap_or_else(|| panic!("structured_content missing 'results': {:?}", sc2));
    assert!(results2.is_array(), "'results' must be an array");
    let arr2 = results2.as_array().unwrap();
    assert!(
        !arr2.is_empty(),
        "expected at least one result for query 'search'"
    );
    let first2 = arr2[0]
        .as_object()
        .expect("first result should be an object");
    let name2 = first2
        .get("name")
        .and_then(|v| v.as_str())
        .expect("result missing string 'name'");
    let path2 = first2
        .get("path")
        .and_then(|v| v.as_str())
        .expect("result missing string 'path'");
    assert_eq!(name2, "search", "first result name should be 'search'");
    // CI layouts can differ (compiled sources, path prefixes). Ensure the result
    // references the `repo_index` crate/directory rather than asserting a
    // specific filename.
    assert!(
        path2.contains("repo_index"),
        "path should reference repo_index, got: {}",
        path2
    );

    // 6) Shutdown cleanly to avoid flaky hangs in CI.
    client_arc.shut_down().await?;
    Ok(())
}
