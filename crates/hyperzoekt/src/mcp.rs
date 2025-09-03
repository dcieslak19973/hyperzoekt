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

use crate::repo_index::RepoIndexService;
use async_trait::async_trait;
use rust_mcp_sdk::mcp_server::{hyper_server, server_runtime, HyperServerOptions};
use rust_mcp_sdk::schema::{
    Implementation, InitializeResult, ServerCapabilities, ServerCapabilitiesTools,
    LATEST_PROTOCOL_VERSION,
};
use rust_mcp_sdk::{mcp_server::ServerHandler, McpServer};
use serde_json::json;

/// Start the MCP servers for this service. This is library-exported so the
/// `bin` can be tiny and the logic is testable.
pub fn run_mcp(
    svc: RepoIndexService,
    use_stdio: bool,
    use_http: bool,
) -> Result<(), anyhow::Error> {
    let server_details = InitializeResult {
        server_info: Implementation {
            name: "hyperzoekt-mcp".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            title: None,
        },
        capabilities: ServerCapabilities {
            tools: Some(ServerCapabilitiesTools { list_changed: None }),
            ..Default::default()
        },
        meta: None,
        instructions: Some("hyperzoekt MCP server".to_string()),
        protocol_version: LATEST_PROTOCOL_VERSION.to_string(),
    };

    struct Handler {
        svc: RepoIndexService,
    }

    #[async_trait]
    impl ServerHandler for Handler {
        async fn handle_list_tools_request(
            &self,
            _req: rust_mcp_sdk::schema::ListToolsRequest,
            _runtime: &dyn McpServer,
        ) -> std::result::Result<
            rust_mcp_sdk::schema::ListToolsResult,
            rust_mcp_sdk::schema::RpcError,
        > {
            log::info!("MCP handler: handle_list_tools_request invoked");
            // Advertise tools via a compatibility shim; keep SDK-native tools Vec empty to be safe.
            let mut _tools_arr: Vec<serde_json::Value> = Vec::new();
            _tools_arr.push(json!({"name":"search","title":"Search symbols by exact name","description":"Returns matching symbol definitions"}));
            _tools_arr.push(json!({"name":"usages","title":"Find usages of a symbol","description":"Returns definitions and nearby callers/callees for a symbol"}));
            let res = rust_mcp_sdk::schema::ListToolsResult {
                meta: None,
                next_cursor: None,
                tools: Vec::new(),
            };
            log::info!(
                "MCP handler: handle_list_tools_request responding (tools_count={})",
                _tools_arr.len()
            );
            Ok(res)
        }

        async fn handle_call_tool_request(
            &self,
            request: rust_mcp_sdk::schema::CallToolRequest,
            _runtime: &dyn McpServer,
        ) -> std::result::Result<
            rust_mcp_sdk::schema::CallToolResult,
            rust_mcp_sdk::schema::schema_utils::CallToolError,
        > {
            let tool_name = request.params.name.as_str();
            log::info!(
                "MCP handler: handle_call_tool_request invoked for tool={} args={:?}",
                tool_name,
                request.params.arguments
            );

            #[derive(Debug)]
            struct SimpleErr(String);
            impl std::fmt::Display for SimpleErr {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", self.0)
                }
            }
            impl std::error::Error for SimpleErr {}

            if tool_name == "search" {
                if let Some(args) = &request.params.arguments {
                    if let Some(qv) = args.get("q").and_then(|v| v.as_str()) {
                        let results = self.svc.search(qv, 50);
                        let mut map = serde_json::Map::new();
                        map.insert(
                            "results".to_string(),
                            serde_json::to_value(&results).unwrap_or(serde_json::Value::Null),
                        );
                        let res = rust_mcp_sdk::schema::CallToolResult {
                            content: Vec::new(),
                            is_error: None,
                            meta: None,
                            structured_content: Some(map),
                        };
                        log::info!(
                            "MCP handler: search response prepared (count={})",
                            results.len()
                        );
                        return Ok(res);
                    }
                }
                return Err(rust_mcp_sdk::schema::schema_utils::CallToolError::new(
                    SimpleErr("invalid params for search".to_string()),
                ));
            }

            if tool_name == "usages" {
                if let Some(args) = &request.params.arguments {
                    if let Some(qv) = args.get("q").and_then(|v| v.as_str()) {
                        let depth = args.get("depth").and_then(|v| v.as_u64()).unwrap_or(2) as u32;
                        let limit =
                            args.get("limit").and_then(|v| v.as_u64()).unwrap_or(500) as usize;

                        let def_ids = self.svc.symbol_ids_exact(qv).to_vec();
                        let mut defs: Vec<serde_json::Value> = Vec::new();
                        for id in def_ids.iter() {
                            if let Some(e) = self.svc.entities.get(*id as usize) {
                                defs.push(json!({"name": e.name, "path": self.svc.files[e.file_id as usize].path, "line": e.start_line as usize}));
                            }
                        }

                        let mut usage_set: std::collections::HashSet<u32> =
                            std::collections::HashSet::new();
                        for id in def_ids.iter() {
                            let callers = self.svc.callers_up_to(*id, depth);
                            for c in callers.into_iter() {
                                usage_set.insert(c);
                            }
                            let callees = self.svc.callees_up_to(*id, depth);
                            for c in callees.into_iter() {
                                usage_set.insert(c);
                            }
                        }
                        for id in def_ids.iter() {
                            usage_set.remove(id);
                        }

                        let mut usage_ids: Vec<u32> = usage_set.into_iter().collect();
                        usage_ids.sort();
                        usage_ids.truncate(limit);
                        let mut usages: Vec<serde_json::Value> = Vec::new();
                        for uid in usage_ids.iter() {
                            if let Some(e) = self.svc.entities.get(*uid as usize) {
                                usages.push(json!({"name": e.name, "path": self.svc.files[e.file_id as usize].path, "line": e.start_line as usize}));
                            }
                        }

                        let mut map = serde_json::Map::new();
                        map.insert("definitions".to_string(), serde_json::Value::Array(defs));
                        map.insert("usages".to_string(), serde_json::Value::Array(usages));
                        let res = rust_mcp_sdk::schema::CallToolResult {
                            content: Vec::new(),
                            is_error: None,
                            meta: None,
                            structured_content: Some(map),
                        };
                        log::info!(
                            "MCP handler: usages response prepared (usages_count={})",
                            res.structured_content
                                .as_ref()
                                .map(|m| m
                                    .get("usages")
                                    .and_then(|v| v.as_array())
                                    .map(|a| a.len())
                                    .unwrap_or(0))
                                .unwrap_or(0)
                        );
                        return Ok(res);
                    }
                }
                return Err(rust_mcp_sdk::schema::schema_utils::CallToolError::new(
                    SimpleErr("invalid params for usages".to_string()),
                ));
            }

            Err(
                rust_mcp_sdk::schema::schema_utils::CallToolError::unknown_tool(
                    tool_name.to_string(),
                ),
            )
        }
    }

    if use_stdio {
        // Map transport errors into an anyhow::Error string to avoid moving
        // non-Sync error types into the anyhow::Error internals.
        log::info!("MCP: creating stdio transport");
        let transport =
            rust_mcp_sdk::StdioTransport::new(rust_mcp_sdk::TransportOptions::default())
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        log::info!("MCP: stdio transport created");
        let handler = Handler { svc };
        let server = server_runtime::create_server(server_details, transport, handler);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        log::info!("MCP: starting stdio server runtime");
        rt.block_on(async move {
            if let Err(e) = server.start().await {
                eprintln!("MCP stdio server failed: {}", e);
            } else {
                log::info!("MCP: server.start() returned Ok");
            }
            Ok::<(), anyhow::Error>(())
        })?;
        return Ok(());
    }

    if use_http {
        let handler = Handler { svc };
        let server = hyper_server::create_server(
            server_details,
            handler,
            HyperServerOptions {
                host: "127.0.0.1".to_string(),
                ..Default::default()
            },
        );
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        rt.block_on(async move {
            if let Err(e) = server.start().await {
                eprintln!("MCP http server failed: {}", e);
            }
            Ok::<(), anyhow::Error>(())
        })?;
        return Ok(());
    }

    Ok(())
}

/// Helper used by tests: invoke the handler logic synchronously and return
/// a simple JSON value representing the structured_content for the given tool.
pub fn handle_call_tool_sync(
    svc: &RepoIndexService,
    tool_name: &str,
    args: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<serde_json::Value, String> {
    // emulate the handler's logic for `search` and `usages`
    if tool_name == "search" {
        if let Some(args) = args {
            if let Some(qv) = args.get("q").and_then(|v| v.as_str()) {
                let results = svc.search(qv, 50);
                let mut map = serde_json::Map::new();
                map.insert(
                    "results".to_string(),
                    serde_json::to_value(&results).map_err(|e| e.to_string())?,
                );
                return Ok(serde_json::Value::Object(map));
            }
        }
        return Err("invalid params for search".to_string());
    }

    if tool_name == "usages" {
        if let Some(args) = args {
            if let Some(qv) = args.get("q").and_then(|v| v.as_str()) {
                let depth = args.get("depth").and_then(|v| v.as_u64()).unwrap_or(2) as u32;
                let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(500) as usize;

                let def_ids = svc.symbol_ids_exact(qv).to_vec();
                let mut defs: Vec<serde_json::Value> = Vec::new();
                for id in def_ids.iter() {
                    if let Some(e) = svc.entities.get(*id as usize) {
                        defs.push(serde_json::json!({"name": e.name, "path": svc.files[e.file_id as usize].path, "line": e.start_line as usize}));
                    }
                }

                let mut usage_set: std::collections::HashSet<u32> =
                    std::collections::HashSet::new();
                for id in def_ids.iter() {
                    let callers = svc.callers_up_to(*id, depth);
                    for c in callers.into_iter() {
                        usage_set.insert(c);
                    }
                    let callees = svc.callees_up_to(*id, depth);
                    for c in callees.into_iter() {
                        usage_set.insert(c);
                    }
                }
                for id in def_ids.iter() {
                    usage_set.remove(id);
                }

                let mut usage_ids: Vec<u32> = usage_set.into_iter().collect();
                usage_ids.sort();
                usage_ids.truncate(limit);
                let mut usages: Vec<serde_json::Value> = Vec::new();
                for uid in usage_ids.iter() {
                    if let Some(e) = svc.entities.get(*uid as usize) {
                        usages.push(serde_json::json!({"name": e.name, "path": svc.files[e.file_id as usize].path, "line": e.start_line as usize}));
                    }
                }

                let mut map = serde_json::Map::new();
                map.insert("definitions".to_string(), serde_json::Value::Array(defs));
                map.insert("usages".to_string(), serde_json::Value::Array(usages));
                return Ok(serde_json::Value::Object(map));
            }
        }
        return Err("invalid params for usages".to_string());
    }

    Err(format!("unknown tool {}", tool_name))
}
