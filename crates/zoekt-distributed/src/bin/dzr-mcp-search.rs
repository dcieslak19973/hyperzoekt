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

use anyhow::Result;
use clap::Parser;
use reqwest::Client;
use tracing_subscriber::EnvFilter;
use zoekt_distributed::{
    distributed_search::{DistributedSearchService, DistributedSearchTool},
    LeaseManager, NodeConfig, NodeType,
};

// UltraFast MCP imports
use async_trait::async_trait;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Redirect},
    routing::{get, options, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use ultrafast_mcp::{
    ListToolsRequest, ListToolsResponse, MCPError, MCPResult, Tool, ToolAnnotations, ToolCall,
    ToolContent, ToolHandler, ToolResult,
};

// Session store for managing client sessions
type SessionStore = Arc<RwLock<HashMap<String, SessionData>>>;

#[derive(Clone, Debug)]
struct SessionData {
    headers: HashMap<String, String>,
    _experimental: HashMap<String, String>,
    _created_at: std::time::Instant,
}

impl Default for SessionData {
    fn default() -> Self {
        Self {
            headers: HashMap::new(),
            _experimental: HashMap::new(),
            _created_at: std::time::Instant::now(),
        }
    }
}

// Enhanced Git credentials structure
#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
struct GitCredentials {
    github_username: Option<String>,
    github_token: Option<String>,
    gitlab_username: Option<String>,
    gitlab_token: Option<String>,
    bitbucket_username: Option<String>,
    bitbucket_token: Option<String>,
}

// Shared state combining credentials and sessions
#[derive(Clone)]
struct AppState {
    credentials: Arc<RwLock<GitCredentials>>,
    sessions: SessionStore,
}

// Type alias for backward compatibility
type SharedCredentials = Arc<RwLock<GitCredentials>>;

// JSON-RPC structures for MCP protocol
#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum JsonRpcMessage {
    Single(JsonRpcRequest),
    Batch(Vec<JsonRpcRequest>),
}

#[derive(Deserialize, Debug)]
struct JsonRpcRequest {
    _jsonrpc: String,
    method: String,
    params: Option<serde_json::Value>,
    id: Option<serde_json::Value>,
}

#[derive(Serialize, Debug)]
#[serde(untagged)]
#[allow(dead_code)]
enum JsonRpcResponseMessage {
    Single(JsonRpcResponse),
    Batch(Vec<JsonRpcResponse>),
}

#[derive(Serialize, Debug)]
struct JsonRpcResponse {
    jsonrpc: String,
    result: serde_json::Value,
    id: Option<serde_json::Value>,
}

#[derive(Serialize, Debug)]
struct JsonRpcErrorResponse {
    jsonrpc: String,
    error: JsonRpcError,
    id: Option<serde_json::Value>,
}

#[derive(Serialize, Debug)]
struct JsonRpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
}

// MCP-specific structures
#[derive(Serialize, Debug)]
struct InitializeResult {
    protocol_version: String,
    capabilities: ServerCapabilities,
    server_info: ServerInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    instructions: Option<String>,
}

#[derive(Serialize, Debug)]
struct ServerCapabilities {
    tools: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    roots: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sampling: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompts: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resources: Option<serde_json::Value>,
}

#[derive(Serialize, Debug)]
struct ServerInfo {
    name: String,
    version: String,
}
struct DistributedSearchHandler {
    search_service: DistributedSearchService,
    credentials: SharedCredentials,
}

#[async_trait]
impl ToolHandler for DistributedSearchHandler {
    async fn handle_tool_call(&self, call: ToolCall) -> MCPResult<ToolResult> {
        let tool_start = std::time::Instant::now();
        tracing::info!(
            "🔧 MCP handler: handle_tool_call invoked for tool='{}' args={:?}",
            call.name,
            call.arguments
        );

        // Get current Git credentials from shared state
        let creds = self.credentials.read().await;
        let github_username = creds.github_username.clone();
        let github_token = creds.github_token.clone();
        let gitlab_username = creds.gitlab_username.clone();
        let gitlab_token = creds.gitlab_token.clone();
        let bitbucket_username = creds.bitbucket_username.clone();
        let bitbucket_token = creds.bitbucket_token.clone();

        tracing::info!(
            "🔐 MCP handler: using Git credentials - GitHub: {}, GitLab: {}, BitBucket: {}",
            if github_username.is_some() {
                "✅ present"
            } else {
                "❌ missing"
            },
            if gitlab_username.is_some() {
                "✅ present"
            } else {
                "❌ missing"
            },
            if bitbucket_username.is_some() {
                "✅ present"
            } else {
                "❌ missing"
            }
        );

        match call.name.as_str() {
            "distributed_search" => {
                tracing::info!("🔍 Starting distributed search execution");
                if let Some(args) = &call.arguments {
                    if let Some(qv) = args.get("regex").and_then(|v| v.as_str()) {
                        tracing::info!("📝 Search parameters - regex: '{}'", qv);

                        let include = args.get("include").and_then(|v| v.as_str());
                        let exclude = args.get("exclude").and_then(|v| v.as_str());
                        let repo = args.get("repo").and_then(|v| v.as_str());
                        let branch = args.get("branch").and_then(|v| v.as_str());
                        let max_results = args
                            .get("max_results")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(100) as usize;
                        let context =
                            args.get("context").and_then(|v| v.as_u64()).unwrap_or(2) as usize;
                        let case_sensitive = args
                            .get("case_sensitive")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        // Pagination params (cursor is offset)
                        let start = args
                            .get("cursor")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(0);
                        let limit = args
                            .get("limit")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as usize)
                            .unwrap_or(50)
                            .clamp(1, 500);

                        tracing::info!("⚙️  Search configuration - max_results: {}, context: {}, case_sensitive: {}, include: {:?}, exclude: {:?}, repo: {:?}",
                            max_results, context, case_sensitive, include, exclude, repo);

                        let params = DistributedSearchTool {
                            regex: qv.to_string(),
                            include: include.map(|s| s.to_string()),
                            exclude: exclude.map(|s| s.to_string()),
                            repo: repo.map(|s| s.to_string()),
                            branch: branch.map(|s| s.to_string()),
                            max_results: Some(max_results),
                            context: Some(context),
                            case_sensitive: Some(case_sensitive),
                            github_username,
                            github_token,
                            gitlab_username,
                            gitlab_token,
                            bitbucket_username,
                            bitbucket_token,
                        };

                        tracing::info!("🚀 Executing distributed search...");
                        let search_start = std::time::Instant::now();

                        // Execute the distributed search
                        match self
                            .search_service
                            .execute_distributed_search_json(params)
                            .await
                        {
                            Ok(results) => {
                                let search_duration = search_start.elapsed();
                                let results_count = results.len();
                                tracing::info!(
                                    "✅ Search completed successfully - found {} results in {:?}",
                                    results_count,
                                    search_duration
                                );

                                // Format the results for display and apply pagination
                                let mut lines: Vec<String> = Vec::new();

                                // Add summary as first line
                                lines.push(format!(
                                    "Search completed: {} results found",
                                    results_count
                                ));

                                // Add actual search results as serialized blocks
                                for (i, result) in results.iter().enumerate() {
                                    if let Some(_summary) =
                                        result.get("summary").and_then(|s| s.as_str())
                                    {
                                        continue;
                                    }
                                    if let Some(error) =
                                        result.get("error").and_then(|e| e.as_str())
                                    {
                                        lines.push(format!("Error: {}", error));
                                        continue;
                                    }
                                    let file = result
                                        .get("path")
                                        .or_else(|| result.get("file"))
                                        .or_else(|| result.get("relative_path"))
                                        .and_then(|f| f.as_str())
                                        .unwrap_or("unknown");
                                    let line = result
                                        .get("symbol_loc")
                                        .and_then(|sl| sl.get("line"))
                                        .and_then(|l| l.as_u64())
                                        .or_else(|| result.get("line").and_then(|l| l.as_u64()))
                                        .unwrap_or(0);
                                    let content_match = result
                                        .get("snippet")
                                        .or_else(|| result.get("content"))
                                        .and_then(|c| c.as_str())
                                        .unwrap_or("");
                                    let node_id = result
                                        .get("node_id")
                                        .and_then(|n| n.as_str())
                                        .unwrap_or("unknown");
                                    let result_text = format!(
                                        "\n--- Result {} ---\nFile: {}\nLine: {}\nNode: {}\nContent:\n{}",
                                        i + 1,
                                        file,
                                        line,
                                        node_id,
                                        content_match
                                    );
                                    lines.push(result_text);
                                }

                                // Apply pagination to lines
                                let total = lines.len();
                                let end = (start + limit).min(total);
                                let next = if end < total {
                                    Some(end.to_string())
                                } else {
                                    None
                                };
                                let mut content: Vec<ToolContent> = Vec::new();
                                if start == 0 {
                                    content.push(ToolContent::text(format!(
                                        "total={} window={}..{}",
                                        total, start, end
                                    )));
                                }
                                for l in &lines[start..end] {
                                    content.push(ToolContent::text(l.clone()));
                                }
                                if let Some(nc) = next {
                                    content
                                        .push(ToolContent::text(format!("__NEXT_CURSOR__:{nc}")));
                                }

                                let res = ToolResult {
                                    content,
                                    is_error: Some(false),
                                };

                                let total_duration = tool_start.elapsed();
                                tracing::info!(
                                    "🎯 MCP handler: distributed_search response prepared (count={}, total_time={:?})",
                                    results_count, total_duration
                                );
                                return Ok(res);
                            }
                            Err(e) => {
                                let search_duration = search_start.elapsed();
                                tracing::error!(
                                    "❌ Search execution failed after {:?}: {}",
                                    search_duration,
                                    e
                                );
                                return Err(MCPError::internal_error(format!(
                                    "Search failed: {}",
                                    e
                                )));
                            }
                        }
                    } else {
                        tracing::warn!(
                            "⚠️  Missing 'regex' parameter in distributed_search arguments"
                        );
                    }
                } else {
                    tracing::warn!("⚠️  No arguments provided for distributed_search");
                }
                return Err(MCPError::invalid_params(
                    "invalid params for distributed_search".to_string(),
                ));
            }
            _ => {
                tracing::warn!("⚠️  Unknown tool requested: {}", call.name);
                Err(MCPError::method_not_found(format!(
                    "Unknown tool: {}",
                    call.name
                )))
            }
        }
    }

    async fn list_tools(&self, _request: ListToolsRequest) -> MCPResult<ListToolsResponse> {
        tracing::info!("MCP handler: list_tools invoked");
        let tools = vec![Tool {
            name: "distributed_search".to_string(),
            description: "Search across distributed zoekt indexers using regex patterns"
                .to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "regex": {"type": "string", "description": "Regex pattern to search for"},
                    "include": {"type": "string", "description": "Path patterns to include"},
                    "exclude": {"type": "string", "description": "Path patterns to exclude"},
                    "repo": {"type": "string", "description": "Repository to search in"},
                    "max_results": {"type": "integer", "description": "Maximum number of results", "default": 100},
                    "context": {"type": "integer", "description": "Number of context lines", "default": 2},
                    "case_sensitive": {"type": "boolean", "description": "Case sensitive search", "default": false}
                },
                "required": ["regex"]
            }),
            output_schema: None,
            annotations: None,
        }];
        tracing::info!(
            "MCP handler: list_tools responding (tools_count={})",
            tools.len()
        );
        Ok(ListToolsResponse {
            tools,
            next_cursor: None,
        })
    }
}

// Header extraction middleware
async fn extract_git_headers(headers: &HeaderMap, credentials: &SharedCredentials) {
    let mut creds = credentials.write().await;
    let mut extracted_count = 0;

    tracing::debug!("Starting header extraction from {} headers", headers.len());

    // Extract GitHub credentials
    if let Some(username) = headers
        .get("x-github-username")
        .and_then(|v| v.to_str().ok())
    {
        creds.github_username = Some(username.to_string());
        tracing::info!("✅ Extracted GitHub username from header");
        extracted_count += 1;
    }
    if let Some(token) = headers.get("x-github-token").and_then(|v| v.to_str().ok()) {
        creds.github_token = Some(token.to_string());
        tracing::info!(
            "✅ Extracted GitHub token from header (length: {})",
            token.len()
        );
        extracted_count += 1;
    }

    // Extract GitLab credentials
    if let Some(username) = headers
        .get("x-gitlab-username")
        .and_then(|v| v.to_str().ok())
    {
        creds.gitlab_username = Some(username.to_string());
        tracing::info!("✅ Extracted GitLab username from header");
        extracted_count += 1;
    }
    if let Some(token) = headers.get("x-gitlab-token").and_then(|v| v.to_str().ok()) {
        creds.gitlab_token = Some(token.to_string());
        tracing::info!(
            "✅ Extracted GitLab token from header (length: {})",
            token.len()
        );
        extracted_count += 1;
    }

    // Extract BitBucket credentials
    if let Some(username) = headers
        .get("x-bitbucket-username")
        .and_then(|v| v.to_str().ok())
    {
        creds.bitbucket_username = Some(username.to_string());
        tracing::info!("✅ Extracted BitBucket username from header");
        extracted_count += 1;
    }
    if let Some(token) = headers
        .get("x-bitbucket-token")
        .and_then(|v| v.to_str().ok())
    {
        creds.bitbucket_token = Some(token.to_string());
        tracing::info!(
            "✅ Extracted BitBucket token from header (length: {})",
            token.len()
        );
        extracted_count += 1;
    }

    tracing::info!(
        "🔐 Header extraction completed - extracted {} credential(s)",
        extracted_count
    );

    // Log current credential status
    let github_present = creds.github_username.is_some() && creds.github_token.is_some();
    let gitlab_present = creds.gitlab_username.is_some() && creds.gitlab_token.is_some();
    let bitbucket_present = creds.bitbucket_username.is_some() && creds.bitbucket_token.is_some();

    tracing::debug!(
        "Current credential status - GitHub: {}, GitLab: {}, BitBucket: {}",
        if github_present { "✅" } else { "❌" },
        if gitlab_present { "✅" } else { "❌" },
        if bitbucket_present { "✅" } else { "❌" }
    );
}

// Enhanced MCP handler with session management and batch support
async fn handle_mcp_with_headers(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let start_time = std::time::Instant::now();
    let request_id = format!("req_{}", start_time.elapsed().as_nanos());

    tracing::info!(
        "🚀 [{}] Incoming MCP request - body size: {} bytes",
        request_id,
        body.len()
    );

    // Extract session ID from headers or generate one
    let session_id = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("session_{}", start_time.elapsed().as_nanos()));

    // Store session data
    {
        let mut sessions = state.sessions.write().await;
        let session_data = sessions.entry(session_id.clone()).or_insert(SessionData {
            headers: headers
                .iter()
                .filter_map(|(k, v)| v.to_str().ok().map(|v| (k.to_string(), v.to_string())))
                .collect(),
            _experimental: HashMap::new(),
            _created_at: start_time,
        });

        // Update headers if session already exists
        for (k, v) in &headers {
            if let Ok(v_str) = v.to_str() {
                session_data
                    .headers
                    .insert(k.to_string(), v_str.to_string());
            }
        }
    }

    // Extract Git credentials from headers
    extract_git_headers(&headers, &state.credentials).await;

    // Parse the JSON-RPC request (support both single and batch)
    let message: Result<JsonRpcMessage, _> = serde_json::from_slice(&body);

    match message {
        Ok(JsonRpcMessage::Single(req)) => {
            handle_single_request(req, request_id, start_time, state).await
        }
        Ok(JsonRpcMessage::Batch(requests)) => {
            handle_batch_requests(requests, request_id, start_time, state).await
        }
        Err(e) => {
            tracing::error!(
                "❌ [{}] Failed to parse JSON-RPC request: {}",
                request_id,
                e
            );
            let error = JsonRpcError {
                code: -32700,
                message: "Parse error".to_string(),
                data: Some(serde_json::json!({ "details": e.to_string() })),
            };

            let error_response = JsonRpcErrorResponse {
                jsonrpc: "2.0".to_string(),
                error,
                id: None,
            };

            let duration = start_time.elapsed();
            tracing::info!(
                "⏱️  [{}] Parse error response prepared in {:?}",
                request_id,
                duration
            );
            let json_response = Json(error_response);
            let response = json_response.into_response();
            let response = add_cors_headers(response);
            (StatusCode::BAD_REQUEST, response).into_response()
        }
    }
}

// Handle single JSON-RPC request
async fn handle_single_request(
    req: JsonRpcRequest,
    request_id: String,
    start_time: std::time::Instant,
    state: AppState,
) -> axum::response::Response {
    tracing::info!(
        "📨 [{}] Parsed JSON-RPC request - method: '{}', id: {:?}",
        request_id,
        req.method,
        req.id
    );

    if let Some(params) = &req.params {
        tracing::debug!("📋 [{}] Request params: {}", request_id, params);
    }

    // Check if this is a notification (no id field)
    if req.id.is_none() {
        tracing::info!("🔔 [{}] Processing notification", request_id);
        return handle_notification(req, request_id, start_time)
            .await
            .into_response();
    }

    // Handle requests (with id)
    tracing::info!(
        "🔧 [{}] Processing request method: {}",
        request_id,
        req.method
    );

    match req.method.as_str() {
        "initialize" => {
            let response = handle_initialize_request(req, request_id, start_time).await;
            // Convert impl IntoResponse to Response
            response.into_response()
        }
        "tools/list" => {
            let response = handle_tools_list_request(req, request_id, start_time).await;
            response.into_response()
        }
        "tools/call" => {
            let response = handle_tools_call_request(req, request_id, start_time, state).await;
            response.into_response()
        }
        "shutdown" => {
            let response = handle_shutdown_request(req, request_id, start_time).await;
            response.into_response()
        }
        "exit" => {
            let response = handle_exit_request(req, request_id, start_time).await;
            response.into_response()
        }
        "$/ping" => {
            let response = handle_ping_request(req, request_id, start_time).await;
            response.into_response()
        }
        _ => {
            let response = handle_unknown_method(req, request_id, start_time).await;
            response.into_response()
        }
    }
}

// Handle batch JSON-RPC requests
async fn handle_batch_requests(
    requests: Vec<JsonRpcRequest>,
    request_id: String,
    start_time: std::time::Instant,
    state: AppState,
) -> axum::response::Response {
    tracing::info!(
        "📦 [{}] Processing batch of {} requests",
        request_id,
        requests.len()
    );

    let responses = Vec::new();
    let mut has_notifications = false;

    for req in requests {
        let req_id = format!("{}_{}", request_id, req.method);

        if req.id.is_none() {
            has_notifications = true;
            // Handle notification
            handle_notification(req, req_id, start_time).await;
        } else {
            // Handle request and collect response
            let response: axum::response::Response = match req.method.as_str() {
                "initialize" => {
                    let resp = handle_initialize_request(req, req_id, start_time).await;
                    resp.into_response()
                }
                "tools/list" => {
                    let resp = handle_tools_list_request(req, req_id, start_time).await;
                    resp.into_response()
                }
                "tools/call" => {
                    let resp =
                        handle_tools_call_request(req, req_id, start_time, state.clone()).await;
                    resp.into_response()
                }
                "shutdown" => {
                    let resp = handle_shutdown_request(req, req_id, start_time).await;
                    resp.into_response()
                }
                "exit" => {
                    let resp = handle_exit_request(req, req_id, start_time).await;
                    resp.into_response()
                }
                "$/ping" => {
                    let resp = handle_ping_request(req, req_id, start_time).await;
                    resp.into_response()
                }
                _ => {
                    let resp = handle_unknown_method(req, req_id, start_time).await;
                    resp.into_response()
                }
            };

            // For simplicity, we'll just handle one response for now
            // In a full implementation, we'd collect all responses
            return response;
        }
    }

    let duration = start_time.elapsed();
    tracing::info!(
        "✅ [{}] Batch processing completed in {:?} - {} responses, {} notifications",
        request_id,
        duration,
        responses.len(),
        if has_notifications { "with" } else { "no" }
    );

    if has_notifications && responses.is_empty() {
        // All notifications - return 202 Accepted
        let response = add_cors_headers((StatusCode::ACCEPTED, "").into_response());
        (StatusCode::ACCEPTED, response).into_response()
    } else {
        // Return the first response for now
        responses.into_iter().next().unwrap_or_else(|| {
            let error = JsonRpcErrorResponse {
                jsonrpc: "2.0".to_string(),
                error: JsonRpcError {
                    code: -32600,
                    message: "Invalid Request".to_string(),
                    data: None,
                },
                id: None,
            };
            let json_response = Json(error);
            let response = json_response.into_response();
            add_cors_headers(response)
        })
    }
}

// Helper function to extract JSON from axum response (simplified)
fn _extract_json_response(_response: axum::response::Response) -> Option<JsonRpcResponse> {
    // This is a simplified extraction - in practice you'd need to handle the response properly
    // For now, we'll return None and handle this differently
    None
}

// Health check endpoint
async fn _handle_health() -> impl IntoResponse {
    tracing::info!("🏥 Health check requested");

    // Basic health check - in a real implementation you'd check database connections, etc.
    let health_response = serde_json::json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "service": "dzr-mcp-search",
        "version": "0.1.0",
        "tools_loaded": 1, // We have 1 tool: distributed_search
        "mcp_servers": {
            "distributed_search": 1
        }
    });

    tracing::debug!("✅ Health check response prepared");
    (StatusCode::OK, Json(health_response))
}

// Status endpoint
async fn _handle_status() -> impl IntoResponse {
    tracing::info!("📊 Status requested");

    let status_response = serde_json::json!({
        "status": "running",
        "server": "dzr-mcp-search",
        "version": "0.1.0",
        "description": "MCP server for distributed code search across zoekt indexers"
    });

    tracing::debug!("✅ Status response prepared");
    (StatusCode::OK, Json(status_response))
}

// About endpoint
async fn _handle_about() -> impl IntoResponse {
    tracing::info!("ℹ️  About requested");

    let about_text = "dzr-mcp-search: MCP server for distributed code search across zoekt indexers. Supports regex search with Git credentials for private repositories.";

    tracing::debug!("✅ About response prepared");
    (StatusCode::OK, about_text)
}

async fn handle_notification(
    req: JsonRpcRequest,
    request_id: String,
    start_time: std::time::Instant,
) -> impl IntoResponse {
    tracing::info!("🔧 [{}] Handling notification: {}", request_id, req.method);

    match req.method.as_str() {
        "notifications/initialized" => {
            tracing::info!("✅ [{}] Handled notifications/initialized", request_id);
        }
        "exit" => {
            tracing::info!("✅ [{}] Handled exit notification", request_id);
        }
        _ => {
            tracing::warn!(
                "⚠️  [{}] Unknown notification method: {}",
                request_id,
                req.method
            );
        }
    }

    let duration = start_time.elapsed();
    tracing::info!(
        "⏱️  [{}] Notification processed in {:?}",
        request_id,
        duration
    );

    // Return empty response for notifications
    let response = add_cors_headers((StatusCode::ACCEPTED, "").into_response());
    (StatusCode::ACCEPTED, response).into_response()
}

async fn handle_initialize_request(
    req: JsonRpcRequest,
    request_id: String,
    start_time: std::time::Instant,
) -> impl IntoResponse {
    tracing::info!("🔧 [{}] Handling initialize request", request_id);
    let protocol_version = "2025-03-26".to_string();
    let result = InitializeResult {
        protocol_version: protocol_version.clone(),
        capabilities: ServerCapabilities {
            tools: serde_json::json!({"listChanged": true}),
            roots: Some(serde_json::json!({"listChanged": false})),
            sampling: Some(serde_json::json!({})),
            prompts: Some(serde_json::json!({"listChanged": false})),
            resources: Some(serde_json::json!({"listChanged": false})),
        },
        server_info: ServerInfo {
            name: "dzr-mcp-search".to_string(),
            version: "0.1.0".to_string(),
        },
        instructions: Some("MCP server for distributed code search across zoekt indexers. Supports regex search with Git credentials for private repositories.".to_string()),
    };

    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result: serde_json::to_value(result).unwrap(),
        id: req.id,
    };

    let duration = start_time.elapsed();
    tracing::info!(
        "✅ [{}] Initialize response prepared in {:?}",
        request_id,
        duration
    );
    let json_response = Json(response);
    let response = json_response.into_response();
    add_cors_headers(response)
}

async fn handle_tools_list_request(
    req: JsonRpcRequest,
    request_id: String,
    start_time: std::time::Instant,
) -> impl IntoResponse {
    tracing::info!("🔧 [{}] Handling tools/list request", request_id);

    let tools = vec![Tool {
        name: "distributed_search".to_string(),
        description: "Search across distributed zoekt indexers using regex patterns".to_string(),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "regex": {
                    "type": "string",
                    "description": "Regex pattern to search for"
                },
                "include": {
                    "type": "string",
                    "description": "Path patterns to include (optional glob patterns like 'src/**/*.rs')"
                },
                "exclude": {
                    "type": "string",
                    "description": "Path patterns to exclude (optional glob patterns like 'target/**')"
                },
                "repo": {
                    "type": "string",
                    "description": "Repository to search in (optional, searches all if not specified)"
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of results to return",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 1000
                },
                "context": {
                    "type": "integer",
                    "description": "Number of context lines around matches",
                    "default": 2,
                    "minimum": 0,
                    "maximum": 10
                },
                    "case_sensitive": {
                    "type": "boolean",
                    "description": "Case sensitive search",
                    "default": false
                }
                    ,
                    "limit": { "type": "integer", "description": "Page size for results (client-side)", "default": 50 },
                    "cursor": { "type": "string", "description": "Pagination cursor (offset)" }
            },
            "required": ["regex"],
            "additionalProperties": false
        }),
        output_schema: Some(serde_json::json!({
            "type": "object",
            "properties": {
                "content": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["text"]
                            },
                            "text": {
                                "type": "string"
                            }
                        },
                        "required": ["type", "text"]
                    }
                },
                "is_error": {
                    "type": "boolean"
                }
            },
            "required": ["content"]
        })),
        annotations: Some(ToolAnnotations {
            title: Some("Distributed Code Search".to_string()),
            read_only_hint: Some(true),
            destructive_hint: None,
            idempotent_hint: Some(true),
            open_world_hint: None,
        }),
    }];

    let tools_count = tools.len();
    let result = ListToolsResponse {
        tools,
        next_cursor: None,
    };

    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result: serde_json::to_value(result).unwrap(),
        id: req.id,
    };

    let duration = start_time.elapsed();
    tracing::info!(
        "✅ [{}] Tools/list response prepared with {} tools in {:?}",
        request_id,
        tools_count,
        duration
    );
    let json_response = Json(response);
    let response = json_response.into_response();
    add_cors_headers(response)
}

async fn handle_tools_call_request(
    req: JsonRpcRequest,
    request_id: String,
    start_time: std::time::Instant,
    state: AppState,
) -> impl IntoResponse {
    tracing::info!("🔧 [{}] Handling tools/call request", request_id);

    if let Some(params) = req.params {
        tracing::debug!("📋 [{}] Tools/call params: {}", request_id, params);
        if let (Some(name), Some(arguments)) = (
            params.get("name").and_then(|v| v.as_str()),
            params.get("arguments").and_then(|v| v.as_object()),
        ) {
            tracing::info!(
                "🔧 [{}] Calling tool '{}' with {} arguments",
                request_id,
                name,
                arguments.len()
            );

            // Create the handler for tool execution
            let lease_mgr = LeaseManager::new().await;
            let client = Client::new();
            let search_service = DistributedSearchService::new(lease_mgr, client);
            let handler = DistributedSearchHandler {
                search_service,
                credentials: state.credentials,
            };

            let tool_call = ToolCall {
                name: name.to_string(),
                arguments: Some(serde_json::Value::Object(arguments.clone())),
            };

            match handler.handle_tool_call(tool_call).await {
                Ok(mut result) => {
                    // Inspect trailing content for sentinel and extract next_cursor
                    let mut next_cursor: Option<String> = None;
                    if let Some(ultrafast_mcp::ToolContent::Text { text }) = result.content.last() {
                        if let Some(rest) = text.strip_prefix("__NEXT_CURSOR__:") {
                            next_cursor = Some(rest.trim().to_string());
                        }
                    }
                    if next_cursor.is_some() {
                        result.content.pop();
                    }
                    let mut value = serde_json::to_value(&result).unwrap_or_default();
                    if let Some(cursor) = next_cursor {
                        value["next_cursor"] = serde_json::Value::String(cursor);
                    }
                    let response = JsonRpcResponse {
                        jsonrpc: "2.0".to_string(),
                        result: value,
                        id: req.id,
                    };
                    let duration = start_time.elapsed();
                    tracing::info!("✅ [{}] Tool call successful in {:?}", request_id, duration);
                    let json_response = Json(response);
                    let response = json_response.into_response();
                    add_cors_headers(response)
                }
                Err(e) => {
                    tracing::error!("❌ [{}] Tool call failed: {:?}", request_id, e);
                    let error = JsonRpcError {
                        code: -32603,
                        message: format!("Tool execution failed: {:?}", e),
                        data: Some(serde_json::json!({ "tool": name })),
                    };
                    let error_response = JsonRpcErrorResponse {
                        jsonrpc: "2.0".to_string(),
                        error,
                        id: req.id,
                    };
                    let duration = start_time.elapsed();
                    tracing::info!(
                        "⏱️  [{}] Error response prepared in {:?}",
                        request_id,
                        duration
                    );
                    let json_response = Json(error_response);
                    let response = json_response.into_response();
                    let response = add_cors_headers(response);
                    (StatusCode::INTERNAL_SERVER_ERROR, response).into_response()
                }
            }
        } else {
            tracing::warn!(
                "⚠️  [{}] Invalid params for tools/call - missing name or arguments",
                request_id
            );
            let error = JsonRpcError {
                code: -32602,
                message: "Invalid params for tools/call".to_string(),
                data: Some(serde_json::json!({ "required": ["name", "arguments"] })),
            };
            let error_response = JsonRpcErrorResponse {
                jsonrpc: "2.0".to_string(),
                error,
                id: req.id,
            };
            let duration = start_time.elapsed();
            tracing::info!(
                "⏱️  [{}] Invalid params error response prepared in {:?}",
                request_id,
                duration
            );
            let json_response = Json(error_response);
            let response = json_response.into_response();
            let response = add_cors_headers(response);
            (StatusCode::BAD_REQUEST, response).into_response()
        }
    } else {
        tracing::warn!("⚠️  [{}] Missing params for tools/call", request_id);
        let error = JsonRpcError {
            code: -32602,
            message: "Missing params for tools/call".to_string(),
            data: None,
        };
        let error_response = JsonRpcErrorResponse {
            jsonrpc: "2.0".to_string(),
            error,
            id: req.id,
        };
        let duration = start_time.elapsed();
        tracing::info!(
            "⏱️  [{}] Missing params error response prepared in {:?}",
            request_id,
            duration
        );
        let json_response = Json(error_response);
        let response = json_response.into_response();
        let response = add_cors_headers(response);
        (StatusCode::BAD_REQUEST, response).into_response()
    }
}

async fn handle_shutdown_request(
    req: JsonRpcRequest,
    request_id: String,
    start_time: std::time::Instant,
) -> impl IntoResponse {
    tracing::info!("🔧 [{}] Handling shutdown request", request_id);
    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result: serde_json::Value::Null,
        id: req.id,
    };
    let duration = start_time.elapsed();
    tracing::info!(
        "✅ [{}] Shutdown response prepared in {:?}",
        request_id,
        duration
    );
    let json_response = Json(response);
    let response = json_response.into_response();
    add_cors_headers(response)
}

async fn handle_exit_request(
    req: JsonRpcRequest,
    request_id: String,
    start_time: std::time::Instant,
) -> impl IntoResponse {
    tracing::info!("🔧 [{}] Handling exit request", request_id);
    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result: serde_json::Value::Null,
        id: req.id,
    };
    let duration = start_time.elapsed();
    tracing::info!(
        "✅ [{}] Exit response prepared in {:?}",
        request_id,
        duration
    );
    let json_response = Json(response);
    let response = json_response.into_response();
    add_cors_headers(response)
}

async fn handle_ping_request(
    req: JsonRpcRequest,
    request_id: String,
    start_time: std::time::Instant,
) -> impl IntoResponse {
    tracing::info!("🔧 [{}] Handling ping request", request_id);
    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result: serde_json::Value::Null,
        id: req.id,
    };
    let duration = start_time.elapsed();
    tracing::info!(
        "✅ [{}] Ping response prepared in {:?}",
        request_id,
        duration
    );
    let json_response = Json(response);
    let response = json_response.into_response();
    add_cors_headers(response)
}

async fn handle_unknown_method(
    req: JsonRpcRequest,
    request_id: String,
    start_time: std::time::Instant,
) -> impl IntoResponse {
    tracing::warn!("⚠️  [{}] Unknown method: {}", request_id, req.method);
    let error = JsonRpcError {
        code: -32601,
        message: format!("Method '{}' not found", req.method),
        data: Some(
            serde_json::json!({ "available_methods": ["initialize", "tools/list", "tools/call", "shutdown", "exit", "$/ping"] }),
        ),
    };

    let error_response = JsonRpcErrorResponse {
        jsonrpc: "2.0".to_string(),
        error,
        id: req.id,
    };

    let duration = start_time.elapsed();
    tracing::info!(
        "⏱️  [{}] Method not found error response prepared in {:?}",
        request_id,
        duration
    );
    let json_response = Json(error_response);
    let response = json_response.into_response();
    let response = add_cors_headers(response);
    (StatusCode::METHOD_NOT_ALLOWED, response).into_response()
}

// Root endpoint redirect
async fn handle_root() -> impl IntoResponse {
    tracing::info!("🔄 Root endpoint accessed - redirecting to /mcp");
    tracing::debug!("🔄 Sending 308 Permanent Redirect to /mcp");
    Redirect::permanent("/mcp")
}

// OPTIONS handler for CORS preflight requests
async fn handle_options() -> impl IntoResponse {
    tracing::debug!("🔄 Handling OPTIONS preflight request");
    let mut response = (StatusCode::OK, "").into_response();
    response.headers_mut().insert(
        "Access-Control-Allow-Origin",
        axum::http::HeaderValue::from_static("*"),
    );
    response.headers_mut().insert(
        "Access-Control-Allow-Methods",
        axum::http::HeaderValue::from_static("POST, OPTIONS"),
    );
    response.headers_mut().insert(
        "Access-Control-Allow-Headers",
        axum::http::HeaderValue::from_static("Content-Type"),
    );
    response
}

// VSCode /v1/ compatibility endpoints
async fn handle_v1_root() -> impl IntoResponse {
    tracing::info!("🔄 /v1 root endpoint accessed");
    (StatusCode::OK, "MCP server running on /v1 endpoints")
}

async fn handle_v1_mcp_messages(
    State(credentials): State<SharedCredentials>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    tracing::info!("📨 /v1 MCP message received");

    // Extract Git credentials from headers
    extract_git_headers(&headers, &credentials).await;

    // Parse the JSON-RPC request
    let message: Result<JsonRpcMessage, _> = serde_json::from_slice(&body);

    match message {
        Ok(JsonRpcMessage::Single(req)) => handle_v1_single_request(req, credentials).await,
        Ok(JsonRpcMessage::Batch(requests)) => {
            handle_v1_batch_requests(requests, credentials).await
        }
        Err(e) => {
            tracing::error!("❌ Failed to parse JSON-RPC request: {}", e);
            let error = JsonRpcError {
                code: -32700,
                message: "Parse error".to_string(),
                data: Some(serde_json::json!({ "details": e.to_string() })),
            };
            let error_response = JsonRpcErrorResponse {
                jsonrpc: "2.0".to_string(),
                error,
                id: None,
            };
            let json_response = Json(error_response);
            let response = json_response.into_response();
            let response = add_cors_headers(response);
            (StatusCode::BAD_REQUEST, response).into_response()
        }
    }
}

async fn handle_v1_single_request(
    req: JsonRpcRequest,
    credentials: SharedCredentials,
) -> axum::response::Response {
    tracing::info!("📨 /v1 single request - method: '{}'", req.method);

    // Check if this is a notification
    if req.id.is_none() {
        tracing::info!("🔔 /v1 notification: {}", req.method);
        return handle_v1_notification(req).await.into_response();
    }

    // Handle requests
    match req.method.as_str() {
        "initialize" => {
            let response = handle_v1_initialize_request(req, credentials).await;
            response.into_response()
        }
        "tools/list" => {
            let response = handle_v1_tools_list_request(req, credentials).await;
            response.into_response()
        }
        "tools/call" => {
            let response = handle_v1_tools_call_request(req, credentials).await;
            response.into_response()
        }
        _ => {
            let response = handle_v1_unknown_method(req).await;
            response.into_response()
        }
    }
}

async fn handle_v1_batch_requests(
    requests: Vec<JsonRpcRequest>,
    credentials: SharedCredentials,
) -> axum::response::Response {
    tracing::info!("📦 /v1 batch request with {} items", requests.len());

    let mut responses = Vec::new();

    for req in requests {
        if req.id.is_none() {
            // Handle notification
            handle_v1_notification(req).await;
        } else {
            // Handle request and collect response
            let response: axum::response::Response = match req.method.as_str() {
                "initialize" => {
                    let resp = handle_v1_initialize_request(req, credentials.clone()).await;
                    resp.into_response()
                }
                "tools/list" => {
                    let resp = handle_v1_tools_list_request(req, credentials.clone()).await;
                    resp.into_response()
                }
                "tools/call" => {
                    let resp = handle_v1_tools_call_request(req, credentials.clone()).await;
                    resp.into_response()
                }
                _ => {
                    let resp = handle_v1_unknown_method(req).await;
                    resp.into_response()
                }
            };
            responses.push(response);
        }
    }

    if responses.is_empty() {
        // All notifications
        let response = add_cors_headers((StatusCode::ACCEPTED, "").into_response());
        (StatusCode::ACCEPTED, response).into_response()
    } else {
        // Return first response for simplicity
        responses.into_iter().next().unwrap()
    }
}

async fn handle_v1_notification(req: JsonRpcRequest) -> impl IntoResponse {
    tracing::info!("🔔 /v1 notification: {}", req.method);

    match req.method.as_str() {
        "notifications/initialized" => {
            tracing::info!("✅ /v1 notifications/initialized handled");
        }
        _ => {
            tracing::warn!("⚠️  /v1 unknown notification: {}", req.method);
        }
    }

    let response = add_cors_headers((StatusCode::ACCEPTED, "").into_response());
    (StatusCode::ACCEPTED, response).into_response()
}

async fn handle_v1_initialize_request(
    req: JsonRpcRequest,
    _credentials: SharedCredentials,
) -> impl IntoResponse {
    tracing::info!("🔧 /v1 initialize request");

    let result = serde_json::json!({
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {"listChanged": true}
        },
        "serverInfo": {"name": "dzr-mcp-search", "version": "0.1.0"}
    });

    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result,
        id: req.id,
    };

    tracing::info!("✅ /v1 initialize response prepared");
    let json_response = Json(response);
    let response = json_response.into_response();
    add_cors_headers(response)
}

async fn handle_v1_tools_list_request(
    req: JsonRpcRequest,
    _credentials: SharedCredentials,
) -> impl IntoResponse {
    tracing::info!("🔧 /v1 tools/list request");

    let tools = vec![serde_json::json!({
        "name": "distributed_search",
        "description": "Search across distributed zoekt indexers using regex patterns",
        "inputSchema": {
            "type": "object",
            "properties": {
                "regex": {
                    "type": "string",
                    "description": "Regex pattern to search for"
                },
                "include": {
                    "type": "string",
                    "description": "Path patterns to include (optional glob patterns like 'src/**/*.rs')"
                },
                "exclude": {
                    "type": "string",
                    "description": "Path patterns to exclude (optional glob patterns like 'target/**')"
                },
                "repo": {
                    "type": "string",
                    "description": "Repository to search in (optional, searches all if not specified)"
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of results to return",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 1000
                },
                "context": {
                    "type": "integer",
                    "description": "Number of context lines around matches",
                    "default": 2,
                    "minimum": 0,
                    "maximum": 10
                },
                "case_sensitive": {
                    "type": "boolean",
                    "description": "Case sensitive search",
                    "default": false
                }
            },
            "required": ["regex"],
            "additionalProperties": false
        }
    })];

    let result = serde_json::json!({
        "tools": tools
    });

    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result,
        id: req.id,
    };

    tracing::info!("✅ /v1 tools/list response prepared");
    let json_response = Json(response);
    let response = json_response.into_response();
    add_cors_headers(response)
}

async fn handle_v1_tools_call_request(
    req: JsonRpcRequest,
    _credentials: SharedCredentials,
) -> impl IntoResponse {
    tracing::info!("🔧 /v1 tools/call request");

    if let Some(params) = req.params {
        if let (Some(name), Some(arguments)) = (
            params.get("name").and_then(|v| v.as_str()),
            params.get("arguments").and_then(|v| v.as_object()),
        ) {
            tracing::info!(
                "🔧 /v1 calling tool '{}' with {} arguments",
                name,
                arguments.len()
            );

            // For now, return a simple success response
            // In a full implementation, this would execute the actual tool
            let result = serde_json::json!({
                "content": [
                    {
                        "type": "text",
                        "text": format!("Tool '{}' called successfully with {} arguments", name, arguments.len())
                    }
                ],
                "is_error": false
            });

            let response = JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result,
                id: req.id,
            };

            tracing::info!("✅ /v1 tool call response prepared");
            let json_response = Json(response);
            let response = json_response.into_response();
            add_cors_headers(response)
        } else {
            let error = JsonRpcError {
                code: -32602,
                message: "Invalid params for tools/call".to_string(),
                data: Some(serde_json::json!({ "required": ["name", "arguments"] })),
            };
            let error_response = JsonRpcErrorResponse {
                jsonrpc: "2.0".to_string(),
                error,
                id: req.id,
            };
            let json_response = Json(error_response);
            let response = json_response.into_response();
            let response = add_cors_headers(response);
            (StatusCode::BAD_REQUEST, response).into_response()
        }
    } else {
        let error = JsonRpcError {
            code: -32602,
            message: "Missing params for tools/call".to_string(),
            data: None,
        };
        let error_response = JsonRpcErrorResponse {
            jsonrpc: "2.0".to_string(),
            error,
            id: req.id,
        };
        let json_response = Json(error_response);
        let response = json_response.into_response();
        let response = add_cors_headers(response);
        (StatusCode::BAD_REQUEST, response).into_response()
    }
}

async fn handle_v1_unknown_method(req: JsonRpcRequest) -> impl IntoResponse {
    tracing::warn!("⚠️  /v1 unknown method: {}", req.method);
    let error = JsonRpcError {
        code: -32601,
        message: format!("Method '{}' not found", req.method),
        data: Some(
            serde_json::json!({ "available_methods": ["initialize", "tools/list", "tools/call"] }),
        ),
    };
    let error_response = JsonRpcErrorResponse {
        jsonrpc: "2.0".to_string(),
        error,
        id: req.id,
    };
    let json_response = Json(error_response);
    let response = json_response.into_response();
    let response = add_cors_headers(response);
    (StatusCode::METHOD_NOT_ALLOWED, response).into_response()
}

async fn handle_v1_initialize(
    State(credentials): State<SharedCredentials>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<serde_json::Value>,
) -> impl IntoResponse {
    tracing::info!("🔧 /v1/initialize endpoint called");

    // Extract Git credentials from headers
    extract_git_headers(&headers, &credentials).await;

    let request_id = body.get("id").and_then(|v| v.as_u64()).unwrap_or(1);

    // VSCode-compatible initialize response
    let response = serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {"listChanged": true}
            },
            "serverInfo": {"name": "mcp-server", "version": "0.1.0"}
        }
    });

    tracing::info!("✅ /v1/initialize response prepared");
    let json_response = axum::response::Json(response);
    let response = json_response.into_response();
    add_cors_headers(response)
}

async fn handle_v1_tools_list(
    State(credentials): State<SharedCredentials>,
    headers: HeaderMap,
) -> impl IntoResponse {
    tracing::info!("🔧 /v1/tools/list endpoint called");

    // Extract Git credentials from headers
    extract_git_headers(&headers, &credentials).await;

    // Create the tool definition
    let tools = vec![serde_json::json!({
        "name": "distributed_search",
        "description": "Search across distributed zoekt indexers using regex patterns",
        "inputSchema": {
            "type": "object",
            "properties": {
                "regex": {
                    "type": "string",
                    "description": "Regex pattern to search for"
                },
                "include": {
                    "type": "string",
                    "description": "Path patterns to include (optional glob patterns like 'src/**/*.rs')"
                },
                "exclude": {
                    "type": "string",
                    "description": "Path patterns to exclude (optional glob patterns like 'target/**')"
                },
                "repo": {
                    "type": "string",
                    "description": "Repository to search in (optional, searches all if not specified)"
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of results to return",
                    "default": 100,
                    "minimum": 1,
                    "maximum": 1000
                },
                "context": {
                    "type": "integer",
                    "description": "Number of context lines around matches",
                    "default": 2,
                    "minimum": 0,
                    "maximum": 10
                },
                "case_sensitive": {
                    "type": "boolean",
                    "description": "Case sensitive search",
                    "default": false
                }
            },
            "required": ["regex"],
            "additionalProperties": false
        }
    })];

    let response = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "tools": tools
        }
    });

    tracing::info!(
        "✅ /v1/tools/list response prepared with {} tools",
        tools.len()
    );
    let json_response = axum::response::Json(response);
    let response = json_response.into_response();
    add_cors_headers(response)
}

// Helper function to add CORS headers to responses
fn add_cors_headers(mut response: axum::response::Response) -> axum::response::Response {
    response.headers_mut().insert(
        "Access-Control-Allow-Origin",
        axum::http::HeaderValue::from_static("*"),
    );
    response.headers_mut().insert(
        "Access-Control-Allow-Methods",
        axum::http::HeaderValue::from_static("POST, OPTIONS"),
    );
    response.headers_mut().insert(
        "Access-Control-Allow-Headers",
        axum::http::HeaderValue::from_static("Content-Type"),
    );
    response
}

#[derive(Parser)]
struct Opts {
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    #[arg(long)]
    id: Option<String>,
    #[arg(long)]
    lease_ttl_seconds: Option<u64>,
    #[arg(long)]
    poll_interval_seconds: Option<u64>,
    /// Address to listen on for HTTP search (env: ZOEKTD_BIND_ADDR)
    #[arg(long)]
    host: Option<String>,
    /// Port to listen on for HTTP search
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let server_start = std::time::Instant::now();

    // Initialize tracing using the RUST_LOG env var when present, default to `info`
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,hyper_util=warn,hyper=warn,h2=warn,reqwest=warn,tower_http=warn,ignore=warn",
        )
    });
    let filter_str = filter.to_string();
    tracing_subscriber::fmt().with_env_filter(filter).init();

    tracing::info!("🚀 Starting dzr-mcp-search MCP server...");
    tracing::info!("📋 Log level: {}", filter_str);

    let opts = Opts::parse();
    tracing::info!("⚙️  Command line options parsed");

    let _cfg = zoekt_distributed::load_node_config(
        NodeConfig {
            node_type: NodeType::Search,
            ..Default::default()
        },
        zoekt_distributed::MergeOpts {
            config_path: opts.config,
            cli_id: opts.id,
            cli_lease_ttl_seconds: opts.lease_ttl_seconds,
            cli_poll_interval_seconds: opts.poll_interval_seconds,
            cli_endpoint: None,
            cli_enable_reindex: None,
            cli_index_once: None,
        },
    )?;
    tracing::info!("✅ Node configuration loaded");

    let lease_mgr = LeaseManager::new().await;
    tracing::info!("✅ Lease manager initialized");

    let client = Client::new();
    tracing::info!("✅ HTTP client created");

    // Create shared credentials store
    let credentials = Arc::new(RwLock::new(GitCredentials::default()));
    tracing::info!("✅ Shared credentials store initialized");

    // Create the MCP handler (for future integration with UltraFastServer)
    let search_service = DistributedSearchService::new(lease_mgr.clone(), client.clone());
    tracing::info!("✅ Distributed search service created");

    let _handler = DistributedSearchHandler {
        search_service,
        credentials: credentials.clone(),
    };
    tracing::info!("✅ MCP handler created");

    // Determine bind address
    let host = opts
        .host
        .or_else(|| std::env::var("ZOEKTD_HOST").ok())
        .unwrap_or_else(|| "127.0.0.1".into());
    let port = opts
        .port
        .or_else(|| {
            std::env::var("ZOEKTD_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(8081);

    tracing::info!(host = %host, port = %port, "🌐 Server configuration");
    tracing::info!("🚀 Starting dzr-mcp-search MCP server on {}:{}", host, port);

    // Create shared state for the application
    let app_state = AppState {
        credentials: credentials.clone(),
        sessions: Arc::new(RwLock::new(HashMap::new())),
    };
    tracing::info!("✅ Application state initialized");

    // For demonstration: Create a simple Axum server that shows header extraction
    // In production, you would integrate this with UltraFastServer's transport layer

    // Main MCP router with AppState
    let main_router = Router::new()
        .route("/", get(handle_root))
        .route("/mcp", post(handle_mcp_with_headers))
        .route("/mcp", options(handle_options))
        .with_state(app_state);

    // V1 compatibility router with SharedCredentials
    let v1_router = Router::new()
        .route("/v1", get(handle_v1_root))
        .route("/v1", post(handle_v1_mcp_messages))
        .route("/v1/initialize", post(handle_v1_initialize))
        .route("/v1/tools/list", get(handle_v1_tools_list))
        .route("/v1/tools/list", post(handle_v1_tools_list))
        .with_state(credentials.clone());

    // Merge the routers
    let app = main_router.merge(v1_router);

    tracing::info!("🛣️  Routes configured:");
    tracing::info!("   GET  /     -> handle_root (redirect to /mcp)");
    tracing::info!("   POST /mcp  -> handle_mcp_with_headers (main MCP endpoint)");
    tracing::info!("   OPTIONS /mcp -> handle_options (CORS preflight)");
    tracing::info!("   GET  /v1     -> handle_v1_root (VSCode compatibility)");
    tracing::info!("   POST /v1     -> handle_v1_mcp_messages (VSCode compatibility)");
    tracing::info!("   POST /v1/initialize  -> handle_v1_initialize (VSCode compatibility)");
    tracing::info!("   GET  /v1/tools/list  -> handle_v1_tools_list (VSCode compatibility)");
    tracing::info!("   POST /v1/tools/list  -> handle_v1_tools_list (VSCode compatibility)");

    let addr: std::net::SocketAddr = format!("{}:{}", host, port).parse().unwrap();
    tracing::info!("🔌 Attempting to bind to {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("✅ Successfully bound to {}", addr);

    let startup_duration = server_start.elapsed();
    tracing::info!("🎉 Server startup completed in {:?}", startup_duration);
    tracing::info!("🌐 Server listening on http://{}", addr);
    tracing::info!("📡 Ready to accept MCP connections from VSCode and other clients");
    tracing::info!("💡 Supported endpoints:");
    tracing::info!("   - initialize");
    tracing::info!("   - tools/list");
    tracing::info!("   - tools/call (distributed_search)");
    tracing::info!("   - notifications/initialized");
    tracing::info!("   - shutdown");
    tracing::info!("   - exit");
    tracing::info!("   - $/ping");

    axum::serve(listener, app).await.unwrap();

    Ok(())
}
