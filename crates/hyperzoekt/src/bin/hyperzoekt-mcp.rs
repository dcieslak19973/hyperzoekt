// HyperZoekt MCP server: exposes similarity search and repo dupes tools using internal logic
use anyhow::Result;
use async_trait::async_trait;
use axum::{extract::State, http::HeaderMap, response::IntoResponse, routing::post, Json, Router};
use hyperzoekt::db_writer::connection::{connect, SurrealConnection};
use hyperzoekt::{repo_index::indexer::payload::EntityPayload, similarity};
use serde::Deserialize;
use std::sync::Arc;
use ultrafast_mcp::{
    ListToolsRequest, ListToolsResponse, MCPError, MCPResult, Tool, ToolAnnotations, ToolCall,
    ToolContent, ToolHandler, ToolResult,
};

#[derive(Clone)]
struct AppState {
    db: Arc<SurrealConnection>,
    webui_base: String,
    http: reqwest::Client,
}

#[derive(Clone)]
struct HZHandler {
    state: AppState,
}

#[async_trait]
impl ToolHandler for HZHandler {
    async fn handle_tool_call(&self, call: ToolCall) -> MCPResult<ToolResult> {
        match call.name.as_str() {
            "similarity_search" => {
                let args = call.arguments.clone().unwrap_or_default();
                let q = args
                    .get("q")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| MCPError::invalid_params("missing q".into()))?;
                let repo = args.get("repo").and_then(|v| v.as_str());
                let results: Vec<EntityPayload> =
                    similarity::similarity_with_conn(&self.state.db, q, repo)
                        .await
                        .map_err(|e| MCPError::internal_error(e.to_string()))?;
                let mut content = Vec::new();
                if results.is_empty() {
                    content.push(ToolContent::text("No similar entities found.".into()));
                }
                for e in results.iter().take(50) {
                    let name = &e.name;
                    let repo_name = &e.repo_name;
                    let file = e.source_display.as_deref().unwrap_or("");
                    let line = e.start_line.unwrap_or(0);
                    content.push(ToolContent::text(format!(
                        "{} | {} | {}:{}",
                        repo_name, name, file, line
                    )));
                }
                Ok(ToolResult {
                    content,
                    is_error: Some(false),
                })
            }
            "repo_dupes" => {
                let args = call.arguments.clone().unwrap_or_default();
                let repo = args
                    .get("repo")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| MCPError::invalid_params("missing repo".into()))?;
                let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
                let min_score = args.get("min_score").and_then(|v| v.as_f64());
                let language = args.get("language").and_then(|v| v.as_str());
                let kind = args.get("kind").and_then(|v| v.as_str());
                let base = self.state.webui_base.trim_end_matches('/');
                let url_in = format!("{}/api/repo/{}/dupes", base, repo);
                let url_ex = format!("{}/api/repo/{}/external_dupes", base, repo);
                let mut qparams: Vec<(&str, String)> = vec![("limit", limit.to_string())];
                if let Some(ms) = min_score {
                    qparams.push(("min_score", ms.to_string()));
                }
                if let Some(l) = language {
                    qparams.push(("language", l.to_string()));
                }
                if let Some(k) = kind {
                    qparams.push(("kind", k.to_string()));
                }
                let (r_in, r_ex) = tokio::join!(
                    self.state.http.get(&url_in).query(&qparams).send(),
                    self.state.http.get(&url_ex).query(&qparams).send()
                );
                let mut content = Vec::new();
                match (r_in, r_ex) {
                    (Ok(in_res), Ok(ex_res)) => {
                        let in_json: serde_json::Value = in_res
                            .json()
                            .await
                            .map_err(|e| MCPError::internal_error(e.to_string()))?;
                        let ex_json: serde_json::Value = ex_res
                            .json()
                            .await
                            .map_err(|e| MCPError::internal_error(e.to_string()))?;
                        let in_pairs = in_json
                            .get("pairs")
                            .and_then(|v| v.as_array())
                            .cloned()
                            .unwrap_or_default();
                        let ex_pairs = ex_json
                            .get("pairs")
                            .and_then(|v| v.as_array())
                            .cloned()
                            .unwrap_or_default();
                        content.push(ToolContent::text(format!(
                            "Within Repo pairs: {}",
                            in_pairs.len()
                        )));
                        for p in in_pairs.iter().take(50) {
                            let a = p
                                .get("a")
                                .and_then(|v| v.as_object())
                                .cloned()
                                .unwrap_or_default();
                            let b = p
                                .get("b")
                                .and_then(|v| v.as_object())
                                .cloned()
                                .unwrap_or_default();
                            let score = p.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
                            let an = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
                            let bn = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                            let af = a
                                .get("source_display")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            let al = a.get("start_line").and_then(|v| v.as_u64()).unwrap_or(0);
                            let bf = b
                                .get("source_display")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            let bl = b.get("start_line").and_then(|v| v.as_u64()).unwrap_or(0);
                            let au = a.get("source_url").and_then(|v| v.as_str()).unwrap_or("");
                            let bu = b.get("source_url").and_then(|v| v.as_str()).unwrap_or("");
                            let line = if !au.is_empty() && !bu.is_empty() {
                                format!("{an} ~ {bn} (score {score:.3})\n- {af}:{al} -> {au}\n- {bf}:{bl} -> {bu}")
                            } else {
                                format!("{an} ~ {bn} (score {score:.3})\n- {af}:{al}\n- {bf}:{bl}")
                            };
                            content.push(ToolContent::text(line));
                        }
                        content.push(ToolContent::text(format!(
                            "Cross Repo pairs: {}",
                            ex_pairs.len()
                        )));
                        for p in ex_pairs.iter().take(50) {
                            let a = p
                                .get("a")
                                .and_then(|v| v.as_object())
                                .cloned()
                                .unwrap_or_default();
                            let b = p
                                .get("b")
                                .and_then(|v| v.as_object())
                                .cloned()
                                .unwrap_or_default();
                            let score = p.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0);
                            let an = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
                            let ar = a.get("repo_name").and_then(|v| v.as_str()).unwrap_or("");
                            let bn = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                            let br = b.get("repo_name").and_then(|v| v.as_str()).unwrap_or("");
                            let af = a
                                .get("source_display")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            let al = a.get("start_line").and_then(|v| v.as_u64()).unwrap_or(0);
                            let bf = b
                                .get("source_display")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            let bl = b.get("start_line").and_then(|v| v.as_u64()).unwrap_or(0);
                            let au = a.get("source_url").and_then(|v| v.as_str()).unwrap_or("");
                            let bu = b.get("source_url").and_then(|v| v.as_str()).unwrap_or("");
                            let line = if !au.is_empty() && !bu.is_empty() {
                                format!("{an} [{ar}] ~ {bn} [{br}] (score {score:.3})\n- {af}:{al} -> {au}\n- {bf}:{bl} -> {bu}")
                            } else {
                                format!("{an} [{ar}] ~ {bn} [{br}] (score {score:.3})\n- {af}:{al}\n- {bf}:{bl}")
                            };
                            content.push(ToolContent::text(line));
                        }
                        Ok(ToolResult {
                            content,
                            is_error: Some(false),
                        })
                    }
                    (Err(e), _) | (_, Err(e)) => Err(MCPError::internal_error(e.to_string())),
                }
            }
            _ => Err(MCPError::method_not_found(format!(
                "Unknown tool: {}",
                call.name
            ))),
        }
    }

    async fn list_tools(&self, _request: ListToolsRequest) -> MCPResult<ListToolsResponse> {
        let tools = vec![
            Tool {
                name: "similarity_search".into(),
                description: "Find entities similar to a query using embeddings".into(),
                input_schema: serde_json::json!({
                    "type": "object", "properties": { "q": {"type": "string"}, "repo": {"type": "string"} }, "required": ["q"]
                }),
                output_schema: None,
                annotations: Some(ToolAnnotations::default()),
            },
            Tool {
                name: "repo_dupes".into(),
                description: "List duplicate-like pairs within and across a repository".into(),
                input_schema: serde_json::json!({
                    "type": "object", "properties": { "repo": {"type": "string"}, "limit": {"type": "integer"}, "min_score": {"type": "number"}, "language": {"type": "string"}, "kind": {"type": "string"} }, "required": ["repo"]
                }),
                output_schema: None,
                annotations: Some(ToolAnnotations::default()),
            },
        ];
        Ok(ListToolsResponse {
            tools,
            next_cursor: None,
        })
    }
}

// Minimal JSON-RPC request/response types
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

async fn handle_mcp(
    State(state): State<AppState>,
    _headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let handler = HZHandler { state };
    let parsed: Result<JsonRpcMessage, _> = serde_json::from_slice(&body);
    match parsed {
        Ok(JsonRpcMessage::Single(req)) => handle_single(handler, req).await,
        Ok(JsonRpcMessage::Batch(reqs)) => {
            let mut out = Vec::new();
            for req in reqs {
                out.push(
                    handle_single(handler.clone(), req)
                        .await
                        .into_response_json(),
                );
            }
            axum::response::Json(serde_json::Value::Array(out))
        }
        Err(e) => Json(
            serde_json::json!({ "jsonrpc": "2.0", "error": {"code": -32700, "message": format!("Parse error: {}", e)}, "id": null }),
        ),
    }
}

trait IntoResponseJson {
    fn into_response_json(self) -> serde_json::Value;
}
impl IntoResponseJson for Json<serde_json::Value> {
    fn into_response_json(self) -> serde_json::Value {
        self.0
    }
}

async fn handle_single(handler: HZHandler, req: JsonRpcRequest) -> Json<serde_json::Value> {
    match req.method.as_str() {
        "initialize" => Json(serde_json::json!({
            "jsonrpc":"2.0", "result": { "protocol_version": "2024-11-05", "capabilities": {"tools": {}} , "server_info": {"name":"hyperzoekt-mcp","version":"0.1.0"}}, "id": req.id
        })),
        "tools/list" => match handler.list_tools(ListToolsRequest { cursor: None }).await {
            Ok(resp) => Json(
                serde_json::json!({"jsonrpc":"2.0", "result": serde_json::to_value(resp).unwrap(), "id": req.id}),
            ),
            Err(e) => Json(
                serde_json::json!({"jsonrpc":"2.0", "error": {"code": -32000, "message": e.to_string()}, "id": req.id}),
            ),
        },
        "tools/call" => {
            if let Some(params) = req.params.as_ref() {
                if let (Some(name), Some(arguments)) = (params.get("name"), params.get("arguments"))
                {
                    let tc = ToolCall {
                        name: name.as_str().unwrap_or("").to_string(),
                        arguments: arguments
                            .as_object()
                            .cloned()
                            .map(serde_json::Value::Object),
                    };
                    match handler.handle_tool_call(tc).await {
                        Ok(res) => Json(
                            serde_json::json!({"jsonrpc":"2.0", "result": serde_json::to_value(res).unwrap(), "id": req.id}),
                        ),
                        Err(e) => Json(
                            serde_json::json!({"jsonrpc":"2.0", "error": {"code": -32000, "message": e.to_string()}, "id": req.id}),
                        ),
                    }
                } else {
                    Json(
                        serde_json::json!({"jsonrpc":"2.0", "error": {"code": -32602, "message": "Invalid params for tools/call"}, "id": req.id}),
                    )
                }
            } else {
                Json(
                    serde_json::json!({"jsonrpc":"2.0", "error": {"code": -32602, "message": "Missing params for tools/call"}, "id": req.id}),
                )
            }
        }
        _ => Json(
            serde_json::json!({"jsonrpc":"2.0", "error": {"code": -32601, "message": format!("Unknown method: {}", req.method)}, "id": req.id}),
        ),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    // DB connection for internal similarity logic
    let url = std::env::var("SURREALDB_URL").ok();
    let user = std::env::var("SURREALDB_USERNAME").ok();
    let pass = std::env::var("SURREALDB_PASSWORD").ok();
    let ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
    let dbn = std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());
    let db = connect(&url, &user, &pass, &ns, &dbn).await?;

    let webui_base =
        std::env::var("HZ_WEBUI_BASE").unwrap_or_else(|_| "http://127.0.0.1:7878".into());
    let state = AppState {
        db: Arc::new(db),
        webui_base,
        http: reqwest::Client::new(),
    };
    let app = Router::new()
        .route("/mcp", post(handle_mcp))
        .with_state(state);
    let host = std::env::var("HZ_MCP_HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let port: u16 = std::env::var("HZ_MCP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7979);
    let addr: std::net::SocketAddr = format!("{}:{}", host, port)
        .parse()
        .expect("invalid HZ_MCP_HOST/HZ_MCP_PORT");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
