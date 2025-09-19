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

// Local helper duplicated from db_writer::helpers (kept minimal) to avoid changing visibility.
fn extract_surreal_id(v: &serde_json::Value) -> Option<String> {
    if v.is_null() {
        return None;
    }
    if let Some(s) = v.as_str() {
        return Some(s.to_string());
    }
    if let Some(obj) = v.as_object() {
        if let (Some(tb_val), Some(id_val)) = (obj.get("tb"), obj.get("id")) {
            if let (Some(tb), Some(id)) = (tb_val.as_str(), id_val.as_str()) {
                return Some(format!("{}:{}", tb, id));
            }
        }
        if let Some(id_field) = obj.get("id").and_then(|x| x.as_str()) {
            if id_field.contains(':') {
                return Some(id_field.to_string());
            }
        }
    }
    None
}

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

fn paginate(total: usize, start: usize, limit: usize) -> (usize, usize, Option<String>) {
    let end = (start + limit).min(total);
    let next = if end < total {
        Some(end.to_string())
    } else {
        None
    };
    (start, end, next)
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
                let results: Vec<EntityPayload> =
                    similarity::similarity_with_conn(&self.state.db, q, repo)
                        .await
                        .map_err(|e| MCPError::internal_error(e.to_string()))?;
                let (s, e, next) = paginate(results.len(), start, limit);
                let mut content = Vec::new();
                if s == 0 {
                    content.push(ToolContent::text(format!(
                        "total={} window={}..{}",
                        results.len(),
                        s,
                        e
                    )));
                }
                if results.is_empty() {
                    content.push(ToolContent::text("No similar entities found.".into()));
                }
                for r in results[s..e].iter() {
                    let file = r.source_display.as_deref().unwrap_or("");
                    let line = r.start_line.unwrap_or(0);
                    content.push(ToolContent::text(format!(
                        "{} | {} | {}:{}",
                        r.repo_name, r.name, file, line
                    )));
                }
                if let Some(nc) = next {
                    content.push(ToolContent::text(format!("__NEXT_CURSOR__:{nc}")));
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
                let min_score = args.get("min_score").and_then(|v| v.as_f64());
                let language = args.get("language").and_then(|v| v.as_str());
                let kind = args.get("kind").and_then(|v| v.as_str());
                let base = self.state.webui_base.trim_end_matches('/');
                let url_in = format!("{}/api/repo/{}/dupes", base, repo);
                let url_ex = format!("{}/api/repo/{}/external_dupes", base, repo);
                // We ask backend for larger window; we paginate client-side for now.
                let mut qparams: Vec<(&str, String)> =
                    vec![("limit", (start + limit + 200).to_string())];
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
                        // Merge two categories into a single vector of printable lines with a prefix so we can paginate.
                        let mut lines: Vec<String> = Vec::new();
                        lines.push(format!("Within Repo pairs: {}", in_pairs.len()));
                        for p in &in_pairs {
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
                            lines.push(line);
                        }
                        lines.push(format!("Cross Repo pairs: {}", ex_pairs.len()));
                        for p in &ex_pairs {
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
                            lines.push(line);
                        }
                        let (s, e, next) = paginate(lines.len(), start, limit);
                        if s == 0 {
                            content.push(ToolContent::text(format!(
                                "total={} window={}..{}",
                                lines.len(),
                                s,
                                e
                            )));
                        }
                        for line in &lines[s..e] {
                            content.push(ToolContent::text(line.clone()));
                        }
                        if let Some(nc) = next {
                            content.push(ToolContent::text(format!("__NEXT_CURSOR__:{nc}")));
                        }
                        Ok(ToolResult {
                            content,
                            is_error: Some(false),
                        })
                    }
                    (Err(e), _) | (_, Err(e)) => Err(MCPError::internal_error(e.to_string())),
                }
            }
            "repo_dependencies" => {
                let args = call.arguments.clone().unwrap_or_default();
                let repo = args
                    .get("repo")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| MCPError::invalid_params("missing repo".into()))?;
                let start = args
                    .get("cursor")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let limit = args
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as usize)
                    .unwrap_or(100)
                    .clamp(1, 500);
                // Optional branch or tag override
                let branch = args.get("branch").and_then(|v| v.as_str());
                // Default branch fallback logic (main) applied only if branch not provided.
                let branch_or_default = branch.unwrap_or("main");
                // Query strategy:
                // 1. Find repo id by name (and optional branch match). If not found with branch, fallback to name only.
                // 2. Use graph traversal: SELECT -> (->depends_on)->dependency to retrieve dependency info.
                // Using Surreal SQL: SELECT ->depends_on.out as dep FROM <repo_record> FETCH dep;
                // We construct deterministic repo id candidate using same sanitize logic in repo_deps (approximation here).
                fn sanitize_id(s: &str) -> String {
                    let mut out = String::with_capacity(s.len());
                    for ch in s.chars() {
                        if ch.is_ascii_alphanumeric() {
                            out.push(ch);
                        } else {
                            out.push('_');
                        }
                    }
                    let trimmed = out.trim_matches('_');
                    if trimmed.is_empty() {
                        "_".to_string()
                    } else {
                        trimmed.to_string()
                    }
                }
                let candidate_repo_key = sanitize_id(repo);
                // Attempt select by name + branch first
                let mut repo_id: Option<String> = None;
                // Query by name & branch (branch may be null in DB so also attempt OR branch is null if default branch requested)
                let name_branch_sql = "SELECT id FROM repo WHERE name = $name AND (branch = $branch OR $branch IS NULL) LIMIT 1;";
                if let Ok(mut r) = self
                    .state
                    .db
                    .query_with_binds(
                        name_branch_sql,
                        vec![("name", repo.into()), ("branch", branch_or_default.into())],
                    )
                    .await
                {
                    if let Ok(rows) = r.take::<Vec<serde_json::Value>>(0) {
                        if let Some(first) = rows.first() {
                            if let Some(idv) = first.get("id") {
                                repo_id = extract_surreal_id(idv);
                            }
                        }
                    }
                }
                // Fallback: by name only
                if repo_id.is_none() {
                    if let Ok(mut r) = self
                        .state
                        .db
                        .query_with_binds(
                            "SELECT id FROM repo WHERE name = $name LIMIT 1;",
                            vec![("name", repo.into())],
                        )
                        .await
                    {
                        if let Ok(rows) = r.take::<Vec<serde_json::Value>>(0) {
                            if let Some(first) = rows.first() {
                                if let Some(idv) = first.get("id") {
                                    repo_id = extract_surreal_id(idv);
                                }
                            }
                        }
                    }
                }
                // Fallback: deterministic id assumption
                if repo_id.is_none() {
                    repo_id = Some(format!("repo:{}", candidate_repo_key));
                }
                let rid = repo_id.unwrap();
                // Traverse depends_on edges. Surreal relation form: repo:<key>->depends_on->dependency
                // We use a RELATE-style traversal query to gather dependencies.
                // Two strategies: direct graph traversal or explicit depends_on table scan.
                // We'll try traversal first; if empty, attempt explicit table scan referencing in/out.
                let traversal_sql = format!(
                    "SELECT ->depends_on->dependency as deps FROM {} LIMIT 1;",
                    rid
                );
                let mut deps: Vec<serde_json::Value> = Vec::new();
                if let Ok(mut qres) = self.state.db.query(traversal_sql.as_str()).await {
                    if let Ok(rows) = qres.take::<Vec<serde_json::Value>>(0) {
                        if let Some(first) = rows.first() {
                            if let Some(arr) = first.get("deps").and_then(|v| v.as_array()) {
                                deps = arr.clone();
                            }
                        }
                    }
                }
                if deps.is_empty() {
                    // fallback explicit depends_on table: SELECT out.* FROM depends_on WHERE in = <repo_record>
                    let explicit_sql =
                        format!("SELECT out.* FROM depends_on WHERE in = {} LIMIT 500;", rid);
                    if let Ok(mut q2) = self.state.db.query(explicit_sql.as_str()).await {
                        // attempt both json value and sql value extraction paths
                        if let Ok(rows) = q2.take::<Vec<serde_json::Value>>(0) {
                            for r in rows {
                                if r.is_object() {
                                    deps.push(r);
                                }
                            }
                        }
                    }
                }
                let mut lines: Vec<String> = Vec::new();
                if deps.is_empty() {
                    lines.push(format!(
                        "No dependencies found for repo {} (branch {}).",
                        repo, branch_or_default
                    ));
                } else {
                    lines.push(format!(
                        "Found {} dependencies for repo {} (branch {}).",
                        deps.len(),
                        repo,
                        branch_or_default
                    ));
                    for d in &deps {
                        // we'll paginate after building
                        let name = d.get("name").and_then(|v| v.as_str()).unwrap_or("");
                        let lang = d.get("language").and_then(|v| v.as_str()).unwrap_or("");
                        let ver = d.get("version").and_then(|v| v.as_str()).unwrap_or("");
                        lines.push(format!("{name} {lang} {ver}"));
                    }
                }
                let (s, e, next) = paginate(lines.len(), start, limit);
                let mut content = Vec::new();
                if s == 0 {
                    content.push(ToolContent::text(format!(
                        "total={} window={}..{}",
                        lines.len(),
                        s,
                        e
                    )));
                }
                for line in &lines[s..e] {
                    content.push(ToolContent::text(line.clone()));
                }
                if let Some(nc) = next {
                    content.push(ToolContent::text(format!("__NEXT_CURSOR__:{nc}")));
                }
                Ok(ToolResult {
                    content,
                    is_error: Some(false),
                })
            }
            "call_graph" => {
                let args = call.arguments.clone().unwrap_or_default();
                let start = args
                    .get("cursor")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let limit = args
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as usize)
                    .unwrap_or(100)
                    .clamp(1, 500);
                // Resolve stable_id either directly or via name+repo lookup
                let stable_id_opt = args
                    .get("stable_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let stable_id = if let Some(sid) = stable_id_opt {
                    sid
                } else {
                    // name+repo resolution: try to find entity by name within repo via SurrealDB
                    let name = args.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
                        MCPError::invalid_params("missing stable_id or (name+repo)".into())
                    })?;
                    let repo = args.get("repo").and_then(|v| v.as_str()).ok_or_else(|| {
                        MCPError::invalid_params("missing stable_id or (name+repo)".into())
                    })?;
                    // Simple lookup: SELECT stable_id FROM entity WHERE name = $name AND repo_name = $repo LIMIT 1
                    let sql = "SELECT stable_id FROM entity WHERE name = $name AND repo_name = $repo LIMIT 1;";
                    let mut sid_candidate: Option<String> = None;
                    if let Ok(mut q) = self
                        .state
                        .db
                        .query_with_binds(sql, vec![("name", name.into()), ("repo", repo.into())])
                        .await
                    {
                        if let Ok(rows) = q.take::<Vec<serde_json::Value>>(0) {
                            if let Some(first) = rows.first() {
                                if let Some(sv) = first.get("stable_id").and_then(|v| v.as_str()) {
                                    sid_candidate = Some(sv.to_string());
                                }
                            }
                        }
                    }
                    sid_candidate
                        .ok_or_else(|| MCPError::internal_error("entity not found".into()))?
                };

                // Fetch graph with a per-relation limit larger than pagination window to allow client-side slicing
                let relations_limit = start + limit + 200;
                let cfg = hyperzoekt::graph_api::GraphDbConfig::from_env();
                let g =
                    hyperzoekt::graph_api::fetch_entity_graph(&cfg, &stable_id, relations_limit)
                        .await
                        .map_err(|e| MCPError::internal_error(e.to_string()))?;

                // Build printable lines. Include header lines only on first page (start==0).
                let mut lines: Vec<String> = Vec::new();
                if g.callers.is_empty() && g.callees.is_empty() && g.imports.is_empty() {
                    lines.push(format!("No graph edges found for {}", stable_id));
                } else {
                    lines.push(format!("callers: {}", g.callers.len()));
                    for c in &g.callers {
                        lines.push(format!("C {} | {}", c.name, c.stable_id));
                    }
                    lines.push(format!("callees: {}", g.callees.len()));
                    for c in &g.callees {
                        lines.push(format!("E {} | {}", c.name, c.stable_id));
                    }
                    lines.push(format!("imports: {}", g.imports.len()));
                    for im in &g.imports {
                        lines.push(format!("I {} | {}", im.name, im.stable_id));
                    }
                }
                let (s, e, next) = paginate(lines.len(), start, limit);
                let mut content = Vec::new();
                if s == 0 {
                    content.push(ToolContent::text(format!(
                        "total={} window={}..{}",
                        lines.len(),
                        s,
                        e
                    )));
                }
                // If not first page, avoid repeating the first header line; but keep consistent slicing
                for line in &lines[s..e] {
                    content.push(ToolContent::text(line.clone()));
                }
                if let Some(nc) = next {
                    content.push(ToolContent::text(format!("__NEXT_CURSOR__:{nc}")));
                }
                Ok(ToolResult {
                    content,
                    is_error: Some(false),
                })
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
                    "type": "object", "properties": { "q": {"type": "string"}, "repo": {"type": "string"}, "limit": {"type":"integer"}, "cursor": {"type":"string"} }, "required": ["q"]
                }),
                output_schema: None,
                annotations: Some(ToolAnnotations::default()),
            },
            Tool {
                name: "repo_dupes".into(),
                description: "List duplicate-like pairs within and across a repository".into(),
                input_schema: serde_json::json!({
                    "type": "object", "properties": { "repo": {"type": "string"}, "limit": {"type": "integer"}, "min_score": {"type": "number"}, "language": {"type": "string"}, "kind": {"type": "string"}, "cursor": {"type":"string"} }, "required": ["repo"]
                }),
                output_schema: None,
                annotations: Some(ToolAnnotations::default()),
            },
            Tool {
                name: "repo_dependencies".into(),
                description: "List declared dependencies for a repository (default branch main)"
                    .into(),
                input_schema: serde_json::json!({
                    "type": "object", "properties": { "repo": {"type": "string"}, "branch": {"type": "string", "description": "Branch or tag (defaults to main)."}, "limit": {"type": "integer"}, "cursor": {"type": "string"} }, "required": ["repo"]
                }),
                output_schema: None,
                annotations: Some(ToolAnnotations::default()),
            },
            Tool {
                name: "call_graph".into(),
                description: "Show callers and callees (and imports) for a function/class.".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "stable_id": {"type": "string", "description": "Stable id of the entity (preferred)."},
                        "name": {"type": "string", "description": "Entity name (used with repo)."},
                        "repo": {"type": "string", "description": "Repository name when resolving by name."},
                        "limit": {"type": "integer", "description": "Max callers/callees to return (default 100)."},
                        "cursor": {"type": "string", "description": "Pagination cursor (offset)."}
                    },
                    "anyOf": [
                        {"required": ["stable_id"]},
                        {"required": ["name", "repo"]}
                    ]
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
                        Ok(mut res) => {
                            let mut next_cursor: Option<String> = None;
                            if let Some(ultrafast_mcp::ToolContent::Text { text }) =
                                res.content.last()
                            {
                                if let Some(rest) = text.strip_prefix("__NEXT_CURSOR__:") {
                                    next_cursor = Some(rest.trim().to_string());
                                }
                            }
                            if next_cursor.is_some() {
                                res.content.pop();
                            }
                            let mut value = serde_json::to_value(&res).unwrap_or_default();
                            if let Some(cursor) = next_cursor {
                                value["next_cursor"] = serde_json::Value::String(cursor);
                            }
                            Json(
                                serde_json::json!({"jsonrpc":"2.0", "result": value, "id": req.id}),
                            )
                        }
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
