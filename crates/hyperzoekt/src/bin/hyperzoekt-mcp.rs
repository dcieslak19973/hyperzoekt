// HyperZoekt MCP server: exposes similarity search and repo dupes tools using internal logic
use anyhow::Result;
use async_trait::async_trait;
use axum::{extract::State, http::HeaderMap, response::IntoResponse, Json, Router};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use hyperzoekt::db::connection::SurrealConnection;
use hyperzoekt::{db, hirag, repo_index::indexer::payload::EntityPayload, similarity, utils};
use serde::Deserialize;
use ultrafast_mcp::{
    ListToolsRequest, ListToolsResponse, MCPError, MCPResult, Tool, ToolAnnotations, ToolCall,
    ToolContent, ToolHandler, ToolResult,
};

const ENTITY_FIELDS: &str = "id AS id, language, kind, name, rank AS rank, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content";

// Local helper duplicated from db_writer::helpers (kept minimal) to avoid changing visibility.

#[derive(Clone)]
struct AppState {
    db: db::Database,
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
    #[allow(unused_assignments)]
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
                // If repo param contains an explicit ref `repo@ref`, resolve it to
                // a commit and attempt to lookup a matching snapshot id. Pass the
                // cleaned repo name and the optional snapshot id into similarity
                // so sampling can be snapshot-scoped.
                // resolve_ref_to_snapshot now returns (Option<repo_filter>, Option<commit>, Option<snapshot>)
                let (repo_filter_to_pass, snapshot_id_to_pass) = if let Some(r) = repo {
                    if let Some(idx) = r.find('@') {
                        let repo_name = r[..idx].to_string();
                        let raw_ref = &r[idx + 1..];
                        let (repo_filter_opt, _commit_opt, snapshot_opt) =
                            utils::resolve_ref_to_snapshot(
                                self.state.db.connection(),
                                &repo_name,
                                Some(raw_ref),
                            )
                            .await
                            .map_err(|e| MCPError::internal_error(e.to_string()))?;
                        (repo_filter_opt, snapshot_opt)
                    } else {
                        let (repo_filter_opt, _commit_opt, snapshot_opt) =
                            utils::resolve_ref_to_snapshot(self.state.db.connection(), r, None)
                                .await
                                .map_err(|e| MCPError::internal_error(e.to_string()))?;
                        (repo_filter_opt, snapshot_opt)
                    }
                } else {
                    (None, None)
                };

                let results: Vec<EntityPayload> = similarity::similarity_with_conn(
                    self.state.db.connection(),
                    q,
                    repo_filter_to_pass.as_deref(),
                    snapshot_id_to_pass.as_deref(),
                )
                .await
                .map_err(|e| MCPError::internal_error(e.to_string()))?;

                // If similarity search returned no results, fall back to text search
                let final_results = if results.is_empty() {
                    log::debug!(
                        "similarity search returned no results, falling back to text search"
                    );
                    // Use text search as fallback - similar to webui search_api_handler
                    if let Some(repo_name) = repo_filter_to_pass.as_ref() {
                        // Single repo text search
                        match self.state.db.connection().as_ref() {
                            SurrealConnection::Local(db_conn) => {
                                let sql = format!(
                                    "SELECT {} FROM entity WHERE string::contains(name, $q) AND repo_name = $repo LIMIT $limit",
                                    ENTITY_FIELDS
                                );
                                let mut resp = db_conn
                                    .query(&sql)
                                    .bind(("q", q.to_lowercase()))
                                    .bind(("repo", repo_name.clone()))
                                    .bind(("limit", limit as i64))
                                    .await
                                    .map_err(|e| MCPError::internal_error(e.to_string()))?;
                                resp.take(0).unwrap_or_default()
                            }
                            SurrealConnection::RemoteHttp(db_conn) => {
                                let sql = format!(
                                    "SELECT {} FROM entity WHERE string::contains(name, $q) AND repo_name = $repo LIMIT $limit",
                                    ENTITY_FIELDS
                                );
                                let mut resp = db_conn
                                    .query(&sql)
                                    .bind(("q", q.to_lowercase()))
                                    .bind(("repo", repo_name.clone()))
                                    .bind(("limit", limit as i64))
                                    .await
                                    .map_err(|e| MCPError::internal_error(e.to_string()))?;
                                resp.take(0).unwrap_or_default()
                            }
                            SurrealConnection::RemoteWs(db_conn) => {
                                let sql = format!(
                                    "SELECT {} FROM entity WHERE string::contains(name, $q) AND repo_name = $repo LIMIT $limit",
                                    ENTITY_FIELDS
                                );
                                let mut resp = db_conn
                                    .query(&sql)
                                    .bind(("q", q.to_lowercase()))
                                    .bind(("repo", repo_name.clone()))
                                    .bind(("limit", limit as i64))
                                    .await
                                    .map_err(|e| MCPError::internal_error(e.to_string()))?;
                                resp.take(0).unwrap_or_default()
                            }
                        }
                    } else {
                        // No repo filter text search
                        match self.state.db.connection().as_ref() {
                            SurrealConnection::Local(db_conn) => {
                                let sql = format!(
                                    "SELECT {} FROM entity WHERE string::contains(name, $q) LIMIT $limit",
                                    ENTITY_FIELDS
                                );
                                let mut resp = db_conn
                                    .query(&sql)
                                    .bind(("q", q.to_lowercase()))
                                    .bind(("limit", limit as i64))
                                    .await
                                    .map_err(|e| MCPError::internal_error(e.to_string()))?;
                                resp.take(0).unwrap_or_default()
                            }
                            SurrealConnection::RemoteHttp(db_conn) => {
                                let sql = format!(
                                    "SELECT {} FROM entity WHERE string::contains(name, $q) LIMIT $limit",
                                    ENTITY_FIELDS
                                );
                                let mut resp = db_conn
                                    .query(&sql)
                                    .bind(("q", q.to_lowercase()))
                                    .bind(("limit", limit as i64))
                                    .await
                                    .map_err(|e| MCPError::internal_error(e.to_string()))?;
                                resp.take(0).unwrap_or_default()
                            }
                            SurrealConnection::RemoteWs(db_conn) => {
                                let sql = format!(
                                    "SELECT {} FROM entity WHERE string::contains(name, $q) LIMIT $limit",
                                    ENTITY_FIELDS
                                );
                                let mut resp = db_conn
                                    .query(&sql)
                                    .bind(("q", q.to_lowercase()))
                                    .bind(("limit", limit as i64))
                                    .await
                                    .map_err(|e| MCPError::internal_error(e.to_string()))?;
                                resp.take(0).unwrap_or_default()
                            }
                        }
                    }
                } else {
                    results
                };
                let (s, e, next) = paginate(final_results.len(), start, limit);
                let mut content = Vec::new();
                if s == 0 {
                    content.push(ToolContent::text(format!(
                        "total={} window={}..{}",
                        final_results.len(),
                        s,
                        e
                    )));
                }
                if final_results.is_empty() {
                    content.push(ToolContent::text("No similar entities found.".into()));
                }
                for r in final_results[s..e].iter() {
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
                    .connection()
                    .query_with_binds(
                        name_branch_sql,
                        vec![("name", repo.into()), ("branch", branch_or_default.into())],
                    )
                    .await
                {
                    if let Ok(rows) = r.take::<Vec<serde_json::Value>>(0) {
                        if let Some(first) = rows.first() {
                            if let Some(idv) = first.get("id") {
                                repo_id = utils::extract_surreal_id(idv);
                            }
                        }
                    }
                }
                // Fallback: by name only
                if repo_id.is_none() {
                    if let Ok(mut r) = self
                        .state
                        .db
                        .connection()
                        .query_with_binds(
                            "SELECT id FROM repo WHERE name = $name LIMIT 1;",
                            vec![("name", repo.into())],
                        )
                        .await
                    {
                        if let Ok(rows) = r.take::<Vec<serde_json::Value>>(0) {
                            if let Some(first) = rows.first() {
                                if let Some(idv) = first.get("id") {
                                    repo_id = utils::extract_surreal_id(idv);
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
                if let Ok(mut qres) = self
                    .state
                    .db
                    .connection()
                    .query(traversal_sql.as_str())
                    .await
                {
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
                    if let Ok(mut q2) = self
                        .state
                        .db
                        .connection()
                        .query(explicit_sql.as_str())
                        .await
                    {
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
                        .connection()
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
            "pagerank" => {
                let args = call.arguments.clone().unwrap_or_default();
                let repo = args
                    .get("repo")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| MCPError::invalid_params("missing repo".into()))?;
                let limit = args
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as usize)
                    .unwrap_or(500)
                    .clamp(1, 1000);

                // Query entities ordered by rank DESC
                let entity_fields = ENTITY_FIELDS;
                let response = match self.state.db.connection().as_ref() {
                    SurrealConnection::Local(db_conn) => {
                        db_conn
                            .query(format!(
                                "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY rank DESC LIMIT $limit",
                                entity_fields
                            ))
                            .bind(("repo", repo.to_string()))
                            .bind(("limit", limit as i64))
                            .await
                    }
                    SurrealConnection::RemoteHttp(db_conn) => {
                        db_conn
                            .query(format!(
                                "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY rank DESC LIMIT $limit",
                                entity_fields
                            ))
                            .bind(("repo", repo.to_string()))
                            .bind(("limit", limit as i64))
                            .await
                    }
                    SurrealConnection::RemoteWs(db_conn) => {
                        db_conn
                            .query(format!(
                                "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY rank DESC LIMIT $limit",
                                entity_fields
                            ))
                            .bind(("repo", repo.to_string()))
                            .bind(("limit", limit as i64))
                            .await
                    }
                };

                let entities: Vec<EntityPayload> = match response {
                    Ok(mut r) => r.take(0).unwrap_or_default(),
                    Err(e) => {
                        log::error!("pagerank query failed for {}: {}", repo, e);
                        return Err(MCPError::internal_error(e.to_string()));
                    }
                };

                log::debug!(
                    "pagerank: fetched {} entities for repo={}",
                    entities.len(),
                    repo
                );

                // Get repo metadata for URL construction
                let _clean_repo = utils::remove_uuid_suffix(repo);
                let _uuid_repo_name = entities
                    .first()
                    .map(utils::file_hint_for_entity)
                    .and_then(|s| if s.is_empty() { None } else { Some(s) })
                    .as_deref()
                    .and_then(utils::extract_uuid_repo_name);

                // Build result lines
                let mut lines: Vec<String> = Vec::new();
                for entity in &entities {
                    let rank = entity.rank.unwrap_or(0.0);
                    let file_hint = utils::file_hint_for_entity(entity);
                    let name = &entity.name;
                    let kind = &entity.kind;
                    let line = format!("{:.6} | {} | {} | {}", rank, kind, name, file_hint);
                    lines.push(line);
                }

                let mut content = Vec::new();
                content.push(ToolContent::text(format!(
                    "PageRank entities for {} (limit={})",
                    repo, limit
                )));
                for line in lines {
                    content.push(ToolContent::text(line));
                }

                Ok(ToolResult {
                    content,
                    is_error: Some(false),
                })
            }
            "sbom_dependencies" => {
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

                // Resolve ref to commit if branch is specified
                let commit_opt = if let Some(br) = branch {
                    // Query refs table similarly to webui Database::resolve_ref_to_snapshot
                    #[derive(serde::Deserialize)]
                    struct RefRow {
                        name: String,
                        target: String,
                    }
                    let candidates = [
                        br.to_string(),
                        format!("refs/heads/{}", br),
                        format!("refs/tags/{}", br),
                    ];
                    let sql = format!(
                        "SELECT name, target FROM refs WHERE repo = $repo AND name IN [{}]",
                        candidates
                            .iter()
                            .enumerate()
                            .map(|(i, _)| format!("$n{}", i))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    let resp_opt = match self.state.db.connection().as_ref() {
                        SurrealConnection::Local(db_conn) => {
                            let mut q = db_conn.query(&sql);
                            q = q.bind(("repo", repo.to_string()));
                            for (i, v) in candidates.iter().enumerate() {
                                q = q.bind((format!("n{}", i), v.to_string()));
                            }
                            q.await.ok()
                        }
                        SurrealConnection::RemoteHttp(db_conn) => {
                            let mut q = db_conn.query(&sql);
                            q = q.bind(("repo", repo.to_string()));
                            for (i, v) in candidates.iter().enumerate() {
                                q = q.bind((format!("n{}", i), v.to_string()));
                            }
                            q.await.ok()
                        }
                        SurrealConnection::RemoteWs(db_conn) => {
                            let mut q = db_conn.query(&sql);
                            q = q.bind(("repo", repo.to_string()));
                            for (i, v) in candidates.iter().enumerate() {
                                q = q.bind((format!("n{}", i), v.to_string()));
                            }
                            q.await.ok()
                        }
                    };
                    if let Some(mut resp) = resp_opt {
                        if let Ok(rows) = resp.take::<Vec<RefRow>>(0) {
                            if !rows.is_empty() {
                                // prefer exact, then heads, then tags
                                let chosen = if let Some(rw) = rows.iter().find(|r| r.name == br) {
                                    rw.target.clone()
                                } else if let Some(rw) =
                                    rows.iter().find(|r| r.name == format!("refs/heads/{}", br))
                                {
                                    rw.target.clone()
                                } else if let Some(rw) =
                                    rows.iter().find(|r| r.name == format!("refs/tags/{}", br))
                                {
                                    rw.target.clone()
                                } else {
                                    String::new()
                                };
                                if !chosen.is_empty() {
                                    Some(chosen)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Get SBOM dependencies - implement similar to webui get_sbom_dependencies_for_repo
                let deps = if let Some(commit) = commit_opt.as_ref() {
                    // Query sboms table for this repo and commit
                    let sbom_sql =
                        "SELECT id FROM sboms WHERE repo = $repo AND commit = $commit LIMIT 1";
                    let sbom_resp = match self.state.db.connection().as_ref() {
                        SurrealConnection::Local(db_conn) => {
                            db_conn
                                .query(sbom_sql)
                                .bind(("repo", repo.to_string()))
                                .bind(("commit", commit.to_string()))
                                .await
                        }
                        SurrealConnection::RemoteHttp(db_conn) => {
                            db_conn
                                .query(sbom_sql)
                                .bind(("repo", repo.to_string()))
                                .bind(("commit", commit.to_string()))
                                .await
                        }
                        SurrealConnection::RemoteWs(db_conn) => {
                            db_conn
                                .query(sbom_sql)
                                .bind(("repo", repo.to_string()))
                                .bind(("commit", commit.to_string()))
                                .await
                        }
                    };

                    #[derive(serde::Deserialize)]
                    struct SbomRow {
                        id: Option<String>,
                    }

                    let sbom_id_opt: Option<String> = if let Ok(mut resp) = sbom_resp {
                        if let Ok(rows) = resp.take::<Vec<SbomRow>>(0) {
                            if let Some(row) = rows.into_iter().next() {
                                row.id
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    if let Some(sbom_id) = sbom_id_opt {
                        // Query sbom_deps for this sbom_id
                        let deps_sql =
                            "SELECT name, version, purl FROM sbom_deps WHERE sbom_id = $sbom_id";
                        let deps_resp = match self.state.db.connection().as_ref() {
                            SurrealConnection::Local(db_conn) => {
                                db_conn.query(deps_sql).bind(("sbom_id", sbom_id)).await
                            }
                            SurrealConnection::RemoteHttp(db_conn) => {
                                db_conn.query(deps_sql).bind(("sbom_id", sbom_id)).await
                            }
                            SurrealConnection::RemoteWs(db_conn) => {
                                db_conn.query(deps_sql).bind(("sbom_id", sbom_id)).await
                            }
                        };

                        if let Ok(mut resp) = deps_resp {
                            resp.take::<Vec<serde_json::Value>>(0).unwrap_or_default()
                        } else {
                            vec![]
                        }
                    } else {
                        vec![]
                    }
                } else {
                    // No commit specified, try to find default branch commit
                    let default_refs = ["refs/heads/main", "refs/heads/master", "refs/heads/trunk"];
                    let mut found_deps = vec![];

                    for ref_name in &default_refs {
                        let sbom_sql = "SELECT id FROM sboms WHERE repo = $repo AND commit IN (SELECT target FROM refs WHERE repo = $repo AND name = $ref_name LIMIT 1) LIMIT 1";
                        let sbom_resp = match self.state.db.connection().as_ref() {
                            SurrealConnection::Local(db_conn) => {
                                db_conn
                                    .query(sbom_sql)
                                    .bind(("repo", repo.to_string()))
                                    .bind(("ref_name", ref_name.to_string()))
                                    .await
                            }
                            SurrealConnection::RemoteHttp(db_conn) => {
                                db_conn
                                    .query(sbom_sql)
                                    .bind(("repo", repo.to_string()))
                                    .bind(("ref_name", ref_name.to_string()))
                                    .await
                            }
                            SurrealConnection::RemoteWs(db_conn) => {
                                db_conn
                                    .query(sbom_sql)
                                    .bind(("repo", repo.to_string()))
                                    .bind(("ref_name", ref_name.to_string()))
                                    .await
                            }
                        };

                        #[derive(serde::Deserialize)]
                        struct SbomRow {
                            id: Option<String>,
                        }

                        let sbom_id_opt: Option<String> = if let Ok(mut resp) = sbom_resp {
                            if let Ok(rows) = resp.take::<Vec<SbomRow>>(0) {
                                if let Some(row) = rows.into_iter().next() {
                                    row.id
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        if let Some(sbom_id) = sbom_id_opt {
                            // Query sbom_deps for this sbom_id
                            let deps_sql = "SELECT name, version, purl FROM sbom_deps WHERE sbom_id = $sbom_id";
                            let deps_resp = match self.state.db.connection().as_ref() {
                                SurrealConnection::Local(db_conn) => {
                                    db_conn.query(deps_sql).bind(("sbom_id", sbom_id)).await
                                }
                                SurrealConnection::RemoteHttp(db_conn) => {
                                    db_conn.query(deps_sql).bind(("sbom_id", sbom_id)).await
                                }
                                SurrealConnection::RemoteWs(db_conn) => {
                                    db_conn.query(deps_sql).bind(("sbom_id", sbom_id)).await
                                }
                            };

                            if let Ok(mut resp) = deps_resp {
                                if let Ok(deps_vec) = resp.take::<Vec<serde_json::Value>>(0) {
                                    found_deps = deps_vec;
                                    break; // Found deps for this ref, use them
                                }
                            }
                        }
                    }

                    found_deps
                };

                let mut lines: Vec<String> = Vec::new();
                if deps.is_empty() {
                    lines.push(format!(
                        "No SBOM dependencies found for repo {} (branch {})",
                        repo, branch_or_default
                    ));
                } else {
                    lines.push(format!(
                        "Found {} SBOM dependencies for repo {} (branch {})",
                        deps.len(),
                        repo,
                        branch_or_default
                    ));
                    for d in &deps {
                        let name = d.get("name").and_then(|v| v.as_str()).unwrap_or("");
                        let ver = d.get("version").and_then(|v| v.as_str()).unwrap_or("");
                        let purl = d.get("purl").and_then(|v| v.as_str()).unwrap_or("");
                        let line = format!("{} {} ({})", name, ver, purl);
                        lines.push(line);
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
            "sbom_fetch" => {
                let args = call.arguments.clone().unwrap_or_default();
                let repo = args
                    .get("repo")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| MCPError::invalid_params("missing repo".into()))?;
                // Optional branch or tag override
                let branch = args.get("branch").and_then(|v| v.as_str());

                // Resolve ref to commit if branch is specified (reuse sbom_dependencies logic)
                let commit_opt = if let Some(br) = branch {
                    #[derive(serde::Deserialize)]
                    struct RefRow {
                        name: String,
                        target: String,
                    }
                    let candidates = [
                        br.to_string(),
                        format!("refs/heads/{br}"),
                        format!("refs/tags/{br}"),
                    ];
                    let sql = format!(
                        "SELECT name, target FROM refs WHERE repo = $repo AND name IN [{}]",
                        candidates
                            .iter()
                            .enumerate()
                            .map(|(i, _)| format!("$n{}", i))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                    let resp_opt = match self.state.db.connection().as_ref() {
                        SurrealConnection::Local(db_conn) => {
                            let mut q = db_conn.query(&sql);
                            q = q.bind(("repo", repo.to_string()));
                            for (i, v) in candidates.iter().enumerate() {
                                q = q.bind((format!("n{}", i), v.to_string()));
                            }
                            q.await.ok()
                        }
                        SurrealConnection::RemoteHttp(db_conn) => {
                            let mut q = db_conn.query(&sql);
                            q = q.bind(("repo", repo.to_string()));
                            for (i, v) in candidates.iter().enumerate() {
                                q = q.bind((format!("n{}", i), v.to_string()));
                            }
                            q.await.ok()
                        }
                        SurrealConnection::RemoteWs(db_conn) => {
                            let mut q = db_conn.query(&sql);
                            q = q.bind(("repo", repo.to_string()));
                            for (i, v) in candidates.iter().enumerate() {
                                q = q.bind((format!("n{}", i), v.to_string()));
                            }
                            q.await.ok()
                        }
                    };
                    if let Some(mut resp) = resp_opt {
                        if let Ok(rows) = resp.take::<Vec<RefRow>>(0) {
                            if !rows.is_empty() {
                                let chosen = if let Some(rw) = rows.iter().find(|r| r.name == br) {
                                    rw.target.clone()
                                } else if let Some(rw) =
                                    rows.iter().find(|r| r.name == format!("refs/heads/{br}"))
                                {
                                    rw.target.clone()
                                } else if let Some(rw) =
                                    rows.iter().find(|r| r.name == format!("refs/tags/{br}"))
                                {
                                    rw.target.clone()
                                } else {
                                    String::new()
                                };
                                if !chosen.is_empty() {
                                    Some(chosen)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                // Fetch SBOM row either by explicit commit or by attempting default refs
                #[derive(serde::Deserialize)]
                struct SbomContent {
                    sbom_blob: Option<String>,
                    sbom_encoding: Option<String>,
                }

                let sbom_row_opt: Option<SbomContent> = if let Some(commit) = commit_opt.as_ref() {
                    let sbom_sql = "SELECT sbom_blob, sbom_encoding FROM sboms WHERE repo = $repo AND commit = $commit LIMIT 1";
                    let sbom_resp = match self.state.db.connection().as_ref() {
                        SurrealConnection::Local(db_conn) => {
                            db_conn
                                .query(sbom_sql)
                                .bind(("repo", repo.to_string()))
                                .bind(("commit", commit.to_string()))
                                .await
                        }
                        SurrealConnection::RemoteHttp(db_conn) => {
                            db_conn
                                .query(sbom_sql)
                                .bind(("repo", repo.to_string()))
                                .bind(("commit", commit.to_string()))
                                .await
                        }
                        SurrealConnection::RemoteWs(db_conn) => {
                            db_conn
                                .query(sbom_sql)
                                .bind(("repo", repo.to_string()))
                                .bind(("commit", commit.to_string()))
                                .await
                        }
                    };
                    if let Ok(mut resp) = sbom_resp {
                        if let Ok(rows) = resp.take::<Vec<SbomContent>>(0) {
                            rows.into_iter().next()
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    let default_refs = ["refs/heads/main", "refs/heads/master", "refs/heads/trunk"];
                    let mut found: Option<SbomContent> = None;
                    for ref_name in &default_refs {
                        let sbom_sql = "SELECT sbom_blob, sbom_encoding FROM sboms WHERE repo = $repo AND commit IN (SELECT target FROM refs WHERE repo = $repo AND name = $ref_name LIMIT 1) LIMIT 1";
                        let sbom_resp = match self.state.db.connection().as_ref() {
                            SurrealConnection::Local(db_conn) => {
                                db_conn
                                    .query(sbom_sql)
                                    .bind(("repo", repo.to_string()))
                                    .bind(("ref_name", ref_name.to_string()))
                                    .await
                            }
                            SurrealConnection::RemoteHttp(db_conn) => {
                                db_conn
                                    .query(sbom_sql)
                                    .bind(("repo", repo.to_string()))
                                    .bind(("ref_name", ref_name.to_string()))
                                    .await
                            }
                            SurrealConnection::RemoteWs(db_conn) => {
                                db_conn
                                    .query(sbom_sql)
                                    .bind(("repo", repo.to_string()))
                                    .bind(("ref_name", ref_name.to_string()))
                                    .await
                            }
                        };
                        if let Ok(mut resp) = sbom_resp {
                            if let Ok(rows) = resp.take::<Vec<SbomContent>>(0) {
                                if let Some(r) = rows.into_iter().next() {
                                    found = Some(r);
                                    break;
                                }
                            }
                        }
                    }
                    found
                };

                let mut content = Vec::new();
                if let Some(row) = sbom_row_opt {
                    if let Some(blob) = row.sbom_blob {
                        let encoding = row.sbom_encoding.unwrap_or_else(|| "base64".into());
                        if encoding.eq_ignore_ascii_case("base64") {
                            match BASE64_STANDARD.decode(blob.as_bytes()) {
                                Ok(decoded) => match String::from_utf8(decoded) {
                                    Ok(s) => {
                                        content.push(ToolContent::text(s));
                                    }
                                    Err(e) => {
                                        return Err(MCPError::internal_error(format!(
                                            "SBOM UTF-8 decode error: {}",
                                            e
                                        )))
                                    }
                                },
                                Err(e) => {
                                    return Err(MCPError::internal_error(format!(
                                        "Base64 decode error: {}",
                                        e
                                    )))
                                }
                            }
                        } else {
                            // treat as plain text
                            content.push(ToolContent::text(blob));
                        }
                    } else {
                        content.push(ToolContent::text(format!(
                            "No SBOM blob found for repo {}",
                            repo
                        )));
                    }
                } else {
                    content.push(ToolContent::text(format!(
                        "No SBOM found for repo {}",
                        repo
                    )));
                }

                Ok(ToolResult {
                    content,
                    is_error: Some(false),
                })
            }
            "hirag_retrieve" => {
                let args = call.arguments.clone().unwrap_or_default();
                let q = args
                    .get("q")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| MCPError::invalid_params("missing q".into()))?;
                let top_k = args
                    .get("top_k")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as usize)
                    .unwrap_or(5usize);

                // Accept either a single "repo" string or an array "repos"
                let allowed_repos: Option<Vec<String>> = if let Some(repos_v) = args.get("repos") {
                    repos_v.as_array().map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                } else if let Some(r) = args.get("repo").and_then(|v| v.as_str()) {
                    Some(vec![r.to_string()])
                } else {
                    None
                };

                // Embed the query text
                let emb: Vec<f32> = similarity::embed_query(q)
                    .await
                    .map_err(|e| MCPError::internal_error(e.to_string()))?;

                // Call HiRAG retrieval (3-level) with optional allowed repos
                let (top, clusters, bridge) = hirag::retrieve_three_level_with_allowed_repos(
                    self.state.db.connection(),
                    &emb,
                    top_k,
                    allowed_repos.as_deref(),
                )
                .await
                .map_err(|e| MCPError::internal_error(e.to_string()))?;

                // Build a structured JSON result so consuming agents can parse it easily.
                let clusters_json: Vec<serde_json::Value> = clusters
                    .into_iter()
                    .map(|c| {
                        // Omit centroid vectors/lengths from the returned JSON; callers
                        // should fetch member snapshots individually (see member_fetch_tool).
                        serde_json::json!({
                            "label": c.label,
                            "summary": c.summary,
                            "members": c.members,
                            "member_repos": c.member_repos
                        })
                    })
                    .collect();

                let bridge_json: Vec<serde_json::Value> = bridge
                    .into_iter()
                    .map(|(a, b, c)| serde_json::json!({"a": a, "b": b, "meta": c}))
                    .collect();

                let payload = serde_json::json!({
                    "top_entities": top,
                    "clusters": clusters_json,
                    "bridge": bridge_json,
                    // Guidance for clients/LLMs: use this tool name to fetch member snapshots
                    "member_fetch_tool": "fetch_entity_snapshot"
                });

                // Serialize as compact JSON string in a single ToolContent::text so clients
                // and LLM tools can parse the output deterministically.
                let content: Vec<ToolContent> = vec![ToolContent::text(payload.to_string())];

                Ok(ToolResult {
                    content,
                    is_error: Some(false),
                })
            }
            "fetch_entity_snapshot" => {
                let args = call.arguments.clone().unwrap_or_default();
                let stable_id = args
                    .get("stable_id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| MCPError::invalid_params("missing stable_id".into()))?;
                let start = args
                    .get("start")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as usize);
                let limit = args
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as usize);

                // Query entity_snapshot table for the record with this stable_id
                let sql = format!(
                    "SELECT stable_id, repo_name, file, start_line, source_display, source_content FROM entity_snapshot WHERE stable_id = $sid LIMIT 1"
                );
                let resp = match self.state.db.connection().as_ref() {
                    SurrealConnection::Local(db_conn) => {
                        db_conn.query(&sql).bind(("sid", stable_id)).await
                    }
                    SurrealConnection::RemoteHttp(db_conn) => {
                        db_conn.query(&sql).bind(("sid", stable_id)).await
                    }
                    SurrealConnection::RemoteWs(db_conn) => {
                        db_conn.query(&sql).bind(("sid", stable_id)).await
                    }
                };

                let mut resp = resp.map_err(|e| MCPError::internal_error(e.to_string()))?;
                let rows: Vec<serde_json::Value> = resp
                    .take(0)
                    .map_err(|e| MCPError::internal_error(e.to_string()))?;
                if rows.is_empty() {
                    return Err(MCPError::invalid_params(format!(
                        "entity_snapshot not found: {}",
                        stable_id
                    )));
                }
                let row = &rows[0];

                // Extract fields
                let repo_name = row
                    .get("repo_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let file = row
                    .get("file")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let start_line = row.get("start_line").and_then(|v| v.as_i64());
                let source_display = row
                    .get("source_display")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let source_content = row
                    .get("source_content")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                // Handle slicing if start/limit provided
                let payload = if let (Some(s), Some(l)) = (start, limit) {
                    let total_len = source_content.len();
                    let end = (s + l).min(total_len);
                    let snippet = source_content.get(s..end).unwrap_or("").to_string();
                    let next_cursor = if end < total_len {
                        Some(end.to_string())
                    } else {
                        None
                    };
                    let mut obj = serde_json::json!({
                        "stable_id": stable_id,
                        "repo_name": repo_name,
                        "file": file,
                        "start_line": start_line,
                        "source_display": source_display,
                        "snippet": snippet,
                        "snippet_start": s,
                        "snippet_end": end,
                        "total_len": total_len
                    });
                    if let Some(nc) = next_cursor {
                        obj.as_object_mut()
                            .unwrap()
                            .insert("next_cursor".to_string(), serde_json::Value::String(nc));
                    }
                    obj
                } else {
                    serde_json::json!({
                        "stable_id": stable_id,
                        "repo_name": repo_name,
                        "file": file,
                        "start_line": start_line,
                        "source_display": source_display,
                        "source_content": source_content
                    })
                };

                let content: Vec<ToolContent> = vec![ToolContent::text(payload.to_string())];

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
            Tool {
                name: "pagerank".into(),
                description: "Get entities ranked by importance (PageRank) for a repository".into(),
                input_schema: serde_json::json!({
                    "type": "object", "properties": { "repo": {"type": "string"}, "limit": {"type": "integer", "description": "Maximum entities to return (default 500)."} }, "required": ["repo"]
                }),
                output_schema: None,
                annotations: Some(ToolAnnotations::default()),
            },
            Tool {
                name: "sbom_dependencies".into(),
                description: "List SBOM (Software Bill of Materials) dependencies for a repository"
                    .into(),
                input_schema: serde_json::json!({
                    "type": "object", "properties": { "repo": {"type": "string"}, "branch": {"type": "string", "description": "Branch or tag (defaults to main)."}, "limit": {"type": "integer"}, "cursor": {"type": "string"} }, "required": ["repo"]
                }),
                output_schema: None,
                annotations: Some(ToolAnnotations::default()),
            },
            Tool {
                name: "hirag_retrieve".into(),
                description:
                    "Retrieve HiRAG three-level context (top entities + clusters) for a query"
                        .into(),
                input_schema: serde_json::json!({
                    "type": "object", "properties": { "q": {"type": "string"}, "repo": {"type":"string"}, "repos": {"type":"array","items":{"type":"string"}}, "top_k": {"type":"integer"} }, "required": ["q"]
                }),
                output_schema: Some(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "top_entities": { "type": "array", "items": { "type": "string" } },
                        "clusters": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "label": { "type": "string" },
                                    "summary": { "type": "string" },
                                    "members": { "type": "array", "items": { "type": "string" } },
                                    "member_repos": { "type": "array", "items": { "type": "string" } }
                                },
                                "required": ["label", "summary", "members"]
                            }
                        },
                        "bridge": { "type": "array", "items": { "type": "object", "properties": { "a": {"type":"string"}, "b": {"type":"string"}, "meta": {"type":"string"} }, "required": ["a","b"] } },
                        "member_fetch_tool": { "type": "string", "description": "Tool name clients can call to fetch member snapshots by stable_id" }
                    },
                    "required": ["top_entities", "clusters", "bridge", "member_fetch_tool"]
                })),
                annotations: Some(ToolAnnotations::default()),
            },
            Tool {
                name: "fetch_entity_snapshot".into(),
                description:
                    "Fetch a single entity_snapshot record (metadata + source_content) by stable_id.\nOptional `start` and `limit` parameters can be provided to request a byte/character slice of the source content when supported by the server; large results may be paged. If more content remains, the MCP response will set `next_cursor`.".into(),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "stable_id": { "type": "string", "description": "Stable id of the entity_snapshot to fetch." },
                        "start": { "type": "integer", "description": "Optional start offset (bytes or characters, server-dependent) for slicing the returned source content." },
                        "limit": { "type": "integer", "description": "Optional maximum number of bytes/characters to return starting at `start`. If omitted the full content may be returned." }
                    },
                    "required": ["stable_id"]
                }),
                output_schema: Some(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "stable_id": { "type": "string" },
                        "repo_name": { "type": "string" },
                        "file": { "type": "string" },
                        "start_line": { "type": ["integer", "null"] },
                        "source_display": { "type": ["string", "null"] },
                        "source_content": { "type": ["string", "null"], "description": "Full source content when returned in full." },
                        "snippet": { "type": ["string", "null"], "description": "A sliced snippet of source_content when start/limit were used." },
                        "snippet_start": { "type": ["integer", "null"], "description": "Offset of the returned snippet within the full content." },
                        "snippet_end": { "type": ["integer", "null"], "description": "Offset end (exclusive) of the returned snippet within the full content." },
                        "total_len": { "type": ["integer", "null"], "description": "Total length of the full source_content when known." }
                    }
                })),
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
    let _user = std::env::var("SURREALDB_USERNAME").ok();
    let _pass = std::env::var("SURREALDB_PASSWORD").ok();
    let ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
    let dbn = std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());
    let db = db::Database::new(url.as_deref(), &ns, &dbn)
        .await
        .map_err(|e| anyhow::anyhow!("Database connection failed: {}", e))?;

    let webui_base =
        std::env::var("HZ_WEBUI_BASE").unwrap_or_else(|_| "http://127.0.0.1:7878".into());
    let state = AppState {
        db,
        webui_base,
        http: reqwest::Client::new(),
    };
    let app = Router::new()
        .route("/mcp", axum::routing::post(handle_mcp))
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
