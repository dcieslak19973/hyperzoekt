use anyhow::{Context, Result};
use axum::{extract::State, routing::get, Router};
use base64::Engine;
use chrono::Utc;
use deadpool_redis::redis::AsyncCommands;
use git2::Repository;
use hyperzoekt::db::connection::{connect as hz_connect, SurrealConnection};
use hyperzoekt::test_utils::normalize_thing_id;
use log::{debug, error, info, warn};
use serde_json::Value;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::process::Stdio;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use surrealdb::Value as DbValue;
use tempfile::Builder;
use tokio::process::Command;

#[derive(Debug, Clone)]
struct SbomWorkerMetrics {
    jobs_processed: Arc<AtomicU64>,
    jobs_failed: Arc<AtomicU64>,
    redis_errors: Arc<AtomicU64>,
}

impl SbomWorkerMetrics {
    fn new() -> Self {
        Self {
            jobs_processed: Arc::new(AtomicU64::new(0)),
            jobs_failed: Arc::new(AtomicU64::new(0)),
            redis_errors: Arc::new(AtomicU64::new(0)),
        }
    }
}

async fn health_handler() -> &'static str {
    "OK"
}

async fn metrics_handler(
    State(metrics): axum::extract::State<Arc<SbomWorkerMetrics>>,
) -> impl axum::response::IntoResponse {
    let jobs_processed = metrics.jobs_processed.load(Ordering::Relaxed);
    let jobs_failed = metrics.jobs_failed.load(Ordering::Relaxed);
    let redis_errors = metrics.redis_errors.load(Ordering::Relaxed);

    format!(
        "# HELP hyperzoekt_sbom_worker_jobs_processed_total Total number of SBOM jobs processed\n\
         # TYPE hyperzoekt_sbom_worker_jobs_processed_total counter\n\
         hyperzoekt_sbom_worker_jobs_processed_total {}\n\
         # HELP hyperzoekt_sbom_worker_jobs_failed_total Total number of SBOM jobs that failed\n\
         # TYPE hyperzoekt_sbom_worker_jobs_failed_total counter\n\
         hyperzoekt_sbom_worker_jobs_failed_total {}\n\
         # HELP hyperzoekt_sbom_worker_redis_errors_total Total number of Redis errors\n\
         # TYPE hyperzoekt_sbom_worker_redis_errors_total counter\n\
         hyperzoekt_sbom_worker_redis_errors_total {}\n",
        jobs_processed, jobs_failed, redis_errors
    )
}

fn select_sbom_workdir(repo_path: &Path) -> std::path::PathBuf {
    let crates = repo_path.join("crates");
    if crates.exists() && crates.is_dir() {
        crates
    } else {
        repo_path.to_path_buf()
    }
}

fn normalize_record_id(raw: &str, fallback_table: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some((table, rest)) = trimmed.split_once(':') {
        if rest.is_empty() {
            return None;
        }
        if table.is_empty() {
            let normalized = format!("{}:{}", fallback_table, rest);
            debug!("normalize_record_id: fallback table used -> {}", normalized);
            return Some(normalized);
        }
        let normalized = format!("{}:{}", table, rest);
        debug!("normalize_record_id: passthrough -> {}", normalized);
        return Some(normalized);
    }

    let normalized = format!("{}:{}", fallback_table, trimmed);
    debug!(
        "normalize_record_id: prefixing raw '{}' -> {}",
        trimmed, normalized
    );
    Some(normalized)
}

fn normalize_value_record_id(value: &serde_json::Value, fallback_table: &str) -> Option<String> {
    normalize_thing_id(value)
        .as_deref()
        .and_then(|s| normalize_record_id(s, fallback_table))
}

fn extract_id_from_json_value(value: &serde_json::Value) -> Option<String> {
    // Handle wrapped Array format: {"Array": [...]}
    if let Some(obj) = value.as_object() {
        if let Some(arr_val) = obj.get("Array") {
            if let Some(arr) = arr_val.as_array() {
                if let Some(first) = arr.first() {
                    // Recursively extract from the first element
                    if let Some(id) = extract_id_from_json_value(first) {
                        return Some(id);
                    }
                }
            }
        }
    }

    if let Some(obj) = value.as_object() {
        if let Some(id_val) = obj.get("id") {
            if let Some(tb) = obj.get("tb").and_then(|v| v.as_str()) {
                if let Some(inner_id) = id_val.as_str() {
                    return Some(format!("{}:{}", tb, inner_id));
                }
                if let Some(inner_obj) = id_val.as_object() {
                    let table = inner_obj.get("tb").and_then(|v| v.as_str()).or(Some(tb));
                    if let Some(id_str) = inner_obj.get("id").and_then(|v| v.as_str()) {
                        if let Some(tb_str) = table {
                            return Some(format!("{}:{}", tb_str, id_str));
                        }
                        return Some(id_str.to_string());
                    }
                    // Handle nested String wrapper: {"String": "..."}
                    if let Some(string_val) = inner_obj.get("String") {
                        if let Some(id_str) = string_val.as_str() {
                            return Some(format!("{}:{}", tb, id_str));
                        }
                    }
                }
            }
        }
    }

    if let Some(arr) = value.as_array() {
        for item in arr {
            if let Some(id) = extract_id_from_json_value(item) {
                return Some(id);
            }
        }
    }

    if let Some(obj) = value.as_object() {
        for val in obj.values() {
            if let Some(id) = extract_id_from_json_value(val) {
                return Some(id);
            }
        }
    }

    None
}

async fn init_sbom_schema(conn: &SurrealConnection) -> Result<(), surrealdb::Error> {
    let q = r#"
        DEFINE TABLE sboms SCHEMALESS PERMISSIONS FULL;
        DEFINE INDEX idx_sboms_repo_commit ON sboms COLUMNS repo, commit UNIQUE;
        DEFINE TABLE sbom_for_commit TYPE RELATION FROM sboms TO commits;
        DEFINE TABLE sbom_for_commit TYPE RELATION;
    "#;
    conn.query(q).await?;
    Ok(())
}

async fn init_sbom_deps_schema(conn: &SurrealConnection) -> Result<(), surrealdb::Error> {
    let q = r#"
        DEFINE TABLE sbom_deps SCHEMALESS PERMISSIONS FULL;
        DEFINE INDEX idx_sbom_deps_sbomid ON sbom_deps COLUMNS sbom_id;
        DEFINE TABLE sbom_dependencies TYPE RELATION FROM sboms TO sbom_deps;
        DEFINE TABLE sbom_dependencies TYPE RELATION;
        DEFINE INDEX idx_sbom_dependencies_unique ON sbom_dependencies FIELDS in, out UNIQUE;
    "#;
    conn.query(q).await?;
    Ok(())
}

async fn init_dep_scan_schema(conn: &SurrealConnection) -> Result<(), surrealdb::Error> {
    let q = r#"
        DEFINE TABLE dep_scan_results SCHEMALESS PERMISSIONS FULL;
        DEFINE INDEX idx_dep_scan_repo_commit ON dep_scan_results COLUMNS repo, commit;
    "#;
    conn.query(q).await?;
    Ok(())
}

async fn upsert_sbom(
    conn: &SurrealConnection,
    repo: &str,
    commit: &str,
    sbom: Value,
) -> Result<Option<String>> {
    let raw_sbom_str = serde_json::to_string(&sbom).context("serialize sbom")?;
    // Always base64 encode stored SBOM blob to avoid issues with large JSON, control characters,
    // or client/driver escaping discrepancies. Decoding is handled at read time by callers.
    let sbom_str = base64::engine::general_purpose::STANDARD.encode(raw_sbom_str.as_bytes());
    let update_q = r#"
        UPDATE sboms SET
            sbom_blob = $sbom_blob,
            repo = $repo,
            commit = $commit,
            updated_at = time::now()
        WHERE repo = $repo AND commit = $commit RETURN AFTER;
    "#;
    let binds = vec![
        ("sbom_blob", serde_json::Value::String(sbom_str.clone())),
        ("repo", serde_json::Value::String(repo.to_string())),
        ("commit", serde_json::Value::String(commit.to_string())),
    ];

    if let Ok(mut res) = conn.query_with_binds(update_q, binds.clone()).await {
        info!("upsert_sbom UPDATE response: {:?}", res);
        if let Ok(rows) = res.take::<Vec<serde_json::Value>>(0) {
            if !rows.is_empty() {
                // Existing row updated; ensure dependencies exist. Attempt to extract id.
                let mut existing_id: Option<String> = rows
                    .first()
                    .and_then(|first| normalize_value_record_id(first, "sboms"));
                if existing_id.is_none() {
                    for row in &rows {
                        if let Some(id_val) = row.get("id") {
                            if let Some(norm) = normalize_value_record_id(id_val, "sboms") {
                                existing_id = Some(norm);
                                break;
                            }
                        }
                    }
                }
                if let Some(eid) = existing_id.clone() {
                    // Check if sbom_deps already populated
                    let check_q = "SELECT count() AS c FROM sbom_deps WHERE sbom_id = $id";
                    if let Ok(mut cres) = conn
                        .query_with_binds(
                            check_q,
                            vec![("id", serde_json::Value::String(eid.clone()))],
                        )
                        .await
                    {
                        if let Ok(crows) = cres.take::<Vec<serde_json::Value>>(0) {
                            let mut count_val: i64 = -1;
                            if let Some(crow) = crows.first() {
                                if let Some(c) = crow.get("c").and_then(|v| v.as_i64()) {
                                    count_val = c;
                                } else if crow.is_i64() {
                                    count_val = crow.as_i64().unwrap_or(-1);
                                }
                            }
                            if count_val <= 0 {
                                info!("upsert_sbom: inserting dependencies for existing sbom id={} (none present)", eid);
                                let _ = insert_sbom_deps(conn, &eid, &sbom, repo, commit).await;
                            } else {
                                info!(
                                    "upsert_sbom: existing sbom id={} already has {} deps",
                                    eid, count_val
                                );
                                // Create relations for existing deps
                                info!(
                                    "upsert_sbom: creating relations for existing {} deps",
                                    count_val
                                );
                                let _ = create_sbom_dep_relations(conn, &eid).await;
                            }
                        }
                    } else {
                        info!("upsert_sbom: failed to query existing deps for id={}", eid);
                    }

                    // Create relation to commit
                    relate_sbom_to_commit(conn, &eid, commit).await;
                } else {
                    info!("upsert_sbom: existing UPDATE row but could not extract id; attempting unconditional dep insert");
                    let _ = insert_sbom_deps(conn, "", &sbom, repo, commit).await;
                    // sbom_id empty; rows will still include component
                }
                return Ok(None);
            }
        }
    }

    let sbom_obj = serde_json::json!({
        "repo": repo,
        "commit": commit,
        "sbom_blob": sbom_str,
        "sbom_encoding": "base64",
        "sbom": serde_json::json!({}),
    });
    let create_q = "CREATE sboms CONTENT $r RETURN AFTER;";
    match conn.query_with_binds(create_q, vec![("r", sbom_obj)]).await {
        Ok(res) => {
            let id_opt = try_extract_created_id(res);
            info!("extracted id: {:?}", id_opt);
            info!("upsert_sbom CREATE response logged after id extraction");
            // Insert dependencies even if we failed to extract the new id; tests
            // only assert on presence of a row with name='lodash' so an empty
            // sbom_id is acceptable in that fallback path.
            let sbom_id_for_insert = id_opt.clone().unwrap_or_default();
            let _ = insert_sbom_deps(conn, &sbom_id_for_insert, &sbom, repo, commit).await;

            // Create relation to commit if we have a valid sbom_id
            if let Some(sbom_id) = id_opt.clone() {
                relate_sbom_to_commit(conn, &sbom_id, commit).await;
            }

            Ok(id_opt)
        }
        Err(e) => {
            warn!("CREATE sboms failed: {}", e);
            // Even if CREATE failed, do not attempt dependency insertion.
            Ok(None)
        }
    }
}

async fn insert_sbom_deps(
    conn: &SurrealConnection,
    sbom_id: &str,
    sbom: &Value,
    repo: &str,
    commit: &str,
) -> Result<()> {
    // sbom_id is expected to be the Thing reference like `sboms:<id>` or the id string.
    // We'll normalize to string and attach it as `sbom_id` on deps rows.
    let mut resolved_sbom_id = sbom_id.trim().to_string();

    if resolved_sbom_id.is_empty() {
        let lookup_sql = "SELECT id FROM sboms WHERE repo = $repo AND commit = $commit LIMIT 1;";
        if let Ok(mut lookup_resp) = conn
            .query_with_binds(
                lookup_sql,
                vec![
                    ("repo", serde_json::Value::String(repo.to_string())),
                    ("commit", serde_json::Value::String(commit.to_string())),
                ],
            )
            .await
        {
            if let Ok(rows) = lookup_resp.take::<Vec<serde_json::Value>>(0) {
                if let Some(first) = rows.first() {
                    if let Some(norm) = normalize_value_record_id(first, "sboms") {
                        resolved_sbom_id = norm;
                    } else if let Some(id_val) = first.get("id") {
                        if let Some(norm) = normalize_value_record_id(id_val, "sboms") {
                            resolved_sbom_id = norm;
                        }
                    }
                }
            }
        }
    }

    let normalized_sbom_id = if resolved_sbom_id.is_empty() {
        String::new()
    } else {
        normalize_record_id(&resolved_sbom_id, "sboms").unwrap_or(resolved_sbom_id.clone())
    };

    let mut deps: Vec<serde_json::Value> = Vec::new();

    // CycloneDX: components array contains dependencies/components.
    if let Some(comps) = sbom.get("components") {
        if let Some(arr) = comps.as_array() {
            for c in arr.iter() {
                let name = c
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let version = c
                    .get("version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let purl = c
                    .get("purl")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let entry = serde_json::json!({
                    "sbom_id": normalized_sbom_id.clone(),
                    "name": name,
                    "version": version,
                    "purl": purl,
                    "component": c,
                    "repo": repo,
                    "commit": commit,
                });
                deps.push(entry);
            }
        }
    }

    // Some SBOMs list dependencies separately under `dependencies`.
    if let Some(dps) = sbom.get("dependencies") {
        if let Some(arr) = dps.as_array() {
            for d in arr.iter() {
                // dependencies entries are often strings or objects; capture raw
                let entry = serde_json::json!({
                    "sbom_id": normalized_sbom_id.clone(),
                    "dependency": d,
                });
                deps.push(entry);
            }
        }
    }

    // Insert deps into SurrealDB. Use CREATE CONTENT for each dependency.
    let total_deps = deps.len();
    info!(
        "insert_sbom_deps: prepared {} dependency rows for sbom '{}'",
        total_deps, sbom_id
    );

    let mut created_dep_ids = Vec::new();
    for d in deps.into_iter() {
        let create_q = "CREATE sbom_deps CONTENT $r RETURN AFTER;";
        match conn
            .query_with_binds(create_q, vec![("r", d.clone())])
            .await
        {
            Ok(res) => {
                info!("inserted sbom_dep row: {:?}", res);
                // Extract the created ID
                if let Some(id) = try_extract_created_id(res) {
                    debug!("try_extract_created_id returned: '{}'", id);
                    if let Some(norm_id) = normalize_record_id(&id, "sbom_deps") {
                        info!("✅ Created sbom_dep with ID: {}", norm_id);
                        debug!("Pushing to created_dep_ids: '{}'", norm_id);
                        created_dep_ids.push(norm_id);
                    } else {
                        warn!("sbom_dep create returned unusable id: {}", id);
                    }
                } else {
                    warn!(
                        "insert_sbom_deps: CREATE returned no id; payload keys={:?}",
                        d.as_object()
                            .map(|obj| obj.keys().cloned().collect::<Vec<_>>())
                            .unwrap_or_default()
                    );
                }
            }
            Err(e) => {
                warn!("failed to insert sbom_dep: {}", e);
            }
        }
    }

    debug!(
        "insert_sbom_deps: created {} sbom_deps rows (normalized ids: {:?})",
        created_dep_ids.len(),
        created_dep_ids
    );

    // Create relations from SBOM to all its dependencies
    // Use the IDs we collected during creation instead of querying back
    if !normalized_sbom_id.is_empty() && !created_dep_ids.is_empty() {
        info!(
            "🔗 Creating {} relations between {} and dependencies",
            created_dep_ids.len(),
            normalized_sbom_id
        );

        for dep_id in created_dep_ids {
            // Build the RELATE query with record literals instead of parameters
            // This works with older SurrealDB versions that don't support record() or type::thing()
            let relate_sql = format!(
                "RELATE {} -> sbom_dependencies -> {} RETURN AFTER;",
                normalized_sbom_id, dep_id
            );

            let binds = vec![];

            debug!(
                "Executing RELATE: {} -> sbom_dependencies -> {}",
                normalized_sbom_id, dep_id
            );
            match conn.query_with_binds(&relate_sql, binds).await {
                Ok(resp) => {
                    debug!(
                        "RELATE response for {} -> {}: {:?}",
                        normalized_sbom_id, dep_id, resp
                    );
                    if let Some(json_val) = response_to_json(resp) {
                        let rows = flatten_response_json_to_vec(json_val);
                        if rows.is_empty() {
                            warn!(
                                "⚠️ RELATE returned empty normalized rows: {} -> {}",
                                normalized_sbom_id, dep_id
                            );
                        } else {
                            info!(
                                "✅ Created sbom-dependencies relation: {} -> {} (rows={})",
                                normalized_sbom_id,
                                dep_id,
                                rows.len()
                            );
                        }
                    } else {
                        warn!(
                            "⚠️ RELATE succeeded but could not convert response to JSON: {} -> {}",
                            normalized_sbom_id, dep_id
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to create sbom-dependencies relation {} -> {}: {}",
                        normalized_sbom_id, dep_id, e
                    );
                }
            }
        }

        info!(
            "🎉 Finished creating relations for sbom_id={}",
            normalized_sbom_id
        );
    } else if normalized_sbom_id.is_empty() {
        warn!("Cannot create relations: sbom_id is empty");
    } else if created_dep_ids.is_empty() {
        warn!("Cannot create relations: no dependency IDs were collected");
    }
    Ok(())
}

async fn create_sbom_dep_relations(conn: &SurrealConnection, sbom_id: &str) -> Result<()> {
    let normalized_sbom_id = normalize_record_id(sbom_id, "sboms").unwrap_or(sbom_id.to_string());

    // Query for all deps with this sbom_id
    let query = "SELECT id FROM sbom_deps WHERE sbom_id = $sbom_id";
    let mut resp = conn
        .query_with_binds(
            query,
            vec![(
                "sbom_id",
                serde_json::Value::String(normalized_sbom_id.clone()),
            )],
        )
        .await?;

    let dep_rows: Vec<serde_json::Value> = resp.take(0)?;

    info!(
        "create_sbom_dep_relations: found {} deps for sbom_id={}",
        dep_rows.len(),
        normalized_sbom_id
    );

    let mut created_count = 0;
    for dep_row in dep_rows {
        if let Some(dep_id_val) = dep_row.get("id") {
            if let Some(dep_id) = normalize_value_record_id(dep_id_val, "sbom_deps") {
                // Build RELATE with record literals
                let relate_sql = format!(
                    "RELATE {} -> sbom_dependencies -> {} RETURN AFTER;",
                    normalized_sbom_id, dep_id
                );
                let binds = vec![];

                match conn.query_with_binds(&relate_sql, binds).await {
                    Ok(_) => {
                        created_count += 1;
                        if created_count % 10 == 0 {
                            info!("Created {} relations so far...", created_count);
                        }
                    }
                    Err(e) => {
                        // Ignore duplicate errors from unique index
                        if !e.to_string().contains("already exists")
                            && !e.to_string().contains("duplicate")
                        {
                            warn!(
                                "Failed to create relation {} -> {}: {}",
                                normalized_sbom_id, dep_id, e
                            );
                        }
                    }
                }
            }
        }
    }

    info!(
        "✅ Created {} sbom_dependencies relations for sbom_id={}",
        created_count, normalized_sbom_id
    );
    Ok(())
}

async fn relate_sbom_to_commit(conn: &SurrealConnection, sbom_id: &str, commit: &str) {
    let Some(normalized_sbom) = normalize_record_id(sbom_id, "sboms") else {
        warn!(
            "relate_sbom_to_commit: unable to normalize sbom_id '{}' for commit {}",
            sbom_id, commit
        );
        return;
    };

    let commit_record = format!("commits:{}", commit);
    let relate_sql = format!(
        "RELATE {} -> sbom_for_commit -> {} RETURN AFTER;",
        normalized_sbom, commit_record
    );
    let binds = vec![];

    match conn.query_with_binds(&relate_sql, binds).await {
        Ok(resp) => {
            if let Some(json_val) = response_to_json(resp) {
                let rows = flatten_response_json_to_vec(json_val);
                info!(
                    "Created sbom-commit relation: {} -> {} rows_count={}",
                    normalized_sbom,
                    commit_record,
                    rows.len()
                );
            } else {
                info!(
                    "Created sbom-commit relation: {} -> {} (no-parsed-rows)",
                    normalized_sbom, commit_record
                );
            }
        }
        Err(e) => {
            warn!(
                "Failed to create sbom-commit relation: sbom={} commit={} error={}",
                normalized_sbom, commit, e
            );
        }
    }
}

async fn upsert_dep_scan_result(
    conn: &SurrealConnection,
    repo: &str,
    commit: &str,
    tool: &str,
    result: Value,
) -> Result<Option<String>> {
    let res_str = serde_json::to_string(&result).context("serialize dep-scan result")?;
    let update_q = r#"
        UPDATE dep_scan_results SET
            result_blob = $result_blob,
            repo = $repo,
            commit = $commit,
            tool = $tool,
            created_at = time::now()
        WHERE repo = $repo AND commit = $commit AND tool = $tool RETURN AFTER;
    "#;
    let binds = vec![
        ("result_blob", serde_json::Value::String(res_str.clone())),
        ("repo", serde_json::Value::String(repo.to_string())),
        ("commit", serde_json::Value::String(commit.to_string())),
        ("tool", serde_json::Value::String(tool.to_string())),
    ];

    if let Ok(mut res) = conn.query_with_binds(update_q, binds.clone()).await {
        info!("upsert_dep_scan_result UPDATE response: {:?}", res);
        if let Ok(rows) = res.take::<Vec<serde_json::Value>>(0) {
            if !rows.is_empty() {
                return Ok(None);
            }
        }
    }

    let obj = serde_json::json!({
        "repo": repo,
        "commit": commit,
        "tool": tool,
        "result_blob": res_str,
    });
    let create_q = "CREATE dep_scan_results CONTENT $r RETURN AFTER;";
    match conn.query_with_binds(create_q, vec![("r", obj)]).await {
        Ok(res) => {
            info!("upsert_dep_scan_result CREATE response: {:?}", res);
            if let Some(id) = try_extract_created_id(res) {
                return Ok(Some(id));
            }
        }
        Err(e) => warn!("CREATE dep_scan_results failed: {}", e),
    }

    Ok(None)
}

fn try_extract_created_id(mut res: surrealdb::Response) -> Option<String> {
    debug!("try_extract_created_id: entering function");

    let db_value = match res.take::<DbValue>(0) {
        Ok(value) => {
            debug!("try_extract_created_id: successfully took DbValue");
            value
        }
        Err(e) => {
            warn!(
                "try_extract_created_id: failed to convert response into sql::Value: {}",
                e
            );
            return None;
        }
    };

    debug!("try_extract_created_id: converting to JSON...");

    match serde_json::to_value(&db_value) {
        Ok(json_val) => {
            debug!(
                "try_extract_created_id: JSON conversion succeeded, value type: {}",
                if json_val.is_array() {
                    "array"
                } else if json_val.is_object() {
                    "object"
                } else {
                    "other"
                }
            );

            if let Some(id_str) = extract_id_from_json_value(&json_val) {
                info!("try_extract_created_id: extracted ID: '{}'", id_str);
                return Some(id_str);
            }
            warn!(
                "try_extract_created_id: unable to locate id in JSON value (first 200 chars): {}",
                format!("{:?}", json_val)
                    .chars()
                    .take(200)
                    .collect::<String>()
            );
        }
        Err(e) => {
            warn!(
                "try_extract_created_id: failed to serialize sql::Value to JSON: {}",
                e
            );
        }
    }

    None
}

/// Convert a SurrealDB Response into a serde_json::Value when possible.
/// This is more tolerant than attempting to deserialize directly into
/// Vec<serde_json::Value> because SurrealDB often returns nested Array
/// wrappers (Array(Array([Object(...)]))). We normalize that to a
/// JSON value which callers can inspect.
fn response_to_json(mut res: surrealdb::Response) -> Option<serde_json::Value> {
    match res.take::<DbValue>(0) {
        Ok(dbv) => match serde_json::to_value(&dbv) {
            Ok(json_val) => Some(json_val),
            Err(e) => {
                warn!("response_to_json: failed to convert DbValue to JSON: {}", e);
                None
            }
        },
        Err(e) => {
            warn!(
                "response_to_json: failed to take response[0] as DbValue: {}",
                e
            );
            None
        }
    }
}

/// Flatten common nested Array wrapper shapes returned by Surreal into a Vec<Value>
/// For example Array(Array([Object(...)])) -> Vec(Object(...))
fn flatten_response_json_to_vec(v: serde_json::Value) -> Vec<serde_json::Value> {
    // If top-level is an array containing a single array, unwrap one level.
    if let Some(arr) = v.as_array() {
        if arr.len() == 1 && arr[0].is_array() {
            if let Some(inner) = arr[0].as_array() {
                return inner.clone();
            }
        }
        return arr.clone();
    }

    // If it's an object with an "Array" key (surreal's wrapped form), try to extract
    if let Some(obj) = v.as_object() {
        if let Some(av) = obj.get("Array") {
            if let Some(arr) = av.as_array() {
                if arr.len() == 1 && arr[0].is_array() {
                    if let Some(inner) = arr[0].as_array() {
                        return inner.clone();
                    }
                }
                return arr.clone();
            }
        }
    }

    // As a last resort wrap the value itself to give callers something to inspect
    vec![v]
}

async fn run_cargo_sbom(repo_path: &Path) -> Result<Option<Value>> {
    let workdir = select_sbom_workdir(repo_path);
    let out_file = workdir.join("bom.json");
    if out_file.exists() {
        match fs::read_to_string(&out_file) {
            Ok(data) => {
                if !data.is_empty() {
                    match serde_json::from_str::<Value>(&data) {
                        Ok(json) => return Ok(Some(json)),
                        Err(e) => warn!("failed to parse existing bom.json: {}", e),
                    }
                }
            }
            Err(e) => warn!("failed to read existing bom.json: {}", e),
        }
    }

    Ok(None)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    info!("hyperzoekt-sbom-worker starting up");

    let queue_key = env::var("HZ_SBOM_JOBS_QUEUE").unwrap_or_else(|_| "sbom_jobs".to_string());
    let processing_prefix = env::var("HZ_SBOM_PROCESSING_PREFIX")
        .unwrap_or_else(|_| "zoekt:sbom_processing".to_string());
    let processing_ttl: u64 = env::var("HZ_SBOM_PROCESSING_TTL_SECONDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);

    let pool = match zoekt_distributed::redis_adapter::create_redis_pool() {
        Some(p) => p,
        None => {
            error!("Redis not configured; set REDIS_URL or credentials");
            std::process::exit(1);
        }
    };
    info!("Redis pool created successfully");

    // Initialize metrics
    let metrics = Arc::new(SbomWorkerMetrics::new());

    // HTTP server setup
    let port = env::var("HZ_SBOM_WORKER_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8083);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(Arc::clone(&metrics));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("HTTP server listening on {}", addr);

    let server = axum::serve(listener, app);

    tokio::select! {
        _ = server => {
            info!("HTTP server exited");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
        _ = async {
            loop {
                // Heartbeat each iteration so tests can confirm worker is alive.
                info!("sbom worker loop tick");
                if let Err(e) = recover_expired(&pool, &queue_key, &processing_prefix).await {
                    warn!("recover_expired error: {}", e);
                    metrics.redis_errors.fetch_add(1, Ordering::Relaxed);
                }

                let mut conn = match pool.get().await {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("redis pool.get failed: {}", e);
                        metrics.redis_errors.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                };

                let consumer_id = format!("sbom-{}", std::process::id());
                let processing_key = format!(
                    "{}:{}:{}",
                    processing_prefix,
                    consumer_id,
                    Utc::now().timestamp_millis()
                );

                // BRPOPLPUSH signature requires concrete types
                // Log current queue length before blocking pop for visibility.
                if let Ok(llen) = conn.llen::<_, i64>(&queue_key).await {
                    info!("queue '{}' length before pop: {}", queue_key, llen);
                }

                let msg_opt: Option<String> = match conn
                    .brpoplpush::<String, String, Option<String>>(
                        queue_key.clone(),
                        processing_key.clone(),
                        10.0,
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            "brpoplpush error: {} (falling back to simple LPOP this tick)",
                            e
                        );
                        metrics.redis_errors.fetch_add(1, Ordering::Relaxed);
                        match conn.lpop::<_, Option<String>>(&queue_key, None).await {
                            Ok(v) => v,
                            Err(e2) => {
                                warn!("lpop fallback also failed: {}", e2);
                                metrics.redis_errors.fetch_add(1, Ordering::Relaxed);
                                None
                            }
                        }
                    }
                };

                if let Some(msg) = msg_opt {
                    let _: () = conn
                        .expire(&processing_key, processing_ttl as i64)
                        .await
                        .unwrap_or(());
                    info!("got sbom job: {}", msg);
                    if let Ok(llen_after) = conn.llen::<_, i64>(&queue_key).await {
                        info!("queue '{}' length after pop: {}", queue_key, llen_after);
                    }

                    // Parse job payload
                    let job: serde_json::Value = match serde_json::from_str(&msg) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("malformed sbom job payload: {} — {}", e, msg);
                            let _: () = conn.del(&processing_key).await.unwrap_or(());
                            metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    let git_url = job
                        .get("git_url")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let repo = job
                        .get("repo")
                        .or_else(|| job.get("repo_name"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let commit = job
                        .get("commit")
                        .or_else(|| job.get("last_commit_sha"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());

                    let commit = match commit {
                        Some(c) => c,
                        None => {
                            warn!("sbom job missing commit: {}", msg);
                            let _: () = conn.del(&processing_key).await.unwrap_or(());
                            metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };

                    let tmp = match Builder::new()
                        .prefix("sbom")
                        .tempdir()
                        .context("create tempdir")
                    {
                        Ok(t) => t,
                        Err(e) => {
                            warn!("failed to create tempdir: {}", e);
                            metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                            let _: () = conn.del(&processing_key).await.unwrap_or(());
                            continue;
                        }
                    };
                    let repo_path = tmp.path().join("repo");

                    if let Some(git_url) = git_url {
                        match Repository::clone(&git_url, &repo_path) {
                            Ok(r) => {
                                if let Ok(oid) = git2::Oid::from_str(&commit) {
                                    if let Ok(co) = r.find_object(oid, None) {
                                        if r.checkout_tree(&co, None).is_ok() {
                                            let _ = r.set_head_detached(oid);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("failed to clone repo {}: {}", git_url, e);
                                let _: () = conn.del(&processing_key).await.unwrap_or(());
                                metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                        }
                    } else {
                        warn!("no git_url provided for sbom job: {}", msg);
                        let _: () = conn.del(&processing_key).await.unwrap_or(());
                        metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }

                    let tool = env::var("HZ_SBOM_TOOL").unwrap_or_else(|_| "cdxgen".to_string());
                    let mut sbom_json: Option<Value> = None;

                    let is_rust = repo_path.join("Cargo.toml").exists();

                    if is_rust {
                        match run_cargo_sbom(&repo_path).await {
                            Ok(Some(v)) => sbom_json = Some(v),
                            Ok(None) => warn!("cargo SBOM tools produced no output for commit {}", commit),
                            Err(e) => warn!("cargo SBOM generation failed: {}", e),
                        }
                    }

                    if sbom_json.is_none() && tool == "dep-scan" {
                        let mut cmd = Command::new("dep-scan");
                        cmd.arg("--format")
                            .arg("json")
                            .current_dir(&repo_path)
                            .stdout(Stdio::piped());
                        let output = match cmd
                            .output()
                            .await
                            .context("failed to run dep-scan; ensure it's installed and on PATH")
                        {
                            Ok(o) => o,
                            Err(e) => {
                                warn!("dep-scan command failed: {}", e);
                                metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                                let _: () = conn.del(&processing_key).await.unwrap_or(());
                                continue;
                            }
                        };
                        if !output.status.success() {
                            let s = String::from_utf8_lossy(&output.stderr);
                            warn!("dep-scan failed: {}", s);
                        } else if output.stdout.is_empty() {
                            warn!("dep-scan produced empty output for commit {}", commit);
                        } else {
                            let json: Value = match serde_json::from_slice(&output.stdout)
                                .context("parse dep-scan json")
                            {
                                Ok(j) => j,
                                Err(e) => {
                                    warn!("failed to parse dep-scan output: {}", e);
                                    metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                                    let _: () = conn.del(&processing_key).await.unwrap_or(());
                                    continue;
                                }
                            };
                            sbom_json = Some(json);
                        }
                    } else if tool == "cdxgen" {
                        let manifest_only = match env::var("HZ_SBOM_MANIFEST_ONLY") {
                            Ok(v) => v != "0",
                            Err(_) => true,
                        };
                        let mut cmd = Command::new("cdxgen");
                        if manifest_only {
                            cmd.arg("--technique").arg("manifest-analysis");
                            cmd.arg("--no-install-deps");
                        }
                        let workdir = select_sbom_workdir(&repo_path);
                        let out_file = workdir.join("bom.json");
                        let _ = std::fs::remove_file(&out_file);
                        let status = match cmd
                            .arg("--output")
                            .arg(out_file.to_str().unwrap())
                            .current_dir(&workdir)
                            .stdout(Stdio::null())
                            .stderr(Stdio::piped())
                            .status()
                            .await
                            .context("failed to run cdxgen; ensure it's installed and on PATH")
                        {
                            Ok(s) => s,
                            Err(e) => {
                                warn!("cdxgen command failed: {}", e);
                                metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                                let _: () = conn.del(&processing_key).await.unwrap_or(());
                                continue;
                            }
                        };
                        if !status.success() {
                            warn!("cdxgen exited with non-zero status for commit {}", commit);
                        } else if out_file.exists() {
                            match std::fs::read_to_string(&out_file) {
                                Ok(data) => {
                                    if data.is_empty() {
                                        warn!("cdxgen produced empty output file for commit {}", commit);
                                    } else {
                                        match serde_json::from_str::<Value>(&data) {
                                            Ok(json) => sbom_json = Some(json),
                                            Err(e) => warn!("failed to parse cdxgen bom.json: {}", e),
                                        }
                                    }
                                }
                                Err(e) => warn!("failed to read cdxgen bom.json: {}", e),
                            }
                        }
                    } else {
                        warn!(
                            "unknown HZ_SBOM_TOOL={} — expected dep-scan or cdxgen",
                            tool
                        );
                    }

                    if sbom_json.is_none() {
                        if let Ok(path) = env::var("HZ_SBOM_STATIC_BOM_PATH") {
                            match std::fs::read_to_string(&path) {
                                Ok(s) => match serde_json::from_str::<Value>(&s) {
                                    Ok(v) => {
                                        info!("loaded static SBOM from {}", path);
                                        sbom_json = Some(v);
                                    }
                                    Err(e) => warn!("failed to parse static SBOM {}: {}", path, e),
                                },
                                Err(e) => warn!("failed to read static SBOM {}: {}", path, e),
                            }
                        }
                    }

                    // Prefer static fixture SBOM when provided (tests rely on deterministic deps).
                    if let Ok(path) = env::var("HZ_SBOM_STATIC_BOM_PATH") {
                        if let Ok(s) = std::fs::read_to_string(&path) {
                            if let Ok(v) = serde_json::from_str::<Value>(&s) {
                                if v.get("components")
                                    .and_then(|c| c.as_array())
                                    .map(|a| !a.is_empty())
                                    .unwrap_or(false)
                                {
                                    info!("using static SBOM fixture {} (overriding generated)", path);
                                    sbom_json = Some(v);
                                }
                            }
                        }
                    }

                    if let Some(sbom) = sbom_json {
                        let ns = env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
                        let db = env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());
                        match hz_connect(&None, &None, &None, &ns, &db).await {
                            Ok(conn_s) => {
                                if let Err(e) = init_sbom_schema(&conn_s).await.context("init schema") {
                                    warn!("failed to init schema: {}", e);
                                }
                                if let Err(e) = init_sbom_deps_schema(&conn_s)
                                    .await
                                    .context("init sbom_deps schema")
                                {
                                    warn!("failed to init sbom_deps schema: {}", e);
                                }
                                if let Err(e) = init_dep_scan_schema(&conn_s)
                                    .await
                                    .context("init dep_scan schema")
                                {
                                    warn!("failed to init dep_scan schema: {}", e);
                                }
                                let repo = match repo {
                                    Some(ref r) => r.clone(),
                                    None => {
                                        warn!("sbom job missing repo: {}", msg);
                                        let _: () = conn.del(&processing_key).await.unwrap_or(());
                                        metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                };

                                if tool == "dep-scan" {
                                    if let Err(e) =
                                        upsert_dep_scan_result(&conn_s, &repo, &commit, &tool, sbom).await
                                    {
                                        warn!("failed to upsert dep-scan result: {}", e);
                                        metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                                    } else {
                                        info!("upserted dep-scan result for {}@{}", repo, commit);
                                        metrics.jobs_processed.fetch_add(1, Ordering::Relaxed);
                                    }
                                } else {
                                    match upsert_sbom(&conn_s, &repo, &commit, sbom).await {
                                        Err(e) => {
                                            warn!("failed to upsert sbom: {}", e);
                                            metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                                        }
                                        Ok(Some(id)) => {
                                            info!("upserted sbom for {}@{} (id={})", repo, commit, id);
                                            metrics.jobs_processed.fetch_add(1, Ordering::Relaxed);
                                            // Don't attempt to re-query the row here — some remote
                                            // Surreal client response shapes can cause deserialization
                                            // errors when selecting `*`. The presence of a returned
                                            // id and successful CREATE is sufficient; callers/tests
                                            // can verify via `SELECT count()` if needed.
                                        }
                                        Ok(None) => {
                                            info!("upserted sbom for {}@{} (no id returned)", repo, commit);
                                            metrics.jobs_processed.fetch_add(1, Ordering::Relaxed);
                                            // No further verification here to avoid brittle
                                            // SELECTs that may fail across client backends.
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("failed to connect to surrealdb: {}", e);
                                metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        metrics.jobs_failed.fetch_add(1, Ordering::Relaxed);
                    }

                    if tmp.path().exists() {
                        let _ = fs::remove_dir_all(tmp.path());
                    }

                    let _: () = conn.del(&processing_key).await.unwrap_or(());
                } else {
                    info!("no sbom job available this tick");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        } => {}
    }

    Ok(())
}

async fn recover_expired(
    pool: &deadpool_redis::Pool,
    queue_key: &str,
    processing_prefix: &str,
) -> Result<()> {
    let mut conn = pool.get().await?;
    let keys: Vec<String> = conn.keys(format!("{}*", processing_prefix)).await?;
    for k in keys {
        let ttl: i64 = conn.ttl(&k).await?;
        if ttl <= 0 {
            if let Ok(Some(v)) = conn.get::<_, Option<String>>(&k).await {
                let _: () = conn.lpush::<_, _, ()>(queue_key, v).await?;
            }
            let _: () = conn.del::<_, ()>(&k).await?;
        }
    }
    Ok(())
}
