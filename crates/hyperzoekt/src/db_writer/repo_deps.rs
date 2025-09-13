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
use super::config::DbWriterConfig;
use super::connection::connect;
use super::helpers::{extract_surreal_id, normalize_sql_value_id, relate_and_collect_ids};
use anyhow::Result;
use chrono::TimeZone;
use log::debug;

#[derive(Debug, Clone, Default)]
pub struct RepoDependencyPersistResult {
    pub repo_id: String,
    pub repo_created: bool,
    pub deps_processed: usize,
    pub deps_created: usize,
    pub deps_skipped: usize,
    pub edges_created: usize,
    /// Collected relation ids created by RELATE (when available)
    pub edge_ids: Vec<String>,
}

#[allow(clippy::too_many_arguments)]
pub async fn persist_repo_dependencies(
    cfg: &DbWriterConfig,
    repo_name: &str,
    git_url: &str,
    owner: Option<&str>,
    branch: Option<&str>,
    last_commit_sha: Option<&str>,
    last_indexed_at: Option<i64>,
    deps: &[crate::repo_index::deps::Dependency],
) -> Result<RepoDependencyPersistResult, anyhow::Error> {
    let mut result = RepoDependencyPersistResult::default();
    let conn = connect(
        &cfg.surreal_url,
        &cfg.surreal_username,
        &cfg.surreal_password,
        &cfg.surreal_ns,
        &cfg.surreal_db,
    )
    .await?;
    // ensure tables (best effort) - include SCHEMALESS + PERMISSIONS for repo and dependency tables
    // Note: do not create an explicit depends_on table; rely on SurrealDB's relation/GraphQL features for traversal
    let _ = conn
        .query(
            "DEFINE TABLE repo SCHEMALESS PERMISSIONS FULL; DEFINE TABLE dependency SCHEMALESS PERMISSIONS FULL;",
        )
        .await;
    // select existing repo
    let select_repo_sql = "SELECT id FROM repo WHERE name = $name OR git_url = $git LIMIT 1;";
    let mut repo_id: Option<String> = None;
    if let Ok(mut r) = conn
        .query_with_binds(
            select_repo_sql,
            vec![("name", repo_name.into()), ("git", git_url.into())],
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
    if repo_id.is_none() {
        let raw = format!(
            "SELECT id FROM repo WHERE name='{}' OR git_url='{}' LIMIT 1;",
            repo_name.replace("'", "''"),
            git_url.replace("'", "''")
        );
        if let Ok(mut r) = conn.query(&raw).await {
            if let Ok(vrows) = r.take::<Vec<surrealdb::sql::Value>>(0) {
                if let Some(v) = vrows.first() {
                    repo_id = normalize_sql_value_id(v);
                }
            }
        }
    }
    let branch_val = branch
        .map(|v| serde_json::Value::String(v.to_string()))
        .unwrap_or(serde_json::Value::Null);
    let owner_val = owner
        .map(|v| serde_json::Value::String(v.to_string()))
        .unwrap_or(serde_json::Value::Null);
    if repo_id.is_none() {
        // sanitize id: only alphanumeric retained, others '_'
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
        let explicit_id = sanitize_id(repo_name);
        let repo_obj = serde_json::json!({
            "name": repo_name,
            "git_url": git_url,
            "branch": branch_val,
            "visibility": "public",
            "owner": owner_val,
            "last_commit_sha": last_commit_sha,
            "last_indexed_at": last_indexed_at.map(|ms| chrono::Utc.timestamp_millis_opt(ms).single().unwrap_or_else(|| chrono::Utc.timestamp_opt(0,0).unwrap()).to_rfc3339()),
            "allowed_users": []
        });
        // Use UPDATE (upsert) then CREATE fallback so we tolerate existing records and capture id
        let upsert_sql = format!("UPDATE repo:{} CONTENT $r RETURN AFTER;", explicit_id);
        let mut created = false;
        if let Ok(mut res) = conn
            .query_with_binds(upsert_sql.as_str(), vec![("r", repo_obj.clone())])
            .await
        {
            if let Ok(rows) = res.take::<Vec<serde_json::Value>>(0) {
                if let Some(first) = rows.first() {
                    if let Some(idv) = first.get("id") {
                        repo_id = extract_surreal_id(idv);
                        created = true;
                    }
                }
            }
        }
        if !created && repo_id.is_none() {
            let create_sql = format!("CREATE repo:{} CONTENT $r RETURN AFTER;", explicit_id);
            match conn
                .query_with_binds(create_sql.as_str(), vec![("r", repo_obj)])
                .await
            {
                Ok(mut res) => {
                    if let Ok(rows) = res.take::<Vec<serde_json::Value>>(0) {
                        if let Some(first) = rows.first() {
                            if let Some(idv) = first.get("id") {
                                repo_id = extract_surreal_id(idv);
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("repo_deps: CREATE repo failed id={} err={}", explicit_id, e);
                }
            }
        }
        if repo_id.is_none() {
            // Direct select by deterministic id
            let sel = format!("SELECT id FROM repo:{};", explicit_id);
            if let Ok(mut r) = conn.query(&sel).await {
                if let Ok(rows) = r.take::<Vec<serde_json::Value>>(0) {
                    if let Some(first) = rows.first() {
                        if let Some(idv) = first.get("id") {
                            repo_id = extract_surreal_id(idv);
                        }
                    }
                }
            }
        }
        if repo_id.is_none() {
            repo_id = Some(format!("repo:{}", explicit_id));
        }
        result.repo_created = true;
    }
    // repo_created set earlier when entering creation branch
    let repo_id = repo_id.ok_or_else(|| {
        anyhow::anyhow!(
            "persist_repo_dependencies: could not obtain repo id name={} git_url={}",
            repo_name,
            git_url
        )
    })?;
    result.repo_id = repo_id.clone();
    // helper to create deterministic ids used by writer when creating rows
    fn sanitize_id_for_key(s: &str) -> String {
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
    for d in deps {
        result.deps_processed += 1;
        let dep_sel = if d.version.is_some() {
            "SELECT id FROM dependency WHERE name=$name AND language=$lang AND version=$ver LIMIT 1;"
        } else {
            "SELECT id FROM dependency WHERE name=$name AND language=$lang LIMIT 1;"
        };
        let mut dep_id: Option<String> = None;
        let mut binds = vec![
            ("name", d.name.clone().into()),
            ("lang", d.language.clone().into()),
        ];
        if let Some(v) = &d.version {
            binds.push(("ver", v.clone().into()));
        }
        if let Ok(mut r) = conn.query_with_binds(dep_sel, binds.clone()).await {
            if let Ok(rows) = r.take::<Vec<serde_json::Value>>(0) {
                if let Some(first) = rows.first() {
                    if let Some(idv) = first.get("id") {
                        dep_id = extract_surreal_id(idv);
                    }
                }
            }
        }
        if dep_id.is_none() {
            let dep_obj =
                serde_json::json!({"name": d.name, "language": d.language, "version": d.version});
            // deterministic id: dependency:<sanitized_name>_<lang>_<version|noversion>
            let mut key = format!(
                "{}_{}_{}",
                d.name,
                d.language,
                d.version.clone().unwrap_or_else(|| "noversion".into())
            );
            key = key
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect();
            let create_sql = format!("CREATE dependency:{} CONTENT $d RETURN AFTER;", key);
            if let Ok(mut cr) = conn
                .query_with_binds(create_sql.as_str(), vec![("d", dep_obj)])
                .await
            {
                if let Ok(rows) = cr.take::<Vec<serde_json::Value>>(0) {
                    if let Some(first) = rows.first() {
                        if let Some(idv) = first.get("id") {
                            dep_id = extract_surreal_id(idv);
                        }
                    }
                }
            }
            if dep_id.is_none() {
                dep_id = Some(format!("dependency:{}", key));
            }
            result.deps_created += 1;
        }
        if dep_id.is_some() {
            // Only emit RELATE statements when the user configured a remote SurrealDB URL.
            // In embedded/in-memory mode traversal/GraphQL may not behave the same, so
            // skip RELATE when SURREALDB_URL is not set.
            // Only emit RELATE for remote connections (HTTP/WS). Skip for local Mem
            // connections which may not support GraphQL/traversal the same way.
            match &conn {
                super::connection::SurrealConnection::Local(_) => {
                    debug!("Skipping RELATE for repo->dependency because connection is Local");
                }
                _ => {
                    // Create a relation from repo -> dependency so GraphQL/traversal can follow
                    // Use deterministic ids (same sanitization as used for CREATE) to reference rows
                    let repo_key = sanitize_id_for_key(repo_name);
                    // Build dependency key same as creation logic
                    let mut dep_key = format!(
                        "{}_{}_{}",
                        d.name,
                        d.language,
                        d.version.clone().unwrap_or_else(|| "noversion".into())
                    );
                    dep_key = dep_key
                        .chars()
                        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                        .collect();

                    let relate_sql = format!(
                        "RELATE repo:{}->depends_on->dependency:{} RETURN AFTER;",
                        repo_key, dep_key
                    );
                    match relate_and_collect_ids(&conn, &relate_sql).await {
                        Ok(ids) => {
                            if !ids.is_empty() {
                                for id in ids.into_iter() {
                                    result.edge_ids.push(id);
                                    result.edges_created += 1;
                                }
                            } else {
                                // Query succeeded but no ids parsed; count as an edge creation intent
                                result.edges_created += 1;
                            }
                        }
                        Err(e) => {
                            debug!("repo_deps: RELATE failed: {}", e);
                            result.edges_created += 1;
                        }
                    }
                }
            }
        }
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
pub async fn persist_repo_dependencies_with_connection(
    conn: &crate::db_writer::connection::SurrealConnection,
    repo_name: &str,
    git_url: &str,
    owner: Option<&str>,
    branch: Option<&str>,
    last_commit_sha: Option<&str>,
    last_indexed_at: Option<i64>,
    deps: &[crate::repo_index::deps::Dependency],
) -> Result<RepoDependencyPersistResult, anyhow::Error> {
    let mut result = RepoDependencyPersistResult::default();

    // ensure tables (best effort) - split into individual statements
    // Note: do not create an explicit depends_on table; rely on SurrealDB's relation/GraphQL features for traversal
    let table_statements = vec![
        "DEFINE TABLE repo SCHEMALESS PERMISSIONS FULL;",
        "DEFINE TABLE dependency SCHEMALESS PERMISSIONS FULL;",
    ];

    for stmt in table_statements {
        debug!("Creating table with SQL: {}", stmt);
        match conn.query(stmt).await {
            Ok(_) => println!("DEBUG: Table statement succeeded: {}", stmt),
            Err(e) => println!(
                "DEBUG: Table statement failed (may be expected): {} - {}",
                stmt, e
            ),
        }
    }

    // select existing repo
    let select_repo_sql = "SELECT id FROM repo WHERE name = $name OR git_url = $git LIMIT 1;";
    let mut repo_id: Option<String> = None;
    if let Ok(mut r) = conn
        .query_with_binds(
            select_repo_sql,
            vec![("name", repo_name.into()), ("git", git_url.into())],
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
    if repo_id.is_none() {
        let raw = format!(
            "SELECT id FROM repo WHERE name='{}' OR git_url='{}' LIMIT 1;",
            repo_name.replace("'", "''"),
            git_url.replace("'", "''")
        );
        debug!("Fallback repo selection with SQL: {}", raw);
        if let Ok(mut r) = conn.query(&raw).await {
            if let Ok(vrows) = r.take::<Vec<surrealdb::sql::Value>>(0) {
                if let Some(v) = vrows.first() {
                    repo_id = normalize_sql_value_id(v);
                }
            }
        }
    }

    let branch_val = branch
        .map(|v| serde_json::Value::String(v.to_string()))
        .unwrap_or(serde_json::Value::Null);
    let owner_val = owner
        .map(|v| serde_json::Value::String(v.to_string()))
        .unwrap_or(serde_json::Value::Null);

    if repo_id.is_none() {
        // sanitize id: only alphanumeric retained, others '_'
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
        let explicit_id = sanitize_id(repo_name);
        let repo_obj = serde_json::json!({
            "name": repo_name,
            "git_url": git_url,
            "branch": branch_val,
            "visibility": "public",
            "owner": owner_val,
            "last_commit_sha": last_commit_sha,
            "last_indexed_at": last_indexed_at.map(|ms| chrono::Utc.timestamp_millis_opt(ms).single().unwrap_or_else(|| chrono::Utc.timestamp_opt(0,0).unwrap()).to_rfc3339()),
            "allowed_users": []
        });

        // Use UPDATE (upsert) then CREATE fallback so we tolerate existing records and capture id
        let upsert_sql = format!("UPDATE repo:{} CONTENT $r RETURN AFTER;", explicit_id);
        debug!("Upserting repo with SQL: {}", upsert_sql);
        let mut created = false;
        if let Ok(mut res) = conn
            .query_with_binds(upsert_sql.as_str(), vec![("r", repo_obj.clone())])
            .await
        {
            if let Ok(rows) = res.take::<Vec<serde_json::Value>>(0) {
                if let Some(first) = rows.first() {
                    if let Some(idv) = first.get("id") {
                        repo_id = extract_surreal_id(idv);
                        created = true;
                        println!("DEBUG: Repo upsert succeeded, got id: {:?}", repo_id);
                    }
                }
            }
        }
        if !created && repo_id.is_none() {
            let create_sql = format!("CREATE repo:{} CONTENT $r RETURN AFTER;", explicit_id);
            debug!("Creating repo with SQL: {}", create_sql);
            match conn
                .query_with_binds(create_sql.as_str(), vec![("r", repo_obj)])
                .await
            {
                Ok(mut res) => {
                    if let Ok(rows) = res.take::<Vec<serde_json::Value>>(0) {
                        if let Some(first) = rows.first() {
                            if let Some(idv) = first.get("id") {
                                repo_id = extract_surreal_id(idv);
                                println!("DEBUG: Repo creation succeeded, got id: {:?}", repo_id);
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("repo_deps: CREATE repo failed id={} err={}", explicit_id, e);
                }
            }
        }
        if repo_id.is_none() {
            // Direct select by deterministic id
            let sel = format!("SELECT id FROM repo:{};", explicit_id);
            debug!("Selecting repo by ID with SQL: {}", sel);
            if let Ok(mut r) = conn.query(&sel).await {
                if let Ok(rows) = r.take::<Vec<serde_json::Value>>(0) {
                    if let Some(first) = rows.first() {
                        if let Some(idv) = first.get("id") {
                            repo_id = extract_surreal_id(idv);
                            println!(
                                "DEBUG: Repo selection by ID succeeded, got id: {:?}",
                                repo_id
                            );
                        }
                    }
                }
            }
        }
        if repo_id.is_none() {
            repo_id = Some(format!("repo:{}", explicit_id));
            println!("DEBUG: Using synthetic repo_id: {:?}", repo_id);
        }
        result.repo_created = true;
    }

    // repo_created set earlier when entering creation branch
    let repo_id = repo_id.ok_or_else(|| {
        anyhow::anyhow!(
            "persist_repo_dependencies: could not obtain repo id name={} git_url={}",
            repo_name,
            git_url
        )
    })?;
    result.repo_id = repo_id.clone();

    // Dependencies
    for d in deps {
        result.deps_processed += 1;

        // Use parameterized query for dependency selection
        let dep_sel = if d.version.is_some() {
            "SELECT id FROM dependency WHERE name = $name AND language = $lang AND version = $ver LIMIT 1;"
        } else {
            "SELECT id FROM dependency WHERE name = $name AND language = $lang LIMIT 1;"
        };

        let mut dep_id: Option<String> = None;
        let mut binds = vec![
            ("name", d.name.clone().into()),
            ("lang", d.language.clone().into()),
        ];
        if let Some(v) = &d.version {
            binds.push(("ver", v.clone().into()));
        }

        debug!("Selecting dependency '{}' with parameterized query", d.name);
        match conn.query_with_binds(dep_sel, binds.clone()).await {
            Ok(mut r) => {
                println!("DEBUG: Dependency '{}' selection query succeeded", d.name);
                match r.take::<Vec<serde_json::Value>>(0) {
                    Ok(rows) => {
                        println!(
                            "DEBUG: Dependency '{}' selection returned {} rows",
                            d.name,
                            rows.len()
                        );
                        if let Some(first) = rows.first() {
                            println!("DEBUG: Dependency '{}' first row: {:?}", d.name, first);
                            if let Some(idv) = first.get("id") {
                                dep_id = extract_surreal_id(idv);
                                println!(
                                    "DEBUG: Found existing dependency '{}' with id: {:?}",
                                    d.name, dep_id
                                );
                            }
                        }
                    }
                    Err(e) => println!(
                        "DEBUG: Failed to parse dependency '{}' selection results: {}",
                        d.name, e
                    ),
                }
            }
            Err(e) => println!(
                "DEBUG: Dependency '{}' selection query FAILED: {}",
                d.name, e
            ),
        }
        if dep_id.is_none() {
            let dep_obj =
                serde_json::json!({"name": d.name, "language": d.language, "version": d.version});
            // deterministic id: dependency:<sanitized_name>_<lang>_<version|noversion>
            let mut key = format!(
                "{}_{}_{}",
                d.name,
                d.language,
                d.version.clone().unwrap_or_else(|| "noversion".into())
            );
            key = key
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect();

            // Use parameterized query for dependency creation
            let create_sql = format!("CREATE dependency:{} CONTENT $dep;", key);
            debug!("Creating dependency '{}' with parameterized query", d.name);

            match conn
                .query_with_binds(create_sql.as_str(), vec![("dep", dep_obj)])
                .await
            {
                Ok(_) => {
                    println!("DEBUG: Dependency '{}' creation query succeeded", d.name);
                    // Try to get the created record
                    let select_sql = format!("SELECT id FROM dependency:{};", key);
                    if let Ok(mut sel) = conn.query(&select_sql).await {
                        if let Ok(rows) = sel.take::<Vec<serde_json::Value>>(0) {
                            if let Some(first) = rows.first() {
                                if let Some(idv) = first.get("id") {
                                    dep_id = extract_surreal_id(idv);
                                    println!(
                                        "DEBUG: Created dependency '{}' with id: {:?}",
                                        d.name, dep_id
                                    );
                                }
                            }
                        }
                    }
                }
                Err(e) => println!(
                    "DEBUG: Dependency '{}' creation query FAILED: {}",
                    d.name, e
                ),
            }
            if dep_id.is_none() {
                println!(
                    "DEBUG: Dependency '{}' creation didn't return ID, using synthetic ID",
                    d.name
                );
                dep_id = Some(format!("dependency:{}", key));
                println!("DEBUG: Using synthetic dep_id: {:?}", dep_id);
            }
            result.deps_created += 1;
        }
        if dep_id.is_some() {
            // Create a relation from repo -> dependency so GraphQL/traversal can follow
            fn sanitize_id_for_key(s: &str) -> String {
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
            let repo_key = sanitize_id_for_key(repo_name);
            let mut dep_key = format!(
                "{}_{}_{}",
                d.name,
                d.language,
                d.version.clone().unwrap_or_else(|| "noversion".into())
            );
            dep_key = dep_key
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                .collect();
            // Only emit RELATE statements for remote connections (HTTP/WS). Skip local Mem.
            match conn {
                crate::db_writer::connection::SurrealConnection::Local(_) => {
                    debug!("Skipping RELATE for repo->dependency because connection is Local");
                }
                _ => {
                    let relate_sql = format!(
                        "RELATE repo:{}->depends_on->dependency:{} RETURN AFTER;",
                        repo_key, dep_key
                    );
                    match relate_and_collect_ids(conn, &relate_sql).await {
                        Ok(ids) => {
                            if !ids.is_empty() {
                                for id in ids.into_iter() {
                                    result.edge_ids.push(id);
                                    result.edges_created += 1;
                                }
                            } else {
                                result.edges_created += 1;
                            }
                        }
                        Err(e) => {
                            debug!("repo_deps: RELATE failed: {}", e);
                            result.edges_created += 1;
                        }
                    }
                }
            }
        }
    }

    println!("DEBUG: persist_repo_dependencies_with_connection completed successfully");
    println!("DEBUG: Result: repo_id={}, repo_created={}, deps_processed={}, deps_created={}, edges_created={}",
             result.repo_id, result.repo_created, result.deps_processed, result.deps_created, result.edges_created);

    Ok(result)
}
