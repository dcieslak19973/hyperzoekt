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

use hyperzoekt::db::connection::{connect, SurrealConnection};
use hyperzoekt::db::queries::DatabaseQueries;
use serde::Deserialize;
use serde_json::{to_value, Value};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn timestamp_suffix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u128)
        .unwrap_or(0)
        .wrapping_rem(1_000_000_000_000) as u64
}

fn surreal_remote_url() -> Option<String> {
    // Prefer explicit test override, then the common SURREALDB_URL env var.
    // Only fall back to a localhost developer URL when ALLOW_LOCAL_SURREAL=1
    // is set to avoid CI flakes where no local Surreal is available.
    if let Ok(u) = std::env::var("SURREAL_TEST_HTTP_URL") {
        return Some(u);
    }
    if let Ok(u) = std::env::var("SURREALDB_URL") {
        return Some(u);
    }
    if std::env::var("ALLOW_LOCAL_SURREAL").ok().as_deref() == Some("1") {
        return Some("http://127.0.0.1:8000".to_string());
    }
    None
}

async fn run_query(
    conn: &SurrealConnection,
    sql: &str,
) -> Result<surrealdb::Response, surrealdb::Error> {
    match conn {
        SurrealConnection::Local(db) => db.query(sql).await,
        SurrealConnection::RemoteHttp(db) => db.query(sql).await,
        SurrealConnection::RemoteWs(db) => db.query(sql).await,
    }
}

async fn run_query_with_binds(
    conn: &SurrealConnection,
    sql: &str,
    binds: Vec<(&'static str, Value)>,
) -> Result<surrealdb::Response, surrealdb::Error> {
    match conn {
        SurrealConnection::Local(db) => {
            let mut call = db.query(sql);
            for (k, v) in binds.into_iter() {
                call = call.bind((k, v));
            }
            call.await
        }
        SurrealConnection::RemoteHttp(db) => {
            let mut call = db.query(sql);
            for (k, v) in binds.into_iter() {
                call = call.bind((k, v));
            }
            call.await
        }
        SurrealConnection::RemoteWs(db) => {
            let mut call = db.query(sql);
            for (k, v) in binds.into_iter() {
                call = call.bind((k, v));
            }
            call.await
        }
    }
}

#[derive(Deserialize)]
struct CreatedRow {
    id: surrealdb::sql::Thing,
}

#[tokio::test(flavor = "multi_thread")]
async fn dependencies_resolve_via_relations_and_sbom() -> Result<(), Box<dyn std::error::Error>> {
    // Attempt to connect to the live Surreal instance. If it's unavailable or
    // not configured, skip the test to avoid touching localhost in CI.
    let url = match surreal_remote_url() {
        Some(u) => u,
        None => {
            eprintln!("Skipping dependencies_remote: no remote SurrealDB configured (set SURREALDB_URL or ALLOW_LOCAL_SURREAL=1)");
            return Ok(());
        }
    };
    let user = std::env::var("SURREALDB_USERNAME").ok();
    let pass = std::env::var("SURREALDB_PASSWORD").ok();
    let suffix = timestamp_suffix();
    let ns = format!("deps_ns_{}", suffix);
    let db_name = format!("deps_db_{}", suffix);

    let conn = match connect(&Some(url.clone()), &user, &pass, &ns, &db_name).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "dependencies_remote: skipping test, unable to connect to {:?}: {}",
                url, e
            );
            return Ok(());
        }
    };

    let _ = env_logger::builder().is_test(true).try_init();

    // Make sure the connection is scoped to our isolated namespace/database.
    conn.use_ns(&ns).await?;
    conn.use_db(&db_name).await?;

    let conn = Arc::new(conn);
    let queries = DatabaseQueries::new(conn.clone());

    // Ensure the minimal schema for repo/dependency relations exists for this namespace.
    let schema_sql = r#"
        DEFINE TABLE repo SCHEMALESS PERMISSIONS FULL;
        DEFINE TABLE dependency SCHEMALESS PERMISSIONS FULL;
        DEFINE TABLE depends_on TYPE RELATION FROM repo TO dependency PERMISSIONS FULL;
    "#;
    run_query(&conn, schema_sql).await?;

    // Scenario 1: canonical depends_on relation should be discovered via traversal.
    let repo_name = format!("deps_rel_{}", suffix);
    let mut resp = run_query_with_binds(
        &conn,
        "CREATE repo CONTENT { name: $name, branch: 'main' } RETURN AFTER;",
        vec![("name", Value::String(repo_name.clone()))],
    )
    .await?;
    let repo_rows: Vec<CreatedRow> = resp.take(0)?;
    let repo_thing = repo_rows
        .first()
        .ok_or("failed to create repo row")?
        .id
        .clone();
    let repo_id_str = repo_thing.to_string();

    let mut resp = run_query(
        &conn,
        "CREATE dependency CONTENT { name: 'serde', version: '1.0.0', language: 'rust' } RETURN AFTER;",
    )
    .await?;
    let dep_rows: Vec<CreatedRow> = resp.take(0)?;
    let dep_thing = dep_rows
        .first()
        .ok_or("failed to create dependency row")?
        .id
        .clone();
    let dep_id_str = dep_thing.to_string();

    let relate_sql = format!("RELATE {}->depends_on->{};", repo_id_str, dep_id_str);
    run_query(&conn, &relate_sql).await?;

    let deps = queries
        .get_dependencies_for_repo(&repo_name, Some("main"))
        .await?;
    assert_eq!(deps.len(), 1, "expected exactly one canonical dependency");
    let entry = deps.first().unwrap();
    assert_eq!(entry.get("name").and_then(|v| v.as_str()), Some("serde"));
    assert_eq!(entry.get("version").and_then(|v| v.as_str()), Some("1.0.0"));

    // Scenario 2: when canonical edges are absent, SBOM fallback should surface dependencies.
    let repo_name_sbom = format!("deps_sbom_{}", suffix);
    let commit_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    let mut resp = run_query_with_binds(
        &conn,
        "CREATE repo CONTENT { name: $name, branch: 'main' } RETURN AFTER;",
        vec![("name", Value::String(repo_name_sbom.clone()))],
    )
    .await?;
    let repo_rows: Vec<CreatedRow> = resp.take(0)?;
    let sbom_repo_thing = repo_rows
        .first()
        .ok_or("failed to create sbom repo row")?
        .id
        .clone();

    let mut resp = run_query_with_binds(
        &conn,
        "CREATE sboms CONTENT { repo: $repo, commit: $commit } RETURN AFTER;",
        vec![
            ("repo", Value::String(repo_name_sbom.clone())),
            ("commit", Value::String(commit_hash.to_string())),
        ],
    )
    .await?;
    let sbom_rows: Vec<CreatedRow> = resp.take(0)?;
    let sbom_id = sbom_rows
        .first()
        .ok_or("failed to create sbom row")?
        .id
        .to_string();

    run_query_with_binds(
        &conn,
        "CREATE sbom_deps CONTENT { sbom_id: $sbom_id, name: 'dep-from-sbom', version: '0.1.0', purl: 'pkg:cargo/dep-from-sbom@0.1.0' } RETURN AFTER;",
        vec![("sbom_id", Value::String(sbom_id.clone()))],
    )
    .await?;

    let deps = queries
        .get_dependencies_for_repo(&repo_name_sbom, Some(commit_hash))
        .await?;
    assert_eq!(deps.len(), 1, "expected one dependency from SBOM fallback");
    let entry = deps.first().unwrap();
    assert_eq!(
        entry.get("name").and_then(|v| v.as_str()),
        Some("dep-from-sbom")
    );
    assert_eq!(entry.get("version").and_then(|v| v.as_str()), Some("0.1.0"));
    assert_eq!(
        entry.get("purl").and_then(|v| v.as_str()),
        Some("pkg:cargo/dep-from-sbom@0.1.0"),
    );

    // Ensure no stray canonical records exist for fallback repo.
    let mut resp = run_query_with_binds(
        &conn,
        "SELECT VALUE count() FROM depends_on WHERE in = $repo_id;",
        vec![("repo_id", to_value(&sbom_repo_thing)?)],
    )
    .await?;
    let counts: Vec<Value> = resp.take(0)?;
    let count_val = counts.first().and_then(|v| v.as_u64()).unwrap_or_default();
    assert_eq!(
        count_val, 0,
        "expected no canonical depends_on edges for fallback repo"
    );

    Ok(())
}
