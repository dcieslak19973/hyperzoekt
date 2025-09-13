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
use hyperzoekt::db_writer::connection::connect;
use hyperzoekt::db_writer::{persist_repo_dependencies, DbWriterConfig};
use hyperzoekt::repo_index::deps::Dependency;
use log::info;
use serial_test::serial;
use std::env;

// Strict test: ensures dependency row and direct dependency relationship are created (Mem engine, no fallbacks)
#[tokio::test]
#[serial]
async fn dependency_and_edge_created() {
    // Initialize logging for this test (no-op if already initialized)
    // Ensure test logs are visible in CI/local runs by defaulting to debug when
    // RUST_LOG is not set.
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "debug");
    }
    let _ = env_logger::builder().is_test(true).try_init();

    // Only run this test when a remote SurrealDB URL is configured. It must not run
    // against the in-memory engine because traversal/GraphQL behavior differs.
    let surreal_url = match env::var("SURREALDB_URL") {
        Ok(u) => {
            let (schemeful, _no_scheme) = hyperzoekt::test_utils::normalize_surreal_host(&u);
            std::env::set_var("SURREALDB_URL", &schemeful);
            std::env::set_var("SURREALDB_HTTP_BASE", &schemeful);
            schemeful
        }
        Err(_) => {
            eprintln!("Skipping dependency_and_edge_created: SURREALDB_URL not set");
            return;
        }
    };

    // Prefer namespace/database from env vars; fall back to sensible defaults
    let surreal_ns = env::var("SURREAL_NS").unwrap_or_else(|_| "strict_dep_ns".into());
    let surreal_db = env::var("SURREAL_DB").unwrap_or_else(|_| "strict_dep_db".into());

    info!(
        "Running dependency_and_edge_created against {} (ns={}, db={})",
        surreal_url, surreal_ns, surreal_db
    );

    // Try connecting to the remote Surreal instance first. If the URL is set
    // but the service is unreachable (DNS/IO errors), skip the test instead
    // of failing. This avoids CI flakes when SURREALDB_URL points to a
    // non-responsive host.
    if let Err(e) = connect(
        &Some(surreal_url.clone()),
        &None,
        &None,
        &surreal_ns,
        &surreal_db,
    )
    .await
    {
        eprintln!(
            "Skipping dependency_and_edge_created: unable to connect to SURREALDB_URL: {}",
            e
        );
        return;
    }

    let cfg = DbWriterConfig {
        channel_capacity: 4,
        batch_capacity: Some(10),
        batch_timeout_ms: Some(50),
        max_retries: Some(1),
        surreal_url: Some(surreal_url.clone()),
        surreal_username: None,
        surreal_password: None,
        surreal_ns: surreal_ns.clone(),
        surreal_db: surreal_db.clone(),
        initial_batch: false,
        ..Default::default()
    };
    let deps = vec![Dependency {
        name: "alpha".into(),
        version: Some("0.1.0".into()),
        language: "rust".into(),
    }];
    let res = persist_repo_dependencies(
        &cfg,
        "strict_repo",
        "git://example/strict_repo.git",
        None,
        None,
        None,
        None,
        &deps,
    )
    .await
    .expect("persist ok");
    info!("strict test persist result: {:?}", res);
    assert_eq!(res.deps_processed, 1);
    assert_eq!(res.deps_created, 1, "dependency should be created");
    // When a remote SurrealDB URL is configured, the writer should emit RELATE
    // statements and record created edges. Ensure we did not skip relation creation
    // and that at least one edge was created.
    assert_eq!(
        res.deps_skipped, 0,
        "dependency relation creation should not be skipped when SURREALDB_URL is set"
    );
    assert!(
        res.edges_created >= 1 || !res.edge_ids.is_empty(),
        "writer should create dependency relation edges when SURREALDB_URL is set"
    );
}
