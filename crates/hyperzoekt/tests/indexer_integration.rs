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

//! Integration test: simulate an indexing_finished repo event and run the indexer pipeline
//! (clone -> index -> persist entities) without requiring Redis.
//!
//! This exercises `EventProcessor::process_event` the same way the
//! `hyperzoekt-indexer` binary does after receiving a queued event.
//!
//! Requirements / assumptions:
//! - Uses a small local fixture repository under `tests/fixtures/example-treesitter-repo`.
//! - SurrealDB: attempts remote if `SURREALDB_URL` is set (with auth if provided),
//!   otherwise uses the embedded Mem engine via db_writer connection logic.
//! - Skips dependency traversal assertions if permissions prevent table creation.

use hyperzoekt::db::connection::connect;
use hyperzoekt::event_consumer::EventProcessor;
use std::time::Duration;
use zoekt_distributed::lease_manager::RepoEvent;

// Helper: create an EventProcessor without Redis by constructing an unbounded channel.
fn test_event_processor() -> EventProcessor {
    let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
    EventProcessor::new(rx)
}

#[tokio::test]
async fn indexing_finished_event_end_to_end() {
    // Gate long-running test behind an env var so CI can skip it by default.
    if std::env::var("LONG_RUNNING_INTEGRATION_TESTS").is_err() {
        eprintln!(
            "Skipping indexing_finished_event_end_to_end: set LONG_RUNNING_INTEGRATION_TESTS=1 to run"
        );
        return;
    }

    // Spawn a periodic progress logger that prints every 30 seconds while the
    // test is running to reassure long-running CI runners that the test is alive.
    let progress_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            eprintln!("indexing_finished_event_end_to_end: still running...");
        }
    });

    // We'll abort the progress logger when the test scope ends by dropping the
    // handle (we'll explicitly abort it near the end of the test).
    // Reduce noise in test logs; default includes silence for the hyper_util pool module.
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var(
            "RUST_LOG",
            "info,hyper_util::client::legacy::pool=warn,hyper_util=warn",
        );
    }
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .is_test(true)
        .try_init();

    // Build a fake indexing_finished event pointed at our local fixture repo.
    // For git_url we can use a file:// URL so the clone step can proceed.
    let fixture_repo = std::path::Path::new("tests/fixtures/example-treesitter-repo");
    assert!(fixture_repo.exists(), "fixture repo missing");
    // Create a temporary git repository and copy fixture contents into it so git2 clone works over file://
    let temp_src = tempfile::tempdir().expect("temp repo dir");
    let temp_path = temp_src.path().to_path_buf();
    // init git repo
    {
        let repo = git2::Repository::init(&temp_path).expect("init git repo");
        // copy files
        for entry in walkdir::WalkDir::new(fixture_repo) {
            let e = entry.expect("walk entry");
            if e.file_type().is_file() {
                let rel = e.path().strip_prefix(fixture_repo).unwrap();
                let dest = temp_path.join(rel);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }
                std::fs::copy(e.path(), &dest).expect("copy file");
            }
        }
        // add + commit
        let mut index = repo.index().expect("index");
        index
            .add_all(["."].iter(), git2::IndexAddOption::DEFAULT, None)
            .expect("add all");
        index.write().expect("index write");
        let tree_id = index.write_tree().expect("write tree");
        let tree = repo.find_tree(tree_id).expect("find tree");
        let sig = git2::Signature::now("tester", "tester@example.com").expect("sig");
        repo.commit(Some("HEAD"), &sig, &sig, "initial", &tree, &[])
            .expect("commit");
    }
    let git_url = format!("file://{}", temp_path.display());
    let event = RepoEvent {
        event_type: "indexing_finished".into(),
        repo_name: "fixture_example".into(),
        git_url: git_url.clone(),
        branch: None, // let clone logic use default HEAD
        node_id: "test-node".into(),
        timestamp: 0,
        last_commit_sha: None,
        last_indexed_at: Some(chrono::Utc::now().timestamp_millis()),
    };

    // If using remote Surreal, perform targeted cleanup for this repo before processing so
    // validations are deterministic (no residue from prior test runs).
    if std::env::var("SURREALDB_URL").is_ok() {
        let surreal_url = std::env::var("SURREALDB_URL").ok();
        let user = std::env::var("SURREALDB_USERNAME").ok();
        let pass = std::env::var("SURREALDB_PASSWORD").ok();
        let ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
        let db = std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());
        if let Ok(conn) = connect(&surreal_url, &user, &pass, &ns, &db).await {
            for stmt in [
                "DELETE has_file WHERE in=repo:fixture_example;",
                "DELETE in_repo WHERE out=repo:fixture_example;",
                "DELETE repo:fixture_example;",
            ] {
                let _ = conn.query(stmt).await; // best-effort
            }
        }
    }

    // Run process_event directly. We construct a fresh EventProcessor
    // and call its now pub(crate) process_event method.
    let processor = test_event_processor();
    // Allow writer to run normally (no HZ_SINGLE_BATCH=1) so multi-batch behavior can be exercised
    if let Err(e) = processor.process_event(event.clone()).await {
        panic!("process_event failed: {e}");
    }

    // After processing, we expect the repo to have been cloned, indexed, and entities
    // persisted via the DB writer. When using in-memory Surreal (no SURREALDB_URL),
    // the connection state is isolated per process; we therefore only assert that
    // the indexer completed by verifying no panic and that a temp directory is removed.
    // If remote Surreal is configured we can attempt a lightweight verification.
    if std::env::var("SURREALDB_URL").is_ok() {
        // Best-effort remote verification: poll a few times for a repo row by name.
        let surreal_url = std::env::var("SURREALDB_URL").ok();
        let user = std::env::var("SURREALDB_USERNAME").ok();
        let pass = std::env::var("SURREALDB_PASSWORD").ok();
        let ns = std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into());
        let db = std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into());
        let conn = match connect(&surreal_url, &user, &pass, &ns, &db).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "Skipping indexing_finished_event_end_to_end: unable to connect to SurrealDB: {e}"
                );
                return;
            }
        };
        let mut found = false;
        for _ in 0..20 {
            if let Ok(mut resp) = conn
                .query("SELECT count() AS c FROM repo WHERE name='fixture_example';")
                .await
            {
                if let Ok(rows) = resp.take::<Vec<serde_json::Value>>(0) {
                    if let Some(first) = rows.first() {
                        // Some Surreal responses name the count field `c`, others `count`.
                        let cnt = first
                            .get("c")
                            .or_else(|| first.get("count"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        if cnt > 0 {
                            found = true;
                            break;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            found,
            "expected repo row for fixture_example in remote Surreal"
        );

        // Additional validation: ensure single repo, multiple files, and bidirectional repo<->file edges.
        // Helper closure to run a count query returning u64 (0 on error/empty).
        async fn run_count(conn: &hyperzoekt::db::connection::SurrealConnection, q: &str) -> u64 {
            if let Ok(mut resp) = conn.query(q).await {
                if let Ok(rows) = resp.take::<Vec<serde_json::Value>>(0) {
                    if let Some(first) = rows.first() {
                        return first
                            .get("c")
                            .or_else(|| first.get("count"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                    }
                }
            }
            0
        }

        // Repo-scoped counts (total repos containing this name, total files linked to it by either direction)
        let repo_total = run_count(
            &conn,
            "SELECT count() AS c FROM repo WHERE name='fixture_example' GROUP ALL;",
        )
        .await;
        assert_eq!(
            repo_total, 1,
            "expected exactly 1 repo row for fixture_example, got {}",
            repo_total
        );

        // Direct file table count (ensures file rows persisted, not just edges)
        let file_table_total = run_count(&conn, "SELECT count() AS c FROM file GROUP ALL;").await;
        assert!(
            file_table_total > 0,
            "expected >0 rows in file table, got {}",
            file_table_total
        );

        // Count distinct file ids reachable from commit via has_file
        let mut file_total_from_commit = 0;
        // First find the commit id for this repo
        let commit_query = r#"SELECT id FROM commits WHERE ->in_repo->repo:fixture_example;"#;
        if let Ok(mut commit_resp) = conn.query(commit_query).await {
            if let Ok(commit_rows) = commit_resp.take::<Vec<serde_json::Value>>(0) {
                eprintln!(
                    "DEBUG: Found {} commits for fixture_example",
                    commit_rows.len()
                );
                if let Some(first_commit) = commit_rows.first() {
                    eprintln!("DEBUG: First commit: {:?}", first_commit);
                    if let Some(commit_id_val) = first_commit.get("id") {
                        if let Some(commit_id_str) = commit_id_val.as_str() {
                            eprintln!("DEBUG: Commit ID string: {}", commit_id_str);
                            let has_file_query = format!(
                                r#"SELECT count() AS c FROM has_file WHERE in = commits:{};"#,
                                commit_id_str
                            );
                            eprintln!("DEBUG: has_file query: {}", has_file_query);
                            file_total_from_commit = run_count(&conn, &has_file_query).await;
                            eprintln!("DEBUG: file_total_from_commit: {}", file_total_from_commit);

                            // Debug: check all has_file relations
                            let all_has_file_query =
                                "SELECT in, out FROM has_file LIMIT 10;".to_string();
                            if let Ok(mut resp) = conn.query(&all_has_file_query).await {
                                if let Ok(rows) = resp.take::<Vec<serde_json::Value>>(0) {
                                    eprintln!("DEBUG: Sample has_file relations: {:?}", rows);
                                }
                            }

                            // Debug: check has_file relations with any commit
                            let any_commit_has_file_query =
                                "SELECT count() AS c FROM has_file WHERE in =~ 'commits:';"
                                    .to_string();
                            let any_count = run_count(&conn, &any_commit_has_file_query).await;
                            eprintln!("DEBUG: has_file relations with commits: {}", any_count);
                        }
                    }
                }
            }
        }

        assert!(
            file_total_from_commit > 1,
            "expected >1 files related to commit, got {}",
            file_total_from_commit
        ); // Fetch distinct edge pairs from both directions and compare normalized sets.
           // has_file: in=repo, out=file. in_repo: in=file, out=repo. Normalize to (repo_id,file_id).
        fn extract_pairs(
            rows: &[serde_json::Value],
            in_is_repo: bool,
        ) -> std::collections::HashSet<(String, String)> {
            let mut set = std::collections::HashSet::new();
            for row in rows {
                if let (Some(i), Some(o)) = (row.get("in"), row.get("out")) {
                    if let (Some(is), Some(os)) = (i.as_str(), o.as_str()) {
                        let (repo_id, file_id) = if in_is_repo {
                            (is.to_string(), os.to_string())
                        } else {
                            (os.to_string(), is.to_string())
                        };
                        // No need to scope since query already filters to repo
                        set.insert((repo_id, file_id));
                    }
                }
            }
            set
        }

        let mut has_file_rows_resp = conn
            .query(r#"SELECT in, out FROM has_file WHERE in->in_repo->repo:fixture_example;"#)
            .await
            .expect("has_file pairs");
        let has_file_rows: Vec<serde_json::Value> = has_file_rows_resp.take(0).unwrap_or_default();
        let mut in_repo_rows_resp = conn
            .query(r#"SELECT in, out FROM in_repo WHERE out->in_repo->repo:fixture_example;"#)
            .await
            .expect("in_repo pairs");
        let in_repo_rows: Vec<serde_json::Value> = in_repo_rows_resp.take(0).unwrap_or_default();
        let set_has = extract_pairs(&has_file_rows, true);
        let set_in = extract_pairs(&in_repo_rows, false);

        if set_has != set_in {
            let only_has: Vec<_> = set_has.difference(&set_in).cloned().collect();
            let only_in: Vec<_> = set_in.difference(&set_has).cloned().collect();
            panic!(
                "repo<->file edge pair asymmetry: only_in_has={:?} only_in_in_repo={:?}",
                only_has, only_in
            );
        }
    }

    // Stop the progress logger now that the test finished.
    progress_handle.abort();
}
