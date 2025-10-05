use anyhow::Result;
use base64::Engine;
use deadpool_redis::redis::AsyncCommands;
use serde_json::json;
use std::env;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

// This test is an integration-style check that requires a running Redis and uses
// SurrealDB in-memory for verification. It spawns the `hyperzoekt-sbom-worker`
// binary and enqueues a job; the worker should consume it and write to SurrealDB.

#[tokio::test]
async fn integration_sbom_worker_roundtrip() -> Result<()> {
    // Ensure test uses an in-memory SurrealDB by not setting SURREALDB_URL.
    // Use default values for queue and TTL to match the worker defaults.

    // Gate long-running test behind an env var so CI can skip it by default.
    // Set `LONG_RUNNING_INTEGRATION_TESTS=1` to run (matches other slow tests).
    if env::var("LONG_RUNNING_INTEGRATION_TESTS").is_err() {
        eprintln!("Skipping sbom integration test: set LONG_RUNNING_INTEGRATION_TESTS=1 to run");
        return Ok(());
    }

    // Configurable URLs for Redis and Surreal
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    // Prefer explicit SURREALDB_URL; allow a local developer fallback only when ALLOW_LOCAL_SURREAL=1
    let surreal_url = match env::var("SURREALDB_URL") {
        Ok(u) => u,
        Err(_) => {
            if env::var("ALLOW_LOCAL_SURREAL").ok().as_deref() == Some("1") {
                "http://127.0.0.1:8000".to_string()
            } else {
                eprintln!("Skipping sbom integration test: SURREALDB_URL not set and ALLOW_LOCAL_SURREAL!=1");
                return Ok(());
            }
        }
    };

    // Parse Surreal URL to get host:port for client
    // The test anticipates a host:port like 127.0.0.1:8000 for the remote HTTP client
    let (_, no_scheme) = hyperzoekt::test_utils::normalize_surreal_host(&surreal_url);
    let client_target = no_scheme;

    // Set REDIS_URL for the test process to match worker
    env::set_var("REDIS_URL", &redis_url);

    // Spawn a periodic progress logger that prints every 1 second while the
    // test is running to reassure runners that the test is alive.
    let progress_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            eprintln!("integration_sbom_worker_roundtrip: still running...");
        }
    });
    // Prepend our test `cdxgen` shim so the worker uses a hermetic, deterministic
    // CycloneDX JSON producer. This avoids requiring the real `cdxgen` binary
    // and keeps the integration test lightweight.
    let mut new_path = env::var("PATH").unwrap_or_default();
    let shim_dir = format!("{}/crates/hyperzoekt/tests/bin", env!("CARGO_MANIFEST_DIR"));
    new_path = format!("{}:{}", shim_dir, new_path);

    // Create a minimal local git repo to scan so cloning succeeds.
    let repo_tmp = tempfile::tempdir()?;
    let repo_path = repo_tmp.path().join("repo");
    std::fs::create_dir_all(&repo_path)?;
    // create files for the repo, init repo, add file, commit
    std::fs::write(repo_path.join("README.md"), "hello world")?;
    // Create a minimal Rust project so the worker exercises the cargo path.
    std::fs::write(
        repo_path.join("Cargo.toml"),
        r#"[package]
name = "test_repo"
version = "0.1.0"
edition = "2021"
"#,
    )?;
    std::fs::create_dir_all(repo_path.join("src"))?;
    std::fs::write(
        repo_path.join("src").join("lib.rs"),
        r#"pub fn hello() -> &'static str { "hello" }"#,
    )?;
    // Generate a Cargo.lock if cargo is available; ignore errors.
    let _ = Command::new("cargo")
        .arg("generate-lockfile")
        .current_dir(&repo_path)
        .status();
    // init repo, add file, commit
    let repo = git2::Repository::init(&repo_path)?;
    // Ensure local git config has user info so commits succeed in CI/dev
    {
        let mut cfg = repo.config()?;
        let _ = cfg.set_str("user.name", "Test User");
        let _ = cfg.set_str("user.email", "test@example.com");
    }
    // Create a commit so we have a commit SHA to pass to the worker.
    let mut index = repo.index()?;
    index.add_path(std::path::Path::new("README.md"))?;
    index.write()?;
    let oid = index.write_tree()?;
    let signature = repo.signature()?;
    let tree = repo.find_tree(oid)?;
    let commit_oid = repo.commit(Some("HEAD"), &signature, &signature, "initial", &tree, &[])?;
    let commit_str = commit_oid.to_string();

    // Enqueue a simple job into Redis. This test expects a Redis server
    // running at REDIS_URL or localhost:6379. If not present, the test will fail.
    let client = match zoekt_distributed::redis_adapter::create_redis_pool() {
        Some(p) => p,
        None => {
            eprintln!("Skipping integration test: Redis not configured");
            return Ok(());
        }
    };
    let mut conn = match client.get().await {
        Ok(c) => c,
        Err(_) => {
            eprintln!("Skipping integration test: Redis connection failed");
            return Ok(());
        }
    };
    // Diagnostic: ping Redis to confirm connectivity
    let ping_result: String = conn.ping().await?;
    eprintln!("integration test: Redis PING result: {}", ping_result);
    // Diagnostic: test basic SET/GET
    let _: () = conn.set("test_key", "test_value").await?;
    let get_result: Option<String> = conn.get("test_key").await?;
    eprintln!("integration test: Redis SET/GET test: {:?}", get_result);

    // Enqueue a simple job into Redis so the worker will process it.
    let job = json!({
        "repo": "test/repo",
        "git_url": repo_path.to_str().unwrap(),
        "commit": commit_str
    });
    let queue: String = env::var("HZ_SBOM_JOBS_QUEUE").unwrap_or_else(|_| "sbom_jobs".into());
    let res: Result<usize, _> = conn.lpush(queue.clone(), job.to_string()).await;
    if let Err(e) = res {
        eprintln!("integration test: LPUSH failed: {}", e);
    }
    // Diagnostic: confirm job was enqueued
    let queue_len: i64 = conn.llen(&queue).await.unwrap_or(-1);
    eprintln!(
        "integration test: enqueued job to queue '{}', length now: {}",
        queue, queue_len
    );

    // Spawn the worker binary as a child process. Wire it to the user's
    // running Redis and SurrealDB instances (ports 7777 and 8000 respectively).
    let mut child = Command::new(env!("CARGO_BIN_EXE_hyperzoekt-sbom-worker"))
        .env("PATH", &new_path)
        .env("RUST_LOG", "info")
        // Use our static fixture if the real tool emits nothing.
        .env(
            "HZ_SBOM_STATIC_BOM_PATH",
            format!(
                "{}/tests/fixtures/fixture_bom.json",
                env!("CARGO_MANIFEST_DIR")
            ),
        )
        .env("HZ_SBOM_WORKER_PORT", "8084")
        .env("HZ_SBOM_TOOL", "cdxgen")
        .env("HZ_SBOM_MANIFEST_ONLY", "1")
        .env("REDIS_URL", &redis_url)
        .env("SURREALDB_URL", &surreal_url)
        .env("SURREALDB_USERNAME", "root")
        .env("SURREALDB_PASSWORD", "root")
        .env("SURREAL_DB", "repos")
        .env("SURREAL_NS", "zoekt")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .spawn()?;

    // Stream worker stderr so we can observe SBOM processing / Surreal activity.
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(async move {
            use tokio::io::{AsyncBufReadExt, BufReader};
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("[worker] {}", line);
            }
        });
    }

    // Enqueue a simple job into Redis. This test expects a Redis server
    // running at REDIS_URL or localhost:6379. If not present, the test will fail.
    let client = match zoekt_distributed::redis_adapter::create_redis_pool() {
        Some(p) => p,
        None => {
            eprintln!("Skipping integration test: Redis not configured");
            let _ = child.kill();
            return Ok(());
        }
    };
    let mut conn = match client.get().await {
        Ok(c) => c,
        Err(_) => {
            eprintln!("Skipping integration test: Redis connection failed");
            let _ = child.kill();
            return Ok(());
        }
    };
    // Diagnostic: ping Redis to confirm connectivity
    let ping_result: String = conn.ping().await?;
    eprintln!("integration test: Redis PING result: {}", ping_result);
    // Diagnostic: test basic SET/GET
    let _: () = conn.set("test_key", "test_value").await?;
    let get_result: Option<String> = conn.get("test_key").await?;
    eprintln!("integration test: Redis SET/GET test: {:?}", get_result);

    // Give the worker time to start and begin listening
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Enqueue a simple job into Redis so the worker will process it.
    let job = json!({
        "repo": "test/repo",
        "git_url": repo_path.to_str().unwrap(),
        "commit": commit_str
    });
    let queue: String = env::var("HZ_SBOM_JOBS_QUEUE").unwrap_or_else(|_| "sbom_jobs".into());
    let res: Result<usize, _> = conn.lpush(queue.clone(), job.to_string()).await;
    if let Err(e) = res {
        eprintln!("integration test: LPUSH failed: {}", e);
    }
    // Diagnostic: confirm job was enqueued
    let queue_len: i64 = conn.llen(&queue).await.unwrap_or(-1);
    eprintln!(
        "integration test: enqueued job to queue '{}', length now: {}",
        queue, queue_len
    );
    use surrealdb::engine::remote::http::Http;
    // Tests in `tests/` are compiled as separate crates and cannot import
    // internal `crate::` modules directly. Provide a small local helper that
    // mirrors the `response_to_json` logic but only extracts the
    // `sbom_blob` string when present. Prefers sql::Value shapes to avoid
    // double-encoded JSON strings.
    // fn extract_sbom_blob_from_response(mut resp: surrealdb::Response) -> Option<String> {
    //     use surrealdb::sql::Value as SqlValue;
    //     // Probe sql::Value shapes first (nested Vec variants)
    //     for slot in 0usize..10usize {
    //         if let Ok(nested) = resp.take::<Vec<Vec<SqlValue>>>(slot) {
    //             for inner in nested.into_iter() {
    //                 for v in inner.into_iter() {
    //                     if let SqlValue::Object(obj) = v {
    //                         if let Some(sbv) = obj.get("sbom_blob") {
    //                             match sbv {
    //                                 SqlValue::Strand(s) => return Some(s.to_string()),
    //                                 SqlValue::Thing(t) => return Some(t.to_string()),
    //                                 _ => return Some(sbv.to_string()),
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         if let Ok(opt_nested) = resp.take::<Option<Vec<Vec<SqlValue>>>>(slot) {
    //             if let Some(nv) = opt_nested {
    //                 for inner in nv.into_iter() {
    //                     for v in inner.into_iter() {
    //                         if let SqlValue::Object(obj) = v {
    //                             if let Some(sbv) = obj.get("sbom_blob") {
    //                                 match sbv {
    //                                     SqlValue::Strand(s) => return Some(s.to_string()),
    //                                     SqlValue::Thing(t) => return Some(t.to_string()),
    //                                     _ => return Some(sbv.to_string()),
    //                                 }
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         if let Ok(rows) = resp.take::<Vec<SqlValue>>(slot) {
    //             for v in rows.into_iter() {
    //                 if let SqlValue::Object(obj) = v {
    //                     if let Some(sbv) = obj.get("sbom_blob") {
    //                         match sbv {
    //                             SqlValue::Strand(s) => return Some(s.to_string()),
    //                             SqlValue::Thing(t) => return Some(t.to_string()),
    //                             _ => return Some(sbv.to_string()),
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         if let Ok(opt_rows) = resp.take::<Option<Vec<SqlValue>>>(slot) {
    //             if let Some(rows) = opt_rows {
    //                 for v in rows.into_iter() {
    //                     if let SqlValue::Object(obj) = v {
    //                         if let Some(sbv) = obj.get("sbom_blob") {
    //                             match sbv {
    //                                 SqlValue::Strand(s) => return Some(s.to_string()),
    //                                 SqlValue::Thing(t) => return Some(t.to_string()),
    //                                 _ => return Some(sbv.to_string()),
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         // Option<SqlValue>
    //         if let Ok(opt_val) = resp.take::<Option<SqlValue>>(slot) {
    //             if let Some(val) = opt_val {
    //                 if let SqlValue::Object(obj) = val {
    //                     if let Some(sbv) = obj.get("sbom_blob") {
    //                         match sbv {
    //                             SqlValue::Strand(s) => return Some(s.to_string()),
    //                             SqlValue::Thing(t) => return Some(t.to_string()),
    //                             _ => return Some(sbv.to_string()),
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //
    //     // Fallback: try serde_json shapes and extract string if present
    //     for slot in 0usize..10usize {
    //         if let Ok(rows) = resp.take::<Vec<serde_json::Value>>(slot) {
    //             for row in rows.into_iter() {
    //                 if let Some(obj) = row.as_object() {
    //                     if let Some(v) = obj.get("sbom_blob") {
    //                         if v.is_string() {
    //                             return Some(v.as_str().unwrap().to_string());
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         if let Ok(opt_row) = resp.take::<Option<serde_json::Value>>(slot) {
    //             if let Some(v) = opt_row {
    //                 if let Some(obj) = v.as_object() {
    //                     if let Some(sv) = obj.get("sbom_blob") {
    //                         if sv.is_string() {
    //                             return Some(sv.as_str().unwrap().to_string());
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //     None
    // }
    // Use the library's exported `response_to_json` helper which handles the
    // many remote and embedded client shapes without destructively taking
    // typed slots from the `Response`.
    use hyperzoekt::db::response_to_json;

    eprintln!(
        "integration test: SURREALDB_URL={:?} SURREALDB_HTTP_BASE={:?} SURREALDB_USERNAME={:?} SURREAL_NS={:?} SURREAL_DB={:?}",
        std::env::var("SURREALDB_URL").ok(),
        std::env::var("SURREALDB_HTTP_BASE").ok(),
        std::env::var("SURREALDB_USERNAME").ok(),
        std::env::var("SURREAL_NS").ok(),
        std::env::var("SURREAL_DB").ok(),
    );
    eprintln!("integration test: client_target = {}", client_target);
    let db_client = surrealdb::Surreal::new::<Http>(client_target.as_str()).await?;
    db_client
        .signin(surrealdb::opt::auth::Root {
            username: "root",
            password: "root",
        })
        .await?;
    db_client.use_ns("zoekt").await?;
    db_client.use_db("repos").await?;

    let q = format!(
        "USE NS zoekt DB repos; SELECT sbom_blob FROM sboms WHERE repo = '{}' AND commit = '{}' LIMIT 1",
        "test/repo", commit_str
    );

    let mut found = false;
    let mut found_sbom_blob: Option<String> = None;
    let start_time = std::time::Instant::now();
    let timeout_duration = Duration::from_secs(120); // 2 minutes timeout

    for attempt in 0..30 {
        let elapsed = start_time.elapsed();
        if elapsed > timeout_duration {
            eprintln!(
                "integration test: TIMEOUT after {} seconds - giving up",
                elapsed.as_secs()
            );
            break;
        }

        eprintln!(
            "integration test: query attempt {}/30 (elapsed: {:.1}s, timeout: {}s)",
            attempt + 1,
            elapsed.as_secs_f32(),
            timeout_duration.as_secs()
        );

        // Query once and normalize the Response using the shared
        // `response_to_json` helper (non-destructive). If we get rows, try
        // to extract the `sbom_blob` string field from the normalized JSON.
        let res = db_client.query(q.clone()).await?;
        if let Some(j) = response_to_json(res) {
            // Candidate shapes:
            // - Array of row objects: [{"sbom_blob": "..."}]
            // - Array of values: ["{...}"] when selecting a single column
            let rows = if j.is_array() {
                j.as_array().unwrap().clone()
            } else {
                vec![j]
            };
            'outer: for row in rows.into_iter() {
                if row.is_string() {
                    let s = row.as_str().unwrap().to_string();
                    if let Ok(decoded) =
                        base64::engine::general_purpose::STANDARD.decode(s.as_bytes())
                    {
                        if let Ok(decoded_str) = String::from_utf8(decoded) {
                            if decoded_str.contains("bomFormat") {
                                found = true;
                                found_sbom_blob = Some(s);
                                break 'outer;
                            }
                        }
                    }
                }
                if let Some(obj) = row.as_object() {
                    if let Some(v) = obj.get("sbom_blob") {
                        if v.is_string() {
                            let s = v.as_str().unwrap().to_string();
                            if let Ok(decoded) =
                                base64::engine::general_purpose::STANDARD.decode(s.as_bytes())
                            {
                                if let Ok(decoded_str) = String::from_utf8(decoded) {
                                    if decoded_str.contains("bomFormat") {
                                        found = true;
                                        found_sbom_blob = Some(s);
                                        break 'outer;
                                    }
                                }
                            }
                        } else {
                            // Fallback: stringify non-string values.
                            let s = v.to_string();
                            if let Ok(decoded) =
                                base64::engine::general_purpose::STANDARD.decode(s.as_bytes())
                            {
                                if let Ok(decoded_str) = String::from_utf8(decoded) {
                                    if decoded_str.contains("bomFormat") {
                                        found = true;
                                        found_sbom_blob = Some(s);
                                        break 'outer;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if found {
                eprintln!(
                    "integration test: SUCCESS - found sboms row after {} attempts ({:.1}s)",
                    attempt + 1,
                    elapsed.as_secs_f32()
                );
                break;
            }
        }

        // Diagnostic: re-run query to get a fresh Response to inspect and
        // print typed takes so we can see why the probe failed.
        eprintln!("integration test: query returned no rows; dumping diagnostics");
        eprintln!("integration test: query = {}", q.clone());
        eprintln!("integration test: client_target = {}", client_target);
        eprintln!("integration test env: SURREALDB_URL={:?} SURREALDB_HTTP_BASE={:?} SURREALDB_USERNAME={:?} SURREAL_NS={:?} SURREAL_DB={:?}",
            std::env::var("SURREALDB_URL").ok(),
            std::env::var("SURREALDB_HTTP_BASE").ok(),
            std::env::var("SURREALDB_USERNAME").ok(),
            std::env::var("SURREAL_NS").ok(),
            std::env::var("SURREAL_DB").ok(),
        );
        // For diagnostics we want to check the fresh response for rows first
        // before doing any typed `take` calls which consume slots. If the
        // fresh response contains data we'll stop; otherwise re-query and
        // perform typed takes to print helpful diagnostics about why the
        // probe failed.
        let res2 = db_client.query(q.clone()).await?;
        eprintln!("integration test: raw Response debug = {:?}", res2);

        // First try to extract the sbom_blob directly from this fresh
        // diagnostic Response — this handles the case where the initial
        // probe raced and returned empty but a subsequent response has the
        // actual row. `response_to_json` returns `Option<serde_json::Value>`
        // so convert to a string field like in the main probe above.
        if let Some(j) = response_to_json(res2) {
            let rows = if j.is_array() {
                j.as_array().unwrap().clone()
            } else {
                vec![j]
            };
            'diag_outer: for row in rows.into_iter() {
                if row.is_string() {
                    found = true;
                    found_sbom_blob = Some(row.as_str().unwrap().to_string());
                    break 'diag_outer;
                }
                if let Some(obj) = row.as_object() {
                    if let Some(v) = obj.get("sbom_blob") {
                        if v.is_string() {
                            found = true;
                            found_sbom_blob = Some(v.as_str().unwrap().to_string());
                            break 'diag_outer;
                        } else {
                            found = true;
                            found_sbom_blob = Some(v.to_string());
                            break 'diag_outer;
                        }
                    }
                }
            }
            if found {
                eprintln!("integration test: SUCCESS - found sboms row in diagnostics after {} attempts ({:.1}s)", 
                         attempt + 1, elapsed.as_secs_f32());
                break;
            }
        }

        // The fresh response didn't contain rows; re-run the query and
        // perform typed `take` diagnostics on this new Response so we can
        // see what shapes the client returned.
        let mut res3 = db_client.query(q.clone()).await?;
        // Try typed takes and print outcomes (prefer Option<serde_json::Value>
        // because we selected `sbom_blob` which may be returned as a single
        // JSON/string value in an Option wrapper).
        if let Ok(opt) = res3.take::<Option<serde_json::Value>>(0) {
            eprintln!(
                "integration test: take::<Option<serde_json::Value>>(0) = {:?}",
                opt
            );
        } else {
            eprintln!("integration test: take::<Option<serde_json::Value>>(0) failed");
        }
        if let Ok(rows) = res3.take::<Vec<serde_json::Value>>(0) {
            eprintln!(
                "integration test: take::<Vec<serde_json::Value>>(0) = {:?}",
                rows
            );
        } else {
            eprintln!("integration test: take::<Vec<serde_json::Value>>(0) failed");
        }
        if let Ok(optrows) = res3.take::<Option<Vec<surrealdb::sql::Value>>>(0) {
            eprintln!(
                "integration test: take::<Option<Vec<sql::Value>>>(0) = {:?}",
                optrows
            );
        } else {
            eprintln!("integration test: take::<Option<Vec<sql::Value>>>(0) failed");
        }
        if let Ok(rows) = res3.take::<Vec<surrealdb::sql::Value>>(0) {
            eprintln!("integration test: take::<Vec<sql::Value>>(0) = {:?}", rows);
        } else {
            eprintln!("integration test: take::<Vec<sql::Value>>(0) failed");
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    // After probing for the sboms row, also verify sbom_deps contains at
    // least one dependency row for the created SBOM. This confirms we
    // persisted both the raw SBOM and its parsed dependency rows.
    let q_deps = "USE NS zoekt DB repos; SELECT count() FROM sbom_deps GROUP ALL".to_string();
    let mut deps_found = false;
    let deps_start_time = std::time::Instant::now();
    let deps_timeout_duration = Duration::from_secs(60); // 1 minute timeout for deps check

    for attempt in 0..10 {
        let elapsed = deps_start_time.elapsed();
        if elapsed > deps_timeout_duration {
            eprintln!(
                "integration test: DEPS TIMEOUT after {} seconds - giving up",
                elapsed.as_secs()
            );
            break;
        }

        eprintln!(
            "integration test: deps query attempt {}/10 (elapsed: {:.1}s, timeout: {}s)",
            attempt + 1,
            elapsed.as_secs_f32(),
            deps_timeout_duration.as_secs()
        );

        let res = db_client.query(q_deps.clone()).await?;
        if let Some(j) = response_to_json(res) {
            // Check if we got a count result
            if let Some(obj) = j.as_object() {
                if let Some(count_val) = obj.get("count") {
                    if let Some(count) = count_val.as_i64() {
                        if count > 0 {
                            deps_found = true;
                            eprintln!("integration test: SUCCESS - found {} sbom_deps rows after {} attempts ({:.1}s)", 
                                     count, attempt + 1, elapsed.as_secs_f32());
                            break;
                        }
                    }
                }
            }
            // Also check if it's an array with count
            if let Some(arr) = j.as_array() {
                if let Some(first) = arr.first() {
                    if let Some(obj) = first.as_object() {
                        if let Some(count_val) = obj.get("count") {
                            if let Some(count) = count_val.as_i64() {
                                if count > 0 {
                                    deps_found = true;
                                    eprintln!("integration test: SUCCESS - found {} sbom_deps rows after {} attempts ({:.1}s)", 
                                             count, attempt + 1, elapsed.as_secs_f32());
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // If we extracted an sbom blob, do a quick sanity check that it looks
    // like the CycloneDX payload we expect (contains `"bomFormat"`).
    if let Some(sbom_str) = found_sbom_blob {
        // Attempt base64 decode if it does not contain bomFormat directly.
        if !sbom_str.contains("bomFormat") {
            if let Ok(decoded) =
                base64::engine::general_purpose::STANDARD.decode(sbom_str.as_bytes())
            {
                if let Ok(s) = String::from_utf8(decoded) {
                    assert!(
                        s.contains("bomFormat"),
                        "decoded sbom_blob missing bomFormat"
                    );
                } else {
                    panic!("sbom_blob not valid utf8 after base64 decode");
                }
            } else {
                panic!("sbom_blob missing bomFormat and not valid base64");
            }
        }
    }

    let _ = child.kill();
    // Stop the progress logger now that the test finished.
    progress_handle.abort();
    assert!(found, "expected sboms row, got none");
    assert!(deps_found, "expected sbom_deps row (lodash), got none");
    Ok(())
}
