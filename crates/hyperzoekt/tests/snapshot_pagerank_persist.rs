// Focused integration test: ensure the DB writer persists page_rank_value into entity_snapshot

use hyperzoekt::db::connection::connect;
use hyperzoekt::db::{spawn_db_writer, DbWriterConfig};
use hyperzoekt::repo_index::indexer::payload::EntityPayload;
use std::sync::mpsc::{channel, Receiver};
use std::thread;
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_writer_persists_snapshot_page_rank_value() -> Result<(), Box<dyn std::error::Error>> {
    // Ensure tests use the embedded in-memory SurrealDB (don't pick up env overrides)
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");

    // Build a minimal payload with rank set
    let payload = EntityPayload {
        id: "".to_string(),
        language: "rust".to_string(),
        kind: "function".to_string(),
        name: "pagerank_test_fn".to_string(),
        parent: None,
        signature: "fn pagerank_test()".to_string(),
        start_line: None,
        end_line: None,
        doc: None,
        rank: Some(0.4242f32),
        imports: vec![],
        unresolved_imports: vec![],
        methods: vec![],
        stable_id: "pr-test-1".to_string(),
        repo_name: "pr-repo".to_string(),
        file: Some("pr-repo/src/lib.rs".to_string()),
        source_url: None,
        source_display: None,
        calls: vec![],
        source_content: None,
    };

    // Spawn a local SurrealDB and run the writer in initial_batch mode by calling spawn_db_writer
    let mut cfg = DbWriterConfig::default();
    cfg.surreal_ns = "test".into();
    cfg.surreal_db = "test".into();
    cfg.initial_batch = true;
    cfg.repo_name = "pr-repo".to_string();

    // We pass the payloads to the writer by sending them on the returned channel;
    // the writer loop reads from this channel and will process the batch.
    let (tx, handle) = spawn_db_writer(vec![], cfg.clone(), None)?;

    // Send the single-payload initial batch and then drop the sender so the
    // writer sees a disconnected channel and exits after processing.
    tx.send(vec![payload.clone()])?;
    drop(tx);

    // Create a channel so the background join thread can report the writer's
    // result back to the test. This lets the test fail fast if the writer
    // returns an error or panics, instead of silently timing out.
    let (tx_res, rx_res): (
        std::sync::mpsc::Sender<Result<(), String>>,
        Receiver<Result<(), String>>,
    ) = channel();
    thread::spawn(move || {
        let send_err = |s: String| {
            let _ = tx_res.send(Err(s));
        };
        match handle.join() {
            Ok(res) => match res {
                Ok(_) => {
                    let _ = tx_res.send(Ok(()));
                }
                Err(e) => {
                    send_err(format!("writer returned Err: {}", e));
                }
            },
            Err(e) => {
                // Thread panicked; capture a debug string
                send_err(format!("writer panicked: {:?}", e));
            }
        }
    });

    // Poll the ephemeral SurrealDB for the entity snapshot we expect, printing
    // progress periodically. This avoids a single long blocking join and gives
    // useful diagnostics when the writer appears stalled.
    let timeout_ms: u128 = std::env::var("HZ_TEST_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse::<u128>().ok())
        .unwrap_or(10_000u128);
    let poll_interval = Duration::from_millis(200);
    let start = Instant::now();
    let mut last_rows: Option<Vec<serde_json::Value>> = None;
    let mut writer_done: bool = false;
    let mut writer_done_at: Option<Instant> = None;
    // Grace period (ms) after writer reports completion to allow final DB writes to appear
    let grace_ms: u128 = 5000;
    loop {
        // Check if the writer thread reported an error or success
        if let Ok(res) = rx_res.try_recv() {
            match res {
                Ok(_) => {
                    writer_done = true;
                    writer_done_at = Some(Instant::now());
                    eprintln!(
                        "writer reported completion; waiting up to {}ms for finalization",
                        grace_ms
                    );
                }
                Err(s) => {
                    panic!("writer reported error: {}", s);
                }
            }
        }

        // If writer signaled completion and grace window expired, fail fast
        if writer_done {
            if let Some(t) = writer_done_at {
                if t.elapsed().as_millis() > grace_ms {
                    // Run a couple of final diagnostic queries to include in the panic
                    // so we can inspect what the DB contains when the test fails.
                    let conn_diag = connect(
                        &None,
                        &None,
                        &None,
                        &cfg.surreal_ns.clone(),
                        &cfg.surreal_db.clone(),
                    )
                    .await
                    .ok();
                    let mut diag_snapshot: Option<Vec<serde_json::Value>> = None;
                    let sanitized_eid = payload.stable_id.replace('-', "_");
                    if let Some(conn) = conn_diag {
                        if let Ok(mut r) = conn
                            .query(&format!(
                                "SELECT * FROM entity_snapshot WHERE stable_id = '{}' LIMIT 1;",
                                payload.stable_id
                            ))
                            .await
                        {
                            diag_snapshot = r.take(0).ok();
                        }
                        // Also attempt direct lookup by deterministic Thing id
                        if let Ok(mut r2) = conn
                            .query(&format!(
                                "SELECT * FROM entity_snapshot:{id};",
                                id = sanitized_eid
                            ))
                            .await
                        {
                            let _ = r2.take::<Vec<serde_json::Value>>(0).ok();
                        }
                    }
                    panic!("writer completed but snapshot did not appear within {}ms; last_rows={:?} diag_snapshot={:?}", grace_ms, last_rows, diag_snapshot);
                }
            }
        }

        if start.elapsed().as_millis() > timeout_ms {
            panic!(
                "timeout waiting for writer to persist snapshot; last_rows={:?}",
                last_rows
            );
        }
        // Connect to the same ephemeral SurrealDB used by the writer and query the entity row's snapshot
        let conn = connect(
            &None,
            &None,
            &None,
            &cfg.surreal_ns.clone(),
            &cfg.surreal_db.clone(),
        )
        .await?;
        // First try the top-level entity's embedded snapshot field, then fall back to entity_snapshot table.
        let q_entity = format!("SELECT snapshot.page_rank_value AS page_rank_value FROM entity WHERE stable_id = '{}' LIMIT 1;", payload.stable_id);
        match conn.query(&q_entity).await {
            Ok(mut resp) => {
                let rows: Vec<serde_json::Value> = resp.take(0)?;
                if !rows.is_empty() {
                    if let Some(val) = rows[0].get("page_rank_value") {
                        if !val.is_null() {
                            let got = val.as_f64().unwrap();
                            assert!(
                                (got - 0.4242).abs() < 1e-6,
                                "page_rank_value mismatch (entity): {}",
                                got
                            );
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("query error while polling entity snapshot: {}", e);
            }
        }
        // Fallback to entity_snapshot table
        let q_snapshot = format!("SELECT page_rank_value AS page_rank_value FROM entity_snapshot WHERE stable_id = '{}' LIMIT 1;", payload.stable_id);
        match conn.query(&q_snapshot).await {
            Ok(mut resp) => {
                let rows: Vec<serde_json::Value> = resp.take(0)?;
                if !rows.is_empty() {
                    last_rows = Some(rows.clone());
                    if let Some(val) = rows[0].get("page_rank_value") {
                        if !val.is_null() {
                            let got = val.as_f64().unwrap();
                            assert!(
                                (got - 0.4242).abs() < 1e-6,
                                "page_rank_value mismatch (entity_snapshot): {}",
                                got
                            );
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("query error while polling for snapshot: {}", e);
            }
        }
        // Print periodic progress every second
        if start.elapsed().as_millis() % 1000 < poll_interval.as_millis() {
            eprintln!(
                "waiting for snapshot persist... elapsed={}ms",
                start.elapsed().as_millis()
            );
        }
        tokio::time::sleep(poll_interval).await;
    }

    Ok(())
}
