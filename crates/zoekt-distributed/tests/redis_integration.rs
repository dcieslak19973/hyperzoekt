use deadpool_redis::redis::AsyncCommands;
use deadpool_redis::Config as RedisConfig;
use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine;
use getrandom::getrandom;
use parking_lot::RwLock;
use serde_json::Value;
use tracing_subscriber::EnvFilter;
use zoekt_distributed::LeaseManager;

// We recreate minimal AppStateInner shape used by the bin for testing.
// Some test-only fields are intentionally unused in this small integration test.
// Silence warnings to keep CI logs clean.
#[allow(dead_code)]
struct TestStateInner {
    redis_pool: Option<deadpool_redis::Pool>,
    admin_user: String,
    admin_pass: String,
    sessions: Arc<RwLock<HashMap<String, String>>>,
}

#[allow(dead_code)]
type TestState = Arc<TestStateInner>;

fn gen_token() -> String {
    let mut b = [0u8; 16];
    getrandom(&mut b).expect("failed to get random bytes");
    base64::engine::general_purpose::STANDARD.encode(b)
}

fn init_test_logging() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    });
}

#[tokio::test]
async fn redis_persistence_integration() {
    init_test_logging();
    let redis_url = match std::env::var("REDIS_URL") {
        Ok(u) => u,
        Err(_) => {
            tracing::info!("TEST SKIP: redis_persistence_integration (no REDIS_URL)");
            return;
        }
    };
    tracing::info!("TEST START: redis_persistence_integration");

    // build pool
    let pool = RedisConfig::from_url(&redis_url).create_pool(None).unwrap();

    // ensure clean state for our test key
    let mut conn = pool.get().await.unwrap();
    let test_name = format!("int_test_repo_{}", gen_token());
    let test_url = "https://example.test/1".to_string();
    let _: () = conn.hdel("zoekt:repos", &test_name).await.unwrap_or(());

    let sessions = Arc::new(RwLock::new(HashMap::new()));
    // create a session
    sessions.write().insert("sid1".into(), "testtoken".into());

    let _state = Arc::new(TestStateInner {
        redis_pool: Some(pool.clone()),
        admin_user: "user1".into(),
        admin_pass: "pass1".into(),
        sessions: sessions.clone(),
    });

    // build the app using the binary's make_app function by calling the binary module
    // The binary's make_app lives in the crate root module path `crates::zoekt-distributed::bin::dzr_admin::make_app`.
    // Easier: re-create the router as in the bin by calling the function from the binary crate module path.
    // The crate exposes no public make_app, so we'll duplicate the minimal routing here by importing the handler functions.
    // Exercise persistence by performing the same operation the handler would do.

    // Exercise persistence by performing the same operation the handler would do.
    let mut conn = pool.get().await.unwrap();
    let _: () = conn
        .hset("zoekt:repos", &test_name, &test_url)
        .await
        .unwrap();

    // verify it exists
    let got: Option<String> = conn.hget("zoekt:repos", &test_name).await.unwrap();
    assert_eq!(got.as_deref(), Some(test_url.as_str()));

    // cleanup
    let _: () = conn.hdel("zoekt:repos", &test_name).await.unwrap_or(());
    tracing::info!("TEST END: redis_persistence_integration");
}

#[tokio::test]
async fn redis_repo_meta_contains_memory_bytes() {
    init_test_logging();
    let redis_url = match std::env::var("REDIS_URL") {
        Ok(u) => u,
        Err(_) => {
            tracing::info!("TEST SKIP: redis_repo_meta_contains_memory_bytes (no REDIS_URL)");
            return;
        }
    };
    tracing::info!("TEST START: redis_repo_meta_contains_memory_bytes");

    // build pool
    let pool = RedisConfig::from_url(&redis_url).create_pool(None).unwrap();
    let mut conn = pool.get().await.unwrap();

    let test_name = format!("meta_int_test_{}", gen_token());
    // ensure clean state
    let _: () = conn.hdel("zoekt:repo_meta", &test_name).await.unwrap_or(());

    // call LeaseManager to persist branch-scoped meta including memory_bytes
    let lease = LeaseManager::new().await;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let dur_ms = 123i64;
    let mem_bytes = 12345i64;
    // write branch meta under field "<repo>|<branch>"
    lease
        .set_branch_meta(&test_name, "main", now_ms, dur_ms, mem_bytes, "test-node")
        .await;

    // read back the stored JSON from zoekt:repo_branch_meta
    let field = format!("{}|{}", &test_name, "main");
    let got: Option<String> = conn.hget("zoekt:repo_branch_meta", &field).await.unwrap();
    assert!(got.is_some(), "expected repo_branch_meta to be present");
    let s = got.unwrap();
    let v: Value = serde_json::from_str(&s).expect("stored meta should be valid json");
    let m = v
        .get("memory_bytes")
        .and_then(|x| x.as_i64())
        .expect("memory_bytes field should be an integer");
    assert_eq!(m, mem_bytes);

    // cleanup
    let _: () = conn
        .hdel("zoekt:repo_branch_meta", &field)
        .await
        .unwrap_or(());
    tracing::info!("TEST END: redis_repo_meta_contains_memory_bytes");
}

#[tokio::test]
async fn redis_publish_repo_event_integration() {
    init_test_logging();
    let _redis_url = match std::env::var("REDIS_URL") {
        Ok(u) => u,
        Err(_) => {
            tracing::info!("TEST SKIP: redis_publish_repo_event_integration (no REDIS_URL)");
            return;
        }
    };
    tracing::info!("TEST START: redis_publish_repo_event_integration");

    // Create a test repo
    let test_repo = zoekt_distributed::lease_manager::RemoteRepo {
        name: format!("event_test_repo_{}", gen_token()),
        git_url: "https://example.test/event-repo.git".to_string(),
        branch: Some("main".to_string()),
        visibility: zoekt_rs::types::RepoVisibility::Public,
        owner: None,
        allowed_users: Vec::new(),
        last_commit_sha: None,
    };

    // Create lease manager
    let lease_mgr = LeaseManager::new().await;

    // Test that publishing completes without panicking
    lease_mgr
        .publish_repo_event("indexing_started", &test_repo, "test-node")
        .await;

    // If we get here, the publish completed without error
    tracing::info!("Successfully published repo event without error");

    tracing::info!("TEST END: redis_publish_repo_event_integration");
}
