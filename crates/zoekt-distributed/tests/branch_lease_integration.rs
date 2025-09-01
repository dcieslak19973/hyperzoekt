use deadpool_redis::{redis::AsyncCommands, Config as RedisConfig};
use rand::RngCore;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use base64::Engine;
use tracing_subscriber::EnvFilter;
use zoekt_distributed::{LeaseManager, Node, NodeConfig, NodeType, RemoteRepo};

use zoekt_distributed::test_utils::FakeIndexer;

fn gen_token() -> String {
    let mut b = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut b);
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
async fn branch_level_leasing_integration() {
    init_test_logging();
    let redis_url = match std::env::var("REDIS_URL") {
        Ok(u) => u,
        Err(_) => {
            tracing::info!("TEST SKIP: branch_level_leasing_integration (no REDIS_URL)");
            return;
        }
    };

    let pool = RedisConfig::from_url(&redis_url).create_pool(None).unwrap();
    let mut conn = pool.get().await.unwrap();

    let repo_name = format!("int_branch_repo_{}", gen_token());
    let url = "https://example.test/repo.git".to_string();
    let field_main = format!("{}|{}", repo_name, "main");
    let field_dev = format!("{}|{}", repo_name, "dev");

    // cleanup any leftover
    let _: () = conn
        .hdel("zoekt:repo_branches", &field_main)
        .await
        .unwrap_or(());
    let _: () = conn
        .hdel("zoekt:repo_branches", &field_dev)
        .await
        .unwrap_or(());

    // write branch entries
    let _: () = conn
        .hset("zoekt:repo_branches", &field_main, &url)
        .await
        .unwrap();
    let _: () = conn
        .hset("zoekt:repo_branches", &field_dev, &url)
        .await
        .unwrap();

    // ensure LeaseManager will pick up REDIS_URL
    std::env::set_var("REDIS_URL", &redis_url);
    let lease = LeaseManager::new().await;

    let cfg = NodeConfig {
        id: "node-int".into(),
        lease_ttl: Duration::from_secs(2),
        poll_interval: Duration::from_millis(50),
        node_type: NodeType::Indexer,
        endpoint: None,
    };

    let count = Arc::new(AtomicUsize::new(0));
    let idx = FakeIndexer::with_count(count.clone());
    let node = Node::new(cfg, lease.clone(), idx);

    // run node briefly to pick up branch entries and attempt to acquire leases
    let _ = node.run_for(Duration::from_millis(300)).await;

    let rr_main = RemoteRepo {
        name: repo_name.clone(),
        git_url: url.clone(),
        branch: Some("main".into()),
    };
    let rr_dev = RemoteRepo {
        name: repo_name.clone(),
        git_url: url.clone(),
        branch: Some("dev".into()),
    };

    let l_main = lease
        .get_lease(&rr_main)
        .await
        .expect("lease main should exist");
    let l_dev = lease
        .get_lease(&rr_dev)
        .await
        .expect("lease dev should exist");

    assert_eq!(l_main.holder, "node-int");
    assert_eq!(l_dev.holder, "node-int");
    // indexer should have run at least twice (one per branch)
    assert!(count.load(Ordering::SeqCst) >= 2);

    // cleanup
    let _: () = conn
        .hdel("zoekt:repo_branches", &field_main)
        .await
        .unwrap_or(());
    let _: () = conn
        .hdel("zoekt:repo_branches", &field_dev)
        .await
        .unwrap_or(());
}
