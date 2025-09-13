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

//! Minimal "distributed" layer prototype for zoekt-rs.
//!
//! Goals in this initial patch:
//! - Provide a small in-process lease manager that tracks "remote repos"
//! - Provide a Node that periodically tries to acquire leases and runs a provided indexer
//! - Keep the API small so tests can plug a fake indexer easily

pub mod config;
pub mod distributed_search;
pub mod lease_manager;
pub mod node;
pub mod poller;
pub mod redis_adapter;
pub mod surreal_repo_store;
pub mod test_utils;
pub mod web_utils;

pub use config::{load_node_config, MergeOpts, NodeConfig, NodeType};
pub use lease_manager::{Lease, LeaseManager, RemoteRepo};
pub use node::{Indexer, Node};
pub use zoekt_rs::InMemoryIndex;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{init_test_logging, EnvGuard, FakeIndexer, SleepIndexer};
    use anyhow::Result;
    use std::env;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::time::Duration;

    #[tokio::test]
    async fn node_acquires_and_indexes() -> Result<()> {
        init_test_logging();
        tracing::info!("TEST START: node_acquires_and_indexes");
        let lease = LeaseManager::new().await;
        let cfg = NodeConfig {
            id: "test-node".into(),
            lease_ttl: Duration::from_secs(2),
            poll_interval: Duration::from_millis(10),
            node_type: NodeType::Indexer,
            endpoint: None,
            enable_reindex: true,
            index_once: false,
        };
        let node = Node::new(cfg, lease.clone(), FakeIndexer::new());
        let repo = RemoteRepo {
            name: "r1".into(),
            // Use a non-local URL so the node attempts to acquire a lease in tests.
            git_url: "https://example.com/fake-repo.git".into(),
            branch: Some("main".into()),
            visibility: zoekt_rs::types::RepoVisibility::Public,
            owner: None,
            allowed_users: Vec::new(),
            last_commit_sha: None,
        };
        node.add_remote(repo.clone());
        node.run_for(Duration::from_millis(500)).await?;
        let l = lease.get_lease(&repo).await.expect("lease should exist");
        assert_eq!(l.holder, "test-node");
        tracing::info!("TEST END: node_acquires_and_indexes");
        Ok(())
    }

    #[tokio::test]
    async fn node_records_repo_meta_on_index() -> Result<()> {
        init_test_logging();
        // This test requires Redis to be available; skip when REDIS_URL is not set so local runs stay fast.
        if env::var("REDIS_URL").is_err() {
            tracing::info!("TEST SKIP: node_records_repo_meta_on_index (no REDIS_URL)");
            return Ok(());
        }
        // If Redis is configured but not reachable, skip the test to avoid spurious failures
        if let Some(pool) = crate::redis_adapter::create_redis_pool() {
            if pool.get().await.is_err() {
                tracing::info!("TEST SKIP: node_records_repo_meta_on_index (redis unreachable)");
                return Ok(());
            }
        } else {
            tracing::info!(
                "TEST SKIP: node_records_repo_meta_on_index (redis pool creation failed)"
            );
            return Ok(());
        }
        tracing::info!("TEST START: node_records_repo_meta_on_index");

        // Create a lease manager and set a meta callback to capture values
        let lease = LeaseManager::new().await;
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        lease.set_meta_sender(tx);

        let cfg = NodeConfig {
            id: "test-node-meta".into(),
            lease_ttl: Duration::from_secs(2),
            poll_interval: Duration::from_millis(10),
            node_type: NodeType::Indexer,
            endpoint: None,
            enable_reindex: true,
            index_once: false,
        };

        let indexer_node = Node::new(cfg, lease.clone(), SleepIndexer);
        let repo = RemoteRepo {
            name: "r-meta".into(),
            git_url: "/tmp/fake-repo-meta".into(),
            branch: Some("main".into()),
            visibility: zoekt_rs::types::RepoVisibility::Public,
            owner: None,
            allowed_users: Vec::new(),
            last_commit_sha: None,
        };
        indexer_node.add_remote(repo.clone());
        // Run the node for a short duration to trigger indexing
        let run_result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            indexer_node.run_for(Duration::from_millis(50)),
        )
        .await;
        match run_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("node run failed: {}", e),
            Err(_) => panic!("node run timed out after 10 seconds"),
        }

        // Wait for meta message and verify values look reasonable
        let rec = match tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await {
            Ok(Some(r)) => r,
            Ok(None) => panic!("meta sender channel closed unexpectedly"),
            Err(_) => panic!("timed out waiting for meta message after 5 seconds"),
        };
        assert_eq!(rec.0, "r-meta");
        // branch should be present and match
        assert_eq!(rec.1, "main");
        // last_indexed should be close to now (within 10s)
        let now_ms = chrono::Utc::now().timestamp_millis();
        assert!(rec.2 <= now_ms && rec.2 > now_ms - 10000);
        // duration should be positive
        assert!(rec.3 > 0);
        // memory estimate should be non-negative
        assert!(rec.4 >= 0);
        // leased node should match config id
        assert_eq!(rec.5, "test-node-meta");

        tracing::info!("TEST END: node_records_repo_meta_on_index");
        Ok(())
    }

    // Concurrent integration test that requires a real Redis instance. The test will be
    // skipped when REDIS_URL is not set so local runs stay fast.
    #[tokio::test]
    async fn concurrent_nodes_contend_for_lease() -> Result<()> {
        init_test_logging();
        if env::var("REDIS_URL").is_err() {
            tracing::info!("TEST SKIP: concurrent_nodes_contend_for_lease (no REDIS_URL)");
            return Ok(());
        }
        // If Redis is configured but not reachable, skip the test to avoid spurious failures
        if let Some(pool) = crate::redis_adapter::create_redis_pool() {
            if pool.get().await.is_err() {
                tracing::info!("TEST SKIP: concurrent_nodes_contend_for_lease (redis unreachable)");
                return Ok(());
            }
        } else {
            tracing::info!(
                "TEST SKIP: concurrent_nodes_contend_for_lease (redis pool creation failed)"
            );
            return Ok(());
        }
        tracing::info!("TEST START: concurrent_nodes_contend_for_lease");

        let lease = LeaseManager::new().await;
        let repo = RemoteRepo {
            name: "r-contend".into(),
            // Use a non-local URL so nodes contend for a Redis-backed lease in this test
            git_url: "https://example.com/fake-repo-contend.git".into(),
            branch: Some("main".into()),
            visibility: zoekt_rs::types::RepoVisibility::Public,
            owner: None,
            allowed_users: Vec::new(),
            last_commit_sha: None,
        };

        // Two fake indexers that count how many times they were invoked (i.e. wins)
        let a_cnt = Arc::new(AtomicUsize::new(0));
        let b_cnt = Arc::new(AtomicUsize::new(0));
        let idx_a = FakeIndexer::with_count(a_cnt.clone());
        let idx_b = FakeIndexer::with_count(b_cnt.clone());

        let cfg_a = NodeConfig {
            id: "node-a".into(),
            lease_ttl: Duration::from_secs(1),
            poll_interval: Duration::from_millis(5),
            node_type: NodeType::Indexer,
            endpoint: None,
            enable_reindex: true,
            index_once: false,
        };
        let cfg_b = NodeConfig {
            id: "node-b".into(),
            lease_ttl: Duration::from_secs(1),
            poll_interval: Duration::from_millis(5),
            node_type: NodeType::Indexer,
            endpoint: None,
            enable_reindex: true,
            index_once: false,
        };

        let node_a = Node::new(cfg_a, lease.clone(), idx_a);
        let node_b = Node::new(cfg_b, lease.clone(), idx_b);

        // Both nodes observe the same repo and will run their loops, placing bids when they fail to acquire.
        node_a.add_remote(repo.clone());
        node_b.add_remote(repo.clone());

        let t1 = tokio::spawn(async move { node_a.run_for(Duration::from_millis(200)).await });
        let t2 = tokio::spawn(async move { node_b.run_for(Duration::from_millis(200)).await });

        let _ = tokio::join!(t1, t2);

        let a_wins = a_cnt.load(Ordering::SeqCst);
        let b_wins = b_cnt.load(Ordering::SeqCst);
        assert!(
            a_wins + b_wins > 0,
            "expected some successful acquisitions, got {}+{}",
            a_wins,
            b_wins
        );
        tracing::info!("TEST END: concurrent_nodes_contend_for_lease");
        Ok(())
    }

    #[test]
    fn test_looks_like_remote_git_url() {
        // remote forms
        assert!(crate::node::looks_like_remote_git_url(
            "https://example.com/repo.git"
        ));
        assert!(crate::node::looks_like_remote_git_url(
            "http://example.com/repo"
        ));
        assert!(crate::node::looks_like_remote_git_url(
            "git@github.com:owner/repo.git"
        ));
        assert!(crate::node::looks_like_remote_git_url(
            "ssh://git@host/owner/repo.git"
        ));

        // local/file forms
        assert!(!crate::node::looks_like_remote_git_url("/tmp/repo"));
        assert!(!crate::node::looks_like_remote_git_url("file:///tmp/repo"));
        assert!(!crate::node::looks_like_remote_git_url("./relative/path"));
    }

    #[test]
    fn test_node_type_from_str() {
        assert_eq!("indexer".parse::<NodeType>().unwrap(), NodeType::Indexer);
        assert_eq!("INDEXER".parse::<NodeType>().unwrap(), NodeType::Indexer);
        assert_eq!("admin".parse::<NodeType>().unwrap(), NodeType::Admin);
        assert_eq!("search".parse::<NodeType>().unwrap(), NodeType::Search);

        assert!("invalid".parse::<NodeType>().is_err());
        assert!("".parse::<NodeType>().is_err());
    }

    #[serial_test::serial]
    #[test]
    fn test_discover_statefulset_pod_endpoint_success() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "my_service_SERVICE_HOST",
            "my_service_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
        ]);

        // Set up environment variables to simulate a StatefulSet pod
        env_guard.set("POD_NAME", "zoekt-indexer-2");
        env_guard.set("ZOEKTD_SERVICE_NAME", "zoekt-indexer");
        env_guard.set("ZOEKTD_SERVICE_PORT", "8080");
        env_guard.set("ZOEKTD_SERVICE_PROTOCOL", "http");

        let result = NodeConfig::discover_statefulset_pod_endpoint("http");
        assert_eq!(
            result,
            Some("http://zoekt-indexer-2.zoekt-indexer.svc.cluster.local:8080".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[serial_test::serial]
    #[test]
    fn test_discover_statefulset_pod_endpoint_pod_name() {
        // Explicitly clear all potentially interfering environment variables
        std::env::remove_var("ZOEKTD_SERVICE_NAME");
        std::env::remove_var("ZOEKTD_SERVICE_PORT");
        std::env::remove_var("ZOEKTD_SERVICE_PROTOCOL");
        std::env::remove_var("MY_SERVICE_SERVICE_HOST");
        std::env::remove_var("MY_SERVICE_SERVICE_PORT");
        std::env::remove_var("zoekt_SERVICE_HOST");
        std::env::remove_var("zoekt_SERVICE_PORT");
        std::env::remove_var("ZOekt_SERVICE_HOST");
        std::env::remove_var("ZOekt_SERVICE_PORT");
        std::env::remove_var("my_service_SERVICE_HOST");
        std::env::remove_var("my_service_SERVICE_PORT");
        std::env::remove_var("testservice_SERVICE_HOST");
        std::env::remove_var("testservice_SERVICE_PORT");
        std::env::remove_var("POD_NAME");
        std::env::remove_var("HOSTNAME");

        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "my_service_SERVICE_HOST",
            "my_service_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
            "POD_NAME",
            "HOSTNAME",
        ]);

        // Test using POD_NAME instead of HOSTNAME (since HOSTNAME is no longer used)
        env_guard.set("POD_NAME", "zoekt-indexer-1");
        env_guard.set("ZOEKTD_SERVICE_NAME", "zoekt-indexer");
        env_guard.set("ZOEKTD_SERVICE_PORT", "9090");

        let result = NodeConfig::discover_statefulset_pod_endpoint("https");
        assert_eq!(
            result,
            Some("https://zoekt-indexer-1.zoekt-indexer.svc.cluster.local:9090".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[serial_test::serial]
    #[test]
    fn test_discover_statefulset_pod_endpoint_infer_service_name() {
        // Explicitly clear all potentially interfering environment variables
        std::env::remove_var("ZOEKTD_SERVICE_NAME");
        std::env::remove_var("ZOEKTD_SERVICE_PORT");
        std::env::remove_var("ZOEKTD_SERVICE_PROTOCOL");
        std::env::remove_var("MY_SERVICE_SERVICE_HOST");
        std::env::remove_var("MY_SERVICE_SERVICE_PORT");
        std::env::remove_var("zoekt_SERVICE_HOST");
        std::env::remove_var("zoekt_SERVICE_PORT");
        std::env::remove_var("ZOekt_SERVICE_HOST");
        std::env::remove_var("ZOekt_SERVICE_PORT");
        std::env::remove_var("my_service_SERVICE_HOST");
        std::env::remove_var("my_service_SERVICE_PORT");
        std::env::remove_var("testservice_SERVICE_HOST");
        std::env::remove_var("testservice_SERVICE_PORT");
        std::env::remove_var("POD_NAME");
        std::env::remove_var("HOSTNAME");

        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "my_service_SERVICE_HOST",
            "my_service_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
            "POD_NAME",
            "HOSTNAME",
        ]);

        // Test inferring service name from pod name
        // Don't set ZOEKTD_SERVICE_NAME at all to ensure it's not present
        env_guard.set("POD_NAME", "my-indexer-0");
        env_guard.set("ZOEKTD_SERVICE_PORT", "8080");
        let result = NodeConfig::discover_statefulset_pod_endpoint("http");
        assert_eq!(
            result,
            Some("http://my-indexer-0.my-indexer.svc.cluster.local:8080".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[serial_test::serial]
    #[test]
    fn test_discover_statefulset_pod_endpoint_no_pod_name() {
        // Explicitly clear all potentially interfering environment variables
        std::env::remove_var("POD_NAME");
        std::env::remove_var("HOSTNAME");
        std::env::remove_var("ZOEKTD_SERVICE_NAME");
        std::env::remove_var("ZOEKTD_SERVICE_PORT");
        std::env::remove_var("ZOEKTD_SERVICE_PROTOCOL");
        std::env::remove_var("MY_SERVICE_SERVICE_HOST");
        std::env::remove_var("MY_SERVICE_SERVICE_PORT");
        std::env::remove_var("zoekt_SERVICE_HOST");
        std::env::remove_var("zoekt_SERVICE_PORT");
        std::env::remove_var("ZOekt_SERVICE_HOST");
        std::env::remove_var("ZOekt_SERVICE_PORT");
        std::env::remove_var("my_service_SERVICE_HOST");
        std::env::remove_var("my_service_SERVICE_PORT");
        std::env::remove_var("testservice_SERVICE_HOST");
        std::env::remove_var("testservice_SERVICE_PORT");

        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "my_service_SERVICE_HOST",
            "my_service_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
        ]);

        let result = NodeConfig::discover_statefulset_pod_endpoint("http");
        assert_eq!(result, None);

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[serial_test::serial]
    #[test]
    fn test_discover_statefulset_pod_endpoint_no_service_name() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "my_service_SERVICE_HOST",
            "my_service_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
        ]);

        // Test when pod name is available but no service name can be determined
        // Use a pod name that doesn't contain '-' to avoid service name inference
        env_guard.set("POD_NAME", "singlepodnoordinal");
        env_guard.set("ZOEKTD_SERVICE_PORT", "8080");

        let result = NodeConfig::discover_statefulset_pod_endpoint("http");
        assert_eq!(result, None);

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[serial_test::serial]
    #[test]
    fn test_discover_kubernetes_endpoint_manual_override() {
        // Clear any existing environment variables that might interfere
        std::env::remove_var("ZOEKTD_ENDPOINT");
        std::env::remove_var("POD_NAME");
        std::env::remove_var("HOSTNAME");
        std::env::remove_var("ZOEKTD_SERVICE_NAME");
        std::env::remove_var("ZOEKTD_SERVICE_PORT");
        std::env::remove_var("ZOEKTD_SERVICE_PROTOCOL");
        std::env::remove_var("ZOEKTD_NODE_TYPE");

        // Test manual override in NodeConfig::default() (not in discover_kubernetes_endpoint directly)
        std::env::set_var("ZOEKTD_ENDPOINT", "http://custom-endpoint:9999");
        std::env::set_var("ZOEKTD_NODE_TYPE", "indexer");

        let config = NodeConfig::default();
        assert_eq!(
            config.endpoint,
            Some("http://custom-endpoint:9999".to_string())
        );

        // Clean up
        std::env::remove_var("ZOEKTD_ENDPOINT");
        std::env::remove_var("ZOEKTD_NODE_TYPE");
    }

    #[serial_test::serial]
    #[test]
    fn test_discover_kubernetes_endpoint_service_name_priority() {
        // Explicitly clear all potentially interfering environment variables
        std::env::remove_var("ZOEKTD_ENDPOINT");
        std::env::remove_var("POD_NAME");
        std::env::remove_var("HOSTNAME");
        std::env::remove_var("ZOEKTD_SERVICE_NAME");
        std::env::remove_var("ZOEKTD_SERVICE_PORT");
        std::env::remove_var("ZOEKTD_SERVICE_PROTOCOL");
        std::env::remove_var("MY_SERVICE_SERVICE_HOST");
        std::env::remove_var("MY_SERVICE_SERVICE_PORT");
        std::env::remove_var("zoekt_SERVICE_HOST");
        std::env::remove_var("zoekt_SERVICE_PORT");
        std::env::remove_var("ZOekt_SERVICE_HOST");
        std::env::remove_var("ZOekt_SERVICE_PORT");

        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
        ]);

        // Test ZOEKTD_SERVICE_NAME takes priority over auto-discovery
        env_guard.set("HOSTNAME", "test"); // Prevent StatefulSet discovery
        env_guard.set("POD_NAME", ""); // Ensure POD_NAME is not set to prevent StatefulSet discovery
        env_guard.set("ZOEKTD_SERVICE_NAME", "my-service");
        env_guard.set("MY_SERVICE_SERVICE_HOST", "10.0.1.1");
        env_guard.set("MY_SERVICE_SERVICE_PORT", "8080");

        let result = NodeConfig::discover_kubernetes_endpoint();
        assert_eq!(result, Some("http://10.0.1.1:8080".to_string()));

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[serial_test::serial]
    #[test]
    fn test_discover_kubernetes_endpoint_statefulset_fallback() {
        // Explicitly clear all potentially interfering environment variables
        std::env::remove_var("ZOEKTD_ENDPOINT");
        std::env::remove_var("POD_NAME");
        std::env::remove_var("HOSTNAME");
        std::env::remove_var("ZOEKTD_SERVICE_NAME");
        std::env::remove_var("ZOEKTD_SERVICE_PORT");
        std::env::remove_var("ZOEKTD_SERVICE_PROTOCOL");
        std::env::remove_var("MY_SERVICE_SERVICE_HOST");
        std::env::remove_var("MY_SERVICE_SERVICE_PORT");
        std::env::remove_var("zoekt_SERVICE_HOST");
        std::env::remove_var("zoekt_SERVICE_PORT");
        std::env::remove_var("ZOekt_SERVICE_HOST");
        std::env::remove_var("ZOekt_SERVICE_PORT");
        std::env::remove_var("my_service_SERVICE_HOST");
        std::env::remove_var("my_service_SERVICE_PORT");
        std::env::remove_var("testservice_SERVICE_HOST");
        std::env::remove_var("testservice_SERVICE_PORT");

        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "my_service_SERVICE_HOST",
            "my_service_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
        ]);

        // Test StatefulSet discovery as fallback when no service env vars
        env_guard.set("POD_NAME", "zoekt-indexer-0");
        env_guard.set("ZOEKTD_SERVICE_NAME", "zoekt-indexer");
        env_guard.set("ZOEKTD_SERVICE_PORT", "8080");

        let result = NodeConfig::discover_kubernetes_endpoint();
        assert_eq!(
            result,
            Some("http://zoekt-indexer-0.zoekt-indexer.svc.cluster.local:8080".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[serial_test::serial]
    #[test]
    fn test_discover_kubernetes_endpoint_service_env_vars() {
        // Explicitly clear all potentially interfering environment variables
        std::env::remove_var("ZOEKTD_ENDPOINT");
        std::env::remove_var("POD_NAME");
        std::env::remove_var("HOSTNAME");
        std::env::remove_var("ZOEKTD_SERVICE_NAME");
        std::env::remove_var("ZOEKTD_SERVICE_PORT");
        std::env::remove_var("ZOEKTD_SERVICE_PROTOCOL");
        std::env::remove_var("ZOekt_SERVICE_HOST");
        std::env::remove_var("ZOekt_SERVICE_PORT");
        std::env::remove_var("zoekt_SERVICE_HOST");
        std::env::remove_var("zoekt_SERVICE_PORT");
        std::env::remove_var("testservice_SERVICE_HOST");
        std::env::remove_var("testservice_SERVICE_PORT");
        std::env::remove_var("TESTSERVICE_SERVICE_HOST");
        std::env::remove_var("TESTSERVICE_SERVICE_PORT");

        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "ZOekt_SERVICE_HOST",
            "ZOekt_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
            "TESTSERVICE_SERVICE_HOST",
            "TESTSERVICE_SERVICE_PORT",
        ]);

        // Test auto-discovery from service environment variables
        // Use a simple service name to avoid conflicts
        env_guard.set("POD_NAME", ""); // Ensure POD_NAME is not set
        env_guard.set("HOSTNAME", "test"); // Set HOSTNAME to a value that doesn't trigger StatefulSet
        env_guard.set("testservice_SERVICE_HOST", "10.0.2.1");
        env_guard.set("TESTSERVICE_SERVICE_PORT", "9090");

        let result = NodeConfig::discover_kubernetes_endpoint();
        assert_eq!(result, Some("http://10.0.2.1:9090".to_string()));

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[serial_test::serial]
    #[test]
    fn test_discover_kubernetes_endpoint_no_discovery() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "MY_SERVICE_SERVICE_HOST",
            "MY_SERVICE_SERVICE_PORT",
            "zoekt_SERVICE_HOST",
            "zoekt_SERVICE_PORT",
            "testservice_SERVICE_HOST",
            "testservice_SERVICE_PORT",
        ]);

        // Test when no discovery method works
        let result = NodeConfig::discover_kubernetes_endpoint();
        assert_eq!(result, None);

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[serial_test::serial]
    #[test]
    fn test_node_config_default_with_statefulset_env() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "ZOEKTD_NODE_TYPE",
        ]);

        // Test NodeConfig::default() with StatefulSet environment
        env_guard.set("HOSTNAME", ""); // Ensure HOSTNAME doesn't interfere
        env_guard.set("POD_NAME", "zoekt-indexer-1");
        env_guard.set("ZOEKTD_SERVICE_NAME", "zoekt-indexer");
        env_guard.set("ZOEKTD_SERVICE_PORT", "8080");
        env_guard.set("ZOEKTD_NODE_TYPE", "indexer");

        let config = NodeConfig::default();
        assert_eq!(config.node_type, NodeType::Indexer);
        assert_eq!(
            config.endpoint,
            Some("http://zoekt-indexer-1.zoekt-indexer.svc.cluster.local:8080".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }

    #[serial_test::serial]
    #[test]
    fn test_node_config_default_manual_endpoint_override() {
        let mut env_guard = EnvGuard::new();
        env_guard.save_and_clear(&[
            "ZOEKTD_ENDPOINT",
            "POD_NAME",
            "HOSTNAME",
            "ZOEKTD_SERVICE_NAME",
            "ZOEKTD_SERVICE_PORT",
            "ZOEKTD_SERVICE_PROTOCOL",
            "ZOEKTD_NODE_TYPE",
        ]);

        // Test that manual endpoint override works in NodeConfig::default()
        env_guard.set("HOSTNAME", ""); // Ensure HOSTNAME doesn't interfere
        env_guard.set("ZOEKTD_ENDPOINT", "http://manual-override:9999");
        env_guard.set("POD_NAME", "zoekt-indexer-0"); // This should be ignored

        let config = NodeConfig::default();
        assert_eq!(
            config.endpoint,
            Some("http://manual-override:9999".to_string())
        );

        // EnvGuard will automatically clean up when it goes out of scope
    }
}

#[tokio::test]
async fn branch_scoped_leases_are_independent() {
    let lease = LeaseManager::new().await;
    let repo_base = RemoteRepo {
        name: "r-branch-test".into(),
        git_url: "https://example.com/branch-repo.git".into(),
        branch: Some("main".into()),
        visibility: zoekt_rs::types::RepoVisibility::Public,
        owner: None,
        allowed_users: Vec::new(),
        last_commit_sha: None,
    };

    let repo_a = RemoteRepo {
        name: repo_base.name.clone(),
        git_url: repo_base.git_url.clone(),
        branch: Some("main".into()),
        visibility: zoekt_rs::types::RepoVisibility::Public,
        owner: None,
        allowed_users: Vec::new(),
        last_commit_sha: None,
    };
    let repo_b = RemoteRepo {
        name: repo_base.name.clone(),
        git_url: repo_base.git_url.clone(),
        branch: Some("dev".into()),
        visibility: zoekt_rs::types::RepoVisibility::Public,
        owner: None,
        allowed_users: Vec::new(),
        last_commit_sha: None,
    };

    // Two different holders should be able to acquire different branch leases independently
    let a_ok = lease
        .try_acquire(&repo_a, "node-a".into(), std::time::Duration::from_secs(2))
        .await;
    let b_ok = lease
        .try_acquire(&repo_b, "node-b".into(), std::time::Duration::from_secs(2))
        .await;

    assert!(a_ok, "expected node-a to acquire main branch lease");
    assert!(b_ok, "expected node-b to acquire dev branch lease");

    // Confirm leases are held by respective nodes
    let la = lease.get_lease(&repo_a).await.expect("lease a exists");
    let lb = lease.get_lease(&repo_b).await.expect("lease b exists");
    assert_eq!(la.holder, "node-a");
    assert_eq!(lb.holder, "node-b");
}
