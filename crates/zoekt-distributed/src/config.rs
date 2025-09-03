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

use anyhow::Result;
use rand;
use std::fs;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeType {
    Indexer,
    Admin,
    Search,
}

impl std::str::FromStr for NodeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "indexer" => Ok(NodeType::Indexer),
            "admin" => Ok(NodeType::Admin),
            "search" => Ok(NodeType::Search),
            _ => Err(format!("Unknown node type: {}", s)),
        }
    }
}

/// Node configuration.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub id: String,
    pub lease_ttl: Duration,
    pub poll_interval: Duration,
    pub node_type: NodeType,
    pub endpoint: Option<String>,
    /// If false, the node will not run indexing work (useful for disabling reindexing in dev)
    pub enable_reindex: bool,
    /// If true, index each registered repo once and then skip further re-index attempts
    pub index_once: bool,
}

impl Default for NodeConfig {
    fn default() -> Self {
        // Read configuration from environment variables when present. Useful for k8s.
        // Env vars:
        // - ZOEKTD_NODE_ID
        // - POD_NAME (Kubernetes pod name, used as default if ZOEKTD_NODE_ID not set)
        // - ZOEKTD_LEASE_TTL_SECONDS
        // - ZOEKTD_POLL_INTERVAL_SECONDS
        // - ZOEKTD_ENDPOINT (manual override)
        // - ZOEKTD_SERVICE_NAME (for k8s service discovery)
        // - ZOEKTD_SERVICE_PORT (for k8s service discovery)
        // - ZOEKTD_SERVICE_PROTOCOL (http/https, defaults to http)
        let id = std::env::var("ZOEKTD_NODE_ID")
            .or_else(|_| std::env::var("POD_NAME"))
            .unwrap_or_else(|_| format!("node-{}", rand::random::<u64>()));
        let lease_ttl = std::env::var("ZOEKTD_LEASE_TTL_SECONDS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(150));
        let poll_interval = std::env::var("ZOEKTD_POLL_INTERVAL_SECONDS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(5));

        let node_type: NodeType = std::env::var("ZOEKTD_NODE_TYPE")
            .ok()
            .and_then(|s| s.parse::<NodeType>().ok())
            .unwrap_or(NodeType::Indexer);

        // Determine endpoint: manual override takes precedence, then try k8s auto-discovery
        let endpoint = if let Ok(manual_endpoint) = std::env::var("ZOEKTD_ENDPOINT") {
            Some(manual_endpoint)
        } else if node_type == NodeType::Indexer {
            // Try Kubernetes service discovery for indexers
            Self::discover_kubernetes_endpoint()
        } else {
            None
        };

        Self {
            id,
            lease_ttl,
            poll_interval,
            node_type,
            endpoint,
            enable_reindex: std::env::var("ZOEKTD_ENABLE_REINDEX")
                .ok()
                .and_then(|s| s.parse::<bool>().ok())
                .unwrap_or(true),
            index_once: std::env::var("ZOEKTD_INDEX_ONCE")
                .ok()
                .and_then(|s| s.parse::<bool>().ok())
                .unwrap_or(false),
        }
    }
}

impl NodeConfig {
    /// Attempt to auto-discover the endpoint using Kubernetes service environment variables.
    /// This supports both direct service discovery and custom service name configuration.
    /// For StatefulSets, each pod discovers its own individual endpoint.
    pub(crate) fn discover_kubernetes_endpoint() -> Option<String> {
        let protocol = std::env::var("ZOEKTD_SERVICE_PROTOCOL").unwrap_or_else(|_| "http".into());

        // First, try using ZOEKTD_SERVICE_NAME if explicitly set
        if let Ok(service_name) = std::env::var("ZOEKTD_SERVICE_NAME") {
            let normalized_service_name = service_name.replace("-", "_").to_uppercase();
            let service_host_env = format!("{}_SERVICE_HOST", normalized_service_name);
            let service_port_env = format!("{}_SERVICE_PORT", normalized_service_name);

            if let (Ok(host), Ok(port)) = (
                std::env::var(&service_host_env),
                std::env::var(&service_port_env),
            ) {
                tracing::info!(service_name = %service_name, host = %host, port = %port, protocol = %protocol, "discovered kubernetes service endpoint");
                return Some(format!("{}://{}:{}", protocol, host, port));
            } else {
                tracing::warn!(service_name = %service_name, "kubernetes service environment variables not found");
            }
        }

        // For StatefulSets: try to discover pod-specific endpoint
        // Each pod in a StatefulSet gets a stable DNS name like: <statefulset-name>-<ordinal>.<service-name>
        if let Some(pod_endpoint) = Self::discover_statefulset_pod_endpoint(&protocol) {
            return Some(pod_endpoint);
        }

        // Fallback: try common Kubernetes service patterns
        // Look for any environment variables that match the pattern *_SERVICE_HOST
        for (key, value) in std::env::vars() {
            if key.ends_with("_SERVICE_HOST") && !key.starts_with("KUBERNETES_") {
                let service_name = key.trim_end_matches("_SERVICE_HOST").to_lowercase();
                let port_env = format!("{}_SERVICE_PORT", service_name.to_uppercase());

                if let Ok(port) = std::env::var(&port_env) {
                    let protocol =
                        std::env::var("ZOEKTD_SERVICE_PROTOCOL").unwrap_or_else(|_| "http".into());
                    tracing::info!(service_name = %service_name, host = %value, port = %port, protocol = %protocol, "auto-discovered kubernetes service endpoint");
                    return Some(format!("{}://{}:{}", protocol, value, port));
                }
            }
        }

        tracing::debug!("no kubernetes service endpoint discovered, will use manual configuration");
        None
    }

    /// Discover the endpoint for a StatefulSet pod using pod metadata and service information.
    /// Each pod in a StatefulSet gets a stable DNS name: <pod-name>.<service-name>
    pub(crate) fn discover_statefulset_pod_endpoint(protocol: &str) -> Option<String> {
        // Try to get pod name from environment (injected by Downward API or via env)
        // Only use POD_NAME, not HOSTNAME, as HOSTNAME may be set to arbitrary values in containers
        let pod_name = std::env::var("POD_NAME").ok()?;
        if pod_name.is_empty() {
            return None;
        }

        // Try to get namespace
        let namespace = std::env::var("POD_NAMESPACE")
            .or_else(|_| std::env::var("NAMESPACE"))
            .unwrap_or_else(|_| "default".into());

        // Try to get service name from various sources
        let service_name = std::env::var("ZOEKTD_SERVICE_NAME")
            .or_else(|_| {
                // Try to infer from pod name (remove ordinal suffix)
                pod_name
                    .rfind('-')
                    .map(|pos| pod_name[..pos].to_string())
                    .ok_or(std::env::VarError::NotPresent)
            })
            .or_else(|_| {
                // Look for service environment variables and infer service name
                std::env::vars()
                    .find(|(key, _)| {
                        key.ends_with("_SERVICE_HOST") && !key.starts_with("KUBERNETES_")
                    })
                    .map(|(key, _)| key.trim_end_matches("_SERVICE_HOST").to_lowercase())
                    .ok_or(std::env::VarError::NotPresent)
            })
            .ok()?;

        // Try to get port from service environment variables
        let service_port_env = format!(
            "{}_SERVICE_PORT",
            service_name.replace("-", "_").to_uppercase()
        );
        let port = std::env::var(&service_port_env)
            .or_else(|_| std::env::var("ZOEKTD_SERVICE_PORT"))
            .unwrap_or_else(|_| "8080".into());

        // Construct the pod-specific DNS name for StatefulSet
        let pod_dns = format!("{}.{}.svc.cluster.local", pod_name, service_name);

        tracing::info!(
            pod_name = %pod_name,
            service_name = %service_name,
            namespace = %namespace,
            pod_dns = %pod_dns,
            port = %port,
            protocol = %protocol,
            "discovered statefulset pod endpoint"
        );

        Some(format!("{}://{}:{}", protocol, pod_dns, port))
    }
}

/// CLI-level options that binaries pass to `load_node_config`.
/// Keep this small and explicit; binaries can expand for extra fields.
#[derive(Clone, Debug)]
pub struct MergeOpts {
    pub config_path: Option<std::path::PathBuf>,
    pub cli_id: Option<String>,
    pub cli_lease_ttl_seconds: Option<u64>,
    pub cli_poll_interval_seconds: Option<u64>,
    pub cli_endpoint: Option<String>,
    pub cli_enable_reindex: Option<bool>,
    pub cli_index_once: Option<bool>,
}

/// Load and merge NodeConfig from: defaults <- config file <- env vars <- CLI
pub fn load_node_config(mut base: NodeConfig, opts: MergeOpts) -> Result<NodeConfig> {
    if let Some(path) = opts.config_path.as_ref() {
        if path.exists() {
            let s = fs::read_to_string(path)?;
            let v: toml::Value = toml::from_str(&s)?;
            if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                base.id = id.to_string();
            }
            if let Some(t) = v.get("lease_ttl_seconds").and_then(|x| x.as_integer()) {
                base.lease_ttl = Duration::from_secs(t as u64);
            }
            if let Some(p) = v.get("poll_interval_seconds").and_then(|x| x.as_integer()) {
                base.poll_interval = Duration::from_secs(p as u64);
            }
            if let Some(e) = v.get("endpoint").and_then(|x| x.as_str()) {
                base.endpoint = Some(e.to_string());
            }
        }
    }

    // env vars override file
    if let Ok(id) = std::env::var("ZOEKTD_NODE_ID") {
        base.id = id;
    }
    if let Ok(ttl) = std::env::var("ZOEKTD_LEASE_TTL_SECONDS") {
        if let Ok(v) = ttl.parse::<u64>() {
            base.lease_ttl = Duration::from_secs(v);
        }
    }
    if let Ok(pi) = std::env::var("ZOEKTD_POLL_INTERVAL_SECONDS") {
        if let Ok(v) = pi.parse::<u64>() {
            base.poll_interval = Duration::from_secs(v);
        }
    }
    if let Ok(e) = std::env::var("ZOEKTD_ENDPOINT") {
        base.endpoint = Some(e);
    }
    if let Ok(v) = std::env::var("ZOEKTD_ENABLE_REINDEX") {
        if let Ok(b) = v.parse::<bool>() {
            base.enable_reindex = b;
        }
    }
    if let Ok(v) = std::env::var("ZOEKTD_INDEX_ONCE") {
        if let Ok(b) = v.parse::<bool>() {
            base.index_once = b;
        }
    }

    // CLI overrides everything
    if let Some(id) = opts.cli_id {
        base.id = id;
    }
    if let Some(ttl) = opts.cli_lease_ttl_seconds {
        base.lease_ttl = Duration::from_secs(ttl);
    }
    if let Some(pi) = opts.cli_poll_interval_seconds {
        base.poll_interval = Duration::from_secs(pi);
    }
    if let Some(e) = opts.cli_endpoint {
        base.endpoint = Some(e);
    }
    if let Some(b) = opts.cli_enable_reindex {
        base.enable_reindex = b;
    }
    if let Some(b) = opts.cli_index_once {
        base.index_once = b;
    }

    // NodeType may still be set by the caller
    Ok(base)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tracing_subscriber::EnvFilter;

    fn init_test_logging() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let filter =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
            let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
        });
    }

    #[test]
    #[serial_test::serial]
    fn test_merge_file_env_cli_precedence() {
        init_test_logging();
        tracing::info!("TEST START: config::tests::test_merge_file_env_cli_precedence");
        // ensure a clean environment for the test
        std::env::remove_var("ZOEKTD_NODE_ID");
        std::env::remove_var("ZOEKTD_LEASE_TTL_SECONDS");
        std::env::remove_var("ZOEKTD_POLL_INTERVAL_SECONDS");

        let base = NodeConfig {
            id: "base".into(),
            lease_ttl: Duration::from_secs(150),
            poll_interval: Duration::from_secs(5),
            node_type: NodeType::Indexer,
            endpoint: None,
            enable_reindex: true,
            index_once: false,
        };

        // create a temp file with config
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let toml = r#"
id = "from_file"
lease_ttl_seconds = 11
poll_interval_seconds = 3
"#;
        fs::write(tmp.path(), toml).unwrap();

        // set env overrides
        std::env::set_var("ZOEKTD_NODE_ID", "from_env");
        std::env::set_var("ZOEKTD_LEASE_TTL_SECONDS", "22");
        std::env::set_var("ZOEKTD_POLL_INTERVAL_SECONDS", "4");

        // CLI opts highest precedence
        let opts = MergeOpts {
            config_path: Some(tmp.path().to_path_buf()),
            cli_id: Some("from_cli".into()),
            cli_lease_ttl_seconds: Some(33),
            cli_poll_interval_seconds: Some(5),
            cli_endpoint: None,
            cli_enable_reindex: None,
            cli_index_once: None,
        };

        let got = load_node_config(base, opts).expect("load");
        assert_eq!(got.id, "from_cli");
        assert_eq!(got.lease_ttl.as_secs(), 33);
        assert_eq!(got.poll_interval.as_secs(), 5);

        std::env::remove_var("ZOEKTD_NODE_ID");
        std::env::remove_var("ZOEKTD_LEASE_TTL_SECONDS");
        std::env::remove_var("ZOEKTD_POLL_INTERVAL_SECONDS");
        tracing::info!("TEST END: config::tests::test_merge_file_env_cli_precedence");
    }

    #[test]
    #[serial_test::serial]
    fn test_file_then_env() {
        init_test_logging();
        tracing::info!("TEST START: config::tests::test_file_then_env");
        // ensure a clean environment for the test
        std::env::remove_var("ZOEKTD_NODE_ID");
        std::env::remove_var("ZOEKTD_LEASE_TTL_SECONDS");
        std::env::remove_var("ZOEKTD_POLL_INTERVAL_SECONDS");

        let base = NodeConfig {
            id: "base".into(),
            lease_ttl: Duration::from_secs(150),
            poll_interval: Duration::from_secs(5),
            node_type: NodeType::Indexer,
            endpoint: None,
            enable_reindex: true,
            index_once: false,
        };
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let toml = r#"
id = "file_only"
lease_ttl_seconds = 7
poll_interval_seconds = 2
"#;
        fs::write(tmp.path(), toml).unwrap();

        // ensure other env vars don't interfere with this test
        std::env::remove_var("ZOEKTD_LEASE_TTL_SECONDS");
        std::env::remove_var("ZOEKTD_POLL_INTERVAL_SECONDS");
        std::env::remove_var("ZOEKTD_NODE_ID");
        std::env::set_var("ZOEKTD_NODE_ID", "env_only");

        let opts = MergeOpts {
            config_path: Some(tmp.path().to_path_buf()),
            cli_id: None,
            cli_lease_ttl_seconds: None,
            cli_poll_interval_seconds: None,
            cli_endpoint: None,
            cli_enable_reindex: None,
            cli_index_once: None,
        };
        let got = load_node_config(base, opts).expect("load");
        // env should override file for id
        assert_eq!(got.id, "env_only");
        assert_eq!(got.lease_ttl.as_secs(), 7);

        std::env::remove_var("ZOEKTD_NODE_ID");
        std::env::remove_var("ZOEKTD_LEASE_TTL_SECONDS");
        std::env::remove_var("ZOEKTD_POLL_INTERVAL_SECONDS");
        tracing::info!("TEST END: config::tests::test_file_then_env");
    }

    #[test]
    #[serial_test::serial]
    fn test_ttl_and_poll_precedence_file_env_cli() {
        init_test_logging();
        tracing::info!("TEST START: config::tests::test_ttl_and_poll_precedence_file_env_cli");
        // ensure a clean environment for the test
        std::env::remove_var("ZOEKTD_NODE_ID");
        std::env::remove_var("ZOEKTD_LEASE_TTL_SECONDS");
        std::env::remove_var("ZOEKTD_POLL_INTERVAL_SECONDS");

        let base = NodeConfig::default();

        // file provides small values
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let toml = r#"
lease_ttl_seconds = 8
poll_interval_seconds = 1
"#;
        fs::write(tmp.path(), toml).unwrap();

        // set env to override file for ttl, but not poll
        std::env::set_var("ZOEKTD_LEASE_TTL_SECONDS", "16");
        std::env::remove_var("ZOEKTD_POLL_INTERVAL_SECONDS");

        // CLI opts override both
        let opts = MergeOpts {
            config_path: Some(tmp.path().to_path_buf()),
            cli_id: None,
            cli_lease_ttl_seconds: Some(99),
            cli_poll_interval_seconds: Some(7),
            cli_endpoint: None,
            cli_enable_reindex: None,
            cli_index_once: None,
        };

        let got = load_node_config(base, opts).expect("load");
        // CLI highest precedence
        assert_eq!(got.lease_ttl.as_secs(), 99);
        assert_eq!(got.poll_interval.as_secs(), 7);

        std::env::remove_var("ZOEKTD_NODE_ID");
        std::env::remove_var("ZOEKTD_LEASE_TTL_SECONDS");
        std::env::remove_var("ZOEKTD_POLL_INTERVAL_SECONDS");
        tracing::info!("TEST END: config::tests::test_ttl_and_poll_precedence_file_env_cli");
    }

    #[test]
    #[serial_test::serial]
    fn test_invalid_env_is_ignored() {
        init_test_logging();
        tracing::info!("TEST START: config::tests::test_invalid_env_is_ignored");
        // ensure a clean environment for the test
        std::env::remove_var("ZOEKTD_NODE_ID");
        std::env::remove_var("ZOEKTD_LEASE_TTL_SECONDS");
        std::env::remove_var("ZOEKTD_POLL_INTERVAL_SECONDS");

        let base = NodeConfig::default();

        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let toml = r#"
lease_ttl_seconds = 12
poll_interval_seconds = 6
"#;
        fs::write(tmp.path(), toml).unwrap();

        // set invalid env values which should be ignored and fall back to file
        std::env::set_var("ZOEKTD_LEASE_TTL_SECONDS", "not-a-number");
        std::env::set_var("ZOEKTD_POLL_INTERVAL_SECONDS", "also-bad");

        let opts = MergeOpts {
            config_path: Some(tmp.path().to_path_buf()),
            cli_id: None,
            cli_lease_ttl_seconds: None,
            cli_poll_interval_seconds: None,
            cli_endpoint: None,
            cli_enable_reindex: None,
            cli_index_once: None,
        };

        let got = load_node_config(base, opts).expect("load");
        // invalid env should be ignored => values from file
        assert_eq!(got.lease_ttl.as_secs(), 12);
        assert_eq!(got.poll_interval.as_secs(), 6);

        std::env::remove_var("ZOEKTD_NODE_ID");
        std::env::remove_var("ZOEKTD_LEASE_TTL_SECONDS");
        std::env::remove_var("ZOEKTD_POLL_INTERVAL_SECONDS");
        tracing::info!("TEST END: config::tests::test_invalid_env_is_ignored");
    }
}
