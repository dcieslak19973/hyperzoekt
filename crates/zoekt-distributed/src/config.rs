use anyhow::Result;
use std::fs;
use std::time::Duration;

use crate::NodeConfig;

/// CLI-level options that binaries pass to `load_node_config`.
/// Keep this small and explicit; binaries can expand for extra fields.
#[derive(Clone, Debug)]
pub struct MergeOpts {
    pub config_path: Option<std::path::PathBuf>,
    pub cli_id: Option<String>,
    pub cli_lease_ttl_seconds: Option<u64>,
    pub cli_poll_interval_seconds: Option<u64>,
    pub cli_endpoint: Option<String>,
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

    // NodeType may still be set by the caller
    Ok(base)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeType;
    use std::time::Duration;
    use tracing_subscriber::EnvFilter;

    fn init_test_logging() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let filter =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
            tracing_subscriber::fmt().with_env_filter(filter).init();
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
