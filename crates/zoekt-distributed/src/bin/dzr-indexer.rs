use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::time::Duration;

use zoekt_distributed::{
    InMemoryIndex, Indexer, LeaseManager, Node, NodeConfig, NodeType, RemoteRepo,
};

#[derive(Parser)]
struct Opts {
    /// Path to a TOML config file (optional)
    #[arg(long)]
    config: Option<PathBuf>,

    /// Node id (overrides env/config)
    #[arg(long)]
    id: Option<String>,

    /// Lease TTL in seconds (overrides env/config)
    #[arg(long)]
    lease_ttl_seconds: Option<u64>,

    /// Poll interval in seconds (overrides env/config)
    #[arg(long)]
    poll_interval_seconds: Option<u64>,

    /// Remote repo name
    #[arg(long)]
    remote_name: Option<String>,

    /// Remote repo URL/path
    #[arg(long)]
    remote_url: Option<String>,
}

struct SimpleIndexer;

impl Indexer for SimpleIndexer {
    fn index_repo(&self, _repo_path: PathBuf) -> Result<InMemoryIndex> {
        // lightweight in-memory index used for the prototype and demos
        let idx = zoekt_rs::test_helpers::make_index_with_trigrams(vec![], "cli", vec![], None);
        Ok(idx)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let opts = Opts::parse();

    let cfg = zoekt_distributed::load_node_config(
        NodeConfig {
            node_type: NodeType::Indexer,
            ..Default::default()
        },
        zoekt_distributed::MergeOpts {
            config_path: opts.config,
            cli_id: opts.id,
            cli_lease_ttl_seconds: opts.lease_ttl_seconds,
            cli_poll_interval_seconds: opts.poll_interval_seconds,
        },
    )?;

    let lease_mgr = LeaseManager::new().await;
    let indexer = SimpleIndexer;
    let node = Node::new(cfg, lease_mgr, indexer);

    let repo = RemoteRepo {
        name: opts
            .remote_name
            .or_else(|| std::env::var("REMOTE_REPO_NAME").ok())
            .unwrap_or_else(|| "demo".into()),
        git_url: opts
            .remote_url
            .or_else(|| std::env::var("REMOTE_REPO_URL").ok())
            .unwrap_or_else(|| "/tmp/demo".into()),
    };

    node.add_remote(repo);

    // run the node loop for 60s in demo mode
    node.run_for(Duration::from_secs(60)).await?;
    Ok(())
}
