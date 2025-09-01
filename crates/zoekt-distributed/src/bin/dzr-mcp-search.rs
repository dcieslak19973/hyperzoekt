use anyhow::Result;
use clap::Parser;
use std::time::Duration;

use zoekt_distributed::{LeaseManager, NodeConfig, NodeType};

#[derive(Parser)]
struct Opts {
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    #[arg(long)]
    id: Option<String>,
    #[arg(long)]
    lease_ttl_seconds: Option<u64>,
    #[arg(long)]
    poll_interval_seconds: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opts = Opts::parse();

    let _cfg = zoekt_distributed::load_node_config(
        NodeConfig {
            node_type: NodeType::Search,
            ..Default::default()
        },
        zoekt_distributed::MergeOpts {
            config_path: opts.config,
            cli_id: opts.id,
            cli_lease_ttl_seconds: opts.lease_ttl_seconds,
            cli_poll_interval_seconds: opts.poll_interval_seconds,
            cli_endpoint: None,
        },
    )?;

    let _lease_mgr = LeaseManager::new().await;

    // MCP search placeholder - implement MCP protocol for distributed search
    tracing::info!("started mcp search node");
    tokio::time::sleep(Duration::from_secs(5)).await;
    Ok(())
}
