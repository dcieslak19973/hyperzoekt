use crate::lease_manager::{LeaseManager, RemoteRepo};
use crate::NodeConfig;
use anyhow::Result;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use zoekt_rs::InMemoryIndex;

/// The Indexer trait abstracts building/holding an index for a repo. Tests can provide
/// a simple implementation that uses `zoekt_rs::build_in_memory_index`.
pub trait Indexer: Send + Sync + 'static {
    /// Build/index the repo at `repo_path` and return some index handle. For the prototype,
    /// we return an `InMemoryIndex` from `zoekt_rs`.
    fn index_repo(&self, repo_path: PathBuf) -> Result<InMemoryIndex>;
}

/// Simple Node that periodically tries to acquire leases for known remote repos and then
/// calls the provided indexer to index them.
pub struct Node<I: Indexer> {
    config: NodeConfig,
    lease_mgr: LeaseManager,
    repos: Arc<parking_lot::RwLock<Vec<RemoteRepo>>>,
    indexer: Arc<I>,
}

impl<I: Indexer> Node<I> {
    pub fn new(config: NodeConfig, lease_mgr: LeaseManager, indexer: I) -> Self {
        Self {
            config,
            lease_mgr,
            repos: Arc::new(parking_lot::RwLock::new(Vec::new())),
            indexer: Arc::new(indexer),
        }
    }

    pub fn add_remote(&self, repo: RemoteRepo) {
        self.repos.write().push(repo);
    }

    /// Run the node loop for `duration`. Async version for use in async apps/tests.
    pub async fn run_for(&self, duration: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        // If this is an indexer with an endpoint, register it
        if self.config.node_type == crate::NodeType::Indexer {
            if let Some(ref endpoint) = self.config.endpoint {
                tracing::info!(node_id = %self.config.id, endpoint = %endpoint, "registering indexer endpoint");
                let _ = self
                    .lease_mgr
                    .set_indexer_endpoint(&self.config.id, endpoint)
                    .await;
            }
        }
        // Load branch-specific work units from Redis (if configured) and register them.
        // This allows admin to expand branch wildcards into concrete RemoteRepo entries
        // stored in `zoekt:repo_branches`.
        let branch_entries = self.lease_mgr.get_repo_branches().await;
        for (name, branch, url) in branch_entries {
            let rr = RemoteRepo {
                name: name.clone(),
                git_url: url.clone(),
                branch: Some(branch.clone()),
            };
            tracing::info!(repo=%name, branch=%branch, "registering branch-specific RemoteRepo");
            self.add_remote(rr);
        }
        while start.elapsed() < duration {
            // Periodically update endpoint if indexer
            if self.config.node_type == crate::NodeType::Indexer && self.config.endpoint.is_some() {
                let _ = self
                    .lease_mgr
                    .set_indexer_endpoint(&self.config.id, self.config.endpoint.as_ref().unwrap())
                    .await;
            }
            let repos = self.repos.read().clone();
            for repo in repos {
                // If the repo git_url points to a local path (absolute path or file://) we skip
                // the lease acquisition and index directly. This makes local dev workflows fast
                // and avoids requiring Redis/leases for local repos.
                let git_url = repo.git_url.clone();
                let is_local =
                    PathBuf::from(&git_url).is_absolute() || git_url.starts_with("file://");

                if is_local {
                    tracing::info!(repo = %repo.name, "local repo, indexing without lease");
                    let repo_for_index = repo.clone();
                    let indexer = self.indexer.clone();

                    // Log ownership/permission bits for debugging prior to indexing.
                    log_path_permissions(&repo_for_index.git_url);

                    let index_started = std::time::Instant::now();
                    // run the synchronous indexer in a blocking task
                    let index_result = match tokio::task::spawn_blocking(move || {
                        indexer.index_repo(PathBuf::from(&repo_for_index.git_url))
                    })
                    .await
                    {
                        Ok(r) => r,
                        Err(e) => Err(anyhow::anyhow!(e.to_string())),
                    };

                    match index_result {
                        Ok(index) => {
                            tracing::info!(repo = %repo.name, docs = index.doc_count(), "indexed");
                            let dur = index_started.elapsed();
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            let dur_ms = dur.as_millis() as i64;
                            let mem_est = index.total_scanned_bytes() as i64;
                            // best-effort: persist meta. We only write branch-specific meta now.
                            tracing::debug!(repo=%repo.name, "about to set branch meta (local). repo-level meta writes are disabled");
                            if let Some(branch) = &repo.branch {
                                let _ = self
                                    .lease_mgr
                                    .set_branch_meta(
                                        &repo.name,
                                        branch,
                                        now_ms,
                                        dur_ms,
                                        mem_est,
                                        &self.config.id,
                                    )
                                    .await;
                            } else {
                                tracing::debug!(repo=%repo.name, "skipping repo-level meta write for legacy/unspecified branch");
                            }
                        }
                        Err(e) => {
                            tracing::warn!(repo = %repo.name, error = %e, "failed to index");
                        }
                    }
                    continue;
                }

                // For remote (non-local) repos, also log path metadata before attempting to acquire a lease
                // so permission/ownership issues are visible in logs.
                log_path_permissions(&git_url);

                if self
                    .lease_mgr
                    .try_acquire(&repo, self.config.id.clone(), self.config.lease_ttl)
                    .await
                {
                    tracing::info!(repo = %repo.name, "acquired lease, indexing");
                    // Run indexing in a blocking task (indexer is synchronous). While indexing
                    // runs, periodically renew the lease so long-running indexing doesn't expire it.
                    let repo_for_index = repo.clone();
                    let holder = self.config.id.clone();
                    let lease_mgr = self.lease_mgr.clone();
                    let indexer = self.indexer.clone();

                    let index_started = std::time::Instant::now();
                    let mut index_handle = tokio::task::spawn_blocking(move || {
                        indexer.index_repo(PathBuf::from(&repo_for_index.git_url))
                    });

                    // Renewal loop: wake up every (ttl/3) seconds to renew; stop when indexing completes.
                    let renew_interval = std::cmp::max(1, self.config.lease_ttl.as_secs() / 3);
                    let mut renew_tick =
                        tokio::time::interval(tokio::time::Duration::from_secs(renew_interval));
                    renew_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

                    let mut indexing_done = false;
                    let mut index_result = Err(anyhow::anyhow!("indexing not run"));

                    while !indexing_done {
                        tokio::select! {
                            biased;
                            _ = renew_tick.tick() => {
                                // try renew; if renewal fails we stop renewing (lease may be lost)
                                let _ = lease_mgr.renew(&repo, holder.clone(), self.config.lease_ttl).await;
                            }
                            res = &mut index_handle => {
                                indexing_done = true;
                                match res {
                                    Ok(r) => index_result = r,
                                    Err(e) => index_result = Err(anyhow::anyhow!(e.to_string())),
                                }
                            }
                        }
                    }

                    match index_result {
                        Ok(index) => {
                            tracing::info!(repo = %repo.name, docs = index.doc_count(), "indexed");
                            // compute duration and record meta in redis
                            let dur = index_started.elapsed();
                            let now_ms = chrono::Utc::now().timestamp_millis();
                            let dur_ms = dur.as_millis() as i64;
                            // best-effort: persist meta. We only write branch-specific meta now.
                            let mem_est = index.total_scanned_bytes() as i64;
                            tracing::debug!(repo=%repo.name, "about to set branch meta (remote). repo-level meta writes are disabled");
                            if let Some(branch) = &repo.branch {
                                let _ = self
                                    .lease_mgr
                                    .set_branch_meta(
                                        &repo.name,
                                        branch,
                                        now_ms,
                                        dur_ms,
                                        mem_est,
                                        &self.config.id,
                                    )
                                    .await;
                            } else {
                                tracing::debug!(repo=%repo.name, "skipping repo-level meta write for legacy/unspecified branch");
                            }
                        }
                        Err(e) => {
                            tracing::warn!(repo = %repo.name, error = %e, "failed to index");
                            self.lease_mgr.release(&repo, &self.config.id).await;
                        }
                    }
                }
            }
            tokio::time::sleep(self.config.poll_interval).await;
        }
        Ok(())
    }
}

/// Log basic ownership and permission bits for the provided path for debugging.
fn log_path_permissions(path: &str) {
    let p = PathBuf::from(path);
    // Only attempt to stat when the path looks like a local filesystem path.
    // Remote git URLs (http(s)://, git@, ssh://) will not be statted.
    let is_remote = looks_like_remote_git_url(path);
    let is_file_scheme = path.starts_with("file://");

    if is_remote && !is_file_scheme {
        tracing::debug!(path = %path, "skipping stat for remote git URL");
        return;
    }

    match std::fs::metadata(&p) {
        Ok(md) => {
            let mode = md.permissions().mode();
            let uid = md.uid();
            let gid = md.gid();
            tracing::info!(path = %path, uid = uid, gid = gid, mode = format_args!("{:o}", mode), "path metadata");
        }
        Err(e) => {
            tracing::warn!(path = %path, error = %e, "failed to stat path");
        }
    }
}

// Lightweight helper used by `log_path_permissions` and unit tests.
pub(crate) fn looks_like_remote_git_url(path: &str) -> bool {
    path.starts_with("http://")
        || path.starts_with("https://")
        || path.starts_with("git@")
        || path.starts_with("ssh://")
}
