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

use crate::lease_manager::{LeaseManager, RemoteRepo};
use crate::NodeConfig;
use anyhow::Result;
use std::collections::HashSet;
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

    /// Remove an index from memory by repo URL/path key
    fn remove_index(&self, repo_key: &str);

    /// Get all repo keys currently in memory
    fn get_indexed_repos(&self) -> Vec<String>;
}

/// Simple Node that periodically tries to acquire leases for known remote repos and then
/// calls the provided indexer to index them.
pub struct Node<I: Indexer> {
    config: NodeConfig,
    lease_mgr: LeaseManager,
    repos: Arc<parking_lot::RwLock<Vec<RemoteRepo>>>,
    indexer: Arc<I>,
    // track repos that have been indexed once when index_once is enabled
    seen_once: Arc<parking_lot::RwLock<HashSet<String>>>,
    // track the last known commit SHA for remote repositories to avoid unnecessary re-indexing
    last_known_sha: Arc<parking_lot::RwLock<std::collections::HashMap<String, String>>>,
}

impl<I: Indexer> Node<I> {
    pub fn new(config: NodeConfig, lease_mgr: LeaseManager, indexer: I) -> Self {
        Self {
            config,
            lease_mgr,
            repos: Arc::new(parking_lot::RwLock::new(Vec::new())),
            indexer: Arc::new(indexer),
            seen_once: Arc::new(parking_lot::RwLock::new(HashSet::new())),
            last_known_sha: Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Expose the internal registered repos list so external listeners (HTTP handlers)
    /// can observe repos that the node dynamically registers at runtime.
    pub fn registered_repos(&self) -> std::sync::Arc<parking_lot::RwLock<Vec<RemoteRepo>>> {
        self.repos.clone()
    }

    pub fn add_remote(&self, repo: RemoteRepo) {
        let mut repos = self.repos.write();
        // Check if this repo is already in the list
        let exists = repos.iter().any(|existing| {
            existing.name == repo.name
                && existing.git_url == repo.git_url
                && existing.branch == repo.branch
        });
        if !exists {
            repos.push(repo);
        }
    }

    /// Check if a remote repository should be skipped based on SHA comparison
    fn should_skip_remote_indexing(&self, repo: &RemoteRepo, current_sha: Option<&str>) -> bool {
        if let Some(current_sha) = current_sha {
            let repo_key = format!("{}|{}", repo.name, repo.branch.as_deref().unwrap_or(""));
            let last_sha = self.last_known_sha.read().get(&repo_key).cloned();

            if let Some(last_sha) = last_sha {
                if last_sha == current_sha {
                    tracing::info!(
                        repo=%repo.name,
                        branch=?repo.branch,
                        sha=%current_sha,
                        "skipping indexing: SHA unchanged"
                    );
                    return true;
                } else {
                    tracing::info!(
                        repo=%repo.name,
                        branch=?repo.branch,
                        last_sha=%last_sha,
                        current_sha=%current_sha,
                        "SHA changed, will re-index"
                    );
                }
            } else {
                tracing::info!(
                    repo=%repo.name,
                    branch=?repo.branch,
                    current_sha=%current_sha,
                    "first time indexing repository"
                );
            }
        } else {
            tracing::warn!(
                repo=%repo.name,
                branch=?repo.branch,
                "could not determine current SHA, will index anyway"
            );
        }
        false
    }

    /// Update the last known SHA for a repository after successful indexing
    fn update_last_known_sha(&self, repo: &RemoteRepo, sha: &str) {
        let repo_key = format!("{}|{}", repo.name, repo.branch.as_deref().unwrap_or(""));
        self.last_known_sha
            .write()
            .insert(repo_key, sha.to_string());
        tracing::debug!(
            repo=%repo.name,
            branch=?repo.branch,
            sha=%sha,
            "updated last known SHA"
        );
    }

    /// Perform heartbeat maintenance tasks for indexers
    async fn perform_heartbeat_maintenance(&self) {
        tracing::debug!(node_id=%self.config.id, "performing heartbeat maintenance");

        // 1. Check for branches in memory whose parent repos no longer exist
        let indexed_repos = self.indexer.get_indexed_repos();
        tracing::debug!(node_id=%self.config.id, indexed_repos_count=%indexed_repos.len(), "checking for stale indexes");

        for repo_key in &indexed_repos {
            // Try to determine repo name and branch from the key
            // The key format could be a URL or path, we need to find matching registered repos
            let registered_branches = self.lease_mgr.get_repo_branches().await;

            // Check if this repo key matches any registered branch
            let mut found_registered = false;
            for (_repo_name, _branch, url) in &registered_branches {
                // Check if the repo_key matches the URL or is a path that corresponds to it
                if repo_key == url || repo_key.contains(url) || url.contains(repo_key) {
                    found_registered = true;
                    break;
                }
            }

            if !found_registered {
                tracing::info!(node_id=%self.config.id, repo=%repo_key, "removing index for unregistered repo");
                self.indexer.remove_index(repo_key);
            }
        }

        // 2. Renew existing leases held by this indexer
        let registered_branches = self.lease_mgr.get_repo_branches().await;
        tracing::debug!(node_id=%self.config.id, registered_branches_count=%registered_branches.len(), "renewing existing leases");

        for (repo_name, branch, url) in &registered_branches {
            let repo = RemoteRepo {
                name: repo_name.clone(),
                git_url: url.clone(),
                branch: Some(branch.clone()),
                visibility: zoekt_rs::types::RepoVisibility::Public,
                owner: None,
                allowed_users: Vec::new(),
                last_commit_sha: None,
            };

            // Check if we hold this lease
            if let Some(holder) = self.lease_mgr.get_current_lease_holder(&repo).await {
                if holder == self.config.id {
                    // We hold this lease, renew it
                    if self
                        .lease_mgr
                        .renew(&repo, self.config.id.clone(), self.config.lease_ttl)
                        .await
                    {
                        tracing::debug!(node_id=%self.config.id, repo=%repo_name, branch=%branch, "renewed lease during heartbeat");
                    } else {
                        tracing::warn!(node_id=%self.config.id, repo=%repo_name, branch=%branch, "failed to renew lease during heartbeat");
                    }
                }
            }
        }

        // 3. Check for unleased branches that we can bid on
        let unleased_branches = self.lease_mgr.get_unleased_branches().await;
        tracing::debug!(node_id=%self.config.id, unleased_branches_count=%unleased_branches.len(), "checking for available leases");

        // Track how many pending bids we have
        let mut pending_bids = 0;

        for (repo_name, branch, url) in unleased_branches {
            // Check if we already have a pending bid for this repo
            let repo = RemoteRepo {
                name: repo_name.clone(),
                git_url: url.clone(),
                branch: Some(branch.clone()),
                visibility: zoekt_rs::types::RepoVisibility::Public,
                owner: None,
                allowed_users: Vec::new(),
                last_commit_sha: None,
            };

            // Check if we already hold this lease
            if let Some(holder) = self.lease_mgr.get_current_lease_holder(&repo).await {
                if holder == self.config.id {
                    tracing::debug!(node_id=%self.config.id, repo=%repo_name, branch=%branch, "already hold lease, skipping bid");
                    continue;
                }
            }

            // Only bid if we don't have too many pending bids
            if pending_bids >= 1 {
                tracing::debug!(node_id=%self.config.id, "maximum pending bids reached, skipping additional bids");
                break;
            }

            // Try to acquire the lease
            if self
                .lease_mgr
                .try_acquire(&repo, self.config.id.clone(), self.config.lease_ttl)
                .await
            {
                tracing::info!(node_id=%self.config.id, repo=%repo_name, branch=%branch, "acquired lease during heartbeat");

                // Get current commit SHA for this remote repository
                let current_sha = get_current_commit_sha(&repo.git_url).await;

                // Check if we should skip indexing based on SHA
                if self.should_skip_remote_indexing(&repo, current_sha.as_deref()) {
                    // Release the lease since we're not going to index
                    self.lease_mgr.release(&repo, &self.config.id).await;
                    continue;
                }

                pending_bids += 1;

                // Add to our repos list so it gets processed in the main loop
                self.add_remote(repo);
            } else {
                tracing::debug!(node_id=%self.config.id, repo=%repo_name, branch=%branch, "failed to acquire lease during heartbeat");
            }
        }

        tracing::debug!(node_id=%self.config.id, pending_bids=%pending_bids, "heartbeat maintenance complete");
    }

    /// Run the node loop for `duration`. Async version for use in async apps/tests.
    pub async fn run_for(&self, duration: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        // If this is an indexer with an endpoint, register it
        if self.config.node_type == crate::NodeType::Indexer {
            if let Some(ref endpoint) = self.config.endpoint {
                tracing::info!(node_id = %self.config.id, endpoint = %endpoint, "registering indexer endpoint on startup");
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
                visibility: zoekt_rs::types::RepoVisibility::Public, // Default to public
                owner: None,
                allowed_users: Vec::new(),
                last_commit_sha: None,
            };
            tracing::info!(repo=%name, branch=%branch, "registering branch-specific RemoteRepo");
            self.add_remote(rr);
        }
        // If reindexing is disabled, perform a single initial pass for local repos so
        // developers still get an index on startup, but skip periodic re-index cycles.
        if !self.config.enable_reindex {
            let repos = self.repos.read().clone();
            for repo in repos {
                // Only index local paths during the initial pass
                let git_url = repo.git_url.clone();
                let is_local = PathBuf::from(&git_url).is_absolute()
                    || git_url.starts_with("file://")
                    || is_current_repo(&git_url).await;
                if !is_local {
                    continue;
                }

                // Honor index_once: skip if already seen
                if self.config.index_once {
                    let key = format!("{}|{}", repo.name, repo.branch.clone().unwrap_or_default());
                    if self.seen_once.read().contains(&key) {
                        tracing::debug!(repo=%repo.name, branch=?repo.branch, "index_once: already indexed (initial pass), skipping");
                        continue;
                    }
                }

                tracing::info!(repo = %repo.name, "initial local index (reindex disabled)");
                // Log ownership/permission bits for debugging prior to indexing.
                log_path_permissions(&repo.git_url);

                let repo_for_index = repo.clone();
                let indexer = self.indexer.clone();

                let index_started = std::time::Instant::now();
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
                        tracing::info!(repo = %repo.name, docs = index.doc_count(), "indexed (initial)");
                        let dur = index_started.elapsed();
                        let now_ms = chrono::Utc::now().timestamp_millis();
                        let dur_ms = dur.as_millis() as i64;
                        let mem_est = index.total_scanned_bytes() as i64;
                        tracing::debug!(repo=%repo.name, "about to set branch meta (local, initial). repo-level meta writes are disabled");
                        if let Some(branch) = &repo.branch {
                            let _ = self
                                .lease_mgr
                                .set_branch_meta(&repo.name, branch, now_ms, dur_ms, mem_est)
                                .await;
                        } else {
                            tracing::debug!(repo=%repo.name, "skipping repo-level meta write for legacy/unspecified branch");
                        }
                        if self.config.index_once {
                            let key = format!(
                                "{}|{}",
                                repo.name,
                                repo.branch.clone().unwrap_or_default()
                            );
                            self.seen_once.write().insert(key);
                            tracing::info!(repo=%repo.name, "index_once: marked as indexed (initial)");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(repo = %repo.name, error = %e, "failed to index (initial)");
                    }
                };
            }
        }
        while start.elapsed() < duration {
            // Periodically update endpoint if indexer
            if self.config.node_type == crate::NodeType::Indexer && self.config.endpoint.is_some() {
                let heartbeat_start = std::time::Instant::now();
                tracing::info!(node_id=%self.config.id, endpoint=%self.config.endpoint.as_ref().unwrap(), "sending indexer heartbeat to Redis");

                self.lease_mgr
                    .set_indexer_endpoint(&self.config.id, self.config.endpoint.as_ref().unwrap())
                    .await;

                // Perform heartbeat maintenance tasks
                self.perform_heartbeat_maintenance().await;

                let heartbeat_duration = heartbeat_start.elapsed();
                tracing::info!(
                    node_id=%self.config.id,
                    duration_ms=%heartbeat_duration.as_millis(),
                    "indexer heartbeat cycle completed"
                );
            }
            let repos = self.repos.read().clone();
            for repo in repos {
                // Check if we should skip indexing based on permissions (always check permissions)
                if !self.lease_mgr.should_skip_indexing(&repo.git_url, "").await {
                    // Get current commit SHA for tracking
                    let current_sha = get_current_commit_sha(&repo.git_url).await;

                    // Update repository metadata in SurrealDB
                    self.lease_mgr
                        .update_repo_metadata(&repo, current_sha)
                        .await;
                }

                // If index_once is enabled and we've already indexed this repo, skip it
                if self.config.index_once {
                    let key = format!("{}|{}", repo.name, repo.branch.clone().unwrap_or_default());
                    if self.seen_once.read().contains(&key) {
                        tracing::debug!(repo=%repo.name, branch=?repo.branch, "index_once: already indexed, skipping");
                        continue;
                    }
                }
                // If the repo git_url points to a local path (absolute path or file://) we skip
                // the lease acquisition and index directly. This makes local dev workflows fast
                // and avoids requiring Redis/leases for local repos.
                let git_url = repo.git_url.clone();
                let is_local = PathBuf::from(&git_url).is_absolute()
                    || git_url.starts_with("file://")
                    || is_current_repo(&git_url).await;

                if is_local {
                    tracing::info!(repo = %repo.name, "local repo, indexing without lease");
                    if !self.config.enable_reindex {
                        tracing::info!(repo=%repo.name, "reindexing disabled by config; skipping local index");
                        continue;
                    }
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
                                    .set_branch_meta(&repo.name, branch, now_ms, dur_ms, mem_est)
                                    .await;
                            } else {
                                tracing::debug!(repo=%repo.name, "skipping repo-level meta write for legacy/unspecified branch");
                            }
                            if self.config.index_once {
                                let key = format!(
                                    "{}|{}",
                                    repo.name,
                                    repo.branch.clone().unwrap_or_default()
                                );
                                self.seen_once.write().insert(key);
                                tracing::info!(repo=%repo.name, "index_once: marked as indexed");
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

                if !self.config.enable_reindex {
                    tracing::debug!(repo=%repo.name, "reindexing disabled; skipping remote index attempt");
                    continue;
                }

                // Check if we already hold this lease
                let current_holder = self.lease_mgr.get_current_lease_holder(&repo).await;
                tracing::debug!(node_id=%self.config.id, repo=%repo.name, branch=?repo.branch, current_holder=?current_holder, "checking current lease holder");
                if let Some(holder) = current_holder {
                    if holder == self.config.id {
                        tracing::debug!(node_id=%self.config.id, repo=%repo.name, branch=?repo.branch, "already hold lease, proceeding with indexing");

                        // Get current commit SHA for this remote repository
                        let current_sha = get_current_commit_sha(&repo.git_url).await;

                        // Check if we should skip indexing based on SHA
                        if self.should_skip_remote_indexing(&repo, current_sha.as_deref()) {
                            // Don't release the lease since we already hold it
                            continue;
                        }

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

                                // Update the last known SHA after successful indexing
                                if let Some(ref sha) = current_sha {
                                    self.update_last_known_sha(&repo, sha);
                                }

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
                                            &repo.name, branch, now_ms, dur_ms, mem_est,
                                        )
                                        .await;
                                } else {
                                    tracing::debug!(repo=%repo.name, "skipping repo-level meta write for legacy/unspecified branch");
                                }
                                if self.config.index_once {
                                    let key = format!(
                                        "{}|{}",
                                        repo.name,
                                        repo.branch.clone().unwrap_or_default()
                                    );
                                    self.seen_once.write().insert(key);
                                    tracing::info!(repo=%repo.name, "index_once: marked as indexed");
                                }
                            }
                            Err(e) => {
                                tracing::warn!(repo = %repo.name, error = %e, "failed to index");
                                // Don't release the lease since we already held it
                            }
                        }
                        continue;
                    }
                }

                if self
                    .lease_mgr
                    .try_acquire(&repo, self.config.id.clone(), self.config.lease_ttl)
                    .await
                {
                    tracing::info!(repo = %repo.name, "acquired lease, indexing");

                    // Get current commit SHA for this remote repository
                    let current_sha = get_current_commit_sha(&repo.git_url).await;

                    // Check if we should skip indexing based on SHA
                    if self.should_skip_remote_indexing(&repo, current_sha.as_deref()) {
                        // Release the lease since we're not going to index
                        self.lease_mgr.release(&repo, &self.config.id).await;
                        continue;
                    }

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

                            // Update the last known SHA after successful indexing
                            if let Some(ref sha) = current_sha {
                                self.update_last_known_sha(&repo, sha);
                            }

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
                                    .set_branch_meta(&repo.name, branch, now_ms, dur_ms, mem_est)
                                    .await;
                            } else {
                                tracing::debug!(repo=%repo.name, "skipping repo-level meta write for legacy/unspecified branch");
                            }
                            if self.config.index_once {
                                let key = format!(
                                    "{}|{}",
                                    repo.name,
                                    repo.branch.clone().unwrap_or_default()
                                );
                                self.seen_once.write().insert(key);
                                tracing::info!(repo=%repo.name, "index_once: marked as indexed");
                            }
                        }
                        Err(e) => {
                            tracing::warn!(repo = %repo.name, error = %e, "failed to index");
                            self.lease_mgr.release(&repo, &self.config.id).await;
                        }
                    }
                } else {
                    tracing::debug!(node_id=%self.config.id, repo=%repo.name, branch=?repo.branch, "lease acquisition failed, skipping indexing");
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

/// Check if the given git_url matches the current repository's remote URL
async fn is_current_repo(git_url: &str) -> bool {
    // Try to get the remote URL of the current directory
    match std::process::Command::new("git")
        .args(["remote", "get-url", "origin"])
        .current_dir(".")
        .output()
    {
        Ok(output) if output.status.success() => {
            let remote_url = String::from_utf8_lossy(&output.stdout).trim().to_string();
            // Normalize URLs by removing trailing .git and comparing
            let normalized_git_url = git_url.trim_end_matches(".git");
            let normalized_remote = remote_url.trim_end_matches(".git");
            if normalized_git_url == normalized_remote {
                tracing::debug!(git_url=%git_url, remote_url=%remote_url, "detected current repo, treating as local");
                return true;
            }
        }
        _ => {
            // If we can't get the remote URL, assume it's not the current repo
        }
    }
    false
}

/// Get the current commit SHA for a repository
async fn get_current_commit_sha(git_url: &str) -> Option<String> {
    // For local repos, try to get the HEAD commit SHA
    if std::path::Path::new(git_url).is_absolute() || git_url.starts_with("file://") {
        let repo_path = if git_url.starts_with("file://") {
            git_url.strip_prefix("file://").unwrap_or(git_url)
        } else {
            git_url
        };

        match std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(repo_path)
            .output()
        {
            Ok(output) if output.status.success() => {
                let sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
                Some(sha)
            }
            _ => {
                tracing::debug!(repo=%git_url, "failed to get commit SHA for local repo");
                None
            }
        }
    } else {
        // For remote repos, use git ls-remote to get the HEAD commit SHA
        match tokio::process::Command::new("git")
            .args(["ls-remote", git_url, "HEAD"])
            .output()
            .await
        {
            Ok(output) if output.status.success() => {
                let output_str = String::from_utf8_lossy(&output.stdout);
                // git ls-remote output format: <sha>\t<ref>
                // We want the SHA from the first line
                if let Some(first_line) = output_str.lines().next() {
                    if let Some(sha) = first_line.split('\t').next() {
                        let sha = sha.trim().to_string();
                        if !sha.is_empty() {
                            tracing::debug!(repo=%git_url, sha=%sha, "got remote commit SHA");
                            return Some(sha);
                        }
                    }
                }
                tracing::debug!(repo=%git_url, "failed to parse SHA from git ls-remote output");
                None
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::debug!(repo=%git_url, status=%output.status, stderr=%stderr, "git ls-remote failed");
                None
            }
            Err(e) => {
                tracing::debug!(repo=%git_url, error=%e, "failed to run git ls-remote");
                None
            }
        }
    }
}
