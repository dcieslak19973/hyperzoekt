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

use crate::LeaseManager;
use anyhow::Result;
use reqwest::Client;
use serde_json::json;
use std::time::Duration;
use tokio::time::timeout;

/// Tool definition for distributed search parameters
#[derive(serde::Deserialize, Clone)]
pub struct DistributedSearchTool {
    /// Regex pattern to search for (required)
    pub regex: String,
    /// Path patterns to include (optional, supports glob patterns like "src/**/*.rs")
    pub include: Option<String>,
    /// Path patterns to exclude (optional, supports glob patterns like "target/**")
    pub exclude: Option<String>,
    /// Repository to search in (optional, searches all if not specified)
    pub repo: Option<String>,
    /// Maximum number of results to return (optional, default 100)
    pub max_results: Option<usize>,
    /// Number of context lines around matches (optional, default 2)
    pub context: Option<usize>,
    /// Case sensitive search (optional, default false)
    pub case_sensitive: Option<bool>,
    /// GitHub username for authentication
    pub github_username: Option<String>,
    /// GitHub personal access token
    pub github_token: Option<String>,
    /// GitLab username for authentication
    pub gitlab_username: Option<String>,
    /// GitLab personal access token
    pub gitlab_token: Option<String>,
    /// BitBucket username for authentication
    pub bitbucket_username: Option<String>,
    /// BitBucket app password or token
    pub bitbucket_token: Option<String>,
}

/// Shared distributed search functionality
#[derive(Clone)]
pub struct DistributedSearchService {
    lease_mgr: LeaseManager,
    client: Client,
}

impl DistributedSearchService {
    pub fn new(lease_mgr: LeaseManager, client: Client) -> Self {
        Self { lease_mgr, client }
    }

    pub async fn execute_distributed_search_json(
        &self,
        params: DistributedSearchTool,
    ) -> Result<Vec<serde_json::Value>> {
        // Get indexer endpoints
        let endpoints = self.lease_mgr.get_indexer_endpoints().await;
        if endpoints.is_empty() {
            tracing::warn!("No indexer endpoints found in Redis - no indexers are registered");
            return Err(anyhow::anyhow!("no indexers available"));
        }

        tracing::info!(
            "Found {} indexer endpoints: {:?}",
            endpoints.len(),
            endpoints.keys().collect::<Vec<_>>()
        );

        // Build search query for indexers
        let max_results = params.max_results.unwrap_or(100);
        let context_lines = params.context.unwrap_or(2);
        let mut total_results = 0;
        let mut all_results = Vec::new();
        // Track a normalized fingerprint of results so we don't return duplicates when
        // multiple indexers respond with identical hits. We strip node-specific fields
        // and per-index counters so equivalent results dedupe reliably.
        let mut seen_keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Query all indexers in parallel
        let mut search_tasks = Vec::new();
        for (node_id, endpoint) in endpoints {
            let client_clone = self.client.clone();
            let params_clone = params.clone();
            let task = tokio::spawn(async move {
                tracing::debug!("Querying indexer {} at endpoint: {}", node_id, endpoint);
                Self::search_single_indexer(
                    client_clone,
                    params_clone,
                    endpoint,
                    node_id,
                    context_lines,
                )
                .await
            });
            search_tasks.push(task);
        }

        // Process results as they come in
        for task in search_tasks {
            match task.await {
                Ok(Ok(results)) => {
                    tracing::debug!("Indexer query succeeded, got {} results", results.len());
                    for result in results {
                        if total_results >= max_results {
                            break;
                        }

                        // Build a normalization key that ignores node-specific metadata and
                        // per-index values like 'doc' and 'score' that can differ across nodes.
                        let mut normalized = result.clone();
                        if let Some(obj) = normalized.as_object_mut() {
                            for k in [
                                "node_id",
                                "regex",
                                "context_lines",
                                "relative_path",
                                "doc",
                                "score",
                            ] {
                                obj.remove(k);
                            }
                        }
                        let key = normalized.to_string();
                        if seen_keys.insert(key) {
                            all_results.push(result);
                            total_results += 1;
                        } else {
                            tracing::debug!("dropping duplicate result from aggregated indexers");
                        }
                    }
                }
                Ok(Err(error)) => {
                    tracing::error!("Indexer query failed: {}", error);
                    all_results.push(json!({
                        "error": format!("Error from indexer: {}", error)
                    }));
                }
                Err(e) => {
                    tracing::error!("Indexer task panicked: {}", e);
                    all_results.push(json!({
                        "error": format!("Task error: {}", e)
                    }));
                }
            }

            if total_results >= max_results {
                break;
            }
        }

        // Don't add summary here - let the caller handle it
        // The summary will be added in the MCP handler for better formatting

        Ok(all_results)
    }

    async fn search_single_indexer(
        client: Client,
        params: DistributedSearchTool,
        endpoint: String,
        node_id: String,
        context_lines: usize,
    ) -> Result<Vec<serde_json::Value>, String> {
        // Build the search query for the indexer
        let mut query_parts = vec![params.regex.clone()];

        // Add path filters
        if let Some(include) = &params.include {
            query_parts.push(format!("file:{}", include));
        }

        if let Some(exclude) = &params.exclude {
            query_parts.push(format!("-file:{}", exclude));
        }

        let query = query_parts.join(" ");

        // Build the search URL
        let mut url = format!(
            "{}/search?q={}&context_lines={}&repo={}",
            endpoint,
            urlencoding::encode(&query),
            context_lines,
            urlencoding::encode("*") // Use wildcard to search all repos
        );

        // Add case sensitivity
        if params.case_sensitive.unwrap_or(false) {
            url.push_str("&case=yes");
        }

        // Make the request with timeout
        let response = timeout(Duration::from_secs(30), client.get(&url).send())
            .await
            .map_err(|_| "request timeout".to_string())?
            .map_err(|e| format!("request error: {}", e))?;

        tracing::debug!(
            "Indexer {} request to {} returned status: {}",
            node_id,
            url,
            response.status()
        );

        if !response.status().is_success() {
            return Err(format!("search failed with status: {}", response.status()));
        }

        // Parse the response
        let json_response: serde_json::Value = response
            .json()
            .await
            .map_err(|e| format!("json parse error: {}", e))?;

        let mut results = Vec::new();

        if let Some(search_results) = json_response.get("results").and_then(|r| r.as_array()) {
            for result in search_results {
                let mut search_result = result.clone();

                // Add metadata fields
                if let Some(obj) = search_result.as_object_mut() {
                    obj.insert("node_id".to_string(), json!(node_id));
                    obj.insert("regex".to_string(), json!(params.regex));
                    obj.insert("context_lines".to_string(), json!(context_lines));

                    // Add file path information
                    if let Some(file_path) = obj.get("file").and_then(|f| f.as_str()) {
                        obj.insert("relative_path".to_string(), json!(file_path));
                    }
                }

                results.push(search_result);
            }
        }

        Ok(results)
    }
}
