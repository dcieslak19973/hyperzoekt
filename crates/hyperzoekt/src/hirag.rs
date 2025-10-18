// Minimal HiRAG-style hierarchical indexing + retrieval scaffolding for HyperZoekt
// Inspired by: "Retrieval-Augmented Generation with Hierarchical Knowledge (HiRAG)"
// Paper: https://arxiv.org/html/2503.10150v3
// This file provides a conservative, local-first implementation that is
// intentionally small and Rust-native. It is not a full HiRAG port — instead
// it offers the integration points to run hierarchical clustering on entity
// embeddings stored in SurrealDB, persist simple cluster summaries, and
// retrieve a three-level context package for a query or target entity.
//
// TODO: replace kmeans stub with GMM, add LLM-based summary generation, and add
// bridge-level shortest-path extraction. This module keeps all IO through the
// existing `db::SurrealConnection` abstraction so it integrates with the repo.

use anyhow::Result;
// Deterministic selection used for initial centers to avoid pulling RNG traits
use serde::{Deserialize, Serialize};

use crate::db::{helpers::normalize_git_url, SurrealConnection};
use log::{info, warn};
use serde_json::Value as JsonValue;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::OnceLock;

// Small in-memory buffer to hold the most recent CREATE responses for debug.
static CREATE_RESPONSES: OnceLock<Mutex<VecDeque<JsonValue>>> = OnceLock::new();
fn create_responses() -> &'static Mutex<VecDeque<JsonValue>> {
    CREATE_RESPONSES.get_or_init(|| Mutex::new(VecDeque::with_capacity(100)))
}

/// Record a CREATE response JSON value (keeps last 100 entries)
pub fn record_create_response(v: JsonValue) {
    let m = create_responses();
    if let Ok(mut guard) = m.lock() {
        let dq = &mut *guard;
        if dq.len() >= 100 {
            dq.pop_front();
        }
        dq.push_back(v);
    }
}

/// Return up to `n` most recent CREATE response JSON values (newest first)
pub fn get_create_responses(n: usize) -> Vec<JsonValue> {
    let m = create_responses();
    if let Ok(guard) = m.lock() {
        let dq = &*guard;
        return dq.iter().rev().take(n).cloned().collect();
    }
    Vec::new()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterSummary {
    pub id: String,
    pub label: String,
    pub summary: String,
    pub members: Vec<String>,
    /// Centroid embedding for the persisted cluster (normalized). Empty when unknown.
    pub centroid: Vec<f32>,
    /// Length of the centroid embedding (0 when unknown)
    pub centroid_len: usize,
    /// Provenance: list of repo ids/names that contributed members to this cluster.
    pub member_repos: Vec<String>,
}

/// Persist relation edges for a cluster: link cluster -> entity_snapshot (has_member)
/// and cluster -> commit (has_commit). Also update the cluster row with a
/// `member_commits` array for easy access.
pub async fn persist_cluster_relations(
    conn: &SurrealConnection,
    cluster_id: &str,
    members: &[String],
) -> Result<()> {
    // Best-effort: define relation tables (ignore errors if they already exist)
    let _ = conn
        .query("DEFINE TABLE has_member TYPE RELATION FROM hirag_cluster TO entity_snapshot;")
        .await;
    let _ = conn
        .query("DEFINE TABLE has_commit TYPE RELATION FROM hirag_cluster TO commit;")
        .await;

    use std::collections::HashSet;
    let mut commit_set: HashSet<String> = HashSet::new();

    #[derive(serde::Deserialize)]
    struct ESRow {
        id: Option<surrealdb::sql::Thing>,
        sourcecontrol_commit: Option<String>,
    }

    for m in members.iter() {
        // If member looks like a HiRAG cluster id (persisted as `hirag::...`), relate to that cluster
        if m.starts_with("hirag::") {
            // Ensure a relation table exists for cluster->cluster relations
            let _ = conn
                .query(
                    "DEFINE TABLE has_member_cluster TYPE RELATION FROM hirag_cluster TO hirag_cluster;",
                )
                .await;
            // RELATE cluster -> has_member_cluster -> hirag_cluster:`member`
            let relate_sql = format!(
                "RELATE hirag_cluster:`{}` -> has_member_cluster -> hirag_cluster:`{}` RETURN AFTER;",
                cluster_id, m
            );
            if let Err(e) = conn.query(&relate_sql).await {
                warn!(
                    "failed to create cluster->cluster has_member_cluster relation {} -> {}: {}",
                    cluster_id, m, e
                );
            }

            // Try to gather member_commits from the referenced cluster (best-effort)
            let q = "SELECT member_commits FROM hirag_cluster WHERE stable_id = $sid LIMIT 1";
            let binds = vec![("sid", serde_json::Value::String(m.clone()))];
            match conn.query_with_binds(q, binds.clone()).await {
                Ok(mut resp) => {
                    #[derive(serde::Deserialize)]
                    struct CommRow {
                        member_commits: Option<Vec<String>>,
                    }
                    if let Ok(rows) = resp.take::<Vec<CommRow>>(0) {
                        if let Some(r) = rows.into_iter().next() {
                            if let Some(mut commits) = r.member_commits {
                                for commit_ref in commits.drain(..) {
                                    if !commit_ref.is_empty()
                                        && commit_set.insert(commit_ref.clone())
                                    {
                                        let relate_commit_sql = format!(
                                            "RELATE hirag_cluster:`{}` -> has_commit -> {} RETURN AFTER;",
                                            cluster_id, commit_ref
                                        );
                                        // best-effort
                                        let _ = conn.query(&relate_commit_sql).await;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    // not fatal
                    log::debug!(
                        "hirag: failed to fetch member_commits for cluster member {}: {}",
                        m,
                        e
                    );
                }
            }
            continue;
        }

        // Default: member is an entity stable_id -> relate to entity_snapshot
        let sel_sql =
            "SELECT id, sourcecontrol_commit FROM entity_snapshot WHERE stable_id = $sid LIMIT 1";
        let binds = vec![("sid", serde_json::Value::String(m.clone()))];
        match conn.query_with_binds(sel_sql, binds.clone()).await {
            Ok(mut resp) => {
                let rows: Vec<ESRow> = resp.take(0)?;
                if let Some(row) = rows.into_iter().next() {
                    if let Some(es_thing) = row.id {
                        let es_id = es_thing.to_string();
                        // RELATE cluster -> has_member -> entity_snapshot
                        let relate_sql = format!(
                            "RELATE hirag_cluster:`{}` -> has_member -> {} RETURN AFTER;",
                            cluster_id, es_id
                        );
                        conn.query(&relate_sql).await?;
                    }
                    if let Some(commit_ref) = row.sourcecontrol_commit {
                        let is_new = commit_set.insert(commit_ref.clone());
                        if !commit_ref.is_empty() && is_new {
                            // RELATE cluster -> has_commit -> commit
                            let relate_commit_sql = format!(
                                "RELATE hirag_cluster:`{}` -> has_commit -> {} RETURN AFTER;",
                                cluster_id, commit_ref
                            );
                            conn.query(&relate_commit_sql).await?;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("failed to fetch snapshot for cluster member {}: {}", m, e);
            }
        }
    }

    if !commit_set.is_empty() {
        let arr = serde_json::Value::Array(
            commit_set
                .into_iter()
                .map(serde_json::Value::String)
                .collect(),
        );
        let update_sql = format!(
            "UPDATE hirag_cluster:`{}` SET member_commits = $commits",
            cluster_id
        );
        let binds = vec![("commits", arr)];
        conn.query_with_binds(&update_sql, binds).await?;
    }

    Ok(())
}

/// Simple in-memory KMeans for small clusters. This is intentionally tiny and
/// uses cosine similarity as a distance proxy by normalizing vectors. For large
/// datasets you should replace this with a scalable implementation.
fn kmeans_assign(embeddings: &[(String, Vec<f32>)], k: usize, _seed: u64) -> Vec<usize> {
    if embeddings.is_empty() || k == 0 {
        return vec![];
    }
    let dim = embeddings[0].1.len();
    // pick first k embeddings as initial centers deterministically
    let mut centers: Vec<Vec<f32>> = Vec::new();
    for (_id, v) in embeddings.iter().take(k.min(embeddings.len())) {
        centers.push(v.clone());
    }

    // normalize centers
    for c in centers.iter_mut() {
        let n = c.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
        if n > 0.0 {
            for x in c.iter_mut() {
                *x = (*x as f64 / n) as f32;
            }
        }
    }

    let mut assignments = vec![0usize; embeddings.len()];
    for _iter in 0..10 {
        // assign
        for (i, (_id, emb)) in embeddings.iter().enumerate() {
            let mut best = 0usize;
            let mut best_score = f64::MIN;
            let mut normed = emb.clone();
            let nn = normed
                .iter()
                .map(|x| (*x as f64).powi(2))
                .sum::<f64>()
                .sqrt();
            if nn > 0.0 {
                for x in normed.iter_mut() {
                    *x = (*x as f64 / nn) as f32;
                }
            }
            for (ci, center) in centers.iter().enumerate() {
                let mut dot = 0f64;
                for (a, b) in center.iter().zip(normed.iter()) {
                    dot += (*a as f64) * (*b as f64);
                }
                if dot > best_score {
                    best_score = dot;
                    best = ci;
                }
            }
            assignments[i] = best;
        }
        // recompute centers
        let mut sums: Vec<Vec<f64>> = vec![vec![0.0f64; dim]; centers.len()];
        let mut counts: Vec<usize> = vec![0; centers.len()];
        for (i, (_id, emb)) in embeddings.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for (j, v) in emb.iter().enumerate() {
                sums[c][j] += *v as f64;
            }
        }
        for (ci, s) in sums.into_iter().enumerate() {
            if counts[ci] == 0 {
                continue;
            }
            let mut newc: Vec<f32> = s
                .into_iter()
                .map(|x| (x / (counts[ci] as f64)) as f32)
                .collect();
            // normalize
            let n = newc.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
            if n > 0.0 {
                for v in newc.iter_mut() {
                    *v = (*v as f64 / n) as f32;
                }
                centers[ci] = newc;
            }
        }
    }
    assignments
}

/// Compute cosine distance between two embeddings (1.0 - cosine_similarity).
/// Lower distance = more similar. Returns f64::MAX if vectors have different lengths or zero norm.
fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return f64::MAX;
    }

    let mut dot = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;

    for (x, y) in a.iter().zip(b.iter()) {
        let xd = *x as f64;
        let yd = *y as f64;
        dot += xd * yd;
        norm_a += xd * xd;
        norm_b += yd * yd;
    }

    let denom = (norm_a.sqrt()) * (norm_b.sqrt());
    if denom == 0.0 {
        return f64::MAX;
    }

    let cosine_sim = dot / denom;
    1.0 - cosine_sim // distance = 1 - similarity
}

/// Sort cluster members by distance to centroid (closest first).
/// Filters out members with no embeddings.
fn sort_members_by_centroid(
    members: &[String],
    centroid: &[f32],
    emb_map: &std::collections::HashMap<String, Vec<f32>>,
) -> Vec<String> {
    let mut scored: Vec<(String, f64)> = members
        .iter()
        .filter_map(|m| {
            emb_map.get(m).map(|emb| {
                let dist = cosine_distance(emb, centroid);
                (m.clone(), dist)
            })
        })
        .collect();

    // Sort by distance (ascending = closest first)
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    scored.into_iter().map(|(id, _)| id).collect()
}

/// Build a first-pass hierarchical layer over the entities stored in Surreal.
/// This will read `entity` rows that have embeddings, cluster them, and write
/// cluster summaries back into Surreal as `hirag_cluster` records (id,label,summary,members).
pub async fn build_first_layer(conn: &SurrealConnection, k: usize) -> Result<()> {
    // Fetch candidate entity stable_ids. For production we would page.
    // Embeddings are stored on `entity_snapshot` records (snapshot-level), directly
    // alongside source_content. This is the current data model after denormalization.
    let sql = "SELECT stable_id FROM entity LIMIT 5000";
    // Execute the typed query and log the typed rows for diagnostics
    #[derive(serde::Deserialize, serde::Serialize)]
    struct Row {
        stable_id: String,
    }
    let mut resp = conn.query(sql).await?;
    let rows: Vec<Row> = resp.take(0)?;
    info!("Found {} entities for clustering", rows.len());
    let mut embeddings: Vec<(String, Vec<f32>)> = Vec::new();
    // For small batches, fetch each embedding individually from entity_snapshot
    for r in rows.into_iter() {
        // Fetch embedding directly from entity_snapshot by stable_id
        if let Ok(Some(e)) = crate::db::helpers::get_embedding_for_entity(conn, &r.stable_id).await
        {
            if !e.is_empty() {
                embeddings.push((r.stable_id, e));
            }
        }
    }

    // Helper to clone Option<Vec<f32>> safely
    fn crowd_safe(opt: &Option<Vec<f32>>) -> Option<Vec<f32>> {
        opt.clone()
    }

    if embeddings.is_empty() {
        return Ok(());
    }

    // Allow callers to pass k == 0 to let the builder pick an "organic" k.
    // Heuristic: use sqrt(N) (rounded) with a minimum of 2 clusters, but
    // never exceed the number of available embeddings.
    let k_eff = if k == 0 {
        let n = embeddings.len();
        let mut calc = (n as f64).sqrt().round() as usize;
        if calc < 2 {
            calc = 2;
        }
        calc.min(n)
    } else {
        k.min(embeddings.len())
    };
    let assignments = kmeans_assign(&embeddings, k_eff, 42u64);

    use std::collections::{HashMap, HashSet};
    let mut groups: HashMap<usize, Vec<String>> = HashMap::new();
    for (i, cid) in assignments.into_iter().enumerate() {
        groups.entry(cid).or_default().push(embeddings[i].0.clone());
    }

    // Build a quick lookup for embeddings by stable_id to compute centroids.
    let mut emb_map: HashMap<String, Vec<f32>> = HashMap::new();
    for (id, v) in embeddings.into_iter() {
        emb_map.insert(id, v);
    }

    // Persist cluster metadata (with optional LLM-generated summaries)
    for (cid, members) in groups.into_iter() {
        let cluster_id = format!("hirag::layer0::{}", cid);

        // Compute centroid embedding first (mean of member embeddings, then normalize)
        let mut centroid: Vec<f32> = Vec::new();
        let mut count = 0usize;
        for m in members.iter() {
            if let Some(e) = emb_map.get(m) {
                if centroid.is_empty() {
                    centroid = vec![0.0f32; e.len()];
                }
                for (i, v) in e.iter().enumerate() {
                    centroid[i] += *v;
                }
                count += 1;
            }
        }
        if count > 0 && !centroid.is_empty() {
            for v in centroid.iter_mut() {
                *v = (*v as f64 / (count as f64)) as f32;
            }
            // normalize
            let n = centroid
                .iter()
                .map(|x| (*x as f64).powi(2))
                .sum::<f64>()
                .sqrt();
            if n > 0.0 {
                for x in centroid.iter_mut() {
                    *x = (*x as f64 / n) as f32;
                }
            }
        }

        // Sort members by distance to centroid (closest first) - this prioritizes
        // the most representative entities for snippet collection
        let sorted_members = if !centroid.is_empty() {
            sort_members_by_centroid(&members, &centroid, &emb_map)
        } else {
            members.clone()
        };

        // Fetch short representative snippets from members closest to centroid.
        // Query entity_snapshot.source_content directly since content has been denormalized.
        // Try to collect up to 20 snippets, checking up to 100 members if needed.
        // This ensures we get snippets even if many early members have empty content.
        let mut snippets: Vec<String> = Vec::new();
        let mut checked_count = 0;
        const MAX_SNIPPETS: usize = 20;
        const MAX_MEMBERS_TO_CHECK: usize = 100;

        for m in sorted_members.iter().take(MAX_MEMBERS_TO_CHECK) {
            checked_count += 1;
            if snippets.len() >= MAX_SNIPPETS {
                break; // We have enough snippets
            }
            if let Ok(Some(s)) = crate::db::helpers::get_snippet_for_entity_snapshot(conn, m).await
            {
                snippets.push(s);
                continue;
            }
            if let Ok(Some(s)) = crate::db::helpers::get_snippet_for_entity(conn, m).await {
                snippets.push(s);
            }
        }

        info!(
            "Cluster {} checked {} members and collected {} snippets",
            cid,
            checked_count,
            snippets.len()
        );

        // Call LLM to summarize cluster (non-fatal)
        info!(
            "Cluster {} has {} snippets from {} total members (checked {} members). HZ_LLM_URL={:?}",
            cid,
            snippets.len(),
            members.len(),
            checked_count,
            std::env::var("HZ_LLM_URL").ok()
        );
        let summary_text: String = if snippets.is_empty() {
            warn!(
                "No snippets available for cluster {} after checking {} of {} members, skipping LLM summary",
                cid, checked_count, members.len()
            );
            String::new()
        } else if std::env::var("HZ_LLM_URL").is_err() {
            info!(
                "HZ_LLM_URL not set, skipping LLM summary for cluster {}",
                cid
            );
            String::new()
        } else {
            info!(
                "Calling LLM to summarize cluster {} with {} snippets",
                cid,
                snippets.len()
            );
            match crate::llm::summarize_cluster(&format!("cluster-{}", cid), &snippets).await {
                Ok(summary) => {
                    info!(
                        "LLM returned summary for cluster {} (length: {} chars): {}",
                        cid,
                        summary.len(),
                        if summary.len() > 100 {
                            format!("{}...", &summary[..100])
                        } else {
                            summary.clone()
                        }
                    );
                    summary
                }
                Err(e) => {
                    warn!("LLM summarization failed for cluster {}: {}", cid, e);
                    String::new()
                }
            }
        };

        // Gather repo provenance for the member entities (best-effort). We try common
        // field names (`repo`, `repo_id`) and dedupe the result. Use per-entity queries.
        let mut repos_set: HashSet<String> = HashSet::new();
        for m in members.iter() {
            if let Ok(repos) = crate::db::helpers::get_repos_for_entity(conn, m).await {
                for r in repos.into_iter() {
                    repos_set.insert(r);
                }
            }
        }
        let mut member_repos: Vec<String> = repos_set.into_iter().collect();
        member_repos.sort();

        let summary = ClusterSummary {
            id: cluster_id.clone(),
            label: format!("cluster-{}", cid),
            summary: summary_text,
            members: members.clone(),
            centroid: centroid.clone(),
            centroid_len: centroid.len(),
            member_repos: member_repos.clone(),
        };
        // Ensure we persist an explicit stable_id / stable_label field to avoid
        // consumers needing to parse Surreal Thing ids.
        let mut js = serde_json::to_value(&summary)?;
        if let serde_json::Value::Object(map) = &mut js {
            map.insert(
                "stable_id".to_string(),
                serde_json::Value::String(cluster_id.clone()),
            );
            map.insert(
                "stable_label".to_string(),
                serde_json::Value::String(format!("cluster-{}", cid)),
            );
            // Note: members array is denormalized for fast retrieval. The source
            // of truth is the has_member relationship edges.
        }
        // Upsert a simple record into Surreal and log the response for debugging
        let upsert_sql = format!("CREATE hirag_cluster:`{}` CONTENT $data", cluster_id);
        let binds = vec![("data", js)];
        match conn.query_with_binds(&upsert_sql, binds.clone()).await {
            Ok(resp) => {
                if let Some(jv) = crate::db::helpers::response_to_json(resp) {
                    // record the response for debug endpoints
                    record_create_response(jv.clone());
                }
                // Simplified logging - just report cluster creation success
                info!("Created cluster: {}", cluster_id);
                // Best-effort: create RELATE edges from cluster -> members/commits
                if let Err(e) = persist_cluster_relations(conn, &cluster_id, &members).await {
                    warn!(
                        "failed to persist cluster relations for {}: {}",
                        cluster_id, e
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Surreal CREATE failed: {} binds={:?} -> {}",
                    upsert_sql, binds, e
                );
                return Err(anyhow::anyhow!(e));
            }
        }
    }
    Ok(())
}

/// Build a first-pass hierarchical layer but scoped to a single repo (or repo id).
/// The `commit` parameter is optional and currently unused but kept for future
/// incremental logic. This filters candidate entities to those whose `content`
/// row has a matching `repo` or `repo_id` field equal to `repo`.
pub async fn build_first_layer_for_repo(
    conn: &SurrealConnection,
    repo: &str,
    _commit: Option<&str>,
    k: usize,
) -> Result<()> {
    info!("build_first_layer_for_repo start: repo='{}' k={}", repo, k);

    // Stage 1: Query entity_snapshot directly for this repo's entities with embeddings and content.
    // Only include entities with non-empty source_content so clusters can generate meaningful summaries.
    let primary_sql = "SELECT stable_id, embedding FROM entity_snapshot WHERE repo_name = $repo AND embedding_len > 0 AND source_content != NONE AND string::len(source_content) > 0 LIMIT 5000";
    let binds = vec![("repo", serde_json::Value::String(repo.to_string()))];
    if let Ok(mut resp) = conn.query_with_binds(primary_sql, binds.clone()).await {
        #[derive(serde::Deserialize, serde::Serialize)]
        struct RowEmb {
            stable_id: String,
            embedding: Option<Vec<f32>>,
        }
        let rows: Vec<RowEmb> = resp.take(0)?;
        let mut embeddings: Vec<(String, Vec<f32>)> = Vec::new();
        for r in rows.into_iter() {
            if let Some(e) = r.embedding {
                if !e.is_empty() {
                    embeddings.push((r.stable_id, e));
                }
            }
        }

        // If we found embeddings, continue to clustering flow below.
        if !embeddings.is_empty() {
            info!(
                "Building {} clusters from {} embeddings for repo '{}'",
                if k == 0 { "auto" } else { &k.to_string() },
                embeddings.len(),
                repo
            );
            // continue with collected embeddings (we'll reuse the remainder of the function)
            // Replace the local embeddings variable in outer scope by shadowing: jump to clustering below
            // (we'll duplicate clustering logic by early-returning into a helper block)
            let embeddings = embeddings; // shadow to keep names consistent
                                         // clustering block (identical to below) ------------------------------------------------
                                         // See top-level builder: allow k==0 to mean "auto" and compute a
                                         // small, data-driven branching factor instead of a fixed constant.
            let k_eff = if k == 0 {
                let n = embeddings.len();
                let mut calc = (n as f64).sqrt().round() as usize;
                if calc < 2 {
                    calc = 2;
                }
                calc.min(n)
            } else {
                k.min(embeddings.len())
            };
            let assignments = kmeans_assign(&embeddings, k_eff, 42u64);

            use std::collections::{HashMap, HashSet};
            let mut groups: HashMap<usize, Vec<String>> = HashMap::new();
            for (i, cid) in assignments.into_iter().enumerate() {
                groups.entry(cid).or_default().push(embeddings[i].0.clone());
            }

            let mut emb_map: HashMap<String, Vec<f32>> = HashMap::new();
            for (id, v) in embeddings.into_iter() {
                emb_map.insert(id, v);
            }

            for (cid, members) in groups.into_iter() {
                let cluster_id = format!("hirag::layer0::{}::repo::{}", cid, repo);

                // Compute centroid embedding first (mean of member embeddings, then normalize)
                let mut centroid: Vec<f32> = Vec::new();
                let mut count = 0usize;
                for m in members.iter() {
                    if let Some(e) = emb_map.get(m) {
                        if centroid.is_empty() {
                            centroid = vec![0.0f32; e.len()];
                        }
                        for (i, v) in e.iter().enumerate() {
                            centroid[i] += *v;
                        }
                        count += 1;
                    }
                }
                if count > 0 && !centroid.is_empty() {
                    for v in centroid.iter_mut() {
                        *v = (*v as f64 / (count as f64)) as f32;
                    }
                    // normalize
                    let n = centroid
                        .iter()
                        .map(|x| (*x as f64).powi(2))
                        .sum::<f64>()
                        .sqrt();
                    if n > 0.0 {
                        for x in centroid.iter_mut() {
                            *x = (*x as f64 / n) as f32;
                        }
                    }
                }

                // Sort members by distance to centroid (closest first) - prioritizes
                // the most representative entities for snippet collection
                let sorted_members = if !centroid.is_empty() {
                    sort_members_by_centroid(&members, &centroid, &emb_map)
                } else {
                    members.clone()
                };

                // Try to collect snippets from members closest to centroid.
                // Try up to 100 members to collect up to 20 snippets, ensuring we get content
                // even if many early members have empty source_content.
                let mut snippets: Vec<String> = Vec::new();
                let mut checked_count = 0;
                const MAX_SNIPPETS: usize = 20;
                const MAX_MEMBERS_TO_CHECK: usize = 100;

                for m in sorted_members.iter().take(MAX_MEMBERS_TO_CHECK) {
                    checked_count += 1;
                    if snippets.len() >= MAX_SNIPPETS {
                        break; // We have enough snippets
                    }
                    if let Ok(Some(s)) =
                        crate::db::helpers::get_snippet_for_entity_snapshot(conn, m).await
                    {
                        snippets.push(s);
                        continue;
                    }
                    if let Ok(Some(s)) = crate::db::helpers::get_snippet_for_entity(conn, m).await {
                        snippets.push(s);
                    }
                }

                info!(
                    "Cluster {} (repo: {}) checked {} members and collected {} snippets from {} total members. HZ_LLM_URL={:?}",
                    cid,
                    repo,
                    checked_count,
                    snippets.len(),
                    members.len(),
                    std::env::var("HZ_LLM_URL").ok()
                );
                let summary_text: String = if snippets.is_empty() {
                    warn!(
                        "No snippets available for cluster {} (repo: {}) after checking {} of {} members, skipping LLM summary",
                        cid, repo, checked_count, members.len()
                    );
                    String::new()
                } else if std::env::var("HZ_LLM_URL").is_err() {
                    info!(
                        "HZ_LLM_URL not set, skipping LLM summary for cluster {} (repo: {})",
                        cid, repo
                    );
                    String::new()
                } else {
                    info!(
                        "Calling LLM to summarize cluster {} (repo: {}) with {} snippets",
                        cid,
                        repo,
                        snippets.len()
                    );
                    match crate::llm::summarize_cluster(
                        &format!("cluster-{}-{}", cid, repo),
                        &snippets,
                    )
                    .await
                    {
                        Ok(summary) => {
                            info!(
                                "LLM returned summary for cluster {} (repo: {}, length: {} chars): {}",
                                cid,
                                repo,
                                summary.len(),
                                if summary.len() > 100 {
                                    format!("{}...", &summary[..100])
                                } else {
                                    summary.clone()
                                }
                            );
                            summary
                        }
                        Err(e) => {
                            warn!(
                                "LLM summarization failed for cluster {} (repo: {}): {}",
                                cid, repo, e
                            );
                            String::new()
                        }
                    }
                };

                // Normalize the repo name using our standardized helper
                let mut repos_set: HashSet<String> = HashSet::new();
                // Normalize the explicitly-provided repo provenance so we don't
                // end up with mixed forms like "https://foo" and "foo".
                let norm_repo = normalize_git_url(repo);
                if !norm_repo.is_empty() {
                    repos_set.insert(norm_repo);
                } else {
                    repos_set.insert(repo.to_string());
                }
                for m in members.iter() {
                    if let Ok(repos) = crate::db::helpers::get_repos_for_entity(conn, m).await {
                        for r in repos.into_iter() {
                            repos_set.insert(r);
                        }
                    }
                }
                let mut member_repos: Vec<String> = repos_set.into_iter().collect();
                member_repos.sort();

                let summary = ClusterSummary {
                    id: cluster_id.clone(),
                    label: format!("{}::{}", repo, cid),
                    summary: summary_text,
                    members: members.clone(),
                    centroid: centroid.clone(),
                    centroid_len: centroid.len(),
                    member_repos: member_repos.clone(),
                };

                let mut js = serde_json::to_value(&summary)?;
                if let serde_json::Value::Object(map) = &mut js {
                    map.insert(
                        "stable_id".to_string(),
                        serde_json::Value::String(cluster_id.clone()),
                    );
                    map.insert(
                        "stable_label".to_string(),
                        serde_json::Value::String(format!("{}::{}", repo, cid)),
                    );
                    // Note: members array is denormalized for fast retrieval. The source
                    // of truth is the has_member relationship edges.
                }
                let upsert_sql = format!("CREATE hirag_cluster:`{}` CONTENT $data", cluster_id);
                let binds = vec![("data", js)];
                match conn.query_with_binds(&upsert_sql, binds.clone()).await {
                    Ok(resp) => {
                        if let Some(jv) = crate::db::helpers::response_to_json(resp) {
                            record_create_response(jv.clone());
                        }
                        // Simplified logging
                        info!("Created cluster: {} (repo: {})", cluster_id, repo);
                        if let Err(e) = persist_cluster_relations(conn, &cluster_id, &members).await
                        {
                            warn!(
                                "failed to persist cluster relations for {}: {}",
                                cluster_id, e
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Surreal CREATE (per-repo) failed: {} binds={:?} -> {}",
                            upsert_sql, binds, e
                        );
                        return Err(anyhow::anyhow!(e));
                    }
                }
            }

            return Ok(());
        }
    } else {
        warn!(
            "primary SELECT failed for repo='{}' sql='{}' binds={:?}",
            repo, primary_sql, binds
        );
    }

    // Stage 2: Fallback to snapshot-level discovery. Query `entity_snapshot`
    // directly by repo
    info!(
        "primary path yielded no embeddings for repo='{}', trying snapshot fallback",
        repo
    );
    let snapshot_sql = "SELECT stable_id FROM entity_snapshot WHERE repo_name = $repo AND source_content != NONE AND string::len(source_content) > 0 LIMIT 5000";
    let mut resp2 = conn.query_with_binds(snapshot_sql, binds.clone()).await?;
    #[derive(serde::Deserialize, serde::Serialize)]
    struct SnapshotRow {
        stable_id: String,
    }
    let rows2: Vec<SnapshotRow> = resp2.take(0)?;
    let mut embeddings: Vec<(String, Vec<f32>)> = Vec::new();
    for r in rows2.into_iter() {
        // Prefer snapshot-level embedding for this stable_id
        if let Ok(Some(e)) = crate::db::helpers::get_embedding_for_entity(conn, &r.stable_id).await
        {
            if !e.is_empty() {
                embeddings.push((r.stable_id.clone(), e));
            }
        }
    }

    if embeddings.is_empty() {
        info!("No embeddings found for repo '{}', skipping", repo);
        return Ok(());
    }

    info!(
        "Building {} clusters from {} embeddings for repo '{}' (fallback path)",
        if k == 0 { "auto" } else { &k.to_string() },
        embeddings.len(),
        repo
    );

    let k_eff = if k == 0 {
        let n = embeddings.len();
        let mut calc = (n as f64).sqrt().round() as usize;
        if calc < 2 {
            calc = 2;
        }
        calc.min(n)
    } else {
        k.min(embeddings.len())
    };
    let assignments = kmeans_assign(&embeddings, k_eff, 42u64);

    use std::collections::{HashMap, HashSet};
    let mut groups: HashMap<usize, Vec<String>> = HashMap::new();
    for (i, cid) in assignments.into_iter().enumerate() {
        groups.entry(cid).or_default().push(embeddings[i].0.clone());
    }

    let mut emb_map: HashMap<String, Vec<f32>> = HashMap::new();
    for (id, v) in embeddings.into_iter() {
        emb_map.insert(id, v);
    }

    for (cid, members) in groups.into_iter() {
        let cluster_id = format!("hirag::layer0::{}::repo::{}", cid, repo);

        // Compute centroid embedding first (mean of member embeddings, then normalize)
        let mut centroid: Vec<f32> = Vec::new();
        let mut count = 0usize;
        for m in members.iter() {
            if let Some(e) = emb_map.get(m) {
                if centroid.is_empty() {
                    centroid = vec![0.0f32; e.len()];
                }
                for (i, v) in e.iter().enumerate() {
                    centroid[i] += *v;
                }
                count += 1;
            }
        }
        if count > 0 && !centroid.is_empty() {
            for v in centroid.iter_mut() {
                *v = (*v as f64 / (count as f64)) as f32;
            }
            let n = centroid
                .iter()
                .map(|x| (*x as f64).powi(2))
                .sum::<f64>()
                .sqrt();
            if n > 0.0 {
                for x in centroid.iter_mut() {
                    *x = (*x as f64 / n) as f32;
                }
            }
        }

        // Sort members by distance to centroid (closest first)
        let sorted_members = if !centroid.is_empty() {
            sort_members_by_centroid(&members, &centroid, &emb_map)
        } else {
            members.clone()
        };

        // Try to collect snippets from members closest to centroid.
        // Try up to 20 members to ensure we get a good sample.
        let mut snippets: Vec<String> = Vec::new();
        for m in sorted_members.iter().take(20) {
            if snippets.len() >= 20 {
                break; // We have enough snippets
            }
            // Prefer snapshot-level source_content snippet
            if let Ok(Some(s)) = crate::db::helpers::get_snippet_for_entity_snapshot(conn, m).await
            {
                snippets.push(s);
            } else if let Ok(Some(s2)) = crate::db::helpers::get_snippet_for_entity(conn, m).await {
                snippets.push(s2);
            }
        }

        let summary_text: String = if snippets.is_empty() {
            warn!(
                "No snippets available for cluster {} (repo: {}, fallback) after checking {} members, skipping LLM summary",
                cid, repo, sorted_members.iter().take(20).count()
            );
            String::new()
        } else if std::env::var("HZ_LLM_URL").is_err() {
            info!(
                "HZ_LLM_URL not set, skipping LLM summary for cluster {} (repo: {}, fallback)",
                cid, repo
            );
            String::new()
        } else {
            (crate::llm::summarize_cluster(&format!("cluster-{}-{}", cid, repo), &snippets).await)
                .unwrap_or_else(|e| {
                    warn!(
                        "LLM summarization failed for cluster {} (repo: {}, fallback): {}",
                        cid, repo, e
                    );
                    String::new()
                })
        };

        let mut repos_set: HashSet<String> = HashSet::new();
        // Since we're scoped to a repo, include it explicitly and normalize.
        let norm_repo = normalize_git_url(repo);
        if !norm_repo.is_empty() {
            repos_set.insert(norm_repo);
        } else {
            repos_set.insert(repo.to_string());
        }
        for m in members.iter() {
            if let Ok(repos) = crate::db::helpers::get_repos_for_entity(conn, m).await {
                for r in repos.into_iter() {
                    repos_set.insert(r);
                }
            }
        }
        let mut member_repos: Vec<String> = repos_set.into_iter().collect();
        member_repos.sort();

        let summary = ClusterSummary {
            id: cluster_id.clone(),
            label: format!("{}::{}", repo, cid),
            summary: summary_text,
            members: members.clone(),
            centroid: centroid.clone(),
            centroid_len: centroid.len(),
            member_repos: member_repos.clone(),
        };

        let mut js = serde_json::to_value(&summary)?;
        if let serde_json::Value::Object(map) = &mut js {
            map.insert(
                "stable_id".to_string(),
                serde_json::Value::String(cluster_id.clone()),
            );
            map.insert(
                "stable_label".to_string(),
                serde_json::Value::String(format!("{}::{}", repo, cid)),
            );
            // Note: members array is denormalized for fast retrieval. The source
            // of truth is the has_member relationship edges.
        }
        let upsert_sql = format!("CREATE hirag_cluster:`{}` CONTENT $data", cluster_id);
        let binds = vec![("data", js)];
        match conn.query_with_binds(&upsert_sql, binds.clone()).await {
            Ok(resp) => {
                if let Some(jv) = crate::db::helpers::response_to_json(resp) {
                    // record the response for the debug endpoint
                    record_create_response(jv.clone());
                }
                // Simplified logging
                info!("Created cluster: {} (repo: {})", cluster_id, repo);
                if let Err(e) = persist_cluster_relations(conn, &cluster_id, &members).await {
                    warn!(
                        "failed to persist cluster relations for {}: {}",
                        cluster_id, e
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Surreal CREATE (per-repo) failed: {} binds={:?} -> {}",
                    upsert_sql, binds, e
                );
                return Err(anyhow::anyhow!(e));
            }
        }
    }

    Ok(())
}

/// Build hierarchical layers iteratively until a termination condition is met.
/// This continues clustering layer N to create layer N+1 until either:
/// 1. The number of clusters is small enough (< min_clusters_for_next_layer, default 3)
/// 2. We reach max_depth layers (configurable via HZ_HIRAG_MAX_DEPTH, default 24)
/// 3. The similarity within clusters is high enough (future enhancement)
///
/// # Arguments
/// * `conn` - Database connection
/// * `repo` - Optional repo filter; if provided, only clusters for that repo are included
/// * `k` - Branching factor for each layer (0 = auto-detect based on sqrt(N))
/// * `min_clusters_for_next_layer` - Stop when layer has fewer than this many clusters (default 3)
pub async fn build_hierarchical_layers(
    conn: &SurrealConnection,
    repo: Option<&str>,
    k: usize,
    min_clusters_for_next_layer: usize,
) -> Result<()> {
    let max_depth = std::env::var("HZ_HIRAG_MAX_DEPTH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(24usize);

    let min_clusters = if min_clusters_for_next_layer == 0 {
        3
    } else {
        min_clusters_for_next_layer
    };

    info!(
        "build_hierarchical_layers start: repo={:?}, k={}, min_clusters={}, max_depth={}",
        repo, k, min_clusters, max_depth
    );

    // Start from layer 1 (layer 0 is already built by build_first_layer_for_repo)
    let mut current_layer = 0usize;

    loop {
        if current_layer >= max_depth {
            info!(
                "Reached max depth {} for repo={:?}, stopping hierarchical build",
                max_depth, repo
            );
            break;
        }

        let next_layer = current_layer + 1;
        info!(
            "Building layer {} from layer {} clusters (repo={:?})",
            next_layer, current_layer, repo
        );

        // Query clusters from current layer
        let sql = if let Some(_r) = repo {
            format!(
                "SELECT id, stable_id, label, summary, centroid, centroid_len, members, member_repos \
                 FROM hirag_cluster \
                 WHERE stable_id CONTAINS 'layer{}' \
                 AND (member_repos CONTAINS $repo OR member_repos CONTAINS $norm_repo) \
                 AND centroid_len > 0",
                current_layer
            )
        } else {
            format!(
                "SELECT id, stable_id, label, summary, centroid, centroid_len, members, member_repos \
                 FROM hirag_cluster \
                 WHERE stable_id CONTAINS 'layer{}' \
                 AND centroid_len > 0",
                current_layer
            )
        };

        #[derive(serde::Deserialize)]
        struct LayerRow {
            stable_id: String,
            label: String,
            summary: String,
            centroid: Vec<f32>,
            centroid_len: usize,
            members: Vec<String>,
            member_repos: Vec<String>,
        }

        let mut resp = if let Some(r) = repo {
            let norm_repo = normalize_git_url(r);
            let binds = vec![
                ("repo", serde_json::Value::String(r.to_string())),
                ("norm_repo", serde_json::Value::String(norm_repo)),
            ];
            conn.query_with_binds(&sql, binds).await?
        } else {
            conn.query(&sql).await?
        };

        let layer_clusters: Vec<LayerRow> = resp.take(0)?;
        let num_clusters = layer_clusters.len();

        info!(
            "Found {} layer{} clusters for next-layer clustering (repo={:?})",
            num_clusters, current_layer, repo
        );

        // Termination condition: too few clusters to continue
        if num_clusters < min_clusters {
            info!(
                "Layer {} has only {} clusters (< {}), terminating hierarchical build for repo={:?}",
                current_layer, num_clusters, min_clusters, repo
            );
            break;
        }

        if num_clusters == 0 {
            warn!(
                "No layer{} clusters found for repo={:?}, stopping hierarchical build",
                current_layer, repo
            );
            break;
        }

        // Build embeddings list: (cluster_stable_id, centroid_embedding)
        let embeddings: Vec<(String, Vec<f32>)> = layer_clusters
            .iter()
            .filter(|c| !c.centroid.is_empty())
            .map(|c| (c.stable_id.clone(), c.centroid.clone()))
            .collect();

        if embeddings.is_empty() {
            warn!(
                "No layer{} clusters with valid centroids for repo={:?}, stopping",
                current_layer, repo
            );
            break;
        }

        // Determine effective k for this layer
        let k_eff = if k == 0 {
            let n = embeddings.len();
            let mut calc = (n as f64).sqrt().round() as usize;
            if calc < 2 {
                calc = 2;
            }
            calc.min(n)
        } else {
            k.min(embeddings.len())
        };

        // Check if k_eff would result in almost no reduction
        if k_eff >= embeddings.len() - 1 {
            info!(
                "Layer {} has {} clusters, k_eff={} would not reduce hierarchy (repo={:?}), stopping",
                current_layer, embeddings.len(), k_eff, repo
            );
            break;
        }

        info!(
            "Clustering {} layer{} clusters into {} layer{} meta-clusters (repo={:?})",
            embeddings.len(),
            current_layer,
            k_eff,
            next_layer,
            repo
        );

        // Run k-means on cluster centroids
        let assignments = kmeans_assign(&embeddings, k_eff, 43 + current_layer as u64);

        use std::collections::HashMap;
        let mut groups: HashMap<usize, Vec<String>> = HashMap::new();
        for (i, cid) in assignments.into_iter().enumerate() {
            groups.entry(cid).or_default().push(embeddings[i].0.clone());
        }

        // Build lookup maps for current layer cluster data
        let mut centroid_map: HashMap<String, Vec<f32>> = HashMap::new();
        let mut summary_map: HashMap<String, String> = HashMap::new();
        let mut label_map: HashMap<String, String> = HashMap::new();
        let mut members_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut repos_map: HashMap<String, Vec<String>> = HashMap::new();

        for cluster in layer_clusters.iter() {
            centroid_map.insert(cluster.stable_id.clone(), cluster.centroid.clone());
            summary_map.insert(cluster.stable_id.clone(), cluster.summary.clone());
            label_map.insert(cluster.stable_id.clone(), cluster.label.clone());
            members_map.insert(cluster.stable_id.clone(), cluster.members.clone());
            repos_map.insert(cluster.stable_id.clone(), cluster.member_repos.clone());
        }

        // Create next layer meta-clusters
        for (meta_cid, member_cluster_ids) in groups.into_iter() {
            let cluster_id = if let Some(r) = repo {
                format!("hirag::layer{}::{}::repo::{}", next_layer, meta_cid, r)
            } else {
                format!("hirag::layer{}::{}", next_layer, meta_cid)
            };

            // Compute meta-centroid (average of lower layer centroids)
            let mut meta_centroid: Vec<f32> = Vec::new();
            let mut count = 0usize;
            for cluster_stable_id in member_cluster_ids.iter() {
                if let Some(centroid) = centroid_map.get(cluster_stable_id) {
                    if meta_centroid.is_empty() {
                        meta_centroid = vec![0.0f32; centroid.len()];
                    }
                    for (i, v) in centroid.iter().enumerate() {
                        meta_centroid[i] += *v;
                    }
                    count += 1;
                }
            }
            if count > 0 && !meta_centroid.is_empty() {
                for v in meta_centroid.iter_mut() {
                    *v = (*v as f64 / (count as f64)) as f32;
                }
                // Normalize
                let n = meta_centroid
                    .iter()
                    .map(|x| (*x as f64).powi(2))
                    .sum::<f64>()
                    .sqrt();
                if n > 0.0 {
                    for x in meta_centroid.iter_mut() {
                        *x = (*x as f64 / n) as f32;
                    }
                }
            }

            // Sort member clusters by distance to meta-centroid
            let sorted_members = if !meta_centroid.is_empty() {
                sort_members_by_centroid(&member_cluster_ids, &meta_centroid, &centroid_map)
            } else {
                member_cluster_ids.clone()
            };

            // Collect summaries from member clusters
            let mut cluster_summaries: Vec<String> = Vec::new();
            for cluster_stable_id in sorted_members.iter().take(20) {
                if let Some(summary) = summary_map.get(cluster_stable_id) {
                    if !summary.trim().is_empty() {
                        cluster_summaries.push(format!(
                            "{}: {}",
                            label_map
                                .get(cluster_stable_id)
                                .unwrap_or(&"unknown".to_string()),
                            summary
                        ));
                    }
                }
            }

            info!(
                "Layer{} meta-cluster {} has {} cluster summaries from {} member clusters",
                next_layer,
                meta_cid,
                cluster_summaries.len(),
                member_cluster_ids.len()
            );

            // Generate meta-summary by having LLM summarize the cluster summaries
            let meta_summary: String =
                if cluster_summaries.is_empty() {
                    warn!(
                        "No summaries available for layer{} meta-cluster {}, skipping LLM",
                        next_layer, meta_cid
                    );
                    String::new()
                } else if std::env::var("HZ_LLM_URL").is_err() {
                    info!(
                        "HZ_LLM_URL not set, skipping LLM for layer{} meta-cluster {}",
                        next_layer, meta_cid
                    );
                    String::new()
                } else {
                    info!(
                    "Calling LLM to summarize layer{} meta-cluster {} with {} cluster summaries",
                    next_layer, meta_cid, cluster_summaries.len()
                );
                    match crate::llm::summarize_cluster(
                        &format!("layer{}-cluster-{}", next_layer, meta_cid),
                        &cluster_summaries,
                    )
                    .await
                    {
                        Ok(summary) => {
                            info!(
                            "LLM returned meta-summary for layer{} cluster {} (length: {} chars)",
                            next_layer, meta_cid, summary.len()
                        );
                            summary
                        }
                        Err(e) => {
                            warn!(
                                "LLM meta-summarization failed for layer{} cluster {}: {}",
                                next_layer, meta_cid, e
                            );
                            String::new()
                        }
                    }
                };

            // Aggregate member repos from all lower layer clusters in this meta-cluster
            use std::collections::HashSet;
            let mut all_repos: HashSet<String> = HashSet::new();
            for cluster_stable_id in member_cluster_ids.iter() {
                if let Some(repos) = repos_map.get(cluster_stable_id) {
                    for r in repos {
                        all_repos.insert(r.clone());
                    }
                }
            }
            let mut member_repos: Vec<String> = all_repos.into_iter().collect();
            member_repos.sort();

            // Store member cluster IDs (from lower layer)
            let summary = ClusterSummary {
                id: cluster_id.clone(),
                label: format!("layer{}-cluster-{}", next_layer, meta_cid),
                summary: meta_summary,
                members: sorted_members.clone(), // These are lower layer cluster stable_ids
                centroid: meta_centroid.clone(),
                centroid_len: meta_centroid.len(),
                member_repos,
            };

            let mut js = serde_json::to_value(&summary)?;
            if let serde_json::Value::Object(map) = &mut js {
                map.insert(
                    "stable_id".to_string(),
                    serde_json::Value::String(cluster_id.clone()),
                );
                map.insert(
                    "stable_label".to_string(),
                    serde_json::Value::String(format!("layer{}-cluster-{}", next_layer, meta_cid)),
                );
                map.insert(
                    "layer".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(next_layer)),
                );
            }

            let upsert_sql = format!("CREATE hirag_cluster:`{}` CONTENT $data", cluster_id);
            let binds = vec![("data", js)];
            match conn.query_with_binds(&upsert_sql, binds.clone()).await {
                Ok(resp) => {
                    if let Some(jv) = crate::db::helpers::response_to_json(resp) {
                        record_create_response(jv.clone());
                    }
                    info!("Created layer{} meta-cluster: {}", next_layer, cluster_id);
                }
                Err(e) => {
                    warn!(
                        "Surreal CREATE (layer{}) failed: {} binds={:?} -> {}",
                        next_layer, upsert_sql, binds, e
                    );
                    return Err(anyhow::anyhow!(e));
                }
            }
        }

        info!(
            "Layer {} clustering complete, created {} clusters",
            next_layer, k_eff
        );
        current_layer = next_layer;
    }

    info!(
        "Hierarchical clustering complete for repo={:?}, built {} layers total",
        repo,
        current_layer + 1
    );
    Ok(())
}

/// Legacy function for building a single second layer (deprecated, use build_hierarchical_layers instead)
/// This is now a thin wrapper around build_hierarchical_layers with max_depth effectively set to build just layer 1.
pub async fn build_second_layer(
    conn: &SurrealConnection,
    k: usize,
    repo: Option<&str>,
) -> Result<()> {
    info!(
        "build_second_layer (legacy wrapper) start: k={}, repo={:?}",
        k, repo
    );
    info!("Calling build_hierarchical_layers with min_clusters=999999 to build only layer 1");

    // Set min_clusters very high so it only builds one layer beyond layer0
    build_hierarchical_layers(conn, repo, k, 999999).await
}

/// Retrieve a three-level context for a query text. Returns local entities (top-k by similarity),
/// cluster summaries for clusters that contain those entities, and a placeholder bridge-level
/// subgraph (TODO: compute shortest paths).
/// Retrieve a three-level context for a query text with an optional allowed-repos filter.
/// If `allowed_repos` is provided, only entities/clusters whose provenance intersects the
/// allow-list (or that lack repo provenance) will be returned.
pub async fn retrieve_three_level_with_allowed_repos(
    conn: &SurrealConnection,
    query_embedding: &[f32],
    top_k: usize,
    allowed_repos: Option<&[String]>,
) -> Result<(
    Vec<String>,
    Vec<ClusterSummary>,
    Vec<(String, String, String)>,
)> {
    use std::collections::HashSet;

    // Build a lookup of allowed repositories (both raw and normalized forms) if provided.
    let allow_set: Option<HashSet<String>> = allowed_repos.map(|repos| {
        let mut set = HashSet::new();
        for r in repos {
            if !r.is_empty() {
                set.insert(r.clone());
                let norm = normalize_git_url(r);
                if !norm.is_empty() {
                    set.insert(norm);
                }
            }
        }
        set
    });

    // Sample candidates and compute local cosine similarity; embeddings now live on entity_snapshot.
    // Only consider entities with non-empty source_content so we can return meaningful snippets.
    let sql = "SELECT stable_id, embedding, repo_name FROM entity_snapshot WHERE embedding_len > 0 AND source_content != NONE AND string::len(source_content) > 0 LIMIT 2000";
    let mut resp = conn.query(sql).await?;
    #[derive(serde::Deserialize)]
    struct Cand {
        stable_id: String,
        embedding: Option<Vec<f32>>,
        repo_name: Option<String>,
    }
    let rows: Vec<Cand> = resp.take(0)?;
    let mut scored: Vec<(String, f64)> = Vec::new();
    for r in rows.into_iter() {
        if let Some(ref allow) = allow_set {
            if let Some(ref repo) = r.repo_name {
                let norm = normalize_git_url(repo);
                let repo_allowed =
                    allow.contains(repo) || (!norm.is_empty() && allow.contains(&norm));
                if !repo_allowed {
                    continue;
                }
            }
        }
        if let Some(e) = r.embedding {
            if e.len() == query_embedding.len() {
                let mut dot = 0f64;
                let mut a2 = 0f64;
                let mut b2 = 0f64;
                for (a, b) in e.iter().zip(query_embedding.iter()) {
                    dot += (*a as f64) * (*b as f64);
                    a2 += (*a as f64) * (*a as f64);
                    b2 += (*b as f64) * (*b as f64);
                }
                let denom = a2.sqrt() * b2.sqrt();
                if denom > 0.0 {
                    scored.push((r.stable_id, dot / denom));
                }
            }
        }
    }
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let top = scored
        .into_iter()
        .take(top_k)
        .map(|(s, _)| s)
        .collect::<Vec<_>>();

    // Fetch clusters and filter by membership + allowed_repos (if provided)
    // Avoid selecting the raw Surreal `Thing` id (embedded Mem rejects deserializing it).
    // Use `label` as the stable identifier for tests and consumers.
    let fields = "label, summary, members, centroid, member_repos";
    let sql2 = format!("SELECT {} FROM hirag_cluster START AT 0 LIMIT 1000", fields);
    let mut resp2 = conn.query(&sql2).await?;
    #[derive(serde::Deserialize)]
    struct HRow {
        label: Option<String>,
        summary: Option<String>,
        members: Option<Vec<String>>,
        centroid: Option<serde_json::Value>,
        member_repos: Option<serde_json::Value>,
    }
    let rows2: Vec<HRow> = resp2.take(0)?;
    let mut clusters: Vec<ClusterSummary> = Vec::new();
    for r in rows2.into_iter() {
        let mut include = false;
        if let Some(m) = &r.members {
            for mid in m.iter() {
                if top.contains(mid) {
                    include = true;
                    break;
                }
            }
        }
        if !include {
            continue;
        }
        // If allowed_repos is set, ensure cluster provenance intersects the allow-list (or has no provenance)
        if let Some(allow) = allowed_repos {
            if let Some(provs_val) = &r.member_repos {
                // convert to Vec<String> if possible
                let provs: Vec<String> = match provs_val {
                    serde_json::Value::Array(arr) => arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect(),
                    _ => Vec::new(),
                };
                let mut ok = false;
                for p in provs.iter() {
                    if allow.iter().any(|a| a == p) {
                        ok = true;
                        break;
                    }
                }
                if !ok {
                    continue;
                }
            }
        }
        // convert centroid (serde_json) -> Vec<f32>
        let centroid_vec: Vec<f32> = match r.centroid {
            Some(serde_json::Value::Array(arr)) => arr
                .into_iter()
                .filter_map(|v| v.as_f64().map(|x| x as f32))
                .collect(),
            _ => Vec::new(),
        };
        let centroid_len = centroid_vec.len();

        // convert member_repos value -> Vec<String>
        let member_repos_vec: Vec<String> = match r.member_repos {
            Some(serde_json::Value::Array(arr)) => arr
                .into_iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            _ => Vec::new(),
        };

        let label = r.label.clone().unwrap_or_default();
        clusters.push(ClusterSummary {
            id: label.clone(),
            label,
            summary: r.summary.unwrap_or_default(),
            members: r.members.unwrap_or_default(),
            centroid: centroid_vec,
            centroid_len,
            member_repos: member_repos_vec,
        });
    }

    let bridge: Vec<(String, String, String)> = Vec::new();

    Ok((top, clusters, bridge))
}

/// Backwards-compatible wrapper that preserves the previous API (no repo filter).
pub async fn retrieve_three_level(
    conn: &SurrealConnection,
    query_embedding: &[f32],
    top_k: usize,
) -> Result<(
    Vec<String>,
    Vec<ClusterSummary>,
    Vec<(String, String, String)>,
)> {
    retrieve_three_level_with_allowed_repos(conn, query_embedding, top_k, None).await
}
