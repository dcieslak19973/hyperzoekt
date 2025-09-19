use anyhow::Result;
use reqwest::Client;

use crate::{
    db_writer::connection::SurrealConnection, repo_index::indexer::payload::EntityPayload,
};

#[derive(serde::Deserialize)]
struct TeiOut {
    embeddings: Vec<Vec<f32>>,
}
#[derive(serde::Deserialize)]
struct OAData {
    embedding: Vec<f32>,
}
#[derive(serde::Deserialize)]
struct OAOut {
    data: Vec<OAData>,
}

pub async fn embed_query(query_text: &str) -> Result<Vec<f32>> {
    let tei_base = std::env::var("HZ_TEI_BASE").unwrap_or_else(|_| "http://tei:80".to_string());
    let tei_endpoint = format!("{}/embeddings", tei_base.trim_end_matches('/'));
    let model =
        std::env::var("HZ_EMBED_MODEL").map_err(|_| anyhow::anyhow!("HZ_EMBED_MODEL not set"))?;
    #[derive(serde::Serialize)]
    struct TeiReq<'a> {
        model: &'a str,
        input: [&'a str; 1],
    }
    let body = serde_json::to_vec(&TeiReq {
        model: &model,
        input: [query_text],
    })?;
    let http = Client::builder().build()?;
    let resp = http
        .post(&tei_endpoint)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow::anyhow!(format!("TEI error: {}", resp.status())));
    }
    let bytes = resp.bytes().await?;
    if let Ok(t) = serde_json::from_slice::<TeiOut>(&bytes) {
        Ok(t.embeddings
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("no embedding"))?)
    } else if let Ok(o) = serde_json::from_slice::<OAOut>(&bytes) {
        Ok(o.data
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("no embedding"))?
            .embedding)
    } else {
        Err(anyhow::anyhow!("unrecognized TEI response shape"))
    }
}

pub fn score_top_k(
    query_embedding: &[f32],
    candidates: &[(String, Vec<f32>)],
    top_k: usize,
) -> Vec<(String, f64)> {
    if query_embedding.is_empty() {
        return Vec::new();
    }
    let qn = (query_embedding.iter().map(|f| (f * f) as f64).sum::<f64>()).sqrt();
    if qn == 0.0 {
        return Vec::new();
    }
    let mut scored: Vec<(String, f64)> = Vec::with_capacity(candidates.len());
    for (sid, emb) in candidates.iter() {
        if emb.len() != query_embedding.len() {
            continue;
        }
        let mut dot = 0.0f64;
        let mut cn2 = 0.0f64;
        for (a, b) in emb.iter().zip(query_embedding.iter()) {
            dot += (*a as f64) * (*b as f64);
            cn2 += (*a as f64) * (*a as f64);
        }
        let denom = (cn2.sqrt()) * qn;
        if denom > 0.0 {
            scored.push((sid.clone(), dot / denom));
        }
    }
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let take = top_k.min(scored.len());
    scored.truncate(take);
    scored
}

pub async fn similarity_with_conn(
    conn: &SurrealConnection,
    query_text: &str,
    repo_filter: Option<&str>,
) -> Result<Vec<EntityPayload>> {
    // Backwards-compatible wrapper: convert single repo string into a one-element
    // slice and delegate to the multi-repo implementation.
    if let Some(rf) = repo_filter {
        let v = vec![rf.to_string()];
        return similarity_with_conn_multi(conn, query_text, Some(&v)).await;
    }
    similarity_with_conn_multi(conn, query_text, None).await
}

/// Multi-repo variant of similarity search. Accepts an optional slice of repo
/// names to filter embeddings sampling. If `repo_filters` is None, search across
/// all repos. Filters which look like paths (start with '/') are ignored for the
/// embedding sampling stage.
pub async fn similarity_with_conn_multi(
    conn: &SurrealConnection,
    query_text: &str,
    repo_filters: Option<&[String]>,
) -> Result<Vec<EntityPayload>> {
    let top_k: usize = std::env::var("HZ_SIMSEARCH_TOPK")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    let sample: usize = std::env::var("HZ_SIMSEARCH_SAMPLE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3000);
    let query_embedding = embed_query(query_text).await?;

    #[derive(serde::Deserialize)]
    struct Cand {
        stable_id: String,
        embedding: Vec<f32>,
    }

    // Build SQL for sampling candidate embeddings. If repo_filters contains
    // at least one repo name, use `repo_name IN $repos` bind. Otherwise sample
    // across all entities with embeddings.
    let (sql, binds): (String, Vec<(&'static str, serde_json::Value)>) = if let Some(rfs) =
        repo_filters
    {
        let names: Vec<String> = rfs
            .iter()
            .filter(|s| !s.starts_with('/'))
            .cloned()
            .collect();
        if !names.is_empty() {
            let bind_arr = serde_json::Value::Array(
                names
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            );
            (
                format!(
                    "SELECT stable_id, embedding FROM entity WHERE embedding_len > 0 AND repo_name IN $repos START AT 0 LIMIT {}",
                    sample
                ),
                vec![("repos", bind_arr)],
            )
        } else {
            (
                format!(
                    "SELECT stable_id, embedding FROM entity WHERE embedding_len > 0 START AT 0 LIMIT {}",
                    sample
                ),
                vec![],
            )
        }
    } else {
        (
            format!(
                "SELECT stable_id, embedding FROM entity WHERE embedding_len > 0 START AT 0 LIMIT {}",
                sample
            ),
            vec![],
        )
    };
    let mut resp = conn.query_with_binds(&sql, binds).await?;
    let cands: Vec<Cand> = resp.take(0)?;
    if cands.is_empty() {
        return Ok(vec![]);
    }
    let scored = score_top_k(
        &query_embedding,
        &cands
            .iter()
            .map(|c| (c.stable_id.clone(), c.embedding.clone()))
            .collect::<Vec<_>>(),
        top_k,
    );
    if scored.is_empty() {
        return Ok(vec![]);
    }
    let fields = "file, language, kind, name, parent, signature, start_line, end_line, doc, rank, imports, unresolved_imports, stable_id, repo_name, source_url, source_display";
    let ids: Vec<serde_json::Value> = scored
        .iter()
        .map(|(sid, _)| serde_json::Value::String(sid.clone()))
        .collect();
    let sql2 = format!("SELECT {} FROM entity WHERE stable_id INSIDE $ids", fields);
    let mut resp2 = conn
        .query_with_binds(&sql2, vec![("ids", serde_json::Value::Array(ids))])
        .await?;
    let mut ents: Vec<EntityPayload> = resp2.take(0)?;
    let order: std::collections::HashMap<String, usize> = scored
        .iter()
        .enumerate()
        .map(|(i, (sid, _))| (sid.clone(), i))
        .collect();
    ents.sort_by_key(|e| order.get(&e.stable_id).cloned().unwrap_or(usize::MAX));
    Ok(ents)
}
