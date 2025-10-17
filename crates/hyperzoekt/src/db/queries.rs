use crate::db::connection::SurrealConnection;
use crate::repo_index::indexer::payload::EntityPayload;
use crate::utils::{RepoSummary, RepoSummaryQueryResult};
use serde::Deserialize;
use surrealdb::Value as DbValue;

// Helper to safely convert SurrealDB response to JSON
fn db_value_to_json(db_val: DbValue) -> Option<serde_json::Value> {
    match serde_json::to_value(&db_val) {
        Ok(json) => Some(json),
        Err(e) => {
            log::warn!("Failed to convert DbValue to JSON: {}", e);
            None
        }
    }
}

// Sort a Vec<serde_json::Value> of dependency objects by their `name` field (case-insensitive).
fn sort_deps_alphabetical(mut deps: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
    deps.sort_by(|a, b| {
        let a_name = a
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        let b_name = b
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        a_name.cmp(&b_name)
    });
    deps
}

#[derive(Clone)]
pub struct DatabaseQueries {
    pub db: std::sync::Arc<SurrealConnection>,
}

impl DatabaseQueries {
    pub fn new(db: std::sync::Arc<SurrealConnection>) -> Self {
        Self { db }
    }

    /// Resolve a human ref name (e.g. "main" or "refs/heads/main") to a commit id
    /// and attempt to find a matching `snapshot_meta` row. Returns (repo_filter, commit_opt, snapshot_id_opt)
    /// if the lookup completed successfully. If no ref/commit was identified, commit_opt will be None.
    pub async fn resolve_ref_to_snapshot(
        &self,
        repo: &str,
        raw: &str,
    ) -> Result<Option<(String, Option<String>)>, Box<dyn std::error::Error + Send + Sync>> {
        let (_, commit_opt, snapshot_id_opt) =
            crate::utils::resolve_ref_to_snapshot(&self.db, repo, Some(raw)).await?;
        if let Some(commit) = commit_opt {
            Ok(Some((commit, snapshot_id_opt)))
        } else {
            Ok(None)
        }
    }

    /// Return the repo's configured default branch, falling back to `SOURCE_BRANCH` env or `main`.
    pub async fn get_repo_default_branch(
        &self,
        repo_name: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        #[derive(Deserialize)]
        struct BranchRow {
            branch: Option<String>,
        }

        let sql = "SELECT branch FROM repo WHERE name = $name LIMIT 1";
        let mut res = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                let q = db_conn.query(sql).bind(("name", repo_name.to_string()));
                q.await?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                let q = db_conn.query(sql).bind(("name", repo_name.to_string()));
                q.await?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                let q = db_conn.query(sql).bind(("name", repo_name.to_string()));
                q.await?
            }
        };
        if let Ok(rows) = res.take::<Vec<BranchRow>>(0) {
            if let Some(r) = rows.into_iter().next() {
                if let Some(b) = r.branch {
                    return Ok(b);
                }
            }
        }

        // Fallbacks
        // If repo row didn't contain a branch, try to find a common default branch
        // by querying the refs table (this mirrors the SBOM lookup behavior and
        // centralizes fallback logic so both UI paths behave the same).
        let refs_query = "SELECT VALUE name FROM refs WHERE repo = $repo AND name IN ['refs/heads/main', 'refs/heads/master', 'refs/heads/trunk'] LIMIT 1";
        let default_from_refs: Option<String> = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                let result = db_conn
                    .query(refs_query)
                    .bind(("repo", repo_name.to_string()))
                    .await?;
                if let Some(json_val) = crate::db::response_to_json(result) {
                    if let Some(arr) = json_val.as_array() {
                        arr.first().and_then(|v| v.as_str()).map(String::from)
                    } else {
                        json_val.as_str().map(String::from)
                    }
                } else {
                    None
                }
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                let result = db_conn
                    .query(refs_query)
                    .bind(("repo", repo_name.to_string()))
                    .await?;
                if let Some(json_val) = crate::db::response_to_json(result) {
                    if let Some(arr) = json_val.as_array() {
                        arr.first().and_then(|v| v.as_str()).map(String::from)
                    } else {
                        json_val.as_str().map(String::from)
                    }
                } else {
                    None
                }
            }
            SurrealConnection::RemoteWs(db_conn) => {
                let result = db_conn
                    .query(refs_query)
                    .bind(("repo", repo_name.to_string()))
                    .await?;
                if let Some(json_val) = crate::db::response_to_json(result) {
                    if let Some(arr) = json_val.as_array() {
                        arr.first().and_then(|v| v.as_str()).map(String::from)
                    } else {
                        json_val.as_str().map(String::from)
                    }
                } else {
                    None
                }
            }
        };

        if let Some(ref_name) = default_from_refs {
            // ref_name will be like "refs/heads/main"; strip prefix if present
            if let Some(stripped) = ref_name.strip_prefix("refs/heads/") {
                return Ok(stripped.to_string());
            }
            return Ok(ref_name);
        }

        if let Ok(envb) = std::env::var("SOURCE_BRANCH") {
            return Ok(envb);
        }
        Ok("main".to_string())
    }

    pub async fn get_repo_summaries(&self) -> Result<Vec<RepoSummary>, Box<dyn std::error::Error>> {
        // Use the stored repo_name field instead of parsing from file paths
        // Handle cases where repo_name might be null for existing data
        let query = r#"
            SELECT
                repo_name ?? 'unknown' as repo_name,
                count() as entity_count,
                array::distinct(file) as files,
                array::distinct(language) as languages
            FROM entity
            GROUP BY repo_name
            ORDER BY entity_count DESC
        "#;

        let mut response = self.db.query(query).await?;
        let query_results: Vec<RepoSummaryQueryResult> = response.take(0)?;

        Ok(query_results
            .into_iter()
            .map(|s| {
                // Filter out any nulls returned by the DB and produce stable Vec<String> types
                let files: Vec<String> = s.files.into_iter().flatten().collect();
                let languages: Vec<String> = s.languages.into_iter().flatten().collect();
                let file_count = files.len() as u64; // Count distinct files
                RepoSummary {
                    name: s.repo_name,
                    entity_count: s.entity_count,
                    file_count,
                    languages,
                }
            })
            .collect())
    }

    pub async fn get_entities_for_repo(
        &self,
        repo_name: &str,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        // Prefer filtering on the explicit `repo_name` field (safer and more direct).
        // Fall back to matching file path prefixes for older/imported records that
        // don't have `repo_name` populated.
        // Use the per-snapshot page rank value stored under snapshot.page_rank_value.
        // We no longer fall back to a top-level entity field; page_rank is snapshot-scoped.
        let entity_fields = "<string>id AS id, language, kind, name, snapshot.page_rank_value AS page_rank_value, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content";
        let sql = format!(
            "SELECT {} FROM entity WHERE repo_name = $repo ORDER BY snapshot.file, snapshot.start_line",
            entity_fields
        );
        let mut resp = self
            .db
            .query_with_binds(
                &sql,
                vec![("repo", serde_json::Value::String(repo_name.to_string()))],
            )
            .await?;
        let entities: Vec<EntityPayload> = match resp.take::<Vec<EntityPayload>>(0) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "get_entities_for_repo: failed to deserialize response for repo='{}': {}",
                    repo_name,
                    e
                );
                return Err(Box::new(e));
            }
        };

        if !entities.is_empty() {
            return Ok(entities);
        }

        // Fallback: look for file paths that start with the provided repo_name.
        // Use a parameterized query to avoid injection.
        let entity_fields = "<string>id AS id, language, kind, name, snapshot.page_rank_value AS page_rank_value, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content";
        let sql2 = format!(
            "SELECT {} FROM entity WHERE string::starts_with(snapshot.file ?? '', $repo) ORDER BY snapshot.file, snapshot.start_line",
            entity_fields
        );
        let mut resp2 = self
            .db
            .query_with_binds(
                &sql2,
                vec![("repo", serde_json::Value::String(repo_name.to_string()))],
            )
            .await?;
        let entities2: Vec<EntityPayload> = resp2.take(0)?;

        Ok(entities2)
    }

    pub async fn get_all_entities(&self) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        let entity_fields = "<string>id AS id, language, kind, name, snapshot.page_rank_value AS page_rank_value, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content";
        let query_sql = format!(
            "SELECT {} FROM entity ORDER BY snapshot.file, snapshot.start_line",
            entity_fields
        );
        let mut response = self.db.query(&query_sql).await?;
        // Defensive deserialization with logging to help debug malformed DB rows
        match response.take::<Vec<EntityPayload>>(0) {
            Ok(ents) => Ok(ents),
            Err(e) => {
                log::error!(
                    "get_all_entities: failed to deserialize response for query='{}': {}",
                    query_sql,
                    e
                );
                Err(Box::new(e))
            }
        }
    }

    pub async fn get_entity_by_id(
        &self,
        stable_id: &str,
    ) -> Result<Option<EntityPayload>, Box<dyn std::error::Error>> {
        let entity_fields = "<string>id AS id, language, kind, name, snapshot.page_rank_value AS page_rank_value, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content";
        let sql = format!(
            "SELECT {} FROM entity WHERE stable_id = $stable_id",
            entity_fields
        );
        let mut response = self
            .db
            .query_with_binds(
                &sql,
                vec![(
                    "stable_id",
                    serde_json::Value::String(stable_id.to_string()),
                )],
            )
            .await?;
        let entities: Vec<EntityPayload> = response.take(0)?;

        Ok(entities.into_iter().next())
    }

    /// Fetch similar entities for a given entity stable_id using relation tables with score.
    /// Returns two lists: same-repo and external-repo, each with basic metadata and score.
    pub async fn get_similar_for_entity(
        &self,
        stable_id: &str,
        limit: usize,
    ) -> Result<(Vec<serde_json::Value>, Vec<serde_json::Value>), Box<dyn std::error::Error>> {
        // Resolve the entity Thing id from stable_id first
        #[derive(Deserialize)]
        struct IdRow {
            id: surrealdb::sql::Thing,
        }

        let rows: Vec<IdRow> = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                let mut r = db_conn
                    .query("SELECT id FROM entity WHERE stable_id = $sid LIMIT 1")
                    .bind(("sid", stable_id.to_string()))
                    .await?;
                r.take(0)?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                let mut r = db_conn
                    .query("SELECT id FROM entity WHERE stable_id = $sid LIMIT 1")
                    .bind(("sid", stable_id.to_string()))
                    .await?;
                r.take(0)?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                let mut r = db_conn
                    .query("SELECT id FROM entity WHERE stable_id = $sid LIMIT 1")
                    .bind(("sid", stable_id.to_string()))
                    .await?;
                r.take(0)?
            }
        };
        if rows.is_empty() {
            return Ok((vec![], vec![]));
        }
        let ent_id_raw = rows[0].id.to_string();

        // Convert entity ID to entity_snapshot ID (assuming same sanitized stable_id)
        let snapshot_id = format!(
            "entity_snapshot:{}",
            ent_id_raw.split(':').next_back().unwrap_or(&ent_id_raw)
        );

        // Query edges and target entity minimal fields with scores for both relation tables
        let sql = r#"
            SELECT array::slice((SELECT out AS target, score FROM type::thing($id)->similar_same_repo ORDER BY score DESC), 0, $lim) AS same_repo,
                   array::slice((SELECT out AS target, score FROM type::thing($id)->similar_external_repo ORDER BY score DESC), 0, $lim) AS external_repo;
        "#;
        let mut resp2 = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(sql)
                    .bind(("id", snapshot_id.clone()))
                    .bind(("lim", limit as i64))
                    .await?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn
                    .query(sql)
                    .bind(("id", snapshot_id.clone()))
                    .bind(("lim", limit as i64))
                    .await?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn
                    .query(sql)
                    .bind(("id", snapshot_id.clone()))
                    .bind(("lim", limit as i64))
                    .await?
            }
        };

        #[derive(Deserialize)]
        struct EdgeRow {
            target: surrealdb::sql::Thing,
            score: f64,
        }

        #[derive(Deserialize)]
        struct RelRows {
            same_repo: Option<Vec<EdgeRow>>,
            external_repo: Option<Vec<EdgeRow>>,
        }

        let rels: Vec<RelRows> = resp2.take(0)?;
        let (same_repo, external_repo): (Vec<EdgeRow>, Vec<EdgeRow>) =
            if let Some(r) = rels.into_iter().next() {
                (
                    r.same_repo.unwrap_or_default(),
                    r.external_repo.unwrap_or_default(),
                )
            } else {
                (vec![], vec![])
            };

        // Gather unique target ids and fetch minimal entity data in one query
        let mut targets: Vec<String> = Vec::new();
        for e in same_repo.iter().chain(external_repo.iter()) {
            let mut s = e.target.to_string();
            // Convert entity_snapshot ID to entity ID
            if s.starts_with("entity_snapshot:") {
                s = s.replace("entity_snapshot:", "entity:");
            }
            if !targets.contains(&s) {
                targets.push(s);
            }
        }
        if targets.is_empty() {
            return Ok((vec![], vec![]));
        }

        // Build a parameterized query using array::concat of type::thing
        let mut thing_exprs: Vec<String> = Vec::with_capacity(targets.len());
        for (i, _) in targets.iter().enumerate() {
            thing_exprs.push(format!("type::thing($t{})", i));
        }
        let sql2 = format!(
            "SELECT id, stable_id, name, snapshot.file AS file, repo_name, language, kind, snapshot.source_url AS source_url, snapshot.source_display AS source_display FROM entity WHERE id IN [{}]",
            thing_exprs.join(", ")
        );
        let mut resp3 = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                let mut q = db_conn.query(&sql2);
                for (i, t) in targets.iter().enumerate() {
                    q = q.bind((format!("t{}", i), t.to_string()));
                }
                q.await?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                let mut q = db_conn.query(&sql2);
                for (i, t) in targets.iter().enumerate() {
                    q = q.bind((format!("t{}", i), t.to_string()));
                }
                q.await?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                let mut q = db_conn.query(&sql2);
                for (i, t) in targets.iter().enumerate() {
                    q = q.bind((format!("t{}", i), t.to_string()));
                }
                q.await?
            }
        };

        #[derive(Deserialize)]
        struct EntRow {
            id: surrealdb::sql::Thing,
            stable_id: String,
            name: String,
            file: Option<String>,
            repo_name: String,
            language: String,
            kind: String,
            source_url: Option<String>,
            source_display: Option<String>,
        }

        let ents: Vec<EntRow> = resp3.take(0)?;
        use std::collections::HashMap;
        let mut ent_map: HashMap<String, EntRow> = HashMap::new();
        for e in ents {
            ent_map.insert(e.id.to_string(), e);
        }

        let mut out_same: Vec<serde_json::Value> = Vec::new();
        let mut out_external: Vec<serde_json::Value> = Vec::new();
        for e in same_repo.into_iter() {
            // Convert entity_snapshot ID to entity ID for map lookup
            let target_id = if e.target.to_string().starts_with("entity_snapshot:") {
                e.target.to_string().replace("entity_snapshot:", "entity:")
            } else {
                e.target.to_string()
            };
            if let Some(er) = ent_map.get(&target_id) {
                out_same.push(serde_json::json!({
                    "stable_id": er.stable_id,
                    "name": er.name,
                    "file": er.file,
                    "repo_name": er.repo_name,
                    "language": er.language,
                    "kind": er.kind,
                    "source_url": er.source_url,
                    "source_display": er.source_display,
                    "score": e.score,
                }));
            }
        }
        for e in external_repo.into_iter() {
            // Convert entity_snapshot ID to entity ID for map lookup
            let target_id = if e.target.to_string().starts_with("entity_snapshot:") {
                e.target.to_string().replace("entity_snapshot:", "entity:")
            } else {
                e.target.to_string()
            };
            if let Some(er) = ent_map.get(&target_id) {
                out_external.push(serde_json::json!({
                    "stable_id": er.stable_id,
                    "name": er.name,
                    "file": er.file,
                    "repo_name": er.repo_name,
                    "language": er.language,
                    "kind": er.kind,
                    "source_url": er.source_url,
                    "source_display": er.source_display,
                    "score": e.score,
                }));
            }
        }

        Ok((out_same, out_external))
    }

    /// Fetch top duplicate-like pairs within a repository based on similar_same_repo edges.
    /// Returns a list of pairs {a: {...}, b: {...}, score} limited by `limit`.
    pub async fn get_dupes_for_repo(
        &self,
        repo_name: &str,
        limit: usize,
        offset: usize,
        min_score: Option<f64>,
        language: Option<&str>,
        kind: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        // Find edges where both endpoints are in the same repo.
        // We perform pagination at the relation-edge level using LIMIT/OFFSET
        // to avoid fetching the entire table into memory.
        // Use START AT/ LIMIT form to be accepted by SurrealDB and interpolate
        // numeric offset/limit values directly (they are validated integers).
        let edge_sql = format!(
            "SELECT in AS a, out AS b, score FROM similar_same_repo ORDER BY score DESC START AT {} LIMIT {}",
            (offset as i64) * 4,
            (limit as i64) * 4
        );

        #[derive(Deserialize)]
        struct EdgeRowMin {
            a: surrealdb::sql::Thing,
            b: surrealdb::sql::Thing,
            score: f64,
        }

        let edge_rows: Vec<EdgeRowMin> = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                let mut r = db_conn.query(&edge_sql).await?;
                r.take(0)?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                let mut r = db_conn.query(&edge_sql).await?;
                r.take(0)?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                let mut r = db_conn.query(&edge_sql).await?;
                r.take(0)?
            }
        };

        if edge_rows.is_empty() {
            return Ok(vec![]);
        }

        // Gather unique ids referenced and bulk load entity rows to filter by repo
        use std::collections::{HashMap, HashSet};
        let mut ids: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for e in &edge_rows {
            for tid in [e.a.to_string(), e.b.to_string()] {
                // Convert entity_snapshot ID to entity ID
                let entity_id = if tid.starts_with("entity_snapshot:") {
                    tid.replace("entity_snapshot:", "entity:")
                } else {
                    tid
                };
                if seen.insert(entity_id.clone()) {
                    ids.push(entity_id);
                }
            }
        }
        let mut thing_exprs: Vec<String> = Vec::with_capacity(ids.len());
        for (i, _) in ids.iter().enumerate() {
            thing_exprs.push(format!("type::thing($x{})", i));
        }
        let sql2 = format!(
            "SELECT id, stable_id, name, snapshot.file AS file, repo_name, language, kind, snapshot.source_url AS source_url, snapshot.source_display AS source_display FROM entity WHERE id IN [{}]",
            thing_exprs.join(", ")
        );
        let mut resp2 = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                let mut q = db_conn.query(&sql2);
                for (i, t) in ids.iter().enumerate() {
                    q = q.bind((format!("x{}", i), t.to_string()));
                }
                q.await?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                let mut q = db_conn.query(&sql2);
                for (i, t) in ids.iter().enumerate() {
                    q = q.bind((format!("x{}", i), t.to_string()));
                }
                q.await?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                let mut q = db_conn.query(&sql2);
                for (i, t) in ids.iter().enumerate() {
                    q = q.bind((format!("x{}", i), t.to_string()));
                }
                q.await?
            }
        };

        #[derive(Deserialize, Clone)]
        struct EntMin {
            id: surrealdb::sql::Thing,
            stable_id: String,
            name: String,
            file: Option<String>,
            repo_name: String,
            language: String,
            kind: String,
            source_url: Option<String>,
            source_display: Option<String>,
        }

        let ents: Vec<EntMin> = resp2.take(0)?;
        let mut map: HashMap<String, EntMin> = HashMap::new();
        for e in ents.into_iter() {
            map.insert(e.id.to_string(), e);
        }

        // Build output for pairs where both entities belong to repo_name
        let mut out: Vec<serde_json::Value> = Vec::new();
        let lang_lc = language.map(|s| s.to_lowercase());
        let kind_lc = kind.map(|s| s.to_lowercase());
        for e in edge_rows.into_iter() {
            // Convert entity_snapshot IDs to entity IDs for map lookup
            let a_id = if e.a.to_string().starts_with("entity_snapshot:") {
                e.a.to_string().replace("entity_snapshot:", "entity:")
            } else {
                e.a.to_string()
            };
            let b_id = if e.b.to_string().starts_with("entity_snapshot:") {
                e.b.to_string().replace("entity_snapshot:", "entity:")
            } else {
                e.b.to_string()
            };
            if let (Some(ae), Some(be)) = (map.get(&a_id), map.get(&b_id)) {
                if ae.repo_name == repo_name && be.repo_name == repo_name {
                    if let Some(ms) = min_score {
                        if e.score < ms {
                            continue;
                        }
                    }
                    if let Some(ref ll) = lang_lc {
                        if ae.language.to_lowercase() != *ll || be.language.to_lowercase() != *ll {
                            continue;
                        }
                    }
                    if let Some(ref kk) = kind_lc {
                        if ae.kind.to_lowercase() != *kk || be.kind.to_lowercase() != *kk {
                            continue;
                        }
                    }
                    out.push(serde_json::json!({
                        "score": e.score,
                        "a": {
                            "stable_id": ae.stable_id,
                            "name": ae.name,
                            "file": ae.file,
                            "repo_name": ae.repo_name,
                            "language": ae.language,
                            "kind": ae.kind,
                            "source_url": ae.source_url,
                            "source_display": ae.source_display,
                        },
                        "b": {
                            "stable_id": be.stable_id,
                            "name": be.name,
                            "file": be.file,
                            "repo_name": be.repo_name,
                            "language": be.language,
                            "kind": be.kind,
                            "source_url": be.source_url,
                            "source_display": be.source_display,
                        }
                    }));
                }
            }
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Fetch cross-repo duplicate-like pairs where `a` belongs to `repo_name` and `b` belongs to another repo.
    pub async fn get_external_dupes_for_repo(
        &self,
        repo_name: &str,
        limit: usize,
        offset: usize,
        min_score: Option<f64>,
        language: Option<&str>,
        kind: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        let edge_sql = format!(
            "SELECT in AS a, out AS b, score FROM similar_external_repo ORDER BY score DESC START AT {} LIMIT {}",
            (offset as i64) * 4,
            (limit as i64) * 4
        );

        #[derive(Deserialize)]
        struct EdgeRowMin {
            a: surrealdb::sql::Thing,
            b: surrealdb::sql::Thing,
            score: f64,
        }

        let edge_rows: Vec<EdgeRowMin> = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                let mut r = db_conn.query(&edge_sql).await?;
                r.take(0)?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                let mut r = db_conn.query(&edge_sql).await?;
                r.take(0)?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                let mut r = db_conn.query(&edge_sql).await?;
                r.take(0)?
            }
        };
        if edge_rows.is_empty() {
            return Ok(vec![]);
        }

        use std::collections::{HashMap, HashSet};
        let mut ids: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for e in &edge_rows {
            for tid in [e.a.to_string(), e.b.to_string()] {
                // Convert entity_snapshot ID to entity ID
                let entity_id = if tid.starts_with("entity_snapshot:") {
                    tid.replace("entity_snapshot:", "entity:")
                } else {
                    tid
                };
                if seen.insert(entity_id.clone()) {
                    ids.push(entity_id);
                }
            }
        }
        let mut thing_exprs: Vec<String> = Vec::with_capacity(ids.len());
        for (i, _) in ids.iter().enumerate() {
            thing_exprs.push(format!("type::thing($x{})", i));
        }
        let sql2 = format!(
            "SELECT id, stable_id, name, snapshot.file AS file, repo_name, language, kind, snapshot.source_url AS source_url, snapshot.source_display AS source_display FROM entity WHERE id IN [{}]",
            thing_exprs.join(", ")
        );
        let mut resp2 = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                let mut q = db_conn.query(&sql2);
                for (i, t) in ids.iter().enumerate() {
                    q = q.bind((format!("x{}", i), t.to_string()));
                }
                q.await?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                let mut q = db_conn.query(&sql2);
                for (i, t) in ids.iter().enumerate() {
                    q = q.bind((format!("x{}", i), t.to_string()));
                }
                q.await?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                let mut q = db_conn.query(&sql2);
                for (i, t) in ids.iter().enumerate() {
                    q = q.bind((format!("x{}", i), t.to_string()));
                }
                q.await?
            }
        };
        #[derive(Deserialize, Clone)]
        struct EntMin {
            id: surrealdb::sql::Thing,
            stable_id: String,
            name: String,
            file: Option<String>,
            repo_name: String,
            language: String,
            kind: String,
            source_url: Option<String>,
            source_display: Option<String>,
        }
        let ents: Vec<EntMin> = resp2.take(0)?;
        let mut map: HashMap<String, EntMin> = HashMap::new();
        for e in ents.into_iter() {
            map.insert(e.id.to_string(), e);
        }

        let mut out: Vec<serde_json::Value> = Vec::new();
        for e in edge_rows.into_iter() {
            // Convert entity_snapshot IDs to entity IDs for map lookup
            let a_id = if e.a.to_string().starts_with("entity_snapshot:") {
                e.a.to_string().replace("entity_snapshot:", "entity:")
            } else {
                e.a.to_string()
            };
            let b_id = if e.b.to_string().starts_with("entity_snapshot:") {
                e.b.to_string().replace("entity_snapshot:", "entity:")
            } else {
                e.b.to_string()
            };
            if let (Some(ae), Some(be)) = (map.get(&a_id), map.get(&b_id)) {
                if ae.repo_name == repo_name && be.repo_name != repo_name {
                    if let Some(ms) = min_score {
                        if e.score < ms {
                            continue;
                        }
                    }
                    if let Some(ll) = language {
                        if ae.language.to_lowercase() != *ll || be.language.to_lowercase() != *ll {
                            continue;
                        }
                    }
                    if let Some(kk) = kind {
                        if ae.kind.to_lowercase() != *kk || be.kind.to_lowercase() != *kk {
                            continue;
                        }
                    }
                    out.push(serde_json::json!({
                        "score": e.score,
                        "a": {
                            "stable_id": ae.stable_id,
                            "name": ae.name,
                            "file": ae.file,
                            "repo_name": ae.repo_name,
                            "language": ae.language,
                            "kind": ae.kind,
                            "source_url": ae.source_url,
                            "source_display": ae.source_display,
                        },
                        "b": {
                            "stable_id": be.stable_id,
                            "name": be.name,
                            "file": be.file,
                            "repo_name": be.repo_name,
                            "language": be.language,
                            "kind": be.kind,
                            "source_url": be.source_url,
                            "source_display": be.source_display,
                        }
                    }));
                }
            }
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Fetch methods for a given class/struct/interface/trait entity by matching on parent name within the same repo.
    /// Note: For Rust, impl methods are exported as top-level functions with parent = NULL, so this will return empty.
    pub async fn get_methods_for_parent(
        &self,
        repo_name: &str,
        parent_name: &str,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        let fields = "<string>id AS id, language, kind, name, rank AS rank, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content";

        let mut response = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE repo_name = $repo AND kind = 'function' AND parent = $parent ORDER BY snapshot.start_line, name",
                        fields
                    ))
                    .bind(("repo", repo_name.to_string()))
                    .bind(("parent", parent_name.to_string()))
                    .await?
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE repo_name = $repo AND kind = 'function' AND parent = $parent ORDER BY snapshot.start_line, name",
                        fields
                    ))
                    .bind(("repo", repo_name.to_string()))
                    .bind(("parent", parent_name.to_string()))
                    .await?
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn
                    .query(format!(
                        "SELECT {} FROM entity WHERE repo_name = $repo AND kind = 'function' AND parent = $parent ORDER BY snapshot.start_line, name",
                        fields
                    ))
                    .bind(("repo", repo_name.to_string()))
                    .bind(("parent", parent_name.to_string()))
                    .await?
            }
        };

        let methods: Vec<EntityPayload> = response.take(0)?;
        Ok(methods)
    }

    pub async fn get_dependencies_for_repo(
        &self,
        repo_name: &str,
        branch: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        // We expect dependency rows to exist in `dependency` table and relations from repo -> depends_on -> dependency
        // We'll try to resolve by repo name and optional branch by looking up the repo Thing first
        #[derive(Deserialize)]
        struct RepoRow {
            id: surrealdb::sql::Thing,
        }

        // Find repo id by exact name or sanitized variants
        let mut repo_id: Option<String> = None;
        let select_repo = "SELECT id FROM repo WHERE name = $name LIMIT 1";
        if let Ok(mut r) = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(select_repo)
                    .bind(("name", repo_name.to_string()))
                    .await
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn
                    .query(select_repo)
                    .bind(("name", repo_name.to_string()))
                    .await
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn
                    .query(select_repo)
                    .bind(("name", repo_name.to_string()))
                    .await
            }
        } {
            if let Ok(rows) = r.take::<Vec<RepoRow>>(0) {
                if let Some(rr) = rows.into_iter().next() {
                    repo_id = Some(rr.id.to_string());
                }
            }
        }

        // If branch provided, try to match repo by name and branch
        if repo_id.is_none() {
            if let Some(br) = branch {
                let select_repo_b =
                    "SELECT id FROM repo WHERE name = $name AND branch = $branch LIMIT 1";
                if let Ok(mut r) = match &*self.db {
                    SurrealConnection::Local(db_conn) => {
                        db_conn
                            .query(select_repo_b)
                            .bind(("name", repo_name.to_string()))
                            .bind(("branch", br.to_string()))
                            .await
                    }
                    SurrealConnection::RemoteHttp(db_conn) => {
                        db_conn
                            .query(select_repo_b)
                            .bind(("name", repo_name.to_string()))
                            .bind(("branch", br.to_string()))
                            .await
                    }
                    SurrealConnection::RemoteWs(db_conn) => {
                        db_conn
                            .query(select_repo_b)
                            .bind(("name", repo_name.to_string()))
                            .bind(("branch", br.to_string()))
                            .await
                    }
                } {
                    if let Ok(rows) = r.take::<Vec<RepoRow>>(0) {
                        if let Some(rr) = rows.into_iter().next() {
                            repo_id = Some(rr.id.to_string());
                        }
                    }
                }
            }
        }

        // If still none, we will attempt to lookup by repo record id heuristic of repo:<sanitized>
        if repo_id.is_none() {
            let explicit = format!(
                "repo:{}",
                repo_name.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
            );
            repo_id = Some(explicit);
        }

        let repo_id = repo_id.unwrap();
        log::info!(
            "get_dependencies_for_repo: resolved repo identifier for '{}' -> {}",
            repo_name,
            repo_id
        );

        // Query related dependencies using traversal if supported, otherwise fallback to selecting dependency by relation
        // Try traversal: SELECT array::distinct((SELECT out FROM type::thing($id)->depends_on)) AS deps
        let traversal_sql =
            "SELECT array::distinct((SELECT out FROM type::thing($id)->depends_on)) AS deps";
        if let Ok(mut r) = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(traversal_sql)
                    .bind(("id", repo_id.clone()))
                    .await
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn
                    .query(traversal_sql)
                    .bind(("id", repo_id.clone()))
                    .await
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn
                    .query(traversal_sql)
                    .bind(("id", repo_id.clone()))
                    .await
            }
        } {
            #[derive(Deserialize)]
            struct DepsRow {
                deps: Option<Vec<surrealdb::sql::Thing>>,
            }
            if let Ok(rows) = r.take::<Vec<DepsRow>>(0) {
                if let Some(first) = rows.into_iter().next() {
                    let mut out: Vec<serde_json::Value> = Vec::new();
                    if let Some(dep_things) = first.deps {
                        log::info!(
                            "get_dependencies_for_repo: traversal lookup for repo='{}' returned {} dependency things",
                            repo_name,
                            dep_things.len()
                        );
                        // Bulk fetch dependency records by id
                        if dep_things.is_empty() {
                            log::info!(
                                "get_dependencies_for_repo: traversal returned zero dependency things for repo='{}'",
                                repo_name
                            );
                            return Ok(vec![]);
                        }
                        let mut exprs: Vec<String> = Vec::new();
                        for (i, _) in dep_things.iter().enumerate() {
                            exprs.push(format!("type::thing($d{})", i));
                        }
                        let sql2 = format!(
                            "SELECT name, language, version FROM dependency WHERE id IN [{}]",
                            exprs.join(", ")
                        );
                        if let Ok(mut r2) = match &*self.db {
                            SurrealConnection::Local(db_conn) => {
                                let mut q = db_conn.query(&sql2);
                                for (i, t) in dep_things.iter().enumerate() {
                                    q = q.bind((format!("d{}", i), t.to_string()));
                                }
                                q.await
                            }
                            SurrealConnection::RemoteHttp(db_conn) => {
                                let mut q = db_conn.query(&sql2);
                                for (i, t) in dep_things.iter().enumerate() {
                                    q = q.bind((format!("d{}", i), t.to_string()));
                                }
                                q.await
                            }
                            SurrealConnection::RemoteWs(db_conn) => {
                                let mut q = db_conn.query(&sql2);
                                for (i, t) in dep_things.iter().enumerate() {
                                    q = q.bind((format!("d{}", i), t.to_string()));
                                }
                                q.await
                            }
                        } {
                            if let Ok(rows2) = r2.take::<Vec<serde_json::Value>>(0) {
                                for dep in rows2.into_iter() {
                                    out.push(dep);
                                }
                            }
                        }
                    }
                    let out = sort_deps_alphabetical(out);
                    return Ok(out);
                }
            }
        }

        // Fallback: explicitly query relation table for depends_on edges, then bulk fetch dependencies
        // Compare using `type::thing($id)` so binding the string id (e.g. "repo:kafka") matches Thing columns.
        let rel_sql =
            "SELECT out FROM relation WHERE in = type::thing($id) AND rname = 'depends_on'";
        #[derive(Deserialize)]
        struct RelOutRow {
            out: surrealdb::sql::Thing,
        }
        if let Ok(mut r) = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                db_conn.query(rel_sql).bind(("id", repo_id.clone())).await
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn.query(rel_sql).bind(("id", repo_id.clone())).await
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn.query(rel_sql).bind(("id", repo_id.clone())).await
            }
        } {
            if let Ok(rel_rows) = r.take::<Vec<RelOutRow>>(0) {
                let outs: Vec<surrealdb::sql::Thing> =
                    rel_rows.into_iter().map(|r| r.out).collect();
                log::info!(
                    "get_dependencies_for_repo: relation fallback returned {} outs for repo='{}'",
                    outs.len(),
                    repo_name
                );
                if !outs.is_empty() {
                    // Build parameterized thing list for IN query
                    let mut exprs: Vec<String> = Vec::new();
                    for (i, _) in outs.iter().enumerate() {
                        exprs.push(format!("type::thing($o{})", i));
                    }
                    let sql2 = format!(
                        "SELECT name, language, version FROM dependency WHERE id IN [{}]",
                        exprs.join(", ")
                    );
                    if let Ok(mut r2) = match &*self.db {
                        SurrealConnection::Local(db_conn) => {
                            let mut q = db_conn.query(&sql2);
                            for (i, t) in outs.iter().enumerate() {
                                q = q.bind((format!("o{}", i), t.to_string()));
                            }
                            q.await
                        }
                        SurrealConnection::RemoteHttp(db_conn) => {
                            let mut q = db_conn.query(&sql2);
                            for (i, t) in outs.iter().enumerate() {
                                q = q.bind((format!("o{}", i), t.to_string()));
                            }
                            q.await
                        }
                        SurrealConnection::RemoteWs(db_conn) => {
                            let mut q = db_conn.query(&sql2);
                            for (i, t) in outs.iter().enumerate() {
                                q = q.bind((format!("o{}", i), t.to_string()));
                            }
                            q.await
                        }
                    } {
                        if let Ok(rows2) = r2.take::<Vec<serde_json::Value>>(0) {
                            let rows2 = sort_deps_alphabetical(rows2);
                            return Ok(rows2);
                        }
                    }
                }
            }
        }

        // Final fallback: some environments persist explicit `depends_on` rows
        // (table named `depends_on`) instead of using SurrealDB relation graph.
        // Compare using `type::thing($id)` so binding the string id (e.g. "repo:kafka") matches Thing columns.
        let dep_table_sql = "SELECT out FROM depends_on WHERE in = type::thing($id)";
        #[derive(Deserialize)]
        struct DepOutRow {
            out: surrealdb::sql::Thing,
        }
        if let Ok(mut r) = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(dep_table_sql)
                    .bind(("id", repo_id.clone()))
                    .await
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                db_conn
                    .query(dep_table_sql)
                    .bind(("id", repo_id.clone()))
                    .await
            }
            SurrealConnection::RemoteWs(db_conn) => {
                db_conn
                    .query(dep_table_sql)
                    .bind(("id", repo_id.clone()))
                    .await
            }
        } {
            if let Ok(dep_rows) = r.take::<Vec<DepOutRow>>(0) {
                let outs: Vec<surrealdb::sql::Thing> =
                    dep_rows.into_iter().map(|r| r.out).collect();
                log::info!(
                    "get_dependencies_for_repo: depends_on table fallback returned {} outs for repo='{}'",
                    outs.len(),
                    repo_name
                );
                if !outs.is_empty() {
                    let mut exprs: Vec<String> = Vec::new();
                    for (i, _) in outs.iter().enumerate() {
                        exprs.push(format!("type::thing($o{})", i));
                    }
                    let sql2 = format!(
                        "SELECT name, language, version FROM dependency WHERE id IN [{}]",
                        exprs.join(", ")
                    );
                    if let Ok(mut r2) = match &*self.db {
                        SurrealConnection::Local(db_conn) => {
                            let mut q = db_conn.query(&sql2);
                            for (i, t) in outs.iter().enumerate() {
                                q = q.bind((format!("o{}", i), t.to_string()));
                            }
                            q.await
                        }
                        SurrealConnection::RemoteHttp(db_conn) => {
                            let mut q = db_conn.query(&sql2);
                            for (i, t) in outs.iter().enumerate() {
                                q = q.bind((format!("o{}", i), t.to_string()));
                            }
                            q.await
                        }
                        SurrealConnection::RemoteWs(db_conn) => {
                            let mut q = db_conn.query(&sql2);
                            for (i, t) in outs.iter().enumerate() {
                                q = q.bind((format!("o{}", i), t.to_string()));
                            }
                            q.await
                        }
                    } {
                        if let Ok(rows2) = r2.take::<Vec<serde_json::Value>>(0) {
                            let rows2 = sort_deps_alphabetical(rows2);
                            return Ok(rows2);
                        }
                    }
                }
            }
        }

        // No canonical dependency rows found. As a graceful fallback, try
        // to derive dependencies from stored SBOMs for this repo (if any).
        log::info!("get_dependencies_for_repo: no canonical dependencies found for repo={}; attempting SBOM fallback", repo_name);
        match self.get_sbom_dependencies_for_repo(repo_name, branch).await {
            Ok(sbom_deps) if !sbom_deps.is_empty() => {
                log::info!(
                    "get_dependencies_for_repo: returning {} SBOM-derived dependencies for repo={}",
                    sbom_deps.len(),
                    repo_name
                );
                let sbom_deps = sort_deps_alphabetical(sbom_deps);
                return Ok(sbom_deps);
            }
            Ok(_) => {
                log::info!(
                    "get_dependencies_for_repo: SBOM fallback returned no dependencies for repo={}",
                    repo_name
                );
            }
            Err(e) => {
                log::warn!(
                    "get_dependencies_for_repo: SBOM fallback failed for {}: {}",
                    repo_name,
                    e
                );
            }
        }

        Ok(vec![])
    }

    /// Get SBOM dependencies for a specific repository, optionally scoped to a commit.
    /// Returns a vector of JSON objects with name, language, version fields.
    pub async fn get_sbom_dependencies_for_repo(
        &self,
        repo_name: &str,
        commit: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        log::info!("*** NEW CODE IS RUNNING: get_sbom_dependencies_for_repo called with repo={} commit={:?}", repo_name, commit);

        // If no commit specified, find the default branch commit
        // If a ref name (branch/tag) is specified, resolve it to a commit hash
        let commit_to_use = if let Some(c) = commit {
            // Check if this looks like a commit hash (40 hex chars) or a ref name
            if c.len() == 40 && c.chars().all(|ch| ch.is_ascii_hexdigit()) {
                // It's already a commit hash
                c.to_string()
            } else {
                // Try to resolve it as a ref (branch or tag)
                // Support both short names like "main" and full names like "refs/heads/main"
                let ref_patterns = if c.starts_with("refs/") {
                    vec![c.to_string()]
                } else {
                    vec![
                        format!("refs/heads/{}", c),
                        format!("refs/tags/{}", c),
                        c.to_string(), // also try as-is
                    ]
                };

                let refs_query = "SELECT VALUE target FROM refs WHERE repo = $repo AND name IN $ref_names LIMIT 1";

                let resolved_commit: Option<String> = match &*self.db {
                    SurrealConnection::Local(db_conn) => {
                        let mut result = db_conn
                            .query(refs_query)
                            .bind(("repo", repo_name.to_string()))
                            .bind(("ref_names", ref_patterns.clone()))
                            .await?;
                        let db_val: DbValue = result.take(0)?;
                        if let Some(json_val) = db_value_to_json(db_val) {
                            if let Some(arr) = json_val.as_array() {
                                arr.first().and_then(|v| v.as_str()).map(String::from)
                            } else {
                                json_val.as_str().map(String::from)
                            }
                        } else {
                            None
                        }
                    }
                    SurrealConnection::RemoteHttp(db_conn) => {
                        let mut result = db_conn
                            .query(refs_query)
                            .bind(("repo", repo_name.to_string()))
                            .bind(("ref_names", ref_patterns.clone()))
                            .await?;
                        let db_val: DbValue = result.take(0)?;
                        if let Some(json_val) = db_value_to_json(db_val) {
                            if let Some(arr) = json_val.as_array() {
                                arr.first().and_then(|v| v.as_str()).map(String::from)
                            } else {
                                json_val.as_str().map(String::from)
                            }
                        } else {
                            None
                        }
                    }
                    SurrealConnection::RemoteWs(db_conn) => {
                        let mut result = db_conn
                            .query(refs_query)
                            .bind(("repo", repo_name.to_string()))
                            .bind(("ref_names", ref_patterns.clone()))
                            .await?;
                        let db_val: DbValue = result.take(0)?;
                        if let Some(json_val) = db_value_to_json(db_val) {
                            if let Some(arr) = json_val.as_array() {
                                arr.first().and_then(|v| v.as_str()).map(String::from)
                            } else {
                                json_val.as_str().map(String::from)
                            }
                        } else {
                            None
                        }
                    }
                };

                if let Some(resolved) = resolved_commit {
                    log::info!(
                        "get_sbom_dependencies_for_repo: resolved ref '{}' to commit: {}",
                        c,
                        resolved
                    );
                    resolved
                } else {
                    // Couldn't resolve as ref, assume it's a commit hash (maybe abbreviated)
                    log::info!(
                        "get_sbom_dependencies_for_repo: could not resolve '{}' as ref, using as commit hash",
                        c
                    );
                    c.to_string()
                }
            }
        } else {
            // Query refs table to find default branch (main, master, or trunk)
            // TODO: Store the actual default branch name when first processing the repo
            // by querying git's HEAD symbolic ref, rather than hardcoding common names.
            let refs_query = "SELECT VALUE target FROM refs WHERE repo = $repo AND name IN ['refs/heads/main', 'refs/heads/master', 'refs/heads/trunk'] LIMIT 1";

            let default_commit: Option<String> = match &*self.db {
                SurrealConnection::Local(db_conn) => {
                    let mut result = db_conn
                        .query(refs_query)
                        .bind(("repo", repo_name.to_string()))
                        .await?;
                    let db_val: DbValue = result.take(0)?;
                    if let Some(json_val) = db_value_to_json(db_val) {
                        if let Some(arr) = json_val.as_array() {
                            arr.first().and_then(|v| v.as_str()).map(String::from)
                        } else {
                            json_val.as_str().map(String::from)
                        }
                    } else {
                        None
                    }
                }
                SurrealConnection::RemoteHttp(db_conn) => {
                    let mut result = db_conn
                        .query(refs_query)
                        .bind(("repo", repo_name.to_string()))
                        .await?;
                    let db_val: DbValue = result.take(0)?;
                    if let Some(json_val) = db_value_to_json(db_val) {
                        if let Some(arr) = json_val.as_array() {
                            arr.first().and_then(|v| v.as_str()).map(String::from)
                        } else {
                            json_val.as_str().map(String::from)
                        }
                    } else {
                        None
                    }
                }
                SurrealConnection::RemoteWs(db_conn) => {
                    let mut result = db_conn
                        .query(refs_query)
                        .bind(("repo", repo_name.to_string()))
                        .await?;
                    let db_val: DbValue = result.take(0)?;
                    if let Some(json_val) = db_value_to_json(db_val) {
                        if let Some(arr) = json_val.as_array() {
                            arr.first().and_then(|v| v.as_str()).map(String::from)
                        } else {
                            json_val.as_str().map(String::from)
                        }
                    } else {
                        None
                    }
                }
            };

            if let Some(commit) = default_commit {
                log::info!(
                    "get_sbom_dependencies_for_repo: found default commit: {}",
                    commit
                );
                commit
            } else {
                log::warn!(
                    "get_sbom_dependencies_for_repo: no default branch found for repo={}",
                    repo_name
                );
                return Ok(vec![]);
            }
        };

        log::info!(
            "get_sbom_dependencies_for_repo: using commit: {}",
            commit_to_use
        );

        // Now find the SBOM for this repo and commit
        let sbom_query = "SELECT id FROM sboms WHERE repo = $repo AND (commit = $commit OR commit IS NONE) AND id IS NOT NONE LIMIT 1";

        log::debug!(
            "get_sbom_dependencies_for_repo: executing query: {} with repo=$repo='{}'",
            sbom_query,
            repo_name
        );

        // Extract sbom_id as a string directly to match sbom_id field format in sbom_deps
        let sbom_id_string: Option<String> = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                let mut result = db_conn
                    .query(sbom_query)
                    .bind(("repo", repo_name.to_string()))
                    .bind(("commit", commit_to_use.clone()))
                    .await?;

                #[derive(serde::Deserialize)]
                struct SbomRow {
                    id: surrealdb::sql::Thing,
                }

                let sbom_rows: Vec<SbomRow> = match result.take(0) {
                    Ok(rows) => rows,
                    Err(e) => {
                        log::debug!(
                            "get_sbom_dependencies_for_repo: no SBOM found (take failed): {}",
                            e
                        );
                        return Ok(vec![]);
                    }
                };

                if sbom_rows.is_empty() {
                    log::info!(
                        "get_sbom_dependencies_for_repo: no SBOM found for repo={}",
                        repo_name
                    );
                    return Ok(vec![]);
                }

                let sbom_id = sbom_rows[0].id.to_string();
                log::info!(
                    "get_sbom_dependencies_for_repo: found SBOM with id={}",
                    sbom_id
                );
                Some(sbom_id)
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                let mut result = db_conn
                    .query(sbom_query)
                    .bind(("repo", repo_name.to_string()))
                    .bind(("commit", commit_to_use.clone()))
                    .await?;

                #[derive(serde::Deserialize)]
                struct SbomRow {
                    id: surrealdb::sql::Thing,
                }

                let sbom_rows: Vec<SbomRow> = match result.take(0) {
                    Ok(rows) => rows,
                    Err(e) => {
                        log::debug!(
                            "get_sbom_dependencies_for_repo: no SBOM found (take failed): {}",
                            e
                        );
                        return Ok(vec![]);
                    }
                };

                if sbom_rows.is_empty() {
                    log::info!(
                        "get_sbom_dependencies_for_repo: no SBOM found for repo={}",
                        repo_name
                    );
                    return Ok(vec![]);
                }

                let sbom_id = sbom_rows[0].id.to_string();
                log::info!(
                    "get_sbom_dependencies_for_repo: found SBOM with id={}",
                    sbom_id
                );
                Some(sbom_id)
            }
            SurrealConnection::RemoteWs(db_conn) => {
                let mut result = db_conn
                    .query(sbom_query)
                    .bind(("repo", repo_name.to_string()))
                    .bind(("commit", commit_to_use.clone()))
                    .await?;

                #[derive(serde::Deserialize)]
                struct SbomRow {
                    id: surrealdb::sql::Thing,
                }

                let sbom_rows: Vec<SbomRow> = match result.take(0) {
                    Ok(rows) => rows,
                    Err(e) => {
                        log::debug!(
                            "get_sbom_dependencies_for_repo: no SBOM found (take failed): {}",
                            e
                        );
                        return Ok(vec![]);
                    }
                };

                if sbom_rows.is_empty() {
                    log::info!(
                        "get_sbom_dependencies_for_repo: no SBOM found for repo={}",
                        repo_name
                    );
                    return Ok(vec![]);
                }

                let sbom_id = sbom_rows[0].id.to_string();
                log::info!(
                    "get_sbom_dependencies_for_repo: found SBOM with id={}",
                    sbom_id
                );
                Some(sbom_id)
            }
        };

        if let Some(sbom_id_string) = sbom_id_string {
            // Now get the dependencies for this SBOM using the string sbom_id
            log::info!(
                "get_sbom_dependencies_for_repo: looking up deps for sbom_id={}",
                sbom_id_string
            );
            let deps_sql = "SELECT name, version, purl FROM sbom_deps WHERE sbom_id = $sbom_id";

            let deps: Vec<serde_json::Value> = match &*self.db {
                SurrealConnection::Local(db_conn) => {
                    let mut result = db_conn
                        .query(deps_sql)
                        .bind(("sbom_id", sbom_id_string.clone()))
                        .await?;
                    match result.take::<Vec<serde_json::Value>>(0) {
                        Ok(rows) => {
                            log::info!(
                                "get_sbom_dependencies_for_repo: retrieved {} dependency rows",
                                rows.len()
                            );
                            rows
                        }
                        Err(e) => {
                            log::warn!(
                                "get_sbom_dependencies_for_repo: failed to deserialize rows: {}",
                                e
                            );
                            return Err(e.into());
                        }
                    }
                }
                SurrealConnection::RemoteHttp(db_conn) => {
                    let mut result = db_conn
                        .query(deps_sql)
                        .bind(("sbom_id", sbom_id_string.clone()))
                        .await?;
                    match result.take::<Vec<serde_json::Value>>(0) {
                        Ok(rows) => {
                            log::info!(
                                "get_sbom_dependencies_for_repo: retrieved {} dependency rows",
                                rows.len()
                            );
                            rows
                        }
                        Err(e) => {
                            log::warn!(
                                "get_sbom_dependencies_for_repo: failed to deserialize rows: {}",
                                e
                            );
                            return Err(e.into());
                        }
                    }
                }
                SurrealConnection::RemoteWs(db_conn) => {
                    let mut result = db_conn
                        .query(deps_sql)
                        .bind(("sbom_id", sbom_id_string.clone()))
                        .await?;
                    match result.take::<Vec<serde_json::Value>>(0) {
                        Ok(rows) => {
                            log::info!(
                                "get_sbom_dependencies_for_repo: retrieved {} dependency rows",
                                rows.len()
                            );
                            rows
                        }
                        Err(e) => {
                            log::warn!(
                                "get_sbom_dependencies_for_repo: failed to deserialize rows: {}",
                                e
                            );
                            return Err(e.into());
                        }
                    }
                }
            };

            log::info!(
                "get_sbom_dependencies_for_repo: returning {} dependencies",
                deps.len()
            );
            let deps = sort_deps_alphabetical(deps);
            Ok(deps)
        } else {
            log::info!(
                "get_sbom_dependencies_for_repo: no SBOM found for repo={}",
                repo_name
            );
            // No SBOM found for this repo
            Ok(vec![])
        }
    }

    // Graph-based entity relation fetch using calls/imports edges.
    pub async fn fetch_entity_graph(
        &self,
        stable_id: &str,
        limit: usize,
    ) -> Result<
        (
            Vec<(String, String)>,
            Vec<(String, String)>,
            Vec<(String, String)>,
        ),
        Box<dyn std::error::Error>,
    > {
        // Delegate to the shared graph API which already handles SurrealDB Thing/enum
        // forms and falls back to tolerant raw JSON parsing when needed.
        let cfg = crate::graph_api::GraphDbConfig::from_env();
        match crate::graph_api::fetch_entity_graph(&cfg, stable_id, limit).await {
            Ok(g) => {
                let callers = g
                    .callers
                    .into_iter()
                    .map(|r| (r.name, r.stable_id))
                    .collect::<Vec<(String, String)>>();
                let callees = g
                    .callees
                    .into_iter()
                    .map(|r| (r.name, r.stable_id))
                    .collect::<Vec<(String, String)>>();
                let imports = g
                    .imports
                    .into_iter()
                    .map(|r| (r.name, r.stable_id))
                    .collect::<Vec<(String, String)>>();
                Ok((callers, callees, imports))
            }
            Err(e) => {
                log::error!(
                    "fetch_entity_graph: delegated graph fetch failed for {}: {}",
                    stable_id,
                    e
                );
                Err(Box::<dyn std::error::Error>::from(e))
            }
        }
    }

    pub async fn search_entities(
        &self,
        query: &str,
        repo_filter: Option<&str>,
        _limit: usize,
        _offset: usize,
        _fields: &str,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        // Use a fixed set of fields and parameterized binds to avoid SQL injection and SELECT *
        let _fields = "e.id, es.file, e.language, e.kind, e.name, e.parent, e.signature, es.start_line, es.end_line, es.doc, e.rank, es.imports, es.unresolved_imports, e.stable_id, e.repo_name, es.source_url, es.source_display";

        let mut response = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                // Use embedded record fields
                let sql = "SELECT <string>id AS id, language, kind, name, rank, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content FROM entity WHERE string::contains(name, $q) LIMIT 10".to_string();
                if let Some(repo) = repo_filter {
                    // append repo filter by building a new query with starts_with
                    let sql = "SELECT <string>id AS id, language, kind, name, rank, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content FROM entity WHERE string::contains(name, $q) AND string::starts_with(repo_name, $repo) LIMIT 10".to_string();
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("repo", repo.to_string()))
                        .await?
                } else {
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .await?
                }
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                // Use embedded record fields
                let sql = "SELECT <string>id AS id, language, kind, name, rank, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content FROM entity WHERE string::contains(name, $q) LIMIT 10".to_string();
                if let Some(repo) = repo_filter {
                    // append repo filter by building a new query with starts_with
                    let sql = "SELECT <string>id AS id, language, kind, name, rank, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content FROM entity WHERE string::contains(name, $q) AND string::starts_with(repo_name, $repo) LIMIT 10".to_string();
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("repo", repo.to_string()))
                        .await?
                } else {
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .await?
                }
            }
            SurrealConnection::RemoteWs(db_conn) => {
                // Use embedded record fields
                let sql = "SELECT <string>id AS id, language, kind, name, rank, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content FROM entity WHERE string::contains(name, $q) LIMIT 10".to_string();
                if let Some(repo) = repo_filter {
                    // append repo filter by building a new query with starts_with
                    let sql = "SELECT <string>id AS id, language, kind, name, rank, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content FROM entity WHERE string::contains(name, $q) AND string::starts_with(repo_name, $repo) LIMIT 10".to_string();
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("repo", repo.to_string()))
                        .await?
                } else {
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .await?
                }
            }
        };

        let entities: Vec<EntityPayload> = match response.take::<Vec<EntityPayload>>(0) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "search_entities: failed to deserialize response for q='{}' repo={:?}: {}",
                    query,
                    repo_filter,
                    e
                );
                return Err(Box::new(e));
            }
        };

        Ok(entities)
    }

    /// Search entities allowing multiple repo filters. Each repo filter may be
    /// either a repo name (matched against `repo_name`) or a file path prefix
    /// (if it starts with `/` or contains a leading path). When multiple repo
    /// names are provided, this uses `repo_name IN $repos` for database-side
    /// filtering to avoid multiple roundtrips.
    pub async fn search_entities_multi(
        &self,
        query: &str,
        repo_filters: Option<&[String]>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        let fields = "<string>id AS id, language, kind, name, snapshot.page_rank_value AS page_rank_value, repo_name, signature, stable_id, snapshot.file AS file, snapshot.parent AS parent, snapshot.start_line AS start_line, snapshot.end_line AS end_line, snapshot.doc AS doc, snapshot.imports AS imports, snapshot.unresolved_imports AS unresolved_imports, snapshot.methods AS methods, snapshot.source_url AS source_url, snapshot.source_display AS source_display, snapshot.calls AS calls, snapshot.source_content AS source_content";

        // If we have repo_filters, split into repo_names (for repo_name IN) and
        // path_prefixes (for string::starts_with on file)
        let mut repo_names: Vec<String> = Vec::new();
        let mut path_prefixes: Vec<String> = Vec::new();
        if let Some(rfs) = repo_filters {
            for r in rfs.iter() {
                if r.starts_with('/') || r.contains('/') {
                    path_prefixes.push(r.clone());
                } else {
                    repo_names.push(r.clone());
                }
            }
        }

        // Prefer repo_name IN when we have at least one repo_name; otherwise,
        // if there are path_prefixes, use a starts_with filter for the first one
        // (legacy behavior).
        let mut response = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                if !repo_names.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND repo_name IN $repos ORDER BY snapshot.page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND repo_name IN $repos ORDER BY snapshot.page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind((
                            "repos",
                            serde_json::Value::Array(
                                repo_names
                                    .iter()
                                    .map(|s| serde_json::Value::String(s.clone()))
                                    .collect(),
                            ),
                        ))
                        .await?
                } else if !path_prefixes.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND string::starts_with(snapshot.file ?? '', $repo) ORDER BY snapshot.page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND string::starts_with(snapshot.file ?? '', $repo) ORDER BY snapshot.page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("repo", path_prefixes[0].clone()))
                        .await?
                } else {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) ORDER BY snapshot.page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) ORDER BY snapshot.page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .await?
                }
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                if !repo_names.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND repo_name IN $repos ORDER BY snapshot.page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND repo_name IN $repos ORDER BY snapshot.page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind((
                            "repos",
                            serde_json::Value::Array(
                                repo_names
                                    .iter()
                                    .map(|s| serde_json::Value::String(s.clone()))
                                    .collect(),
                            ),
                        ))
                        .await?
                } else if !path_prefixes.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND string::starts_with(snapshot.file ?? '', $repo) ORDER BY snapshot.page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND string::starts_with(snapshot.file ?? '', $repo) ORDER BY snapshot.page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("repo", path_prefixes[0].clone()))
                        .await?
                } else {
                    let q0 = if offset == 0 {
                        db_conn.query(format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) ORDER BY snapshot.page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        ))
                    } else {
                        db_conn.query(format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) ORDER BY snapshot.page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        ))
                    };
                    q0.bind(("q", query.to_lowercase())).await?
                }
            }
            SurrealConnection::RemoteWs(db_conn) => {
                if !repo_names.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND repo_name IN $repos ORDER BY snapshot.page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND repo_name IN $repos ORDER BY snapshot.page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind((
                            "repos",
                            serde_json::Value::Array(
                                repo_names
                                    .iter()
                                    .map(|s| serde_json::Value::String(s.clone()))
                                    .collect(),
                            ),
                        ))
                        .await?
                } else if !path_prefixes.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND string::starts_with(snapshot.file ?? '', $repo) ORDER BY snapshot.page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) AND string::starts_with(snapshot.file ?? '', $repo) ORDER BY snapshot.page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("repo", path_prefixes[0].clone()))
                        .await?
                } else {
                    let q0 = if offset == 0 {
                        db_conn.query(format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) ORDER BY snapshot.page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        ))
                    } else {
                        db_conn.query(format!(
                            "SELECT {} FROM entity WHERE string::contains(name, $q) ORDER BY snapshot.page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        ))
                    };
                    q0.bind(("q", query.to_lowercase())).await?
                }
            }
        };

        let entities: Vec<EntityPayload> = match response.take::<Vec<EntityPayload>>(0) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "search_entities_multi: failed to deserialize response for q='{}' repo_filters={:?}: {}",
                    query,
                    repo_filters,
                    e
                );
                return Err(Box::new(e));
            }
        };
        Ok(entities)
    }

    /// Snapshot-scoped variant of search_entities_multi. When a snapshot_id is
    /// provided, this queries `entity_snapshot` instead of `entity` and applies
    /// the same repo/path filtering semantics.
    pub async fn search_entities_multi_snapshot(
        &self,
        query: &str,
        repo_filters: Option<&[String]>,
        limit: usize,
        offset: usize,
        snapshot_id: &str,
    ) -> Result<Vec<EntityPayload>, Box<dyn std::error::Error>> {
        // Fields for entity_snapshot are top-level (not under `snapshot.`)
        let fields = "<string>id AS id, language, kind, name, page_rank_value, repo_name, signature, stable_id, file AS file, parent AS parent, start_line AS start_line, end_line AS end_line, doc AS doc, imports AS imports, unresolved_imports AS unresolved_imports, methods AS methods, source_url AS source_url, source_display AS source_display, calls AS calls, source_content AS source_content";

        // Split repo_filters into repo_names and path_prefixes as in the non-snapshot variant
        let mut repo_names: Vec<String> = Vec::new();
        let mut path_prefixes: Vec<String> = Vec::new();
        if let Some(rfs) = repo_filters {
            for r in rfs.iter() {
                if r.starts_with('/') || r.contains('/') {
                    path_prefixes.push(r.clone());
                } else {
                    repo_names.push(r.clone());
                }
            }
        }

        let mut response = match &*self.db {
            SurrealConnection::Local(db_conn) => {
                if !repo_names.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND repo_name IN $repos ORDER BY page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND repo_name IN $repos ORDER BY page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("sid", snapshot_id.to_string()))
                        .bind((
                            "repos",
                            serde_json::Value::Array(
                                repo_names
                                    .iter()
                                    .map(|s| serde_json::Value::String(s.clone()))
                                    .collect(),
                            ),
                        ))
                        .await?
                } else if !path_prefixes.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND string::starts_with(file ?? '', $repo) ORDER BY page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND string::starts_with(file ?? '', $repo) ORDER BY page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("sid", snapshot_id.to_string()))
                        .bind(("repo", path_prefixes[0].clone()))
                        .await?
                } else {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) ORDER BY page_rank_value DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) ORDER BY page_rank_value DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("sid", snapshot_id.to_string()))
                        .await?
                }
            }
            SurrealConnection::RemoteHttp(db_conn) => {
                if !repo_names.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND repo_name IN $repos ORDER BY rank DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND repo_name IN $repos ORDER BY rank DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("sid", snapshot_id.to_string()))
                        .bind((
                            "repos",
                            serde_json::Value::Array(
                                repo_names
                                    .iter()
                                    .map(|s| serde_json::Value::String(s.clone()))
                                    .collect(),
                            ),
                        ))
                        .await?
                } else if !path_prefixes.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND string::starts_with(file ?? '', $repo) ORDER BY rank DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND string::starts_with(file ?? '', $repo) ORDER BY rank DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("sid", snapshot_id.to_string()))
                        .bind(("repo", path_prefixes[0].clone()))
                        .await?
                } else {
                    let q0 = if offset == 0 {
                        db_conn.query(format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) ORDER BY rank DESC LIMIT {}",
                            fields,
                            limit
                        ))
                    } else {
                        db_conn.query(format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) ORDER BY rank DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        ))
                    };
                    q0.bind(("q", query.to_lowercase()))
                        .bind(("sid", snapshot_id.to_string()))
                        .await?
                }
            }
            SurrealConnection::RemoteWs(db_conn) => {
                if !repo_names.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND repo_name IN $repos ORDER BY rank DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND repo_name IN $repos ORDER BY rank DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("sid", snapshot_id.to_string()))
                        .bind((
                            "repos",
                            serde_json::Value::Array(
                                repo_names
                                    .iter()
                                    .map(|s| serde_json::Value::String(s.clone()))
                                    .collect(),
                            ),
                        ))
                        .await?
                } else if !path_prefixes.is_empty() {
                    let sql = if offset == 0 {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND string::starts_with(file ?? '', $repo) ORDER BY rank DESC LIMIT {}",
                            fields,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) AND string::starts_with(file ?? '', $repo) ORDER BY rank DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        )
                    };
                    db_conn
                        .query(&sql)
                        .bind(("q", query.to_lowercase()))
                        .bind(("sid", snapshot_id.to_string()))
                        .bind(("repo", path_prefixes[0].clone()))
                        .await?
                } else {
                    let q0 = if offset == 0 {
                        db_conn.query(format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) ORDER BY rank DESC LIMIT {}",
                            fields,
                            limit
                        ))
                    } else {
                        db_conn.query(format!(
                            "SELECT {} FROM entity_snapshot WHERE snapshot_id = $sid AND string::contains(name, $q) ORDER BY rank DESC START AT {} LIMIT {}",
                            fields,
                            offset,
                            limit
                        ))
                    };
                    q0.bind(("q", query.to_lowercase()))
                        .bind(("sid", snapshot_id.to_string()))
                        .await?
                }
            }
        };

        let entities: Vec<EntityPayload> = match response.take::<Vec<EntityPayload>>(0) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "search_entities_multi_snapshot: failed to deserialize response for q='{}' repo_filters={:?} snapshot_id={} : {}",
                    query,
                    repo_filters,
                    snapshot_id,
                    e
                );
                return Err(Box::new(e));
            }
        };
        Ok(entities)
    }
}
