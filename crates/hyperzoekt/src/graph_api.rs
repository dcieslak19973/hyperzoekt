//! Public API to fetch an entity's callers, callees, and imports via SurrealDB graph edges.

use serde::{Deserialize, Serialize};
use surrealdb::sql::{Id, Thing};
use surrealdb::Surreal;

// Helper: normalize SurrealDB id/value representations into stable strings.
fn extract_surreal_id_value(v: &serde_json::Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        return Some(s.to_string());
    }
    if let Some(obj) = v.as_object() {
        if let (Some(tb), Some(idv)) = (obj.get("tb"), obj.get("id")) {
            if let (Some(tb_s), Some(id_s)) = (tb.as_str(), idv.as_str()) {
                return Some(format!("{}:{}", tb_s, id_s));
            }
        }
        if let Some(idv) = obj.get("id").and_then(|x| x.as_str()) {
            return Some(idv.to_string());
        }
    }
    None
}

// Parse a serde_json::Value row returned by SurrealDB for graph queries and
// convert to EntityRef vectors. This accepts string or object elements
// (Thing objects) in the arrays.
fn parse_graph_value_row(
    row: &serde_json::Value,
) -> (Vec<EntityRef>, Vec<EntityRef>, Vec<EntityRef>) {
    let mut callers = Vec::new();
    let mut callees = Vec::new();
    let mut imports = Vec::new();

    let push_pairs = |arr_n: Option<&serde_json::Value>,
                      arr_i: Option<&serde_json::Value>,
                      out: &mut Vec<EntityRef>| {
        if let (Some(nv), Some(iv)) = (arr_n, arr_i) {
            if let (Some(names), Some(ids)) = (nv.as_array(), iv.as_array()) {
                for (idx, namev) in names.iter().enumerate() {
                    // Normalize name to string if possible
                    let name = if let Some(n) = namev.as_str() {
                        n.to_string()
                    } else if namev.is_object() {
                        extract_surreal_id_value(namev).unwrap_or_default()
                    } else {
                        continue;
                    };

                    let id = ids
                        .get(idx)
                        .and_then(|v| {
                            if let Some(s) = v.as_str() {
                                Some(s.to_string())
                            } else {
                                extract_surreal_id_value(v)
                            }
                        })
                        .unwrap_or_default();

                    out.push(EntityRef {
                        name,
                        stable_id: id,
                    });
                }
            }
        }
    };

    push_pairs(row.get("callers_name"), row.get("callers_id"), &mut callers);
    push_pairs(row.get("callees_name"), row.get("callees_id"), &mut callees);
    push_pairs(row.get("imports_name"), row.get("imports_id"), &mut imports);

    (callers, callees, imports)
}

async fn fetch_relations_fallback<C: surrealdb::Connection>(
    conn: &Surreal<C>,
    snapshot_id: &str,
    limit: usize,
) -> anyhow::Result<(Vec<EntityRef>, Vec<EntityRef>, Vec<EntityRef>)> {
    #[derive(Deserialize)]
    struct OutRow {
        out: Thing,
    }

    #[derive(Deserialize)]
    struct InRow {
        #[serde(rename = "in")]
        inbound: Thing,
    }

    async fn query_outgoing<C: surrealdb::Connection>(
        conn: &Surreal<C>,
        table: &str,
        snapshot_id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<Thing>> {
        let sql = format!(
            "SELECT out FROM {} WHERE in = type::thing($raw) LIMIT $limit",
            table
        );
        let mut resp = conn
            .query(sql)
            .bind(("raw", snapshot_id.to_string()))
            .bind(("limit", limit as i64))
            .await?;
        let rows: Vec<OutRow> = resp.take(0)?;
        Ok(rows.into_iter().map(|row| row.out).collect())
    }

    async fn query_incoming<C: surrealdb::Connection>(
        conn: &Surreal<C>,
        table: &str,
        snapshot_id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<Thing>> {
        let sql = format!(
            "SELECT in FROM {} WHERE out = type::thing($raw) LIMIT $limit",
            table
        );
        let mut resp = conn
            .query(sql)
            .bind(("raw", snapshot_id.to_string()))
            .bind(("limit", limit as i64))
            .await?;
        let rows: Vec<InRow> = resp.take(0)?;
        Ok(rows.into_iter().map(|row| row.inbound).collect())
    }

    fn stable_id_from_thing(thing: &Thing) -> String {
        match &thing.id {
            Id::String(s) => s.clone(),
            other => other.to_string(),
        }
    }

    fn table_from_thing(thing: &Thing) -> String {
        thing.tb.clone()
    }

    async fn hydrate_names<C: surrealdb::Connection>(
        conn: &Surreal<C>,
        ids: &[String],
    ) -> anyhow::Result<std::collections::HashMap<String, String>> {
        #[derive(Deserialize)]
        struct EntityNameRow {
            stable_id: String,
            name: Option<String>,
        }

        if ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let mut resp = conn
            .query("SELECT stable_id, name FROM entity WHERE stable_id INSIDE $ids")
            .bind(("ids", ids.to_vec()))
            .await?;
        let rows: Vec<EntityNameRow> = resp.take(0)?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let name = row.name.or_else(|| Some(row.stable_id.clone()));
                name.map(|n| (row.stable_id, n))
            })
            .collect())
    }

    let mut callers_things = query_incoming(conn, "calls", snapshot_id, limit)
        .await
        .unwrap_or_default();
    let mut callees_things = query_outgoing(conn, "calls", snapshot_id, limit)
        .await
        .unwrap_or_default();
    let mut imports_things = query_outgoing(conn, "imports", snapshot_id, limit)
        .await
        .unwrap_or_default();

    callers_things.truncate(limit);
    callees_things.truncate(limit);
    imports_things.truncate(limit);

    let mut all_ids = Vec::new();
    let extract_ids = |things: &[Thing]| {
        things
            .iter()
            .map(stable_id_from_thing)
            .collect::<Vec<String>>()
    };

    let callers_ids = extract_ids(&callers_things);
    let callees_ids = extract_ids(&callees_things);
    let imports_ids = extract_ids(&imports_things);

    all_ids.extend(callers_ids.iter().cloned());
    all_ids.extend(callees_ids.iter().cloned());
    all_ids.extend(imports_ids.iter().cloned());

    let mut seen = std::collections::HashSet::new();
    let unique_ids: Vec<String> = all_ids
        .iter()
        .filter(|id| seen.insert((*id).clone()))
        .cloned()
        .collect();

    let name_map = hydrate_names(conn, &unique_ids).await.unwrap_or_default();

    let build_refs = |ids: Vec<String>, things: Vec<Thing>| {
        ids.into_iter()
            .zip(things.into_iter())
            .map(|(sid, thing)| {
                let table = table_from_thing(&thing);
                let default_name = format!("{}:{}", table, sid);
                let name = name_map.get(&sid).cloned().unwrap_or(default_name);
                EntityRef {
                    name,
                    stable_id: sid,
                }
            })
            .collect::<Vec<_>>()
    };

    let callers = build_refs(callers_ids, callers_things);
    let callees = build_refs(callees_ids, callees_things);
    let imports = build_refs(imports_ids, imports_things);

    Ok((callers, callees, imports))
}

#[derive(Clone, Debug, Default)]
pub struct GraphDbConfig {
    pub surreal_url: Option<String>,
    pub surreal_ns: String,
    pub surreal_db: String,
    pub surreal_username: Option<String>,
    pub surreal_password: Option<String>,
}

impl GraphDbConfig {
    pub fn from_env() -> Self {
        Self {
            surreal_url: std::env::var("SURREALDB_URL").ok(),
            surreal_ns: std::env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".into()),
            surreal_db: std::env::var("SURREAL_DB").unwrap_or_else(|_| "repos".into()),
            surreal_username: std::env::var("SURREALDB_USERNAME").ok(),
            surreal_password: std::env::var("SURREALDB_PASSWORD").ok(),
        }
    }
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct EntityRef {
    pub name: String,
    pub stable_id: String,
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct EntityGraph {
    pub callers: Vec<EntityRef>,
    pub callees: Vec<EntityRef>,
    pub imports: Vec<EntityRef>,
}

#[derive(Deserialize)]
struct EntityIdRow {
    id: surrealdb::sql::Thing,
    name: Option<String>,
}

#[derive(Deserialize)]
struct GraphRow {
    callers_name: Option<Vec<String>>,
    callers_id: Option<Vec<String>>,
    callees_name: Option<Vec<String>>,
    callees_id: Option<Vec<String>>,
    imports_name: Option<Vec<String>>,
    imports_id: Option<Vec<String>>,
}

const GRAPH_TRAVERSAL_QUERY: &str = r#"
SELECT
    ->calls->entity_snapshot.name AS callees_name,
    ->calls->entity_snapshot.stable_id AS callees_id,
    <-calls<-entity_snapshot.name AS callers_name,
    <-calls<-entity_snapshot.stable_id AS callers_id,
    ->imports->entity_snapshot.name AS imports_name,
    ->imports->entity_snapshot.stable_id AS imports_id
FROM type::thing($raw) LIMIT 1
"#;

pub async fn fetch_entity_graph(
    cfg: &GraphDbConfig,
    stable_id: &str,
    limit: usize,
) -> anyhow::Result<EntityGraph> {
    use crate::db::connection::{connect, SurrealConnection as HZConn};

    let url_opt = cfg.surreal_url.clone();
    let user_opt = cfg.surreal_username.clone();
    let pass_opt = cfg.surreal_password.clone();

    let conn = connect(
        &url_opt,
        &user_opt,
        &pass_opt,
        &cfg.surreal_ns,
        &cfg.surreal_db,
    )
    .await?;

    match conn {
        HZConn::Local(arc) => {
            // Arc<Surreal<Mem>> -> &Surreal<Mem>
            fetch_entity_graph_with_conn(arc.as_ref(), stable_id, limit).await
        }
        HZConn::RemoteHttp(c) => fetch_entity_graph_with_conn(&c, stable_id, limit).await,
        HZConn::RemoteWs(c) => fetch_entity_graph_with_conn(&c, stable_id, limit).await,
    }
}

enum SurrealConn {
    Mem(Surreal<surrealdb::engine::local::Db>), // May be shared embedded
    Http(Surreal<surrealdb::engine::remote::http::Client>),
    Ws(Surreal<surrealdb::engine::remote::ws::Client>),
}

async fn query_graph(
    conn: &SurrealConn,
    stable_id: &str,
    limit: usize,
) -> anyhow::Result<EntityGraph> {
    async fn inner<C: surrealdb::Connection>(
        c: &Surreal<C>,
        stable_id: &str,
        limit: usize,
    ) -> anyhow::Result<EntityGraph> {
        let mut resp = c
            .query("SELECT id, name FROM entity WHERE stable_id = $sid LIMIT 1")
            .bind(("sid", stable_id.to_string()))
            .await?;
        let rows: Vec<EntityIdRow> = resp.take(0)?;
        if rows.is_empty() {
            return Ok(EntityGraph {
                callers: vec![],
                callees: vec![],
                imports: vec![],
            });
        }
        let ent_id = rows[0].id.to_string();
        if ent_id.is_empty() {
            return Ok(EntityGraph {
                callers: vec![],
                callees: vec![],
                imports: vec![],
            });
        }
        // Convert entity ID to entity_snapshot ID (assuming same sanitized stable_id)
        let snapshot_id = format!(
            "entity_snapshot:{}",
            ent_id.split(':').next_back().unwrap_or(&ent_id)
        );
        let mut resp2 = c
            .query(GRAPH_TRAVERSAL_QUERY.trim())
            .bind(("raw", snapshot_id.clone()))
            .await?;
        let mut callers = Vec::new();
        let mut callees = Vec::new();
        let mut imports = Vec::new();
        // Try to deserialize into the typed `GraphRow`. If that fails (for example
        // when SurrealDB returns Thing objects or other non-string enums), fall back
        // to taking the raw serde_json::Value and normalize using
        // `parse_graph_value_row` which tolerates Thing objects.
        match resp2.take::<Vec<GraphRow>>(0) {
            Ok(grows) => {
                if let Some(row) = grows.into_iter().next() {
                    let build = |names: Option<Vec<String>>, ids: Option<Vec<String>>| {
                        let mut v = Vec::new();
                        if let (Some(n), Some(i)) = (names, ids) {
                            for (n1, id1) in n.into_iter().zip(i.into_iter()) {
                                v.push(EntityRef {
                                    name: n1,
                                    stable_id: id1,
                                });
                            }
                        }
                        v
                    };
                    callers = build(row.callers_name, row.callers_id);
                    callees = build(row.callees_name, row.callees_id);
                    imports = build(row.imports_name, row.imports_id);
                }
            }
            Err(e) => {
                log::debug!(
                        "graph_api: typed GraphRow deserialization failed, falling back to raw JSON parse: {}",
                        e
                    );
                // Try to inspect the raw response to aid debugging
                match resp2.take::<Vec<serde_json::Value>>(0) {
                    Ok(raw_rows) => {
                        // Log the raw row shape to help diagnose SurrealDB enum/Thing returns
                        log::debug!("graph_api: raw graph rows: {:?}", raw_rows);
                        if let Some(row) = raw_rows.into_iter().next() {
                            let (c, ce, im) = parse_graph_value_row(&row);
                            callers = c.into_iter().collect();
                            callees = ce.into_iter().collect();
                            imports = im.into_iter().collect();
                        }
                    }
                    Err(e2) => {
                        log::debug!(
                            "graph_api: failed to take raw JSON rows after GraphRow failure: {}",
                            e2
                        );
                    }
                }
            }
        }

        if callers.is_empty() && callees.is_empty() && imports.is_empty() {
            if let Ok((fb_callers, fb_callees, fb_imports)) =
                fetch_relations_fallback(c, snapshot_id.as_str(), limit).await
            {
                callers = fb_callers;
                callees = fb_callees;
                imports = fb_imports;
            }
        }
        callers.truncate(limit);
        callees.truncate(limit);
        imports.truncate(limit);
        Ok(EntityGraph {
            callers,
            callees,
            imports,
        })
    }

    match conn {
        SurrealConn::Mem(c) => inner(c, stable_id, limit).await,
        SurrealConn::Http(c) => inner(c, stable_id, limit).await,
        SurrealConn::Ws(c) => inner(c, stable_id, limit).await,
    }
}

pub async fn fetch_entity_graph_with_conn<C: surrealdb::Connection>(
    conn: &Surreal<C>,
    stable_id: &str,
    limit: usize,
) -> anyhow::Result<EntityGraph> {
    let mut resp = conn
        .query("SELECT id, name FROM entity WHERE stable_id = $sid LIMIT 1")
        .bind(("sid", stable_id.to_string()))
        .await?;
    let rows: Vec<EntityIdRow> = resp.take(0)?;
    if rows.is_empty() {
        return Ok(EntityGraph {
            callers: vec![],
            callees: vec![],
            imports: vec![],
        });
    }
    let ent_id = rows[0].id.to_string();
    if ent_id.is_empty() {
        return Ok(EntityGraph {
            callers: vec![],
            callees: vec![],
            imports: vec![],
        });
    }
    // Convert entity ID to entity_snapshot ID (assuming same sanitized stable_id)
    let snapshot_id = format!(
        "entity_snapshot:{}",
        ent_id.split(':').next_back().unwrap_or(&ent_id)
    );
    let mut resp2 = conn
        .query(GRAPH_TRAVERSAL_QUERY.trim())
        .bind(("raw", snapshot_id.clone()))
        .await?;
    let mut callers = Vec::new();
    let mut callees = Vec::new();
    let mut imports = Vec::new();
    // Same fallback logic as above: prefer typed deserialization, but tolerate
    // Thing objects and other forms by parsing raw JSON values.
    match resp2.take::<Vec<GraphRow>>(0) {
        Ok(grows) => {
            if let Some(row) = grows.into_iter().next() {
                let build = |names: Option<Vec<String>>, ids: Option<Vec<String>>| {
                    let mut v = Vec::new();
                    if let (Some(n), Some(i)) = (names, ids) {
                        for (n1, id1) in n.into_iter().zip(i.into_iter()) {
                            v.push(EntityRef {
                                name: n1,
                                stable_id: id1,
                            });
                        }
                    }
                    v
                };
                callers = build(row.callers_name, row.callers_id);
                callees = build(row.callees_name, row.callees_id);
                imports = build(row.imports_name, row.imports_id);
            }
        }
        Err(e) => {
            log::debug!(
                "graph_api: typed GraphRow deserialization failed (conn), falling back to raw JSON parse: {}",
                e
            );
            match resp2.take::<Vec<serde_json::Value>>(0) {
                Ok(raw_rows) => {
                    if let Some(row) = raw_rows.into_iter().next() {
                        let (c, ce, im) = parse_graph_value_row(&row);
                        callers = c.into_iter().collect();
                        callees = ce.into_iter().collect();
                        imports = im.into_iter().collect();
                    }
                }
                Err(e2) => {
                    log::debug!(
                        "graph_api: failed to take raw JSON rows after GraphRow failure (conn): {}",
                        e2
                    );
                }
            }
        }
    }

    if callers.is_empty() && callees.is_empty() && imports.is_empty() {
        if let Ok((fb_callers, fb_callees, fb_imports)) =
            fetch_relations_fallback(conn, snapshot_id.as_str(), limit).await
        {
            callers = fb_callers;
            callees = fb_callees;
            imports = fb_imports;
        }
    }
    callers.truncate(limit);
    callees.truncate(limit);
    imports.truncate(limit);
    Ok(EntityGraph {
        callers,
        callees,
        imports,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use surrealdb::engine::local::Mem;

    struct RootCredentialGuard {
        saved_user: Option<String>,
        saved_pass: Option<String>,
    }

    impl RootCredentialGuard {
        fn new() -> Self {
            let saved_user = std::env::var("SURREALDB_USERNAME").ok();
            let saved_pass = std::env::var("SURREALDB_PASSWORD").ok();
            std::env::set_var("SURREALDB_USERNAME", "root");
            std::env::set_var("SURREALDB_PASSWORD", "root");
            Self {
                saved_user,
                saved_pass,
            }
        }
    }

    impl Drop for RootCredentialGuard {
        fn drop(&mut self) {
            if let Some(user) = &self.saved_user {
                std::env::set_var("SURREALDB_USERNAME", user);
            } else {
                std::env::remove_var("SURREALDB_USERNAME");
            }
            if let Some(pass) = &self.saved_pass {
                std::env::set_var("SURREALDB_PASSWORD", pass);
            } else {
                std::env::remove_var("SURREALDB_PASSWORD");
            }
        }
    }

    async fn relation_exists(
        conn: &crate::db::connection::SurrealConnection,
        table: &str,
    ) -> Result<bool, String> {
        #[derive(serde::Deserialize)]
        struct CountRow {
            cnt: Option<usize>,
        }

        let query = format!(
            "SELECT count() AS cnt FROM {} WHERE in = type::thing('entity_snapshot:A') AND out = type::thing('entity_snapshot:B')",
            table
        );

        match conn.query(&query).await {
            Ok(mut resp) => match resp.take::<Vec<CountRow>>(0) {
                Ok(rows) => Ok(rows.into_iter().next().and_then(|r| r.cnt).unwrap_or(0) > 0),
                Err(e) => Err(format!(
                    "unable to deserialize {} relation count: {}",
                    table, e
                )),
            },
            Err(e) => Err(format!("unable to query {} relation count: {}", table, e)),
        }
    }

    #[tokio::test]
    async fn graph_fetch_no_entity() {
        // Ensure this test uses an in-memory SurrealDB even when the environment
        // defines a remote SURREALDB_URL (as CI does). Save and restore any
        // existing env vars to avoid leaking state between tests.
        let saved_url = std::env::var("SURREALDB_URL").ok();
        let saved_user = std::env::var("SURREALDB_USERNAME").ok();
        let saved_pass = std::env::var("SURREALDB_PASSWORD").ok();
        let saved_disable = std::env::var("HZ_DISABLE_SURREAL_ENV").ok();
        std::env::remove_var("SURREALDB_URL");
        std::env::remove_var("SURREALDB_USERNAME");
        std::env::remove_var("SURREALDB_PASSWORD");
        // Force the connect() helper to ignore environment-specified SurrealDB
        // endpoints for the duration of this test.
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");

        let cfg = GraphDbConfig {
            surreal_url: None,
            surreal_ns: "testns".into(),
            surreal_db: "testdb".into(),
            ..Default::default()
        };
        let g = fetch_entity_graph(&cfg, "missing", 5).await.unwrap();
        assert!(g.callers.is_empty());

        if let Some(v) = saved_url {
            std::env::set_var("SURREALDB_URL", v);
        }
        if let Some(v) = saved_user {
            std::env::set_var("SURREALDB_USERNAME", v);
        }
        if let Some(v) = saved_pass {
            std::env::set_var("SURREALDB_PASSWORD", v);
        }
        if let Some(v) = saved_disable {
            std::env::set_var("HZ_DISABLE_SURREAL_ENV", v);
        } else {
            std::env::remove_var("HZ_DISABLE_SURREAL_ENV");
        }
    }

    #[tokio::test]
    async fn graph_fetch_with_edges_empty() {
        let cfg = GraphDbConfig {
            surreal_url: None,
            surreal_ns: "testns".into(),
            surreal_db: "testdb2".into(),
            ..Default::default()
        };
        let db = Surreal::new::<Mem>(()).await.unwrap();
        db.use_ns(&cfg.surreal_ns)
            .use_db(&cfg.surreal_db)
            .await
            .unwrap();
        db.query("CREATE entity SET stable_id='A', name='FnA', file='r/f.rs', language='rust', kind='function', start_line=1, end_line=2, imports=[], unresolved_imports=[], repo_name='r'").await.unwrap();
        let g = fetch_entity_graph_with_conn(&db, "A", 10).await.unwrap();
        assert!(g.callers.is_empty() && g.callees.is_empty() && g.imports.is_empty());
    }

    #[tokio::test]
    async fn graph_fetch_with_edges_populated() {
        // This test exercises RELATE / graph traversal which the in-memory
        // Surreal engine does not reliably support. Require a remote
        // SurrealDB (via SURREALDB_URL) to run this test. If the env var is
        // not present, skip the test so CI/local runs without a graph-capable
        // Surreal aren't marked as failures.
        if std::env::var("SURREALDB_URL").is_err() {
            eprintln!("Skipping graph_fetch_with_edges_populated: SURREALDB_URL not set (remote Surreal required)");
            return;
        }

        // Use the configured remote Surreal to create entities and RELATE them.
        // Ensure we exercise the graph traversal with the privileged root credentials so
        // relation creation/traversal isn't blocked by restrictive roles.
        let _root_guard = RootCredentialGuard::new();
        let cfg = GraphDbConfig::from_env();
        use crate::db::connection::connect;
        let conn = match connect(
            &cfg.surreal_url,
            &cfg.surreal_username,
            &cfg.surreal_password,
            &cfg.surreal_ns,
            &cfg.surreal_db,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "Skipping graph_fetch_with_edges_populated: unable to connect to SURREALDB_URL: {}",
                    e
                );
                return;
            }
        };

        // Clean up any existing test entities
        if let Err(e) = conn
            .query("DELETE entity WHERE stable_id IN ['A', 'B']; DELETE entity_snapshot WHERE stable_id IN ['A', 'B']")
            .await
        {
            eprintln!("Skipping graph test; cleanup failed: {}", e);
            return;
        }

        // Define tables for relations
        if let Err(e) = conn
            .query("DEFINE TABLE calls TYPE RELATION FROM entity_snapshot TO entity_snapshot; DEFINE TABLE imports TYPE RELATION FROM entity_snapshot TO entity_snapshot;")
            .await
        {
            eprintln!("Skipping graph test; define tables failed: {}", e);
            return;
        }

        // Create entities and relations on the remote Surreal instance.
        if let Err(e) = conn
            .query("CREATE entity:`A` SET stable_id='A', name='FnA', file='r/f.rs', language='rust', kind='function', start_line=1, end_line=2, imports=[], unresolved_imports=[], repo_name='r'")
            .await
        {
            eprintln!("Skipping graph test; create A failed: {}", e);
            return;
        }
        if let Err(e) = conn
            .query("CREATE entity_snapshot:`A` SET stable_id='A', name='FnA', file='r/f.rs', language='rust', kind='function', start_line=1, end_line=2, imports=[], unresolved_imports=[], repo_name='r'")
            .await
        {
            eprintln!("Skipping graph test; create A snapshot failed: {}", e);
            return;
        }
        if let Err(e) = conn
            .query("CREATE entity:`B` SET stable_id='B', name='FnB', file='r/f.rs', language='rust', kind='function', start_line=3, end_line=4, imports=[], unresolved_imports=[], repo_name='r'")
            .await
        {
            eprintln!("Skipping graph test; create B failed: {}", e);
            return;
        }
        if let Err(e) = conn
            .query("CREATE entity_snapshot:`B` SET stable_id='B', name='FnB', file='r/f.rs', language='rust', kind='function', start_line=3, end_line=4, imports=[], unresolved_imports=[], repo_name='r'")
            .await
        {
            eprintln!("Skipping graph test; create B snapshot failed: {}", e);
            return;
        }
        if let Err(e) = conn
            .query("RELATE entity_snapshot:A -> calls -> entity_snapshot:B; RELATE entity_snapshot:A -> imports -> entity_snapshot:B;")
            .await
        {
            eprintln!("RELATE failed: {}", e);
            return;
        } else {
            eprintln!("RELATE succeeded");
        }

        let calls_relation_present = relation_exists(&conn, "calls")
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "Graph test cannot verify 'calls' relation presence: {}. Ensure the SurrealDB user has SELECT permissions on the relation table.",
                    err
                );
            });
        assert!(
            calls_relation_present,
            "Calls relation was not persisted despite RELATE succeeding. Ensure the SurrealDB role can CREATE and SELECT on relation tables."
        );

        let imports_relation_present = relation_exists(&conn, "imports")
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "Graph test cannot verify 'imports' relation presence: {}. Ensure the SurrealDB user has SELECT permissions on the relation table.",
                    err
                );
            });
        assert!(
            imports_relation_present,
            "Imports relation was not persisted despite RELATE succeeding. Ensure the SurrealDB role can CREATE and SELECT on relation tables."
        );

        // Debug: check records
        if let Ok(mut r) = conn.query("SELECT * FROM entity_snapshot").await {
            if let Ok(rows) = r.take::<Vec<serde_json::Value>>(0) {
                eprintln!("DEBUG entity_snapshot table: {:?}", rows);
            }
        }
        if let Ok(mut r) = conn.query("SELECT * FROM calls").await {
            if let Ok(rows) = r.take::<Vec<serde_json::Value>>(0) {
                eprintln!("DEBUG calls table: {:?}", rows);
            }
        }
        if let Ok(mut r) = conn.query("SELECT ->calls FROM entity_snapshot:A").await {
            if let Ok(rows) = r.take::<Vec<serde_json::Value>>(0) {
                eprintln!("DEBUG calls from A: {:?}", rows);
            }
        }

        let g_a = fetch_entity_graph(&cfg, "A", 10).await.unwrap();
        eprintln!("DEBUG g_a: {:?}", g_a);
        let g_b = fetch_entity_graph(&cfg, "B", 10).await.unwrap();

        let creds = cfg
            .surreal_username
            .as_deref()
            .unwrap_or("configured SurrealDB user");

        assert!(
            !g_a.callees.is_empty(),
            "Graph traversal returned no callees for entity A even though relations exist. Confirm that {} has permission to traverse relation edges (SELECT on calls).",
            creds
        );
        assert!(
            !g_a.imports.is_empty(),
            "Graph traversal returned no imports for entity A even though relations exist. Confirm that {} has permission to traverse relation edges (SELECT on imports).",
            creds
        );
        assert!(
            !g_b.callers.is_empty(),
            "Graph traversal returned no callers for entity B even though relations exist. Confirm that {} has permission to traverse relation edges (SELECT on calls).",
            creds
        );

        assert_eq!(
            g_a.callees.len(),
            1,
            "expected one callee after traversing calls relation"
        );
        assert_eq!(
            g_a.imports.len(),
            1,
            "expected one import edge after traversing imports relation"
        );
        assert_eq!(
            g_b.callers.len(),
            1,
            "expected one caller after traversing calls relation"
        );
    }

    #[test]
    fn parse_graph_value_row_handles_things() {
        // Simulate a SurrealDB row where ids are Thing objects and names are strings
        let row = serde_json::json!({
            "callers_name": ["CallerFn"],
            "callers_id": [{"tb":"entity","id":"1"}],
            "callees_name": ["CalleeFn"],
            "callees_id": [{"tb":"entity","id":"2"}],
            "imports_name": ["ImpFn"],
            "imports_id": [{"tb":"entity","id":"3"}]
        });

        let (callers, callees, imports) = parse_graph_value_row(&row);
        assert_eq!(callers.len(), 1);
        assert_eq!(callers[0].name, "CallerFn");
        assert_eq!(callers[0].stable_id, "entity:1");

        assert_eq!(callees.len(), 1);
        assert_eq!(callees[0].name, "CalleeFn");
        assert_eq!(callees[0].stable_id, "entity:2");

        assert_eq!(imports.len(), 1);
        assert_eq!(imports[0].name, "ImpFn");
        assert_eq!(imports[0].stable_id, "entity:3");
    }
}
