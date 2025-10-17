use super::writer::sanitize_json_strings;
use crate::db::connection::SurrealConnection;
use anyhow::Result;
use serde_json::Value;

/// Struct used to provide parameters for `upsert_entity_snapshot`.
#[derive(Debug)]
pub struct EntitySnapshotUpsert<'a> {
    pub snapshot_id: &'a str,
    pub stable_id: &'a str,
    pub entity_id: &'a str,
    pub commit_id: Option<&'a str>,
    pub file: Option<&'a str>,
    pub parent: Option<&'a str>,
    pub start_line: Option<u64>,
    pub end_line: Option<u64>,
    pub doc: Option<&'a str>,
    pub imports: &'a [crate::repo_index::indexer::payload::ImportItem],
    pub unresolved_imports: &'a [crate::repo_index::indexer::payload::UnresolvedImport],
    pub methods: &'a [crate::repo_index::indexer::payload::MethodItem],
    pub source_url: Option<&'a str>,
    pub source_display: Option<&'a str>,
    pub calls: &'a [String],
    pub source_content: Option<&'a str>,
}

/// Upsert or create an entity_snapshot record for a given snapshot and entity.
pub async fn upsert_entity_snapshot(
    conn: &SurrealConnection,
    up: &EntitySnapshotUpsert<'_>,
) -> Result<()> {
    log::info!(
        "upsert_entity_snapshot called with snapshot_id={}, stable_id={}",
        up.snapshot_id,
        up.stable_id,
    );
    // Build content object via serde_json to avoid manual escaping
    let mut content = serde_json::Map::new();
    let id = format!("{}:{}", up.snapshot_id, up.stable_id);
    // Note: id is now specified in CREATE statement, not in content object
    content.insert(
        "snapshot_id".to_string(),
        Value::String(up.snapshot_id.to_string()),
    );
    content.insert(
        "stable_id".to_string(),
        Value::String(up.stable_id.to_string()),
    );
    content.insert(
        "entity_id".to_string(),
        Value::String(up.entity_id.to_string()),
    );
    if let Some(commit_id) = up.commit_id {
        content.insert(
            "commit_id".to_string(),
            Value::String(commit_id.to_string()),
        );
    }
    if let Some(f) = up.file {
        content.insert("file".to_string(), Value::String(f.to_string()));
    }
    if let Some(p) = up.parent {
        content.insert("parent".to_string(), Value::String(p.to_string()));
    }
    if let Some(sv) = up.start_line {
        content.insert(
            "start_line".to_string(),
            Value::Number(serde_json::Number::from(sv)),
        );
    }
    if let Some(ev) = up.end_line {
        content.insert(
            "end_line".to_string(),
            Value::Number(serde_json::Number::from(ev)),
        );
    }
    if let Some(d) = up.doc {
        content.insert("doc".to_string(), Value::String(d.to_string()));
    }
    if !up.imports.is_empty() {
        content.insert(
            "imports".to_string(),
            serde_json::to_value(up.imports).unwrap_or(Value::Array(vec![])),
        );
    }
    if !up.unresolved_imports.is_empty() {
        content.insert(
            "unresolved_imports".to_string(),
            serde_json::to_value(up.unresolved_imports).unwrap_or(Value::Array(vec![])),
        );
    }
    if !up.methods.is_empty() {
        content.insert(
            "methods".to_string(),
            serde_json::to_value(up.methods).unwrap_or(Value::Array(vec![])),
        );
    }
    if let Some(su) = up.source_url {
        content.insert("source_url".to_string(), Value::String(su.to_string()));
    }
    if let Some(sd) = up.source_display {
        content.insert("source_display".to_string(), Value::String(sd.to_string()));
    }
    if !up.calls.is_empty() {
        content.insert(
            "calls".to_string(),
            Value::Array(up.calls.iter().map(|s| Value::String(s.clone())).collect()),
        );
    }
    if let Some(sc) = up.source_content {
        content.insert(
            "source_content".to_string(),
            Value::String(sc.replace('\0', " ")),
        );
    }

    let content_value = Value::Object(content);
    // binds are constructed inline at call sites below; no local binds needed.

    // 1) Update existing snapshot row
    let update_sql = format!("UPDATE entity_snapshot:`{}` SET entity_id = $entity_id, commit_id = $commit_id, file = $file, parent = $parent, start_line = $start_line, end_line = $end_line, doc = $doc, imports = $imports, unresolved_imports = $unresolved_imports, methods = $methods, source_url = $source_url, source_display = $source_display, calls = $calls, source_content = $source_content;", id);
    let _ = conn
        .query_with_binds(
            &update_sql,
            vec![
                (
                    "entity_id",
                    serde_json::Value::String(up.entity_id.to_string()),
                ),
                (
                    "commit_id",
                    match up.commit_id {
                        Some(s) => serde_json::Value::String(s.to_string()),
                        None => serde_json::Value::Null,
                    },
                ),
                (
                    "file",
                    match up.file {
                        Some(s) => serde_json::Value::String(s.to_string()),
                        None => serde_json::Value::Null,
                    },
                ),
                (
                    "parent",
                    match up.parent {
                        Some(s) => serde_json::Value::String(s.to_string()),
                        None => serde_json::Value::Null,
                    },
                ),
                (
                    "start_line",
                    match up.start_line {
                        Some(n) => serde_json::Value::Number(serde_json::Number::from(n)),
                        None => serde_json::Value::Null,
                    },
                ),
                (
                    "end_line",
                    match up.end_line {
                        Some(n) => serde_json::Value::Number(serde_json::Number::from(n)),
                        None => serde_json::Value::Null,
                    },
                ),
                (
                    "doc",
                    match up.doc {
                        Some(s) => serde_json::Value::String(s.to_string()),
                        None => serde_json::Value::Null,
                    },
                ),
                (
                    "imports",
                    serde_json::to_value(up.imports).unwrap_or(Value::Array(vec![])),
                ),
                (
                    "unresolved_imports",
                    serde_json::to_value(up.unresolved_imports).unwrap_or(Value::Array(vec![])),
                ),
                (
                    "methods",
                    serde_json::to_value(up.methods).unwrap_or(Value::Array(vec![])),
                ),
                (
                    "source_url",
                    match up.source_url {
                        Some(s) => serde_json::Value::String(s.to_string()),
                        None => serde_json::Value::Null,
                    },
                ),
                (
                    "source_display",
                    match up.source_display {
                        Some(s) => serde_json::Value::String(s.to_string()),
                        None => serde_json::Value::Null,
                    },
                ),
                (
                    "calls",
                    Value::Array(up.calls.iter().map(|s| Value::String(s.clone())).collect()),
                ),
                (
                    "source_content",
                    match up.source_content {
                        Some(s) => serde_json::Value::String(s.to_string()),
                        None => serde_json::Value::Null,
                    },
                ),
            ],
        )
        .await;

    // 2) Create the snapshot row if missing
    let mut exists = false;
    // Compare by record id using a record literal with backtick-escaped id
    let select_sql = format!(
        "SELECT id FROM entity_snapshot WHERE id = entity_snapshot:`{}` LIMIT 1;",
        id
    );
    if let Ok(mut sel) = conn.query(&select_sql).await {
        if let Ok(rows) = sel.take::<Vec<serde_json::Value>>(0) {
            if !rows.is_empty() {
                exists = true;
            }
        }
    }
    if !exists {
        let create_sql = format!("CREATE entity_snapshot:`{}` CONTENT $content;", id);
        let _ = conn
            .query_with_binds(
                &create_sql,
                vec![("content", serde_json::to_value(&content_value)?)],
            )
            .await;
    }

    // 3) Ensure created_at token is set when missing
    let created_at_sql = format!(
        "UPDATE entity_snapshot:`{}` SET created_at = time::now() WHERE created_at = NONE;",
        id
    );
    let _ = conn.query(&created_at_sql).await?;

    // 4) No separate content table: embeddings and source_content are stored
    //    directly on the entity_snapshot record. Nothing to relate here.

    Ok(())
}

/// Create an entity snapshot for a repository commit.
#[allow(clippy::too_many_arguments)]
pub async fn create_entity_snapshot(
    conn: &SurrealConnection,
    repo_name: &str,
    commit_id: &str,
    tree: Option<&str>,
    author: Option<&str>,
    message: Option<&str>,
    snapshot_id: &str,
    size_bytes: u64,
) -> Result<()> {
    let name = format!("repo:{}", repo_name);
    let signature = format!("{}:{}", commit_id, tree.unwrap_or(""));
    let doc = message.unwrap_or("");
    let sanitized_doc = {
        let mut val = serde_json::Value::String(doc.to_string());
        sanitize_json_strings(&mut val);
        val.as_str().unwrap_or("").to_string()
    };
    let _content = if !sanitized_doc.is_empty() {
        sanitized_doc.clone()
    } else {
        format!("{}:{}", name, signature)
    };

    // Create entity_snapshot
    let mut content_obj = serde_json::Map::new();
    content_obj.insert(
        "snapshot_id".to_string(),
        serde_json::Value::String(snapshot_id.to_string()),
    );
    content_obj.insert(
        "repo_name".to_string(),
        serde_json::Value::String(repo_name.to_string()),
    );
    content_obj.insert(
        "commit_id".to_string(),
        serde_json::Value::String(commit_id.to_string()),
    );
    if let Some(tree) = tree {
        content_obj.insert(
            "tree_id".to_string(),
            serde_json::Value::String(tree.to_string()),
        );
    }
    if let Some(author) = author {
        content_obj.insert(
            "author".to_string(),
            serde_json::Value::String(author.to_string()),
        );
    }
    if let Some(message) = message {
        content_obj.insert(
            "message".to_string(),
            serde_json::Value::String(message.to_string()),
        );
    }
    content_obj.insert(
        "size_bytes".to_string(),
        serde_json::Value::Number(serde_json::Number::from(size_bytes)),
    );
    let content_value = serde_json::Value::Object(content_obj);

    let create_sql = format!("CREATE entity_snapshot:`{}` CONTENT $content;", snapshot_id);
    conn.query_with_binds(&create_sql, vec![("content", content_value)])
        .await?;

    Ok(())
}
