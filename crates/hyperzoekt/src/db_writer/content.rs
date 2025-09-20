use crate::db_writer::connection::SurrealConnection;
use anyhow::Result;
use serde_json::Value;

/// Upsert a content row keyed by content_id (e.g. sha256 hex). Stores raw text
/// and marks embedding as pending. Idempotent: only creates if not exists.
pub async fn upsert_content_if_missing(
    conn: &SurrealConnection,
    content_id: &str,
    text: &str,
) -> Result<()> {
    // Build content as a serde_json object and serialize safely.
    let mut content = serde_json::Map::new();
    content.insert("id".to_string(), Value::String(content_id.to_string()));
    content.insert("text".to_string(), Value::String(text.to_string()));
    content.insert(
        "embedding_status".to_string(),
        Value::String("pending".to_string()),
    );
    // Use binds for content to avoid manual string assembly. We'll create the
    // content object without `created_at` and then ensure `created_at` is set
    // to `time::now()` only when missing. This keeps Surreal tokens unquoted
    // while avoiding format!-based injection risks.
    let content_value = Value::Object(content);

    // 1) Update existing row text if present
    let _ = conn
        .query_with_binds(
            "UPDATE content SET text = $text WHERE id = $id;",
            vec![
                ("text", text.to_string().into()),
                ("id", content_id.to_string().into()),
            ],
        )
        .await;

    // 2) Create the content row if it does not already exist (SELECT then CREATE)
    let mut exists = false;
    if let Ok(mut sel) = conn
        .query_with_binds(
            "SELECT id FROM content WHERE id = $id LIMIT 1;",
            vec![("id", content_id.to_string().into())],
        )
        .await
    {
        if let Ok(rows) = sel.take::<Vec<serde_json::Value>>(0) {
            if !rows.is_empty() {
                exists = true;
            }
        }
    }

    if !exists {
        let _ = conn
            .query_with_binds(
                "CREATE content CONTENT $content;",
                vec![("content", serde_json::to_value(&content_value)?)],
            )
            .await;
    }

    // 3) Ensure created_at token is set (only if missing)
    let _ = conn
        .query_with_binds(
            "UPDATE content SET created_at = time::now() WHERE id = $id AND created_at = NONE;",
            vec![("id", content_id.to_string().into())],
        )
        .await?;
    Ok(())
}

/// Write an embedding for a content_id. Overwrites existing embedding row.
pub async fn write_content_embedding(
    conn: &SurrealConnection,
    content_id: &str,
    embedding: Vec<f32>,
) -> Result<()> {
    // Store as JSON array for simplicity. Build the embedding object via serde
    let embed_val = serde_json::to_value(&embedding)?;
    let mut obj = serde_json::Map::new();
    obj.insert("id".to_string(), Value::String(content_id.to_string()));
    obj.insert("embedding".to_string(), embed_val);
    obj.insert(
        "embedding_len".to_string(),
        Value::Number(serde_json::Number::from(embedding.len())),
    );
    let obj_value = Value::Object(obj);

    // 1) Update existing embedding if present
    let _ = conn
        .query_with_binds(
            "UPDATE content_embedding SET embedding = $embedding, embedding_len = $len WHERE id = $id;",
            vec![
                ("embedding", serde_json::to_value(&embedding)?),
                ("len", serde_json::Number::from(embedding.len()).to_string().into()),
                ("id", content_id.to_string().into()),
            ],
        )
        .await;

    // 2) Create embedding row if missing
    let mut exists = false;
    if let Ok(mut sel) = conn
        .query_with_binds(
            "SELECT id FROM content_embedding WHERE id = $id LIMIT 1;",
            vec![("id", content_id.to_string().into())],
        )
        .await
    {
        if let Ok(rows) = sel.take::<Vec<serde_json::Value>>(0) {
            if !rows.is_empty() {
                exists = true;
            }
        }
    }
    if !exists {
        let _ = conn
            .query_with_binds(
                "CREATE content_embedding CONTENT $obj;",
                vec![("obj", serde_json::to_value(&obj_value)?)],
            )
            .await;
    }

    // 3) Mark embedding as ready on content
    let _ = conn
        .query_with_binds(
            "UPDATE content SET embedding_status = \"ready\" WHERE id = $id;",
            vec![("id", content_id.to_string().into())],
        )
        .await?;
    Ok(())
}

/// Struct used to provide parameters for `upsert_entity_snapshot`.
#[derive(Debug)]
pub struct EntitySnapshotUpsert<'a> {
    pub snapshot_id: &'a str,
    pub stable_id: &'a str,
    pub content_id: &'a str,
    pub file: Option<&'a str>,
    pub start_line: Option<u64>,
    pub end_line: Option<u64>,
    pub name: Option<&'a str>,
}

/// Upsert a snapshot mapping row for an entity: (snapshot_id, stable_id) -> content_id
pub async fn upsert_entity_snapshot(
    conn: &SurrealConnection,
    up: &EntitySnapshotUpsert<'_>,
) -> Result<()> {
    // Build content object via serde_json to avoid manual escaping
    let mut content = serde_json::Map::new();
    let id = format!("{}:{}", up.snapshot_id, up.stable_id);
    content.insert("id".to_string(), Value::String(id.clone()));
    content.insert(
        "snapshot_id".to_string(),
        Value::String(up.snapshot_id.to_string()),
    );
    content.insert(
        "stable_id".to_string(),
        Value::String(up.stable_id.to_string()),
    );
    content.insert(
        "content_id".to_string(),
        Value::String(up.content_id.to_string()),
    );
    if let Some(f) = up.file {
        content.insert("file".to_string(), Value::String(f.to_string()));
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
    if let Some(n) = up.name {
        content.insert("name".to_string(), Value::String(n.to_string()));
    }

    let content_value = Value::Object(content);
    // binds are constructed inline at call sites below; no local binds needed.

    // 1) Update existing snapshot row
    let _ = conn
        .query_with_binds(
            "UPDATE entity_snapshot SET content_id = $content_id, file = $file, start_line = $start_line, end_line = $end_line, name = $name WHERE id = $id;",
            vec![
                ("content_id", serde_json::Value::String(up.content_id.to_string())),
                ("file", match up.file { Some(s) => serde_json::Value::String(s.to_string()), None => serde_json::Value::Null }),
                ("start_line", match up.start_line { Some(n) => serde_json::Value::Number(serde_json::Number::from(n)), None => serde_json::Value::Null }),
                ("end_line", match up.end_line { Some(n) => serde_json::Value::Number(serde_json::Number::from(n)), None => serde_json::Value::Null }),
                ("name", match up.name { Some(s) => serde_json::Value::String(s.to_string()), None => serde_json::Value::Null }),
                ("id", serde_json::Value::String(id.clone())),
            ],
        )
        .await;

    // 2) Create the snapshot row if missing
    let mut exists = false;
    if let Ok(mut sel) = conn
        .query_with_binds(
            "SELECT id FROM entity_snapshot WHERE id = $id LIMIT 1;",
            vec![("id", serde_json::Value::String(id.clone()))],
        )
        .await
    {
        if let Ok(rows) = sel.take::<Vec<serde_json::Value>>(0) {
            if !rows.is_empty() {
                exists = true;
            }
        }
    }
    if !exists {
        let _ = conn
            .query_with_binds(
                "CREATE entity_snapshot CONTENT $content;",
                vec![("content", serde_json::to_value(&content_value)?)],
            )
            .await;
    }

    // 3) Ensure created_at token is set when missing
    let _ = conn
        .query_with_binds(
            "UPDATE entity_snapshot SET created_at = time::now() WHERE id = $id AND created_at = NONE;",
            vec![("id", serde_json::Value::String(id.clone()))],
        )
        .await?;
    Ok(())
}
