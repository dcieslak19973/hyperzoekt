use crate::db_writer::connection::SurrealConnection;
use anyhow::Result;
use serde_json::Value;

/// Create a commit row in SurrealDB. Commits are immutable; callers should
/// ensure idempotency by using a deterministic `commit_id` (VCS hash or
/// content-addressed id).
pub async fn create_commit(
    conn: &SurrealConnection,
    commit_id: &str,
    repo: &str,
    parents: &[&str],
    tree: Option<&str>,
    author: Option<&str>,
    message: Option<&str>,
) -> Result<()> {
    // Build a JSON object for the content safely via serde_json
    let parents_val = serde_json::to_value(parents)?;
    let mut content = serde_json::Map::new();
    content.insert("id".to_string(), Value::String(commit_id.to_string()));
    content.insert("repo".to_string(), Value::String(repo.to_string()));
    content.insert("parents".to_string(), parents_val);
    if let Some(t) = tree {
        content.insert("tree".to_string(), Value::String(t.to_string()));
    }
    if let Some(a) = author {
        content.insert("author".to_string(), Value::String(a.to_string()));
    }
    if let Some(m) = message {
        content.insert("message".to_string(), Value::String(m.to_string()));
    }

    let content_value = Value::Object(content);

    // Use parameterized CREATE with deterministic id and then set timestamp tokens
    // via conditional UPDATEs so Surreal tokens (time::now()) remain unquoted.
    let sql = format!(
        "BEGIN; DELETE FROM commits WHERE id = $id; CREATE commits:{} CONTENT $c; COMMIT;",
        commit_id
    );
    let _ = conn
        .query_with_binds(
            sql.as_str(),
            vec![("id", commit_id.to_string().into()), ("c", content_value)],
        )
        .await?;

    // Ensure timestamp token is set on created rows (only if not present)
    let _ = conn
        .query_with_binds(
            "UPDATE commits SET timestamp = time::now() WHERE id = $id AND timestamp = NONE;",
            vec![("id", commit_id.to_string().into())],
        )
        .await?;
    Ok(())
}

/// Create or update a branch ref (mutable pointer)
pub async fn create_branch(
    conn: &SurrealConnection,
    repo: &str,
    branch: &str,
    commit_id: &str,
) -> Result<()> {
    let id = format!("{}:refs/heads/{}", repo, branch);
    let name = format!("refs/heads/{}", branch);

    let mut content = serde_json::Map::new();
    content.insert("id".to_string(), Value::String(id.clone()));
    content.insert("repo".to_string(), Value::String(repo.to_string()));
    content.insert("name".to_string(), Value::String(name.clone()));
    content.insert("kind".to_string(), Value::String("branch".to_string()));
    content.insert("target".to_string(), Value::String(commit_id.to_string()));

    let content_value = Value::Object(content);

    let sql = "BEGIN; DELETE FROM refs WHERE id = $id; CREATE refs CONTENT $r; COMMIT;";
    let _ = conn
        .query_with_binds(
            sql,
            vec![("id", id.to_string().into()), ("r", content_value)],
        )
        .await?;

    // Set created_at only if missing, and always update updated_at to now()
    let id_clone = id.to_string();
    let _ = conn
        .query_with_binds(
            "UPDATE refs SET created_at = time::now() WHERE id = $id AND created_at = NONE;",
            vec![("id", id_clone.clone().into())],
        )
        .await?;
    let _ = conn
        .query_with_binds(
            "UPDATE refs SET updated_at = time::now() WHERE id = $id;",
            vec![("id", id_clone.into())],
        )
        .await?;
    Ok(())
}

/// Move a branch to a new commit atomically
pub async fn move_branch(
    conn: &SurrealConnection,
    repo: &str,
    branch: &str,
    new_commit: &str,
) -> Result<()> {
    let id = format!("{}:refs/heads/{}", repo, branch);
    let sql =
        "BEGIN; UPDATE refs SET target = $commit, updated_at = time::now() WHERE id = $id; COMMIT;"
            .to_string();
    let _ = conn
        .query_with_binds(
            sql.as_str(),
            vec![("commit", new_commit.to_string().into()), ("id", id.into())],
        )
        .await?;
    Ok(())
}

/// Create an immutable tag. Caller must ensure the tag doesn't already exist.
pub async fn create_tag(
    conn: &SurrealConnection,
    repo: &str,
    tag: &str,
    commit_id: &str,
) -> Result<()> {
    let id = format!("{}:refs/tags/{}", repo, tag);
    let name = format!("refs/tags/{}", tag);

    let mut content = serde_json::Map::new();
    content.insert("id".to_string(), Value::String(id.clone()));
    content.insert("repo".to_string(), Value::String(repo.to_string()));
    content.insert("name".to_string(), Value::String(name.clone()));
    content.insert("kind".to_string(), Value::String("tag".to_string()));
    content.insert("target".to_string(), Value::String(commit_id.to_string()));

    let content_value = Value::Object(content);

    let sql = "BEGIN; DELETE FROM refs WHERE id = $id; CREATE refs CONTENT $r; COMMIT;";
    let _ = conn
        .query_with_binds(sql, vec![("id", id.clone().into()), ("r", content_value)])
        .await?;

    // Ensure created_at is set via a conditional UPDATE so time::now() remains a token
    let _ = conn
        .query_with_binds(
            "UPDATE refs SET created_at = time::now() WHERE id = $id AND created_at = NONE;",
            vec![("id", id.into())],
        )
        .await?;
    Ok(())
}

// Deprecated: use `create_snapshot_meta` instead.
pub async fn create_snapshot_meta(
    conn: &SurrealConnection,
    repo: &str,
    commit_id: &str,
    snapshot_id: &str,
    size_bytes: Option<u64>,
) -> Result<()> {
    let id = snapshot_id;
    let mut content = serde_json::Map::new();
    content.insert("id".to_string(), Value::String(id.to_string()));
    content.insert("snapshot_id".to_string(), Value::String(id.to_string()));
    content.insert("repo".to_string(), Value::String(repo.to_string()));
    content.insert("commit".to_string(), Value::String(commit_id.to_string()));
    match size_bytes {
        Some(s) => {
            content.insert(
                "size_bytes".to_string(),
                Value::Number(serde_json::Number::from(s)),
            );
        }
        None => {
            content.insert("size_bytes".to_string(), Value::Null);
        }
    }

    let content_value = Value::Object(content);

    // Use parameterized CREATE to avoid embedding colon-containing ids into
    // the SQL identifier (which Surreal's parser rejects). Put the id inside
    // the content payload instead and create the record with `CREATE snapshot_meta CONTENT $s`.
    let sql =
        "BEGIN; DELETE FROM snapshot_meta WHERE id = $id; CREATE snapshot_meta CONTENT $s; COMMIT;";
    let _ = conn
        .query_with_binds(
            sql,
            vec![("id", id.to_string().into()), ("s", content_value)],
        )
        .await?;

    // Ensure created_at token is applied when missing
    let _ = conn
        .query_with_binds(
            "UPDATE snapshot_meta SET created_at = time::now() WHERE id = $id AND created_at = NONE;",
            vec![("id", id.to_string().into())],
        )
        .await?;
    Ok(())
}
