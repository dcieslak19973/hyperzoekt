use crate::db::connection::SurrealConnection;
use anyhow::Result;
use log::{debug, info, warn};
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
    content.insert("repo".to_string(), Value::String(format!("repo:{}", repo)));
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
    conn.query_with_binds(
        "DELETE type::thing('commits', $id);",
        vec![("id", commit_id.to_string().into())],
    )
    .await?;

    let create_resp = conn
        .query_with_binds(
            "CREATE type::thing('commits', $id) CONTENT $c RETURN AFTER;",
            vec![("id", commit_id.to_string().into()), ("c", content_value)],
        )
        .await?;
    let mut create_resp = create_resp.check()?;
    let created_rows = create_resp.take::<Vec<Value>>(0).unwrap_or_default();
    debug!(
        "Created commit row commits:{} rows={:?}",
        commit_id, created_rows
    );

    // Ensure timestamp token is set on created rows (only if not present)
    let _ = conn
        .query_with_binds(
            "UPDATE commits SET timestamp = time::now() WHERE id = $id AND timestamp = NONE;",
            vec![("id", commit_id.to_string().into())],
        )
        .await?;

    // Create relationship between commit and repo using fully-qualified record IDs
    // Use LET bindings for thing() objects and then RELATE the bound variables.
    let relate_sql = "LET $c = type::thing('commits', $commit_id); LET $r = type::thing('repo', $repo_id); RELATE $c -> in_repo -> $r RETURN AFTER;";
    let relate_result = conn
        .query_with_binds(
            relate_sql,
            vec![
                ("commit_id", commit_id.to_string().into()),
                ("repo_id", repo.to_string().into()),
            ],
        )
        .await;

    if let Err(e) = relate_result {
        warn!("Failed to create commit-repo relation: {}", e);
    } else {
        info!(
            "Created commit-repo relation: commits:{} -> repo:{}",
            commit_id, repo
        );
    }

    Ok(())
}

async fn validate_points_to_relation(conn: &SurrealConnection, ref_id: &str, commit_id: &str) {
    match conn
        .query_with_binds(
            "SELECT count() AS c FROM points_to WHERE in = type::thing('refs', $ref_id) AND out = type::thing('commits', $commit_id);",
            vec![
                ("ref_id", ref_id.to_string().into()),
                ("commit_id", commit_id.to_string().into()),
            ],
        )
        .await
    {
        Ok(mut resp) => {
            let count_val = resp
                .take::<Vec<Value>>(0)
                .ok()
                .and_then(|rows| rows.into_iter().next())
                .and_then(|row| {
                    if row.is_i64() {
                        row.as_i64()
                    } else {
                        row.get("c").and_then(|v| v.as_i64())
                    }
                })
                .unwrap_or(0);
            if count_val <= 0 {
                let total_sql = "SELECT count() AS total FROM points_to GROUP ALL;";
                let total_info = match conn.query(total_sql).await {
                    Ok(mut total_resp) => total_resp
                        .take::<Vec<Value>>(0)
                        .ok()
                        .and_then(|rows| rows.into_iter().next())
                        .and_then(|row| {
                            if row.is_i64() {
                                row.as_i64()
                            } else {
                                row.get("total").and_then(|v| v.as_i64())
                            }
                        })
                        .unwrap_or(-1),
                    Err(_) => -1,
                };
                warn!(
                    "points_to validation: relation missing after create ref={} commit={} total_points_to_rows={}",
                    ref_id, commit_id, total_info
                );

                let _ = conn
                    .query_with_binds(
                        "SELECT target FROM refs WHERE id = $ref;",
                        vec![("ref", ref_id.to_string().into())],
                    )
                    .await
                    .map(|mut resp| {
                        let rows = resp.take::<Vec<Value>>(0).unwrap_or_default();
                        warn!(
                            "points_to validation: ref row snapshot for {} -> {:?}",
                            ref_id, rows
                        );
                    });

                let _ = conn
                    .query("SELECT in, out FROM points_to LIMIT 5;")
                    .await
                    .map(|mut resp| {
                        let rows = resp.take::<Vec<Value>>(0).unwrap_or_default();
                        debug!(
                            "points_to validation: sample edges when missing relation: {:?}",
                            rows
                        );
                    });
            } else {
                debug!(
                    "points_to validation: relation present ref={} commit={} count={}",
                    ref_id, commit_id, count_val
                );
            }
        }
        Err(e) => {
            warn!(
                "points_to validation query failed ref={} commit={} err={}",
                ref_id, commit_id, e
            );
        }
    }
}

async fn upsert_points_to(conn: &SurrealConnection, ref_id: &str, commit_id: &str) -> Result<()> {
    // Remove any existing edges for idempotency before inserting the new relation.
    conn.query_with_binds(
        "DELETE FROM points_to WHERE in = type::thing('refs', $ref_id);",
        vec![("ref_id", ref_id.to_string().into())],
    )
    .await?;

    let sql = "LET $ref_obj = type::thing('refs', $ref_id);
LET $commit_obj = type::thing('commits', $commit_id);
RELATE $ref_obj -> points_to -> $commit_obj RETURN AFTER;";
    match conn
        .query_with_binds(
            sql,
            vec![
                ("ref_id", ref_id.to_string().into()),
                ("commit_id", commit_id.to_string().into()),
            ],
        )
        .await
    {
        Ok(mut resp) => {
            let rows = resp.take::<Vec<Value>>(0).unwrap_or_default();
            debug!(
                "points_to upsert relation created for {} -> {} rows={:?}",
                ref_id, commit_id, rows
            );
        }
        Err(e) => {
            warn!(
                "Failed to relate ref {} to commit {}: {}",
                ref_id, commit_id, e
            );
            return Err(e.into());
        }
    }

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

    conn.query_with_binds(
        "DELETE type::thing('refs', $id);",
        vec![("id", id.to_string().into())],
    )
    .await?;

    let create_resp = conn
        .query_with_binds(
            "CREATE type::thing('refs', $id) CONTENT $r RETURN AFTER;",
            vec![("id", id.to_string().into()), ("r", content_value)],
        )
        .await?;
    let mut create_resp = create_resp.check()?;
    let created_rows = create_resp.take::<Vec<Value>>(0).unwrap_or_default();
    debug!("Created branch ref {} rows={:?}", id, created_rows);

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

    // Create relationship between ref and commit
    info!(
        "Setting branch points_to relation: refs:{} -> commits:{}",
        id, commit_id
    );
    if let Err(e) = upsert_points_to(conn, &id, commit_id).await {
        warn!("Failed to create ref-commit relation: {}", e);
    } else {
        validate_points_to_relation(conn, &id, commit_id).await;
    }

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

    // Always remove any existing relation(s) for this ref before creating the new one
    if let Err(e) = conn
        .query_with_binds(
            "DELETE FROM points_to WHERE in = type::thing('refs', $ref_id);",
            vec![("ref_id", id.to_string().into())],
        )
        .await
    {
        warn!("Failed to clear existing points_to edges for {}: {}", id, e);
    }

    // Update the target
    let sql =
        "BEGIN; UPDATE refs SET target = $commit, updated_at = time::now() WHERE id = $id; COMMIT;"
            .to_string();
    let _ = conn
        .query_with_binds(
            sql.as_str(),
            vec![
                ("commit", new_commit.to_string().into()),
                ("id", id.clone().into()),
            ],
        )
        .await?;

    // Create new relation
    info!(
        "Updating branch points_to relation: refs:{} -> commits:{}",
        id, new_commit
    );
    if let Err(e) = upsert_points_to(conn, &id, new_commit).await {
        warn!("Failed to create ref-commit relation: {}", e);
    } else {
        validate_points_to_relation(conn, &id, new_commit).await;
    }

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

    conn.query_with_binds(
        "DELETE type::thing('refs', $id);",
        vec![("id", id.clone().into())],
    )
    .await?;

    let create_resp = conn
        .query_with_binds(
            "CREATE type::thing('refs', $id) CONTENT $r RETURN AFTER;",
            vec![("id", id.clone().into()), ("r", content_value)],
        )
        .await?;
    let mut create_resp = create_resp.check()?;
    let created_rows = create_resp.take::<Vec<Value>>(0).unwrap_or_default();
    debug!("Created tag ref {} rows={:?}", id, created_rows);

    // Ensure created_at is set via a conditional UPDATE so time::now() remains a token
    let _ = conn
        .query_with_binds(
            "UPDATE refs SET created_at = time::now() WHERE id = $id AND created_at = NONE;",
            vec![("id", id.clone().into())],
        )
        .await?;

    // Create relationship between ref and commit
    info!(
        "Setting tag points_to relation: refs:{} -> commits:{}",
        id, commit_id
    );
    if let Err(e) = upsert_points_to(conn, &id, commit_id).await {
        warn!("Failed to create ref-commit relation: {}", e);
    } else {
        validate_points_to_relation(conn, &id, commit_id).await;
    }

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
