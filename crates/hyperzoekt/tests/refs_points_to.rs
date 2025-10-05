use anyhow::Result;
use hyperzoekt::db::connection::{connect, SurrealConnection};
use hyperzoekt::db::{create_branch, create_commit, create_tag, move_branch};
use serde_json::Value;

async fn points_to_count(conn: &SurrealConnection, ref_key: &str, commit_key: &str) -> Result<i64> {
    let mut resp = conn
        .query_with_binds(
            "SELECT count() AS c FROM points_to WHERE in = type::thing('refs', $ref_id) AND out = type::thing('commits', $commit_id);",
            vec![
                ("ref_id", ref_key.to_string().into()),
                ("commit_id", commit_key.to_string().into()),
            ],
        )
        .await?;
    let rows: Vec<Value> = resp.take::<Vec<Value>>(0).unwrap_or_default();
    let count = rows
        .into_iter()
        .filter_map(|row| row.get("c").and_then(|v| v.as_i64()))
        .next()
        .unwrap_or(0);
    Ok(count)
}

#[tokio::test]
async fn points_to_relations_stay_consistent() -> Result<()> {
    std::env::set_var("HZ_EPHEMERAL_MEM", "1");
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");

    let conn = connect(&None, &None, &None, "testns", "testdb").await?;

    conn
        .query(
            "CREATE repo:myrepo CONTENT {\"name\":\"myrepo\",\"branch\":\"main\",\"git_url\":\"https://example.com/myrepo.git\"}",
        )
        .await?;

    let parents: Vec<&str> = Vec::new();
    create_commit(&conn, "commit1", "myrepo", &parents, None, None, None).await?;
    create_branch(&conn, "myrepo", "main", "commit1").await?;

    assert_eq!(
        1,
        points_to_count(&conn, "myrepo:refs/heads/main", "commit1").await?,
        "branch should point at initial commit",
    );

    create_commit(&conn, "commit2", "myrepo", &parents, None, None, None).await?;
    move_branch(&conn, "myrepo", "main", "commit2").await?;

    assert_eq!(
        0,
        points_to_count(&conn, "myrepo:refs/heads/main", "commit1").await?,
        "old branch relation should be removed",
    );
    assert_eq!(
        1,
        points_to_count(&conn, "myrepo:refs/heads/main", "commit2").await?,
        "branch should point at new commit",
    );

    create_tag(&conn, "myrepo", "v1.0.0", "commit2").await?;
    assert_eq!(
        1,
        points_to_count(&conn, "myrepo:refs/tags/v1.0.0", "commit2").await?,
        "tag should point at tagged commit",
    );

    Ok(())
}
