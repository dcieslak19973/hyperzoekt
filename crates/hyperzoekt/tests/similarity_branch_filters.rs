use anyhow::Result;
use hyperzoekt::db::{
    connection::connect as hz_connect, create_branch, create_commit, resolve_commit_branch,
    resolve_default_branch,
};

#[tokio::test]
async fn resolve_branch_helpers() -> Result<()> {
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    std::env::set_var("HZ_EPHEMERAL_MEM", "1");

    let conn = hz_connect(&None, &None, &None, "testns", "testdb").await?;
    conn.use_ns("test").await?;
    conn.use_db("test").await?;

    conn.query(
        "CREATE repo:demo CONTENT {name: 'demo', branch: 'main', git_url: 'https://example.com/demo.git'};",
    )
    .await?;

    create_commit(&conn, "commit123", "demo", &[], None, None, None).await?;
    create_branch(&conn, "demo", "main", "commit123").await?;

    let branch = resolve_commit_branch(&conn, "demo", "commits:commit123").await?;
    assert_eq!(branch.as_deref(), Some("main"));

    // Allow repo-prefixed refs rows (legacy data may populate repo field with repo:<name>)
    conn.query("UPDATE refs SET repo = 'repo:demo' WHERE id = 'demo:refs/heads/main';")
        .await?;
    let branch_prefixed = resolve_commit_branch(&conn, "demo", "commits:commit123").await?;
    assert_eq!(branch_prefixed.as_deref(), Some("main"));

    // Remove repo.branch to exercise fallback through refs table
    conn.query("UPDATE repo:demo SET branch = NONE;").await?;

    let default_branch = resolve_default_branch(&conn, "demo").await?;
    let default_branch = default_branch.expect("expected default branch info");
    assert_eq!(default_branch.branch.as_deref(), Some("main"));

    let missing_branch = resolve_commit_branch(&conn, "demo", "commits:missing").await?;
    assert!(missing_branch.is_none());

    // Remove refs row and rely on SOURCE_BRANCH + final fallback to "main"
    conn.query("DELETE refs WHERE id = 'demo:refs/heads/main';")
        .await?;
    conn.query("DELETE FROM refs WHERE repo IN ['demo', 'repo:demo'];")
        .await?;

    let mut remaining_refs = conn
        .query("SELECT name FROM refs WHERE repo IN ['demo', 'repo:demo'];")
        .await?;
    let leftover: Vec<serde_json::Value> = remaining_refs.take(0).unwrap_or_default();
    assert!(
        leftover.is_empty(),
        "expected refs to be removed, found: {:?}",
        leftover
    );

    let fallback_branch = resolve_default_branch(&conn, "demo").await?;
    let fallback_branch = fallback_branch.expect("expected fallback branch info");
    assert!(fallback_branch.matches("main"));

    Ok(())
}
