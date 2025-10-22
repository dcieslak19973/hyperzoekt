use hyperzoekt::db::Database;
use std::env;

#[tokio::test]
async fn uses_refs_when_repo_branch_missing() {
    // Force in-memory Surreal so tests are hermetic
    env::set_var("HZ_DISABLE_SURREAL_ENV", "1");

    let db = Database::new(None, "test_ns", "test_db").await.unwrap();
    let repo_name = "example/repo_ref_fallback";

    // Create a repo record without `branch`
    db.connection()
        .query_with_binds(
            "CREATE repo CONTENT { name: $name, git_url: $git_url }",
            vec![
                ("name", serde_json::Value::String(repo_name.to_string())),
                (
                    "git_url",
                    serde_json::Value::String("https://example.com/repo.git".to_string()),
                ),
            ],
        )
        .await
        .unwrap();

    // Create a refs row that should be discovered as the default
    // Create refs both as plain repo string and as a prefixed thing id to
    // tolerate differences in how other helpers populate the `repo` field.
    db.connection()
        .query_with_binds(
            "CREATE refs CONTENT { repo: $repo, name: $name }",
            vec![
                ("repo", serde_json::Value::String(repo_name.to_string())),
                (
                    "name",
                    serde_json::Value::String("refs/heads/trunk".to_string()),
                ),
            ],
        )
        .await
        .unwrap();

    db.connection()
        .query_with_binds(
            "CREATE refs CONTENT { repo: $repo, name: $name }",
            vec![
                (
                    "repo",
                    serde_json::Value::String(format!("repo:{}", repo_name)),
                ),
                (
                    "name",
                    serde_json::Value::String("refs/heads/trunk".to_string()),
                ),
            ],
        )
        .await
        .unwrap();

    let branch = db.get_repo_default_branch(repo_name).await.unwrap();
    // Debug: query the refs table the same way get_repo_default_branch does
    let mut repo_resp = db
        .connection()
        .query_with_binds(
            "SELECT branch FROM repo WHERE name = $name LIMIT 1",
            vec![("name", serde_json::Value::String(repo_name.to_string()))],
        )
        .await
        .unwrap();
    let repo_rows: Vec<serde_json::Value> = repo_resp.take(0).unwrap_or_default();
    eprintln!("DEBUG repo row for {}: {:?}", repo_name, repo_rows);

    let mut resp = db
        .connection()
        .query_with_binds(
            "SELECT VALUE name FROM refs WHERE repo = $repo AND name IN ['refs/heads/main', 'refs/heads/master', 'refs/heads/trunk'] LIMIT 10",
            vec![("repo", serde_json::Value::String(repo_name.to_string()))],
        )
        .await
        .unwrap();
    let vals: Vec<serde_json::Value> = resp.take(0).unwrap_or_default();
    eprintln!("DEBUG refs query result for {}: {:?}", repo_name, vals);

    assert_eq!(branch, "trunk");
}

#[tokio::test]
async fn prefers_repo_branch_if_present() {
    env::set_var("HZ_DISABLE_SURREAL_ENV", "1");

    let db = Database::new(None, "test_ns2", "test_db2").await.unwrap();
    let repo_name = "example/repo_with_branch";

    // Create a repo record that includes an explicit branch value
    db.connection()
        .query_with_binds(
            "CREATE repo CONTENT { name: $name, git_url: $git_url, branch: $branch }",
            vec![
                ("name", serde_json::Value::String(repo_name.to_string())),
                (
                    "git_url",
                    serde_json::Value::String("https://example.com/repo2.git".to_string()),
                ),
                ("branch", serde_json::Value::String("custom".to_string())),
            ],
        )
        .await
        .unwrap();

    // Also create a refs row to ensure fallback would differ
    db.connection()
        .query_with_binds(
            "CREATE refs CONTENT { repo: $repo, name: $name }",
            vec![
                ("repo", serde_json::Value::String(repo_name.to_string())),
                (
                    "name",
                    serde_json::Value::String("refs/heads/trunk".to_string()),
                ),
            ],
        )
        .await
        .unwrap();

    db.connection()
        .query_with_binds(
            "CREATE refs CONTENT { repo: $repo, name: $name }",
            vec![
                (
                    "repo",
                    serde_json::Value::String(format!("repo:{}", repo_name)),
                ),
                (
                    "name",
                    serde_json::Value::String("refs/heads/trunk".to_string()),
                ),
            ],
        )
        .await
        .unwrap();

    let branch = db.get_repo_default_branch(repo_name).await.unwrap();
    assert_eq!(branch, "custom");
}
