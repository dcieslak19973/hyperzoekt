// Integration test for persist_cluster_relations: ensure cluster->cluster has_member is created
// and member_commits are inherited when present on the referenced cluster.

use serde_json::json;

#[tokio::test]
async fn cluster_to_cluster_relations_and_commit_inheritance(
) -> Result<(), Box<dyn std::error::Error>> {
    use hyperzoekt::db::connection::connect;
    use hyperzoekt::hirag::persist_cluster_relations;

    // Force embedded Mem instance for the test to avoid remote dependencies
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    let conn = connect(&None, &None, &None, "testns", "testdb").await?;

    conn.use_ns("test_pcr_ns").await?;
    conn.use_db("test_pcr_db").await?;

    // Clean up
    let _ = conn.query("REMOVE TABLE has_member").await;
    let _ = conn.query("REMOVE TABLE has_commit").await;
    let _ = conn.query("REMOVE TABLE hirag_cluster").await;

    // Create a child cluster that has member_commits set
    let child_cluster_id = "hirag::layer0::child";
    let child = json!({
        "stable_id": child_cluster_id,
        "label": "child",
        "summary": "child cluster",
        "members": ["ent_x"],
        "centroid": [0.0, 1.0],
        "centroid_len": 2,
        "member_repos": ["repoChild"],
        "member_commits": ["commit:childCommit"]
    });
    conn.query_with_binds(
        &format!("CREATE hirag_cluster:`{}` CONTENT $c", child_cluster_id),
        vec![("c", child)],
    )
    .await?;

    // Create a parent cluster that lists the child cluster as a member (hirag::...)
    let parent_cluster_id = "hirag::layer1::parent";
    let parent_members = vec![child_cluster_id.to_string(), "ent_y".to_string()];
    // Create parent cluster record so updates to member_commits succeed
    let parent = json!({
        "stable_id": parent_cluster_id,
        "label": "parent",
        "summary": "parent cluster",
        "members": parent_members.clone(),
        "centroid": [0.0, 0.0],
        "centroid_len": 2,
        "member_repos": ["repoParent"]
    });
    conn.query_with_binds(
        &format!("CREATE hirag_cluster:`{}` CONTENT $p", parent_cluster_id),
        vec![("p", parent)],
    )
    .await?;

    // Call persist_cluster_relations which should create a has_member edge parent->child
    // and inherit the child's member_commits by creating has_commit edges and updating parent member_commits.
    persist_cluster_relations(&conn, parent_cluster_id, &parent_members).await?;

    // Query has_member_cluster edges to ensure cluster->cluster relation exists
    let mut members_resp = conn
        .query("SELECT string::concat(meta::tb(in), ':', meta::id(in)) AS cluster, string::concat(meta::tb(out), ':', meta::id(out)) AS member FROM has_member_cluster")
        .await?;
    let member_rows: Vec<serde_json::Value> = members_resp.take(0)?;
    // There should be at least one relation for the parent cluster referencing the child cluster
    let mut found = false;
    for r in member_rows.iter() {
        if let (Some(cluster), Some(member)) = (
            r.get("cluster").and_then(|v| v.as_str()),
            r.get("member").and_then(|v| v.as_str()),
        ) {
            if cluster.contains(parent_cluster_id) && member.contains(child_cluster_id) {
                found = true;
                break;
            }
        }
    }
    assert!(
        found,
        "expected has_member_cluster edge from parent cluster to child cluster"
    );

    // Query has_commit edges to ensure child's commit was related to parent
    let mut commit_resp = conn
        .query("SELECT string::concat(meta::tb(in), ':', meta::id(in)) AS cluster, string::concat(meta::tb(out), ':', meta::id(out)) AS commit FROM has_commit")
        .await?;
    let commit_rows: Vec<serde_json::Value> = commit_resp.take(0)?;
    let mut commit_found = false;
    for r in commit_rows.iter() {
        if let Some(cluster) = r.get("cluster").and_then(|v| v.as_str()) {
            if cluster.contains(parent_cluster_id) {
                if let Some(commit) = r.get("commit").and_then(|v| v.as_str()) {
                    if commit == "commit:childCommit" {
                        commit_found = true;
                        break;
                    }
                }
            }
        }
    }
    assert!(
        commit_found,
        "expected parent's has_commit to include child's commit"
    );

    // Verify parent hirag_cluster row member_commits contains the inherited commit
    let mut parent_resp = conn
        .query(&format!(
            "SELECT member_commits FROM hirag_cluster WHERE stable_id = '{}' LIMIT 1",
            parent_cluster_id
        ))
        .await?;
    let parent_rows: Vec<serde_json::Value> = parent_resp.take(0)?;
    let mut inherited = false;
    if let Some(v) = parent_rows.get(0) {
        if let Some(arr) = v.get("member_commits").and_then(|x| x.as_array()) {
            for entry in arr.iter() {
                if let Some(s) = entry.as_str() {
                    if s == "commit:childCommit" {
                        inherited = true;
                        break;
                    }
                }
            }
        }
    }
    assert!(
        inherited,
        "expected parent.member_commits to include inherited commit"
    );

    Ok(())
}
