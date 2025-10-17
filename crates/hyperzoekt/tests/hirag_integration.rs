// Integration tests for HiRAG helpers (centroid persistence and repo-filtered retrieval)

use std::sync::OnceLock;
use tokio::sync::Mutex;

fn surreal_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[tokio::test]
async fn build_first_layer_persists_centroid_and_provenance(
) -> Result<(), Box<dyn std::error::Error>> {
    use hyperzoekt::db::connection::connect;
    use hyperzoekt::hirag::build_first_layer;
    use serde_json::json;

    let _guard = surreal_test_lock().lock().await;

    // Force embedded Mem instance for the test to avoid remote dependencies
    let prev = std::env::var("HZ_DISABLE_SURREAL_ENV").ok();
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    let conn = connect(&None, &None, &None, "testns", "testdb").await?;
    if let Some(v) = prev {
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", v);
    } else {
        std::env::remove_var("HZ_DISABLE_SURREAL_ENV");
    }

    conn.use_ns("test_centroid_ns")
        .await
        .expect("failed to select centroid namespace");
    conn.use_db("test_centroid_db")
        .await
        .expect("failed to select centroid database");

    // Clean up potential existing collections
    let _ = conn.query("REMOVE TABLE has_member").await;
    let _ = conn.query("REMOVE TABLE has_commit").await;
    let _ = conn.query("REMOVE TABLE hirag_cluster").await;
    let _ = conn.query("REMOVE TABLE entity_snapshot").await;
    let _ = conn.query("REMOVE TABLE entity").await;

    // Create entity rows and corresponding entity_snapshot records with embeddings
    let e1 = json!({ "stable_id": "ent1" });
    let e2 = json!({ "stable_id": "ent2" });
    conn.query_with_binds("CREATE entity CONTENT $e", vec![("e", e1)])
        .await?;
    conn.query_with_binds("CREATE entity CONTENT $e", vec![("e", e2)])
        .await?;

    // Create snapshots that hold the embedding and repo provenance
    let s1 = json!({ "id": "ent1", "stable_id": "ent1", "source_content": "fn a() {}", "embedding": [0.0, 1.0], "embedding_len": 2, "repo_name": "repoA" });
    let s2 = json!({ "id": "ent2", "stable_id": "ent2", "source_content": "fn b() {}", "embedding": [1.0, 0.0], "embedding_len": 2, "repo_name": "repoB" });
    conn.query_with_binds("CREATE entity_snapshot:ent1 CONTENT $s", vec![("s", s1)])
        .await?;
    conn.query_with_binds("CREATE entity_snapshot:ent2 CONTENT $s", vec![("s", s2)])
        .await?;

    // Run clustering with k=2 (two clusters)
    build_first_layer(&conn, 2).await?;

    // Query hirag_cluster rows (use `label` as identifier to avoid Thing deserialization)
    let mut resp = conn
        .query("SELECT label, centroid, member_repos FROM hirag_cluster")
        .await?;
    let rows: Vec<serde_json::Value> = resp.take(0)?;
    assert!(!rows.is_empty(), "expected at least one hirag_cluster row");

    // Check centroid and member_repos exist and have sensible lengths
    for r in rows.iter() {
        let centroid = r
            .get("centroid")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        assert!(centroid > 0, "expected non-empty centroid");
        let provs = r
            .get("member_repos")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        assert!(provs >= 1, "expected at least one member_repos entry");
    }

    Ok(())
}

#[tokio::test]
async fn build_first_layer_creates_relation_edges() -> Result<(), Box<dyn std::error::Error>> {
    use hyperzoekt::db::connection::connect;
    use hyperzoekt::hirag::build_first_layer;
    use serde_json::json;

    let _guard = surreal_test_lock().lock().await;

    let prev = std::env::var("HZ_DISABLE_SURREAL_ENV").ok();
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    let conn = connect(&None, &None, &None, "testns", "testdb").await?;
    if let Some(v) = prev {
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", v);
    } else {
        std::env::remove_var("HZ_DISABLE_SURREAL_ENV");
    }

    conn.use_ns("test_relations_ns")
        .await
        .expect("failed to select relations namespace");
    conn.use_db("test_relations_db")
        .await
        .expect("failed to select relations database");

    let _ = conn.query("REMOVE TABLE has_member").await;
    let _ = conn.query("REMOVE TABLE has_commit").await;
    let _ = conn.query("REMOVE TABLE hirag_cluster").await;
    conn.query("REMOVE TABLE entity_snapshot")
        .await
        .expect("failed to remove entity_snapshot table");
    conn.query("REMOVE TABLE entity")
        .await
        .expect("failed to remove entity table");
    let _ = conn.query("REMOVE TABLE commit").await;

    let commit = json!({ "id": "testCommit", "sha": "abc123", "repo": "repoA" });
    conn.query_with_binds("CREATE commit:testCommit CONTENT $c", vec![("c", commit)])
        .await?;

    let e1 = json!({ "stable_id": "ent_rel_1" });
    let e2 = json!({ "stable_id": "ent_rel_2" });
    conn.query_with_binds("CREATE entity CONTENT $e", vec![("e", e1)])
        .await?;
    conn.query_with_binds("CREATE entity CONTENT $e", vec![("e", e2)])
        .await?;

    let mut entity_debug = conn.query("SELECT stable_id FROM entity").await?;
    let entity_rows: Vec<serde_json::Value> = entity_debug.take(0)?;
    assert_eq!(
        entity_rows.len(),
        2,
        "expected only relation test entities to be present"
    );

    let s1 = json!({
        "id": "ent_rel_1",
        "stable_id": "ent_rel_1",
        "source_content": "fn rel1() {}",
        "embedding": [0.0, 1.0],
        "embedding_len": 2,
        "repo_name": "repoA",
        "sourcecontrol_commit": "commit:testCommit"
    });
    let s2 = json!({
        "id": "ent_rel_2",
        "stable_id": "ent_rel_2",
        "source_content": "fn rel2() {}",
        "embedding": [0.1, 0.9],
        "embedding_len": 2,
        "repo_name": "repoA",
        "sourcecontrol_commit": "commit:testCommit"
    });
    conn.query_with_binds(
        "CREATE entity_snapshot:ent_rel_1 CONTENT $s",
        vec![("s", s1)],
    )
    .await?;
    conn.query_with_binds(
        "CREATE entity_snapshot:ent_rel_2 CONTENT $s",
        vec![("s", s2)],
    )
    .await?;

    build_first_layer(&conn, 1).await?;

    #[derive(serde::Deserialize)]
    struct ClusterRow {
        id: surrealdb::sql::Thing,
        stable_id: String,
        members: Vec<String>,
        member_commits: Option<Vec<String>>,
    }

    let mut cluster_resp = conn
        .query("SELECT id, stable_id, members, member_commits FROM hirag_cluster")
        .await?;
    let clusters: Vec<ClusterRow> = cluster_resp.take(0)?;
    assert!(!clusters.is_empty(), "expected at least one cluster row");
    let mut matching_clusters = clusters.iter().filter(|c| {
        c.member_commits
            .as_ref()
            .map(|v| v.iter().any(|entry| entry == "commit:testCommit"))
            .unwrap_or(false)
    });
    let cluster = matching_clusters
        .next()
        .expect("cluster containing commit:testCommit");
    assert!(
        matching_clusters.next().is_none(),
        "expected exactly one cluster with commit link"
    );

    assert_eq!(cluster.members.len(), 2, "expected two members in cluster");
    assert!(
        cluster.members.contains(&"ent_rel_1".to_string())
            && cluster.members.contains(&"ent_rel_2".to_string()),
        "cluster members should include both entity snapshots"
    );

    let member_commits = cluster.member_commits.clone().unwrap_or_default();
    assert_eq!(member_commits.len(), 1, "expected one member commit link");
    assert_eq!(member_commits[0], "commit:testCommit");

    let canonical_cluster_id = format!("{}:{}", cluster.id.tb, cluster.stable_id);
    assert!(
        cluster.id.to_string().contains(&cluster.stable_id),
        "cluster Thing id should reference stable id"
    );

    #[derive(serde::Deserialize)]
    struct RelationRow {
        cluster: String,
        member: String,
    }

    let mut member_rel = conn
        .query(
            "SELECT string::concat(meta::tb(in), ':', meta::id(in)) AS cluster, \
             string::concat(meta::tb(out), ':', meta::id(out)) AS member FROM has_member",
        )
        .await?;
    let member_rows: Vec<RelationRow> = member_rel.take(0)?;
    assert_eq!(member_rows.len(), 2, "expected two has_member edges");
    for row in member_rows.iter() {
        assert_eq!(row.cluster, canonical_cluster_id);
        assert!(row.member.starts_with("entity_snapshot:"));
    }

    #[derive(serde::Deserialize)]
    struct CommitRelationRow {
        cluster: String,
        commit: String,
    }

    let mut commit_rel = conn
        .query(
            "SELECT string::concat(meta::tb(in), ':', meta::id(in)) AS cluster, \
             string::concat(meta::tb(out), ':', meta::id(out)) AS commit FROM has_commit",
        )
        .await?;
    let commit_rows: Vec<CommitRelationRow> = commit_rel.take(0)?;
    assert_eq!(commit_rows.len(), 1, "expected a single has_commit edge");
    let commit_edge = &commit_rows[0];
    assert_eq!(commit_edge.cluster, canonical_cluster_id);
    assert_eq!(commit_edge.commit, "commit:testCommit");

    Ok(())
}

#[tokio::test]
async fn retrieve_three_level_respects_allowed_repos() -> Result<(), Box<dyn std::error::Error>> {
    use hyperzoekt::db::connection::connect;
    use hyperzoekt::hirag::{build_first_layer, retrieve_three_level_with_allowed_repos};
    use serde_json::json;

    // Force embedded Mem instance for the test
    let _guard = surreal_test_lock().lock().await;
    let prev = std::env::var("HZ_DISABLE_SURREAL_ENV").ok();
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    let conn = connect(&None, &None, &None, "testns", "testdb").await?;
    if let Some(v) = prev {
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", v);
    } else {
        std::env::remove_var("HZ_DISABLE_SURREAL_ENV");
    }

    conn.use_ns("test_retrieve_ns")
        .await
        .expect("failed to select retrieve namespace");
    conn.use_db("test_retrieve_db")
        .await
        .expect("failed to select retrieve database");

    let _ = conn.query("REMOVE TABLE hirag_cluster").await;
    conn.query("REMOVE TABLE entity_snapshot")
        .await
        .expect("failed to remove entity_snapshot table for retrieve test");
    conn.query("REMOVE TABLE entity")
        .await
        .expect("failed to remove entity table for retrieve test");

    // Create entities and snapshots with embedding and repo metadata
    let e1 = json!({ "stable_id": "ent3" });
    let e2 = json!({ "stable_id": "ent4" });
    let e3 = json!({ "stable_id": "ent5" });
    conn.query_with_binds("CREATE entity CONTENT $e", vec![("e", e1)])
        .await?;
    conn.query_with_binds("CREATE entity CONTENT $e", vec![("e", e2)])
        .await?;
    conn.query_with_binds("CREATE entity CONTENT $e", vec![("e", e3)])
        .await?;

    let s1 = json!({ "id": "ent3", "stable_id": "ent3", "source_content": "alpha", "embedding": [0.0, 1.0], "embedding_len": 2, "repo_name": "repoX" });
    let s2 = json!({ "id": "ent4", "stable_id": "ent4", "source_content": "beta",  "embedding": [0.1, 0.9], "embedding_len": 2, "repo_name": "repoX" });
    let s3 = json!({ "id": "ent5", "stable_id": "ent5", "source_content": "gamma", "embedding": [1.0, 0.0], "embedding_len": 2, "repo_name": "repoY" });
    conn.query_with_binds("CREATE entity_snapshot:ent3 CONTENT $s", vec![("s", s1)])
        .await?;
    conn.query_with_binds("CREATE entity_snapshot:ent4 CONTENT $s", vec![("s", s2)])
        .await?;
    conn.query_with_binds("CREATE entity_snapshot:ent5 CONTENT $s", vec![("s", s3)])
        .await?;

    // Build clusters with k=2
    build_first_layer(&conn, 2).await?;

    // Query with allowed_repos = ["repoX"]
    let query_emb = vec![0.0f32, 1.0f32];
    let allowed = vec!["repoX".to_string()];
    let (_top, clusters, _bridge) =
        retrieve_three_level_with_allowed_repos(&conn, &query_emb, 5, Some(&allowed)).await?;

    // All returned clusters should have member_repos intersecting repoX
    for c in clusters.iter() {
        assert!(
            c.member_repos.iter().any(|p| p == "repoX"),
            "cluster missing repoX provenance"
        );
    }

    Ok(())
}

#[tokio::test]
async fn build_hierarchical_layers_creates_multiple_levels(
) -> Result<(), Box<dyn std::error::Error>> {
    use hyperzoekt::db::connection::connect;
    use hyperzoekt::hirag::{build_first_layer_for_repo, build_hierarchical_layers};
    use serde_json::json;

    let _guard = surreal_test_lock().lock().await;

    // Force embedded Mem instance for the test
    let prev = std::env::var("HZ_DISABLE_SURREAL_ENV").ok();
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    let conn = connect(&None, &None, &None, "testns", "testdb").await?;
    if let Some(v) = prev {
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", v);
    } else {
        std::env::remove_var("HZ_DISABLE_SURREAL_ENV");
    }

    conn.use_ns("test_hierarchical_ns")
        .await
        .expect("failed to select hierarchical namespace");
    conn.use_db("test_hierarchical_db")
        .await
        .expect("failed to select hierarchical database");

    // Clean up
    let _ = conn.query("REMOVE TABLE hirag_cluster").await;
    let _ = conn.query("REMOVE TABLE entity_snapshot").await;
    let _ = conn.query("REMOVE TABLE entity").await;

    // Create enough entities to build multiple layers
    // We'll create 20 entities to ensure we can build at least 2-3 layers
    for i in 0..20 {
        let eid = format!("ent_hier_{}", i);
        let e = json!({ "stable_id": eid.clone() });
        conn.query_with_binds("CREATE entity CONTENT $e", vec![("e", e)])
            .await?;

        // Create embedding with slight variations to ensure clustering
        let emb = vec![
            (i as f32) / 20.0,
            1.0 - (i as f32) / 20.0,
            0.5 + ((i % 5) as f32) / 10.0,
        ];
        let s = json!({
            "id": eid.clone(),
            "stable_id": eid.clone(),
            "source_content": format!("fn func_{}() {{}}", i),
            "embedding": emb,
            "embedding_len": 3,
            "repo_name": "testRepo"
        });
        conn.query_with_binds(
            &format!("CREATE entity_snapshot:{} CONTENT $s", eid),
            vec![("s", s)],
        )
        .await?;
    }

    // Build layer 0 with k=5 (should create ~4 clusters from 20 entities)
    build_first_layer_for_repo(&conn, "testRepo", None, 5).await?;

    // Query layer0 clusters
    let mut resp = conn
        .query("SELECT stable_id FROM hirag_cluster WHERE stable_id CONTAINS 'testRepo'")
        .await?;
    let layer0_clusters: Vec<serde_json::Value> = resp.take(0)?;
    assert!(
        layer0_clusters.len() >= 4,
        "expected at least 4 layer0 clusters, got {}",
        layer0_clusters.len()
    );

    // Build hierarchical layers with min_clusters=2 (should build at least layer 1)
    // With 4+ layer0 clusters, sqrt(4)=2, so it should create 2 layer1 clusters
    build_hierarchical_layers(&conn, Some("testRepo"), 0, 2).await?;

    // Query all clusters with layer field to check we have multiple layers
    let mut all_resp = conn
        .query("SELECT stable_id, layer FROM hirag_cluster WHERE stable_id CONTAINS 'testRepo'")
        .await?;
    let all_clusters: Vec<serde_json::Value> = all_resp.take(0)?;

    // Find unique layers
    let mut layers = std::collections::HashSet::new();
    for c in all_clusters.iter() {
        if let Some(layer) = c.get("layer").and_then(|v| v.as_i64()) {
            layers.insert(layer);
        } else if let Some(sid) = c.get("stable_id").and_then(|v| v.as_str()) {
            if sid.contains("layer0") {
                layers.insert(0);
            } else if sid.contains("layer1") {
                layers.insert(1);
            } else if sid.contains("layer2") {
                layers.insert(2);
            }
        }
    }

    assert!(
        layers.len() >= 2,
        "expected at least 2 layers (0 and 1), got layers: {:?}",
        layers
    );
    assert!(
        layers.contains(&0),
        "expected layer 0 to exist, got layers: {:?}",
        layers
    );
    assert!(
        layers.contains(&1),
        "expected layer 1 to exist, got layers: {:?}",
        layers
    );

    Ok(())
}

#[tokio::test]
async fn build_hierarchical_layers_respects_max_depth() -> Result<(), Box<dyn std::error::Error>> {
    use hyperzoekt::db::connection::connect;
    use hyperzoekt::hirag::{build_first_layer_for_repo, build_hierarchical_layers};
    use serde_json::json;

    let _guard = surreal_test_lock().lock().await;

    let prev_disable = std::env::var("HZ_DISABLE_SURREAL_ENV").ok();
    let prev_max_depth = std::env::var("HZ_HIRAG_MAX_DEPTH").ok();

    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    std::env::set_var("HZ_HIRAG_MAX_DEPTH", "2"); // Force max depth of 2

    let conn = connect(&None, &None, &None, "testns", "testdb").await?;

    conn.use_ns("test_max_depth_ns")
        .await
        .expect("failed to select max depth namespace");
    conn.use_db("test_max_depth_db")
        .await
        .expect("failed to select max depth database");
    let _ = conn.query("REMOVE TABLE hirag_cluster").await;
    let _ = conn.query("REMOVE TABLE entity_snapshot").await;
    let _ = conn.query("REMOVE TABLE entity").await;

    // Create many entities (50) to ensure we could build more than 2 layers if allowed
    for i in 0..50 {
        let eid = format!("ent_depth_{}", i);
        let e = json!({ "stable_id": eid.clone() });
        conn.query_with_binds("CREATE entity CONTENT $e", vec![("e", e)])
            .await?;

        let emb = vec![(i as f32) / 50.0, 1.0 - (i as f32) / 50.0];
        let s = json!({
            "id": eid.clone(),
            "stable_id": eid.clone(),
            "source_content": format!("fn func_{}() {{}}", i),
            "embedding": emb,
            "embedding_len": 2,
            "repo_name": "depthRepo"
        });
        conn.query_with_binds(
            &format!("CREATE entity_snapshot:{} CONTENT $s", eid),
            vec![("s", s)],
        )
        .await?;
    }

    // Build layer 0
    build_first_layer_for_repo(&conn, "depthRepo", None, 7).await?;

    // Build hierarchical with min_clusters=1 (very permissive)
    // This should still stop at layer 2 due to HZ_HIRAG_MAX_DEPTH=2
    build_hierarchical_layers(&conn, Some("depthRepo"), 0, 1).await?;

    // Check that we don't have layer 3 or higher
    let mut resp = conn
        .query("SELECT id, layer FROM hirag_cluster WHERE layer > 2 OR id CONTAINS 'layer3'")
        .await?;
    let layer3_plus: Vec<serde_json::Value> = resp.take(0)?;

    assert_eq!(
        layer3_plus.len(),
        0,
        "expected no clusters beyond layer 2 due to max depth, got: {:?}",
        layer3_plus
    );

    // Restore env vars
    if let Some(v) = prev_disable {
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", v);
    } else {
        std::env::remove_var("HZ_DISABLE_SURREAL_ENV");
    }
    if let Some(v) = prev_max_depth {
        std::env::set_var("HZ_HIRAG_MAX_DEPTH", v);
    } else {
        std::env::remove_var("HZ_HIRAG_MAX_DEPTH");
    }

    Ok(())
}

#[tokio::test]
async fn build_hierarchical_layers_stops_at_min_clusters() -> Result<(), Box<dyn std::error::Error>>
{
    use hyperzoekt::db::connection::connect;
    use hyperzoekt::hirag::{build_first_layer_for_repo, build_hierarchical_layers};
    use serde_json::json;

    let _guard = surreal_test_lock().lock().await;

    let prev = std::env::var("HZ_DISABLE_SURREAL_ENV").ok();
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    let conn = connect(&None, &None, &None, "testns", "testdb").await?;
    if let Some(v) = prev {
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", v);
    } else {
        std::env::remove_var("HZ_DISABLE_SURREAL_ENV");
    }

    conn.use_ns("test_min_clusters_ns")
        .await
        .expect("failed to select min clusters namespace");
    conn.use_db("test_min_clusters_db")
        .await
        .expect("failed to select min clusters database");

    let _ = conn.query("REMOVE TABLE hirag_cluster").await;
    let _ = conn.query("REMOVE TABLE entity_snapshot").await;
    let _ = conn.query("REMOVE TABLE entity").await;

    // Create 10 entities
    for i in 0..10 {
        let eid = format!("ent_min_{}", i);
        let e = json!({ "stable_id": eid.clone() });
        conn.query_with_binds("CREATE entity CONTENT $e", vec![("e", e)])
            .await?;

        let emb = vec![(i as f32) / 10.0, 1.0 - (i as f32) / 10.0];
        let s = json!({
            "id": eid.clone(),
            "stable_id": eid.clone(),
            "source_content": format!("fn func_{}() {{}}", i),
            "embedding": emb,
            "embedding_len": 2,
            "repo_name": "minRepo"
        });
        conn.query_with_binds(
            &format!("CREATE entity_snapshot:{} CONTENT $s", eid),
            vec![("s", s)],
        )
        .await?;
    }

    // Build layer 0 with k=3 (should create 3 clusters)
    build_first_layer_for_repo(&conn, "minRepo", None, 3).await?;

    let mut resp = conn
        .query("SELECT stable_id FROM hirag_cluster WHERE stable_id CONTAINS 'minRepo'")
        .await?;
    let layer0: Vec<serde_json::Value> = resp.take(0)?;
    assert_eq!(
        layer0.len(),
        3,
        "expected exactly 3 layer0 clusters, got {}",
        layer0.len()
    );

    // Build hierarchical with min_clusters=3
    // Since layer0 has exactly 3 clusters, it should NOT build layer1
    build_hierarchical_layers(&conn, Some("minRepo"), 0, 3).await?;

    let mut resp1 = conn
        .query("SELECT stable_id FROM hirag_cluster WHERE stable_id CONTAINS 'layer1'")
        .await?;
    let layer1: Vec<serde_json::Value> = resp1.take(0)?;

    assert_eq!(
        layer1.len(),
        0,
        "expected no layer1 clusters due to min_clusters termination, got {}",
        layer1.len()
    );

    Ok(())
}

#[tokio::test]
async fn hierarchical_clusters_have_proper_structure() -> Result<(), Box<dyn std::error::Error>> {
    use hyperzoekt::db::connection::connect;
    use hyperzoekt::hirag::{build_first_layer_for_repo, build_hierarchical_layers};
    use serde_json::json;

    let _guard = surreal_test_lock().lock().await;

    let prev = std::env::var("HZ_DISABLE_SURREAL_ENV").ok();
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    let conn = connect(&None, &None, &None, "testns", "testdb").await?;
    if let Some(v) = prev {
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", v);
    } else {
        std::env::remove_var("HZ_DISABLE_SURREAL_ENV");
    }

    conn.use_ns("test_structure_ns")
        .await
        .expect("failed to select structure namespace");
    conn.use_db("test_structure_db")
        .await
        .expect("failed to select structure database");

    let _ = conn.query("REMOVE TABLE hirag_cluster").await;
    let _ = conn.query("REMOVE TABLE entity_snapshot").await;
    let _ = conn.query("REMOVE TABLE entity").await;

    // Create 15 entities
    for i in 0..15 {
        let eid = format!("ent_struct_{}", i);
        let e = json!({ "stable_id": eid.clone() });
        conn.query_with_binds("CREATE entity CONTENT $e", vec![("e", e)])
            .await?;

        let emb = vec![(i as f32) / 15.0, 1.0 - (i as f32) / 15.0, 0.5];
        let s = json!({
            "id": eid.clone(),
            "stable_id": eid.clone(),
            "source_content": format!("fn func_{}() {{}}", i),
            "embedding": emb,
            "embedding_len": 3,
            "repo_name": "structRepo"
        });
        conn.query_with_binds(
            &format!("CREATE entity_snapshot:{} CONTENT $s", eid),
            vec![("s", s)],
        )
        .await?;
    }

    // Build layer 0
    build_first_layer_for_repo(&conn, "structRepo", None, 4).await?;

    // Build hierarchical layers
    build_hierarchical_layers(&conn, Some("structRepo"), 0, 2).await?;

    // Check layer1 clusters have proper structure
    #[derive(serde::Deserialize)]
    struct Layer1Cluster {
        stable_id: String,
        label: String,
        members: Vec<String>,
        centroid: Vec<f32>,
        centroid_len: usize,
        layer: Option<i64>,
        member_repos: Vec<String>,
    }

    let mut resp = conn
        .query("SELECT stable_id, label, members, centroid, centroid_len, layer, member_repos FROM hirag_cluster WHERE id CONTAINS 'layer1'")
        .await?;
    let layer1_clusters: Vec<Layer1Cluster> = resp.take(0)?;

    if !layer1_clusters.is_empty() {
        for cluster in layer1_clusters.iter() {
            // Check structure
            assert!(
                cluster.stable_id.contains("layer1"),
                "layer1 cluster should have layer1 in stable_id"
            );
            assert!(
                cluster.label.contains("layer1"),
                "layer1 cluster should have layer1 in label"
            );
            assert_eq!(
                cluster.layer,
                Some(1),
                "layer1 cluster should have layer field = 1"
            );

            // Check members are layer0 cluster IDs
            assert!(
                !cluster.members.is_empty(),
                "layer1 cluster should have members"
            );
            for member in cluster.members.iter() {
                assert!(
                    member.contains("layer0"),
                    "layer1 cluster members should be layer0 cluster IDs, got: {}",
                    member
                );
            }

            // Check centroid
            assert!(
                !cluster.centroid.is_empty(),
                "layer1 cluster should have non-empty centroid"
            );
            assert_eq!(
                cluster.centroid.len(),
                cluster.centroid_len,
                "centroid_len should match centroid length"
            );

            // Check repo provenance
            assert!(
                cluster.member_repos.contains(&"structRepo".to_string()),
                "layer1 cluster should have structRepo in member_repos"
            );
        }
    }

    Ok(())
}
