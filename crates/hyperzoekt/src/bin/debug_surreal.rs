use hyperzoekt::db::connection::connect;
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Force embedded Mem
    std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
    let conn = connect(&None, &None, &None, "testns", "testdb").await?;
    conn.use_ns("test").await.ok();
    conn.use_db("test").await.ok();

    let _ = conn.query("REMOVE entity_snapshot").await;

    let s1 = json!({ "id": "entity_snapshot:ent_dbg", "stable_id": "ent_dbg", "source_content": "fn dbg() {}", "embedding": [0.0, 1.0], "embedding_len": 2, "repo_name": "repoDBG" });
    let res = conn
        .query_with_binds("CREATE entity_snapshot:ent_dbg CONTENT $s", vec![("s", s1)])
        .await;
    println!("CREATE result: {:?}", res);

    let mut resp = conn
        .query("SELECT embedding FROM entity_snapshot WHERE stable_id = \"ent_dbg\" LIMIT 1")
        .await?;
    let rows: Vec<serde_json::Value> = resp.take(0)?;
    println!("SELECT rows: {:?}", rows);

    Ok(())
}
