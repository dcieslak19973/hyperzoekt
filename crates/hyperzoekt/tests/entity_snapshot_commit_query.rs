// Minimal, safe integration test for entity_snapshot queries against a real SurrealDB.
// This file is intentionally small and idempotent so it can be used while
// response normalization is being diagnosed.

use hyperzoekt::db::connection::connect as hz_connect;
// use hyperzoekt::db::helpers; // not needed in this lightweight debug test
use serde_json::Value as JsonValue;
use surrealdb::sql::Value as SqlValue;
use surrealdb::Response as SurrealResponse;

#[allow(dead_code)]
fn sql_value_to_json(v: &SqlValue) -> JsonValue {
    use surrealdb::sql::Value;
    match v {
        Value::Thing(_) => JsonValue::String(v.to_string()),
        Value::Strand(_) => JsonValue::String(v.to_string()),
        Value::Bool(b) => JsonValue::Bool(*b),
        Value::Number(n) => {
            let s = format!("{}", n);
            if let Ok(i) = s.parse::<i64>() {
                JsonValue::Number(serde_json::Number::from(i))
            } else if let Ok(f) = s.parse::<f64>() {
                serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            } else {
                JsonValue::String(s)
            }
        }
        Value::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (k, v2) in obj.iter() {
                map.insert(k.clone(), sql_value_to_json(v2));
            }
            JsonValue::Object(map)
        }
        Value::Array(arr) => JsonValue::Array(arr.iter().map(sql_value_to_json).collect()),
        _ => JsonValue::String(v.to_string()),
    }
}

fn resp_count_debug(resp: SurrealResponse) -> usize {
    let s = format!("{:?}", resp);
    eprintln!("resp_count_debug: resp={}", s);
    let mut count = 0usize;
    count += s.matches("\"stable_id\":").count();
    count += s.matches("Thing(Thing { tb: \"entity_snapshot\"").count();
    count
}

#[tokio::test]
#[ignore = "Requires manual SurrealDB setup and CLI testing"]
async fn test_entity_snapshot_commit_filter_real_db() -> Result<(), Box<dyn std::error::Error>> {
    let conn = hz_connect(
        &Some("http://127.0.0.1:8000".to_string()),
        &Some("root".to_string()),
        &Some("root".to_string()),
        "test_ns",
        "test_db",
    )
    .await?;

    // idempotent cleanup
    conn.query("DELETE entity_snapshot WHERE repo_name = 'test_commit_filter';")
        .await?;

    // create two fixtures with the same commit tag
    conn.query("DELETE entity_snapshot:test_commit_1;").await?;
    conn.query(&format!(
        r#"CREATE entity_snapshot:test_commit_1 CONTENT {{ stable_id: "test_commit_1", repo_name: "test_commit_filter", sourcecontrol_commit: "commits:abc", embedding_len: 768 }};"#
    ))
    .await?;

    conn.query("DELETE entity_snapshot:test_commit_2;").await?;
    conn.query(&format!(
        r#"CREATE entity_snapshot:test_commit_2 CONTENT {{ stable_id: "test_commit_2", repo_name: "test_commit_filter", sourcecontrol_commit: "commits:abc", embedding_len: 768 }};"#
    ))
    .await?;

    let query_with_cast = r#"SELECT id, stable_id FROM entity_snapshot WHERE repo_name = 'test_commit_filter' AND type::string(sourcecontrol_commit) = 'commits:abc'"#;

    let resp = conn.query(query_with_cast).await?;
    let found = resp_count_debug(resp);
    eprintln!("test saw {} candidate rows via debug parsing", found);
    assert!(
        found >= 2,
        "Expected at least two stable_id/Thing occurrences"
    );

    // cleanup
    conn.query("DELETE entity_snapshot WHERE repo_name = 'test_commit_filter';")
        .await?;

    Ok(())
}
