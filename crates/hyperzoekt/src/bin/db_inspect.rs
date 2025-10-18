use hyperzoekt::db::connection::connect;
use std::env;

#[tokio::main]
async fn main() {
    let url = env::var("SURREALDB_URL")
        .ok()
        .unwrap_or_else(|| "http://localhost:8000".to_string());
    let user = env::var("SURREALDB_USERNAME").ok();
    let pass = env::var("SURREALDB_PASSWORD").ok();
    let ns = env::var("SURREAL_NS").unwrap_or_else(|_| "zoekt".to_string());
    let db = env::var("SURREAL_DB").unwrap_or_else(|_| "repos".to_string());

    println!("Connecting to Surreal at {} ns={} db={}", url, ns, db);
    match connect(&Some(url.clone()), &user, &pass, &ns, &db).await {
        Ok(conn) => {
            if let Err(e) = conn.use_ns(&ns).await {
                eprintln!("use_ns failed: {}", e);
            }
            if let Err(e) = conn.use_db(&db).await {
                eprintln!("use_db failed: {}", e);
            }

            for q in [
                "SELECT count() AS c FROM repo GROUP ALL;",
                "SELECT * FROM repo LIMIT 20;",
                "SELECT count() AS c FROM file GROUP ALL;",
                "SELECT * FROM file LIMIT 20;",
                "SELECT count() AS c FROM has_file GROUP ALL;",
                "SELECT * FROM has_file LIMIT 20;",
                "SELECT count() AS c FROM in_repo GROUP ALL;",
                "SELECT * FROM in_repo LIMIT 20;",
                // Test-specific traversal queries for debugging
                "SELECT <-in_repo<-commits->has_file->file.path AS files FROM repo WHERE name='repo_one';",
                "SELECT <-in_repo<-commits->has_file->file.path AS files FROM repo WHERE name='repo_two';",
                "SELECT <-has_file<-commits->in_repo->repo.name AS repos FROM file WHERE path='src/file1.rs';",
            ] {
                println!("--- QUERY: {}", q);
                match conn.query(q).await {
                    Ok(mut resp) => {
                        let vals: Vec<serde_json::Value> = resp.take(0).unwrap_or_default();
                        println!("SLOT0: {} rows", vals.len());
                        for v in vals.iter() {
                            println!("  -> {}", v);
                        }
                        // dump up to 3 slots
                        for i in 1..3 {
                            let other: Vec<serde_json::Value> = resp.take(i).unwrap_or_default();
                            if !other.is_empty() {
                                println!("SLOT{}: {} rows", i, other.len());
                                for v in other.iter() {
                                    println!("  -> {}", v);
                                }
                            }
                        }
                    }
                    Err(e) => eprintln!("query failed: {}", e),
                }
            }
        }
        Err(e) => {
            eprintln!("connect failed: {}", e);
        }
    }
}
