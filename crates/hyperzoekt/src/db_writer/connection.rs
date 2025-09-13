// Copyright 2025 HyperZoekt Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use log::{error, info};
use std::sync::{Arc, OnceLock};
use surrealdb::engine::local::Mem;
use surrealdb::engine::remote::http::{Http, Https};
use surrealdb::Surreal;
use tokio::runtime::Runtime;

fn surreal_debug_enabled() -> bool {
    match std::env::var("SURREAL_DEBUG") {
        Ok(v) => {
            let v = v.to_lowercase();
            v == "1" || v == "true" || v == "yes"
        }
        Err(_) => false,
    }
}

pub static SHARED_MEM: OnceLock<Arc<Surreal<surrealdb::engine::local::Db>>> = OnceLock::new();
// Ensure the runtime that services the embedded Mem instance lives for the
// lifetime of the process. If tests create per-test tokio runtimes and the
// Mem instance was created on one of them, dropping that runtime can leave
// Surreal's internal channels pointing to a closed runtime, causing "sending
// into a closed channel" errors when other tests use the shared instance.
pub static SHARED_MEM_RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();

pub enum SurrealConnection {
    Local(Arc<Surreal<surrealdb::engine::local::Db>>),
    RemoteHttp(Surreal<surrealdb::engine::remote::http::Client>),
    RemoteWs(Surreal<surrealdb::engine::remote::http::Client>),
}

impl SurrealConnection {
    pub async fn use_ns(&self, namespace: &str) -> Result<(), surrealdb::Error> {
        match self {
            Self::Local(db) => db.use_ns(namespace).await,
            Self::RemoteHttp(db) => db.use_ns(namespace).await,
            Self::RemoteWs(db) => db.use_ns(namespace).await,
        }
    }
    pub async fn use_db(&self, database: &str) -> Result<(), surrealdb::Error> {
        match self {
            Self::Local(db) => db.use_db(database).await,
            Self::RemoteHttp(db) => db.use_db(database).await,
            Self::RemoteWs(db) => db.use_db(database).await,
        }
    }
    pub async fn query(&self, sql: &str) -> Result<surrealdb::Response, surrealdb::Error> {
        if surreal_debug_enabled() {
            println!(
                "DEBUG: SurrealConnection.query called with SQL: {}",
                sql.chars().take(100).collect::<String>()
            );
        }
        match self {
            Self::Local(db) => {
                if surreal_debug_enabled() {
                    println!("DEBUG: Using Local connection for query");
                }
                match db.query(sql).await {
                    Ok(resp) => {
                        if surreal_debug_enabled() {
                            println!("DEBUG: SurrealConnection.query response: {:?}", resp);
                        }
                        Ok(resp)
                    }
                    Err(e) => {
                        if surreal_debug_enabled() {
                            println!("DEBUG: SurrealConnection.query error: {}", e);
                        }
                        Err(e)
                    }
                }
            }
            Self::RemoteHttp(db) => {
                if surreal_debug_enabled() {
                    println!("DEBUG: Using RemoteHttp connection for query");
                }
                match db.query(sql).await {
                    Ok(resp) => {
                        if surreal_debug_enabled() {
                            println!("DEBUG: SurrealConnection.query response: {:?}", resp);
                        }
                        Ok(resp)
                    }
                    Err(e) => {
                        if surreal_debug_enabled() {
                            println!("DEBUG: SurrealConnection.query error: {}", e);
                        }
                        Err(e)
                    }
                }
            }
            Self::RemoteWs(db) => {
                if surreal_debug_enabled() {
                    println!("DEBUG: Using RemoteWs (via HTTP client) connection for query");
                }
                match db.query(sql).await {
                    Ok(resp) => {
                        if surreal_debug_enabled() {
                            println!("DEBUG: SurrealConnection.query response: {:?}", resp);
                        }
                        Ok(resp)
                    }
                    Err(e) => {
                        if surreal_debug_enabled() {
                            println!("DEBUG: SurrealConnection.query error: {}", e);
                        }
                        Err(e)
                    }
                }
            } // RemoteWs variant removed; remote HTTP client is used instead.
        }
    }
    pub async fn query_with_binds(
        &self,
        sql: &str,
        binds: Vec<(&'static str, serde_json::Value)>,
    ) -> Result<surrealdb::Response, surrealdb::Error> {
        match self {
            Self::Local(db) => {
                let mut call = db.query(sql);
                for (k, v) in binds.into_iter() {
                    call = call.bind((k, v));
                }
                call.await
            }
            Self::RemoteHttp(db) => {
                let mut call = db.query(sql);
                for (k, v) in binds.into_iter() {
                    call = call.bind((k, v));
                }
                call.await
            }
            Self::RemoteWs(db) => {
                let mut call = db.query(sql);
                for (k, v) in binds.into_iter() {
                    call = call.bind((k, v));
                }
                call.await
            } // RemoteWs variant removed; remote HTTP client is used instead.
        }
    }
}

/// Establish a SurrealDB connection.
/// Precedence for selecting connection parameters:
/// 1. Explicit values passed in DbWriterConfig (Some(..))
/// 2. Environment variables: SURREALDB_URL, SURREALDB_USERNAME, SURREALDB_PASSWORD
/// 3. Embedded in-memory engine (Mem)
///    Tests can force ignoring env-based fallback by setting HZ_DISABLE_SURREAL_ENV=1.
pub async fn connect(
    url: &Option<String>,
    user: &Option<String>,
    pass: &Option<String>,
    ns: &str,
    db: &str,
) -> anyhow::Result<SurrealConnection> {
    if surreal_debug_enabled() {
        println!("LOG: connect called with url={:?}", url);
    }
    let allow_env = std::env::var("HZ_DISABLE_SURREAL_ENV").ok().as_deref() != Some("1");
    let resolved_url = if url.is_some() {
        url.clone()
    } else if allow_env {
        std::env::var("SURREALDB_URL").ok()
    } else {
        None
    };
    if surreal_debug_enabled() {
        eprintln!(
            "LOG: connect called with url={:?}, allow_env={}, resolved_url={:?}",
            url, allow_env, resolved_url
        );
    }
    let resolved_user = if user.is_some() {
        user.clone()
    } else if allow_env {
        std::env::var("SURREALDB_USERNAME").ok()
    } else {
        None
    };
    let resolved_pass = if pass.is_some() {
        pass.clone()
    } else if allow_env {
        std::env::var("SURREALDB_PASSWORD").ok()
    } else {
        None
    };

    let conn = if let Some(url) = resolved_url {
        // A SURREALDB_URL was provided; prefer using a remote HTTP client so
        // tests that require a graph-capable SurrealDB (RELATE queries) get a
        // real remote instance. Only fall back to SHARED_MEM when no URL is
        // provided.
        // Prefer using the HTTP remote client for all remote connections.
        // Normalize ws:// -> http://, wss:// -> https://, and scheme-less host:port -> http://host:port
        let mut http_url = if url.starts_with("http://") || url.starts_with("https://") {
            // Use the provided HTTP(S) URL. Ensure it targets the RPC path when needed.
            if url.contains("/rpc") {
                url.clone()
            } else {
                let stripped = url.trim_end_matches('/');
                format!("{}/rpc", stripped)
            }
        } else if url.starts_with("ws://") || url.starts_with("wss://") {
            // Convert websocket scheme to HTTP(s) and preserve path when present.
            if url.starts_with("wss://") {
                url.replacen("wss://", "https://", 1)
            } else {
                url.replacen("ws://", "http://", 1)
            }
        } else {
            // Scheme-less: assume http and target /rpc
            let host = url.trim_end_matches('/');
            format!("http://{}/rpc", host)
        };

        // Ensure http_url has /rpc path
        if !http_url.contains("/rpc") {
            http_url = format!("{}{}", http_url.trim_end_matches('/'), "/rpc");
        }

        // Sanitize common malformed forms and log final URL for debugging.
        if http_url.contains("http//") {
            http_url = http_url.replace("http//", "http://");
        }
        if http_url.contains("https//") {
            http_url = http_url.replace("https//", "https://");
        }
        // Remove accidental duplicate scheme prefixes like http://http://
        if http_url.contains("http://http://") {
            http_url = http_url.replacen("http://http://", "http://", 1);
        }
        if http_url.contains("https://https://") {
            http_url = http_url.replacen("https://https://", "https://", 1);
        }
        if surreal_debug_enabled() {
            eprintln!("LOG: normalized Surreal HTTP URL: {}", http_url);
        }

        // Also expose a normalized base HTTP URL (without the /rpc suffix)
        // to other components/tests that may read SURREALDB_URL and append
        // paths like /health. This avoids accidental double-scheme forms
        // when callers naively prefix "http://" to an already-broken
        // input (for example "http//127.0.0.1:8000"). Setting the env
        // here keeps the process-wide view consistent after connect().
        let base = if http_url.ends_with("/rpc") {
            http_url
                .trim_end_matches("/rpc")
                .trim_end_matches('/')
                .to_string()
        } else {
            http_url.trim_end_matches('/').to_string()
        };
        // Set both SURREALDB_URL (for legacy callers) and a dedicated
        // SURREALDB_HTTP_BASE to avoid surprises.
        std::env::set_var("SURREALDB_URL", &base);
        std::env::set_var("SURREALDB_HTTP_BASE", &base);
        eprintln!("LOG: normalized Surreal HTTP base set in env: {}", base);

        // Diagnostics: parse and print the final http_url components so we can
        // observe how downstream clients (reqwest/hyper) will interpret it.
        // This helps locate cases where a malformed string (for example
        // starting with "http//") later becomes used as a host when callers
        // naively prefix "http://".
        match url::Url::parse(&http_url) {
            Ok(u) => {
                if surreal_debug_enabled() {
                    eprintln!(
                        "DEBUG: parsed http_url scheme={:?} host={:?} port={:?} path={}",
                        u.scheme(),
                        u.host_str(),
                        u.port(),
                        u.path()
                    );
                }
            }
            Err(e) => {
                if surreal_debug_enabled() {
                    eprintln!("DEBUG: failed to parse http_url='{}' err={}", http_url, e);
                }
            }
        }
        if surreal_debug_enabled() {
            eprintln!(
                "DEBUG: env SURREALDB_URL='{:?}' SURREALDB_HTTP_BASE='{:?}'",
                std::env::var("SURREALDB_URL").ok(),
                std::env::var("SURREALDB_HTTP_BASE").ok()
            );
        }

        // Create the Surreal client with a defensive retry: if the first attempt
        // fails with an error that looks like it's caused by a malformed URL
        // (for example error messages containing "http//" or "http://http"),
        // try sanitizing and retry once. This guards against upstream clients
        // that may build health probes incorrectly from unnormalized inputs.
        let mut last_err: Option<anyhow::Error> = None;
        let mut connection_opt: Option<Surreal<surrealdb::engine::remote::http::Client>> = None;
        // Some variants of the Surreal client internally prefix schemes when
        // composing health-check URLs. To avoid accidental double-prefixing
        // or malformed probe URLs, pass a scheme-less host:port(/rpc) string
        // to Surreal::new. The client will add the appropriate scheme.
        let mut client_target = http_url.clone();
        if client_target.starts_with("http://") {
            client_target = client_target.replacen("http://", "", 1);
        } else if client_target.starts_with("https://") {
            client_target = client_target.replacen("https://", "", 1);
        }
        // Ensure no leading slashes remain
        while client_target.starts_with('/') {
            client_target = client_target.replacen("/", "", 1);
        }

        for attempt in 0..2 {
            let try_conn = if http_url.starts_with("https://") {
                Surreal::new::<Https>(&client_target).await
            } else {
                Surreal::new::<Http>(&client_target).await
            };
            match try_conn {
                Ok(c) => {
                    connection_opt = Some(c);
                    break;
                }
                Err(e) => {
                    let serr = format!("{}", e);
                    eprintln!(
                        "WARN: Surreal::new attempt {} failed: {}",
                        attempt + 1,
                        serr
                    );
                    last_err = Some(anyhow::anyhow!(serr.clone()));
                    // If this looks like a malformed-scheme issue, attempt to sanitize
                    // environment and http_url and retry once.
                    if attempt == 0 && (serr.contains("http//") || serr.contains("http://http")) {
                        eprintln!("WARN: Detected malformed URL pattern in Surreal client error; sanitizing and retrying");
                        // Sanitize env vars we set earlier and recompute http_url/base
                        if let Ok(mut base_env) = std::env::var("SURREALDB_URL") {
                            if base_env.contains("http//") {
                                base_env = base_env.replace("http//", "http://");
                                std::env::set_var("SURREALDB_URL", &base_env);
                                std::env::set_var("SURREALDB_HTTP_BASE", &base_env);
                                if surreal_debug_enabled() {
                                    eprintln!(
                                        "DEBUG: repaired SURREALDB_URL in env to {}",
                                        base_env
                                    );
                                }
                            }
                        }
                        // Also try repairing http_url variable in-place for retry
                        if http_url.contains("http//") {
                            http_url = http_url.replace("http//", "http://");
                        }
                        if http_url.contains("http://http://") {
                            http_url = http_url.replacen("http://http://", "http://", 1);
                        }
                        // Loop will retry
                        continue;
                    }
                }
            }
        }
        let connection = if let Some(c) = connection_opt {
            c
        } else {
            return Err(
                last_err.unwrap_or_else(|| anyhow::anyhow!("failed to create Surreal client"))
            );
        };
        if let (Some(u), Some(p)) = (resolved_user.as_ref(), resolved_pass.as_ref()) {
            connection
                .signin(surrealdb::opt::auth::Root {
                    username: u,
                    password: p,
                })
                .await?;
        }
        SurrealConnection::RemoteHttp(connection)
    } else {
        if surreal_debug_enabled() {
            println!("LOG: taking SHARED_MEM path");
        }
        info!(
            "No SURREALDB_URL provided, using embedded Mem ns={} db={}",
            ns, db
        );

        // If tests explicitly request an ephemeral Mem instance, create it
        // in-process and return it directly to avoid any cross-runtime
        // lifetime issues.
        if std::env::var("HZ_EPHEMERAL_MEM").ok().as_deref() == Some("1") {
            if surreal_debug_enabled() {
                eprintln!("LOG: Creating ephemeral Mem instance (HZ_EPHEMERAL_MEM=1)");
            }
            let arc = Arc::new(Surreal::new::<Mem>(()).await?);
            SurrealConnection::Local(arc)
        } else {
            // Prefer existing shared instance (set by tests or other components)
            let has_existing = SHARED_MEM.get().is_some();
            if surreal_debug_enabled() {
                eprintln!("LOG: SHARED_MEM.get().is_some(): {}", has_existing);
            }
            if let Some(existing) = SHARED_MEM.get() {
                if surreal_debug_enabled() {
                    eprintln!(
                        "LOG: Using existing SHARED_MEM instance: {:p}",
                        Arc::as_ptr(existing)
                    );
                }
                SurrealConnection::Local(existing.clone())
            } else {
                if surreal_debug_enabled() {
                    eprintln!("LOG: Creating new Mem instance and setting SHARED_MEM");
                }
                use std::sync::mpsc::channel as std_channel;
                let (tx, rx) = std_channel();
                std::thread::spawn(move || match Runtime::new() {
                    Ok(rt2) => {
                        let res = rt2.block_on(async { Surreal::new::<Mem>(()).await });
                        match res {
                            Ok(s) => {
                                let _ = tx.send(Ok((Arc::new(s), rt2)));
                            }
                            Err(e) => {
                                let _ = tx.send(Err(anyhow::anyhow!(e)));
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(anyhow::anyhow!(e)));
                    }
                });

                let res = tokio::task::spawn_blocking(move || rx.recv())
                    .await
                    .map_err(|e| anyhow::anyhow!(e))?;
                match res {
                    Ok(Ok((s, rt2))) => {
                        let a = s;
                        let _ = SHARED_MEM.set(a.clone());
                        let _ = SHARED_MEM_RUNTIME.set(Arc::new(rt2));
                        SurrealConnection::Local(a)
                    }
                    Ok(Err(e)) => {
                        error!("Failed to start embedded SurrealDB: {}", e);
                        return Err(e);
                    }
                    Err(e) => {
                        error!("Failed to receive embedded SurrealDB result: {}", e);
                        return Err(anyhow::anyhow!("failed to create embedded SurrealDB"));
                    }
                }
            }
        }
    };
    conn.use_ns(ns).await?;
    conn.use_db(db).await?;
    Ok(conn)
}
