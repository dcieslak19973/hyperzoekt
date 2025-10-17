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

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use surrealdb::engine::local::Mem;
use surrealdb::engine::remote::http::{Http, Https};
use surrealdb::Surreal;
use tokio::sync::RwLock;

enum SurrealConnection {
    Local(Surreal<surrealdb::engine::local::Db>),
    Remote(Surreal<surrealdb::engine::remote::http::Client>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoMetadata {
    pub id: String, // SurrealDB record ID
    pub name: String,
    pub git_url: String,
    pub branch: Option<String>,
    pub visibility: zoekt_rs::types::RepoVisibility,
    pub owner: Option<String>,
    pub allowed_users: Vec<String>,
    pub last_commit_sha: Option<String>,
    pub last_indexed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl SurrealConnection {
    #[allow(dead_code)]
    async fn use_ns(&self, namespace: &str) -> Result<(), surrealdb::Error> {
        match self {
            SurrealConnection::Local(db) => db.use_ns(namespace).await,
            SurrealConnection::Remote(db) => db.use_ns(namespace).await,
        }
    }

    #[allow(dead_code)]
    async fn use_db(&self, database: &str) -> Result<(), surrealdb::Error> {
        match self {
            SurrealConnection::Local(db) => db.use_db(database).await,
            SurrealConnection::Remote(db) => db.use_db(database).await,
        }
    }
}

pub struct SurrealRepoStore {
    db: Arc<RwLock<SurrealConnection>>,
    #[allow(dead_code)]
    namespace: String,
    #[allow(dead_code)]
    database: String,
}

impl SurrealRepoStore {
    /// Create a new SurrealRepoStore with in-memory database (for development/testing)
    pub async fn new_in_memory(namespace: String, database: String) -> Result<Self> {
        let db = Surreal::new::<Mem>(()).await?;
        Self::init_connection(SurrealConnection::Local(db), namespace, database).await
    }

    /// Create a new SurrealRepoStore with remote database connection
    pub async fn new_remote(
        url: String,
        username: String,
        password: String,
        namespace: String,
        database: String,
    ) -> Result<Self> {
        // Defensive sanitization: mirror hyperzoekt's normalization so callers can
        // provide http(s), ws(s), or scheme-less host:port values.
        let mut u = url.trim().to_string();
        if u.starts_with("http//") {
            u = u.replacen("http//", "http://", 1);
        } else if u.starts_with("https//") {
            u = u.replacen("https//", "https://", 1);
        } else if u.starts_with("ws//") {
            u = u.replacen("ws//", "ws://", 1);
        } else if u.starts_with("wss//") {
            u = u.replacen("wss//", "wss://", 1);
        }
        // Remove accidental duplicate schemes like "http://http://"
        if u.contains("http://http://") {
            u = u.replacen("http://http://", "http://", 1);
        }
        if u.contains("https://https://") {
            u = u.replacen("https://https://", "https://", 1);
        }

        // Normalize to an HTTP(S) URL targeting the /rpc path.
        let mut http_url = if u.starts_with("http://") || u.starts_with("https://") {
            if u.contains("/rpc") {
                u.clone()
            } else {
                let stripped = u.trim_end_matches('/');
                format!("{}/rpc", stripped)
            }
        } else if u.starts_with("ws://") || u.starts_with("wss://") {
            if u.starts_with("wss://") {
                let v = u.replacen("wss://", "https://", 1);
                if v.contains("/rpc") {
                    v
                } else {
                    format!("{}/rpc", v.trim_end_matches('/'))
                }
            } else {
                let v = u.replacen("ws://", "http://", 1);
                if v.contains("/rpc") {
                    v
                } else {
                    format!("{}/rpc", v.trim_end_matches('/'))
                }
            }
        } else {
            let host = u.trim_end_matches('/');
            format!("http://{}/rpc", host)
        };

        // Sanitize common malformed forms
        if http_url.contains("http//") {
            http_url = http_url.replace("http//", "http://");
        }
        if http_url.contains("https//") {
            http_url = http_url.replace("https//", "https://");
        }
        if http_url.contains("http://http://") {
            http_url = http_url.replacen("http://http://", "http://", 1);
        }
        if http_url.contains("https://https://") {
            http_url = http_url.replacen("https://https://", "https://", 1);
        }

        // Expose normalized base without /rpc for legacy callers/tests
        let base = if http_url.ends_with("/rpc") {
            http_url
                .trim_end_matches("/rpc")
                .trim_end_matches('/')
                .to_string()
        } else {
            http_url.trim_end_matches('/').to_string()
        };
        std::env::set_var("SURREALDB_URL", &base);
        std::env::set_var("SURREALDB_HTTP_BASE", &base);

        // Prepare client target by removing scheme so Surreal::new won't double-prefix
        let mut client_target = http_url.clone();
        if client_target.starts_with("http://") {
            client_target = client_target.replacen("http://", "", 1);
        } else if client_target.starts_with("https://") {
            client_target = client_target.replacen("https://", "", 1);
        }
        while client_target.starts_with('/') {
            client_target = client_target.replacen("/", "", 1);
        }

        // Create the HTTP(S) client
        let db = if http_url.starts_with("https://") {
            Surreal::new::<Https>(&client_target).await?
        } else {
            Surreal::new::<Http>(&client_target).await?
        };

        db.signin(surrealdb::opt::auth::Root {
            username: &username,
            password: &password,
        })
        .await?;

        Self::init_connection(SurrealConnection::Remote(db), namespace, database).await
    }

    /// Create a new SurrealRepoStore with remote database connection but optional
    /// authentication. If `username` and `password` are provided (Some), an explicit
    /// signin is attempted. If they are None, no signin is performed which is useful
    /// when the remote SurrealDB instance allows anonymous or token-based auth.
    pub async fn new_remote_optional_auth(
        url: String,
        username: Option<String>,
        password: Option<String>,
        namespace: String,
        database: String,
    ) -> Result<Self> {
        // Defensive sanitization: mirror hyperzoekt's normalization so callers can
        // provide http(s), ws(s), or scheme-less host:port values.
        let mut u = url.trim().to_string();
        if u.starts_with("http//") {
            u = u.replacen("http//", "http://", 1);
        } else if u.starts_with("https//") {
            u = u.replacen("https//", "https://", 1);
        } else if u.starts_with("ws//") {
            u = u.replacen("ws//", "ws://", 1);
        } else if u.starts_with("wss//") {
            u = u.replacen("wss//", "wss://", 1);
        }
        // Remove accidental duplicate schemes like "http://http://"
        if u.contains("http://http://") {
            u = u.replacen("http://http://", "http://", 1);
        }
        if u.contains("https://https://") {
            u = u.replacen("https://https://", "https://", 1);
        }

        // Normalize to an HTTP(S) URL targeting the /rpc path.
        let mut http_url = if u.starts_with("http://") || u.starts_with("https://") {
            if u.contains("/rpc") {
                u.clone()
            } else {
                let stripped = u.trim_end_matches('/');
                format!("{}/rpc", stripped)
            }
        } else if u.starts_with("ws://") || u.starts_with("wss://") {
            if u.starts_with("wss://") {
                let v = u.replacen("wss://", "https://", 1);
                if v.contains("/rpc") {
                    v
                } else {
                    format!("{}/rpc", v.trim_end_matches('/'))
                }
            } else {
                let v = u.replacen("ws://", "http://", 1);
                if v.contains("/rpc") {
                    v
                } else {
                    format!("{}/rpc", v.trim_end_matches('/'))
                }
            }
        } else {
            let host = u.trim_end_matches('/');
            format!("http://{}/rpc", host)
        };

        // Sanitize common malformed forms
        if http_url.contains("http//") {
            http_url = http_url.replace("http//", "http://");
        }
        if http_url.contains("https//") {
            http_url = http_url.replace("https//", "https://");
        }
        if http_url.contains("http://http://") {
            http_url = http_url.replacen("http://http://", "http://", 1);
        }
        if http_url.contains("https://https://") {
            http_url = http_url.replacen("https://https://", "https://", 1);
        }

        // Expose normalized base without /rpc for legacy callers/tests
        let base = if http_url.ends_with("/rpc") {
            http_url
                .trim_end_matches("/rpc")
                .trim_end_matches('/')
                .to_string()
        } else {
            http_url.trim_end_matches('/').to_string()
        };
        std::env::set_var("SURREALDB_URL", &base);
        std::env::set_var("SURREALDB_HTTP_BASE", &base);

        // Prepare client target by removing scheme so Surreal::new won't double-prefix
        let mut client_target = http_url.clone();
        if client_target.starts_with("http://") {
            client_target = client_target.replacen("http://", "", 1);
        } else if client_target.starts_with("https://") {
            client_target = client_target.replacen("https://", "", 1);
        }
        while client_target.starts_with('/') {
            client_target = client_target.replacen("/", "", 1);
        }

        // Create the HTTP(S) client
        let db = if http_url.starts_with("https://") {
            Surreal::new::<Https>(&client_target).await?
        } else {
            Surreal::new::<Http>(&client_target).await?
        };

        // Only attempt signin if both username and password provided
        if let (Some(u), Some(p)) = (username.as_ref(), password.as_ref()) {
            db.signin(surrealdb::opt::auth::Root {
                username: u,
                password: p,
            })
            .await?;
        }

        Self::init_connection(SurrealConnection::Remote(db), namespace, database).await
    }

    /// Legacy method for backwards compatibility - always uses in-memory
    pub async fn new(_url: Option<String>, namespace: String, database: String) -> Result<Self> {
        Self::new_in_memory(namespace, database).await
    }

    async fn init_connection(
        db: SurrealConnection,
        namespace: String,
        database: String,
    ) -> Result<Self, anyhow::Error> {
        match &db {
            SurrealConnection::Local(db_conn) => {
                db_conn.use_ns(&namespace).await?;
                db_conn.use_db(&database).await?;
            }
            SurrealConnection::Remote(db_conn) => {
                db_conn.use_ns(&namespace).await?;
                db_conn.use_db(&database).await?;
            }
        }

        // Initialize schema
        Self::init_schema(&db).await?;

        Ok(Self {
            db: Arc::new(RwLock::new(db)),
            namespace,
            database,
        })
    }

    async fn init_schema(db: &SurrealConnection) -> Result<(), surrealdb::Error> {
        match db {
            SurrealConnection::Local(db_conn) => {
                db_conn.query("DEFINE TABLE repo TYPE NORMAL;").await?;
                db_conn
                    .query("DEFINE FIELD name ON repo TYPE string;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD git_url ON repo TYPE string;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD branch ON repo TYPE option<string>;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD visibility ON repo TYPE string;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD owner ON repo TYPE option<string>;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD allowed_users ON repo TYPE array;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD last_commit_sha ON repo TYPE option<string>;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD last_indexed_at ON repo TYPE option<datetime>;")
                    .await?;
                // Add defaults so CREATE statements without explicit timestamps succeed
                db_conn
                    .query("DEFINE FIELD created_at ON repo TYPE datetime DEFAULT time::now();")
                    .await?;
                db_conn
                    .query("DEFINE FIELD updated_at ON repo TYPE datetime DEFAULT time::now();")
                    .await?;

                // Create indexes
                db_conn
                    .query("DEFINE INDEX idx_repo_name ON repo COLUMNS name UNIQUE;")
                    .await?;
                db_conn
                    .query("DEFINE INDEX idx_repo_git_url ON repo COLUMNS git_url UNIQUE;")
                    .await?;
            }
            SurrealConnection::Remote(db_conn) => {
                db_conn.query("DEFINE TABLE repo TYPE NORMAL;").await?;
                db_conn
                    .query("DEFINE FIELD name ON repo TYPE string;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD git_url ON repo TYPE string;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD branch ON repo TYPE option<string>;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD visibility ON repo TYPE string;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD owner ON repo TYPE option<string>;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD allowed_users ON repo TYPE array;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD last_commit_sha ON repo TYPE option<string>;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD last_indexed_at ON repo TYPE option<datetime>;")
                    .await?;
                // Add defaults so CREATE statements without explicit timestamps succeed
                db_conn
                    .query("DEFINE FIELD created_at ON repo TYPE datetime DEFAULT time::now();")
                    .await?;
                db_conn
                    .query("DEFINE FIELD updated_at ON repo TYPE datetime DEFAULT time::now();")
                    .await?;

                // Create indexes
                db_conn
                    .query("DEFINE INDEX idx_repo_name ON repo COLUMNS name UNIQUE;")
                    .await?;
                db_conn
                    .query("DEFINE INDEX idx_repo_git_url ON repo COLUMNS git_url UNIQUE;")
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn get_repo_metadata(&self, git_url: &str) -> anyhow::Result<Option<RepoMetadata>> {
        let db = self.db.read().await;
        let git_url_owned = git_url.to_string();
        let fields = "id, name, git_url, branch, visibility, owner, allowed_users, last_commit_sha, last_indexed_at, created_at, updated_at";
        let mut result = match &*db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(format!(
                        "SELECT {} FROM repo WHERE git_url = $git_url",
                        fields
                    ))
                    .bind(("git_url", git_url_owned))
                    .await?
            }
            SurrealConnection::Remote(db_conn) => {
                db_conn
                    .query(format!(
                        "SELECT {} FROM repo WHERE git_url = $git_url",
                        fields
                    ))
                    .bind(("git_url", git_url_owned))
                    .await?
            }
        };

        let repos: Vec<RepoMetadata> = result.take(0)?;
        Ok(repos.into_iter().next())
    }

    pub async fn upsert_repo_metadata(&self, metadata: RepoMetadata) -> anyhow::Result<()> {
        let db = self.db.read().await;
        let _now = chrono::Utc::now();

        let visibility_str = match &metadata.visibility {
            zoekt_rs::types::RepoVisibility::Public => "public",
            zoekt_rs::types::RepoVisibility::Private => "private",
            zoekt_rs::types::RepoVisibility::Restricted(_) => "restricted",
        };

        // Clone all the values we need to bind
        let name = metadata.name.clone();
        let git_url = metadata.git_url.clone();
        let branch = metadata.branch.clone();
        let visibility = visibility_str.to_string();
        let owner = metadata.owner.clone();
        let allowed_users = metadata.allowed_users.clone();
        let last_commit_sha = metadata.last_commit_sha.clone();
        let last_indexed_at = metadata.last_indexed_at;

        let query_result = match &*db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(
                        r#"
                        UPSERT repo SET
                            name = $name,
                            git_url = $git_url,
                            branch = $branch,
                            visibility = $visibility,
                            owner = $owner,
                            allowed_users = $allowed_users,
                            last_commit_sha = $last_commit_sha,
                            updated_at = time::now()
                        WHERE git_url = $git_url
                    "#,
                    )
                    .bind(("name", name))
                    .bind(("git_url", git_url.clone()))
                    .bind(("branch", branch))
                    .bind(("visibility", visibility))
                    .bind(("owner", owner))
                    .bind(("allowed_users", allowed_users))
                    .bind(("last_commit_sha", last_commit_sha))
                    .await
            }
            SurrealConnection::Remote(db_conn) => {
                db_conn
                    .query(
                        r#"
                        UPSERT repo SET
                            name = $name,
                            git_url = $git_url,
                            branch = $branch,
                            visibility = $visibility,
                            owner = $owner,
                            allowed_users = $allowed_users,
                            last_commit_sha = $last_commit_sha,
                            updated_at = time::now()
                        WHERE git_url = $git_url
                    "#,
                    )
                    .bind(("name", name))
                    .bind(("git_url", git_url.clone()))
                    .bind(("branch", branch))
                    .bind(("visibility", visibility))
                    .bind(("owner", owner))
                    .bind(("allowed_users", allowed_users))
                    .bind(("last_commit_sha", last_commit_sha))
                    .await
            }
        };

        query_result?.check()?;

        // Apply typed datetime for last_indexed_at if provided, after UPSERT
        if let Some(dt) = last_indexed_at {
            match &*db {
                SurrealConnection::Local(db_conn) => {
                    let _ = db_conn
                        .query(
                            r#"
                            UPDATE repo SET last_indexed_at = $last_indexed_at
                            WHERE git_url = $git_url
                        "#,
                        )
                        .bind(("git_url", git_url.clone()))
                        .bind(("last_indexed_at", dt))
                        .await;
                }
                SurrealConnection::Remote(db_conn) => {
                    let _ = db_conn
                        .query(
                            r#"
                            UPDATE repo SET last_indexed_at = $last_indexed_at
                            WHERE git_url = $git_url
                        "#,
                        )
                        .bind(("git_url", git_url.clone()))
                        .bind(("last_indexed_at", dt))
                        .await;
                }
            }
        }
        Ok(())
    }

    pub async fn get_all_repos(&self) -> Result<Vec<RepoMetadata>> {
        let db = self.db.read().await;
        let fields = "id, name, git_url, branch, visibility, owner, allowed_users, last_commit_sha, last_indexed_at, created_at, updated_at";
        let mut result = match &*db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query(format!("SELECT {} FROM repo", fields))
                    .await?
            }
            SurrealConnection::Remote(db_conn) => {
                db_conn
                    .query(format!("SELECT {} FROM repo", fields))
                    .await?
            }
        };
        let repos: Vec<RepoMetadata> = result.take(0)?;
        Ok(repos)
    }

    pub async fn can_user_access_repo(&self, git_url: &str, user_id: Option<&str>) -> Result<bool> {
        let metadata = match self.get_repo_metadata(git_url).await? {
            Some(meta) => meta,
            None => return Ok(true), // If no metadata exists, allow access (public by default)
        };

        match metadata.visibility {
            zoekt_rs::types::RepoVisibility::Public => Ok(true),
            zoekt_rs::types::RepoVisibility::Private => {
                // For private repos, user must be authenticated
                Ok(user_id.is_some())
            }
            zoekt_rs::types::RepoVisibility::Restricted(ref allowed_users) => {
                // For restricted repos, user must be in the allowed list
                match user_id {
                    Some(uid) => Ok(allowed_users.contains(&uid.to_string())),
                    None => Ok(false),
                }
            }
        }
    }

    pub async fn should_skip_indexing(
        &self,
        _git_url: &str,
        _current_sha: &str,
    ) -> anyhow::Result<bool> {
        // Always check permissions, never skip based on SHA alone
        // The user specifically requested this behavior
        Ok(false)
    }
}
