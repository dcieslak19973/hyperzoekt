use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use surrealdb::engine::local::Mem;
use surrealdb::Surreal;
use tokio::sync::RwLock;

enum SurrealConnection {
    Local(Surreal<surrealdb::engine::local::Db>),
    Remote(Surreal<surrealdb::engine::remote::ws::Client>),
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
        use surrealdb::engine::remote::ws::Ws;

        let db = Surreal::new::<Ws>(url).await?;
        db.signin(surrealdb::opt::auth::Root {
            username: &username,
            password: &password,
        })
        .await?;

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
                db_conn
                    .query("DEFINE FIELD created_at ON repo TYPE datetime;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD updated_at ON repo TYPE datetime;")
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
                db_conn
                    .query("DEFINE FIELD created_at ON repo TYPE datetime;")
                    .await?;
                db_conn
                    .query("DEFINE FIELD updated_at ON repo TYPE datetime;")
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
        let mut result = match &*db {
            SurrealConnection::Local(db_conn) => {
                db_conn
                    .query("SELECT * FROM repo WHERE git_url = $git_url")
                    .bind(("git_url", git_url_owned))
                    .await?
            }
            SurrealConnection::Remote(db_conn) => {
                db_conn
                    .query("SELECT * FROM repo WHERE git_url = $git_url")
                    .bind(("git_url", git_url_owned))
                    .await?
            }
        };

        let repos: Vec<RepoMetadata> = result.take(0)?;
        Ok(repos.into_iter().next())
    }

    pub async fn upsert_repo_metadata(&self, metadata: RepoMetadata) -> anyhow::Result<()> {
        let db = self.db.read().await;
        let now = chrono::Utc::now();

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
        let updated_at = now;

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
                            last_indexed_at = $last_indexed_at,
                            updated_at = $updated_at
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
                    .bind(("last_indexed_at", last_indexed_at))
                    .bind(("updated_at", updated_at))
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
                            last_indexed_at = $last_indexed_at,
                            updated_at = $updated_at
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
                    .bind(("last_indexed_at", last_indexed_at))
                    .bind(("updated_at", updated_at))
                    .await
            }
        };

        query_result?.check()?;
        Ok(())
    }

    pub async fn get_all_repos(&self) -> Result<Vec<RepoMetadata>> {
        let db = self.db.read().await;
        let mut result = match &*db {
            SurrealConnection::Local(db_conn) => db_conn.query("SELECT * FROM repo").await?,
            SurrealConnection::Remote(db_conn) => db_conn.query("SELECT * FROM repo").await?,
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
