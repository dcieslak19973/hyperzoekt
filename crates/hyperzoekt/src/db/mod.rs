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

// Consolidated database layer for HyperZoekt
// Combines connection logic, query operations, data models, and writing operations

pub mod branch;
pub mod config;
pub mod connection;
pub mod content;
pub mod helpers;
pub mod models;
pub mod queries;
pub mod refs;
pub mod writer;

pub use branch::{resolve_commit_branch, resolve_default_branch, DefaultBranchInfo};
pub use config::{DbWriterConfig, SpawnResult};
pub use connection::{connect, SurrealConnection, SHARED_MEM};
// content module exists for internal use but functions are not re-exported since
// embeddings and snapshot storage now happen during indexing via entity_snapshot.
pub use helpers::normalize_git_url;
pub use helpers::normalize_sql_value_id;
pub use helpers::response_to_json;
pub use helpers::{init_call_edge_capture, CALL_EDGE_CAPTURE};
pub use models::*;
pub use queries::*;
pub use refs::{create_branch, create_commit, create_snapshot_meta, create_tag, move_branch};
pub use writer::{sanitize_json_strings, spawn_db_writer};

/// A database handle that provides access to all database operations.
/// Wraps DatabaseQueries and provides a connection method for testing.
#[derive(Clone)]
pub struct Database {
    queries: DatabaseQueries,
}

impl Database {
    /// Create a new database handle with the given connection parameters.
    pub async fn new(
        url: Option<&str>,
        ns: &str,
        db_name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        use connection::connect as hz_connect;
        let conn = hz_connect(&url.map(|s| s.to_string()), &None, &None, ns, db_name).await?;
        let queries = DatabaseQueries::new(std::sync::Arc::new(conn));
        Ok(Self { queries })
    }

    /// Get a reference to the underlying connection for testing purposes.
    pub fn connection(&self) -> &std::sync::Arc<connection::SurrealConnection> {
        &self.queries.db
    }
}

impl std::ops::Deref for Database {
    type Target = DatabaseQueries;

    fn deref(&self) -> &Self::Target {
        &self.queries
    }
}
