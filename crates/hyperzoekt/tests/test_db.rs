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
use hyperzoekt::db::connection::{connect, SurrealConnection};
use hyperzoekt::test_utils::normalize_surreal_host;
use surrealdb::Response;

pub struct TestDb {
    inner: SurrealConnection,
}

impl TestDb {
    pub async fn new() -> Self {
        // Prefer explicit test URL override, then local default, then fallback to in-memory.
        let user = std::env::var("SURREALDB_USERNAME").ok();
        let pass = std::env::var("SURREALDB_PASSWORD").ok();

        if let Ok(u) = std::env::var("SURREAL_TEST_HTTP_URL") {
            let (schemeful, _host_no_scheme) = normalize_surreal_host(&u);
            if let Ok(conn) = connect(&Some(schemeful), &user, &pass, "testns", "testdb").await {
                return TestDb { inner: conn };
            }
        }

        // Optionally try common local dev container URL if explicitly allowed.
        // In many developer and CI environments a local SurrealDB at 127.0.0.1:8000
        // may exist but have restrictive permissions. Only attempt this when the
        // ALLOW_LOCAL_SURREAL env var is set to "1" to avoid flaky test failures.
        if std::env::var("ALLOW_LOCAL_SURREAL").ok().as_deref() == Some("1") {
            if let Ok(conn) = connect(
                &Some("http://127.0.0.1:8000".to_string()),
                &user,
                &pass,
                "testns",
                "testdb",
            )
            .await
            {
                return TestDb { inner: conn };
            }
        }

        // Last-resort: force embedded Mem for test fallback even when SURREALDB_URL
        // may be set in the environment. Temporarily disable env-based fallback so
        // connect() creates an in-memory instance instead of attempting a remote
        // connection that may be unreachable in CI.
        let prev = std::env::var("HZ_DISABLE_SURREAL_ENV").ok();
        std::env::set_var("HZ_DISABLE_SURREAL_ENV", "1");
        let conn = match connect(&None, &None, &None, "testns", "testdb").await {
            Ok(c) => c,
            Err(e) => {
                // Restore env before panicking
                if let Some(v) = prev {
                    std::env::set_var("HZ_DISABLE_SURREAL_ENV", v);
                } else {
                    std::env::remove_var("HZ_DISABLE_SURREAL_ENV");
                }
                panic!("create fallback test db: {}", e);
            }
        };
        // Restore previous HZ_DISABLE_SURREAL_ENV value
        if let Some(v) = prev {
            std::env::set_var("HZ_DISABLE_SURREAL_ENV", v);
        } else {
            std::env::remove_var("HZ_DISABLE_SURREAL_ENV");
        }
        TestDb { inner: conn }
    }

    pub async fn use_ns_db(&self, ns: &str, db: &str) {
        let _ = self.inner.use_ns(ns).await;
        let _ = self.inner.use_db(db).await;
    }

    pub async fn query(&self, q: &str) -> surrealdb::Result<Response> {
        self.inner.query(q).await
    }

    pub async fn remove_all(&self, table: &str) {
        let stmt = format!("REMOVE {}", table);
        let _ = self.query(&stmt).await;
    }
}

pub async fn test_db() -> TestDb {
    TestDb::new().await
}
