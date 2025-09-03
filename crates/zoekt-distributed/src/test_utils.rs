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

//! Test utilities and helpers for zoekt-distributed.
//!
//! This module contains shared test infrastructure including environment
//! variable management, fake indexers, and test logging setup.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Once;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tracing_subscriber::EnvFilter;

use crate::{InMemoryIndex as ZoektInMemoryIndex, Indexer};

/// Test helper to manage environment variables and ensure proper cleanup
pub struct EnvGuard {
    original_values: HashMap<String, Option<String>>,
}

impl EnvGuard {
    pub fn new() -> Self {
        Self {
            original_values: HashMap::new(),
        }
    }

    pub fn save_and_clear(&mut self, vars: &[&str]) {
        for &var in vars {
            let original = std::env::var(var).ok();
            self.original_values.insert(var.to_string(), original);
            std::env::remove_var(var);
        }
    }

    pub fn set(&self, var: &str, value: &str) {
        std::env::set_var(var, value);
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // Restore original environment variable values
        for (var, original_value) in &self.original_values {
            match original_value {
                Some(value) => std::env::set_var(var, value),
                None => std::env::remove_var(var),
            }
        }
    }
}

impl Default for EnvGuard {
    fn default() -> Self {
        Self::new()
    }
}

/// Fake indexer implementation for testing
pub struct FakeIndexer {
    pub count: Arc<AtomicUsize>,
}

impl FakeIndexer {
    pub fn new() -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_count(c: Arc<AtomicUsize>) -> Self {
        Self { count: c }
    }
}

impl Indexer for FakeIndexer {
    fn index_repo(&self, _repo_path: PathBuf) -> anyhow::Result<ZoektInMemoryIndex> {
        let _n = self.count.fetch_add(1, Ordering::SeqCst);
        let idx = zoekt_rs::test_helpers::make_index_with_trigrams(vec![], "fake", vec![], None);
        Ok(idx)
    }
}

impl Default for FakeIndexer {
    fn default() -> Self {
        Self::new()
    }
}

/// Initialize tracing only once for tests so logs are visible when running
/// `cargo test -- --nocapture`. Respects RUST_LOG when set; defaults to debug
/// to keep output readable.
pub fn init_test_logging() {
    static TRACING_INIT: Once = Once::new();
    TRACING_INIT.call_once(|| {
        // Prefer env when set; fall back to a quieter default to keep output readable.
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    });
}

/// Sleep indexer that simulates work by sleeping briefly
pub struct SleepIndexer;

impl Indexer for SleepIndexer {
    fn index_repo(&self, _repo_path: PathBuf) -> anyhow::Result<ZoektInMemoryIndex> {
        std::thread::sleep(std::time::Duration::from_millis(30));
        let idx = zoekt_rs::test_helpers::make_index_with_trigrams(vec![], "fake", vec![], None);
        Ok(idx)
    }
}
