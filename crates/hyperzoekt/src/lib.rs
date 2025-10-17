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

// Minimal repo-index crate skeleton for Tree-sitter integration

#![allow(dead_code)]

pub mod repo_index;
pub use repo_index::RepoIndexService;

// Shared modules for binaries
pub mod db;
pub mod event_consumer;
pub mod graph_api;
pub mod index;
pub mod utils; // public graph traversal API
               // Test utilities exposed for integration tests.
pub mod hirag;
pub mod llm;
pub mod similarity;
pub mod test_utils;

// Optional OpenTelemetry/tracing initializer. The functions inside are
// already feature-gated to no-op when the `otel` feature is disabled.
pub mod otel;
