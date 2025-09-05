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

use sha2::{Digest, Sha256};

/// Generate a stable ID for an entity based on its identifying characteristics.
/// This ensures consistent entity identification across different indexing runs.
///
/// The stable ID is generated using SHA256 hash of the following format:
/// `{project}|{repo}|{branch}|{commit}|{file_path}|{entity_name}|{entity_signature}`
pub fn generate_stable_id(
    project: &str,
    repo: &str,
    branch: &str,
    commit: &str,
    file_path: &str,
    entity_name: &str,
    entity_signature: &str,
) -> String {
    let key = format!(
        "{}|{}|{}|{}|{}|{}|{}",
        project, repo, branch, commit, file_path, entity_name, entity_signature
    );
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_stable_id_consistency() {
        let id1 = generate_stable_id(
            "proj",
            "repo",
            "main",
            "abc123",
            "/path/file.rs",
            "func",
            "fn func()",
        );
        let id2 = generate_stable_id(
            "proj",
            "repo",
            "main",
            "abc123",
            "/path/file.rs",
            "func",
            "fn func()",
        );
        assert_eq!(
            id1, id2,
            "Stable IDs should be consistent for identical inputs"
        );
    }

    #[test]
    fn test_generate_stable_id_uniqueness() {
        let id1 = generate_stable_id(
            "proj",
            "repo",
            "main",
            "abc123",
            "/path/file.rs",
            "func",
            "fn func()",
        );
        let id2 = generate_stable_id(
            "proj",
            "repo",
            "main",
            "abc123",
            "/path/file.rs",
            "func",
            "fn func2()",
        );
        assert_ne!(
            id1, id2,
            "Stable IDs should be unique for different signatures"
        );
    }

    #[test]
    fn test_url_parsing_fix() {
        // Test the fixed URL parsing logic
        let test_cases = vec![
            ("https://example.com:8080", "example.com:8080"),
            ("http://example.com", "example.com"),
            ("https://localhost:8000/db", "localhost:8000/db"),
            ("http://surrealdb:8000", "surrealdb:8000"),
        ];

        for (input, expected) in test_cases {
            let result = if let Some(stripped) = input.strip_prefix("https://") {
                stripped
            } else if let Some(stripped) = input.strip_prefix("http://") {
                stripped
            } else {
                input
            };
            assert_eq!(result, expected, "Failed to parse URL: {}", input);
        }
    }
}
