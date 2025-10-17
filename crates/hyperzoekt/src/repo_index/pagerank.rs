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

use crate::repo_index::types::RepoIndexService;
use rayon::prelude::*;
#[cfg(feature = "otel")]
use tracing::{debug_span, info};

pub fn compute_pagerank(svc: &mut RepoIndexService) {
    let n = svc.entities.len();
    if n == 0 {
        return;
    }

    // Extract weights & params
    let crate::repo_index::indexer::types::RankWeights {
        call: w_call,
        import: w_import,
        containment: w_contain,
        damping,
        iterations,
        epsilon,
    } = svc.rank_weights;

    // We'll build weighted adjacency and out sums below in one pass

    // If damping is zero, the rank is uniform (teleport-only). Short-circuit
    // to match previous behavior/tests.
    if damping == 0.0 {
        let v = 1.0f32 / n as f32;
        for ent in svc.entities.iter_mut() {
            ent.rank = v;
        }
        return;
    }
    // Build weighted edges and out sums efficiently
    let mut edges_out: Vec<Vec<(usize, f32)>> = vec![Vec::new(); n];
    let mut out_sum: Vec<f32> = vec![0.0; n];
    for src in 0..n {
        let mut sum = 0.0f32;
        if let Some(calls) = svc.call_edges.get(src) {
            for &t in calls {
                edges_out[src].push((t as usize, w_call));
                sum += w_call;
            }
        }
        if let Some(imps) = svc.import_edges.get(src) {
            for &t in imps {
                edges_out[src].push((t as usize, w_import));
                sum += w_import;
            }
        }
        if let Some(children) = svc.containment_children.get(src) {
            for &t in children {
                edges_out[src].push((t as usize, w_contain));
                sum += w_contain;
            }
        }
        if let Some(Some(p)) = svc.containment_parent.get(src) {
            edges_out[src].push((*p as usize, w_contain));
            sum += w_contain;
        }
        out_sum[src] = sum;
    }

    // Initialize ranks uniformly
    let mut ranks: Vec<f32> = vec![1.0f32 / n as f32; n];
    let base_tele = (1.0f32 - damping) / n as f32;

    #[cfg(feature = "otel")]
    let _span = debug_span!("pagerank_iter", iterations = iterations).entered();
    for _iter in 0..iterations {
        // Compute dangling mass
        let dangling_mass: f32 = ranks
            .iter()
            .enumerate()
            .filter(|(i, _)| out_sum[*i] == 0.0)
            .map(|(_, &r)| r)
            .sum();

        let uniform_dangling = damping * dangling_mass / n as f32;

        // Parallel accumulation of contributions using fold/reduce
        let contribs: Vec<f32> = (0..n)
            .into_par_iter()
            .fold(
                || vec![0.0f32; n],
                |mut acc, u| {
                    let out = out_sum[u];
                    if out > 0.0 {
                        let factor = damping * (ranks[u] / out);
                        for &(v, w) in &edges_out[u] {
                            acc[v] += factor * w;
                        }
                    }
                    acc
                },
            )
            .reduce(
                || vec![0.0f32; n],
                |mut a, b| {
                    for i in 0..n {
                        a[i] += b[i];
                    }
                    a
                },
            );

        // Compose new ranks: base teleport + contributions + dangling redistribution
        let mut l1_change = 0.0f32;
        for i in 0..n {
            let new_val = base_tele + uniform_dangling + contribs[i];
            l1_change += (new_val - ranks[i]).abs();
            ranks[i] = new_val;
        }

        #[cfg(feature = "otel")]
        info!(iter = _iter, l1_change = l1_change, "pagerank iteration");
        if epsilon > 0.0 && l1_change <= epsilon {
            break;
        }
    }

    // Normalize and assign
    let sum: f32 = ranks.iter().copied().sum();
    if sum > 0.0 {
        let inv = 1.0f32 / sum;
        for (i, ent) in svc.entities.iter_mut().enumerate() {
            ent.rank = ranks[i] * inv;
        }
    } else {
        // Fallback: uniform if sum is zero
        let v = 1.0f32 / n as f32;
        for ent in svc.entities.iter_mut() {
            ent.rank = v;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo_index::indexer::types::{EntityKind, RankWeights};
    use crate::repo_index::types::{FileRecord, RepoIndexService, StoredEntity};

    #[test]
    fn pagerank_simple_graph_orders_nodes() {
        let files = vec![FileRecord {
            id: 0,
            path: "<inmem>".to_string(),
            language: "rust".to_string(),
            entities: vec![0, 1, 2],
        }];

        let entities = vec![
            StoredEntity {
                id: 0,
                file_id: 0,
                kind: EntityKind::Function,
                name: "A".to_string(),
                parent: None,
                signature: String::new(),
                start_line: 0,
                end_line: 0,
                // calls removed
                doc: None,
                rank: 0.0,
                calls_raw: vec![],
                methods: Vec::new(),
            },
            StoredEntity {
                id: 1,
                file_id: 0,
                kind: EntityKind::Function,
                name: "B".to_string(),
                parent: None,
                signature: String::new(),
                start_line: 0,
                end_line: 0,
                // calls removed
                doc: None,
                rank: 0.0,
                calls_raw: vec![],
                methods: Vec::new(),
            },
            StoredEntity {
                id: 2,
                file_id: 0,
                kind: EntityKind::Function,
                name: "C".to_string(),
                parent: None,
                signature: String::new(),
                start_line: 0,
                end_line: 0,
                // calls removed
                doc: None,
                rank: 0.0,
                calls_raw: vec![],
                methods: Vec::new(),
            },
        ];

        let n = entities.len();
        let call_edges = vec![vec![1u32, 2u32], vec![2u32], vec![]];
        let reverse_call_edges = vec![Vec::new(); n];
        let import_edges = vec![Vec::new(); n];
        let import_lines = vec![Vec::new(); n];
        let containment_children = vec![Vec::new(); n];
        let containment_parent = vec![None; n];
        let name_index = std::collections::HashMap::new();
        let file_entities = Vec::new();
        let unresolved_imports = vec![Vec::new(); file_entities.len()];

        let mut svc = RepoIndexService {
            files,
            entities,
            name_index,
            containment_children,
            containment_parent,
            call_edges,
            reverse_call_edges,
            import_edges,
            import_lines,
            file_entities,
            unresolved_imports,
            rank_weights: RankWeights {
                call: 1.0,
                import: 0.5,
                containment: 0.25,
                damping: 0.85,
                iterations: 30,
                epsilon: 1e-6,
            },
        };

        compute_pagerank(&mut svc);

        let r0 = svc.entities[0].rank;
        let r1 = svc.entities[1].rank;
        let r2 = svc.entities[2].rank;

        assert!(r0 > 0.0 && r1 > 0.0 && r2 > 0.0);
        assert!(r2 > r1, "expected C > B (got {} <= {})", r2, r1);
        assert!(r1 > r0, "expected B > A (got {} <= {})", r1, r0);
        let sum = r0 + r1 + r2;
        assert!((sum - 1.0).abs() < 1e-3, "ranks sum to {}", sum);
    }

    #[test]
    fn pagerank_empty_no_panic() {
        let files: Vec<FileRecord> = Vec::new();
        let entities: Vec<StoredEntity> = Vec::new();
        let mut svc = RepoIndexService {
            files,
            entities,
            name_index: std::collections::HashMap::new(),
            containment_children: Vec::new(),
            containment_parent: Vec::new(),
            call_edges: Vec::new(),
            reverse_call_edges: Vec::new(),
            import_edges: Vec::new(),
            import_lines: Vec::new(),
            file_entities: Vec::new(),
            unresolved_imports: Vec::new(),
            rank_weights: RankWeights::default(),
        };

        compute_pagerank(&mut svc);
        assert!(svc.entities.is_empty());
    }

    #[test]
    fn pagerank_single_node_is_one() {
        let files = vec![FileRecord {
            id: 0,
            path: "f".into(),
            language: "rust".into(),
            entities: vec![0],
        }];
        let entities = vec![StoredEntity {
            id: 0,
            file_id: 0,
            kind: EntityKind::Function,
            name: "only".into(),
            parent: None,
            signature: "".into(),
            start_line: 0,
            end_line: 0,
            // calls removed
            doc: None,
            rank: 0.0,
            calls_raw: vec![],
            methods: Vec::new(),
        }];
        let mut svc = RepoIndexService {
            files,
            entities,
            name_index: std::collections::HashMap::new(),
            containment_children: vec![Vec::new()],
            containment_parent: vec![None],
            call_edges: vec![Vec::new()],
            reverse_call_edges: vec![Vec::new()],
            import_edges: vec![Vec::new()],
            import_lines: vec![Vec::new()],
            file_entities: vec![0],
            unresolved_imports: vec![Vec::new()],
            rank_weights: RankWeights {
                call: 1.0,
                import: 0.5,
                containment: 0.25,
                damping: 0.85,
                iterations: 10,
                epsilon: 1e-6,
            },
        };

        compute_pagerank(&mut svc);
        let r = svc.entities[0].rank;
        assert!((r - 1.0).abs() < 1e-6, "single node rank {}", r);
    }

    #[test]
    fn pagerank_damping_zero_yields_uniform() {
        let files = vec![FileRecord {
            id: 0,
            path: "f".into(),
            language: "rust".into(),
            entities: vec![0, 1, 2],
        }];
        let entities = vec![
            StoredEntity {
                id: 0,
                file_id: 0,
                kind: EntityKind::Function,
                name: "a".into(),
                parent: None,
                signature: "".into(),
                start_line: 0,
                end_line: 0,
                // calls removed
                doc: None,
                rank: 0.0,
                calls_raw: vec![],
                methods: Vec::new(),
            },
            StoredEntity {
                id: 1,
                file_id: 0,
                kind: EntityKind::Function,
                name: "b".into(),
                parent: None,
                signature: "".into(),
                start_line: 0,
                end_line: 0,
                // calls removed
                doc: None,
                rank: 0.0,
                calls_raw: vec![],
                methods: Vec::new(),
            },
            StoredEntity {
                id: 2,
                file_id: 0,
                kind: EntityKind::Function,
                name: "c".into(),
                parent: None,
                signature: "".into(),
                start_line: 0,
                end_line: 0,
                // calls removed
                doc: None,
                rank: 0.0,
                calls_raw: vec![],
                methods: Vec::new(),
            },
        ];
        let n = entities.len();
        let mut svc = RepoIndexService {
            files,
            entities,
            name_index: std::collections::HashMap::new(),
            containment_children: vec![Vec::new(); n],
            containment_parent: vec![None; n],
            call_edges: vec![vec![1], vec![2], vec![]],
            reverse_call_edges: vec![vec![], vec![0], vec![1]],
            import_edges: vec![Vec::new(); n],
            import_lines: vec![Vec::new(); n],
            file_entities: vec![0],
            unresolved_imports: vec![Vec::new()],
            rank_weights: RankWeights {
                call: 1.0,
                import: 0.5,
                containment: 0.25,
                damping: 0.0,
                iterations: 5,
                epsilon: 1e-6,
            },
        };

        compute_pagerank(&mut svc);
        let sum: f32 = svc.entities.iter().map(|e| e.rank).sum();
        let expected = 1.0f32 / n as f32;
        for e in &svc.entities {
            assert!(
                (e.rank - expected).abs() < 1e-5,
                "rank not uniform: {} vs {}",
                e.rank,
                expected
            );
        }
        assert!((sum - 1.0).abs() < 1e-6, "sum {}", sum);
    }

    #[test]
    fn pagerank_all_dangling_equalizes() {
        let files = vec![FileRecord {
            id: 0,
            path: "f".into(),
            language: "rust".into(),
            entities: vec![0, 1, 2],
        }];
        let entities = vec![
            StoredEntity {
                id: 0,
                file_id: 0,
                kind: EntityKind::Function,
                name: "a".into(),
                parent: None,
                signature: "".into(),
                start_line: 0,
                end_line: 0,
                // calls removed
                doc: None,
                rank: 0.0,
                calls_raw: vec![],
                methods: Vec::new(),
            },
            StoredEntity {
                id: 1,
                file_id: 0,
                kind: EntityKind::Function,
                name: "b".into(),
                parent: None,
                signature: "".into(),
                start_line: 0,
                end_line: 0,
                // calls removed
                doc: None,
                rank: 0.0,
                calls_raw: vec![],
                methods: Vec::new(),
            },
            StoredEntity {
                id: 2,
                file_id: 0,
                kind: EntityKind::Function,
                name: "c".into(),
                parent: None,
                signature: "".into(),
                start_line: 0,
                end_line: 0,
                // calls removed
                doc: None,
                rank: 0.0,
                calls_raw: vec![],
                methods: Vec::new(),
            },
        ];
        let n = entities.len();
        let mut svc = RepoIndexService {
            files,
            entities,
            name_index: std::collections::HashMap::new(),
            containment_children: vec![Vec::new(); n],
            containment_parent: vec![None; n],
            call_edges: vec![Vec::new(); n],
            reverse_call_edges: vec![Vec::new(); n],
            import_edges: vec![Vec::new(); n],
            import_lines: vec![Vec::new(); n],
            file_entities: vec![0],
            unresolved_imports: vec![Vec::new()],
            rank_weights: RankWeights::default(),
        };

        compute_pagerank(&mut svc);
        let sum: f32 = svc.entities.iter().map(|e| e.rank).sum();
        let expected = 1.0f32 / n as f32;
        for e in &svc.entities {
            assert!(
                (e.rank - expected).abs() < 1e-3,
                "dangling not equalized: {} vs {}",
                e.rank,
                expected
            );
        }
        assert!((sum - 1.0).abs() < 1e-3, "sum {}", sum);
    }
}

// Duplicate tests removed; kept the original tests module earlier in the file.
