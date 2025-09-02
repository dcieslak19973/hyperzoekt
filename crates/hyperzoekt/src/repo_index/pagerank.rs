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
use petgraph::algo::page_rank;
use petgraph::graph::Graph;
use petgraph::prelude::NodeIndex;
use petgraph::Directed;

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
    } = svc.rank_weights;

    // Build a directed graph with `n` nodes
    let mut g: Graph<(), f64, Directed> = Graph::with_capacity(n, 0);
    let mut idxs: Vec<NodeIndex> = Vec::with_capacity(n);
    for _ in 0..n {
        idxs.push(g.add_node(()));
    }

    // Add weighted edges according to the configured weights
    for src in 0..n {
        let mut edges: Vec<(usize, f32)> = Vec::new();
        if let Some(calls) = svc.call_edges.get(src) {
            for &t in calls {
                edges.push((t as usize, w_call));
            }
        }
        if let Some(imps) = svc.import_edges.get(src) {
            for &t in imps {
                edges.push((t as usize, w_import));
            }
        }
        if let Some(children) = svc.containment_children.get(src) {
            for &t in children {
                edges.push((t as usize, w_contain));
            }
        }
        if let Some(Some(p)) = svc.containment_parent.get(src) {
            edges.push((*p as usize, w_contain));
        }

        // If there are edges, add them directly to the graph with the weight.
        // petgraph's page_rank will use edge weights via the `edge_weight` function.
        for (t_idx, w) in edges {
            g.add_edge(idxs[src], idxs[t_idx], w as f64);
        }
    }

    // If damping is zero, the rank is uniform (teleport-only). Short-circuit
    // to match previous behavior/tests.
    if damping == 0.0 {
        let v = 1.0f32 / n as f32;
        for ent in svc.entities.iter_mut() {
            ent.rank = v;
        }
        return;
    }

    // petgraph::algo::page_rank expects an adjacency representation; it supports
    // a `node_weight` and `edge_weight` selectors. We can call the basic API.
    // The function returns a vector of ranks keyed by node order.
    let ranks = page_rank(&g, damping as f64, iterations);

    // page_rank returns a Vec<f64> indexed by the node order we created
    let mut sum = 0.0f32;
    for (i, v) in ranks.iter().enumerate().take(n) {
        let r = *v as f32;
        if let Some(ent) = svc.entities.get_mut(i) {
            ent.rank = r;
        }
        sum += r;
    }

    // Normalize ranks to sum to 1.0 (page_rank may already normalize but ensure it)
    if sum > 0.0 {
        for ent in svc.entities.iter_mut() {
            ent.rank /= sum;
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
                calls: Vec::new(),
                doc: None,
                rank: 0.0,
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
                calls: Vec::new(),
                doc: None,
                rank: 0.0,
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
                calls: Vec::new(),
                doc: None,
                rank: 0.0,
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
            calls: vec![],
            doc: None,
            rank: 0.0,
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
                calls: vec!["b".into()],
                doc: None,
                rank: 0.0,
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
                calls: vec!["c".into()],
                doc: None,
                rank: 0.0,
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
                calls: vec![],
                doc: None,
                rank: 0.0,
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
                calls: vec![],
                doc: None,
                rank: 0.0,
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
                calls: vec![],
                doc: None,
                rank: 0.0,
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
                calls: vec![],
                doc: None,
                rank: 0.0,
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
