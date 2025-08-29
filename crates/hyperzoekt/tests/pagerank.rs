use hyperzoekt::repo_index::indexer::types::{EntityKind, RankWeights};
use hyperzoekt::repo_index::pagerank::compute_pagerank;
use hyperzoekt::repo_index::types::{FileRecord, RepoIndexService, StoredEntity};

#[test]
fn pagerank_simple_graph_orders_nodes() {
    // Create a tiny graph of 3 entities where:
    // - entity 0 (A) points to 1 and 2
    // - entity 1 (B) points to 2
    // - entity 2 (C) points to nobody (dangling)
    // Expectation: C receives the most incoming weight, then B, then A.

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
    let unresolved_imports = vec![Vec::new(); files.len()];

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

    // Ranks should be positive
    assert!(r0 > 0.0 && r1 > 0.0 && r2 > 0.0);

    // C should have the largest rank, then B, then A
    assert!(r2 > r1, "expected C > B (got {} <= {})", r2, r1);
    assert!(r1 > r0, "expected B > A (got {} <= {})", r1, r0);

    // Ranks should sum to approximately 1.0 (allow some tolerance)
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

    // should not panic
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
