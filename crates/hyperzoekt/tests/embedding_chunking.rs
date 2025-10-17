use hyperzoekt::db::writer::{append_to_embed_dlq, chunk_payloads_for_embeddings};
use hyperzoekt::repo_index::indexer::payload::EntityPayload;
use tempfile::tempdir;

#[test]
fn test_chunk_payloads_for_embeddings_basic() {
    // Build a long payload that will produce more tokens than token_limit
    let long_text = "a ".repeat(5000);
    let p = EntityPayload {
        id: "e1".to_string(),
        language: "rust".to_string(),
        kind: "function".to_string(),
        name: "long_func".to_string(),
        repo_name: "example/repo".to_string(),
        signature: "sig".to_string(),
        stable_id: "stable1".to_string(),
        source_content: Some(long_text.clone()),
        source_url: None,
        source_display: None,
        calls: Vec::new(),
        parent: None,
        start_line: None,
        end_line: None,
        doc: None,
        rank: None,
        imports: Vec::new(),
        unresolved_imports: Vec::new(),
        methods: Vec::new(),
        file: None,
    };
    let token_limit = 100usize;
    let chunk_overlap = 10usize;
    // Use tiktoken-rs tokenizer; if it fails, we still ensure non-panicky behavior
    let tk = tiktoken_rs::cl100k_base().ok();
    let (chunks, mapping) =
        chunk_payloads_for_embeddings(&[p.clone()], token_limit, chunk_overlap, tk.clone());
    // If tokenizer available, chunks should be > 1
    if tk.is_some() {
        assert!(
            chunks.len() > 1,
            "expected multiple chunks when tokenizer present"
        );
        assert_eq!(chunks.len(), mapping.len());
        // Each chunk should have source_content set
        for c in chunks.iter() {
            assert!(c.source_content.is_some());
            assert!(!c.source_content.as_ref().unwrap().is_empty());
        }
    } else {
        // tokenizer missing: single passthrough chunk
        assert_eq!(chunks.len(), 1);
        assert_eq!(mapping.len(), 1);
    }
}

#[test]
fn test_append_to_embed_dlq_writes_file() {
    let tmp = tempdir().unwrap();
    let path = tmp.path().join("dlq.log");
    std::env::set_var("HZ_EMBED_DLQ_PATH", path.to_string_lossy().as_ref());
    append_to_embed_dlq("id123", "unit_test", 42, "head snippet");
    let content = std::fs::read_to_string(&path).expect("dlq file should exist");
    assert!(content.contains("id123"));
    assert!(content.contains("reason=unit_test"));
}
