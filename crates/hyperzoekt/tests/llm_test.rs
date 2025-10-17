use hyperzoekt::llm;

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires API key
async fn test_llm_summarize_cluster() {
    // This test requires HZ_LLM_URL and OPENAI_API_KEY to be set
    env_logger::init();

    let test_snippets = vec![
        "fn index_repository(repo: &str) -> Result<()> { ... }".to_string(),
        "fn build_index(path: &Path) -> Index { ... }".to_string(),
        "struct Repository { name: String, path: PathBuf }".to_string(),
    ];

    println!("\n=== Testing LLM Summarization ===");
    println!("HZ_LLM_URL: {:?}", std::env::var("HZ_LLM_URL").ok());
    println!("HZ_LLM_MODEL: {:?}", std::env::var("HZ_LLM_MODEL").ok());
    println!(
        "OPENAI_API_KEY set: {}",
        std::env::var("OPENAI_API_KEY").is_ok()
    );
    println!(
        "HZ_OPENAI_API_KEY set: {}",
        std::env::var("HZ_OPENAI_API_KEY").is_ok()
    );

    match llm::summarize_cluster("test-cluster", &test_snippets).await {
        Ok(summary) => {
            println!("\n=== SUCCESS ===");
            println!("Summary length: {}", summary.len());
            println!("Summary content: {}", summary);

            if summary.is_empty() {
                println!("\n⚠️  WARNING: Summary is empty!");
            }
        }
        Err(e) => {
            println!("\n=== ERROR ===");
            println!("Error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_llm_empty_snippets() {
    env_logger::init();

    let empty_snippets = vec!["".to_string(), "   ".to_string(), "\n".to_string()];

    println!("\n=== Testing LLM with Empty Snippets ===");
    match llm::summarize_cluster("empty-cluster", &empty_snippets).await {
        Ok(summary) => {
            println!("✅ Correctly returned empty summary without calling LLM");
            assert!(
                summary.is_empty(),
                "Expected empty summary for empty snippets"
            );
        }
        Err(e) => {
            panic!("Should not error on empty snippets: {}", e);
        }
    }
}
