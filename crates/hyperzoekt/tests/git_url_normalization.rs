// Ensure writer placeholder normalization and event_consumer normalization
// produce the same canonical git_url for a variety of inputs.

fn writer_normalize(rn: &str) -> String {
    // Implementation mirrors the logic in crates/hyperzoekt/src/db/writer.rs
    let raw = rn.trim_end_matches(".git");
    let repo_url =
        if raw.starts_with("http://") || raw.starts_with("https://") || raw.starts_with("git@") {
            if raw.starts_with("git@") {
                if let Some(colon) = raw.find(':') {
                    let host = &raw[4..colon];
                    let rest = &raw[colon + 1..];
                    let rest = rest.trim_end_matches(".git");
                    format!("https://{}/{}", host, rest)
                } else {
                    format!("https://{}", raw)
                }
            } else {
                raw.to_string()
            }
        } else {
            format!("https://{}", raw)
        };
    repo_url
}

fn consumer_normalize(git_url: &str) -> String {
    // Implementation mirrors the logic in crates/hyperzoekt/src/event_consumer.rs
    let mut git_url_for_meta = git_url.to_string();
    if git_url_for_meta.starts_with("git@") {
        if let Some(colon) = git_url_for_meta.find(':') {
            let host = &git_url_for_meta[4..colon];
            let rest = &git_url_for_meta[colon + 1..];
            let rest = rest.strip_suffix(".git").unwrap_or(rest);
            git_url_for_meta = format!("https://{}/{}", host, rest);
        }
    } else if git_url_for_meta.starts_with("http://") || git_url_for_meta.starts_with("https://") {
        git_url_for_meta = git_url_for_meta.trim_end_matches(".git").to_string();
    } else {
        git_url_for_meta = format!("https://{}", git_url_for_meta.trim_end_matches(".git"));
    }
    git_url_for_meta
}

#[test]
fn writer_and_consumer_normalize_match_for_examples() {
    let examples = vec![
        "git@github.com:owner/repo.git",
        "git@github.com:owner/repo",
        "owner/repo",
        "github.com/owner/repo.git",
        "https://github.com/owner/repo.git",
        "http://example.com/owner/repo.git",
        "https://gitlab.com/owner/repo",
    ];

    for input in examples {
        let w = writer_normalize(input);
        let c = consumer_normalize(input);
        assert_eq!(
            w, c,
            "mismatch for input '{}': writer='{}' consumer='{}'",
            input, w, c
        );
    }
}
