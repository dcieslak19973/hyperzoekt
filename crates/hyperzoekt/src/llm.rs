use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct ChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Serialize)]
struct ChatReq<'a> {
    model: &'a str,
    messages: Vec<ChatMessage<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_completion_tokens: Option<u32>,
}

#[derive(Deserialize, Debug)]
struct ChatChoice {
    message: Option<serde_json::Value>,
    finish_reason: Option<String>,
}

#[derive(Deserialize, Debug)]
struct ChatResp {
    choices: Vec<ChatChoice>,
}

/// Summarize a cluster using an OpenAI-compatible chat completion API.
/// The endpoint is read from HZ_LLM_URL (default empty). The model is read
/// from HZ_LLM_MODEL (default: "gpt-5-nano"). Returns an empty string on
/// non-fatal errors so callers can still persist cluster records without
/// summaries.
pub async fn summarize_cluster(cluster_label: &str, member_snippets: &[String]) -> Result<String> {
    let base = std::env::var("HZ_LLM_URL").ok();
    let model = std::env::var("HZ_LLM_MODEL").unwrap_or_else(|_| "gpt-5-nano".to_string());
    let endpoint = if let Some(url) = base {
        // If user provided an OpenAI-compatible base URL, use standard path
        if url.contains("/v1/") {
            url
        } else {
            format!("{}/v1/chat/completions", url.trim_end_matches('/'))
        }
    } else {
        // No LLM configured; return empty summary
        return Ok(String::new());
    };

    // Filter out empty/whitespace-only snippets
    let non_empty_snippets: Vec<&String> = member_snippets
        .iter()
        .filter(|s| !s.trim().is_empty())
        .collect();

    if non_empty_snippets.is_empty() {
        log::warn!(
            "No non-empty snippets available for cluster '{}', skipping LLM call",
            cluster_label
        );
        return Ok(String::new());
    }

    // Select a diverse sample of snippets to show breadth
    let sample_size = non_empty_snippets.len().min(20); // Increased from 10 to 20
    let mut prompt = format!(
        "You are analyzing a code cluster labeled '{}' containing {} code snippets. \
        Your task is to provide a comprehensive analysis, summary, and label.\n\n\
        Here is a representative sample of {} snippets from this cluster:\n\n",
        cluster_label,
        non_empty_snippets.len(),
        sample_size
    );

    // Show snippets with more context
    for (i, s) in non_empty_snippets.iter().take(sample_size).enumerate() {
        prompt.push_str(&format!("{}. {}\n\n", i + 1, s));
    }

    if non_empty_snippets.len() > sample_size {
        prompt.push_str(&format!(
            "... and {} more snippets in this cluster\n\n",
            non_empty_snippets.len() - sample_size
        ));
    }

    prompt.push_str(
        "Based on these code snippets, provide a comprehensive analysis:\n\n\
        1. **Label** (5-10 words): A clear, descriptive label that captures the main purpose or theme of this cluster\n\n\
        2. **Summary** (4-8 sentences): A detailed summary that includes:\n\
           - **Purpose**: What is the primary functionality or responsibility of this code cluster?\n\
           - **Key Components**: What are the main types, functions, classes, or modules involved?\n\
           - **Patterns & Techniques**: What programming patterns, algorithms, or architectural approaches are used?\n\
           - **Relationships**: How do the different pieces interact with each other? What are the key data flows?\n\
           - **Context**: What domain or layer of the application does this code serve? (e.g., data access, business logic, API, UI)\n\
           - **Notable Details**: Any important implementation details, design decisions, or constraints visible in the code?\n\n\
        Return your response as a JSON object with keys: \"label\" and \"summary\".\n\n\
        Example format:\n\
        {\n\
          \"label\": \"Repository Indexing and Search Operations\",\n\
          \"summary\": \"This cluster implements the core repository indexing functionality for a code search engine. \
        It includes functions for cloning repositories, parsing source files, and building inverted indexes. \
        The code uses a multi-threaded architecture with a worker pool pattern to process multiple repositories concurrently. \
        Key data structures include an IndexBuilder that constructs posting lists and a Repository struct that tracks metadata. \
        The cluster integrates with a Git library for repository operations and implements custom tokenization for code-aware search. \
        Error handling follows a Result-based pattern with detailed error types for different failure modes.\"\n\
        }\n\n\
        Provide your analysis now:\n",
    );

    log::info!(
        "LLM request for cluster '{}': sending {} non-empty snippets (out of {} total, prompt length: {} chars) to model '{}' at endpoint '{}'",
        cluster_label,
        sample_size,
        member_snippets.len(),
        prompt.len(),
        model,
        endpoint
    );
    log::info!(
        "LLM prompt for cluster '{}': {}",
        cluster_label,
        if prompt.len() > 500 {
            format!(
                "{}... [truncated, total {} chars]",
                &prompt[..500],
                prompt.len()
            )
        } else {
            prompt.clone()
        }
    );

    let client = Client::new();
    let body = ChatReq {
        model: &model,
        messages: vec![ChatMessage {
            role: "user",
            content: &prompt,
        }],
        max_tokens: None,
        max_completion_tokens: Some(4096), // Increased to 4096 for comprehensive summaries with reasoning models
    };

    let mut req = client.post(&endpoint).json(&body);
    // Support OpenAI API key via HZ_OPENAI_API_KEY or OPENAI_API_KEY
    if let Ok(k) = std::env::var("HZ_OPENAI_API_KEY") {
        req = req.bearer_auth(k);
    } else if let Ok(k) = std::env::var("OPENAI_API_KEY") {
        req = req.bearer_auth(k);
    }

    let resp = req.send().await.map_err(|e| anyhow::anyhow!(e))?;
    let status = resp.status();
    log::info!(
        "LLM response for cluster '{}': status={}, success={}",
        cluster_label,
        status,
        status.is_success()
    );

    if !status.is_success() {
        let body = resp
            .text()
            .await
            .unwrap_or_else(|_| "Unable to read body".to_string());
        log::warn!(
            "LLM request failed for cluster '{}': status={}, body={}",
            cluster_label,
            status,
            body
        );
        // Non-fatal: return empty summary
        return Ok(String::new());
    }

    // Get the response text first so we can log it before parsing
    let response_text = resp.text().await.map_err(|e| anyhow::anyhow!(e))?;
    log::info!(
        "LLM raw response text for cluster '{}': {}",
        cluster_label,
        if response_text.len() > 1000 {
            format!(
                "{}... [truncated, total {} chars]",
                &response_text[..1000],
                response_text.len()
            )
        } else {
            response_text.clone()
        }
    );

    let jr: ChatResp = serde_json::from_str(&response_text)
        .map_err(|e| anyhow::anyhow!("Failed to parse LLM response: {}", e))?;

    // Debug: log the raw response structure
    log::debug!(
        "LLM raw response for cluster '{}': choices.len()={}, first_choice={:?}",
        cluster_label,
        jr.choices.len(),
        jr.choices.first()
    );

    if jr.choices.is_empty() {
        log::warn!(
            "LLM response for cluster '{}' has no choices",
            cluster_label
        );
        return Ok(String::new());
    }
    // Try to extract text content from the first choice.message.content
    if let Some(msg) = &jr.choices[0].message {
        log::debug!(
            "LLM message structure for cluster '{}': {:?}",
            cluster_label,
            msg
        );

        if let Some(txt) = msg.get("content") {
            if let Some(s) = txt.as_str() {
                log::info!(
                    "LLM summary extracted for cluster '{}' (length: {} chars): {}",
                    cluster_label,
                    s.len(),
                    if s.len() > 100 {
                        format!("{}...", &s[..100])
                    } else {
                        s.to_string()
                    }
                );
                return Ok(s.trim().to_string());
            }
        }
        // Some implementations return {"role":"assistant","content":{"text":"..."}}
        if let Some(obj) = msg.as_object() {
            if let Some(content_val) = obj.get("content") {
                if content_val.is_string() {
                    let s = content_val.as_str().unwrap_or("").trim().to_string();
                    log::info!(
                        "LLM summary extracted (nested) for cluster '{}' (length: {} chars): {}",
                        cluster_label,
                        s.len(),
                        if s.len() > 100 {
                            format!("{}...", &s[..100])
                        } else {
                            s.clone()
                        }
                    );
                    return Ok(s);
                } else if let Some(m) = content_val.get("text") {
                    let s = m.as_str().unwrap_or("").trim().to_string();
                    log::info!(
                        "LLM summary extracted (text field) for cluster '{}' (length: {} chars): {}",
                        cluster_label,
                        s.len(),
                        if s.len() > 100 { format!("{}...", &s[..100]) } else { s.clone() }
                    );
                    return Ok(s);
                }
            }
        }
    }
    log::warn!(
        "LLM response for cluster '{}' could not extract content from message",
        cluster_label
    );
    Ok(String::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_summarize_cluster_no_url() {
        std::env::remove_var("HZ_LLM_URL");
        let got = summarize_cluster("test", &vec!["fn foo() {}".to_string()])
            .await
            .unwrap();
        assert_eq!(got, "");
    }
}
