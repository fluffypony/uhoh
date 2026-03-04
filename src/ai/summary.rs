use anyhow::Result;

/// Generate an AI summary for a snapshot diff.
/// This is fire-and-forget: failures are logged but never block snapshot creation.
pub async fn generate_summary(
    port: u16,
    diff_text: &str,
    files_added: &[String],
    files_deleted: &[String],
    files_modified: &[String],
    max_tokens: usize,
) -> Result<String> {
    // Truncate diff to configured max context (prevents memory exhaustion)
    let truncated_diff = if diff_text.len() > max_tokens * 4 {
        // Rough char-to-token ratio
        let cut = max_tokens * 4;
        format!("{}\n...[diff truncated]", &diff_text[..cut])
    } else {
        diff_text.to_string()
    };

    let prompt = format!(
        "You are analyzing a code snapshot diff. Describe what changed in 1-2 sentences.\n\n\
         Files added: {}\nFiles deleted: {}\nFiles modified: {}\n\n\
         Diff:\n{}",
        files_added.join(", "),
        files_deleted.join(", "),
        files_modified.join(", "),
        truncated_diff,
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let resp: serde_json::Value = client
        .post(format!("http://127.0.0.1:{}/v1/chat/completions", port))
        .json(&serde_json::json!({
            "model": "local",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 256,
            "temperature": 0.3,
        }))
        .send()
        .await?
        .json()
        .await?;

    Ok(resp["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or("")
        .to_string())
}
