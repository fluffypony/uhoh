use anyhow::Result;

#[derive(Debug, Clone)]
pub struct FileChangeSummary {
    pub added: Vec<String>,
    pub deleted: Vec<String>,
    pub modified: Vec<String>,
}

/// Blocking generator that spawns the sidecar if needed and queries it for a short summary.
pub fn generate_summary_blocking(
    uhoh_dir: &std::path::Path,
    config: &crate::config::Config,
    diff_text: &str,
    files: &FileChangeSummary,
) -> Result<String> {
    // Cap to a safe upper bound to prevent runaway contexts
    let capped_tokens = std::cmp::min(config.ai.max_context_tokens, 8192);
    // Truncate diff to configured max context (rough 4 chars/token) with UTF-8 boundary safety
    let max_chars = capped_tokens.saturating_mul(4);
    let truncated = if diff_text.len() > max_chars {
        let mut cut = max_chars;
        while cut > 0 && !diff_text.is_char_boundary(cut) { cut -= 1; }
        &diff_text[..cut]
    } else { diff_text };

    // Choose a model tier using centralized selector
    let Some(model) = crate::ai::models::select_model(&config.ai) else {
        tracing::warn!("No suitable AI model tier for available RAM; skipping summary generation");
        return Ok(String::new());
    };

    // Ensure model is available locally
    let model_path = match crate::ai::models::ensure_model_downloaded(uhoh_dir, &model) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("Cannot download model {}: {}", model.name, e);
            return Ok(String::new());
        }
    };

    // Spawn or reuse sidecar
    let port = crate::ai::sidecar::get_or_spawn_port(&model_path, uhoh_dir, config.ai.idle_shutdown_secs)?;

    // Build prompt
    let prompt = format!(
        "You are analyzing a code snapshot diff. Describe what changed in 1-2 sentences.\n\nFiles added: {}\nFiles deleted: {}\nFiles modified: {}\n\nDiff (possibly truncated):\n{}",
        files.added.join(", "), files.deleted.join(", "), files.modified.join(", "), truncated
    );

    let resp: serde_json::Value = std::thread::spawn(move || -> anyhow::Result<serde_json::Value> {
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()?;
        let r = client
            .post(format!("http://127.0.0.1:{}/v1/chat/completions", port))
            .json(&serde_json::json!({
                "model": model.name,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 256,
                "temperature": 0.3,
            }))
            .send()?
            .json()?;
        Ok(r)
    })
    .join()
    .map_err(|_| anyhow::anyhow!("HTTP join error"))??;

    Ok(resp["choices"][0]["message"]["content"].as_str().unwrap_or("").to_string())
}

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
        // Rough char-to-token ratio with UTF-8 boundary safety
        let mut cut = max_tokens * 4;
        while cut > 0 && !diff_text.is_char_boundary(cut) { cut -= 1; }
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
