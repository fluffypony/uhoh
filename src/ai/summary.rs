use anyhow::Result;

#[derive(Debug, Clone)]
pub struct FileChangeSummary {
    pub added: Vec<String>,
    pub deleted: Vec<String>,
    pub modified: Vec<String>,
}

pub fn append_diff_chunk(
    out: &mut String,
    chars_used: &mut usize,
    max_chars: usize,
    truncated: &mut bool,
    chunk: &str,
) {
    if *truncated || chunk.is_empty() {
        return;
    }
    if *chars_used >= max_chars {
        out.push_str("\n[Diff truncated]\n");
        *truncated = true;
        return;
    }

    let remaining = max_chars.saturating_sub(*chars_used);
    if chunk.len() <= remaining {
        out.push_str(chunk);
        *chars_used = chars_used.saturating_add(chunk.len());
        return;
    }

    let mut cut = remaining;
    while cut > 0 && !chunk.is_char_boundary(cut) {
        cut -= 1;
    }
    if cut > 0 {
        out.push_str(&chunk[..cut]);
        *chars_used = chars_used.saturating_add(cut);
    }
    out.push_str("\n[Diff truncated]\n");
    *truncated = true;
}

/// Blocking generator that spawns the sidecar if needed and queries it for a short summary.
pub fn generate_summary_blocking(
    uhoh_dir: &std::path::Path,
    ai_config: &crate::config::AiConfig,
    sidecar_manager: &crate::ai::sidecar::SidecarManager,
    diff_text: &str,
    files: &FileChangeSummary,
) -> Result<String> {
    // Cap to model/server safe upper bound to prevent runaway contexts
    let capped_tokens = ai_config.max_context_tokens;
    // Truncate diff to configured max context (rough 4 chars/token) with UTF-8 boundary safety
    let max_chars = capped_tokens.saturating_mul(4);
    let truncated = if diff_text.len() > max_chars {
        let mut cut = max_chars;
        while cut > 0 && !diff_text.is_char_boundary(cut) {
            cut -= 1;
        }
        &diff_text[..cut]
    } else {
        diff_text
    };

    // Choose a model tier using centralized selector
    let Some(model) = crate::ai::models::select_model(ai_config) else {
        tracing::warn!("No suitable AI model tier for available RAM; skipping summary generation");
        return Ok(String::new());
    };

    // On Apple Silicon with MLX, skip GGUF download — MLX fetches its own model from HuggingFace.
    // Only download GGUF when using llama-server backend.
    let uses_mlx = cfg!(all(target_os = "macos", target_arch = "aarch64"))
        && crate::ai::sidecar::is_mlx_available(uhoh_dir);
    let model_path = if uses_mlx {
        // MLX doesn't use the local GGUF; provide the filename for sidecar mapping
        uhoh_dir.join("models").join(&model.filename)
    } else {
        match crate::ai::models::ensure_model_downloaded(uhoh_dir, &model) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("Cannot download model {}: {}", model.name, e);
                return Ok(String::new());
            }
        }
    };

    // Spawn or reuse sidecar
    // Pass through context size cap so sidecar uses a consistent ctx-size
    let port = sidecar_manager.get_or_spawn_port_with_ctx(
        &model_path,
        uhoh_dir,
        ai_config.idle_shutdown_secs,
        capped_tokens as u64,
    )?;

    // Build prompt
    let prompt = format!(
        "You are analyzing a code snapshot diff. Describe what changed in 1-2 sentences.\n\nFiles added: {}\nFiles deleted: {}\nFiles modified: {}\n\nDiff (possibly truncated):\n{}",
        files.added.join(", "), files.deleted.join(", "), files.modified.join(", "), truncated
    );

    let resp: serde_json::Value =
        std::thread::spawn(move || -> anyhow::Result<serde_json::Value> {
            let client = reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .build()?;
            let r = client
                .post(format!("http://127.0.0.1:{port}/v1/chat/completions"))
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

    Ok(resp["choices"][0]["message"]["content"]
        .as_str()
        .unwrap_or("")
        .to_string())
}

// Removed async generate_summary variant to avoid duplication and drift. The
// blocking variant is used consistently; async callers should spawn it in a
// blocking task if needed.
