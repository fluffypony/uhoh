use anyhow::{Context, Result};
use std::path::Path;

use crate::config::AiConfig;

const MAX_AI_DIFF_FILE_SIZE: u64 = 512 * 1024;
const MAX_DIFF_FILES: usize = 10;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FileChangeSummary {
    pub added: Vec<String>,
    pub deleted: Vec<String>,
    pub modified: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct SummaryBlobRef<'a> {
    pub hash: &'a str,
    pub stored: bool,
    pub size: u64,
}

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct SummaryDiffEntry<'a> {
    pub path: &'a str,
    pub previous: Option<SummaryBlobRef<'a>>,
    pub current: Option<SummaryBlobRef<'a>>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PreparedSummaryInput {
    pub files: FileChangeSummary,
    pub diff_text: String,
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

pub fn prepare_summary_inputs(
    blob_root: &Path,
    ai_config: &AiConfig,
    changes: &[SummaryDiffEntry<'_>],
) -> PreparedSummaryInput {
    let mut files = FileChangeSummary {
        added: Vec::new(),
        deleted: Vec::new(),
        modified: Vec::new(),
    };
    let max_diff_chars = ai_config.max_context_tokens.saturating_mul(4);
    let mut diff_text = String::new();
    let mut diff_chars = 0usize;
    let mut diff_truncated = false;
    let mut diff_files = 0usize;

    for change in changes {
        match (change.previous, change.current) {
            (None, Some(_)) => files.added.push(change.path.to_string()),
            (Some(_), None) => files.deleted.push(change.path.to_string()),
            (Some(previous), Some(current)) if previous.hash != current.hash => {
                files.modified.push(change.path.to_string());
                if diff_truncated || diff_files >= MAX_DIFF_FILES {
                    continue;
                }
                diff_files += 1;
                append_change_diff(
                    blob_root,
                    change.path,
                    previous,
                    current,
                    max_diff_chars,
                    &mut diff_text,
                    &mut diff_chars,
                    &mut diff_truncated,
                );
            }
            _ => {}
        }
    }

    PreparedSummaryInput { files, diff_text }
}

fn append_change_diff(
    blob_root: &Path,
    path: &str,
    previous: SummaryBlobRef<'_>,
    current: SummaryBlobRef<'_>,
    max_diff_chars: usize,
    diff_text: &mut String,
    diff_chars: &mut usize,
    diff_truncated: &mut bool,
) {
    if !previous.stored || !current.stored || previous.hash.is_empty() || current.hash.is_empty() {
        return;
    }

    if previous.size > MAX_AI_DIFF_FILE_SIZE || current.size > MAX_AI_DIFF_FILE_SIZE {
        append_diff_chunk(
            diff_text,
            diff_chars,
            max_diff_chars,
            diff_truncated,
            &format!("--- {path}\n[File too large for AI diff]\n"),
        );
        return;
    }

    let old = crate::cas::read_blob(blob_root, previous.hash)
        .ok()
        .flatten();
    let new = crate::cas::read_blob(blob_root, current.hash)
        .ok()
        .flatten();
    let (Some(old), Some(new)) = (old, new) else {
        return;
    };

    let head_old = &old[..old.len().min(8192)];
    let head_new = &new[..new.len().min(8192)];
    if content_inspector::inspect(head_old).is_binary()
        || content_inspector::inspect(head_new).is_binary()
    {
        append_diff_chunk(
            diff_text,
            diff_chars,
            max_diff_chars,
            diff_truncated,
            &format!("--- {path}\n[Binary file]\n"),
        );
        return;
    }

    let (Ok(old_s), Ok(new_s)) = (String::from_utf8(old), String::from_utf8(new)) else {
        return;
    };

    let diff = similar::TextDiff::from_lines(&old_s, &new_s);
    append_diff_chunk(
        diff_text,
        diff_chars,
        max_diff_chars,
        diff_truncated,
        &format!("--- a/{path}\n+++ b/{path}\n"),
    );

    for hunk in diff.unified_diff().context_radius(2).iter_hunks() {
        if *diff_chars >= max_diff_chars {
            append_diff_truncation_marker(diff_text, diff_truncated);
            break;
        }

        append_diff_chunk(
            diff_text,
            diff_chars,
            max_diff_chars,
            diff_truncated,
            &format!("{}\n", hunk.header()),
        );

        for change in hunk.iter_changes() {
            if *diff_chars >= max_diff_chars {
                append_diff_truncation_marker(diff_text, diff_truncated);
                break;
            }

            let sign = match change.tag() {
                similar::ChangeTag::Delete => '-',
                similar::ChangeTag::Insert => '+',
                similar::ChangeTag::Equal => ' ',
            };
            append_diff_chunk(
                diff_text,
                diff_chars,
                max_diff_chars,
                diff_truncated,
                &format!("{sign}{change}"),
            );
        }

        if *diff_truncated {
            break;
        }
    }
}

fn append_diff_truncation_marker(diff_text: &mut String, diff_truncated: &mut bool) {
    if !*diff_truncated {
        diff_text.push_str("\n[Diff truncated]\n");
        *diff_truncated = true;
    }
}

/// Blocking generator that starts or restarts the sidecar if needed and queries it for a short
/// summary. `Ok(None)` means summary generation was intentionally skipped.
pub fn generate_summary_blocking(
    uhoh_dir: &std::path::Path,
    ai_config: &AiConfig,
    sidecar_manager: &crate::ai::sidecar::SidecarManager,
    diff_text: &str,
    files: &FileChangeSummary,
) -> Result<Option<String>> {
    // Choose a model tier using centralized selector
    let Some(model) = crate::ai::models::select_model(ai_config) else {
        tracing::warn!("No suitable AI model tier for available RAM; skipping summary generation");
        return Ok(None);
    };

    // On Apple Silicon with MLX, skip GGUF download — MLX fetches its own model from HuggingFace.
    // Only download GGUF when using llama-server backend.
    let uses_mlx = cfg!(all(target_os = "macos", target_arch = "aarch64"))
        && crate::ai::sidecar::is_mlx_available(uhoh_dir);
    let model_path = if uses_mlx {
        // MLX doesn't use the local GGUF; provide the filename for sidecar mapping
        uhoh_dir.join("models").join(&model.filename)
    } else {
        crate::ai::models::ensure_model_downloaded(uhoh_dir, &model)
            .with_context(|| format!("Cannot download model {}", model.name))?
    };

    // Spawn or reuse sidecar
    // Pass through context size cap so sidecar uses a consistent ctx-size
    let port = sidecar_manager.start_or_restart_port_with_ctx(
        &model_path,
        uhoh_dir,
        ai_config.idle_shutdown_secs,
        ai_config.max_context_tokens as u64,
    )?;

    // Build prompt
    let prompt = format!(
        "You are analyzing a code snapshot diff. Describe what changed in 1-2 sentences.\n\nFiles added: {}\nFiles deleted: {}\nFiles modified: {}\n\nDiff (possibly truncated):\n{}",
        files.added.join(", "),
        files.deleted.join(", "),
        files.modified.join(", "),
        diff_text
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

    let summary = resp["choices"][0]["message"]["content"]
        .as_str()
        .context("AI summary response missing text content")?
        .trim();
    if summary.is_empty() {
        anyhow::bail!("AI summary response was empty");
    }

    Ok(Some(summary.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{prepare_summary_inputs, SummaryBlobRef, SummaryDiffEntry};
    use crate::cas;

    #[test]
    fn diff_truncation_marker_appears_once_when_budget_is_exceeded() {
        let temp = tempfile::tempdir().expect("tempdir");
        let blob_root = temp.path().join("blobs");
        let old = "alpha\n".repeat(64);
        let new = "beta\n".repeat(64);
        let (old_hash, _) = cas::store_blob(&blob_root, old.as_bytes()).expect("store old blob");
        let (new_hash, _) = cas::store_blob(&blob_root, new.as_bytes()).expect("store new blob");

        let mut config = crate::config::Config::default();
        config.ai.max_context_tokens = 8;

        let prepared = prepare_summary_inputs(
            &blob_root,
            &config.ai,
            &[SummaryDiffEntry {
                path: "src/lib.rs",
                previous: Some(SummaryBlobRef {
                    hash: &old_hash,
                    stored: true,
                    size: old.len() as u64,
                }),
                current: Some(SummaryBlobRef {
                    hash: &new_hash,
                    stored: true,
                    size: new.len() as u64,
                }),
            }],
        );

        assert!(prepared.diff_text.contains("[Diff truncated]"));
        assert_eq!(prepared.diff_text.matches("[Diff truncated]").count(), 1);
        assert_eq!(prepared.files.modified, vec!["src/lib.rs".to_string()]);
    }
}
