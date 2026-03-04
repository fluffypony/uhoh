use crate::config::{AiConfig, ModelTierConfig};
use anyhow::Result;
use std::path::{Path, PathBuf};
use sysinfo::System;

/// Default model tiers (used when config has none).
/// Updated to Qwen3.5 tiers.
pub fn default_model_tiers() -> Vec<ModelTierConfig> {
    vec![
        ModelTierConfig { name: "Qwen3.5-9B-Q4_K_M".into(), filename: "qwen3.5-9b-q4_k_m.gguf".into(), url: "https://huggingface.co/unsloth/Qwen3.5-9B-GGUF/resolve/main/Qwen3.5-9B-Q4_K_M.gguf".into(), min_ram_gb: 8 },
        ModelTierConfig { name: "Qwen3.5-9B-Q8_0".into(), filename: "qwen3.5-9b-q8_0.gguf".into(), url: "https://huggingface.co/unsloth/Qwen3.5-9B-GGUF/resolve/main/Qwen3.5-9B-Q8_0.gguf".into(), min_ram_gb: 16 },
        ModelTierConfig { name: "Qwen3.5-35B-A3B-Q4_K_M".into(), filename: "qwen3.5-35b-a3b-q4_k_m.gguf".into(), url: "https://huggingface.co/unsloth/Qwen3.5-35B-A3B-GGUF/resolve/main/Qwen3.5-35B-A3B-Q4_K_M.gguf".into(), min_ram_gb: 24 },
        ModelTierConfig { name: "Qwen3.5-35B-A3B-Q6_K".into(), filename: "qwen3.5-35b-a3b-q6_k.gguf".into(), url: "https://huggingface.co/unsloth/Qwen3.5-35B-A3B-GGUF/resolve/main/Qwen3.5-35B-A3B-Q6_K.gguf".into(), min_ram_gb: 32 },
        ModelTierConfig { name: "Qwen3.5-35B-A3B-Q8_0".into(), filename: "qwen3.5-35b-a3b-q8_0.gguf".into(), url: "https://huggingface.co/unsloth/Qwen3.5-35B-A3B-GGUF/resolve/main/Qwen3.5-35B-A3B-Q8_0.gguf".into(), min_ram_gb: 48 },
    ]
}

/// Select the best model tier based on available system RAM.
pub fn select_model(config: &AiConfig) -> Option<&ModelTierConfig> {
    let tiers = if config.models.is_empty() { return None } else { &config.models };
    let mut sys = System::new();
    sys.refresh_memory();
    let total_ram_gb = sys.total_memory() / (1024 * 1024 * 1024);
    tiers.iter().rev().find(|t| total_ram_gb >= t.min_ram_gb + 2)
}

/// Ensure the model file is present under ~/.uhoh/sidecar and return its path.
pub fn ensure_model_downloaded(uhoh_dir: &Path, model: &ModelTierConfig) -> Result<PathBuf> {
    let target_dir = uhoh_dir.join("sidecar");
    std::fs::create_dir_all(&target_dir)?;
    let target = target_dir.join(&model.filename);
    if target.exists() { return Ok(target); }

    tracing::info!("Downloading model {}...", model.name);
    let resp = reqwest::blocking::get(&model.url)?;
    if !resp.status().is_success() {
        anyhow::bail!("Failed to download model: HTTP {}", resp.status());
    }
    let bytes = resp.bytes()?;
    std::fs::write(&target, &bytes)?;
    Ok(target)
}
