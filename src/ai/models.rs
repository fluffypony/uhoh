use crate::config::{AiConfig, ModelTierConfig};
use anyhow::Result;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use sysinfo::{System, RefreshKind, MemoryRefreshKind};

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
pub fn select_model(config: &AiConfig) -> Option<ModelTierConfig> {
    let tiers: Vec<ModelTierConfig> = if config.models.is_empty() {
        default_model_tiers()
    } else {
        config.models.clone()
    };
    let mut sys = System::new_with_specifics(RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()));
    let total_ram_gb = sys.total_memory() / (1024 * 1024 * 1024);
    let available_ram_gb = sys.available_memory() / (1024 * 1024 * 1024);
    let min_available_margin = 2; // consistent 2GB margin
    tiers
        .into_iter()
        .rev()
        .find(|t| total_ram_gb >= t.min_ram_gb && available_ram_gb >= t.min_ram_gb.saturating_sub(min_available_margin as u64))
}

/// Ensure the model file is present under ~/.uhoh/sidecar and return its path.
pub fn ensure_model_downloaded(uhoh_dir: &Path, model: &ModelTierConfig) -> Result<PathBuf> {
    let target_dir = uhoh_dir.join("models");
    std::fs::create_dir_all(&target_dir)?;
    let target = target_dir.join(&model.filename);
    if target.exists() { return Ok(target); }

    tracing::info!("Downloading model {} from {}...", model.name, model.url);
    let resp = reqwest::blocking::Client::new().get(&model.url).send()?;
    if !resp.status().is_success() {
        anyhow::bail!("Failed to download model: HTTP {}", resp.status());
    }
    let total = resp.content_length();
    let tmp = target.with_extension("downloading");
    {
        let mut out = std::fs::File::create(&tmp)?;
        let mut reader = std::io::BufReader::new(resp);
        let mut buf = [0u8; 64 * 1024];
        let mut downloaded = 0u64;
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 { break; }
            out.write_all(&buf[..n])?;
            downloaded += n as u64;
            if let Some(total) = total { if downloaded >= total { break; } }
        }
        out.sync_all()?;
    }
    std::fs::rename(&tmp, &target)?;
    Ok(target)
}
