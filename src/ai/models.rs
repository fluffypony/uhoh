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
    let sys = System::new_with_specifics(RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()));
    let total_ram_gb = sys.total_memory() / (1024 * 1024 * 1024);
    let available_ram_gb = sys.available_memory() / (1024 * 1024 * 1024);
    let min_available_margin = 2; // require a small headroom
    tiers
        .into_iter()
        .rev()
        .find(|t| available_ram_gb >= t.min_ram_gb.saturating_add(min_available_margin as u64) || (total_ram_gb >= t.min_ram_gb && available_ram_gb >= t.min_ram_gb))
}

/// Ensure the model file is present under ~/.uhoh/sidecar and return its path.
pub fn ensure_model_downloaded(uhoh_dir: &Path, model: &ModelTierConfig) -> Result<PathBuf> {
    let target_dir = uhoh_dir.join("models");
    std::fs::create_dir_all(&target_dir)?;
    let target = target_dir.join(&model.filename);
    if target.exists() { return Ok(target); }

    tracing::info!("Downloading model {} from {}...", model.name, model.url);
    let client = reqwest::blocking::Client::new();
    let tmp = target.with_extension("downloading");
    let mut start = 0u64;
    if tmp.exists() {
        if let Ok(meta) = std::fs::metadata(&tmp) { start = meta.len(); }
    }
    // HEAD to get length
    let total = client.head(&model.url).send().ok().and_then(|r| r.headers().get(reqwest::header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<u64>().ok()));
    // Open file for append/create
    let mut out = std::fs::OpenOptions::new().create(true).append(true).open(&tmp)?;
    // Show a progress bar if total known
    let is_tty = atty::is(atty::Stream::Stderr);
    let pb = if is_tty { total.map(|t| {
        let bar = indicatif::ProgressBar::new(t);
        bar.set_position(start);
        bar.set_style(indicatif::ProgressStyle::default_bar().template("{spinner:.green} [{bar:40}] {bytes}/{total_bytes} ({eta})").unwrap());
        bar
    }) } else { None };
    // Download loop with Range
    let mut pos = start;
    loop {
        let mut req = client.get(&model.url);
        if pos > 0 { req = req.header(reqwest::header::RANGE, format!("bytes={}-", pos)); }
        let resp = req.send()?;
        if resp.status() == reqwest::StatusCode::RANGE_NOT_SATISFIABLE { break; }
        if !resp.status().is_success() { anyhow::bail!("Failed to download model: HTTP {}", resp.status()); }
        let mut reader = std::io::BufReader::new(resp);
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 { break; }
            out.write_all(&buf[..n])?;
            pos += n as u64;
            if let Some(ref bar) = pb { bar.set_position(pos); }
        }
        out.flush()?;
        if let Some(total) = total { if pos >= total { break; } }
    }
    out.sync_all()?;
    if let Some(bar) = pb { bar.finish_and_clear(); }
    std::fs::rename(&tmp, &target)?;
    Ok(target)
}
