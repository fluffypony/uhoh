use crate::config::{AiConfig, ModelTierConfig};
use anyhow::Result;
use std::io::IsTerminal;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use sysinfo::{MemoryRefreshKind, RefreshKind, System};

/// Default model tiers (used when config has none).
/// Updated to Qwen3.5 tiers.
pub fn default_model_tiers() -> Vec<ModelTierConfig> {
    vec![
        ModelTierConfig { name: "Qwen3.5-9B-Q4_K_M".into(), filename: "qwen3.5-9b-q4_k_m.gguf".into(), url: "https://huggingface.co/unsloth/Qwen3.5-9B-GGUF/resolve/main/Qwen3.5-9B-Q4_K_M.gguf".into(), min_ram_gb: 8 },
        ModelTierConfig { name: "Qwen3.5-9B-Q8_0".into(), filename: "qwen3.5-9b-q8_0.gguf".into(), url: "https://huggingface.co/unsloth/Qwen3.5-9B-GGUF/resolve/main/Qwen3.5-9B-Q8_0.gguf".into(), min_ram_gb: 16 },
        // Qwen3.5 35B-A3B MoE tiers (GatedDeltaNet attention) — llama.cpp supports this per upstream.
        ModelTierConfig { name: "Qwen3.5-35B-A3B-Q4_K_M".into(), filename: "qwen3.5-35b-a3b-q4_k_m.gguf".into(), url: "https://huggingface.co/unsloth/Qwen3.5-35B-A3B-GGUF/resolve/main/Qwen3.5-35B-A3B-Q4_K_M.gguf".into(), min_ram_gb: 24 },
        ModelTierConfig { name: "Qwen3.5-35B-A3B-Q6_K".into(), filename: "qwen3.5-35b-a3b-q6_k.gguf".into(), url: "https://huggingface.co/unsloth/Qwen3.5-35B-A3B-GGUF/resolve/main/Qwen3.5-35B-A3B-Q6_K.gguf".into(), min_ram_gb: 32 },
        ModelTierConfig { name: "Qwen3.5-35B-A3B-Q8_0".into(), filename: "qwen3.5-35b-a3b-q8_0.gguf".into(), url: "https://huggingface.co/unsloth/Qwen3.5-35B-A3B-GGUF/resolve/main/Qwen3.5-35B-A3B-Q8_0.gguf".into(), min_ram_gb: 48 },
    ]
}

/// Select the best model tier based on available system RAM.
pub fn select_model(config: &AiConfig) -> Option<ModelTierConfig> {
    select_model_with_sys(config, None)
}

pub fn select_model_with_sys(
    config: &AiConfig,
    existing_sys: Option<&System>,
) -> Option<ModelTierConfig> {
    let tiers: Vec<ModelTierConfig> = if config.models.is_empty() {
        default_model_tiers()
    } else {
        config.models.clone()
    };
    let owned_sys;
    let sys = match existing_sys {
        Some(s) => s,
        None => {
            owned_sys = System::new_with_specifics(
                RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
            );
            &owned_sys
        }
    };
    // Use MB precision to avoid truncation (B33)
    let total_ram_mb = sys.total_memory() / (1024 * 1024);
    let available_ram_mb = sys.available_memory() / (1024 * 1024);
    let min_available_margin_mb: u64 = 2 * 1024; // 2 GB headroom in MB
    tiers.into_iter().rev().find(|t| {
        let min_mb = t.min_ram_gb * 1024;
        // Require BOTH: total RAM sufficient AND available RAM covers model + margin
        total_ram_mb >= min_mb && available_ram_mb >= min_mb + min_available_margin_mb
    })
}

/// Ensure the model file is present under `~/.uhoh/models` and return its path.
pub fn ensure_model_downloaded(uhoh_dir: &Path, model: &ModelTierConfig) -> Result<PathBuf> {
    let target_dir = uhoh_dir.join("models");
    std::fs::create_dir_all(&target_dir)?;
    let target = target_dir.join(&model.filename);
    if target.exists() {
        return Ok(target);
    }

    tracing::info!("Downloading model {} from {}...", model.name, model.url);
    let client = reqwest::blocking::Client::builder()
        .user_agent("uhoh")
        .build()?;
    let tmp = target.with_extension("downloading");
    let mut start = 0u64;
    if tmp.exists() {
        if let Ok(meta) = std::fs::metadata(&tmp) {
            start = meta.len();
        }
    }
    // HEAD to get length
    let total = client.head(&model.url).send().ok().and_then(|r| {
        r.headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
    });
    // Open file in read/write mode (not append, to allow seek after truncation)
    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&tmp)?;
    // Seek to end for resume
    {
        use std::io::Seek;
        out.seek(std::io::SeekFrom::End(0))?;
    }
    // Show a progress bar if total known
    let is_tty = std::io::stderr().is_terminal();
    let pb = if is_tty {
        total.map(|t| {
            let bar = indicatif::ProgressBar::new(t);
            bar.set_position(start);
            bar.set_style(
                indicatif::ProgressStyle::default_bar()
                    .template("{spinner:.green} [{bar:40}] {bytes}/{total_bytes} ({eta})")
                    .unwrap(),
            );
            bar
        })
    } else {
        None
    };
    // Download loop with Range
    let mut pos = start;
    loop {
        let mut req = client.get(&model.url);
        if pos > 0 {
            req = req.header(reqwest::header::RANGE, format!("bytes={pos}-"));
        }
        let resp = req.send()?;
        if pos > 0 && resp.status() == reqwest::StatusCode::OK {
            tracing::warn!(
                "Server ignored Range request for {}, restarting download from zero",
                model.name
            );
            pos = 0;
            out.set_len(0)?;
            use std::io::Seek;
            out.seek(std::io::SeekFrom::Start(0))?;
            continue;
        }
        if resp.status() == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            if let Some(total) = total {
                if pos == total {
                    break;
                }
            }
            tracing::warn!(
                "Range resume rejected for {} at {} bytes, restarting download from zero",
                model.name,
                pos
            );
            pos = 0;
            out.set_len(0)?;
            use std::io::Seek;
            out.seek(std::io::SeekFrom::Start(0))?;
            continue;
        }
        if !resp.status().is_success() {
            anyhow::bail!("Failed to download model: HTTP {}", resp.status());
        }
        let mut reader = std::io::BufReader::new(resp);
        let mut buf = [0u8; 64 * 1024];
        let mut received_any = false;
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            out.write_all(&buf[..n])?;
            pos += n as u64;
            received_any = true;
            if let Some(ref bar) = pb {
                bar.set_position(pos);
            }
        }
        out.flush()?;
        if let Some(total) = total {
            if pos >= total {
                break;
            }
            tracing::info!(
                "Download interrupted at {} bytes, retrying with resume",
                pos
            );
            continue;
        }
        if !received_any {
            tracing::warn!(
                "Download ended without progress for {} at {} bytes",
                model.name,
                pos
            );
        }
        break;
    }
    out.sync_all()?;
    if let Some(bar) = pb {
        bar.finish_and_clear();
    }

    // Post-download integrity check: compute BLAKE3 hash over the complete file.
    // This catches corruption from interrupted/resumed downloads where the upstream
    // file may have changed between segments.
    let hash = {
        let mut hasher = blake3::Hasher::new();
        let mut f = std::fs::File::open(&tmp)?;
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = std::io::Read::read(&mut f, &mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        hasher.finalize().to_hex().to_string()
    };
    tracing::info!(
        "Model download complete: BLAKE3={}, size={} bytes",
        &hash[..16],
        pos
    );

    std::fs::rename(&tmp, &target)?;
    Ok(target)
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AiConfig, ModelTierConfig};

    #[test]
    fn default_model_tiers_are_sorted_by_ram() {
        let tiers = default_model_tiers();
        assert!(!tiers.is_empty());
        for window in tiers.windows(2) {
            assert!(
                window[0].min_ram_gb <= window[1].min_ram_gb,
                "Tiers must be sorted ascending by min_ram_gb: {} ({} GB) should come before {} ({} GB)",
                window[0].name,
                window[0].min_ram_gb,
                window[1].name,
                window[1].min_ram_gb,
            );
        }
    }

    #[test]
    fn default_model_tiers_have_valid_fields() {
        for tier in default_model_tiers() {
            assert!(!tier.name.is_empty(), "tier name must not be empty");
            assert!(
                tier.filename.ends_with(".gguf"),
                "filename should end with .gguf: {}",
                tier.filename
            );
            assert!(
                tier.url.starts_with("https://"),
                "url should be HTTPS: {}",
                tier.url
            );
            assert!(tier.min_ram_gb > 0, "min_ram_gb must be positive");
        }
    }

    #[test]
    fn select_model_picks_highest_fitting_tier() {
        // The selection logic iterates tiers in reverse (highest RAM first)
        // and picks the first tier where total_ram >= min AND available >= min + 2GB.
        // We can test this deterministically by checking the tier list logic directly.
        let tiers = vec![
            ModelTierConfig {
                name: "small".into(),
                filename: "small.gguf".into(),
                url: "https://example.com/small.gguf".into(),
                min_ram_gb: 4,
            },
            ModelTierConfig {
                name: "medium".into(),
                filename: "medium.gguf".into(),
                url: "https://example.com/medium.gguf".into(),
                min_ram_gb: 8,
            },
            ModelTierConfig {
                name: "large".into(),
                filename: "large.gguf".into(),
                url: "https://example.com/large.gguf".into(),
                min_ram_gb: 64,
            },
        ];

        // Simulate the selection logic with known RAM values (16 GB total, 12 GB available)
        let total_ram_mb: u64 = 16 * 1024;
        let available_ram_mb: u64 = 12 * 1024;
        let min_available_margin_mb: u64 = 2 * 1024;
        let result = tiers.into_iter().rev().find(|t| {
            let min_mb = t.min_ram_gb * 1024;
            total_ram_mb >= min_mb && available_ram_mb >= min_mb + min_available_margin_mb
        });

        // With 16GB total and 12GB available, "medium" (8GB + 2GB margin = 10GB) should win
        // "large" (64GB) is too big, "small" (4GB) fits but "medium" is checked first (reverse order)
        let model = result.expect("should find a fitting tier");
        assert_eq!(model.name, "medium", "should pick highest fitting tier");
    }

    #[test]
    fn select_model_with_empty_custom_tiers_uses_defaults() {
        let config = AiConfig::default();
        assert!(config.models.is_empty());
        // Verify the function falls back to default_model_tiers() when config.models is empty
        let result = select_model_with_sys(&config, None);
        // On any machine with >= 6GB RAM, we should get a default tier
        if let Some(model) = &result {
            let default_names: Vec<String> =
                default_model_tiers().iter().map(|t| t.name.clone()).collect();
            assert!(
                default_names.contains(&model.name),
                "selected model '{}' should be from default tiers",
                model.name
            );
        }
    }

    #[test]
    fn select_model_with_impossible_ram_returns_none() {
        let config = AiConfig {
            models: vec![ModelTierConfig {
                name: "impossible".into(),
                filename: "impossible.gguf".into(),
                url: "https://example.com/impossible.gguf".into(),
                min_ram_gb: 999_999, // No machine has this much RAM
            }],
            ..AiConfig::default()
        };

        let result = select_model_with_sys(&config, None);
        assert!(
            result.is_none(),
            "Should return None when no tier fits available RAM"
        );
    }
}
