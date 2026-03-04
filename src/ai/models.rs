use crate::config::{AiConfig, ModelTierConfig};

/// Default model tiers (used when config has none).
/// Model names/URLs should be verified against HuggingFace before release.
pub fn default_model_tiers() -> Vec<ModelTierConfig> {
    vec![
        ModelTierConfig {
            name: "Qwen3-8B-Q4_K_M".to_string(),
            filename: "qwen3-8b-q4_k_m.gguf".to_string(),
            url: "https://huggingface.co/unsloth/Qwen3-8B-GGUF/resolve/main/Qwen3-8B-Q4_K_M.gguf"
                .to_string(),
            min_ram_gb: 8,
        },
        ModelTierConfig {
            name: "Qwen3-8B-Q8_0".to_string(),
            filename: "qwen3-8b-q8_0.gguf".to_string(),
            url: "https://huggingface.co/unsloth/Qwen3-8B-GGUF/resolve/main/Qwen3-8B-Q8_0.gguf"
                .to_string(),
            min_ram_gb: 16,
        },
    ]
}

/// Select the best model tier based on available system RAM.
pub fn select_model(config: &AiConfig) -> Option<&ModelTierConfig> {
    let tiers = if config.models.is_empty() {
        return None; // Use default_model_tiers at call site
    } else {
        &config.models
    };

    use sysinfo::System;
    let mut sys = System::new();
    sys.refresh_memory();
    let total_ram_gb = sys.total_memory() / (1024 * 1024 * 1024);

    // Select the best model that fits in available RAM (with margin)
    tiers
        .iter()
        .rev()
        .find(|t| total_ram_gb >= t.min_ram_gb + 2) // 2GB margin for OS/apps
}
