use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Schema version for config file migration
const CURRENT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,

    #[serde(default)]
    pub watch: WatchConfig,

    #[serde(default)]
    pub compaction: CompactionConfig,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub ai: AiConfig,

    #[serde(default)]
    pub update: UpdateConfig,
}

fn default_schema_version() -> u32 {
    CURRENT_SCHEMA_VERSION
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchConfig {
    /// Seconds of quiet before creating a snapshot
    #[serde(default = "default_debounce_quiet_secs")]
    pub debounce_quiet_secs: u64,

    /// Minimum seconds between snapshots per project
    #[serde(default = "default_min_snapshot_interval_secs")]
    pub min_snapshot_interval_secs: u64,

    /// Maximum seconds to wait during continuous changes before forcing a snapshot
    #[serde(default = "default_max_debounce_secs")]
    pub max_debounce_secs: u64,

    /// Fraction of tracked files deleted to trigger emergency snapshot
    #[serde(default = "default_emergency_delete_threshold")]
    pub emergency_delete_threshold: f64,

    /// Minimum number of deleted files to trigger emergency (avoids small-project false positives)
    #[serde(default = "default_emergency_delete_min_files")]
    pub emergency_delete_min_files: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    #[serde(default = "default_keep_all_minutes")]
    pub keep_all_minutes: u64,
    #[serde(default = "default_keep_5min_days")]
    pub keep_5min_days: u64,
    #[serde(default = "default_keep_hourly_days")]
    pub keep_hourly_days: u64,
    #[serde(default = "default_keep_daily_days")]
    pub keep_daily_days: u64,
    #[serde(default = "default_keep_weekly_beyond")]
    pub keep_weekly_beyond: bool,
    #[serde(default = "default_emergency_expire_hours")]
    pub emergency_expire_hours: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Max size (bytes) for binary files to store in CAS. Larger binaries get hash recorded only.
    #[serde(default = "default_max_binary_blob_bytes")]
    pub max_binary_blob_bytes: u64,

    /// Max size (bytes) for text files to store in CAS.
    #[serde(default = "default_max_text_blob_bytes")]
    pub max_text_blob_bytes: u64,

    /// Maximum file size (bytes) that will be fully copied into the blob store
    /// when reflink and hardlink are unavailable. Larger files will not be copied.
    #[serde(default = "default_max_copy_blob_bytes")]
    pub max_copy_blob_bytes: u64,

    /// Storage limit as fraction of watched folder size
    #[serde(default = "default_storage_limit_fraction")]
    pub storage_limit_fraction: f64,

    /// Minimum absolute storage floor (bytes) so small projects aren't starved
    #[serde(default = "default_storage_min_bytes")]
    pub storage_min_bytes: u64,

    /// Enable zstd compression for blobs (requires 'compression' feature)
    #[serde(default)]
    pub compress: bool,

    /// Zstd compression level (1-22, default 3)
    #[serde(default = "default_compress_level")]
    pub compress_level: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_skip_on_battery")]
    pub skip_on_battery: bool,
    /// Max tokens of diff context to send to local model
    #[serde(default = "default_max_context_tokens")]
    pub max_context_tokens: usize,
    /// Shut down model server after N seconds idle
    #[serde(default = "default_idle_shutdown_secs")]
    pub idle_shutdown_secs: u64,
    /// Don't start AI if less than this many GB of RAM available
    #[serde(default = "default_min_available_memory_gb")]
    pub min_available_memory_gb: u64,
    /// Configurable model tiers (override defaults)
    #[serde(default)]
    pub models: Vec<ModelTierConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelTierConfig {
    pub name: String,
    pub filename: String,
    pub url: String,
    pub min_ram_gb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateConfig {
    /// Enable auto-update checks
    #[serde(default = "default_auto_update")]
    pub auto_check: bool,
    /// Hours between update checks
    #[serde(default = "default_update_interval_hours")]
    pub check_interval_hours: u64,
}

// === Default functions ===
fn default_debounce_quiet_secs() -> u64 { 2 }
fn default_min_snapshot_interval_secs() -> u64 { 5 }
fn default_max_debounce_secs() -> u64 { 30 }
fn default_emergency_delete_threshold() -> f64 { 0.30 }
fn default_emergency_delete_min_files() -> usize { 5 }
fn default_keep_all_minutes() -> u64 { 60 }
fn default_keep_5min_days() -> u64 { 14 }
fn default_keep_hourly_days() -> u64 { 30 }
fn default_keep_daily_days() -> u64 { 180 }
fn default_keep_weekly_beyond() -> bool { true }
fn default_emergency_expire_hours() -> u64 { 48 }
pub fn default_max_binary_blob_bytes() -> u64 { 1_048_576 }
pub fn default_max_text_blob_bytes() -> u64 { 52_428_800 }
fn default_max_copy_blob_bytes() -> u64 { 50 * 1024 * 1024 }
fn default_storage_limit_fraction() -> f64 { 0.15 }
fn default_storage_min_bytes() -> u64 { 524_288_000 } // 500MB floor
fn default_compress_level() -> i32 { 3 }
fn default_skip_on_battery() -> bool { true }
fn default_max_context_tokens() -> usize { 8192 }
fn default_idle_shutdown_secs() -> u64 { 300 }
fn default_min_available_memory_gb() -> u64 { 4 }
fn default_auto_update() -> bool { true }
fn default_update_interval_hours() -> u64 { 24 }

impl Default for Config {
    fn default() -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            watch: WatchConfig::default(),
            compaction: CompactionConfig::default(),
            storage: StorageConfig::default(),
            ai: AiConfig::default(),
            update: UpdateConfig::default(),
        }
    }
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            debounce_quiet_secs: default_debounce_quiet_secs(),
            min_snapshot_interval_secs: default_min_snapshot_interval_secs(),
            max_debounce_secs: default_max_debounce_secs(),
            emergency_delete_threshold: default_emergency_delete_threshold(),
            emergency_delete_min_files: default_emergency_delete_min_files(),
        }
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            keep_all_minutes: default_keep_all_minutes(),
            keep_5min_days: default_keep_5min_days(),
            keep_hourly_days: default_keep_hourly_days(),
            keep_daily_days: default_keep_daily_days(),
            keep_weekly_beyond: default_keep_weekly_beyond(),
            emergency_expire_hours: default_emergency_expire_hours(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_binary_blob_bytes: default_max_binary_blob_bytes(),
            max_text_blob_bytes: default_max_text_blob_bytes(),
            max_copy_blob_bytes: default_max_copy_blob_bytes(),
            storage_limit_fraction: default_storage_limit_fraction(),
            storage_min_bytes: default_storage_min_bytes(),
            compress: false,
            compress_level: default_compress_level(),
        }
    }
}

impl Default for AiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            skip_on_battery: default_skip_on_battery(),
            max_context_tokens: default_max_context_tokens(),
            idle_shutdown_secs: default_idle_shutdown_secs(),
            min_available_memory_gb: default_min_available_memory_gb(),
            models: Vec::new(),
        }
    }
}

impl Default for UpdateConfig {
    fn default() -> Self {
        Self {
            auto_check: default_auto_update(),
            check_interval_hours: default_update_interval_hours(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        if path.exists() {
            let content = std::fs::read_to_string(path)
                .with_context(|| format!("Failed to read config: {}", path.display()))?;
            let config: Config = toml::from_str(&content)
                .with_context(|| format!("Failed to parse config: {}", path.display()))?;
            // Basic validation
            if config.watch.debounce_quiet_secs == 0 { anyhow::bail!("watch.debounce_quiet_secs must be > 0"); }
            if config.watch.min_snapshot_interval_secs == 0 { anyhow::bail!("watch.min_snapshot_interval_secs must be > 0"); }
            if !(0.0..=1.0).contains(&config.storage.storage_limit_fraction) || config.storage.storage_limit_fraction <= 0.0 {
                anyhow::bail!("storage.storage_limit_fraction must be in (0.0, 1.0]");
            }
            if config.compaction.emergency_expire_hours == 0 { anyhow::bail!("compaction.emergency_expire_hours must be > 0"); }
            if config.ai.max_context_tokens == 0 { anyhow::bail!("ai.max_context_tokens must be > 0"); }
            if !(1..=22).contains(&config.storage.compress_level) { anyhow::bail!("storage.compress_level must be between 1 and 22"); }
            Ok(config)
        } else {
            let config = Config::default();
            let content = toml::to_string_pretty(&config)?;
            // Attempt to write default config but don't fail if we can't
            if let Err(e) = std::fs::write(path, &content) {
                tracing::warn!("Could not write default config: {}", e);
            }
            Ok(config)
        }
    }
}
