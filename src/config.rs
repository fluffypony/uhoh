use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Schema version for config file migration
const CURRENT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_schema_version")]
    /// Schema version of the config file (restart not required)
    pub schema_version: u32,

    #[serde(default)]
    /// Watch configuration. Hot-reload applies to debounce_quiet_secs only; other fields require daemon restart.
    pub watch: WatchConfig,

    #[serde(default)]
    /// Compaction configuration (requires daemon restart to take effect).
    pub compaction: CompactionConfig,

    #[serde(default)]
    /// Storage configuration (requires daemon restart to take effect).
    pub storage: StorageConfig,

    #[serde(default)]
    /// AI configuration. Hot-reload does not reconfigure models or sidecar; restart recommended for changes.
    pub ai: AiConfig,

    #[serde(default)]
    /// Update configuration. Hot-reload applies to check_interval_hours; other fields require restart.
    pub update: UpdateConfig,

    #[serde(default)]
    /// Unified localhost server (MCP + REST + UI)
    pub server: ServerConfig,

    #[serde(default)]
    /// Sidecar updater policy for llama.cpp artifacts.
    pub sidecar_update: SidecarUpdateConfig,

    #[serde(default)]
    /// Unified notification sinks for high-signal events.
    pub notifications: NotificationsConfig,

    #[serde(default)]
    /// Emergency database guard settings.
    pub db_guard: DbGuardConfig,

    #[serde(default)]
    /// Agent monitoring and interception settings.
    pub agent: AgentConfig,
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
    /// Requires daemon restart to take effect.
    pub min_snapshot_interval_secs: u64,

    /// Maximum seconds to wait during continuous changes before forcing a snapshot
    #[serde(default = "default_max_debounce_secs")]
    /// Requires daemon restart to take effect.
    pub max_debounce_secs: u64,

    /// Fraction of tracked files deleted to trigger emergency snapshot
    #[serde(default = "default_emergency_delete_threshold")]
    /// Requires daemon restart to take effect.
    pub emergency_delete_threshold: f64,

    /// Minimum number of deleted files to trigger emergency (avoids small-project false positives)
    #[serde(default = "default_emergency_delete_min_files")]
    /// Requires daemon restart to take effect.
    pub emergency_delete_min_files: usize,

    /// Cooldown in seconds between emergency snapshots per project.
    /// Prevents snapshot spam during sustained delete bursts.
    #[serde(default = "default_emergency_cooldown_secs")]
    pub emergency_cooldown_secs: u64,
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
    /// Requires daemon restart to take effect.
    pub max_binary_blob_bytes: u64,

    /// Max size (bytes) for text files to store in CAS.
    #[serde(default = "default_max_text_blob_bytes")]
    /// Requires daemon restart to take effect.
    pub max_text_blob_bytes: u64,

    /// Maximum file size (bytes) that will be fully copied into the blob store
    /// when reflink and hardlink are unavailable. Larger files will not be copied.
    #[serde(default = "default_max_copy_blob_bytes")]
    /// Requires daemon restart to take effect.
    pub max_copy_blob_bytes: u64,

    /// Storage limit as fraction of watched folder size
    #[serde(default = "default_storage_limit_fraction")]
    /// Requires daemon restart to take effect.
    pub storage_limit_fraction: f64,

    /// Minimum absolute storage floor (bytes) so small projects aren't starved
    #[serde(default = "default_storage_min_bytes")]
    /// Requires daemon restart to take effect.
    pub storage_min_bytes: u64,

    /// Enable zstd compression for blobs (requires 'compression' feature)
    #[serde(default)]
    /// Requires daemon restart to take effect.
    pub compress: bool,

    /// Zstd compression level (1-22, default 3)
    #[serde(default = "default_compress_level")]
    /// Requires daemon restart to take effect.
    pub compress_level: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiConfig {
    #[serde(default)]
    /// Enable AI (requires daemon restart to change the sidecar/model state)
    pub enabled: bool,
    #[serde(default = "default_skip_on_battery")]
    /// Skip on battery (restart recommended)
    pub skip_on_battery: bool,
    /// Max tokens of diff context to send to local model
    #[serde(default = "default_max_context_tokens")]
    /// Restart recommended to apply widely
    pub max_context_tokens: usize,
    /// Shut down model server after N seconds idle
    #[serde(default = "default_idle_shutdown_secs")]
    /// Restart recommended
    pub idle_shutdown_secs: u64,
    /// Don't start AI if less than this many GB of RAM available
    #[serde(default = "default_min_available_memory_gb")]
    /// Restart recommended
    pub min_available_memory_gb: u64,
    /// Configurable model tiers (override defaults)
    #[serde(default)]
    /// Requires daemon restart to take effect.
    pub models: Vec<ModelTierConfig>,

    #[serde(default)]
    /// MLX package update policy and runtime environment.
    pub mlx: MlxConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MlxConfig {
    #[serde(default = "default_true")]
    pub auto_update: bool,
    #[serde(default = "default_mlx_check_interval_hours")]
    pub check_interval_hours: u64,
    #[serde(default)]
    pub python_path: String,
    #[serde(default = "default_mlx_venv_path")]
    pub venv_path: String,
    #[serde(default)]
    pub max_version: Option<String>,
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
    /// Requires daemon restart to take effect.
    pub auto_check: bool,
    /// Hours between update checks
    #[serde(default = "default_update_interval_hours")]
    /// Hot-reload applies to this field; others require restart.
    pub check_interval_hours: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Enable the unified HTTP server.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Port to bind on localhost.
    #[serde(default = "default_server_port")]
    pub port: u16,

    /// Bind address. Must stay loopback for safety.
    #[serde(default = "default_bind_address")]
    pub bind_address: String,

    /// Enable the static Time Machine UI.
    #[serde(default = "default_true")]
    pub ui_enabled: bool,

    /// Enable HTTP MCP endpoint.
    #[serde(default = "default_true")]
    pub mcp_enabled: bool,

    /// Require bearer auth on MCP HTTP endpoint.
    #[serde(default)]
    pub mcp_require_auth: bool,

    /// Require bearer auth on mutating methods.
    #[serde(default = "default_true")]
    pub require_auth: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarUpdateConfig {
    /// Check and install newer llama-server builds.
    #[serde(default = "default_true")]
    pub auto_update: bool,

    /// Hours between sidecar update checks.
    #[serde(default = "default_sidecar_check_hours")]
    pub check_interval_hours: u64,

    /// Optional pinned llama.cpp release tag.
    #[serde(default)]
    pub pin_version: Option<String>,

    /// GitHub repository slug for release metadata.
    #[serde(default = "default_llama_repo")]
    pub llama_repo: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationsConfig {
    #[serde(default = "default_true")]
    pub desktop: bool,
    #[serde(default)]
    pub webhook_url: String,
    #[serde(default = "default_webhook_events")]
    pub webhook_events: Vec<String>,
    #[serde(default = "default_notifications_cooldown")]
    pub cooldown_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbGuardConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_db_mass_delete_row_threshold")]
    pub mass_delete_row_threshold: u64,
    #[serde(default = "default_db_mass_delete_pct_threshold")]
    pub mass_delete_pct_threshold: f64,
    #[serde(default = "default_db_baseline_interval_hours")]
    pub baseline_interval_hours: u64,
    #[serde(default = "default_db_recovery_retention_days")]
    pub recovery_retention_days: u64,
    #[serde(default = "default_db_max_baseline_size_mb")]
    pub max_baseline_size_mb: u64,
    #[serde(default = "default_db_max_recovery_file_mb")]
    pub max_recovery_file_mb: u64,
    #[serde(default = "default_true")]
    pub encrypt_recovery: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub mcp_proxy_enabled: bool,
    #[serde(default = "default_agent_proxy_port")]
    pub mcp_proxy_port: u16,
    #[serde(default)]
    pub mcp_proxy_require_auth: bool,
    #[serde(default = "default_true")]
    pub intercept_enabled: bool,
    #[serde(default)]
    pub audit_enabled: bool,
    #[serde(default = "default_agent_audit_scope")]
    pub audit_scope: String,
    #[serde(default = "default_agent_audit_max_events_per_second")]
    pub audit_max_events_per_second: u64,
    #[serde(default)]
    pub sandbox_enabled: bool,
    #[serde(default = "default_agent_on_dangerous_change")]
    pub on_dangerous_change: String,
    #[serde(default = "default_agent_pause_timeout")]
    pub pause_timeout_seconds: u64,
    #[serde(default = "default_agent_dangerous_patterns")]
    pub dangerous_patterns: Vec<String>,
}

// === Default functions ===
fn default_debounce_quiet_secs() -> u64 {
    2
}
fn default_min_snapshot_interval_secs() -> u64 {
    5
}
fn default_max_debounce_secs() -> u64 {
    30
}
fn default_emergency_delete_threshold() -> f64 {
    0.30
}
fn default_emergency_delete_min_files() -> usize {
    5
}
fn default_emergency_cooldown_secs() -> u64 {
    120
}
fn default_keep_all_minutes() -> u64 {
    60
}
fn default_keep_5min_days() -> u64 {
    14
}
fn default_keep_hourly_days() -> u64 {
    30
}
fn default_keep_daily_days() -> u64 {
    180
}
fn default_keep_weekly_beyond() -> bool {
    true
}
fn default_emergency_expire_hours() -> u64 {
    48
}
pub fn default_max_binary_blob_bytes() -> u64 {
    1_048_576
}
pub fn default_max_text_blob_bytes() -> u64 {
    52_428_800
}
fn default_max_copy_blob_bytes() -> u64 {
    50 * 1024 * 1024
}
fn default_storage_limit_fraction() -> f64 {
    0.15
}
fn default_storage_min_bytes() -> u64 {
    524_288_000
} // 500MB floor
fn default_compress_level() -> i32 {
    3
}
fn default_skip_on_battery() -> bool {
    true
}
fn default_max_context_tokens() -> usize {
    8192
}
fn default_idle_shutdown_secs() -> u64 {
    300
}
fn default_min_available_memory_gb() -> u64 {
    4
}
fn default_auto_update() -> bool {
    true
}
fn default_update_interval_hours() -> u64 {
    24
}
fn default_true() -> bool {
    true
}
fn default_server_port() -> u16 {
    22822
}
fn default_bind_address() -> String {
    "127.0.0.1".to_string()
}
fn default_sidecar_check_hours() -> u64 {
    24
}
fn default_llama_repo() -> String {
    "ggml-org/llama.cpp".to_string()
}
fn default_mlx_check_interval_hours() -> u64 {
    12
}
fn default_mlx_venv_path() -> String {
    "~/.uhoh/venv/mlx".to_string()
}
fn default_webhook_events() -> Vec<String> {
    vec![
        "mass_delete".to_string(),
        "drop_table".to_string(),
        "drop_column".to_string(),
        "truncate".to_string(),
        "dangerous_agent_action".to_string(),
        "mlx_update_failed".to_string(),
        "emergency_delete_detected".to_string(),
    ]
}
fn default_notifications_cooldown() -> u64 {
    60
}
fn default_db_mass_delete_row_threshold() -> u64 {
    100
}
fn default_db_mass_delete_pct_threshold() -> f64 {
    0.05
}
fn default_db_baseline_interval_hours() -> u64 {
    6
}
fn default_db_recovery_retention_days() -> u64 {
    30
}
fn default_db_max_baseline_size_mb() -> u64 {
    500
}
fn default_db_max_recovery_file_mb() -> u64 {
    500
}
fn default_agent_proxy_port() -> u16 {
    22823
}
fn default_agent_audit_scope() -> String {
    "project".to_string()
}
fn default_agent_audit_max_events_per_second() -> u64 {
    500
}
fn default_agent_on_dangerous_change() -> String {
    "none".to_string()
}
fn default_agent_pause_timeout() -> u64 {
    300
}
fn default_agent_dangerous_patterns() -> Vec<String> {
    vec![
        "self_modify_personality".to_string(),
        "new_tool_permission".to_string(),
        "model_downgrade".to_string(),
        "mass_file_delete".to_string(),
        "config_change".to_string(),
    ]
}

impl Default for Config {
    fn default() -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            watch: WatchConfig::default(),
            compaction: CompactionConfig::default(),
            storage: StorageConfig::default(),
            ai: AiConfig::default(),
            update: UpdateConfig::default(),
            server: ServerConfig::default(),
            sidecar_update: SidecarUpdateConfig::default(),
            notifications: NotificationsConfig::default(),
            db_guard: DbGuardConfig::default(),
            agent: AgentConfig::default(),
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
            emergency_cooldown_secs: default_emergency_cooldown_secs(),
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
            mlx: MlxConfig::default(),
        }
    }
}

impl Default for MlxConfig {
    fn default() -> Self {
        Self {
            auto_update: true,
            check_interval_hours: default_mlx_check_interval_hours(),
            python_path: String::new(),
            venv_path: default_mlx_venv_path(),
            max_version: None,
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

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: default_server_port(),
            bind_address: default_bind_address(),
            ui_enabled: true,
            mcp_enabled: true,
            mcp_require_auth: false,
            require_auth: true,
        }
    }
}

impl Default for SidecarUpdateConfig {
    fn default() -> Self {
        Self {
            auto_update: true,
            check_interval_hours: default_sidecar_check_hours(),
            pin_version: None,
            llama_repo: default_llama_repo(),
        }
    }
}

impl Default for NotificationsConfig {
    fn default() -> Self {
        Self {
            desktop: true,
            webhook_url: String::new(),
            webhook_events: default_webhook_events(),
            cooldown_seconds: default_notifications_cooldown(),
        }
    }
}

impl Default for DbGuardConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mass_delete_row_threshold: default_db_mass_delete_row_threshold(),
            mass_delete_pct_threshold: default_db_mass_delete_pct_threshold(),
            baseline_interval_hours: default_db_baseline_interval_hours(),
            recovery_retention_days: default_db_recovery_retention_days(),
            max_baseline_size_mb: default_db_max_baseline_size_mb(),
            max_recovery_file_mb: default_db_max_recovery_file_mb(),
            encrypt_recovery: true,
        }
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mcp_proxy_enabled: true,
            mcp_proxy_port: default_agent_proxy_port(),
            mcp_proxy_require_auth: false,
            intercept_enabled: true,
            audit_enabled: false,
            audit_scope: default_agent_audit_scope(),
            audit_max_events_per_second: default_agent_audit_max_events_per_second(),
            sandbox_enabled: false,
            on_dangerous_change: default_agent_on_dangerous_change(),
            pause_timeout_seconds: default_agent_pause_timeout(),
            dangerous_patterns: default_agent_dangerous_patterns(),
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
            if config.watch.debounce_quiet_secs == 0 {
                anyhow::bail!("watch.debounce_quiet_secs must be > 0");
            }
            if config.watch.min_snapshot_interval_secs == 0 {
                anyhow::bail!("watch.min_snapshot_interval_secs must be > 0");
            }
            if !(0.0..=1.0).contains(&config.storage.storage_limit_fraction)
                || config.storage.storage_limit_fraction <= 0.0
            {
                anyhow::bail!("storage.storage_limit_fraction must be in (0.0, 1.0]");
            }
            if config.compaction.emergency_expire_hours == 0 {
                anyhow::bail!("compaction.emergency_expire_hours must be > 0");
            }
            if config.watch.emergency_delete_threshold <= 0.0
                || config.watch.emergency_delete_threshold > 1.0
            {
                anyhow::bail!(
                    "watch.emergency_delete_threshold must be in (0.0, 1.0], got {}",
                    config.watch.emergency_delete_threshold
                );
            }
            if config.watch.emergency_delete_min_files == 0 {
                anyhow::bail!("watch.emergency_delete_min_files must be > 0");
            }
            if config.watch.emergency_cooldown_secs == 0 {
                anyhow::bail!("watch.emergency_cooldown_secs must be > 0");
            }
            if config.ai.max_context_tokens == 0 {
                anyhow::bail!("ai.max_context_tokens must be > 0");
            }
            if !(1..=22).contains(&config.storage.compress_level) {
                anyhow::bail!("storage.compress_level must be between 1 and 22 (inclusive)");
            }
            if config.server.port == 0 {
                anyhow::bail!("server.port must be > 0");
            }
            if config.sidecar_update.check_interval_hours == 0 {
                anyhow::bail!("sidecar_update.check_interval_hours must be > 0");
            }
            if config.notifications.cooldown_seconds == 0 {
                anyhow::bail!("notifications.cooldown_seconds must be > 0");
            }
            if !(0.0..=1.0).contains(&config.db_guard.mass_delete_pct_threshold)
                || config.db_guard.mass_delete_pct_threshold <= 0.0
            {
                anyhow::bail!("db_guard.mass_delete_pct_threshold must be in (0.0, 1.0]");
            }
            if config.db_guard.mass_delete_row_threshold == 0 {
                anyhow::bail!("db_guard.mass_delete_row_threshold must be > 0");
            }
            if config.agent.mcp_proxy_port == 0 {
                anyhow::bail!("agent.mcp_proxy_port must be > 0");
            }
            if config.agent.pause_timeout_seconds == 0 {
                anyhow::bail!("agent.pause_timeout_seconds must be > 0");
            }
            if !matches!(config.agent.on_dangerous_change.as_str(), "none" | "pause") {
                anyhow::bail!("agent.on_dangerous_change must be one of: none, pause");
            }
            if config.agent.audit_scope.trim().is_empty() {
                anyhow::bail!("agent.audit_scope must not be empty");
            }
            if config.agent.audit_max_events_per_second == 0 {
                anyhow::bail!("agent.audit_max_events_per_second must be > 0");
            }
            if config.ai.mlx.check_interval_hours == 0 {
                anyhow::bail!("ai.mlx.check_interval_hours must be > 0");
            }
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

#[cfg(test)]
mod tests {
    use super::Config;
    use std::io::Write;

    #[test]
    fn invalid_agent_on_dangerous_change_is_rejected() {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        write!(
            file,
            "[agent]\non_dangerous_change = \"block\"\npause_timeout_seconds = 10\n"
        )
        .expect("write config");
        let err = Config::load(file.path()).expect_err("config must reject unknown mode");
        assert!(err
            .to_string()
            .contains("agent.on_dangerous_change must be one of: none, pause"));
    }
}
