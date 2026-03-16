use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[non_exhaustive]
pub struct Config {
    #[serde(default)]
    /// Watch configuration. Hot-reload applies to `debounce_quiet_secs` only; other fields require daemon restart.
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
    /// Update configuration. Hot-reload applies to `check_interval_hours`; other fields require restart.
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
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

    /// Fraction of tracked files (from previous snapshot manifest) deleted to
    /// trigger emergency snapshot.
    /// Both this threshold and `emergency_delete_min_files` must be met.
    /// Range: (0.0, 1.0].
    #[serde(default = "default_emergency_delete_threshold")]
    /// Requires daemon restart to take effect.
    pub emergency_delete_threshold: f64,

    /// Minimum absolute deleted-file count required to trigger emergency
    /// (avoids small-project false positives).
    /// Must be > 0.
    #[serde(default = "default_emergency_delete_min_files")]
    /// Requires daemon restart to take effect.
    pub emergency_delete_min_files: usize,

    /// Cooldown in seconds between emergency snapshots per project.
    /// Prevents snapshot spam during sustained delete bursts.
    /// Must be > 0.
    #[serde(default = "default_emergency_cooldown_secs")]
    /// Requires daemon restart to take effect.
    pub emergency_cooldown_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
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

impl CompactionConfig {
    #[must_use] 
    pub fn new(
        keep_all_minutes: u64,
        keep_5min_days: u64,
        keep_hourly_days: u64,
        keep_daily_days: u64,
        keep_weekly_beyond: bool,
        emergency_expire_hours: u64,
    ) -> Self {
        Self {
            keep_all_minutes,
            keep_5min_days,
            keep_hourly_days,
            keep_daily_days,
            keep_weekly_beyond,
            emergency_expire_hours,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
pub struct ModelTierConfig {
    pub name: String,
    pub filename: String,
    pub url: String,
    pub min_ram_gb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
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
#[non_exhaustive]
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
    #[serde(default = "default_true")]
    pub mcp_require_auth: bool,

    /// Require bearer auth on mutating methods.
    #[serde(default = "default_true")]
    pub require_auth: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
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
#[non_exhaustive]
pub struct NotificationsConfig {
    #[serde(default = "default_true")]
    pub desktop: bool,
    #[serde(default)]
    pub webhook_url: String,
    #[serde(default = "default_webhook_events")]
    pub webhook_events: Vec<WebhookEventKind>,
    #[serde(default = "default_notifications_cooldown")]
    pub cooldown_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEventKind {
    MassDelete,
    MassDeletePct,
    DropTable,
    DropColumn,
    Truncate,
    DangerousAgentAction,
    MlxUpdateFailed,
    EmergencyDeleteDetected,
}

impl WebhookEventKind {
    #[must_use] 
    pub fn as_str(&self) -> &'static str {
        match self {
            WebhookEventKind::MassDelete => "mass_delete",
            WebhookEventKind::MassDeletePct => "mass_delete_pct",
            WebhookEventKind::DropTable => "drop_table",
            WebhookEventKind::DropColumn => "drop_column",
            WebhookEventKind::Truncate => "truncate",
            WebhookEventKind::DangerousAgentAction => "dangerous_agent_action",
            WebhookEventKind::MlxUpdateFailed => "mlx_update_failed",
            WebhookEventKind::EmergencyDeleteDetected => "emergency_delete_detected",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentAuditScope {
    Project,
    Home,
}

impl AgentAuditScope {
    #[must_use] 
    pub fn is_home(self) -> bool {
        matches!(self, AgentAuditScope::Home)
    }
}

impl std::fmt::Display for AgentAuditScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            AgentAuditScope::Project => "project",
            AgentAuditScope::Home => "home",
        };
        f.write_str(value)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DangerousChangePolicy {
    None,
    Pause,
}

impl DangerousChangePolicy {
    #[must_use] 
    pub fn as_str(self) -> &'static str {
        match self {
            DangerousChangePolicy::None => "none",
            DangerousChangePolicy::Pause => "pause",
        }
    }
}

impl std::fmt::Display for DangerousChangePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
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
#[non_exhaustive]
pub struct AgentConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub mcp_proxy_enabled: bool,
    #[serde(default = "default_agent_proxy_port")]
    pub mcp_proxy_port: u16,
    #[serde(default = "default_true")]
    pub mcp_proxy_require_auth: bool,
    #[serde(default = "default_true")]
    pub intercept_enabled: bool,
    #[serde(default)]
    pub audit_enabled: bool,
    #[serde(default = "default_agent_audit_scope")]
    pub audit_scope: AgentAuditScope,
    #[serde(default = "default_agent_audit_max_events_per_second")]
    pub audit_max_events_per_second: u64,
    #[serde(default)]
    pub sandbox_enabled: bool,
    #[serde(default = "default_agent_on_dangerous_change")]
    pub on_dangerous_change: DangerousChangePolicy,
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
#[must_use] 
pub fn default_max_binary_blob_bytes() -> u64 {
    1_048_576
}
#[must_use] 
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
fn default_webhook_events() -> Vec<WebhookEventKind> {
    vec![
        WebhookEventKind::MassDelete,
        WebhookEventKind::MassDeletePct,
        WebhookEventKind::DropTable,
        WebhookEventKind::DropColumn,
        WebhookEventKind::Truncate,
        WebhookEventKind::DangerousAgentAction,
        WebhookEventKind::MlxUpdateFailed,
        WebhookEventKind::EmergencyDeleteDetected,
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
fn default_agent_audit_scope() -> AgentAuditScope {
    AgentAuditScope::Project
}
fn default_agent_audit_max_events_per_second() -> u64 {
    500
}
fn default_agent_on_dangerous_change() -> DangerousChangePolicy {
    DangerousChangePolicy::None
}
fn default_agent_pause_timeout() -> u64 {
    300
}
fn default_agent_dangerous_patterns() -> Vec<String> {
    vec![
        "tool:write".to_string(),
        "tool:apply_patch".to_string(),
        "tool:exec".to_string(),
        "tool:bash".to_string(),
        "path:.env".to_string(),
        "path:package.json".to_string(),
        "path:Cargo.toml".to_string(),
    ]
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
            mcp_require_auth: true,
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
            mcp_proxy_require_auth: true,
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
    /// Load configuration from `path`, returning a default config if the file does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read, the TOML cannot be parsed, or the
    /// parsed values fail validation.
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Config::default());
        }

        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config: {}", path.display()))?;
        let config: Config = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config: {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    /// Load an existing config or write a default one if none exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the existing config cannot be read or parsed, or if writing
    /// the default config file fails.
    pub fn load_or_initialize(path: &Path) -> Result<Self> {
        if path.exists() {
            return Self::load(path);
        }

        let config = Self::default();
        Self::write_default(path, &config)?;
        Ok(config)
    }

    /// Serialize `config` to TOML and write it to `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if TOML serialization fails or the file cannot be written.
    pub fn write_default(path: &Path, config: &Self) -> Result<()> {
        let content = toml::to_string_pretty(config)?;
        std::fs::write(path, content)
            .with_context(|| format!("Failed to write default config: {}", path.display()))
    }

    fn validate(&self) -> Result<()> {
        if self.watch.debounce_quiet_secs == 0 {
            anyhow::bail!("watch.debounce_quiet_secs must be > 0");
        }
        if self.watch.min_snapshot_interval_secs == 0 {
            anyhow::bail!("watch.min_snapshot_interval_secs must be > 0");
        }
        if !(0.0..=1.0).contains(&self.storage.storage_limit_fraction)
            || self.storage.storage_limit_fraction <= 0.0
        {
            anyhow::bail!("storage.storage_limit_fraction must be in (0.0, 1.0]");
        }
        if self.compaction.emergency_expire_hours == 0 {
            anyhow::bail!("compaction.emergency_expire_hours must be > 0");
        }
        if self.watch.emergency_delete_threshold <= 0.0
            || self.watch.emergency_delete_threshold > 1.0
        {
            anyhow::bail!(
                "watch.emergency_delete_threshold must be in (0.0, 1.0], got {}",
                self.watch.emergency_delete_threshold
            );
        }
        if self.watch.emergency_delete_min_files == 0 {
            anyhow::bail!("watch.emergency_delete_min_files must be > 0");
        }
        if self.watch.emergency_cooldown_secs == 0 {
            anyhow::bail!("watch.emergency_cooldown_secs must be > 0");
        }
        if self.ai.max_context_tokens == 0 {
            anyhow::bail!("ai.max_context_tokens must be > 0");
        }
        if !(1..=22).contains(&self.storage.compress_level) {
            anyhow::bail!("storage.compress_level must be between 1 and 22 (inclusive)");
        }
        if self.server.port == 0 {
            anyhow::bail!("server.port must be > 0");
        }
        if self.sidecar_update.check_interval_hours == 0 {
            anyhow::bail!("sidecar_update.check_interval_hours must be > 0");
        }
        if self.notifications.cooldown_seconds == 0 {
            anyhow::bail!("notifications.cooldown_seconds must be > 0");
        }
        if !(0.0..=1.0).contains(&self.db_guard.mass_delete_pct_threshold)
            || self.db_guard.mass_delete_pct_threshold <= 0.0
        {
            anyhow::bail!("db_guard.mass_delete_pct_threshold must be in (0.0, 1.0]");
        }
        if self.db_guard.mass_delete_row_threshold == 0 {
            anyhow::bail!("db_guard.mass_delete_row_threshold must be > 0");
        }
        if self.agent.mcp_proxy_port == 0 {
            anyhow::bail!("agent.mcp_proxy_port must be > 0");
        }
        if self.agent.pause_timeout_seconds == 0 {
            anyhow::bail!("agent.pause_timeout_seconds must be > 0");
        }
        if self.agent.audit_max_events_per_second == 0 {
            anyhow::bail!("agent.audit_max_events_per_second must be > 0");
        }
        if self.ai.mlx.check_interval_hours == 0 {
            anyhow::bail!("ai.mlx.check_interval_hours must be > 0");
        }
        if self.storage.max_binary_blob_bytes == 0 {
            anyhow::bail!("storage.max_binary_blob_bytes must be > 0");
        }
        if self.storage.max_text_blob_bytes == 0 {
            anyhow::bail!("storage.max_text_blob_bytes must be > 0");
        }
        if self.storage.max_copy_blob_bytes == 0 {
            anyhow::bail!("storage.max_copy_blob_bytes must be > 0");
        }
        if self.watch.max_debounce_secs == 0 {
            anyhow::bail!("watch.max_debounce_secs must be > 0");
        }
        if self.watch.max_debounce_secs < self.watch.debounce_quiet_secs {
            anyhow::bail!("watch.max_debounce_secs must be >= watch.debounce_quiet_secs");
        }
        if self.storage.storage_min_bytes == 0 {
            anyhow::bail!("storage.storage_min_bytes must be > 0");
        }
        Ok(())
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    // === Default value tests ===

    #[test]
    fn default_config_passes_validation() {
        let cfg = Config::default();
        cfg.validate().expect("default config must pass validation");
    }

    #[test]
    fn default_watch_values() {
        let w = WatchConfig::default();
        assert_eq!(w.debounce_quiet_secs, 2);
        assert_eq!(w.min_snapshot_interval_secs, 5);
        assert_eq!(w.max_debounce_secs, 30);
        assert!((w.emergency_delete_threshold - 0.30).abs() < f64::EPSILON);
        assert_eq!(w.emergency_delete_min_files, 5);
        assert_eq!(w.emergency_cooldown_secs, 120);
    }

    #[test]
    fn default_compaction_values() {
        let c = CompactionConfig::default();
        assert_eq!(c.keep_all_minutes, 60);
        assert_eq!(c.keep_5min_days, 14);
        assert_eq!(c.keep_hourly_days, 30);
        assert_eq!(c.keep_daily_days, 180);
        assert!(c.keep_weekly_beyond);
        assert_eq!(c.emergency_expire_hours, 48);
    }

    #[test]
    fn default_storage_values() {
        let s = StorageConfig::default();
        assert_eq!(s.max_binary_blob_bytes, 1_048_576);
        assert_eq!(s.max_text_blob_bytes, 52_428_800);
        assert_eq!(s.max_copy_blob_bytes, 50 * 1024 * 1024);
        assert!((s.storage_limit_fraction - 0.15).abs() < f64::EPSILON);
        assert_eq!(s.storage_min_bytes, 524_288_000);
        assert!(!s.compress);
        assert_eq!(s.compress_level, 3);
    }

    #[test]
    fn default_server_values() {
        let s = ServerConfig::default();
        assert!(s.enabled);
        assert_eq!(s.port, 22822);
        assert_eq!(s.bind_address, "127.0.0.1");
        assert!(s.ui_enabled);
        assert!(s.mcp_enabled);
        assert!(s.mcp_require_auth);
        assert!(s.require_auth);
    }

    #[test]
    fn default_ai_values() {
        let a = AiConfig::default();
        assert!(!a.enabled);
        assert!(a.skip_on_battery);
        assert_eq!(a.max_context_tokens, 8192);
        assert_eq!(a.idle_shutdown_secs, 300);
        assert_eq!(a.min_available_memory_gb, 4);
        assert!(a.models.is_empty());
    }

    #[test]
    fn default_agent_values() {
        let a = AgentConfig::default();
        assert!(!a.enabled);
        assert!(a.mcp_proxy_enabled);
        assert_eq!(a.mcp_proxy_port, 22823);
        assert!(a.intercept_enabled);
        assert!(!a.audit_enabled);
        assert_eq!(a.audit_scope, AgentAuditScope::Project);
        assert_eq!(a.on_dangerous_change, DangerousChangePolicy::None);
        assert_eq!(a.pause_timeout_seconds, 300);
        assert!(!a.dangerous_patterns.is_empty());
    }

    // === Validation tests ===

    #[test]
    fn validate_rejects_zero_debounce_quiet_secs() {
        let mut cfg = Config::default();
        cfg.watch.debounce_quiet_secs = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("debounce_quiet_secs must be > 0"));
    }

    #[test]
    fn validate_rejects_zero_min_snapshot_interval() {
        let mut cfg = Config::default();
        cfg.watch.min_snapshot_interval_secs = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("min_snapshot_interval_secs must be > 0"));
    }

    #[test]
    fn validate_rejects_zero_storage_limit_fraction() {
        let mut cfg = Config::default();
        cfg.storage.storage_limit_fraction = 0.0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("storage_limit_fraction"));
    }

    #[test]
    fn validate_rejects_negative_storage_limit_fraction() {
        let mut cfg = Config::default();
        cfg.storage.storage_limit_fraction = -0.1;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("storage_limit_fraction"));
    }

    #[test]
    fn validate_rejects_storage_limit_fraction_above_one() {
        let mut cfg = Config::default();
        cfg.storage.storage_limit_fraction = 1.1;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("storage_limit_fraction"));
    }

    #[test]
    fn validate_accepts_storage_limit_fraction_at_one() {
        let mut cfg = Config::default();
        cfg.storage.storage_limit_fraction = 1.0;
        cfg.validate().expect("1.0 should be valid");
    }

    #[test]
    fn validate_rejects_zero_emergency_expire_hours() {
        let mut cfg = Config::default();
        cfg.compaction.emergency_expire_hours = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("emergency_expire_hours must be > 0"));
    }

    #[test]
    fn validate_rejects_emergency_delete_threshold_zero() {
        let mut cfg = Config::default();
        cfg.watch.emergency_delete_threshold = 0.0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("emergency_delete_threshold"));
    }

    #[test]
    fn validate_rejects_emergency_delete_threshold_above_one() {
        let mut cfg = Config::default();
        cfg.watch.emergency_delete_threshold = 1.5;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("emergency_delete_threshold"));
    }

    #[test]
    fn validate_accepts_emergency_delete_threshold_at_one() {
        let mut cfg = Config::default();
        cfg.watch.emergency_delete_threshold = 1.0;
        cfg.validate().expect("1.0 should be valid");
    }

    #[test]
    fn validate_rejects_zero_emergency_delete_min_files() {
        let mut cfg = Config::default();
        cfg.watch.emergency_delete_min_files = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("emergency_delete_min_files must be > 0"));
    }

    #[test]
    fn validate_rejects_zero_emergency_cooldown_secs() {
        let mut cfg = Config::default();
        cfg.watch.emergency_cooldown_secs = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("emergency_cooldown_secs must be > 0"));
    }

    #[test]
    fn validate_rejects_zero_max_context_tokens() {
        let mut cfg = Config::default();
        cfg.ai.max_context_tokens = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("max_context_tokens must be > 0"));
    }

    #[test]
    fn validate_rejects_compress_level_out_of_range() {
        let mut cfg = Config::default();
        cfg.storage.compress_level = 0;
        assert!(cfg.validate().is_err());

        cfg.storage.compress_level = 23;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_compress_level_boundaries() {
        let mut cfg = Config::default();
        cfg.storage.compress_level = 1;
        cfg.validate().expect("1 should be valid");

        cfg.storage.compress_level = 22;
        cfg.validate().expect("22 should be valid");
    }

    #[test]
    fn validate_rejects_zero_server_port() {
        let mut cfg = Config::default();
        cfg.server.port = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("server.port must be > 0"));
    }

    #[test]
    fn validate_rejects_zero_sidecar_check_interval() {
        let mut cfg = Config::default();
        cfg.sidecar_update.check_interval_hours = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("sidecar_update.check_interval_hours must be > 0"));
    }

    #[test]
    fn validate_rejects_zero_notifications_cooldown() {
        let mut cfg = Config::default();
        cfg.notifications.cooldown_seconds = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("notifications.cooldown_seconds must be > 0"));
    }

    #[test]
    fn validate_rejects_zero_db_guard_row_threshold() {
        let mut cfg = Config::default();
        cfg.db_guard.mass_delete_row_threshold = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("mass_delete_row_threshold must be > 0"));
    }

    #[test]
    fn validate_rejects_db_guard_pct_threshold_zero() {
        let mut cfg = Config::default();
        cfg.db_guard.mass_delete_pct_threshold = 0.0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_agent_proxy_port() {
        let mut cfg = Config::default();
        cfg.agent.mcp_proxy_port = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("agent.mcp_proxy_port must be > 0"));
    }

    #[test]
    fn validate_rejects_zero_pause_timeout() {
        let mut cfg = Config::default();
        cfg.agent.pause_timeout_seconds = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("agent.pause_timeout_seconds must be > 0"));
    }

    #[test]
    fn validate_rejects_zero_audit_max_events() {
        let mut cfg = Config::default();
        cfg.agent.audit_max_events_per_second = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("audit_max_events_per_second must be > 0"));
    }

    #[test]
    fn validate_rejects_zero_mlx_check_interval() {
        let mut cfg = Config::default();
        cfg.ai.mlx.check_interval_hours = 0;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("ai.mlx.check_interval_hours must be > 0"));
    }

    #[test]
    fn validate_rejects_zero_max_binary_blob_bytes() {
        let mut cfg = Config::default();
        cfg.storage.max_binary_blob_bytes = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_max_text_blob_bytes() {
        let mut cfg = Config::default();
        cfg.storage.max_text_blob_bytes = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_max_copy_blob_bytes() {
        let mut cfg = Config::default();
        cfg.storage.max_copy_blob_bytes = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_max_debounce_less_than_quiet() {
        let mut cfg = Config::default();
        cfg.watch.debounce_quiet_secs = 10;
        cfg.watch.max_debounce_secs = 5;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("max_debounce_secs must be >= watch.debounce_quiet_secs"));
    }

    #[test]
    fn validate_rejects_zero_storage_min_bytes() {
        let mut cfg = Config::default();
        cfg.storage.storage_min_bytes = 0;
        assert!(cfg.validate().is_err());
    }

    // === Serialization / deserialization tests ===

    #[test]
    fn config_roundtrip_toml() {
        let cfg = Config::default();
        let serialized = toml::to_string_pretty(&cfg).expect("serialize");
        let deserialized: Config = toml::from_str(&serialized).expect("deserialize");
        deserialized.validate().expect("roundtripped config must be valid");
        assert_eq!(deserialized.watch.debounce_quiet_secs, cfg.watch.debounce_quiet_secs);
        assert_eq!(deserialized.server.port, cfg.server.port);
    }

    #[test]
    fn config_from_empty_toml() {
        let cfg: Config = toml::from_str("").expect("empty toml should parse");
        cfg.validate().expect("empty toml should use valid defaults");
    }

    #[test]
    fn config_partial_toml_uses_defaults() {
        let toml_str = "[watch]\ndebounce_quiet_secs = 5\n";
        let cfg: Config = toml::from_str(toml_str).expect("partial toml should parse");
        assert_eq!(cfg.watch.debounce_quiet_secs, 5);
        // Other fields should have defaults
        assert_eq!(cfg.watch.min_snapshot_interval_secs, 5);
        assert_eq!(cfg.server.port, 22822);
    }

    #[test]
    fn invalid_agent_on_dangerous_change_is_rejected() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().join("config.toml");
        std::fs::write(
            &path,
            "[watch]\ndebounce_quiet_secs = 2\n\n[agent]\non_dangerous_change = \"block\"\npause_timeout_seconds = 10\n",
        )
        .expect("write config");
        let err = Config::load(&path).expect_err("config must reject unknown mode");
        assert!(err.to_string().contains("Failed to parse config"));
    }

    #[test]
    fn dangerous_change_policy_default_is_none() {
        let cfg = Config::default();
        assert_eq!(cfg.agent.on_dangerous_change, DangerousChangePolicy::None);
    }

    #[test]
    fn load_missing_config_does_not_create_file() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().join("config.toml");

        let cfg = Config::load(&path).expect("load default config");

        assert_eq!(cfg.ai.enabled, Config::default().ai.enabled);
        assert!(!path.exists());
    }

    // === Enum display / as_str tests ===

    #[test]
    fn webhook_event_kind_as_str() {
        assert_eq!(WebhookEventKind::MassDelete.as_str(), "mass_delete");
        assert_eq!(WebhookEventKind::DropTable.as_str(), "drop_table");
        assert_eq!(WebhookEventKind::DropColumn.as_str(), "drop_column");
        assert_eq!(WebhookEventKind::Truncate.as_str(), "truncate");
        assert_eq!(WebhookEventKind::DangerousAgentAction.as_str(), "dangerous_agent_action");
        assert_eq!(WebhookEventKind::MlxUpdateFailed.as_str(), "mlx_update_failed");
        assert_eq!(WebhookEventKind::EmergencyDeleteDetected.as_str(), "emergency_delete_detected");
        assert_eq!(WebhookEventKind::MassDeletePct.as_str(), "mass_delete_pct");
    }

    #[test]
    fn dangerous_change_policy_as_str() {
        assert_eq!(DangerousChangePolicy::None.as_str(), "none");
        assert_eq!(DangerousChangePolicy::Pause.as_str(), "pause");
    }

    #[test]
    fn dangerous_change_policy_display() {
        assert_eq!(format!("{}", DangerousChangePolicy::None), "none");
        assert_eq!(format!("{}", DangerousChangePolicy::Pause), "pause");
    }

    #[test]
    fn agent_audit_scope_display() {
        assert_eq!(format!("{}", AgentAuditScope::Project), "project");
        assert_eq!(format!("{}", AgentAuditScope::Home), "home");
    }

    #[test]
    fn agent_audit_scope_is_home() {
        assert!(!AgentAuditScope::Project.is_home());
        assert!(AgentAuditScope::Home.is_home());
    }

    #[test]
    fn webhook_event_kind_serde_roundtrip() {
        let kind = WebhookEventKind::MassDelete;
        let json = serde_json::to_string(&kind).expect("serialize");
        assert_eq!(json, "\"mass_delete\"");
        let back: WebhookEventKind = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, kind);
    }

    #[test]
    fn agent_audit_scope_serde_roundtrip() {
        for scope in [AgentAuditScope::Project, AgentAuditScope::Home] {
            let json = serde_json::to_string(&scope).expect("serialize");
            let back: AgentAuditScope = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, scope);
        }
    }

    // === File-based config tests ===

    #[test]
    fn load_or_initialize_creates_file() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().join("config.toml");
        assert!(!path.exists());

        let cfg = Config::load_or_initialize(&path).expect("load_or_initialize");
        assert!(path.exists());
        assert_eq!(cfg.watch.debounce_quiet_secs, 2);
    }

    #[test]
    fn load_or_initialize_reads_existing() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let path = temp_dir.path().join("config.toml");
        std::fs::write(&path, "[watch]\ndebounce_quiet_secs = 7\n").expect("write");

        let cfg = Config::load_or_initialize(&path).expect("load_or_initialize");
        assert_eq!(cfg.watch.debounce_quiet_secs, 7);
    }

    #[test]
    fn compaction_config_new() {
        let c = CompactionConfig::new(10, 20, 30, 40, false, 50);
        assert_eq!(c.keep_all_minutes, 10);
        assert_eq!(c.keep_5min_days, 20);
        assert_eq!(c.keep_hourly_days, 30);
        assert_eq!(c.keep_daily_days, 40);
        assert!(!c.keep_weekly_beyond);
        assert_eq!(c.emergency_expire_hours, 50);
    }

    #[test]
    fn default_dangerous_patterns_not_empty() {
        let patterns = default_agent_dangerous_patterns();
        assert!(!patterns.is_empty());
        assert!(patterns.iter().any(|p| p.contains("bash")));
        assert!(patterns.iter().any(|p| p.contains(".env")));
    }

    #[test]
    fn default_webhook_events_covers_all_variants() {
        let events = default_webhook_events();
        assert_eq!(events.len(), 8);
    }
}
