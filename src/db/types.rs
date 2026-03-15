use rusqlite::types::Type;
use serde::Serialize;
use std::str::FromStr;

use crate::cas::StorageMethod;

fn invalid_db_text_conversion(index: usize, field: &'static str, value: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        index,
        Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid persisted {field}: {value}"),
        )),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SnapshotTrigger {
    Auto,
    Manual,
    Emergency,
    PreRestore,
    Initial,
    Mcp,
    Api,
}

impl SnapshotTrigger {
    #[must_use] 
    pub fn as_str(self) -> &'static str {
        match self {
            SnapshotTrigger::Auto => "auto",
            SnapshotTrigger::Manual => "manual",
            SnapshotTrigger::Emergency => "emergency",
            SnapshotTrigger::PreRestore => "pre-restore",
            SnapshotTrigger::Initial => "initial",
            SnapshotTrigger::Mcp => "mcp",
            SnapshotTrigger::Api => "api",
        }
    }

    #[must_use] 
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "auto" => Some(SnapshotTrigger::Auto),
            "manual" => Some(SnapshotTrigger::Manual),
            "emergency" => Some(SnapshotTrigger::Emergency),
            "pre-restore" => Some(SnapshotTrigger::PreRestore),
            "initial" => Some(SnapshotTrigger::Initial),
            "mcp" => Some(SnapshotTrigger::Mcp),
            "api" => Some(SnapshotTrigger::Api),
            _ => None,
        }
    }

    pub(crate) fn parse_persisted(value: &str, index: usize) -> rusqlite::Result<Self> {
        Self::parse(value)
            .ok_or_else(|| invalid_db_text_conversion(index, "snapshot trigger", value))
    }

    /// Returns `true` for triggers that represent explicit user action.
    #[must_use] 
    pub fn is_manual_kind(self) -> bool {
        matches!(self, SnapshotTrigger::Manual | SnapshotTrigger::Mcp | SnapshotTrigger::Api)
    }
}

impl FromStr for SnapshotTrigger {
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(value).ok_or(())
    }
}

impl std::fmt::Display for SnapshotTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for SnapshotTrigger {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProjectEntry {
    pub hash: String,
    pub current_path: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SnapshotRow {
    pub rowid: i64,
    pub snapshot_id: u64,
    pub timestamp: String,
    pub trigger: SnapshotTrigger,
    pub message: String,
    pub pinned: bool,
    pub ai_summary: Option<String>,
    pub file_count: u64,
}

#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct SnapshotSummary {
    pub rowid: i64,
    pub snapshot_id: u64,
    pub timestamp: String,
    pub trigger: SnapshotTrigger,
    pub message: String,
    pub pinned: bool,
    pub file_count: u64,
}

#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct SearchResult {
    pub snapshot_rowid: i64,
    pub snapshot_id: u64,
    pub timestamp: String,
    pub trigger: SnapshotTrigger,
    pub message: String,
    pub ai_summary: Option<String>,
    pub match_context: String,
}

#[non_exhaustive]
pub struct CreateSnapshotRow<'a> {
    pub project_hash: &'a str,
    pub snapshot_id: u64,
    pub timestamp: &'a str,
    pub trigger: SnapshotTrigger,
    pub message: &'a str,
    pub pinned: bool,
    pub files: &'a [SnapFileEntry],
    pub deleted: &'a [DeletedFile],
}

impl<'a> CreateSnapshotRow<'a> {
    #[allow(clippy::too_many_arguments)]
    #[must_use] 
    pub fn new(
        project_hash: &'a str,
        snapshot_id: u64,
        timestamp: &'a str,
        trigger: SnapshotTrigger,
        message: &'a str,
        pinned: bool,
        files: &'a [SnapFileEntry],
        deleted: &'a [DeletedFile],
    ) -> Self {
        Self {
            project_hash,
            snapshot_id,
            timestamp,
            trigger,
            message,
            pinned,
            files,
            deleted,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EventLedgerEntry {
    pub id: i64,
    pub ts: String,
    pub source: LedgerSource,
    pub event_type: LedgerEventType,
    pub severity: LedgerSeverity,
    pub project_hash: Option<String>,
    pub agent_name: Option<String>,
    pub guard_name: Option<String>,
    pub path: Option<String>,
    pub detail: Option<String>,
    pub pre_state_ref: Option<String>,
    pub post_state_ref: Option<String>,
    pub causal_parent: Option<i64>,
    pub resolved: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LedgerSeverity {
    Info,
    Warn,
    Error,
    Critical,
}

impl LedgerSeverity {
    #[must_use] 
    pub fn as_str(self) -> &'static str {
        match self {
            LedgerSeverity::Info => "info",
            LedgerSeverity::Warn => "warn",
            LedgerSeverity::Error => "error",
            LedgerSeverity::Critical => "critical",
        }
    }

    #[must_use] 
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "info" => Some(LedgerSeverity::Info),
            "warn" => Some(LedgerSeverity::Warn),
            "error" => Some(LedgerSeverity::Error),
            "critical" => Some(LedgerSeverity::Critical),
            _ => None,
        }
    }

    pub(crate) fn parse_persisted(value: &str, index: usize) -> rusqlite::Result<Self> {
        Self::parse(value)
            .ok_or_else(|| invalid_db_text_conversion(index, "ledger severity", value))
    }
}

impl FromStr for LedgerSeverity {
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(value).ok_or(())
    }
}

impl std::fmt::Display for LedgerSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for LedgerSeverity {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LedgerSource {
    Agent,
    DbGuard,
    Daemon,
    Fs,
    Mlx,
}

impl LedgerSource {
    #[must_use] 
    pub fn as_str(self) -> &'static str {
        match self {
            LedgerSource::Agent => "agent",
            LedgerSource::DbGuard => "db_guard",
            LedgerSource::Daemon => "daemon",
            LedgerSource::Fs => "fs",
            LedgerSource::Mlx => "mlx",
        }
    }

    #[must_use] 
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "agent" => Some(LedgerSource::Agent),
            "db_guard" => Some(LedgerSource::DbGuard),
            "daemon" => Some(LedgerSource::Daemon),
            "fs" => Some(LedgerSource::Fs),
            "mlx" => Some(LedgerSource::Mlx),
            _ => None,
        }
    }

    pub(crate) fn parse_persisted(value: &str, index: usize) -> rusqlite::Result<Self> {
        Self::parse(value).ok_or_else(|| invalid_db_text_conversion(index, "ledger source", value))
    }
}

impl FromStr for LedgerSource {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_lowercase();
        Self::parse(&normalized).ok_or_else(|| {
            format!(
                "invalid source '{value}'; expected one of: fs, db_guard, agent, daemon, mlx"
            )
        })
    }
}

impl std::fmt::Display for LedgerSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for LedgerSource {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

/// Typed event categories for the event ledger.
///
/// Known event types are represented as enum variants; unknown or future event
/// types round-trip through [`LedgerEventType::Other`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LedgerEventType {
    // -- Daemon / FS events --
    EmergencyDeleteDetected,
    ConfigReloadFailed,
    // -- DB Guard events --
    GuardStarted,
    GuardModeNormalized,
    GuardTickFailed,
    PostgresTick,
    PostgresBaseline,
    PostgresOptionalStepDegraded,
    PostgresWildcardTriggerReconciled,
    SqliteTick,
    SqliteDataChanged,
    SqliteBaseline,
    MysqlTick,
    MysqlPollFailed,
    DropTable,
    SchemaChange,
    MassDelete,
    // -- Agent events --
    AgentRegistered,
    ToolCall,
    SessionToolCall,
    PreNotify,
    DangerousAgentAction,
    DangerousActionTimeout,
    DangerousActionDenied,
    DangerousActionApproved,
    McpProxyStarted,
    McpProxyClientConnected,
    McpProxyConnectionFailed,
    McpProxyAcceptFailed,
    AuditTick,
    FanotifyOverflow,
    FanotifyMonitorStarted,
    FanotifyPreimage,
    FanotifyMonitorDegraded,
    /// Catch-all for event types not yet represented as variants.
    Other(String),
}

impl LedgerEventType {
    #[must_use] 
    pub fn as_str(&self) -> &str {
        match self {
            Self::EmergencyDeleteDetected => "emergency_delete_detected",
            Self::ConfigReloadFailed => "config_reload_failed",
            Self::GuardStarted => "guard_started",
            Self::GuardModeNormalized => "guard_mode_normalized",
            Self::GuardTickFailed => "guard_tick_failed",
            Self::PostgresTick => "postgres_tick",
            Self::PostgresBaseline => "postgres_baseline",
            Self::PostgresOptionalStepDegraded => "postgres_optional_step_degraded",
            Self::PostgresWildcardTriggerReconciled => "postgres_wildcard_trigger_reconciled",
            Self::SqliteTick => "sqlite_tick",
            Self::SqliteDataChanged => "sqlite_data_changed",
            Self::SqliteBaseline => "sqlite_baseline",
            Self::MysqlTick => "mysql_tick",
            Self::MysqlPollFailed => "mysql_poll_failed",
            Self::DropTable => "drop_table",
            Self::SchemaChange => "schema_change",
            Self::MassDelete => "mass_delete",
            Self::AgentRegistered => "agent_registered",
            Self::ToolCall => "tool_call",
            Self::SessionToolCall => "session_tool_call",
            Self::PreNotify => "pre_notify",
            Self::DangerousAgentAction => "dangerous_agent_action",
            Self::DangerousActionTimeout => "dangerous_action_timeout",
            Self::DangerousActionDenied => "dangerous_action_denied",
            Self::DangerousActionApproved => "dangerous_action_approved",
            Self::McpProxyStarted => "mcp_proxy_started",
            Self::McpProxyClientConnected => "mcp_proxy_client_connected",
            Self::McpProxyConnectionFailed => "mcp_proxy_connection_failed",
            Self::McpProxyAcceptFailed => "mcp_proxy_accept_failed",
            Self::AuditTick => "audit_tick",
            Self::FanotifyOverflow => "fanotify_overflow",
            Self::FanotifyMonitorStarted => "fanotify_monitor_started",
            Self::FanotifyPreimage => "fanotify_preimage",
            Self::FanotifyMonitorDegraded => "fanotify_monitor_degraded",
            Self::Other(s) => s.as_str(),
        }
    }

    #[must_use] 
    pub fn parse(value: &str) -> Self {
        match value {
            "emergency_delete_detected" => Self::EmergencyDeleteDetected,
            "config_reload_failed" => Self::ConfigReloadFailed,
            "guard_started" => Self::GuardStarted,
            "guard_mode_normalized" => Self::GuardModeNormalized,
            "guard_tick_failed" => Self::GuardTickFailed,
            "postgres_tick" => Self::PostgresTick,
            "postgres_baseline" => Self::PostgresBaseline,
            "postgres_optional_step_degraded" => Self::PostgresOptionalStepDegraded,
            "postgres_wildcard_trigger_reconciled" => Self::PostgresWildcardTriggerReconciled,
            "sqlite_tick" => Self::SqliteTick,
            "sqlite_data_changed" => Self::SqliteDataChanged,
            "sqlite_baseline" => Self::SqliteBaseline,
            "mysql_tick" => Self::MysqlTick,
            "mysql_poll_failed" => Self::MysqlPollFailed,
            "drop_table" => Self::DropTable,
            "schema_change" => Self::SchemaChange,
            "mass_delete" => Self::MassDelete,
            "agent_registered" => Self::AgentRegistered,
            "tool_call" => Self::ToolCall,
            "session_tool_call" => Self::SessionToolCall,
            "pre_notify" => Self::PreNotify,
            "dangerous_agent_action" => Self::DangerousAgentAction,
            "dangerous_action_timeout" => Self::DangerousActionTimeout,
            "dangerous_action_denied" => Self::DangerousActionDenied,
            "dangerous_action_approved" => Self::DangerousActionApproved,
            "mcp_proxy_started" => Self::McpProxyStarted,
            "mcp_proxy_client_connected" => Self::McpProxyClientConnected,
            "mcp_proxy_connection_failed" => Self::McpProxyConnectionFailed,
            "mcp_proxy_accept_failed" => Self::McpProxyAcceptFailed,
            "audit_tick" => Self::AuditTick,
            "fanotify_overflow" => Self::FanotifyOverflow,
            "fanotify_monitor_started" => Self::FanotifyMonitorStarted,
            "fanotify_preimage" => Self::FanotifyPreimage,
            "fanotify_monitor_degraded" => Self::FanotifyMonitorDegraded,
            other => Self::Other(other.to_string()),
        }
    }
}

impl FromStr for LedgerEventType {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self::parse(value))
    }
}

impl From<&str> for LedgerEventType {
    fn from(value: &str) -> Self {
        Self::parse(value)
    }
}

impl std::fmt::Display for LedgerEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DbGuardEngine {
    Sqlite,
    Postgres,
    Mysql,
}

impl DbGuardEngine {
    #[must_use] 
    pub fn as_str(self) -> &'static str {
        match self {
            DbGuardEngine::Sqlite => "sqlite",
            DbGuardEngine::Postgres => "postgres",
            DbGuardEngine::Mysql => "mysql",
        }
    }

    #[must_use] 
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "sqlite" => Some(DbGuardEngine::Sqlite),
            "postgres" => Some(DbGuardEngine::Postgres),
            "mysql" => Some(DbGuardEngine::Mysql),
            _ => None,
        }
    }
}

impl FromStr for DbGuardEngine {
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(value).ok_or(())
    }
}

impl std::fmt::Display for DbGuardEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for DbGuardEngine {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
#[clap(rename_all = "snake_case")]
pub enum DbGuardMode {
    Triggers,
    SchemaPolling,
}

impl DbGuardMode {
    #[must_use] 
    pub fn as_str(self) -> &'static str {
        match self {
            DbGuardMode::Triggers => "triggers",
            DbGuardMode::SchemaPolling => "schema_polling",
        }
    }

    #[must_use] 
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "triggers" => Some(DbGuardMode::Triggers),
            "schema_polling" => Some(DbGuardMode::SchemaPolling),
            _ => None,
        }
    }

    #[must_use] 
    pub fn eq_ignore_ascii_case(self, value: &str) -> bool {
        self.as_str().eq_ignore_ascii_case(value)
    }

}

impl FromStr for DbGuardMode {
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(value).ok_or(())
    }
}

impl std::fmt::Display for DbGuardMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for DbGuardMode {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EventLedgerTraceResult {
    pub entries: Vec<EventLedgerEntry>,
    pub truncated: bool,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NewEventLedgerEntry {
    pub source: LedgerSource,
    pub event_type: LedgerEventType,
    pub severity: LedgerSeverity,
    pub project_hash: Option<String>,
    pub agent_name: Option<String>,
    pub guard_name: Option<String>,
    pub path: Option<String>,
    pub detail: Option<String>,
    pub pre_state_ref: Option<String>,
    pub post_state_ref: Option<String>,
    pub causal_parent: Option<i64>,
    pub prev_hash: Option<String>,
}

#[non_exhaustive]
pub struct DbGuardRegistration<'a> {
    pub name: &'a str,
    pub engine: DbGuardEngine,
    pub connection_ref: &'a str,
    pub tables_csv: &'a str,
    pub watched_tables_cache: Option<&'a str>,
    pub mode: DbGuardMode,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DbGuardEntry {
    pub id: i64,
    pub name: String,
    pub engine: DbGuardEngine,
    pub connection_ref: String,
    pub tables_csv: String,
    pub watched_tables_cache: Option<String>,
    pub mode: DbGuardMode,
    pub created_at: String,
    pub last_baseline_at: Option<String>,
    pub active: bool,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AgentEntry {
    pub id: i64,
    pub name: String,
    pub profile_path: String,
    pub data_dir: Option<String>,
    pub registered_at: String,
    pub active: bool,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FileEntryRow {
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub stored: bool,
    pub executable: bool,
    pub mtime: Option<i64>,
    pub storage_method: StorageMethod,
    pub is_symlink: bool,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct OperationListRow {
    pub id: i64,
    pub label: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub first_snapshot_id: Option<u64>,
    pub last_snapshot_id: Option<u64>,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PendingAiSummaryRow {
    pub snapshot_rowid: i64,
    pub project_hash: String,
    pub attempts: i64,
    pub queued_at: String,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ActiveOperationRow {
    pub id: i64,
    pub label: String,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CompletedOperationRow {
    pub id: i64,
    pub label: String,
    pub first_snapshot_id: u64,
    pub last_snapshot_id: u64,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FileHistoryRow {
    pub snapshot_id: u64,
    pub timestamp: String,
    pub hash: String,
    pub trigger: SnapshotTrigger,
}

// Types used around snapshot creation
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SnapFileEntry {
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub stored: bool,
    pub executable: bool,
    pub mtime: Option<i64>,
    pub storage_method: StorageMethod,
    pub is_symlink: bool,
}

impl SnapFileEntry {
    #[allow(clippy::too_many_arguments)]
    #[must_use] 
    pub fn new(
        path: String,
        hash: String,
        size: u64,
        stored: bool,
        executable: bool,
        mtime: Option<i64>,
        storage_method: StorageMethod,
        is_symlink: bool,
    ) -> Self {
        Self {
            path,
            hash,
            size,
            stored,
            executable,
            mtime,
            storage_method,
            is_symlink,
        }
    }
}
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeletedFile {
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub stored: bool,
    pub storage_method: StorageMethod,
}
