use anyhow::{Context, Result};
use r2d2::{CustomizeConnection, Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, types::Type, Connection, OptionalExtension, Row};
use serde::Serialize;
use std::path::Path;
use std::str::FromStr;

use crate::cas::StorageMethod;

/// Thread-safe SQLite database wrapper.
/// SQLite with WAL mode handles concurrent readers and serialized writers.
pub struct Database {
    pool: Pool<SqliteConnectionManager>,
}

impl Database {
    /// Create a new Database handle sharing the same connection pool.
    /// This is cheap (just clones the Arc-wrapped pool) and avoids
    /// opening a separate pool for background threads.
    pub fn clone_handle(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

#[derive(Debug)]
struct SqliteCustomizer;

impl CustomizeConnection<Connection, rusqlite::Error> for SqliteCustomizer {
    fn on_acquire(&self, conn: &mut Connection) -> std::result::Result<(), rusqlite::Error> {
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        conn.execute_batch("PRAGMA busy_timeout=5000;")?;
        conn.execute_batch("PRAGMA auto_vacuum=INCREMENTAL;")?;
        Ok(())
    }
}

type DbConn = PooledConnection<SqliteConnectionManager>;

mod ai_queue;
mod ledger;
mod operations;
mod projects;
mod search;
mod snapshots;

pub use ledger::LedgerRecentFilters;

fn row_u64(row: &Row<'_>, index: usize, field: &'static str) -> rusqlite::Result<u64> {
    let value = row.get::<_, i64>(index)?;
    u64::try_from(value).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("negative persisted value for {field}: {value}"),
            )),
        )
    })
}

fn row_opt_u64(row: &Row<'_>, index: usize, field: &'static str) -> rusqlite::Result<Option<u64>> {
    row.get::<_, Option<i64>>(index)?
        .map(|value| {
            u64::try_from(value).map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    Type::Integer,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("negative persisted value for {field}: {value}"),
                    )),
                )
            })
        })
        .transpose()
}

fn checked_db_u64(value: i64, field: &'static str) -> Result<u64> {
    u64::try_from(value)
        .map_err(|_| anyhow::anyhow!("negative persisted value for {field}: {value}"))
}

fn checked_usize_u64(value: usize, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow::anyhow!("value too large for {field}: {value}"))
}

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

    fn parse_persisted(value: &str, index: usize) -> rusqlite::Result<Self> {
        Self::parse(value)
            .ok_or_else(|| invalid_db_text_conversion(index, "snapshot trigger", value))
    }

    /// Returns `true` for triggers that represent explicit user action.
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
    pub fn as_str(self) -> &'static str {
        match self {
            LedgerSeverity::Info => "info",
            LedgerSeverity::Warn => "warn",
            LedgerSeverity::Error => "error",
            LedgerSeverity::Critical => "critical",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "info" => Some(LedgerSeverity::Info),
            "warn" => Some(LedgerSeverity::Warn),
            "error" => Some(LedgerSeverity::Error),
            "critical" => Some(LedgerSeverity::Critical),
            _ => None,
        }
    }

    fn parse_persisted(value: &str, index: usize) -> rusqlite::Result<Self> {
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
    pub fn as_str(self) -> &'static str {
        match self {
            LedgerSource::Agent => "agent",
            LedgerSource::DbGuard => "db_guard",
            LedgerSource::Daemon => "daemon",
            LedgerSource::Fs => "fs",
            LedgerSource::Mlx => "mlx",
        }
    }

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

    fn parse_persisted(value: &str, index: usize) -> rusqlite::Result<Self> {
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
    pub fn as_str(self) -> &'static str {
        match self {
            DbGuardEngine::Sqlite => "sqlite",
            DbGuardEngine::Postgres => "postgres",
            DbGuardEngine::Mysql => "mysql",
        }
    }

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
    pub fn as_str(self) -> &'static str {
        match self {
            DbGuardMode::Triggers => "triggers",
            DbGuardMode::SchemaPolling => "schema_polling",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "triggers" => Some(DbGuardMode::Triggers),
            "schema_polling" => Some(DbGuardMode::SchemaPolling),
            _ => None,
        }
    }

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

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let manager = SqliteConnectionManager::file(path);
        let pool = Pool::builder()
            .max_size(16)
            .connection_timeout(std::time::Duration::from_secs(30))
            .connection_customizer(Box::new(SqliteCustomizer))
            .build(manager)
            .with_context(|| format!("Failed to open database pool: {}", path.display()))?;

        let db = Database { pool };
        db.migrate()?;
        // Set database file permissions to 0o600 on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = std::fs::metadata(path) {
                let mut perms = meta.permissions();
                if perms.mode() & 0o077 != 0 {
                    perms.set_mode(0o600);
                    let _ = std::fs::set_permissions(path, perms);
                }
            }
        }
        Ok(db)
    }

    /// Create a consistent backup of the database to the given path.
    /// Uses SQLite online backup API under the hood.
    pub fn backup_to(&self, path: &std::path::Path) -> Result<()> {
        let src = self.conn()?;
        let mut dest = rusqlite::Connection::open(path)?;
        let backup = rusqlite::backup::Backup::new(&src, &mut dest)?;
        backup.run_to_completion(5, std::time::Duration::from_millis(50), None)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).ok();
        }
        Ok(())
    }

    fn conn(&self) -> Result<DbConn> {
        const MAX_ATTEMPTS: u32 = 50; // 50 × 100ms = 5 seconds max
        for attempt in 1..=MAX_ATTEMPTS {
            match self.pool.get() {
                Ok(conn) => return Ok(conn),
                Err(err) => {
                    if attempt == MAX_ATTEMPTS {
                        return Err(anyhow::anyhow!(
                            "Database connection pool unavailable after {MAX_ATTEMPTS} attempts: {err}"
                        ));
                    }
                    tracing::error!(
                        "Database connection pool unavailable (attempt {}/{}, retrying): {}",
                        attempt,
                        MAX_ATTEMPTS,
                        err
                    );
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }
        anyhow::bail!("Database connection pool unavailable after {MAX_ATTEMPTS} attempts")
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        // All DDL uses CREATE TABLE/INDEX IF NOT EXISTS, safe to run every startup
        conn.execute_batch(include_str!("db/schema.sql"))?;
        Ok(())
    }


    fn latest_ledger_hash_with_conn<C>(&self, conn: &C) -> Result<Option<String>>
    where
        C: std::ops::Deref<Target = Connection>,
    {
        conn.query_row(
            "SELECT prev_hash FROM event_ledger ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("Failed to fetch latest event ledger hash")
    }

    pub fn add_db_guard(&self, reg: &DbGuardRegistration<'_>) -> Result<()> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO db_guards (name, engine, connection_ref, tables_csv, watched_tables_cache, mode, created_at, active)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)",
            params![
                reg.name,
                reg.engine.as_str(),
                reg.connection_ref,
                reg.tables_csv,
                reg.watched_tables_cache,
                reg.mode.as_str(),
                now
            ],
        )?;
        Ok(())
    }

    pub fn list_db_guards(&self) -> Result<Vec<DbGuardEntry>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, name, engine, connection_ref, tables_csv, watched_tables_cache, mode, created_at, last_baseline_at, active
             FROM db_guards ORDER BY id ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            let engine_raw: String = row.get(2)?;
            let mode_raw: String = row.get(6)?;
            let engine = DbGuardEngine::parse(&engine_raw).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    format!("invalid db_guard engine: {engine_raw}").into(),
                )
            })?;
            let mode = DbGuardMode::parse(&mode_raw).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    6,
                    rusqlite::types::Type::Text,
                    format!("invalid db_guard mode: {mode_raw}").into(),
                )
            })?;
            Ok(DbGuardEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                engine,
                connection_ref: row.get(3)?,
                tables_csv: row.get(4)?,
                watched_tables_cache: row.get(5)?,
                mode,
                created_at: row.get(7)?,
                last_baseline_at: row.get(8)?,
                active: row.get::<_, i32>(9)? != 0,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn set_db_guard_watched_tables_cache(&self, name: &str, cache: Option<&str>) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE db_guards SET watched_tables_cache = ?1 WHERE name = ?2",
            params![cache, name],
        )?;
        Ok(())
    }

    pub fn remove_db_guard(&self, name: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute("DELETE FROM db_guards WHERE name = ?1", params![name])?;
        Ok(())
    }

    pub fn set_db_guard_baseline_time(&self, name: &str, ts: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE db_guards SET last_baseline_at = ?1 WHERE name = ?2",
            params![ts, name],
        )?;
        Ok(())
    }

    pub fn add_agent(&self, name: &str, profile_path: &str, data_dir: Option<&str>) -> Result<()> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO agents (name, profile_path, data_dir, registered_at, active)
             VALUES (?1, ?2, ?3, ?4, 1)",
            params![name, profile_path, data_dir, now],
        )?;
        Ok(())
    }

    pub fn list_agents(&self) -> Result<Vec<AgentEntry>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, name, profile_path, data_dir, registered_at, active
             FROM agents ORDER BY id ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(AgentEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                profile_path: row.get(2)?,
                data_dir: row.get(3)?,
                registered_at: row.get(4)?,
                active: row.get::<_, i32>(5)? != 0,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn remove_agent(&self, name: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute("DELETE FROM agents WHERE name = ?1", params![name])?;
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── SnapshotTrigger ──

    #[test]
    fn snapshot_trigger_as_str_roundtrip() {
        let variants = [
            (SnapshotTrigger::Auto, "auto"),
            (SnapshotTrigger::Manual, "manual"),
            (SnapshotTrigger::Emergency, "emergency"),
            (SnapshotTrigger::PreRestore, "pre-restore"),
            (SnapshotTrigger::Initial, "initial"),
            (SnapshotTrigger::Mcp, "mcp"),
            (SnapshotTrigger::Api, "api"),
        ];
        for (variant, expected) in variants {
            assert_eq!(variant.as_str(), expected);
            assert_eq!(SnapshotTrigger::parse(expected), Some(variant));
        }
    }

    #[test]
    fn snapshot_trigger_parse_unknown_returns_none() {
        assert_eq!(SnapshotTrigger::parse("unknown"), None);
        assert_eq!(SnapshotTrigger::parse(""), None);
        assert_eq!(SnapshotTrigger::parse("AUTO"), None); // case-sensitive
    }

    #[test]
    fn snapshot_trigger_is_manual_kind() {
        assert!(SnapshotTrigger::Manual.is_manual_kind());
        assert!(SnapshotTrigger::Mcp.is_manual_kind());
        assert!(SnapshotTrigger::Api.is_manual_kind());
        assert!(!SnapshotTrigger::Auto.is_manual_kind());
        assert!(!SnapshotTrigger::Emergency.is_manual_kind());
        assert!(!SnapshotTrigger::PreRestore.is_manual_kind());
        assert!(!SnapshotTrigger::Initial.is_manual_kind());
    }

    #[test]
    fn snapshot_trigger_display() {
        assert_eq!(format!("{}", SnapshotTrigger::PreRestore), "pre-restore");
        assert_eq!(format!("{}", SnapshotTrigger::Auto), "auto");
    }

    #[test]
    fn snapshot_trigger_from_str() {
        assert_eq!("auto".parse::<SnapshotTrigger>(), Ok(SnapshotTrigger::Auto));
        assert!("UNKNOWN".parse::<SnapshotTrigger>().is_err());
    }

    #[test]
    fn snapshot_trigger_case_insensitive_eq() {
        assert!(SnapshotTrigger::Auto == "AUTO");
        assert!(SnapshotTrigger::Auto == "Auto");
        assert!(SnapshotTrigger::PreRestore == "PRE-RESTORE");
    }

    #[test]
    fn snapshot_trigger_serde_roundtrip() {
        let trigger = SnapshotTrigger::PreRestore;
        let json = serde_json::to_string(&trigger).unwrap();
        assert_eq!(json, "\"pre-restore\"");
        let parsed: SnapshotTrigger = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, trigger);
    }

    // ── LedgerSeverity ──

    #[test]
    fn ledger_severity_as_str_roundtrip() {
        let variants = [
            (LedgerSeverity::Info, "info"),
            (LedgerSeverity::Warn, "warn"),
            (LedgerSeverity::Error, "error"),
            (LedgerSeverity::Critical, "critical"),
        ];
        for (variant, expected) in variants {
            assert_eq!(variant.as_str(), expected);
            assert_eq!(LedgerSeverity::parse(expected), Some(variant));
        }
    }

    #[test]
    fn ledger_severity_parse_unknown_returns_none() {
        assert_eq!(LedgerSeverity::parse("unknown"), None);
        assert_eq!(LedgerSeverity::parse("INFO"), None);
    }

    #[test]
    fn ledger_severity_display_and_from_str() {
        assert_eq!(format!("{}", LedgerSeverity::Critical), "critical");
        assert_eq!("warn".parse::<LedgerSeverity>(), Ok(LedgerSeverity::Warn));
        assert!("invalid".parse::<LedgerSeverity>().is_err());
    }

    #[test]
    fn ledger_severity_case_insensitive_eq() {
        assert!(LedgerSeverity::Info == "INFO");
        assert!(LedgerSeverity::Error == "Error");
    }

    // ── LedgerSource ──

    #[test]
    fn ledger_source_as_str_roundtrip() {
        let variants = [
            (LedgerSource::Agent, "agent"),
            (LedgerSource::DbGuard, "db_guard"),
            (LedgerSource::Daemon, "daemon"),
            (LedgerSource::Fs, "fs"),
            (LedgerSource::Mlx, "mlx"),
        ];
        for (variant, expected) in variants {
            assert_eq!(variant.as_str(), expected);
            assert_eq!(LedgerSource::parse(expected), Some(variant));
        }
    }

    #[test]
    fn ledger_source_parse_unknown_returns_none() {
        assert_eq!(LedgerSource::parse("unknown"), None);
    }

    #[test]
    fn ledger_source_from_str_case_insensitive() {
        assert_eq!("FS".parse::<LedgerSource>(), Ok(LedgerSource::Fs));
        assert_eq!("  Agent  ".parse::<LedgerSource>(), Ok(LedgerSource::Agent));
        assert_eq!("DB_GUARD".parse::<LedgerSource>(), Ok(LedgerSource::DbGuard));
        assert!("invalid".parse::<LedgerSource>().is_err());
    }

    #[test]
    fn ledger_source_display() {
        assert_eq!(format!("{}", LedgerSource::DbGuard), "db_guard");
    }

    #[test]
    fn ledger_source_case_insensitive_eq() {
        assert!(LedgerSource::Agent == "AGENT");
        assert!(LedgerSource::Mlx == "Mlx");
    }

    // ── LedgerEventType ──

    #[test]
    fn ledger_event_type_known_variants_roundtrip() {
        let known = [
            "emergency_delete_detected",
            "config_reload_failed",
            "guard_started",
            "guard_mode_normalized",
            "guard_tick_failed",
            "postgres_tick",
            "postgres_baseline",
            "postgres_optional_step_degraded",
            "postgres_wildcard_trigger_reconciled",
            "sqlite_tick",
            "sqlite_data_changed",
            "sqlite_baseline",
            "mysql_tick",
            "mysql_poll_failed",
            "drop_table",
            "schema_change",
            "mass_delete",
            "agent_registered",
            "tool_call",
            "session_tool_call",
            "pre_notify",
            "dangerous_agent_action",
            "dangerous_action_timeout",
            "dangerous_action_denied",
            "dangerous_action_approved",
            "mcp_proxy_started",
            "mcp_proxy_client_connected",
            "mcp_proxy_connection_failed",
            "mcp_proxy_accept_failed",
            "audit_tick",
            "fanotify_overflow",
            "fanotify_monitor_started",
            "fanotify_preimage",
            "fanotify_monitor_degraded",
        ];
        for s in known {
            let parsed = LedgerEventType::parse(s);
            assert_ne!(
                matches!(parsed, LedgerEventType::Other(_)),
                true,
                "'{s}' should parse to a known variant"
            );
            assert_eq!(parsed.as_str(), s);
        }
    }

    #[test]
    fn ledger_event_type_unknown_goes_to_other() {
        let evt = LedgerEventType::parse("custom_event_xyz");
        assert_eq!(evt, LedgerEventType::Other("custom_event_xyz".to_string()));
        assert_eq!(evt.as_str(), "custom_event_xyz");
    }

    #[test]
    fn ledger_event_type_display() {
        assert_eq!(
            format!("{}", LedgerEventType::GuardStarted),
            "guard_started"
        );
        assert_eq!(
            format!("{}", LedgerEventType::Other("foo".into())),
            "foo"
        );
    }

    #[test]
    fn ledger_event_type_from_str_is_infallible() {
        let result: Result<LedgerEventType, _> = "anything".parse();
        assert!(result.is_ok());
    }

    #[test]
    fn ledger_event_type_from_ref_str() {
        let evt: LedgerEventType = "guard_started".into();
        assert_eq!(evt, LedgerEventType::GuardStarted);
    }

    // ── DbGuardEngine ──

    #[test]
    fn db_guard_engine_as_str_roundtrip() {
        let variants = [
            (DbGuardEngine::Sqlite, "sqlite"),
            (DbGuardEngine::Postgres, "postgres"),
            (DbGuardEngine::Mysql, "mysql"),
        ];
        for (variant, expected) in variants {
            assert_eq!(variant.as_str(), expected);
            assert_eq!(DbGuardEngine::parse(expected), Some(variant));
        }
    }

    #[test]
    fn db_guard_engine_parse_unknown() {
        assert_eq!(DbGuardEngine::parse("oracle"), None);
        assert_eq!(DbGuardEngine::parse(""), None);
    }

    #[test]
    fn db_guard_engine_display_and_from_str() {
        assert_eq!(format!("{}", DbGuardEngine::Postgres), "postgres");
        assert_eq!(
            "sqlite".parse::<DbGuardEngine>(),
            Ok(DbGuardEngine::Sqlite)
        );
        assert!("unknown".parse::<DbGuardEngine>().is_err());
    }

    #[test]
    fn db_guard_engine_case_insensitive_eq() {
        assert!(DbGuardEngine::Sqlite == "SQLITE");
        assert!(DbGuardEngine::Postgres == "Postgres");
    }

    // ── DbGuardMode ──

    #[test]
    fn db_guard_mode_as_str_roundtrip() {
        let variants = [
            (DbGuardMode::Triggers, "triggers"),
            (DbGuardMode::SchemaPolling, "schema_polling"),
        ];
        for (variant, expected) in variants {
            assert_eq!(variant.as_str(), expected);
            assert_eq!(DbGuardMode::parse(expected), Some(variant));
        }
    }

    #[test]
    fn db_guard_mode_parse_unknown() {
        assert_eq!(DbGuardMode::parse("listen"), None);
    }

    #[test]
    fn db_guard_mode_display_and_from_str() {
        assert_eq!(format!("{}", DbGuardMode::Triggers), "triggers");
        assert_eq!(
            "schema_polling".parse::<DbGuardMode>(),
            Ok(DbGuardMode::SchemaPolling)
        );
        assert!("invalid".parse::<DbGuardMode>().is_err());
    }

    #[test]
    fn db_guard_mode_eq_ignore_ascii_case() {
        assert!(DbGuardMode::Triggers.eq_ignore_ascii_case("TRIGGERS"));
        assert!(DbGuardMode::SchemaPolling.eq_ignore_ascii_case("Schema_Polling"));
        assert!(!DbGuardMode::Triggers.eq_ignore_ascii_case("schema_polling"));
    }

    #[test]
    fn db_guard_mode_case_insensitive_partial_eq() {
        assert!(DbGuardMode::Triggers == "TRIGGERS");
    }

    // ── Helper functions ──

    #[test]
    fn checked_db_u64_positive_values() {
        assert_eq!(checked_db_u64(0, "test").unwrap(), 0);
        assert_eq!(checked_db_u64(42, "test").unwrap(), 42);
        assert_eq!(checked_db_u64(i64::MAX, "test").unwrap(), i64::MAX as u64);
    }

    #[test]
    fn checked_db_u64_negative_returns_error() {
        assert!(checked_db_u64(-1, "test").is_err());
        assert!(checked_db_u64(i64::MIN, "test").is_err());
    }

    #[test]
    fn checked_usize_u64_conversions() {
        assert_eq!(checked_usize_u64(0, "test").unwrap(), 0);
        assert_eq!(checked_usize_u64(100, "test").unwrap(), 100);
    }

    // ── StorageMethod (from cas module, used in db) ──

    #[test]
    fn storage_method_to_from_db_roundtrip() {
        assert_eq!(StorageMethod::from_db(StorageMethod::None.to_db()), StorageMethod::None);
        assert_eq!(StorageMethod::from_db(StorageMethod::Copy.to_db()), StorageMethod::Copy);
        assert_eq!(StorageMethod::from_db(StorageMethod::Reflink.to_db()), StorageMethod::Reflink);
    }

    #[test]
    fn storage_method_from_db_unknown_defaults_to_none() {
        assert_eq!(StorageMethod::from_db(99), StorageMethod::None);
        assert_eq!(StorageMethod::from_db(-1), StorageMethod::None);
    }

    #[test]
    fn storage_method_is_recoverable() {
        assert!(!StorageMethod::None.is_recoverable());
        assert!(StorageMethod::Copy.is_recoverable());
        assert!(StorageMethod::Reflink.is_recoverable());
    }

    #[test]
    fn storage_method_display_name() {
        assert_eq!(StorageMethod::None.display_name(), "none");
        assert_eq!(StorageMethod::Copy.display_name(), "copy");
        assert_eq!(StorageMethod::Reflink.display_name(), "reflink");
    }

    // ── Database open + CRUD ──

    #[test]
    fn database_open_and_basic_operations() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let db = Database::open(&db_path).unwrap();

        // List empty tables
        let projects = db.list_projects().unwrap();
        assert!(projects.is_empty());

        let guards = db.list_db_guards().unwrap();
        assert!(guards.is_empty());

        let agents = db.list_agents().unwrap();
        assert!(agents.is_empty());
    }

    #[test]
    fn database_agent_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let db = Database::open(&db_path).unwrap();

        db.add_agent("test-agent", "/path/to/profile", Some("/data")).unwrap();
        let agents = db.list_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "test-agent");
        assert_eq!(agents[0].profile_path, "/path/to/profile");
        assert_eq!(agents[0].data_dir.as_deref(), Some("/data"));
        assert!(agents[0].active);

        db.remove_agent("test-agent").unwrap();
        let agents = db.list_agents().unwrap();
        assert!(agents.is_empty());
    }

    #[test]
    fn database_agent_no_data_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let db = Database::open(&db_path).unwrap();

        db.add_agent("agent2", "/profile", None).unwrap();
        let agents = db.list_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert!(agents[0].data_dir.is_none());
    }

    #[test]
    fn database_clone_handle() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let db = Database::open(&db_path).unwrap();

        db.add_agent("agent-a", "/p", None).unwrap();

        let db2 = db.clone_handle();
        let agents = db2.list_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "agent-a");
    }

    #[test]
    fn database_backup() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("source.db");
        let db = Database::open(&db_path).unwrap();

        db.add_agent("backup-test", "/p", None).unwrap();

        let backup_path = tmp.path().join("backup.db");
        db.backup_to(&backup_path).unwrap();

        let db2 = Database::open(&backup_path).unwrap();
        let agents = db2.list_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "backup-test");
    }
}
