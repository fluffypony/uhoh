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
mod search;

mod snapshots;

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

#[derive(Debug, Clone)]
pub struct ProjectEntry {
    pub hash: String,
    pub current_path: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotRow {
    pub rowid: i64,
    pub snapshot_id: u64,
    pub timestamp: String,
    pub trigger: String,
    pub message: String,
    pub pinned: bool,
    pub ai_summary: Option<String>,
    pub file_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotSummary {
    pub rowid: i64,
    pub snapshot_id: u64,
    pub timestamp: String,
    pub trigger: String,
    pub message: String,
    pub pinned: bool,
    pub file_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchResult {
    pub snapshot_rowid: i64,
    pub snapshot_id: u64,
    pub timestamp: String,
    pub trigger: String,
    pub message: String,
    pub ai_summary: Option<String>,
    pub match_context: String,
}

pub struct CreateSnapshotRow<'a> {
    pub project_hash: &'a str,
    pub snapshot_id: u64,
    pub timestamp: &'a str,
    pub trigger: &'a str,
    pub message: &'a str,
    pub pinned: bool,
    pub files: &'a [SnapFileEntry],
    pub deleted: &'a [DeletedFile],
}

#[derive(Debug, Clone)]
pub struct EventLedgerEntry {
    pub id: i64,
    pub ts: String,
    pub source: LedgerSource,
    pub event_type: String,
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
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(value).ok_or(())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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

    pub fn trim(self) -> &'static str {
        self.as_str()
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
pub struct EventLedgerTraceResult {
    pub entries: Vec<EventLedgerEntry>,
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub struct NewEventLedgerEntry {
    pub source: LedgerSource,
    pub event_type: String,
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

#[derive(Debug, Clone)]
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

impl DbGuardEntry {
    pub fn engine_kind(&self) -> DbGuardEngine {
        self.engine
    }

    pub fn mode_kind(&self) -> DbGuardMode {
        self.mode
    }
}

#[derive(Debug, Clone)]
pub struct AgentEntry {
    pub id: i64,
    pub name: String,
    pub profile_path: String,
    pub data_dir: Option<String>,
    pub registered_at: String,
    pub active: bool,
}

#[derive(Debug, Clone)]
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
pub struct OperationListRow {
    pub id: i64,
    pub label: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub first_snapshot_id: Option<u64>,
    pub last_snapshot_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct PendingAiSummaryRow {
    pub snapshot_rowid: i64,
    pub project_hash: String,
    pub attempts: i64,
    pub queued_at: String,
}

#[derive(Debug, Clone)]
pub struct ActiveOperationRow {
    pub id: i64,
    pub label: String,
}

#[derive(Debug, Clone)]
pub struct CompletedOperationRow {
    pub id: i64,
    pub label: String,
    pub first_snapshot_id: u64,
    pub last_snapshot_id: u64,
}

#[derive(Debug, Clone)]
pub struct FileHistoryRow {
    pub snapshot_id: u64,
    pub timestamp: String,
    pub hash: String,
    pub trigger: String,
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
                            "Database connection pool unavailable after {} attempts: {}",
                            MAX_ATTEMPTS,
                            err
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

    // === Projects ===

    pub fn add_project(&self, hash: &str, path: &str) -> Result<()> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO projects (hash, current_path, created_at) VALUES (?1, ?2, ?3)",
            params![hash, path, now],
        )?;
        Ok(())
    }

    pub fn get_project(&self, hash: &str) -> Result<Option<ProjectEntry>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT hash, current_path, created_at FROM projects WHERE hash = ?1",
            params![hash],
            |row| {
                Ok(ProjectEntry {
                    hash: row.get(0)?,
                    current_path: row.get(1)?,
                    created_at: row.get(2)?,
                })
            },
        )
        .optional()
        .context("Failed to query project")
    }

    pub fn find_project_by_path(&self, path: &Path) -> Result<Option<ProjectEntry>> {
        let conn = self.conn()?;
        let path_str = path.to_string_lossy();
        conn.query_row(
            "SELECT hash, current_path, created_at FROM projects WHERE current_path = ?1",
            params![path_str.as_ref()],
            |row| {
                Ok(ProjectEntry {
                    hash: row.get(0)?,
                    current_path: row.get(1)?,
                    created_at: row.get(2)?,
                })
            },
        )
        .optional()
        .context("Failed to query project by path")
    }

    pub fn find_project_by_hash_prefix(&self, prefix: &str) -> Result<Option<ProjectEntry>> {
        let conn = self.conn()?;
        // Escape SQL wildcards in user-provided prefix using backslash + ESCAPE clause
        let esc = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{esc}%");

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM projects WHERE hash LIKE ?1 ESCAPE '\\'",
            params![pattern.clone()],
            |row| row.get(0),
        )?;

        if count > 1 {
            anyhow::bail!(
                "Ambiguous hash prefix '{prefix}' matches {count} projects. Use a longer prefix."
            );
        }

        conn.query_row(
            "SELECT hash, current_path, created_at FROM projects WHERE hash LIKE ?1 ESCAPE '\\' LIMIT 1",
            params![pattern],
            |row| {
                Ok(ProjectEntry {
                    hash: row.get(0)?,
                    current_path: row.get(1)?,
                    created_at: row.get(2)?,
                })
            },
        )
        .optional()
        .context("Failed to query project by hash prefix")
    }

    pub fn update_project_path(&self, hash: &str, new_path: &str) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE projects SET current_path = ?1 WHERE hash = ?2",
            params![new_path, hash],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn remove_project(&self, hash: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM search_index WHERE project_hash = ?1",
            params![hash],
        )?;
        conn.execute("DELETE FROM projects WHERE hash = ?1", params![hash])?;
        Ok(())
    }

    pub fn list_projects(&self) -> Result<Vec<ProjectEntry>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare("SELECT hash, current_path, created_at FROM projects ORDER BY created_at")?;
        let rows = stmt.query_map([], |row| {
            Ok(ProjectEntry {
                hash: row.get(0)?,
                current_path: row.get(1)?,
                created_at: row.get(2)?,
            })
        })?;
        let mut projects = Vec::new();
        for row in rows {
            projects.push(row?);
        }
        Ok(projects)
    }

    // === Snapshots ===

    /// Create a snapshot in a single transaction (atomic).
    pub fn create_snapshot(&self, request: CreateSnapshotRow<'_>) -> Result<(i64, u64)> {
        let CreateSnapshotRow {
            project_hash,
            snapshot_id,
            timestamp,
            trigger,
            message,
            pinned,
            files,
            deleted,
        } = request;
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        // Allocate snapshot_id inside the same transaction if zero passed
        let snapshot_id = if snapshot_id == 0 {
            let id: u64 = tx.query_row(
                "SELECT COALESCE(MAX(snapshot_id), 0) + 1 FROM snapshots WHERE project_hash = ?1",
                params![project_hash],
                |row| row.get(0),
            )?;
            id
        } else {
            snapshot_id
        };

        let rowid = snapshots::create_snapshot_tx(
            &tx,
            snapshots::SnapshotInsert {
                project_hash,
                snapshot_id,
                timestamp,
                trigger,
                message,
                pinned,
                files,
                deleted,
            },
        )?;

        tx.commit()?;
        Ok((rowid, snapshot_id))
    }

    pub fn list_snapshots(&self, project_hash: &str) -> Result<Vec<SnapshotRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    s.file_count
             FROM snapshots s
             WHERE s.project_hash = ?1
             ORDER BY s.timestamp DESC, s.rowid DESC",
        )?;
        let rows = stmt.query_map(params![project_hash], |row| {
            Ok(SnapshotRow {
                rowid: row.get(0)?,
                snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                timestamp: row.get(2)?,
                trigger: row.get(3)?,
                message: row.get(4)?,
                pinned: row.get::<_, i32>(5)? != 0,
                ai_summary: row.get(6)?,
                file_count: row_u64(row, 7, "snapshots.file_count")?,
            })
        })?;
        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(row?);
        }
        Ok(snapshots)
    }

    pub fn list_snapshots_paginated(
        &self,
        project_hash: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<SnapshotRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    s.file_count
             FROM snapshots s
             WHERE s.project_hash = ?1
             ORDER BY s.timestamp DESC, s.snapshot_id DESC
             LIMIT ?2 OFFSET ?3",
        )?;
        let rows = stmt.query_map(params![project_hash, limit as i64, offset as i64], |row| {
            Ok(SnapshotRow {
                rowid: row.get(0)?,
                snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                timestamp: row.get(2)?,
                trigger: row.get(3)?,
                message: row.get(4)?,
                pinned: row.get::<_, i32>(5)? != 0,
                ai_summary: row.get(6)?,
                file_count: row_u64(row, 7, "snapshots.file_count")?,
            })
        })?;
        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(row?);
        }
        Ok(snapshots)
    }

    pub fn list_snapshot_summaries(
        &self,
        project_hash: &str,
        from_ts: Option<&str>,
        to_ts: Option<&str>,
    ) -> Result<Vec<SnapshotSummary>> {
        let conn = self.conn()?;
        match (from_ts, to_ts) {
            (None, None) => {
                let mut stmt = conn.prepare(
                    "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                            s.file_count
                     FROM snapshots s
                     WHERE s.project_hash = ?1
                     ORDER BY s.timestamp DESC, s.snapshot_id DESC",
                )?;
                let rows = stmt.query_map(params![project_hash], |row| {
                    Ok(SnapshotSummary {
                        rowid: row.get(0)?,
                        snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                        timestamp: row.get(2)?,
                        trigger: row.get(3)?,
                        message: row.get(4)?,
                        pinned: row.get::<_, i32>(5)? != 0,
                        file_count: row_u64(row, 6, "snapshots.file_count")?,
                    })
                })?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Into::into)
            }
            (Some(from), None) => {
                let mut stmt = conn.prepare(
                    "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                            s.file_count
                     FROM snapshots s
                     WHERE s.project_hash = ?1 AND s.timestamp >= ?2
                     ORDER BY s.timestamp DESC, s.snapshot_id DESC",
                )?;
                let rows = stmt.query_map(params![project_hash, from], |row| {
                    Ok(SnapshotSummary {
                        rowid: row.get(0)?,
                        snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                        timestamp: row.get(2)?,
                        trigger: row.get(3)?,
                        message: row.get(4)?,
                        pinned: row.get::<_, i32>(5)? != 0,
                        file_count: row_u64(row, 6, "snapshots.file_count")?,
                    })
                })?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Into::into)
            }
            (None, Some(to)) => {
                let mut stmt = conn.prepare(
                    "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                            s.file_count
                     FROM snapshots s
                     WHERE s.project_hash = ?1 AND s.timestamp <= ?2
                     ORDER BY s.timestamp DESC, s.snapshot_id DESC",
                )?;
                let rows = stmt.query_map(params![project_hash, to], |row| {
                    Ok(SnapshotSummary {
                        rowid: row.get(0)?,
                        snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                        timestamp: row.get(2)?,
                        trigger: row.get(3)?,
                        message: row.get(4)?,
                        pinned: row.get::<_, i32>(5)? != 0,
                        file_count: row_u64(row, 6, "snapshots.file_count")?,
                    })
                })?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Into::into)
            }
            (Some(from), Some(to)) => {
                let mut stmt = conn.prepare(
                    "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                            s.file_count
                     FROM snapshots s
                     WHERE s.project_hash = ?1 AND s.timestamp >= ?2 AND s.timestamp <= ?3
                     ORDER BY s.timestamp DESC, s.snapshot_id DESC",
                )?;
                let rows = stmt.query_map(params![project_hash, from, to], |row| {
                    Ok(SnapshotSummary {
                        rowid: row.get(0)?,
                        snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                        timestamp: row.get(2)?,
                        trigger: row.get(3)?,
                        message: row.get(4)?,
                        pinned: row.get::<_, i32>(5)? != 0,
                        file_count: row_u64(row, 6, "snapshots.file_count")?,
                    })
                })?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Into::into)
            }
        }
    }

    /// Lookup a snapshot by its internal rowid
    pub fn get_snapshot_by_rowid(&self, rowid: i64) -> Result<Option<SnapshotRow>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                        s.ai_summary,
                        s.file_count
                 FROM snapshots s WHERE s.rowid = ?1",
            params![rowid],
            |row| {
                Ok(SnapshotRow {
                    rowid: row.get(0)?,
                    snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                    timestamp: row.get(2)?,
                    trigger: row.get(3)?,
                    message: row.get(4)?,
                    pinned: row.get::<_, i32>(5)? != 0,
                    ai_summary: row.get(6)?,
                    file_count: row_u64(row, 7, "snapshots.file_count")?,
                })
            },
        )
        .optional()
        .context("Failed to query snapshot by rowid")
    }

    pub fn snapshot_count(&self, project_hash: &str) -> Result<u64> {
        let conn = self.conn()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM snapshots WHERE project_hash = ?1",
            params![project_hash],
            |row| row.get(0),
        )?;
        checked_db_u64(count, "snapshots.count")
    }

    pub fn find_snapshot_by_base58(
        &self,
        project_hash: &str,
        base58_id: &str,
    ) -> Result<Option<SnapshotRow>> {
        let snapshot_id = crate::cas::base58_to_id(base58_id).context("Invalid snapshot ID")?;
        let conn = self.conn()?;
        conn.query_row(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    s.file_count as fc
             FROM snapshots s
             WHERE s.project_hash = ?1 AND s.snapshot_id = ?2",
            params![project_hash, snapshot_id],
            |row| {
                Ok(SnapshotRow {
                    rowid: row.get(0)?,
                    snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                    timestamp: row.get(2)?,
                    trigger: row.get(3)?,
                    message: row.get(4)?,
                    pinned: row.get::<_, i32>(5)? != 0,
                    ai_summary: row.get(6)?,
                    file_count: row_u64(row, 7, "snapshots.file_count")?,
                })
            },
        )
        .optional()
        .context("Failed to query snapshot")
    }

    pub fn get_snapshot_files(&self, snapshot_rowid: i64) -> Result<Vec<FileEntryRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT path, hash, size, stored, executable, mtime, storage_method, is_symlink
             FROM snapshot_files WHERE snapshot_rowid = ?1",
        )?;
        let rows = stmt.query_map(params![snapshot_rowid], |row| {
            let path: String = row.get(0)?;
            let hash: String = row.get(1)?;
            let size = row_u64(row, 2, "snapshot_files.size")?;
            let stored = row.get::<_, i32>(3)? != 0;
            let executable = row.get::<_, i32>(4)? != 0;
            let mtime = row.get::<_, Option<i64>>(5)?;
            let storage_method = StorageMethod::from_db(row.get::<_, i64>(6)?);
            let is_symlink = row.get::<_, i32>(7)? != 0;
            Ok(FileEntryRow {
                path,
                hash,
                size,
                stored,
                executable,
                mtime,
                storage_method,
                is_symlink,
            })
        })?;
        let mut entries = Vec::new();
        for row in rows {
            entries.push(row?);
        }
        Ok(entries)
    }

    pub fn get_snapshot_deleted_files(&self, snapshot_rowid: i64) -> Result<Vec<FileEntryRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT path, hash, size, stored, storage_method FROM snapshot_deleted WHERE snapshot_rowid = ?1",
        )?;
        let rows = stmt.query_map(params![snapshot_rowid], |row| {
            let path: String = row.get(0)?;
            let hash: String = row.get(1)?;
            let size = row_u64(row, 2, "snapshot_deleted.size")?;
            let stored = row.get::<_, i32>(3)? != 0;
            let storage_method = StorageMethod::from_db(row.get::<_, i64>(4)?);
            Ok(FileEntryRow {
                path,
                hash,
                size,
                stored,
                executable: false,
                mtime: None,
                storage_method,
                is_symlink: false,
            })
        })?;
        let mut entries = Vec::new();
        for row in rows {
            entries.push(row?);
        }
        Ok(entries)
    }

    /// Get the most recent snapshot rowid for a project
    pub fn latest_snapshot_rowid(&self, project_hash: &str) -> Result<Option<i64>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT rowid FROM snapshots WHERE project_hash = ?1 ORDER BY snapshot_id DESC LIMIT 1",
            params![project_hash],
            |row| row.get(0),
        )
        .optional()
        .context("Failed to query latest snapshot")
    }

    /// Get file history: all snapshot entries for a given path, newest first
    pub fn file_history(&self, project_hash: &str, file_path: &str) -> Result<Vec<FileHistoryRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT s.snapshot_id, s.timestamp, f.hash, s.trigger
             FROM snapshot_files f
             JOIN snapshots s ON s.rowid = f.snapshot_rowid
             WHERE s.project_hash = ?1 AND f.path = ?2
             ORDER BY s.snapshot_id DESC",
        )?;
        let rows = stmt.query_map(params![project_hash, file_path], |row| {
            Ok(FileHistoryRow {
                snapshot_id: row_u64(row, 0, "snapshots.snapshot_id")?,
                timestamp: row.get::<_, String>(1)?,
                hash: row.get::<_, String>(2)?,
                trigger: row.get::<_, String>(3)?,
            })
        })?;
        let mut entries = Vec::new();
        for row in rows {
            entries.push(row?);
        }
        Ok(entries)
    }

    /// All blob hashes referenced by any snapshot or event ledger entry
    pub fn all_referenced_blob_hashes(&self) -> Result<std::collections::HashSet<String>> {
        let conn = self.conn()?;
        let mut set = std::collections::HashSet::new();
        let mut stmt = conn.prepare("SELECT DISTINCT hash FROM snapshot_files WHERE stored = 1")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            set.insert(row?);
        }
        let mut stmt2 =
            conn.prepare("SELECT DISTINCT hash FROM snapshot_deleted WHERE stored = 1")?;
        let rows2 = stmt2.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows2 {
            set.insert(row?);
        }
        // Include blob references from the event ledger
        let mut stmt3 = conn.prepare(
            "SELECT pre_state_ref FROM event_ledger WHERE pre_state_ref IS NOT NULL
             UNION SELECT post_state_ref FROM event_ledger WHERE post_state_ref IS NOT NULL",
        )?;
        let rows3 = stmt3.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows3 {
            set.insert(row?);
        }
        Ok(set)
    }

    /// Increment the cached blob bytes counter by delta (can be negative).
    pub fn add_blob_bytes(&self, delta: i64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO stats (key, value) VALUES ('blob_bytes', 0)
             ON CONFLICT(key) DO UPDATE SET value = value + excluded.value",
            [],
        )?;
        conn.execute(
            "UPDATE stats SET value = value + ?1 WHERE key = 'blob_bytes'",
            params![delta],
        )?;
        Ok(())
    }

    /// Get the cached blob bytes total; returns 0 if missing.
    pub fn get_blob_bytes(&self) -> Result<u64> {
        let conn = self.conn()?;
        let v: i64 = conn
            .query_row(
                "SELECT value FROM stats WHERE key = 'blob_bytes'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        checked_db_u64(v.max(0), "stats.blob_bytes")
    }

    /// Run incremental vacuum to reclaim free pages without exclusive lock.
    pub fn vacuum(&self) -> Result<()> {
        let conn = self.conn()?;
        conn.execute_batch("PRAGMA incremental_vacuum(100);")?;
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

    pub fn add_db_guard(
        &self,
        name: &str,
        engine: DbGuardEngine,
        connection_ref: &str,
        tables_csv: &str,
        watched_tables_cache: Option<&str>,
        mode: DbGuardMode,
    ) -> Result<()> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO db_guards (name, engine, connection_ref, tables_csv, watched_tables_cache, mode, created_at, active)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)",
            params![
                name,
                engine.as_str(),
                connection_ref,
                tables_csv,
                watched_tables_cache,
                mode.as_str(),
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

// Type aliases to simplify complex tuple signatures used around snapshot creation
#[derive(Debug, Clone)]
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
pub type DeletedFile = (String, String, u64, bool, StorageMethod); // (path, hash, size, stored, storage_method)
