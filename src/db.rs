use anyhow::{Context, Result};
use r2d2::{CustomizeConnection, Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::path::Path;

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

mod ledger;

mod snapshots;

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

#[derive(Debug, Clone)]
pub struct EventLedgerEntry {
    pub id: i64,
    pub ts: String,
    pub source: String,
    pub event_type: String,
    pub severity: String,
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

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "info" => Some(LedgerSeverity::Info),
            "warn" => Some(LedgerSeverity::Warn),
            "error" => Some(LedgerSeverity::Error),
            "critical" => Some(LedgerSeverity::Critical),
            _ => None,
        }
    }
}

impl From<&str> for LedgerSeverity {
    fn from(value: &str) -> Self {
        Self::from_str(value).unwrap_or(LedgerSeverity::Info)
    }
}

impl From<String> for LedgerSeverity {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
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

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "agent" => Some(LedgerSource::Agent),
            "db_guard" => Some(LedgerSource::DbGuard),
            "daemon" => Some(LedgerSource::Daemon),
            "fs" => Some(LedgerSource::Fs),
            "mlx" => Some(LedgerSource::Mlx),
            _ => None,
        }
    }
}

impl From<&str> for LedgerSource {
    fn from(value: &str) -> Self {
        Self::from_str(value).unwrap_or(LedgerSource::Daemon)
    }
}

impl From<String> for LedgerSource {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
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

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "sqlite" => Some(DbGuardEngine::Sqlite),
            "postgres" => Some(DbGuardEngine::Postgres),
            "mysql" => Some(DbGuardEngine::Mysql),
            _ => None,
        }
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

    pub fn from_str(value: &str) -> Option<Self> {
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
    #[allow(clippy::too_many_arguments)]
    pub fn create_snapshot(
        &self,
        project_hash: &str,
        snapshot_id: u64,
        timestamp: &str,
        trigger: &str,
        message: &str,
        pinned: bool,
        files: &[SnapFileEntry],
        deleted: &[DeletedFile],
    ) -> Result<(i64, u64)> {
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
            project_hash,
            snapshot_id,
            timestamp,
            trigger,
            message,
            pinned,
            files,
            deleted,
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
                snapshot_id: row.get::<_, i64>(1)? as u64,
                timestamp: row.get(2)?,
                trigger: row.get(3)?,
                message: row.get(4)?,
                pinned: row.get::<_, i32>(5)? != 0,
                ai_summary: row.get(6)?,
                file_count: row.get::<_, i64>(7)? as u64,
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
                snapshot_id: row.get::<_, i64>(1)? as u64,
                timestamp: row.get(2)?,
                trigger: row.get(3)?,
                message: row.get(4)?,
                pinned: row.get::<_, i32>(5)? != 0,
                ai_summary: row.get(6)?,
                file_count: row.get::<_, i64>(7)? as u64,
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
                        snapshot_id: row.get::<_, i64>(1)? as u64,
                        timestamp: row.get(2)?,
                        trigger: row.get(3)?,
                        message: row.get(4)?,
                        pinned: row.get::<_, i32>(5)? != 0,
                        file_count: row.get::<_, i64>(6)? as u64,
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
                        snapshot_id: row.get::<_, i64>(1)? as u64,
                        timestamp: row.get(2)?,
                        trigger: row.get(3)?,
                        message: row.get(4)?,
                        pinned: row.get::<_, i32>(5)? != 0,
                        file_count: row.get::<_, i64>(6)? as u64,
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
                        snapshot_id: row.get::<_, i64>(1)? as u64,
                        timestamp: row.get(2)?,
                        trigger: row.get(3)?,
                        message: row.get(4)?,
                        pinned: row.get::<_, i32>(5)? != 0,
                        file_count: row.get::<_, i64>(6)? as u64,
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
                        snapshot_id: row.get::<_, i64>(1)? as u64,
                        timestamp: row.get(2)?,
                        trigger: row.get(3)?,
                        message: row.get(4)?,
                        pinned: row.get::<_, i32>(5)? != 0,
                        file_count: row.get::<_, i64>(6)? as u64,
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
                    snapshot_id: row.get::<_, i64>(1)? as u64,
                    timestamp: row.get(2)?,
                    trigger: row.get(3)?,
                    message: row.get(4)?,
                    pinned: row.get::<_, i32>(5)? != 0,
                    ai_summary: row.get(6)?,
                    file_count: row.get::<_, i64>(7)? as u64,
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
        Ok(count as u64)
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
                    snapshot_id: row.get::<_, i64>(1)? as u64,
                    timestamp: row.get(2)?,
                    trigger: row.get(3)?,
                    message: row.get(4)?,
                    pinned: row.get::<_, i32>(5)? != 0,
                    ai_summary: row.get(6)?,
                    file_count: row.get::<_, i64>(7)? as u64,
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
            let size = row.get::<_, i64>(2)? as u64;
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
            let size = row.get::<_, i64>(2)? as u64;
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
    pub fn file_history(
        &self,
        project_hash: &str,
        file_path: &str,
    ) -> Result<Vec<FileHistoryRow>> {
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
                snapshot_id: row.get::<_, i64>(0)? as u64,
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

    /// Set or clear the pinned flag on a snapshot.
    pub fn pin_snapshot(&self, rowid: i64, pinned: bool) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE snapshots SET pinned = ?1 WHERE rowid = ?2",
            params![pinned as i32, rowid],
        )?;
        Ok(())
    }

    /// Delete old snapshots (used by compaction)
    pub fn delete_snapshot(&self, rowid: i64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM search_index WHERE snapshot_rowid = ?1",
            params![rowid],
        )?;
        conn.execute("DELETE FROM snapshots WHERE rowid = ?1", params![rowid])?;
        Ok(())
    }

    pub fn set_ai_summary(&self, snapshot_rowid: i64, summary: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE snapshots SET ai_summary = ?1 WHERE rowid = ?2",
            params![summary, snapshot_rowid],
        )?;
        let _ = self.update_search_index_summary(snapshot_rowid, summary);
        Ok(())
    }

    pub fn index_snapshot_for_search(
        &self,
        snapshot_rowid: i64,
        project_hash: &str,
        trigger: &str,
        message: &str,
        ai_summary: &str,
        file_paths: &str,
    ) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO search_index(snapshot_rowid, project_hash, trigger_type, message, ai_summary, file_paths)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![snapshot_rowid, project_hash, trigger, message, ai_summary, file_paths],
        )?;
        Ok(())
    }

    pub fn update_search_index_summary(&self, snapshot_rowid: i64, ai_summary: &str) -> Result<()> {
        let conn = self.conn()?;
        let row: Option<(String, String, String, String)> = conn
            .query_row(
                "SELECT project_hash, trigger_type, message, file_paths
                 FROM search_index WHERE snapshot_rowid = ?1 LIMIT 1",
                params![snapshot_rowid],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;

        if let Some((project_hash, trigger, message, file_paths)) = row {
            conn.execute(
                "DELETE FROM search_index WHERE snapshot_rowid = ?1",
                params![snapshot_rowid],
            )?;
            conn.execute(
                "INSERT INTO search_index(snapshot_rowid, project_hash, trigger_type, message, ai_summary, file_paths)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![snapshot_rowid, project_hash, trigger, message, ai_summary, file_paths],
            )?;
        }
        Ok(())
    }

    pub fn search_snapshots(
        &self,
        query: &str,
        project_hash: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        let conn = self.conn()?;

        // Conservative query normalization for FTS5 parser safety.
        // Strip operator/syntax characters and collapse to plain terms.
        let safe = query
            .replace(
                [
                    '"', '*', ':', '(', ')', '+', '-', '^', '{', '}', '[', ']', '\\', '\'', '|',
                    '&', '!', '<', '>', '~', '$', '#', '@', ';',
                ],
                " ",
            )
            .split_whitespace()
            .filter(|w| {
                let upper = w.to_ascii_uppercase();
                // Filter FTS5 keywords and NEAR/N patterns
                !matches!(upper.as_str(), "AND" | "OR" | "NOT" | "NEAR")
                    && !upper.starts_with("NEAR/")
            })
            .collect::<Vec<_>>()
            .join(" ");
        if safe.is_empty() {
            return Ok(Vec::new());
        }

        let terms = safe
            .split_whitespace()
            .map(|t| format!("{}*", t))
            .collect::<Vec<_>>();
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let fts_query = terms.join(" ");

        // Catch FTS5 parse errors gracefully instead of propagating
        match self.search_snapshots_fts(&conn, &fts_query, project_hash, limit) {
            Ok(results) => Ok(results),
            Err(e) => {
                tracing::debug!("FTS5 query failed (malformed query?): {}", e);
                Ok(Vec::new())
            }
        }
    }

    fn search_snapshots_fts(
        &self,
        conn: &DbConn,
        fts_query: &str,
        project_hash: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        let mut out = Vec::new();

        if let Some(ph) = project_hash {
            let mut stmt = conn.prepare(
                "SELECT si.snapshot_rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.ai_summary,
                        snippet(search_index, -1, '<mark>', '</mark>', '...', 32) as match_context
                 FROM search_index si
                 JOIN snapshots s ON s.rowid = si.snapshot_rowid
                 WHERE search_index MATCH ?1 AND si.project_hash = ?2
                 ORDER BY rank
                 LIMIT ?3",
            )?;
            let rows = stmt.query_map(params![fts_query, ph, limit as i64], |row| {
                Ok(SearchResult {
                    snapshot_rowid: row.get(0)?,
                    snapshot_id: row.get::<_, i64>(1)? as u64,
                    timestamp: row.get(2)?,
                    trigger: row.get(3)?,
                    message: row.get(4)?,
                    ai_summary: row.get(5)?,
                    match_context: row.get(6)?,
                })
            })?;
            for row in rows {
                out.push(row?);
            }
        } else {
            let mut stmt = conn.prepare(
                "SELECT si.snapshot_rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.ai_summary,
                        snippet(search_index, -1, '<mark>', '</mark>', '...', 32) as match_context
                 FROM search_index si
                 JOIN snapshots s ON s.rowid = si.snapshot_rowid
                 WHERE search_index MATCH ?1
                 ORDER BY rank
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![fts_query, limit as i64], |row| {
                Ok(SearchResult {
                    snapshot_rowid: row.get(0)?,
                    snapshot_id: row.get::<_, i64>(1)? as u64,
                    timestamp: row.get(2)?,
                    trigger: row.get(3)?,
                    message: row.get(4)?,
                    ai_summary: row.get(5)?,
                    match_context: row.get(6)?,
                })
            })?;
            for row in rows {
                out.push(row?);
            }
        }

        Ok(out)
    }

    // === AI summary queue ===

    /// Enqueue a snapshot for deferred AI summary generation (idempotent per rowid).
    pub fn enqueue_ai_summary(&self, snapshot_rowid: i64, project_hash: &str) -> Result<()> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT OR IGNORE INTO pending_ai_summaries (snapshot_rowid, project_hash, queued_at, attempts)
             VALUES (?1, ?2, ?3, 0)",
            params![snapshot_rowid, project_hash, now],
        )?;
        Ok(())
    }

    /// Fetch up to `limit` oldest pending summaries across all projects.
    pub fn dequeue_pending_ai(&self, limit: u32) -> Result<Vec<PendingAiSummaryRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT snapshot_rowid, project_hash, attempts, queued_at
             FROM pending_ai_summaries
             ORDER BY queued_at ASC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(PendingAiSummaryRow {
                snapshot_rowid: row.get::<_, i64>(0)?,
                project_hash: row.get::<_, String>(1)?,
                attempts: row.get::<_, i64>(2)?,
                queued_at: row.get::<_, String>(3)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn delete_pending_ai(&self, snapshot_rowid: i64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM pending_ai_summaries WHERE snapshot_rowid = ?1",
            params![snapshot_rowid],
        )?;
        Ok(())
    }

    pub fn increment_ai_attempts(&self, snapshot_rowid: i64) -> Result<i64> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE pending_ai_summaries SET attempts = attempts + 1 WHERE snapshot_rowid = ?1",
            params![snapshot_rowid],
        )?;
        let attempts: i64 = conn
            .query_row(
                "SELECT attempts FROM pending_ai_summaries WHERE snapshot_rowid = ?1",
                params![snapshot_rowid],
                |row| row.get(0),
            )
            .unwrap_or(0);
        Ok(attempts)
    }

    /// Remove queue entries older than `ttl_days` days.
    pub fn prune_ai_queue_ttl(&self, ttl_days: i64) -> Result<u64> {
        let conn = self.conn()?;
        let cutoff = chrono::Utc::now() - chrono::Duration::days(ttl_days);
        let cutoff_s = cutoff.to_rfc3339();
        let affected = conn.execute(
            "DELETE FROM pending_ai_summaries WHERE queued_at < ?1",
            params![cutoff_s],
        )? as u64;
        Ok(affected)
    }

    /// Remove event ledger entries older than `ttl_days` to prevent unbounded growth.
    pub fn prune_event_ledger_ttl(&self, ttl_days: i64) -> Result<u64> {
        let conn = self.conn()?;
        let cutoff = chrono::Utc::now() - chrono::Duration::days(ttl_days);
        let cutoff_s = cutoff.to_rfc3339();
        let affected = conn.execute(
            "DELETE FROM event_ledger WHERE ts < ?1 AND resolved = 1",
            params![cutoff_s],
        )? as u64;
        if affected > 0 {
            tracing::debug!(
                "Pruned {} resolved event ledger entries older than {} days",
                affected,
                ttl_days
            );
        }
        Ok(affected)
    }

    // === Operations ===

    pub fn create_operation(&self, project_hash: &str, label: &str) -> Result<i64> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO operations (project_hash, label, started_at) VALUES (?1, ?2, ?3)",
            params![project_hash, label, now],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn finish_operation(
        &self,
        op_id: i64,
        first_snapshot_id: u64,
        last_snapshot_id: u64,
    ) -> Result<()> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE operations SET ended_at = ?1, first_snapshot_id = ?2, last_snapshot_id = ?3
             WHERE id = ?4",
            params![now, first_snapshot_id, last_snapshot_id, op_id],
        )?;
        Ok(())
    }

    /// Set the first snapshot id of an operation (typically at operation start)
    pub fn set_operation_first_snapshot(&self, op_id: i64, first_snapshot_id: u64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE operations SET first_snapshot_id = ?1 WHERE id = ?2",
            params![first_snapshot_id, op_id],
        )?;
        Ok(())
    }

    /// Update the last snapshot id of an operation without closing it.
    pub fn update_operation_last_snapshot(&self, op_id: i64, last_snapshot_id: u64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE operations SET last_snapshot_id = ?1 WHERE id = ?2",
            params![last_snapshot_id, op_id],
        )?;
        Ok(())
    }

    /// Set the last snapshot id and close an operation (preserves first_snapshot_id)
    pub fn close_operation_with_last(&self, op_id: i64, last_snapshot_id: u64) -> Result<()> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE operations SET ended_at = ?1, last_snapshot_id = ?2 WHERE id = ?3",
            params![now, last_snapshot_id, op_id],
        )?;
        Ok(())
    }

    pub fn get_active_operation(&self, project_hash: &str) -> Result<Option<ActiveOperationRow>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT id, label FROM operations WHERE project_hash = ?1 AND ended_at IS NULL
             ORDER BY id DESC LIMIT 1",
            params![project_hash],
            |row| {
                Ok(ActiveOperationRow {
                    id: row.get(0)?,
                    label: row.get(1)?,
                })
            },
        )
        .optional()
        .context("Failed to query active operation")
    }

    pub fn list_operations(&self, project_hash: &str) -> Result<Vec<OperationListRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, label, started_at, ended_at, first_snapshot_id, last_snapshot_id
             FROM operations WHERE project_hash = ?1 ORDER BY id DESC LIMIT 50",
        )?;
        let rows = stmt.query_map(params![project_hash], |row| {
            Ok(OperationListRow {
                id: row.get::<_, i64>(0)?,
                label: row.get::<_, String>(1)?,
                started_at: row.get::<_, String>(2)?,
                ended_at: row.get::<_, Option<String>>(3)?,
                first_snapshot_id: row.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                last_snapshot_id: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
            })
        })?;
        let mut entries = Vec::new();
        for row in rows {
            entries.push(row?);
        }
        Ok(entries)
    }

    pub fn get_latest_completed_operation(
        &self,
        project_hash: &str,
    ) -> Result<Option<CompletedOperationRow>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT id, label, first_snapshot_id, last_snapshot_id
             FROM operations
             WHERE project_hash = ?1 AND ended_at IS NOT NULL
             ORDER BY id DESC LIMIT 1",
            params![project_hash],
            |row| {
                Ok(CompletedOperationRow {
                    id: row.get::<_, i64>(0)?,
                    label: row.get::<_, String>(1)?,
                    first_snapshot_id: row.get::<_, i64>(2)? as u64,
                    last_snapshot_id: row.get::<_, i64>(3)? as u64,
                })
            },
        )
        .optional()
        .context("Failed to query latest operation")
    }

    /// Find the snapshot just before a given snapshot_id
    pub fn snapshot_before(
        &self,
        project_hash: &str,
        snapshot_id: u64,
    ) -> Result<Option<SnapshotRow>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    s.file_count as fc
             FROM snapshots s
             WHERE s.project_hash = ?1 AND s.snapshot_id < ?2
             ORDER BY s.snapshot_id DESC LIMIT 1",
            params![project_hash, snapshot_id],
            |row| {
                Ok(SnapshotRow {
                    rowid: row.get(0)?,
                    snapshot_id: row.get::<_, i64>(1)? as u64,
                    timestamp: row.get(2)?,
                    trigger: row.get(3)?,
                    message: row.get(4)?,
                    pinned: row.get::<_, i32>(5)? != 0,
                    ai_summary: row.get(6)?,
                    file_count: row.get::<_, i64>(7)? as u64,
                })
            },
        )
        .optional()
        .context("Failed to query preceding snapshot")
    }

    /// List snapshots oldest-first for pruning
    pub fn list_snapshots_oldest_first(&self, project_hash: &str) -> Result<Vec<SnapshotRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    s.file_count
             FROM snapshots s
             WHERE s.project_hash = ?1
             ORDER BY s.snapshot_id ASC",
        )?;
        let rows = stmt.query_map(params![project_hash], |row| {
            Ok(SnapshotRow {
                rowid: row.get(0)?,
                snapshot_id: row.get::<_, i64>(1)? as u64,
                timestamp: row.get(2)?,
                trigger: row.get(3)?,
                message: row.get(4)?,
                pinned: row.get::<_, i32>(5)? != 0,
                ai_summary: row.get(6)?,
                file_count: row.get::<_, i64>(7)? as u64,
            })
        })?;
        let mut snapshots = Vec::new();
        for row in rows {
            snapshots.push(row?);
        }
        Ok(snapshots)
    }

    /// Total size of stored blobs referenced by a project's snapshots (approximate, counts duplicates)
    pub fn total_blob_size_for_project(&self, project_hash: &str) -> Result<u64> {
        let conn = self.conn()?;
        let size: i64 = conn.query_row(
            "SELECT COALESCE(SUM(t.size), 0) FROM (
                SELECT DISTINCT sf.hash, sf.size AS size
                FROM snapshot_files sf
                INNER JOIN snapshots s ON sf.snapshot_rowid = s.rowid
                WHERE s.project_hash = ?1 AND sf.stored = 1
            ) t",
            params![project_hash],
            |row| row.get(0),
        )?;
        Ok(size as u64)
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
        Ok(if v < 0 { 0 } else { v as u64 })
    }

    /// Run incremental vacuum to reclaim free pages without exclusive lock.
    pub fn vacuum(&self) -> Result<()> {
        let conn = self.conn()?;
        conn.execute_batch("PRAGMA incremental_vacuum(100);")?;
        Ok(())
    }

    /// Estimate the total size of stored blobs referenced by a single snapshot
    pub fn estimate_snapshot_blob_size(&self, snapshot_rowid: i64) -> Result<u64> {
        let conn = self.conn()?;
        let size: i64 = conn.query_row(
            "SELECT COALESCE(SUM(size), 0) FROM snapshot_files WHERE snapshot_rowid = ?1 AND stored = 1",
            params![snapshot_rowid],
            |row| row.get(0),
        )?;
        Ok(size as u64)
    }

    pub fn insert_event_ledger(&self, event: &NewEventLedgerEntry) -> Result<i64> {
        let mut conn = self.conn()?;
        let ts = chrono::Utc::now().to_rfc3339();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let prev_hash = match event.prev_hash.clone() {
            Some(v) => v,
            None => self.latest_ledger_hash_with_conn(&tx)?.unwrap_or_default(),
        };
        // Insert first without chain_hash, get the canonical ID from last_insert_rowid(),
        // then compute the hash with the real ID and update in place.
        // This avoids divergence between MAX(id)+1 and AUTOINCREMENT.
        tx.execute(
            "INSERT INTO event_ledger (
                ts, source, event_type, severity, project_hash, agent_name, guard_name,
                path, detail, pre_state_ref, post_state_ref, prev_hash, causal_parent, resolved
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 0)",
            params![
                ts,
                event.source.as_str(),
                event.event_type,
                event.severity.as_str(),
                event.project_hash,
                event.agent_name,
                event.guard_name,
                event.path,
                event.detail,
                event.pre_state_ref,
                event.post_state_ref,
                "", // placeholder for chain hash
                event.causal_parent,
            ],
        )?;
        let actual_id = tx.last_insert_rowid();
        let chain_hash =
            ledger::compute_event_chain_hash_with_id(&prev_hash, actual_id, event, &ts);
        tx.execute(
            "UPDATE event_ledger SET prev_hash = ?1 WHERE id = ?2",
            params![chain_hash, actual_id],
        )?;
        tx.commit()?;
        Ok(actual_id)
    }

    pub fn insert_event_ledger_batch(&self, events: &[NewEventLedgerEntry]) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut prev_hash = self.latest_ledger_hash_with_conn(&tx)?.unwrap_or_default();
        for event in events {
            let ts = chrono::Utc::now().to_rfc3339();
            tx.execute(
                "INSERT INTO event_ledger (
                    ts, source, event_type, severity, project_hash, agent_name, guard_name,
                    path, detail, pre_state_ref, post_state_ref, prev_hash, causal_parent, resolved
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 0)",
                params![
                    ts,
                    event.source.as_str(),
                    event.event_type,
                    event.severity.as_str(),
                    event.project_hash,
                    event.agent_name,
                    event.guard_name,
                    event.path,
                    event.detail,
                    event.pre_state_ref,
                    event.post_state_ref,
                    "", // placeholder, updated after obtaining canonical row id
                    event.causal_parent,
                ],
            )?;
            let actual_id = tx.last_insert_rowid();
            let chain_hash =
                ledger::compute_event_chain_hash_with_id(&prev_hash, actual_id, event, &ts);
            tx.execute(
                "UPDATE event_ledger SET prev_hash = ?1 WHERE id = ?2",
                params![chain_hash, actual_id],
            )?;
            prev_hash = chain_hash;
        }
        tx.commit()?;
        Ok(events.len())
    }

    pub fn verify_event_ledger_chain(&self) -> Result<(usize, Vec<i64>)> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                    path, detail, pre_state_ref, post_state_ref, prev_hash, causal_parent, resolved
             FROM event_ledger ORDER BY id ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut prev_hash = String::new();
        let mut count = 0usize;
        let mut broken = Vec::new();
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let ts: String = row.get(1)?;
            let source_raw: String = row.get(2)?;
            let event_type_raw: String = row.get(3)?;
            let severity_raw: String = row.get(4)?;
            let event = NewEventLedgerEntry {
                source: LedgerSource::from_str(&source_raw).unwrap_or(LedgerSource::Daemon),
                event_type: event_type_raw.clone(),
                severity: LedgerSeverity::from_str(&severity_raw).unwrap_or(LedgerSeverity::Info),
                project_hash: row.get(5)?,
                agent_name: row.get(6)?,
                guard_name: row.get(7)?,
                path: row.get(8)?,
                detail: row.get(9)?,
                pre_state_ref: row.get(10)?,
                post_state_ref: row.get(11)?,
                prev_hash: None,
                causal_parent: row.get(13)?,
            };
            let stored_hash: Option<String> = row.get(12)?;
            let expected = ledger::compute_event_chain_hash_with_id_raw(
                &prev_hash,
                id,
                &source_raw,
                &event_type_raw,
                &severity_raw,
                &event,
                &ts,
            );
            if stored_hash.as_deref() != Some(expected.as_str()) {
                broken.push(id);
            }
            prev_hash = stored_hash.unwrap_or_default();
            count += 1;
        }
        Ok((count, broken))
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

    pub fn event_ledger_recent(
        &self,
        source: Option<&str>,
        guard_name: Option<&str>,
        agent_name: Option<&str>,
        session: Option<&str>,
        limit: usize,
    ) -> Result<Vec<EventLedgerEntry>> {
        self.event_ledger_recent_since(source, guard_name, agent_name, session, None, limit)
    }

    pub fn event_ledger_recent_since(
        &self,
        source: Option<&str>,
        guard_name: Option<&str>,
        agent_name: Option<&str>,
        session: Option<&str>,
        since: Option<&str>,
        limit: usize,
    ) -> Result<Vec<EventLedgerEntry>> {
        let conn = self.conn()?;
        let (sql, param_values) = ledger::build_recent_query(
            ledger::LedgerRecentFilters {
                source,
                guard_name,
                agent_name,
                session,
                since,
            },
            limit as i64,
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(param_values.iter()),
            ledger::map_event_ledger_entry,
        )?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn event_ledger_get(&self, id: i64) -> Result<Option<EventLedgerEntry>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                    path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
             FROM event_ledger WHERE id = ?1",
            params![id],
            ledger::map_event_ledger_entry,
        )
        .optional()
        .context("Failed to fetch ledger event")
    }

    pub fn event_ledger_trace(&self, id: i64) -> Result<EventLedgerTraceResult> {
        let mut chain = Vec::new();
        let mut current = Some(id);
        let mut guard = 0usize;
        let mut truncated = false;
        while let Some(cid) = current {
            if guard > 1024 {
                truncated = true;
                break;
            }
            if let Some(entry) = self.event_ledger_get(cid)? {
                current = entry.causal_parent;
                chain.push(entry);
            } else {
                break;
            }
            guard += 1;
        }
        Ok(EventLedgerTraceResult {
            entries: chain,
            truncated,
        })
    }

    pub fn event_ledger_mark_resolved(&self, id: i64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE event_ledger SET resolved = 1 WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// Return the event ID and any descendants linked via causal_parent.
    /// Uses a recursive CTE with a hard depth/row guard to avoid runaway recursion
    /// on malformed graphs.
    pub fn event_ledger_descendant_ids(&self, root_id: i64) -> Result<Vec<i64>> {
        let limit = 10_000i64;
        let conn = self.conn()?;
        let count: i64 = conn.query_row(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             SELECT COUNT(*) FROM descendants",
            params![root_id, limit],
            |row| row.get(0),
        )?;
        if count >= limit {
            anyhow::bail!(
                "Descendant expansion reached limit of {} entries for root event #{}",
                limit,
                root_id
            );
        }

        let mut stmt = conn.prepare(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             SELECT id FROM descendants",
        )?;
        let rows = stmt.query_map(params![root_id, limit], |row| row.get::<_, i64>(0))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn event_ledger_mark_resolved_cascade(&self, root_id: i64) -> Result<usize> {
        let limit = 10_000i64;
        let conn = self.conn()?;
        let count: i64 = conn.query_row(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             SELECT COUNT(*) FROM descendants",
            params![root_id, limit],
            |row| row.get(0),
        )?;
        if count >= limit {
            anyhow::bail!(
                "Cascade descendant expansion reached limit of {} entries for root event #{}",
                limit,
                root_id
            );
        }
        let changed = conn.execute(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             UPDATE event_ledger
             SET resolved = 1
             WHERE id IN (SELECT id FROM descendants)",
            params![root_id, limit],
        )?;
        Ok(changed)
    }

    pub fn event_ledger_mark_resolved_cascade_with_session(
        &self,
        root_id: i64,
        session_id: &str,
    ) -> Result<usize> {
        let limit = 10_000i64;
        let conn = self.conn()?;
        let count: i64 = conn.query_row(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             SELECT COUNT(*)
             FROM event_ledger
             WHERE id IN (SELECT id FROM descendants)
               AND session_id = ?3",
            params![root_id, limit, session_id],
            |row| row.get(0),
        )?;
        if count >= limit {
            anyhow::bail!(
                "Cascade descendant expansion reached limit of {} entries for root event #{}",
                limit,
                root_id
            );
        }
        let changed = conn.execute(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             UPDATE event_ledger
             SET resolved = 1
             WHERE id IN (
                 SELECT id
                 FROM event_ledger
                 WHERE id IN (SELECT id FROM descendants)
                   AND session_id = ?3
             )",
            params![root_id, limit, session_id],
        )?;
        Ok(changed)
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
            let engine = DbGuardEngine::from_str(&engine_raw).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    format!("invalid db_guard engine: {engine_raw}").into(),
                )
            })?;
            let mode = DbGuardMode::from_str(&mode_raw).ok_or_else(|| {
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

    pub fn add_agent(
        &self,
        name: &str,
        profile_path: &str,
        data_dir: Option<&str>,
    ) -> Result<()> {
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
