use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::path::Path;
use std::sync::Mutex;

/// Thread-safe SQLite database wrapper.
/// SQLite with WAL mode handles concurrent readers and serialized writers.
pub struct Database {
    conn: Mutex<Connection>,
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

#[derive(Debug, Clone)]
pub struct NewEventLedgerEntry {
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
}

#[derive(Debug, Clone)]
pub struct DbGuardEntry {
    pub id: i64,
    pub name: String,
    pub engine: String,
    pub connection_ref: String,
    pub tables_csv: String,
    pub mode: String,
    pub created_at: String,
    pub last_baseline_at: Option<String>,
    pub active: bool,
}

#[derive(Debug, Clone)]
pub struct AgentEntry {
    pub id: i64,
    pub name: String,
    pub profile_path: String,
    pub profile_version: i64,
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
    pub storage_method: i64,
    pub is_symlink: bool,
}

type OperationListRow = (
    i64,
    String,
    String,
    Option<String>,
    Option<u64>,
    Option<u64>,
);

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open database: {}", path.display()))?;

        // Enable WAL mode for concurrent read/write
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        conn.execute_batch("PRAGMA busy_timeout=5000;")?;

        let db = Database {
            conn: Mutex::new(conn),
        };
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
        let src = self.conn();
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

    /// Get a connection guard, recovering from mutex poisoning.
    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        match self.conn.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!(
                    "Database mutex was poisoned (a thread panicked while holding the lock). Recovering guard."
                );
                poisoned.into_inner()
            }
        }
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn();
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap_or(0);

        if version < 1 {
            conn.execute_batch(
                "
                CREATE TABLE IF NOT EXISTS projects (
                    hash TEXT PRIMARY KEY,
                    current_path TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    next_snapshot_id INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS project_history (
                    project_hash TEXT NOT NULL REFERENCES projects(hash) ON DELETE CASCADE,
                    old_path TEXT NOT NULL,
                    changed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_hash TEXT NOT NULL REFERENCES projects(hash) ON DELETE CASCADE,
                    snapshot_id INTEGER NOT NULL CHECK (snapshot_id > 0),
                    timestamp TEXT NOT NULL,
                    trigger TEXT NOT NULL,
                    message TEXT NOT NULL DEFAULT '',
                    pinned INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(project_hash, snapshot_id)
                );

                CREATE TABLE IF NOT EXISTS snapshot_files (
                    snapshot_rowid INTEGER NOT NULL REFERENCES snapshots(rowid) ON DELETE CASCADE,
                    path TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    stored INTEGER NOT NULL DEFAULT 1,
                    executable INTEGER NOT NULL DEFAULT 0,
                    mtime INTEGER,
                    storage_method INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY (snapshot_rowid, path)
                );

                CREATE TABLE IF NOT EXISTS snapshot_deleted (
                    snapshot_rowid INTEGER NOT NULL REFERENCES snapshots(rowid) ON DELETE CASCADE,
                    path TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    stored INTEGER NOT NULL DEFAULT 1,
                    storage_method INTEGER NOT NULL DEFAULT 1,
                    PRIMARY KEY (snapshot_rowid, path)
                );

                CREATE TABLE IF NOT EXISTS snapshot_tree (
                    snapshot_rowid INTEGER NOT NULL REFERENCES snapshots(rowid) ON DELETE CASCADE,
                    dir_path TEXT NOT NULL,
                    tree_hash TEXT NOT NULL,
                    PRIMARY KEY (snapshot_rowid, dir_path)
                );

                CREATE INDEX IF NOT EXISTS idx_snapshot_project ON snapshots(project_hash, timestamp);
                CREATE INDEX IF NOT EXISTS idx_snapshot_files_hash ON snapshot_files(hash);
                CREATE INDEX IF NOT EXISTS idx_snapshot_deleted_hash ON snapshot_deleted(hash);
                CREATE INDEX IF NOT EXISTS idx_file_path ON snapshot_files(path, snapshot_rowid);

                PRAGMA user_version = 1;
                "
            )?;
        }

        if version < 2 {
            let _ = conn.execute_batch("ALTER TABLE snapshots ADD COLUMN ai_summary TEXT;");
            conn.execute_batch("PRAGMA user_version = 2;")?;
        }

        if version < 3 {
            let _ = conn.execute_batch(
                "ALTER TABLE snapshots ADD COLUMN file_count INTEGER NOT NULL DEFAULT 0;",
            );
            let _ = conn.execute_batch(
                "ALTER TABLE snapshot_files ADD COLUMN is_symlink INTEGER NOT NULL DEFAULT 0;",
            );
            conn.execute_batch("PRAGMA user_version = 3;")?;
        }

        if version < 4 {
            conn.execute_batch(
                "
                CREATE TABLE IF NOT EXISTS operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_hash TEXT NOT NULL REFERENCES projects(hash) ON DELETE CASCADE,
                    label TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    first_snapshot_id INTEGER,
                    last_snapshot_id INTEGER
                );

                CREATE TABLE IF NOT EXISTS pending_ai_summaries (
                    snapshot_rowid INTEGER PRIMARY KEY REFERENCES snapshots(rowid) ON DELETE CASCADE,
                    project_hash   TEXT NOT NULL,
                    queued_at      TEXT NOT NULL,
                    attempts       INTEGER NOT NULL DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_operations_project ON operations(project_hash);
                CREATE INDEX IF NOT EXISTS idx_ai_queue_time ON pending_ai_summaries(queued_at);

                PRAGMA user_version = 4;
                "
            )?;
        }

        if version < 5 {
            conn.execute_batch(
                "
                CREATE TABLE IF NOT EXISTS stats (
                    key   TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0
                );
                INSERT OR IGNORE INTO stats (key, value) VALUES ('blob_bytes', 0);
                PRAGMA user_version = 5;
                ",
            )?;
        }

        if version < 6 {
            conn.execute_batch(
                "
                CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
                    snapshot_rowid UNINDEXED,
                    project_hash,
                    trigger_type,
                    message,
                    ai_summary,
                    file_paths
                );
                PRAGMA user_version = 6;
                ",
            )?;
        }

        if version < 7 {
            conn.execute_batch(
                "
                BEGIN;

                CREATE TABLE IF NOT EXISTS event_ledger (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    source TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    severity TEXT NOT NULL DEFAULT 'info',
                    project_hash TEXT,
                    agent_name TEXT,
                    guard_name TEXT,
                    path TEXT,
                    detail TEXT,
                    pre_state_ref TEXT,
                    post_state_ref TEXT,
                    causal_parent INTEGER REFERENCES event_ledger(id),
                    resolved INTEGER NOT NULL DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_event_ledger_ts ON event_ledger(ts);
                CREATE INDEX IF NOT EXISTS idx_event_ledger_source ON event_ledger(source);
                CREATE INDEX IF NOT EXISTS idx_event_ledger_agent ON event_ledger(agent_name);
                CREATE INDEX IF NOT EXISTS idx_event_ledger_guard ON event_ledger(guard_name);
                CREATE INDEX IF NOT EXISTS idx_event_ledger_path ON event_ledger(path);
                CREATE INDEX IF NOT EXISTS idx_event_ledger_causal ON event_ledger(causal_parent);
                CREATE INDEX IF NOT EXISTS idx_event_ledger_severity ON event_ledger(severity);

                CREATE TABLE IF NOT EXISTS db_guards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    engine TEXT NOT NULL,
                    connection_ref TEXT NOT NULL,
                    tables_csv TEXT NOT NULL,
                    mode TEXT NOT NULL DEFAULT 'triggers',
                    created_at TEXT NOT NULL,
                    last_baseline_at TEXT,
                    active INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS agents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    profile_path TEXT NOT NULL,
                    profile_version INTEGER NOT NULL DEFAULT 1,
                    data_dir TEXT,
                    registered_at TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1
                );

                PRAGMA user_version = 7;

                COMMIT;
                ",
            )?;
        }

        Ok(())
    }

    // === Projects ===

    pub fn add_project(&self, hash: &str, path: &str) -> Result<()> {
        let conn = self.conn();
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO projects (hash, current_path, created_at) VALUES (?1, ?2, ?3)",
            params![hash, path, now],
        )?;
        Ok(())
    }

    pub fn get_project(&self, hash: &str) -> Result<Option<ProjectEntry>> {
        let conn = self.conn();
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
        let conn = self.conn();
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
        let conn = self.conn();
        // Escape SQL wildcards in user-provided prefix
        let mut esc = String::new();
        for ch in prefix.chars() {
            match ch {
                '%' | '_' => {
                    esc.push('[');
                    esc.push(ch);
                    esc.push(']');
                }
                _ => esc.push(ch),
            }
        }
        let pattern = format!("{esc}%");

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM projects WHERE hash LIKE ?1",
            params![pattern.clone()],
            |row| row.get(0),
        )?;

        if count > 1 {
            anyhow::bail!(
                "Ambiguous hash prefix '{prefix}' matches {count} projects. Use a longer prefix."
            );
        }

        conn.query_row(
            "SELECT hash, current_path, created_at FROM projects WHERE hash LIKE ?1 LIMIT 1",
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
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let now = chrono::Utc::now().to_rfc3339();
        let old_path: Option<String> = tx
            .query_row(
                "SELECT current_path FROM projects WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(old) = old_path {
            if old != new_path {
                tx.execute(
                    "INSERT INTO project_history (project_hash, old_path, changed_at) VALUES (?1, ?2, ?3)",
                    params![hash, old, now],
                )?;
            }
        }
        tx.execute(
            "UPDATE projects SET current_path = ?1 WHERE hash = ?2",
            params![new_path, hash],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn remove_project(&self, hash: &str) -> Result<()> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM search_index WHERE project_hash = ?1",
            params![hash],
        )?;
        conn.execute("DELETE FROM projects WHERE hash = ?1", params![hash])?;
        Ok(())
    }

    pub fn list_projects(&self) -> Result<Vec<ProjectEntry>> {
        let conn = self.conn();
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

    pub fn next_snapshot_id(&self, project_hash: &str) -> Result<u64> {
        // Kept for tests; prefer create_snapshot_with_alloc to avoid races
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let id: u64 = tx.query_row(
            "SELECT next_snapshot_id FROM projects WHERE hash = ?1",
            params![project_hash],
            |row| row.get(0),
        )?;
        tx.execute(
            "UPDATE projects SET next_snapshot_id = ?1 WHERE hash = ?2",
            params![id + 1, project_hash],
        )?;
        tx.commit()?;
        Ok(id)
    }

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
        tree_hashes: &[TreeHash],
    ) -> Result<(i64, u64)> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        // Allocate snapshot_id inside the same transaction if zero passed
        let snapshot_id = if snapshot_id == 0 {
            let id: u64 = tx.query_row(
                "SELECT next_snapshot_id FROM projects WHERE hash = ?1",
                params![project_hash],
                |row| row.get(0),
            )?;
            tx.execute(
                "UPDATE projects SET next_snapshot_id = ?1 WHERE hash = ?2",
                params![id + 1, project_hash],
            )?;
            id
        } else {
            snapshot_id
        };

        tx.execute(
            "INSERT INTO snapshots (project_hash, snapshot_id, timestamp, trigger, message, pinned)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                project_hash,
                snapshot_id,
                timestamp,
                trigger,
                message,
                pinned as i32
            ],
        )?;
        let rowid = tx.last_insert_rowid();

        {
            let mut file_stmt = tx.prepare(
                "INSERT INTO snapshot_files (snapshot_rowid, path, hash, size, stored, executable, mtime, storage_method, is_symlink)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?;
            for SnapFileEntry {
                path,
                hash,
                size,
                stored,
                executable,
                mtime,
                storage_method,
                is_symlink,
            } in files
            {
                file_stmt.execute(params![
                    rowid,
                    path,
                    hash,
                    size,
                    *stored as i32,
                    *executable as i32,
                    mtime,
                    storage_method,
                    *is_symlink as i32,
                ])?;
            }
        }

        {
            let mut del_stmt = tx.prepare(
                "INSERT INTO snapshot_deleted (snapshot_rowid, path, hash, size, stored, storage_method)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for (path, hash, size, stored, storage_method) in deleted {
                del_stmt.execute(params![
                    rowid,
                    path,
                    hash,
                    size,
                    *stored as i32,
                    storage_method
                ])?;
            }
        }

        {
            let mut tree_stmt = tx.prepare(
                "INSERT INTO snapshot_tree (snapshot_rowid, dir_path, tree_hash)
                 VALUES (?1, ?2, ?3)",
            )?;
            for (dir_path, tree_hash) in tree_hashes {
                tree_stmt.execute(params![rowid, dir_path, tree_hash])?;
            }
        }

        // Update denormalized file_count if column exists (schema v2)
        let _ = tx.execute(
            "UPDATE snapshots SET file_count = (
                SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = ?1
            ) WHERE rowid = ?1",
            params![rowid],
        );

        tx.commit()?;
        Ok((rowid, snapshot_id))
    }

    pub fn list_snapshots(&self, project_hash: &str) -> Result<Vec<SnapshotRow>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    COALESCE(s.file_count, (SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = s.rowid)) as file_count
             FROM snapshots s
             WHERE s.project_hash = ?1
             ORDER BY s.timestamp DESC",
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
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    COALESCE(s.file_count, (SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = s.rowid)) as file_count
             FROM snapshots s
             WHERE s.project_hash = ?1
             ORDER BY s.timestamp DESC
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
        let conn = self.conn();
        match (from_ts, to_ts) {
            (None, None) => {
                let mut stmt = conn.prepare(
                    "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                            COALESCE(s.file_count, (SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = s.rowid)) as file_count
                     FROM snapshots s
                     WHERE s.project_hash = ?1
                     ORDER BY s.timestamp DESC",
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
                            COALESCE(s.file_count, (SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = s.rowid)) as file_count
                     FROM snapshots s
                     WHERE s.project_hash = ?1 AND s.timestamp >= ?2
                     ORDER BY s.timestamp DESC",
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
                            COALESCE(s.file_count, (SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = s.rowid)) as file_count
                     FROM snapshots s
                     WHERE s.project_hash = ?1 AND s.timestamp <= ?2
                     ORDER BY s.timestamp DESC",
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
                            COALESCE(s.file_count, (SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = s.rowid)) as file_count
                     FROM snapshots s
                     WHERE s.project_hash = ?1 AND s.timestamp >= ?2 AND s.timestamp <= ?3
                     ORDER BY s.timestamp DESC",
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
        let conn = self.conn();
        conn
            .query_row(
                "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                        s.ai_summary,
                        COALESCE(s.file_count, (SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = s.rowid)) as file_count
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
        let conn = self.conn();
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
        let conn = self.conn();
        conn.query_row(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    COALESCE(s.file_count, (SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = s.rowid)) as fc
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
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT path, hash, size, stored, executable, mtime, storage_method, COALESCE(is_symlink, 0)
             FROM snapshot_files WHERE snapshot_rowid = ?1",
        )?;
        let rows = stmt.query_map(params![snapshot_rowid], |row| {
            let path: String = row.get(0)?;
            let hash: String = row.get(1)?;
            let size = row.get::<_, i64>(2)? as u64;
            let stored = row.get::<_, i32>(3)? != 0;
            let executable = row.get::<_, i32>(4)? != 0;
            let mtime = row.get::<_, Option<i64>>(5).ok().flatten();
            let storage_method = row.get::<_, i64>(6).unwrap_or(1);
            let is_symlink = row.get::<_, Option<i32>>(7).ok().flatten().unwrap_or(0) != 0;
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
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT path, hash, size, stored, storage_method FROM snapshot_deleted WHERE snapshot_rowid = ?1",
        )?;
        let rows = stmt.query_map(params![snapshot_rowid], |row| {
            let path: String = row.get(0)?;
            let hash: String = row.get(1)?;
            let size = row.get::<_, i64>(2)? as u64;
            let stored = row.get::<_, i32>(3)? != 0;
            let storage_method = row.get::<_, i64>(4).unwrap_or(1);
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
        let conn = self.conn();
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
    ) -> Result<Vec<(u64, String, String, String)>> {
        // Returns (snapshot_id, timestamp, hash, trigger)
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT s.snapshot_id, s.timestamp, f.hash, s.trigger
             FROM snapshot_files f
             JOIN snapshots s ON s.rowid = f.snapshot_rowid
             WHERE s.project_hash = ?1 AND f.path = ?2
             ORDER BY s.snapshot_id DESC",
        )?;
        let rows = stmt.query_map(params![project_hash, file_path], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let mut entries = Vec::new();
        for row in rows {
            entries.push(row?);
        }
        Ok(entries)
    }

    /// All blob hashes referenced by any snapshot
    pub fn all_referenced_blob_hashes(&self) -> Result<std::collections::HashSet<String>> {
        let conn = self.conn();
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
        Ok(set)
    }

    /// Delete old snapshots (used by compaction)
    pub fn delete_snapshot(&self, rowid: i64) -> Result<()> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM search_index WHERE snapshot_rowid = ?1",
            params![rowid],
        )?;
        conn.execute("DELETE FROM snapshots WHERE rowid = ?1", params![rowid])?;
        Ok(())
    }

    pub fn set_ai_summary(&self, snapshot_rowid: i64, summary: &str) -> Result<()> {
        let conn = self.conn();
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
        let conn = self.conn();
        conn.execute(
            "INSERT INTO search_index(snapshot_rowid, project_hash, trigger_type, message, ai_summary, file_paths)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![snapshot_rowid, project_hash, trigger, message, ai_summary, file_paths],
        )?;
        Ok(())
    }

    pub fn update_search_index_summary(&self, snapshot_rowid: i64, ai_summary: &str) -> Result<()> {
        let conn = self.conn();
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
        let conn = self.conn();

        // conservative query normalization for FTS parser safety
        let safe = query.replace(['"', '*', ':'], " ").trim().to_string();
        if safe.is_empty() {
            return Ok(Vec::new());
        }

        let fts_query = format!("\"{safe}\"*");
        let mut out = Vec::new();

        if let Some(ph) = project_hash {
            let mut stmt = conn.prepare(
                "SELECT si.snapshot_rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.ai_summary,
                        snippet(search_index, 3, '<mark>', '</mark>', '...', 32) as match_context
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
                        snippet(search_index, 3, '<mark>', '</mark>', '...', 32) as match_context
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
        let conn = self.conn();
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT OR IGNORE INTO pending_ai_summaries (snapshot_rowid, project_hash, queued_at, attempts)
             VALUES (?1, ?2, ?3, 0)",
            params![snapshot_rowid, project_hash, now],
        )?;
        Ok(())
    }

    /// Fetch up to `limit` oldest pending summaries across all projects.
    pub fn dequeue_pending_ai(&self, limit: u32) -> Result<Vec<(i64, String, i64, String)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT snapshot_rowid, project_hash, attempts, queued_at
             FROM pending_ai_summaries
             ORDER BY queued_at ASC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn delete_pending_ai(&self, snapshot_rowid: i64) -> Result<()> {
        let conn = self.conn();
        conn.execute(
            "DELETE FROM pending_ai_summaries WHERE snapshot_rowid = ?1",
            params![snapshot_rowid],
        )?;
        Ok(())
    }

    pub fn increment_ai_attempts(&self, snapshot_rowid: i64) -> Result<i64> {
        let conn = self.conn();
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
        let conn = self.conn();
        let cutoff = chrono::Utc::now() - chrono::Duration::days(ttl_days);
        let cutoff_s = cutoff.to_rfc3339();
        let affected = conn.execute(
            "DELETE FROM pending_ai_summaries WHERE queued_at < ?1",
            params![cutoff_s],
        )? as u64;
        Ok(affected)
    }

    // === Operations ===

    pub fn create_operation(&self, project_hash: &str, label: &str) -> Result<i64> {
        let conn = self.conn();
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
        let conn = self.conn();
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
        let conn = self.conn();
        conn.execute(
            "UPDATE operations SET first_snapshot_id = ?1 WHERE id = ?2",
            params![first_snapshot_id, op_id],
        )?;
        Ok(())
    }

    /// Update the last snapshot id of an operation without closing it.
    pub fn update_operation_last_snapshot(&self, op_id: i64, last_snapshot_id: u64) -> Result<()> {
        let conn = self.conn();
        conn.execute(
            "UPDATE operations SET last_snapshot_id = ?1 WHERE id = ?2",
            params![last_snapshot_id, op_id],
        )?;
        Ok(())
    }

    /// Set the last snapshot id and close an operation (preserves first_snapshot_id)
    pub fn close_operation_with_last(&self, op_id: i64, last_snapshot_id: u64) -> Result<()> {
        let conn = self.conn();
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE operations SET ended_at = ?1, last_snapshot_id = ?2 WHERE id = ?3",
            params![now, last_snapshot_id, op_id],
        )?;
        Ok(())
    }

    pub fn get_active_operation(&self, project_hash: &str) -> Result<Option<(i64, String)>> {
        let conn = self.conn();
        conn.query_row(
            "SELECT id, label FROM operations WHERE project_hash = ?1 AND ended_at IS NULL
             ORDER BY id DESC LIMIT 1",
            params![project_hash],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .context("Failed to query active operation")
    }

    pub fn list_operations(&self, project_hash: &str) -> Result<Vec<OperationListRow>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, label, started_at, ended_at, first_snapshot_id, last_snapshot_id
             FROM operations WHERE project_hash = ?1 ORDER BY id DESC LIMIT 50",
        )?;
        let rows = stmt.query_map(params![project_hash], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
            ))
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
    ) -> Result<Option<(i64, String, u64, u64)>> {
        let conn = self.conn();
        conn.query_row(
            "SELECT id, label, first_snapshot_id, last_snapshot_id
             FROM operations
             WHERE project_hash = ?1 AND ended_at IS NOT NULL
             ORDER BY id DESC LIMIT 1",
            params![project_hash],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)? as u64,
                    row.get::<_, i64>(3)? as u64,
                ))
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
        let conn = self.conn();
        conn.query_row(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    COALESCE(s.file_count, (SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = s.rowid)) as fc
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
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    COALESCE(s.file_count, (SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = s.rowid)) as file_count
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
        let conn = self.conn();
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
        let conn = self.conn();
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
        let conn = self.conn();
        let v: i64 = conn
            .query_row(
                "SELECT value FROM stats WHERE key = 'blob_bytes'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(0);
        Ok(if v < 0 { 0 } else { v as u64 })
    }

    /// Run VACUUM to reclaim free pages; should be scheduled during idle.
    pub fn vacuum(&self) -> Result<()> {
        let conn = self.conn();
        conn.execute_batch("VACUUM;")?;
        Ok(())
    }

    /// Estimate the total size of stored blobs referenced by a single snapshot
    pub fn estimate_snapshot_blob_size(&self, snapshot_rowid: i64) -> Result<u64> {
        let conn = self.conn();
        let size: i64 = conn.query_row(
            "SELECT COALESCE(SUM(size), 0) FROM snapshot_files WHERE snapshot_rowid = ?1 AND stored = 1",
            params![snapshot_rowid],
            |row| row.get(0),
        )?;
        Ok(size as u64)
    }

    pub fn insert_event_ledger(&self, event: &NewEventLedgerEntry) -> Result<i64> {
        let conn = self.conn();
        let ts = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO event_ledger (
                ts, source, event_type, severity, project_hash, agent_name, guard_name,
                path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 0)",
            params![
                ts,
                event.source,
                event.event_type,
                event.severity,
                event.project_hash,
                event.agent_name,
                event.guard_name,
                event.path,
                event.detail,
                event.pre_state_ref,
                event.post_state_ref,
                event.causal_parent,
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn event_ledger_recent(
        &self,
        source: Option<&str>,
        guard_name: Option<&str>,
        agent_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<EventLedgerEntry>> {
        let conn = self.conn();
        let mut out = Vec::new();
        let cap = limit.max(1) as i64;
        match (source, guard_name, agent_name) {
            (Some(src), Some(guard), Some(agent)) => {
                let mut stmt = conn.prepare(
                    "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                            path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
                     FROM event_ledger
                     WHERE source = ?1 AND guard_name = ?2 AND agent_name = ?3
                     ORDER BY id DESC LIMIT ?4",
                )?;
                let rows = stmt.query_map(params![src, guard, agent, cap], |row| {
                    Ok(EventLedgerEntry {
                        id: row.get(0)?,
                        ts: row.get(1)?,
                        source: row.get(2)?,
                        event_type: row.get(3)?,
                        severity: row.get(4)?,
                        project_hash: row.get(5)?,
                        agent_name: row.get(6)?,
                        guard_name: row.get(7)?,
                        path: row.get(8)?,
                        detail: row.get(9)?,
                        pre_state_ref: row.get(10)?,
                        post_state_ref: row.get(11)?,
                        causal_parent: row.get(12)?,
                        resolved: row.get::<_, i32>(13)? != 0,
                    })
                })?;
                for row in rows {
                    out.push(row?);
                }
            }
            (Some(src), Some(guard), None) => {
                let mut stmt = conn.prepare(
                    "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                            path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
                     FROM event_ledger
                     WHERE source = ?1 AND guard_name = ?2
                     ORDER BY id DESC LIMIT ?3",
                )?;
                let rows = stmt.query_map(params![src, guard, cap], |row| {
                    Ok(EventLedgerEntry {
                        id: row.get(0)?,
                        ts: row.get(1)?,
                        source: row.get(2)?,
                        event_type: row.get(3)?,
                        severity: row.get(4)?,
                        project_hash: row.get(5)?,
                        agent_name: row.get(6)?,
                        guard_name: row.get(7)?,
                        path: row.get(8)?,
                        detail: row.get(9)?,
                        pre_state_ref: row.get(10)?,
                        post_state_ref: row.get(11)?,
                        causal_parent: row.get(12)?,
                        resolved: row.get::<_, i32>(13)? != 0,
                    })
                })?;
                for row in rows {
                    out.push(row?);
                }
            }
            (Some(src), None, Some(agent)) => {
                let mut stmt = conn.prepare(
                    "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                            path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
                     FROM event_ledger
                     WHERE source = ?1 AND agent_name = ?2
                     ORDER BY id DESC LIMIT ?3",
                )?;
                let rows = stmt.query_map(params![src, agent, cap], |row| {
                    Ok(EventLedgerEntry {
                        id: row.get(0)?,
                        ts: row.get(1)?,
                        source: row.get(2)?,
                        event_type: row.get(3)?,
                        severity: row.get(4)?,
                        project_hash: row.get(5)?,
                        agent_name: row.get(6)?,
                        guard_name: row.get(7)?,
                        path: row.get(8)?,
                        detail: row.get(9)?,
                        pre_state_ref: row.get(10)?,
                        post_state_ref: row.get(11)?,
                        causal_parent: row.get(12)?,
                        resolved: row.get::<_, i32>(13)? != 0,
                    })
                })?;
                for row in rows {
                    out.push(row?);
                }
            }
            (None, Some(guard), Some(agent)) => {
                let mut stmt = conn.prepare(
                    "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                            path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
                     FROM event_ledger
                     WHERE guard_name = ?1 AND agent_name = ?2
                     ORDER BY id DESC LIMIT ?3",
                )?;
                let rows = stmt.query_map(params![guard, agent, cap], |row| {
                    Ok(EventLedgerEntry {
                        id: row.get(0)?,
                        ts: row.get(1)?,
                        source: row.get(2)?,
                        event_type: row.get(3)?,
                        severity: row.get(4)?,
                        project_hash: row.get(5)?,
                        agent_name: row.get(6)?,
                        guard_name: row.get(7)?,
                        path: row.get(8)?,
                        detail: row.get(9)?,
                        pre_state_ref: row.get(10)?,
                        post_state_ref: row.get(11)?,
                        causal_parent: row.get(12)?,
                        resolved: row.get::<_, i32>(13)? != 0,
                    })
                })?;
                for row in rows {
                    out.push(row?);
                }
            }
            (Some(src), None, None) => {
                let mut stmt = conn.prepare(
                    "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                            path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
                     FROM event_ledger
                     WHERE source = ?1
                     ORDER BY id DESC LIMIT ?2",
                )?;
                let rows = stmt.query_map(params![src, cap], |row| {
                    Ok(EventLedgerEntry {
                        id: row.get(0)?,
                        ts: row.get(1)?,
                        source: row.get(2)?,
                        event_type: row.get(3)?,
                        severity: row.get(4)?,
                        project_hash: row.get(5)?,
                        agent_name: row.get(6)?,
                        guard_name: row.get(7)?,
                        path: row.get(8)?,
                        detail: row.get(9)?,
                        pre_state_ref: row.get(10)?,
                        post_state_ref: row.get(11)?,
                        causal_parent: row.get(12)?,
                        resolved: row.get::<_, i32>(13)? != 0,
                    })
                })?;
                for row in rows {
                    out.push(row?);
                }
            }
            (None, Some(guard), None) => {
                let mut stmt = conn.prepare(
                    "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                            path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
                     FROM event_ledger
                     WHERE guard_name = ?1
                     ORDER BY id DESC LIMIT ?2",
                )?;
                let rows = stmt.query_map(params![guard, cap], |row| {
                    Ok(EventLedgerEntry {
                        id: row.get(0)?,
                        ts: row.get(1)?,
                        source: row.get(2)?,
                        event_type: row.get(3)?,
                        severity: row.get(4)?,
                        project_hash: row.get(5)?,
                        agent_name: row.get(6)?,
                        guard_name: row.get(7)?,
                        path: row.get(8)?,
                        detail: row.get(9)?,
                        pre_state_ref: row.get(10)?,
                        post_state_ref: row.get(11)?,
                        causal_parent: row.get(12)?,
                        resolved: row.get::<_, i32>(13)? != 0,
                    })
                })?;
                for row in rows {
                    out.push(row?);
                }
            }
            (None, None, Some(agent)) => {
                let mut stmt = conn.prepare(
                    "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                            path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
                     FROM event_ledger
                     WHERE agent_name = ?1
                     ORDER BY id DESC LIMIT ?2",
                )?;
                let rows = stmt.query_map(params![agent, cap], |row| {
                    Ok(EventLedgerEntry {
                        id: row.get(0)?,
                        ts: row.get(1)?,
                        source: row.get(2)?,
                        event_type: row.get(3)?,
                        severity: row.get(4)?,
                        project_hash: row.get(5)?,
                        agent_name: row.get(6)?,
                        guard_name: row.get(7)?,
                        path: row.get(8)?,
                        detail: row.get(9)?,
                        pre_state_ref: row.get(10)?,
                        post_state_ref: row.get(11)?,
                        causal_parent: row.get(12)?,
                        resolved: row.get::<_, i32>(13)? != 0,
                    })
                })?;
                for row in rows {
                    out.push(row?);
                }
            }
            (None, None, None) => {
                let mut stmt = conn.prepare(
                    "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                            path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
                     FROM event_ledger
                     ORDER BY id DESC LIMIT ?1",
                )?;
                let rows = stmt.query_map(params![cap], |row| {
                    Ok(EventLedgerEntry {
                        id: row.get(0)?,
                        ts: row.get(1)?,
                        source: row.get(2)?,
                        event_type: row.get(3)?,
                        severity: row.get(4)?,
                        project_hash: row.get(5)?,
                        agent_name: row.get(6)?,
                        guard_name: row.get(7)?,
                        path: row.get(8)?,
                        detail: row.get(9)?,
                        pre_state_ref: row.get(10)?,
                        post_state_ref: row.get(11)?,
                        causal_parent: row.get(12)?,
                        resolved: row.get::<_, i32>(13)? != 0,
                    })
                })?;
                for row in rows {
                    out.push(row?);
                }
            }
        }
        Ok(out)
    }

    pub fn event_ledger_get(&self, id: i64) -> Result<Option<EventLedgerEntry>> {
        let conn = self.conn();
        conn.query_row(
            "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                    path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
             FROM event_ledger WHERE id = ?1",
            params![id],
            |row| {
                Ok(EventLedgerEntry {
                    id: row.get(0)?,
                    ts: row.get(1)?,
                    source: row.get(2)?,
                    event_type: row.get(3)?,
                    severity: row.get(4)?,
                    project_hash: row.get(5)?,
                    agent_name: row.get(6)?,
                    guard_name: row.get(7)?,
                    path: row.get(8)?,
                    detail: row.get(9)?,
                    pre_state_ref: row.get(10)?,
                    post_state_ref: row.get(11)?,
                    causal_parent: row.get(12)?,
                    resolved: row.get::<_, i32>(13)? != 0,
                })
            },
        )
        .optional()
        .context("Failed to fetch ledger event")
    }

    pub fn event_ledger_trace(&self, id: i64) -> Result<Vec<EventLedgerEntry>> {
        let mut chain = Vec::new();
        let mut current = Some(id);
        let mut guard = 0usize;
        while let Some(cid) = current {
            if guard > 1024 {
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
        Ok(chain)
    }

    pub fn event_ledger_mark_resolved(&self, id: i64) -> Result<()> {
        let conn = self.conn();
        conn.execute(
            "UPDATE event_ledger SET resolved = 1 WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    pub fn event_ledger_descendant_ids(&self, root_id: i64) -> Result<Vec<i64>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "WITH RECURSIVE descendants(id) AS (
                 SELECT ?1
                 UNION ALL
                 SELECT e.id
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
             )
             SELECT id FROM descendants",
        )?;

        let rows = stmt.query_map(params![root_id], |row| row.get::<_, i64>(0))?;
        let mut out = Vec::new();
        for id in rows {
            out.push(id?);
        }
        Ok(out)
    }

    pub fn add_db_guard(
        &self,
        name: &str,
        engine: &str,
        connection_ref: &str,
        tables_csv: &str,
        mode: &str,
    ) -> Result<()> {
        let conn = self.conn();
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO db_guards (name, engine, connection_ref, tables_csv, mode, created_at, active)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
            params![name, engine, connection_ref, tables_csv, mode, now],
        )?;
        Ok(())
    }

    pub fn list_db_guards(&self) -> Result<Vec<DbGuardEntry>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, name, engine, connection_ref, tables_csv, mode, created_at, last_baseline_at, active
             FROM db_guards ORDER BY id ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(DbGuardEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                engine: row.get(2)?,
                connection_ref: row.get(3)?,
                tables_csv: row.get(4)?,
                mode: row.get(5)?,
                created_at: row.get(6)?,
                last_baseline_at: row.get(7)?,
                active: row.get::<_, i32>(8)? != 0,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn remove_db_guard(&self, name: &str) -> Result<()> {
        let conn = self.conn();
        conn.execute("DELETE FROM db_guards WHERE name = ?1", params![name])?;
        Ok(())
    }

    pub fn set_db_guard_baseline_time(&self, name: &str, ts: &str) -> Result<()> {
        let conn = self.conn();
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
        profile_version: i64,
        data_dir: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn();
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO agents (name, profile_path, profile_version, data_dir, registered_at, active)
             VALUES (?1, ?2, ?3, ?4, ?5, 1)",
            params![name, profile_path, profile_version, data_dir, now],
        )?;
        Ok(())
    }

    pub fn list_agents(&self) -> Result<Vec<AgentEntry>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, name, profile_path, profile_version, data_dir, registered_at, active
             FROM agents ORDER BY id ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(AgentEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                profile_path: row.get(2)?,
                profile_version: row.get(3)?,
                data_dir: row.get(4)?,
                registered_at: row.get(5)?,
                active: row.get::<_, i32>(6)? != 0,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn remove_agent(&self, name: &str) -> Result<()> {
        let conn = self.conn();
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
    pub storage_method: i64,
    pub is_symlink: bool,
}
pub type DeletedFile = (String, String, u64, bool, i64); // (path, hash, size, stored, storage_method)
pub type TreeHash = (String, String); // (dir_path, tree_hash)
