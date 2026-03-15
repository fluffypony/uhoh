use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};

use crate::cas::StorageMethod;

use super::{
    checked_db_u64, row_u64, CreateSnapshotRow, Database, DeletedFile, FileEntryRow,
    FileHistoryRow, SnapFileEntry, SnapshotRow, SnapshotSummary, SnapshotTrigger,
};

#[non_exhaustive]
pub struct SnapshotInsert<'a> {
    pub project_hash: &'a str,
    pub snapshot_id: u64,
    pub timestamp: &'a str,
    pub trigger: SnapshotTrigger,
    pub message: &'a str,
    pub pinned: bool,
    pub files: &'a [SnapFileEntry],
    pub deleted: &'a [DeletedFile],
}

pub fn create_snapshot_tx(
    tx: &impl std::ops::Deref<Target = Connection>,
    snapshot: SnapshotInsert<'_>,
) -> Result<i64> {
    let SnapshotInsert {
        project_hash,
        snapshot_id,
        timestamp,
        trigger,
        message,
        pinned,
        files,
        deleted,
    } = snapshot;

    tx.execute(
        "INSERT INTO snapshots (project_hash, snapshot_id, timestamp, trigger, message, pinned)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            project_hash,
            snapshot_id,
            timestamp,
            trigger.as_str(),
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
                storage_method.to_db(),
                *is_symlink as i32,
            ])?;
        }
    }

    {
        let mut del_stmt = tx.prepare(
            "INSERT INTO snapshot_deleted (snapshot_rowid, path, hash, size, stored, storage_method)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )?;
        for entry in deleted {
            del_stmt.execute(params![
                rowid,
                entry.path,
                entry.hash,
                entry.size,
                entry.stored as i32,
                entry.storage_method.to_db()
            ])?;
        }
    }

    let _ = tx.execute(
        "UPDATE snapshots SET file_count = (
            SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = ?1
        ) WHERE rowid = ?1",
        params![rowid],
    );

    Ok(rowid)
}

impl Database {
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
                let trigger_raw: String = row.get(3)?;
                Ok(SnapshotRow {
                    rowid: row.get(0)?,
                    snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                    timestamp: row.get(2)?,
                    trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
                    message: row.get(4)?,
                    pinned: row.get::<_, i32>(5)? != 0,
                    ai_summary: row.get(6)?,
                    file_count: row_u64(row, 7, "snapshots.file_count")?,
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
            let trigger_raw: String = row.get(3)?;
            Ok(SnapshotRow {
                rowid: row.get(0)?,
                snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                timestamp: row.get(2)?,
                trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
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
        checked_db_u64(size, "snapshot_files.total_size")
    }

    /// Estimate the total size of stored blobs referenced by a single snapshot
    pub fn estimate_snapshot_blob_size(&self, snapshot_rowid: i64) -> Result<u64> {
        let conn = self.conn()?;
        let size: i64 = conn.query_row(
            "SELECT COALESCE(SUM(size), 0) FROM snapshot_files WHERE snapshot_rowid = ?1 AND stored = 1",
            params![snapshot_rowid],
            |row| row.get(0),
        )?;
        checked_db_u64(size, "snapshot_files.snapshot_size")
    }

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

        let rowid = create_snapshot_tx(
            &tx,
            SnapshotInsert {
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
            let trigger_raw: String = row.get(3)?;
            Ok(SnapshotRow {
                rowid: row.get(0)?,
                snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                timestamp: row.get(2)?,
                trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
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
            let trigger_raw: String = row.get(3)?;
            Ok(SnapshotRow {
                rowid: row.get(0)?,
                snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                timestamp: row.get(2)?,
                trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
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
                    let trigger_raw: String = row.get(3)?;
                    Ok(SnapshotSummary {
                        rowid: row.get(0)?,
                        snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                        timestamp: row.get(2)?,
                        trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
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
                    let trigger_raw: String = row.get(3)?;
                    Ok(SnapshotSummary {
                        rowid: row.get(0)?,
                        snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                        timestamp: row.get(2)?,
                        trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
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
                    let trigger_raw: String = row.get(3)?;
                    Ok(SnapshotSummary {
                        rowid: row.get(0)?,
                        snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                        timestamp: row.get(2)?,
                        trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
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
                    let trigger_raw: String = row.get(3)?;
                    Ok(SnapshotSummary {
                        rowid: row.get(0)?,
                        snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                        timestamp: row.get(2)?,
                        trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
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
                let trigger_raw: String = row.get(3)?;
                Ok(SnapshotRow {
                    rowid: row.get(0)?,
                    snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                    timestamp: row.get(2)?,
                    trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
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
        let snapshot_id = crate::encoding::base58_to_id(base58_id).context("Invalid snapshot ID")?;
        let conn = self.conn()?;
        conn.query_row(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    s.file_count as fc
             FROM snapshots s
             WHERE s.project_hash = ?1 AND s.snapshot_id = ?2",
            params![project_hash, snapshot_id],
            |row| {
                let trigger_raw: String = row.get(3)?;
                Ok(SnapshotRow {
                    rowid: row.get(0)?,
                    snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
                    timestamp: row.get(2)?,
                    trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
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
            let trigger_raw: String = row.get(3)?;
            Ok(FileHistoryRow {
                snapshot_id: row_u64(row, 0, "snapshots.snapshot_id")?,
                timestamp: row.get::<_, String>(1)?,
                hash: row.get::<_, String>(2)?,
                trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
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
}
