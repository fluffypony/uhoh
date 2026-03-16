use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};

use crate::cas::StorageMethod;

use super::{
    checked_db_u64, row_u64, CreateSnapshotRow, Database, DeletedFile, FileEntryRow,
    FileHistoryRow, SnapFileEntry, SnapshotRow, SnapshotSummary, SnapshotTrigger,
};

#[non_exhaustive]
#[derive(Clone, Copy)]
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

/// Insert a snapshot and its file/deleted entries within an existing transaction.
///
/// # Errors
///
/// Returns an error if the database operation fails.
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
            i32::from(pinned)
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
                i32::from(*stored),
                i32::from(*executable),
                mtime,
                storage_method.to_db(),
                i32::from(*is_symlink),
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
                i32::from(entry.stored),
                entry.storage_method.to_db()
            ])?;
        }
    }

    // Denormalized file_count is a performance cache; failure is non-fatal (count falls back to a subquery).
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn pin_snapshot(&self, rowid: i64, pinned: bool) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE snapshots SET pinned = ?1 WHERE rowid = ?2",
            params![i32::from(pinned), rowid],
        )?;
        Ok(())
    }

    /// Delete old snapshots (used by compaction)
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn delete_snapshot(&self, rowid: i64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM search_index WHERE snapshot_rowid = ?1",
            params![rowid],
        )?;
        conn.execute("DELETE FROM snapshots WHERE rowid = ?1", params![rowid])?;
        Ok(())
    }

    /// Find the snapshot just before a given `snapshot_id`
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn list_snapshots_oldest_first(&self, project_hash: &str) -> Result<Vec<SnapshotRow>> {
        self.list_snapshots_with_order(project_hash, "ORDER BY s.snapshot_id ASC")
    }

    /// Total size of stored blobs referenced by a project's snapshots (approximate, counts duplicates)
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails or the transaction cannot be committed.
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

    /// List all snapshots for a project, newest first.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn list_snapshots(&self, project_hash: &str) -> Result<Vec<SnapshotRow>> {
        self.list_snapshots_with_order(project_hash, "ORDER BY s.timestamp DESC, s.rowid DESC")
    }

    fn list_snapshots_with_order(
        &self,
        project_hash: &str,
        order_clause: &str,
    ) -> Result<Vec<SnapshotRow>> {
        let conn = self.conn()?;
        let sql = format!(
            "SELECT s.rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.pinned,
                    s.ai_summary,
                    s.file_count
             FROM snapshots s
             WHERE s.project_hash = ?1
             {order_clause}"
        );
        let mut stmt = conn.prepare(&sql)?;
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

    /// List snapshots for a project with pagination, newest first.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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
        #[allow(clippy::cast_possible_wrap)] // limit and offset are query parameters, never large enough to wrap i64
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

    /// List snapshot summaries for a project, optionally filtered by timestamp range.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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

    /// Count the number of snapshots for a project.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn snapshot_count(&self, project_hash: &str) -> Result<u64> {
        let conn = self.conn()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM snapshots WHERE project_hash = ?1",
            params![project_hash],
            |row| row.get(0),
        )?;
        checked_db_u64(count, "snapshots.count")
    }

    /// Find a snapshot by its base58-encoded snapshot ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the base58 ID is invalid or the database operation fails.
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

    /// Retrieve all file entries for a snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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

    /// Retrieve all deleted file entries for a snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn blob_bytes(&self) -> Result<u64> {
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn vacuum(&self) -> Result<()> {
        let conn = self.conn()?;
        conn.execute_batch("PRAGMA incremental_vacuum(100);")?;
        Ok(())
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::StorageMethod;

    fn temp_db() -> (Database, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = Database::open(&db_path).unwrap();
        // Insert a project so FK constraints are satisfied
        let conn = db.conn().unwrap();
        conn.execute(
            "INSERT INTO projects (hash, current_path, created_at) VALUES ('proj1', '/tmp/proj', '2026-01-01T00:00:00Z')",
            [],
        ).unwrap();
        drop(conn);
        (db, dir)
    }

    fn sample_files() -> Vec<SnapFileEntry> {
        vec![
            SnapFileEntry::new(
                "src/main.rs".into(),
                "abc123".into(),
                1024,
                true,
                false,
                Some(1000),
                StorageMethod::Copy,
                false,
            ),
            SnapFileEntry::new(
                "README.md".into(),
                "def456".into(),
                512,
                true,
                false,
                Some(2000),
                StorageMethod::Reflink,
                false,
            ),
        ]
    }

    fn sample_deleted() -> Vec<DeletedFile> {
        vec![DeletedFile {
            path: "old_file.txt".into(),
            hash: "ghi789".into(),
            size: 256,
            stored: true,
            storage_method: StorageMethod::Copy,
        }]
    }

    #[test]
    fn create_and_list_snapshots() {
        let (db, _dir) = temp_db();
        let files = sample_files();
        let deleted = sample_deleted();
        let req = CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:00:00Z", SnapshotTrigger::Manual,
            "first snapshot", false, &files, &deleted,
        );
        let (rowid, snap_id) = db.create_snapshot(req).unwrap();
        assert!(rowid > 0);
        assert_eq!(snap_id, 1);

        let list = db.list_snapshots("proj1").unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].snapshot_id, 1);
        assert_eq!(list[0].message, "first snapshot");
        assert_eq!(list[0].trigger, SnapshotTrigger::Manual);
        assert!(!list[0].pinned);
        assert_eq!(list[0].file_count, 2);
    }

    #[test]
    fn auto_increment_snapshot_id_when_zero() {
        let (db, _dir) = temp_db();
        let files = sample_files();

        let (_, id1) = db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:00:00Z", SnapshotTrigger::Auto,
            "s1", false, &files, &[],
        )).unwrap();
        let (_, id2) = db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:01:00Z", SnapshotTrigger::Auto,
            "s2", false, &files, &[],
        )).unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn pin_and_unpin_snapshot() {
        let (db, _dir) = temp_db();
        let files = sample_files();
        let (rowid, _) = db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:00:00Z", SnapshotTrigger::Manual,
            "pin test", false, &files, &[],
        )).unwrap();

        // Initially not pinned
        let snap = db.get_snapshot_by_rowid(rowid).unwrap().unwrap();
        assert!(!snap.pinned);

        // Pin it
        db.pin_snapshot(rowid, true).unwrap();
        let snap = db.get_snapshot_by_rowid(rowid).unwrap().unwrap();
        assert!(snap.pinned);

        // Unpin it
        db.pin_snapshot(rowid, false).unwrap();
        let snap = db.get_snapshot_by_rowid(rowid).unwrap().unwrap();
        assert!(!snap.pinned);
    }

    #[test]
    fn snapshot_count() {
        let (db, _dir) = temp_db();
        assert_eq!(db.snapshot_count("proj1").unwrap(), 0);

        let files = sample_files();
        db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:00:00Z", SnapshotTrigger::Auto,
            "s1", false, &files, &[],
        )).unwrap();
        db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-02T00:00:00Z", SnapshotTrigger::Auto,
            "s2", false, &files, &[],
        )).unwrap();

        assert_eq!(db.snapshot_count("proj1").unwrap(), 2);
        assert_eq!(db.snapshot_count("nonexistent").unwrap(), 0);
    }

    #[test]
    fn delete_snapshot_removes_it() {
        let (db, _dir) = temp_db();
        let files = sample_files();
        let (rowid, _) = db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:00:00Z", SnapshotTrigger::Auto,
            "ephemeral", false, &files, &[],
        )).unwrap();

        assert_eq!(db.snapshot_count("proj1").unwrap(), 1);
        db.delete_snapshot(rowid).unwrap();
        assert_eq!(db.snapshot_count("proj1").unwrap(), 0);
    }

    #[test]
    fn snapshot_before_returns_preceding() {
        let (db, _dir) = temp_db();
        let files = sample_files();
        db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:00:00Z", SnapshotTrigger::Auto,
            "first", false, &files, &[],
        )).unwrap();
        db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-02T00:00:00Z", SnapshotTrigger::Auto,
            "second", false, &files, &[],
        )).unwrap();
        db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-03T00:00:00Z", SnapshotTrigger::Auto,
            "third", false, &files, &[],
        )).unwrap();

        // Before snapshot 3 should return snapshot 2
        let before = db.snapshot_before("proj1", 3).unwrap().unwrap();
        assert_eq!(before.snapshot_id, 2);
        assert_eq!(before.message, "second");

        // Before snapshot 1 should return None
        let none = db.snapshot_before("proj1", 1).unwrap();
        assert!(none.is_none());
    }

    #[test]
    fn get_snapshot_files_and_deleted() {
        let (db, _dir) = temp_db();
        let files = sample_files();
        let deleted = sample_deleted();
        let (rowid, _) = db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:00:00Z", SnapshotTrigger::Manual,
            "with files", false, &files, &deleted,
        )).unwrap();

        let got_files = db.get_snapshot_files(rowid).unwrap();
        assert_eq!(got_files.len(), 2);
        assert!(got_files.iter().any(|f| f.path == "src/main.rs" && f.size == 1024));
        assert!(got_files.iter().any(|f| f.path == "README.md" && f.hash == "def456"));

        let got_deleted = db.get_snapshot_deleted_files(rowid).unwrap();
        assert_eq!(got_deleted.len(), 1);
        assert_eq!(got_deleted[0].path, "old_file.txt");
        assert_eq!(got_deleted[0].size, 256);
    }

    #[test]
    fn list_snapshots_oldest_first_order() {
        let (db, _dir) = temp_db();
        let files = sample_files();
        for i in 1..=3 {
            db.create_snapshot(CreateSnapshotRow::new(
                "proj1", 0, &format!("2026-01-0{i}T00:00:00Z"), SnapshotTrigger::Auto,
                &format!("snap{i}"), false, &files, &[],
            )).unwrap();
        }

        let oldest_first = db.list_snapshots_oldest_first("proj1").unwrap();
        assert_eq!(oldest_first.len(), 3);
        assert_eq!(oldest_first[0].snapshot_id, 1);
        assert_eq!(oldest_first[2].snapshot_id, 3);
    }

    #[test]
    fn list_snapshots_paginated() {
        let (db, _dir) = temp_db();
        let files = sample_files();
        for i in 1..=5 {
            db.create_snapshot(CreateSnapshotRow::new(
                "proj1", 0, &format!("2026-01-0{i}T00:00:00Z"), SnapshotTrigger::Auto,
                &format!("snap{i}"), false, &files, &[],
            )).unwrap();
        }

        // Page 1: first 2 (newest first)
        let page1 = db.list_snapshots_paginated("proj1", 2, 0).unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page1[0].snapshot_id, 5);
        assert_eq!(page1[1].snapshot_id, 4);

        // Page 2: next 2
        let page2 = db.list_snapshots_paginated("proj1", 2, 2).unwrap();
        assert_eq!(page2.len(), 2);
        assert_eq!(page2[0].snapshot_id, 3);
    }

    #[test]
    fn list_snapshot_summaries_with_time_filters() {
        let (db, _dir) = temp_db();
        let files = sample_files();
        for i in 1..=5 {
            db.create_snapshot(CreateSnapshotRow::new(
                "proj1", 0, &format!("2026-01-0{i}T00:00:00Z"), SnapshotTrigger::Auto,
                &format!("snap{i}"), false, &files, &[],
            )).unwrap();
        }

        // No filters: all 5
        let all = db.list_snapshot_summaries("proj1", None, None).unwrap();
        assert_eq!(all.len(), 5);

        // from only
        let from = db.list_snapshot_summaries("proj1", Some("2026-01-03T00:00:00Z"), None).unwrap();
        assert_eq!(from.len(), 3);

        // to only
        let to = db.list_snapshot_summaries("proj1", None, Some("2026-01-02T00:00:00Z")).unwrap();
        assert_eq!(to.len(), 2);

        // from + to
        let range = db.list_snapshot_summaries(
            "proj1",
            Some("2026-01-02T00:00:00Z"),
            Some("2026-01-04T00:00:00Z"),
        ).unwrap();
        assert_eq!(range.len(), 3);
    }

    #[test]
    fn latest_snapshot_rowid() {
        let (db, _dir) = temp_db();
        assert!(db.latest_snapshot_rowid("proj1").unwrap().is_none());

        let files = sample_files();
        db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:00:00Z", SnapshotTrigger::Auto,
            "s1", false, &files, &[],
        )).unwrap();
        let (rowid2, _) = db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-02T00:00:00Z", SnapshotTrigger::Auto,
            "s2", false, &files, &[],
        )).unwrap();

        assert_eq!(db.latest_snapshot_rowid("proj1").unwrap(), Some(rowid2));
    }

    #[test]
    fn file_history_tracks_across_snapshots() {
        let (db, _dir) = temp_db();
        let files_v1 = vec![SnapFileEntry::new(
            "src/main.rs".into(), "hash_v1".into(), 100, true, false,
            Some(1000), StorageMethod::Copy, false,
        )];
        let files_v2 = vec![SnapFileEntry::new(
            "src/main.rs".into(), "hash_v2".into(), 200, true, false,
            Some(2000), StorageMethod::Copy, false,
        )];

        db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:00:00Z", SnapshotTrigger::Auto,
            "v1", false, &files_v1, &[],
        )).unwrap();
        db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-02T00:00:00Z", SnapshotTrigger::Manual,
            "v2", false, &files_v2, &[],
        )).unwrap();

        let history = db.file_history("proj1", "src/main.rs").unwrap();
        assert_eq!(history.len(), 2);
        // Newest first
        assert_eq!(history[0].hash, "hash_v2");
        assert_eq!(history[1].hash, "hash_v1");
    }

    #[test]
    fn blob_bytes_tracking() {
        let (db, _dir) = temp_db();
        assert_eq!(db.blob_bytes().unwrap(), 0);

        db.add_blob_bytes(1000).unwrap();
        assert_eq!(db.blob_bytes().unwrap(), 1000);

        db.add_blob_bytes(500).unwrap();
        assert_eq!(db.blob_bytes().unwrap(), 1500);

        db.add_blob_bytes(-300).unwrap();
        assert_eq!(db.blob_bytes().unwrap(), 1200);
    }

    #[test]
    fn total_blob_size_for_project() {
        let (db, _dir) = temp_db();
        assert_eq!(db.total_blob_size_for_project("proj1").unwrap(), 0);

        let files = vec![
            SnapFileEntry::new(
                "a.rs".into(), "hash_a".into(), 100, true, false,
                None, StorageMethod::Copy, false,
            ),
            SnapFileEntry::new(
                "b.rs".into(), "hash_b".into(), 200, false, false,
                None, StorageMethod::Copy, false,
            ),
        ];
        db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:00:00Z", SnapshotTrigger::Auto,
            "s1", false, &files, &[],
        )).unwrap();

        // Only stored files count (a.rs stored=true, b.rs stored=false)
        let size = db.total_blob_size_for_project("proj1").unwrap();
        assert_eq!(size, 100);
    }

    #[test]
    fn all_referenced_blob_hashes() {
        let (db, _dir) = temp_db();
        let files = vec![
            SnapFileEntry::new(
                "a.rs".into(), "hash_a".into(), 100, true, false,
                None, StorageMethod::Copy, false,
            ),
        ];
        let deleted = vec![DeletedFile {
            path: "old.rs".into(),
            hash: "hash_old".into(),
            size: 50,
            stored: true,
            storage_method: StorageMethod::Copy,
        }];
        db.create_snapshot(CreateSnapshotRow::new(
            "proj1", 0, "2026-01-01T00:00:00Z", SnapshotTrigger::Auto,
            "s1", false, &files, &deleted,
        )).unwrap();

        let hashes = db.all_referenced_blob_hashes().unwrap();
        assert!(hashes.contains("hash_a"));
        assert!(hashes.contains("hash_old"));
    }

    #[test]
    fn get_snapshot_by_nonexistent_rowid() {
        let (db, _dir) = temp_db();
        let result = db.get_snapshot_by_rowid(9999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn vacuum_does_not_error() {
        let (db, _dir) = temp_db();
        db.vacuum().unwrap();
    }
}
