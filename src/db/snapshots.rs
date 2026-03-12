use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};

use super::{checked_db_u64, row_u64, Database, DeletedFile, SnapFileEntry, SnapshotRow};

pub struct SnapshotInsert<'a> {
    pub project_hash: &'a str,
    pub snapshot_id: u64,
    pub timestamp: &'a str,
    pub trigger: &'a str,
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
        for (path, hash, size, stored, storage_method) in deleted {
            del_stmt.execute(params![
                rowid,
                path,
                hash,
                size,
                *stored as i32,
                storage_method.to_db()
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
}
