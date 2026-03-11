use anyhow::Result;
use rusqlite::{params, Connection};

use crate::db::{DeletedFile, SnapFileEntry};

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
