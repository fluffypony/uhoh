use anyhow::Result;
use rusqlite::{params, Connection};

use crate::db::{DeletedFile, SnapFileEntry, TreeHash};

pub fn create_snapshot_tx(
    tx: &impl std::ops::Deref<Target = Connection>,
    project_hash: &str,
    snapshot_id: u64,
    timestamp: &str,
    trigger: &str,
    message: &str,
    pinned: bool,
    files: &[SnapFileEntry],
    deleted: &[DeletedFile],
    tree_hashes: &[TreeHash],
) -> Result<i64> {
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

    let _ = tx.execute(
        "UPDATE snapshots SET file_count = (
            SELECT COUNT(*) FROM snapshot_files WHERE snapshot_rowid = ?1
        ) WHERE rowid = ?1",
        params![rowid],
    );

    Ok(rowid)
}
