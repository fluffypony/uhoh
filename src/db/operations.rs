use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};

use super::{
    row_opt_u64, row_u64, ActiveOperationRow, CompletedOperationRow, Database, OperationListRow,
};

impl Database {
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
                first_snapshot_id: row_opt_u64(row, 4, "operations.first_snapshot_id")?,
                last_snapshot_id: row_opt_u64(row, 5, "operations.last_snapshot_id")?,
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
                    first_snapshot_id: row_u64(row, 2, "operations.first_snapshot_id")?,
                    last_snapshot_id: row_u64(row, 3, "operations.last_snapshot_id")?,
                })
            },
        )
        .optional()
        .context("Failed to query latest operation")
    }
}
