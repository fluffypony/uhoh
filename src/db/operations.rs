use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};

use super::{
    row_opt_u64, row_u64, ActiveOperationRow, CompletedOperationRow, Database, OperationListRow,
};

impl Database {
    /// Create a new operation record and return its row ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn create_operation(&self, project_hash: &str, label: &str) -> Result<i64> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO operations (project_hash, label, started_at) VALUES (?1, ?2, ?3)",
            params![project_hash, label, now],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Close an operation, recording its end time and snapshot range.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn set_operation_first_snapshot(&self, op_id: i64, first_snapshot_id: u64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE operations SET first_snapshot_id = ?1 WHERE id = ?2",
            params![first_snapshot_id, op_id],
        )?;
        Ok(())
    }

    /// Update the last snapshot id of an operation without closing it.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn update_operation_last_snapshot(&self, op_id: i64, last_snapshot_id: u64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE operations SET last_snapshot_id = ?1 WHERE id = ?2",
            params![last_snapshot_id, op_id],
        )?;
        Ok(())
    }

    /// Set the last snapshot id and close an operation (preserves `first_snapshot_id`)
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn close_operation_with_last(&self, op_id: i64, last_snapshot_id: u64) -> Result<()> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE operations SET ended_at = ?1, last_snapshot_id = ?2 WHERE id = ?3",
            params![now, last_snapshot_id, op_id],
        )?;
        Ok(())
    }

    /// Get the most recently started open operation for a project, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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

    /// List the most recent operations for a project (up to 50), newest first.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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

    /// Get the most recently completed operation for a project, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
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

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_db() -> (Database, TempDir) {
        let tmp = TempDir::new().unwrap();
        let db = Database::open(&tmp.path().join("test.db")).unwrap();
        db.add_project("proj1", "/fake/path").unwrap();
        (db, tmp)
    }

    #[test]
    fn create_operation_returns_id() {
        let (db, _tmp) = temp_db();
        let id = db.create_operation("proj1", "refactor").unwrap();
        assert!(id > 0);
    }

    #[test]
    fn get_active_operation_finds_open_op() {
        let (db, _tmp) = temp_db();
        let id = db.create_operation("proj1", "test-op").unwrap();

        let active = db.get_active_operation("proj1").unwrap();
        assert!(active.is_some());
        let active = active.unwrap();
        assert_eq!(active.id, id);
        assert_eq!(active.label, "test-op");
    }

    #[test]
    fn get_active_operation_returns_none_when_empty() {
        let (db, _tmp) = temp_db();
        let active = db.get_active_operation("proj1").unwrap();
        assert!(active.is_none());
    }

    #[test]
    fn finish_operation_closes_it() {
        let (db, _tmp) = temp_db();
        let id = db.create_operation("proj1", "my-op").unwrap();
        db.finish_operation(id, 100, 200).unwrap();

        // Should no longer be active
        let active = db.get_active_operation("proj1").unwrap();
        assert!(active.is_none());

        // Should appear as completed
        let completed = db.get_latest_completed_operation("proj1").unwrap();
        assert!(completed.is_some());
        let c = completed.unwrap();
        assert_eq!(c.label, "my-op");
        assert_eq!(c.first_snapshot_id, 100);
        assert_eq!(c.last_snapshot_id, 200);
    }

    #[test]
    fn close_operation_with_last_preserves_first() {
        let (db, _tmp) = temp_db();
        let id = db.create_operation("proj1", "op1").unwrap();
        db.set_operation_first_snapshot(id, 42).unwrap();
        db.close_operation_with_last(id, 99).unwrap();

        let completed = db.get_latest_completed_operation("proj1").unwrap().unwrap();
        assert_eq!(completed.first_snapshot_id, 42);
        assert_eq!(completed.last_snapshot_id, 99);
    }

    #[test]
    fn update_last_snapshot_without_closing() {
        let (db, _tmp) = temp_db();
        let id = db.create_operation("proj1", "op1").unwrap();
        db.update_operation_last_snapshot(id, 50).unwrap();

        // Still active
        let active = db.get_active_operation("proj1").unwrap();
        assert!(active.is_some());
    }

    #[test]
    fn list_operations_returns_correct_count() {
        let (db, _tmp) = temp_db();
        db.create_operation("proj1", "op1").unwrap();
        db.create_operation("proj1", "op2").unwrap();
        db.create_operation("proj1", "op3").unwrap();

        let ops = db.list_operations("proj1").unwrap();
        assert_eq!(ops.len(), 3);
    }

    #[test]
    fn list_operations_ordered_newest_first() {
        let (db, _tmp) = temp_db();
        let id1 = db.create_operation("proj1", "first").unwrap();
        let _id2 = db.create_operation("proj1", "second").unwrap();
        let id3 = db.create_operation("proj1", "third").unwrap();

        let ops = db.list_operations("proj1").unwrap();
        assert_eq!(ops[0].label, "third");
        assert_eq!(ops[0].id, id3);
        assert_eq!(ops[2].label, "first");
        assert_eq!(ops[2].id, id1);
    }

    #[test]
    fn list_operations_empty_for_unknown_project() {
        let (db, _tmp) = temp_db();
        let ops = db.list_operations("nonexistent").unwrap();
        assert!(ops.is_empty());
    }

    #[test]
    fn get_latest_completed_returns_most_recent() {
        let (db, _tmp) = temp_db();
        let id1 = db.create_operation("proj1", "old-op").unwrap();
        db.finish_operation(id1, 1, 10).unwrap();

        let id2 = db.create_operation("proj1", "new-op").unwrap();
        db.finish_operation(id2, 11, 20).unwrap();

        let completed = db.get_latest_completed_operation("proj1").unwrap().unwrap();
        assert_eq!(completed.label, "new-op");
        assert_eq!(completed.first_snapshot_id, 11);
    }

    #[test]
    fn get_latest_completed_ignores_active() {
        let (db, _tmp) = temp_db();
        let id1 = db.create_operation("proj1", "done").unwrap();
        db.finish_operation(id1, 1, 10).unwrap();

        // Create but don't close
        let _id2 = db.create_operation("proj1", "active").unwrap();

        let completed = db.get_latest_completed_operation("proj1").unwrap().unwrap();
        assert_eq!(completed.label, "done");
    }

    #[test]
    fn multiple_active_returns_most_recent() {
        let (db, _tmp) = temp_db();
        let _id1 = db.create_operation("proj1", "earlier").unwrap();
        let id2 = db.create_operation("proj1", "later").unwrap();

        let active = db.get_active_operation("proj1").unwrap().unwrap();
        assert_eq!(active.id, id2);
        assert_eq!(active.label, "later");
    }

    #[test]
    fn set_first_snapshot_works() {
        let (db, _tmp) = temp_db();
        let id = db.create_operation("proj1", "op1").unwrap();
        db.set_operation_first_snapshot(id, 42).unwrap();
        db.finish_operation(id, 42, 99).unwrap();

        let completed = db.get_latest_completed_operation("proj1").unwrap().unwrap();
        assert_eq!(completed.first_snapshot_id, 42);
    }

    #[test]
    fn list_operations_shows_snapshot_range() {
        let (db, _tmp) = temp_db();
        let id = db.create_operation("proj1", "ranged").unwrap();
        db.set_operation_first_snapshot(id, 10).unwrap();
        db.update_operation_last_snapshot(id, 20).unwrap();

        let ops = db.list_operations("proj1").unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].first_snapshot_id, Some(10));
        assert_eq!(ops[0].last_snapshot_id, Some(20));
    }
}
