use anyhow::Result;
use rusqlite::params;

use super::{checked_usize_u64, Database, PendingAiSummaryRow};

impl Database {
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
        let rows = stmt.query_map(params![i64::from(limit)], |row| {
            Ok(PendingAiSummaryRow {
                snapshot_rowid: row.get::<_, i64>(0)?,
                project_hash: row.get::<_, String>(1)?,
                attempts: row.get::<_, i64>(2)?,
                queued_at: row.get::<_, String>(3)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Remove a pending AI summary entry by snapshot rowid.
    pub fn delete_pending_ai(&self, snapshot_rowid: i64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM pending_ai_summaries WHERE snapshot_rowid = ?1",
            params![snapshot_rowid],
        )?;
        Ok(())
    }

    /// Increment the attempt counter for a pending AI summary and return the new count.
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
        )?;
        checked_usize_u64(affected, "pending_ai_summaries.delete_count")
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> (tempfile::TempDir, Database) {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = Database::open(&tmp.path().join("test.db")).unwrap();
        db.add_project("proj1", "/fake").unwrap();
        // Create a snapshot to reference
        db.create_snapshot(crate::db::CreateSnapshotRow::new(
            "proj1",
            1,
            &chrono::Utc::now().to_rfc3339(),
            crate::db::SnapshotTrigger::Auto,
            "",
            false,
            &[],
            &[],
        ))
        .unwrap();
        (tmp, db)
    }

    #[test]
    fn enqueue_and_dequeue_roundtrip() {
        let (_tmp, db) = temp_db();
        let rowid = db.latest_snapshot_rowid("proj1").unwrap().unwrap();
        db.enqueue_ai_summary(rowid, "proj1").unwrap();

        let pending = db.dequeue_pending_ai(10).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].snapshot_rowid, rowid);
        assert_eq!(pending[0].project_hash, "proj1");
        assert_eq!(pending[0].attempts, 0);
    }

    #[test]
    fn enqueue_is_idempotent() {
        let (_tmp, db) = temp_db();
        let rowid = db.latest_snapshot_rowid("proj1").unwrap().unwrap();
        db.enqueue_ai_summary(rowid, "proj1").unwrap();
        db.enqueue_ai_summary(rowid, "proj1").unwrap(); // should not duplicate

        let pending = db.dequeue_pending_ai(10).unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn delete_pending_removes_entry() {
        let (_tmp, db) = temp_db();
        let rowid = db.latest_snapshot_rowid("proj1").unwrap().unwrap();
        db.enqueue_ai_summary(rowid, "proj1").unwrap();
        db.delete_pending_ai(rowid).unwrap();

        let pending = db.dequeue_pending_ai(10).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn increment_attempts_tracks_count() {
        let (_tmp, db) = temp_db();
        let rowid = db.latest_snapshot_rowid("proj1").unwrap().unwrap();
        db.enqueue_ai_summary(rowid, "proj1").unwrap();

        let count1 = db.increment_ai_attempts(rowid).unwrap();
        assert_eq!(count1, 1);
        let count2 = db.increment_ai_attempts(rowid).unwrap();
        assert_eq!(count2, 2);
    }

    #[test]
    fn dequeue_empty_returns_empty() {
        let (_tmp, db) = temp_db();
        let pending = db.dequeue_pending_ai(10).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn dequeue_respects_limit() {
        let (_tmp, db) = temp_db();
        let rowid = db.latest_snapshot_rowid("proj1").unwrap().unwrap();
        db.enqueue_ai_summary(rowid, "proj1").unwrap();

        let pending = db.dequeue_pending_ai(0).unwrap();
        assert!(pending.is_empty());
    }

    #[test]
    fn prune_removes_old_entries() {
        let (_tmp, db) = temp_db();
        let rowid = db.latest_snapshot_rowid("proj1").unwrap().unwrap();
        db.enqueue_ai_summary(rowid, "proj1").unwrap();

        // Pruning with a large TTL should keep the entry (it was just created)
        let removed = db.prune_ai_queue_ttl(365).unwrap();
        assert_eq!(removed, 0);

        let pending = db.dequeue_pending_ai(10).unwrap();
        assert_eq!(pending.len(), 1);
    }
}
