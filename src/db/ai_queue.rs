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
        let rows = stmt.query_map(params![limit as i64], |row| {
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
        )?;
        checked_usize_u64(affected, "pending_ai_summaries.delete_count")
    }
}
