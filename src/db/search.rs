use anyhow::Result;
use rusqlite::{params, OptionalExtension, Row};

use super::{row_u64, Database, DbConn, SearchResult, SnapshotTrigger};

fn map_search_result_row(row: &Row) -> rusqlite::Result<SearchResult> {
    let trigger_raw: String = row.get(3)?;
    Ok(SearchResult {
        snapshot_rowid: row.get(0)?,
        snapshot_id: row_u64(row, 1, "snapshots.snapshot_id")?,
        timestamp: row.get(2)?,
        trigger: SnapshotTrigger::parse_persisted(&trigger_raw, 3)?,
        message: row.get(4)?,
        ai_summary: row.get(5)?,
        match_context: row.get(6)?,
    })
}

impl Database {
    pub fn set_ai_summary(&self, snapshot_rowid: i64, summary: &str) -> Result<()> {
        let conn = self.conn()?;
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
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO search_index(snapshot_rowid, project_hash, trigger_type, message, ai_summary, file_paths)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![snapshot_rowid, project_hash, trigger, message, ai_summary, file_paths],
        )?;
        Ok(())
    }

    pub fn update_search_index_summary(&self, snapshot_rowid: i64, ai_summary: &str) -> Result<()> {
        let conn = self.conn()?;
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
        let conn = self.conn()?;

        let safe = query
            .replace(
                [
                    '"', '*', ':', '(', ')', '+', '-', '^', '{', '}', '[', ']', '\\', '\'', '|',
                    '&', '!', '<', '>', '~', '$', '#', '@', ';',
                ],
                " ",
            )
            .split_whitespace()
            .filter(|word| {
                let upper = word.to_ascii_uppercase();
                !matches!(upper.as_str(), "AND" | "OR" | "NOT" | "NEAR")
                    && !upper.starts_with("NEAR/")
            })
            .collect::<Vec<_>>()
            .join(" ");
        if safe.is_empty() {
            return Ok(Vec::new());
        }

        let terms = safe
            .split_whitespace()
            .map(|term| format!("{}*", term))
            .collect::<Vec<_>>();
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        let fts_query = terms.join(" ");

        match self.search_snapshots_fts(&conn, &fts_query, project_hash, limit) {
            Ok(results) => Ok(results),
            Err(err) => {
                tracing::debug!("FTS5 query failed (malformed query?): {}", err);
                Ok(Vec::new())
            }
        }
    }

    fn search_snapshots_fts(
        &self,
        conn: &DbConn,
        fts_query: &str,
        project_hash: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        let mut out = Vec::new();

        if let Some(project_hash) = project_hash {
            let mut stmt = conn.prepare(
                "SELECT si.snapshot_rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.ai_summary,
                        snippet(search_index, -1, '<mark>', '</mark>', '...', 32) as match_context
                 FROM search_index si
                 JOIN snapshots s ON s.rowid = si.snapshot_rowid
                 WHERE search_index MATCH ?1 AND si.project_hash = ?2
                 ORDER BY rank
                 LIMIT ?3",
            )?;
            let rows = stmt.query_map(params![fts_query, project_hash, limit as i64], map_search_result_row)?;
            for row in rows {
                out.push(row?);
            }
        } else {
            let mut stmt = conn.prepare(
                "SELECT si.snapshot_rowid, s.snapshot_id, s.timestamp, s.trigger, s.message, s.ai_summary,
                        snippet(search_index, -1, '<mark>', '</mark>', '...', 32) as match_context
                 FROM search_index si
                 JOIN snapshots s ON s.rowid = si.snapshot_rowid
                 WHERE search_index MATCH ?1
                 ORDER BY rank
                 LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![fts_query, limit as i64], map_search_result_row)?;
            for row in rows {
                out.push(row?);
            }
        }

        Ok(out)
    }
}
