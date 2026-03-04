#![allow(dead_code)]

use anyhow::Result;
use rusqlite::Connection;

pub fn apply_migrations(conn: &Connection, version: i32) -> Result<()> {
    if version < 1 {
        conn.execute_batch(include_str!("schema_v1.sql"))?;
    }
    if version < 2 {
        conn.execute_batch(
            "ALTER TABLE snapshots ADD COLUMN ai_summary TEXT; PRAGMA user_version = 2;",
        )?;
    }
    if version < 3 {
        conn.execute_batch(
            "ALTER TABLE snapshot_files ADD COLUMN mtime INTEGER;
             ALTER TABLE snapshot_deleted ADD COLUMN storage_method INTEGER NOT NULL DEFAULT 1;
             PRAGMA user_version = 3;",
        )?;
    }
    if version < 4 {
        conn.execute_batch(include_str!("schema_v4.sql"))?;
    }
    if version < 5 {
        conn.execute_batch(include_str!("schema_v5.sql"))?;
    }
    if version < 6 {
        conn.execute_batch(include_str!("schema_v6.sql"))?;
    }
    if version < 7 {
        conn.execute_batch(include_str!("schema_v7.sql"))?;
    }
    if version < 8 {
        let mut columns = std::collections::HashSet::new();
        {
            let mut stmt = conn.prepare("PRAGMA table_info(event_ledger)")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
            for r in rows {
                columns.insert(r?);
            }
        }
        if !columns.contains("prev_hash") {
            conn.execute_batch("ALTER TABLE event_ledger ADD COLUMN prev_hash TEXT;")?;
        }
        if !columns.contains("session_id") {
            conn.execute_batch(
                "ALTER TABLE event_ledger ADD COLUMN session_id TEXT GENERATED ALWAYS AS (
                    CASE WHEN json_valid(detail) THEN json_extract(detail, '$.session_id') ELSE NULL END
                ) VIRTUAL;",
            )?;
        }
        conn.execute_batch(
            "
            CREATE INDEX IF NOT EXISTS idx_event_ledger_prev_hash ON event_ledger(prev_hash);
            CREATE INDEX IF NOT EXISTS idx_event_ledger_session ON event_ledger(session_id);
            PRAGMA user_version = 8;
            ",
        )?;
    }
    Ok(())
}
