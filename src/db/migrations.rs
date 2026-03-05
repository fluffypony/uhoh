use anyhow::Result;
use rusqlite::Connection;

/// Apply schema migrations. Fresh databases get the full consolidated schema.
/// Existing databases get incremental alterations to reach v8.
pub fn apply_migrations(conn: &Connection, version: i32) -> Result<()> {
    if version == 0 {
        // Fresh database: apply consolidated schema
        conn.execute_batch(include_str!("schema.sql"))?;
        return Ok(());
    }

    // Incremental upgrades for existing databases
    if version < 2 {
        let _ = conn.execute_batch("ALTER TABLE snapshots ADD COLUMN ai_summary TEXT;");
        conn.execute_batch("PRAGMA user_version = 2;")?;
    }
    if version < 3 {
        let _ = conn.execute_batch("ALTER TABLE snapshot_files ADD COLUMN mtime INTEGER;");
        let _ = conn.execute_batch(
            "ALTER TABLE snapshot_files ADD COLUMN is_symlink INTEGER NOT NULL DEFAULT 0;",
        );
        let _ = conn.execute_batch(
            "ALTER TABLE snapshot_deleted ADD COLUMN storage_method INTEGER NOT NULL DEFAULT 1;",
        );
        conn.execute_batch("PRAGMA user_version = 3;")?;
    }
    if version < 4 {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS pending_ai_summaries (
                snapshot_rowid INTEGER PRIMARY KEY,
                project_hash TEXT NOT NULL,
                queued_at TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(snapshot_rowid) REFERENCES snapshots(rowid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_ai_queue_time ON pending_ai_summaries(queued_at);
            ALTER TABLE snapshots ADD COLUMN file_count INTEGER NOT NULL DEFAULT 0;
            CREATE TABLE IF NOT EXISTS operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_hash TEXT NOT NULL,
                label TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                first_snapshot_id INTEGER,
                last_snapshot_id INTEGER,
                FOREIGN KEY(project_hash) REFERENCES projects(hash) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_operations_project_started ON operations(project_hash, started_at DESC);
            PRAGMA user_version = 4;",
        )?;
    }
    if version < 5 {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS stats (
                key   TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
            INSERT OR IGNORE INTO stats (key, value) VALUES ('blob_bytes', 0);
            PRAGMA user_version = 5;",
        )?;
    }
    if version < 6 {
        conn.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
                snapshot_rowid UNINDEXED,
                project_hash,
                trigger_type,
                message,
                ai_summary,
                file_paths
            );
            PRAGMA user_version = 6;",
        )?;
    }
    if version < 7 {
        conn.execute_batch(
            "BEGIN;
            CREATE TABLE IF NOT EXISTS event_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                source TEXT NOT NULL,
                event_type TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'info',
                project_hash TEXT,
                agent_name TEXT,
                guard_name TEXT,
                path TEXT,
                detail TEXT,
                pre_state_ref TEXT,
                post_state_ref TEXT,
                prev_hash TEXT,
                causal_parent INTEGER REFERENCES event_ledger(id),
                resolved INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_event_ledger_ts ON event_ledger(ts);
            CREATE INDEX IF NOT EXISTS idx_event_ledger_source ON event_ledger(source);
            CREATE INDEX IF NOT EXISTS idx_event_ledger_agent ON event_ledger(agent_name);
            CREATE INDEX IF NOT EXISTS idx_event_ledger_guard ON event_ledger(guard_name);
            CREATE INDEX IF NOT EXISTS idx_event_ledger_path ON event_ledger(path);
            CREATE INDEX IF NOT EXISTS idx_event_ledger_causal ON event_ledger(causal_parent);
            CREATE INDEX IF NOT EXISTS idx_event_ledger_severity ON event_ledger(severity);
            CREATE INDEX IF NOT EXISTS idx_event_ledger_prev_hash ON event_ledger(prev_hash);
            CREATE TABLE IF NOT EXISTS db_guards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                engine TEXT NOT NULL,
                connection_ref TEXT NOT NULL,
                tables_csv TEXT NOT NULL,
                mode TEXT NOT NULL DEFAULT 'triggers',
                created_at TEXT NOT NULL,
                last_baseline_at TEXT,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS agents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                profile_path TEXT NOT NULL,
                profile_version INTEGER NOT NULL DEFAULT 1,
                data_dir TEXT,
                registered_at TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );
            PRAGMA user_version = 7;
            COMMIT;",
        )?;
    }
    if version < 8 {
        // v8: add prev_hash and session_id columns if missing
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

        // Fix operations table column names if they use the old schema (v4)
        let mut op_columns = std::collections::HashSet::new();
        {
            let mut stmt = conn.prepare("PRAGMA table_info(operations)")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
            for r in rows {
                op_columns.insert(r?);
            }
        }
        if op_columns.contains("completed_at") && !op_columns.contains("ended_at") {
            conn.execute_batch(
                "ALTER TABLE operations RENAME COLUMN completed_at TO ended_at;"
            )?;
        }
        if op_columns.contains("start_snapshot_id") && !op_columns.contains("first_snapshot_id") {
            conn.execute_batch(
                "ALTER TABLE operations RENAME COLUMN start_snapshot_id TO first_snapshot_id;"
            )?;
        }
        if op_columns.contains("end_snapshot_id") && !op_columns.contains("last_snapshot_id") {
            conn.execute_batch(
                "ALTER TABLE operations RENAME COLUMN end_snapshot_id TO last_snapshot_id;"
            )?;
        }

        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_event_ledger_prev_hash ON event_ledger(prev_hash);
            CREATE INDEX IF NOT EXISTS idx_event_ledger_session ON event_ledger(session_id);
            PRAGMA user_version = 8;",
        )?;
    }
    Ok(())
}
