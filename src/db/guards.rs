use anyhow::Result;
use rusqlite::params;

use super::{Database, DbGuardEngine, DbGuardEntry, DbGuardMode, DbGuardRegistration};

impl Database {
    pub fn add_db_guard(&self, reg: &DbGuardRegistration<'_>) -> Result<()> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO db_guards (name, engine, connection_ref, tables_csv, watched_tables_cache, mode, created_at, active)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)",
            params![
                reg.name,
                reg.engine.as_str(),
                reg.connection_ref,
                reg.tables_csv,
                reg.watched_tables_cache,
                reg.mode.as_str(),
                now
            ],
        )?;
        Ok(())
    }

    pub fn list_db_guards(&self) -> Result<Vec<DbGuardEntry>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, name, engine, connection_ref, tables_csv, watched_tables_cache, mode, created_at, last_baseline_at, active
             FROM db_guards ORDER BY id ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            let engine_raw: String = row.get(2)?;
            let mode_raw: String = row.get(6)?;
            let engine = DbGuardEngine::parse(&engine_raw).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Text,
                    format!("invalid db_guard engine: {engine_raw}").into(),
                )
            })?;
            let mode = DbGuardMode::parse(&mode_raw).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    6,
                    rusqlite::types::Type::Text,
                    format!("invalid db_guard mode: {mode_raw}").into(),
                )
            })?;
            Ok(DbGuardEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                engine,
                connection_ref: row.get(3)?,
                tables_csv: row.get(4)?,
                watched_tables_cache: row.get(5)?,
                mode,
                created_at: row.get(7)?,
                last_baseline_at: row.get(8)?,
                active: row.get::<_, i32>(9)? != 0,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn set_db_guard_watched_tables_cache(&self, name: &str, cache: Option<&str>) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE db_guards SET watched_tables_cache = ?1 WHERE name = ?2",
            params![cache, name],
        )?;
        Ok(())
    }

    pub fn remove_db_guard(&self, name: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute("DELETE FROM db_guards WHERE name = ?1", params![name])?;
        Ok(())
    }

    pub fn set_db_guard_baseline_time(&self, name: &str, ts: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE db_guards SET last_baseline_at = ?1 WHERE name = ?2",
            params![ts, name],
        )?;
        Ok(())
    }
}
