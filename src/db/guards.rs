use anyhow::Result;
use rusqlite::params;

use super::{Database, DbGuardEngine, DbGuardEntry, DbGuardMode, DbGuardRegistration};

impl Database {
    /// Register a new database guard.
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

    /// List all registered database guards ordered by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails or a persisted engine/mode value is unrecognized.
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

    /// Update the cached watched-tables list for a database guard.
    pub fn set_db_guard_watched_tables_cache(&self, name: &str, cache: Option<&str>) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE db_guards SET watched_tables_cache = ?1 WHERE name = ?2",
            params![cache, name],
        )?;
        Ok(())
    }

    /// Delete a database guard by name.
    pub fn remove_db_guard(&self, name: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute("DELETE FROM db_guards WHERE name = ?1", params![name])?;
        Ok(())
    }

    /// Record the timestamp of the last successful baseline for a database guard.
    pub fn set_db_guard_baseline_time(&self, name: &str, ts: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE db_guards SET last_baseline_at = ?1 WHERE name = ?2",
            params![ts, name],
        )?;
        Ok(())
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> (tempfile::TempDir, Database) {
        let tmp = tempfile::tempdir().unwrap();
        let db = Database::open(&tmp.path().join("test.db")).unwrap();
        (tmp, db)
    }

    fn sample_registration<'a>() -> DbGuardRegistration<'a> {
        DbGuardRegistration {
            name: "guard-alpha",
            engine: DbGuardEngine::Postgres,
            connection_ref: "postgres://localhost/mydb",
            tables_csv: "users,orders",
            watched_tables_cache: Some("users,orders,products"),
            mode: DbGuardMode::Triggers,
        }
    }

    #[test]
    fn add_then_list_db_guards() {
        let (_tmp, db) = temp_db();
        let reg = sample_registration();
        db.add_db_guard(&reg).unwrap();

        let guards = db.list_db_guards().unwrap();
        assert_eq!(guards.len(), 1);
        assert_eq!(guards[0].name, "guard-alpha");
        assert_eq!(guards[0].engine, DbGuardEngine::Postgres);
        assert_eq!(guards[0].connection_ref, "postgres://localhost/mydb");
        assert_eq!(guards[0].tables_csv, "users,orders");
        assert_eq!(
            guards[0].watched_tables_cache.as_deref(),
            Some("users,orders,products")
        );
        assert_eq!(guards[0].mode, DbGuardMode::Triggers);
        assert!(guards[0].active);
        assert!(guards[0].last_baseline_at.is_none());
    }

    #[test]
    fn add_guard_without_watched_tables_cache() {
        let (_tmp, db) = temp_db();
        let reg = DbGuardRegistration {
            name: "guard-no-cache",
            engine: DbGuardEngine::Sqlite,
            connection_ref: "/tmp/app.db",
            tables_csv: "*",
            watched_tables_cache: None,
            mode: DbGuardMode::SchemaPolling,
        };
        db.add_db_guard(&reg).unwrap();

        let guards = db.list_db_guards().unwrap();
        assert_eq!(guards.len(), 1);
        assert!(guards[0].watched_tables_cache.is_none());
        assert_eq!(guards[0].engine, DbGuardEngine::Sqlite);
        assert_eq!(guards[0].mode, DbGuardMode::SchemaPolling);
    }

    #[test]
    fn remove_then_list_db_guards() {
        let (_tmp, db) = temp_db();
        let reg = sample_registration();
        db.add_db_guard(&reg).unwrap();
        assert_eq!(db.list_db_guards().unwrap().len(), 1);

        db.remove_db_guard("guard-alpha").unwrap();
        assert!(db.list_db_guards().unwrap().is_empty());
    }

    #[test]
    fn remove_nonexistent_guard_is_noop() {
        let (_tmp, db) = temp_db();
        db.remove_db_guard("ghost").unwrap();
        assert!(db.list_db_guards().unwrap().is_empty());
    }

    #[test]
    fn set_baseline_time_then_verify() {
        let (_tmp, db) = temp_db();
        let reg = sample_registration();
        db.add_db_guard(&reg).unwrap();

        // Initially no baseline
        let guards = db.list_db_guards().unwrap();
        assert!(guards[0].last_baseline_at.is_none());

        // Set baseline
        db.set_db_guard_baseline_time("guard-alpha", "2026-03-17T10:00:00Z")
            .unwrap();
        let guards = db.list_db_guards().unwrap();
        assert_eq!(
            guards[0].last_baseline_at.as_deref(),
            Some("2026-03-17T10:00:00Z")
        );

        // Update baseline
        db.set_db_guard_baseline_time("guard-alpha", "2026-03-17T12:00:00Z")
            .unwrap();
        let guards = db.list_db_guards().unwrap();
        assert_eq!(
            guards[0].last_baseline_at.as_deref(),
            Some("2026-03-17T12:00:00Z")
        );
    }

    #[test]
    fn set_watched_tables_cache_then_verify() {
        let (_tmp, db) = temp_db();
        let reg = DbGuardRegistration {
            name: "guard-cache",
            engine: DbGuardEngine::Mysql,
            connection_ref: "mysql://localhost/shop",
            tables_csv: "*",
            watched_tables_cache: None,
            mode: DbGuardMode::SchemaPolling,
        };
        db.add_db_guard(&reg).unwrap();

        // Initially no cache
        let guards = db.list_db_guards().unwrap();
        assert!(guards[0].watched_tables_cache.is_none());

        // Set cache
        db.set_db_guard_watched_tables_cache("guard-cache", Some("orders,inventory"))
            .unwrap();
        let guards = db.list_db_guards().unwrap();
        assert_eq!(
            guards[0].watched_tables_cache.as_deref(),
            Some("orders,inventory")
        );

        // Clear cache back to None
        db.set_db_guard_watched_tables_cache("guard-cache", None)
            .unwrap();
        let guards = db.list_db_guards().unwrap();
        assert!(guards[0].watched_tables_cache.is_none());
    }

    #[test]
    fn list_db_guards_empty_initially() {
        let (_tmp, db) = temp_db();
        let guards = db.list_db_guards().unwrap();
        assert!(guards.is_empty());
    }

    #[test]
    fn add_multiple_guards_ordered_by_id() {
        let (_tmp, db) = temp_db();
        for name in &["zeta-guard", "alpha-guard", "mu-guard"] {
            let reg = DbGuardRegistration {
                name,
                engine: DbGuardEngine::Postgres,
                connection_ref: "pg://localhost/db",
                tables_csv: "*",
                watched_tables_cache: None,
                mode: DbGuardMode::Triggers,
            };
            db.add_db_guard(&reg).unwrap();
        }

        let guards = db.list_db_guards().unwrap();
        assert_eq!(guards.len(), 3);
        assert_eq!(guards[0].name, "zeta-guard");
        assert_eq!(guards[1].name, "alpha-guard");
        assert_eq!(guards[2].name, "mu-guard");
        assert!(guards[0].id < guards[1].id);
        assert!(guards[1].id < guards[2].id);
    }
}
