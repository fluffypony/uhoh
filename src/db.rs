use anyhow::{Context, Result};
use r2d2::{CustomizeConnection, Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OptionalExtension, Row};
use std::path::Path;

#[cfg(test)]
use crate::cas::StorageMethod;

/// Thread-safe SQLite database wrapper.
/// SQLite with WAL mode handles concurrent readers and serialized writers.
pub struct Database {
    pool: Pool<SqliteConnectionManager>,
}

impl Database {
    /// Create a new Database handle sharing the same connection pool.
    /// This is cheap (just clones the Arc-wrapped pool) and avoids
    /// opening a separate pool for background threads.
    pub fn clone_handle(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

#[derive(Debug)]
struct SqliteCustomizer;

impl CustomizeConnection<Connection, rusqlite::Error> for SqliteCustomizer {
    fn on_acquire(&self, conn: &mut Connection) -> std::result::Result<(), rusqlite::Error> {
        conn.execute_batch("PRAGMA journal_mode=WAL;")?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        conn.execute_batch("PRAGMA busy_timeout=5000;")?;
        conn.execute_batch("PRAGMA auto_vacuum=INCREMENTAL;")?;
        Ok(())
    }
}

type DbConn = PooledConnection<SqliteConnectionManager>;

mod agents;
mod ai_queue;
mod guards;
mod ledger;
mod operations;
mod projects;
mod search;
mod snapshots;
mod types;

pub use ledger::LedgerRecentFilters;
pub use types::*;

fn row_u64(row: &Row<'_>, index: usize, field: &'static str) -> rusqlite::Result<u64> {
    let value = row.get::<_, i64>(index)?;
    u64::try_from(value).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("negative persisted value for {field}: {value}"),
            )),
        )
    })
}

fn row_opt_u64(row: &Row<'_>, index: usize, field: &'static str) -> rusqlite::Result<Option<u64>> {
    row.get::<_, Option<i64>>(index)?
        .map(|value| {
            u64::try_from(value).map_err(|_| {
                rusqlite::Error::FromSqlConversionFailure(
                    index,
                    rusqlite::types::Type::Integer,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("negative persisted value for {field}: {value}"),
                    )),
                )
            })
        })
        .transpose()
}

fn checked_db_u64(value: i64, field: &'static str) -> Result<u64> {
    u64::try_from(value)
        .map_err(|_| anyhow::anyhow!("negative persisted value for {field}: {value}"))
}

fn checked_usize_u64(value: usize, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow::anyhow!("value too large for {field}: {value}"))
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let manager = SqliteConnectionManager::file(path);
        let pool = Pool::builder()
            .max_size(16)
            .connection_timeout(std::time::Duration::from_secs(30))
            .connection_customizer(Box::new(SqliteCustomizer))
            .build(manager)
            .with_context(|| format!("Failed to open database pool: {}", path.display()))?;

        let db = Database { pool };
        db.migrate()?;
        // Set database file permissions to 0o600 on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = std::fs::metadata(path) {
                let mut perms = meta.permissions();
                if perms.mode() & 0o077 != 0 {
                    perms.set_mode(0o600);
                    let _ = std::fs::set_permissions(path, perms);
                }
            }
        }
        Ok(db)
    }

    /// Create a consistent backup of the database to the given path.
    /// Uses SQLite online backup API under the hood.
    pub fn backup_to(&self, path: &std::path::Path) -> Result<()> {
        let src = self.conn()?;
        let mut dest = rusqlite::Connection::open(path)?;
        let backup = rusqlite::backup::Backup::new(&src, &mut dest)?;
        backup.run_to_completion(5, std::time::Duration::from_millis(50), None)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).ok();
        }
        Ok(())
    }

    fn conn(&self) -> Result<DbConn> {
        const MAX_ATTEMPTS: u32 = 50; // 50 × 100ms = 5 seconds max
        for attempt in 1..=MAX_ATTEMPTS {
            match self.pool.get() {
                Ok(conn) => return Ok(conn),
                Err(err) => {
                    if attempt == MAX_ATTEMPTS {
                        return Err(anyhow::anyhow!(
                            "Database connection pool unavailable after {MAX_ATTEMPTS} attempts: {err}"
                        ));
                    }
                    tracing::error!(
                        "Database connection pool unavailable (attempt {}/{}, retrying): {}",
                        attempt,
                        MAX_ATTEMPTS,
                        err
                    );
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }
        anyhow::bail!("Database connection pool unavailable after {MAX_ATTEMPTS} attempts")
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn()?;
        conn.execute_batch("PRAGMA foreign_keys=ON;")?;
        // All DDL uses CREATE TABLE/INDEX IF NOT EXISTS, safe to run every startup
        conn.execute_batch(include_str!("db/schema.sql"))?;
        Ok(())
    }


    fn latest_ledger_hash_with_conn<C>(&self, conn: &C) -> Result<Option<String>>
    where
        C: std::ops::Deref<Target = Connection>,
    {
        conn.query_row(
            "SELECT prev_hash FROM event_ledger ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("Failed to fetch latest event ledger hash")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── SnapshotTrigger ──

    #[test]
    fn snapshot_trigger_as_str_roundtrip() {
        let variants = [
            (SnapshotTrigger::Auto, "auto"),
            (SnapshotTrigger::Manual, "manual"),
            (SnapshotTrigger::Emergency, "emergency"),
            (SnapshotTrigger::PreRestore, "pre-restore"),
            (SnapshotTrigger::Initial, "initial"),
            (SnapshotTrigger::Mcp, "mcp"),
            (SnapshotTrigger::Api, "api"),
        ];
        for (variant, expected) in variants {
            assert_eq!(variant.as_str(), expected);
            assert_eq!(SnapshotTrigger::parse(expected), Some(variant));
        }
    }

    #[test]
    fn snapshot_trigger_parse_unknown_returns_none() {
        assert_eq!(SnapshotTrigger::parse("unknown"), None);
        assert_eq!(SnapshotTrigger::parse(""), None);
        assert_eq!(SnapshotTrigger::parse("AUTO"), None); // case-sensitive
    }

    #[test]
    fn snapshot_trigger_is_manual_kind() {
        assert!(SnapshotTrigger::Manual.is_manual_kind());
        assert!(SnapshotTrigger::Mcp.is_manual_kind());
        assert!(SnapshotTrigger::Api.is_manual_kind());
        assert!(!SnapshotTrigger::Auto.is_manual_kind());
        assert!(!SnapshotTrigger::Emergency.is_manual_kind());
        assert!(!SnapshotTrigger::PreRestore.is_manual_kind());
        assert!(!SnapshotTrigger::Initial.is_manual_kind());
    }

    #[test]
    fn snapshot_trigger_display() {
        assert_eq!(format!("{}", SnapshotTrigger::PreRestore), "pre-restore");
        assert_eq!(format!("{}", SnapshotTrigger::Auto), "auto");
    }

    #[test]
    fn snapshot_trigger_from_str() {
        assert_eq!("auto".parse::<SnapshotTrigger>(), Ok(SnapshotTrigger::Auto));
        assert!("UNKNOWN".parse::<SnapshotTrigger>().is_err());
    }

    #[test]
    fn snapshot_trigger_case_insensitive_eq() {
        assert!(SnapshotTrigger::Auto == "AUTO");
        assert!(SnapshotTrigger::Auto == "Auto");
        assert!(SnapshotTrigger::PreRestore == "PRE-RESTORE");
    }

    #[test]
    fn snapshot_trigger_serde_roundtrip() {
        let trigger = SnapshotTrigger::PreRestore;
        let json = serde_json::to_string(&trigger).unwrap();
        assert_eq!(json, "\"pre-restore\"");
        let parsed: SnapshotTrigger = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, trigger);
    }

    // ── LedgerSeverity ──

    #[test]
    fn ledger_severity_as_str_roundtrip() {
        let variants = [
            (LedgerSeverity::Info, "info"),
            (LedgerSeverity::Warn, "warn"),
            (LedgerSeverity::Error, "error"),
            (LedgerSeverity::Critical, "critical"),
        ];
        for (variant, expected) in variants {
            assert_eq!(variant.as_str(), expected);
            assert_eq!(LedgerSeverity::parse(expected), Some(variant));
        }
    }

    #[test]
    fn ledger_severity_parse_unknown_returns_none() {
        assert_eq!(LedgerSeverity::parse("unknown"), None);
        assert_eq!(LedgerSeverity::parse("INFO"), None);
    }

    #[test]
    fn ledger_severity_display_and_from_str() {
        assert_eq!(format!("{}", LedgerSeverity::Critical), "critical");
        assert_eq!("warn".parse::<LedgerSeverity>(), Ok(LedgerSeverity::Warn));
        assert!("invalid".parse::<LedgerSeverity>().is_err());
    }

    #[test]
    fn ledger_severity_case_insensitive_eq() {
        assert!(LedgerSeverity::Info == "INFO");
        assert!(LedgerSeverity::Error == "Error");
    }

    // ── LedgerSource ──

    #[test]
    fn ledger_source_as_str_roundtrip() {
        let variants = [
            (LedgerSource::Agent, "agent"),
            (LedgerSource::DbGuard, "db_guard"),
            (LedgerSource::Daemon, "daemon"),
            (LedgerSource::Fs, "fs"),
            (LedgerSource::Mlx, "mlx"),
        ];
        for (variant, expected) in variants {
            assert_eq!(variant.as_str(), expected);
            assert_eq!(LedgerSource::parse(expected), Some(variant));
        }
    }

    #[test]
    fn ledger_source_parse_unknown_returns_none() {
        assert_eq!(LedgerSource::parse("unknown"), None);
    }

    #[test]
    fn ledger_source_from_str_case_insensitive() {
        assert_eq!("FS".parse::<LedgerSource>(), Ok(LedgerSource::Fs));
        assert_eq!("  Agent  ".parse::<LedgerSource>(), Ok(LedgerSource::Agent));
        assert_eq!("DB_GUARD".parse::<LedgerSource>(), Ok(LedgerSource::DbGuard));
        assert!("invalid".parse::<LedgerSource>().is_err());
    }

    #[test]
    fn ledger_source_display() {
        assert_eq!(format!("{}", LedgerSource::DbGuard), "db_guard");
    }

    #[test]
    fn ledger_source_case_insensitive_eq() {
        assert!(LedgerSource::Agent == "AGENT");
        assert!(LedgerSource::Mlx == "Mlx");
    }

    // ── LedgerEventType ──

    #[test]
    fn ledger_event_type_known_variants_roundtrip() {
        let known = [
            "emergency_delete_detected",
            "config_reload_failed",
            "guard_started",
            "guard_mode_normalized",
            "guard_tick_failed",
            "postgres_tick",
            "postgres_baseline",
            "postgres_optional_step_degraded",
            "postgres_wildcard_trigger_reconciled",
            "sqlite_tick",
            "sqlite_data_changed",
            "sqlite_baseline",
            "mysql_tick",
            "mysql_poll_failed",
            "drop_table",
            "schema_change",
            "mass_delete",
            "agent_registered",
            "tool_call",
            "session_tool_call",
            "pre_notify",
            "dangerous_agent_action",
            "dangerous_action_timeout",
            "dangerous_action_denied",
            "dangerous_action_approved",
            "mcp_proxy_started",
            "mcp_proxy_client_connected",
            "mcp_proxy_connection_failed",
            "mcp_proxy_accept_failed",
            "audit_tick",
            "fanotify_overflow",
            "fanotify_monitor_started",
            "fanotify_preimage",
            "fanotify_monitor_degraded",
        ];
        for s in known {
            let parsed = LedgerEventType::parse(s);
            assert_ne!(
                matches!(parsed, LedgerEventType::Other(_)),
                true,
                "'{s}' should parse to a known variant"
            );
            assert_eq!(parsed.as_str(), s);
        }
    }

    #[test]
    fn ledger_event_type_unknown_goes_to_other() {
        let evt = LedgerEventType::parse("custom_event_xyz");
        assert_eq!(evt, LedgerEventType::Other("custom_event_xyz".to_string()));
        assert_eq!(evt.as_str(), "custom_event_xyz");
    }

    #[test]
    fn ledger_event_type_display() {
        assert_eq!(
            format!("{}", LedgerEventType::GuardStarted),
            "guard_started"
        );
        assert_eq!(
            format!("{}", LedgerEventType::Other("foo".into())),
            "foo"
        );
    }

    #[test]
    fn ledger_event_type_from_str_is_infallible() {
        let result: Result<LedgerEventType, _> = "anything".parse();
        assert!(result.is_ok());
    }

    #[test]
    fn ledger_event_type_from_ref_str() {
        let evt: LedgerEventType = "guard_started".into();
        assert_eq!(evt, LedgerEventType::GuardStarted);
    }

    // ── DbGuardEngine ──

    #[test]
    fn db_guard_engine_as_str_roundtrip() {
        let variants = [
            (DbGuardEngine::Sqlite, "sqlite"),
            (DbGuardEngine::Postgres, "postgres"),
            (DbGuardEngine::Mysql, "mysql"),
        ];
        for (variant, expected) in variants {
            assert_eq!(variant.as_str(), expected);
            assert_eq!(DbGuardEngine::parse(expected), Some(variant));
        }
    }

    #[test]
    fn db_guard_engine_parse_unknown() {
        assert_eq!(DbGuardEngine::parse("oracle"), None);
        assert_eq!(DbGuardEngine::parse(""), None);
    }

    #[test]
    fn db_guard_engine_display_and_from_str() {
        assert_eq!(format!("{}", DbGuardEngine::Postgres), "postgres");
        assert_eq!(
            "sqlite".parse::<DbGuardEngine>(),
            Ok(DbGuardEngine::Sqlite)
        );
        assert!("unknown".parse::<DbGuardEngine>().is_err());
    }

    #[test]
    fn db_guard_engine_case_insensitive_eq() {
        assert!(DbGuardEngine::Sqlite == "SQLITE");
        assert!(DbGuardEngine::Postgres == "Postgres");
    }

    // ── DbGuardMode ──

    #[test]
    fn db_guard_mode_as_str_roundtrip() {
        let variants = [
            (DbGuardMode::Triggers, "triggers"),
            (DbGuardMode::SchemaPolling, "schema_polling"),
        ];
        for (variant, expected) in variants {
            assert_eq!(variant.as_str(), expected);
            assert_eq!(DbGuardMode::parse(expected), Some(variant));
        }
    }

    #[test]
    fn db_guard_mode_parse_unknown() {
        assert_eq!(DbGuardMode::parse("listen"), None);
    }

    #[test]
    fn db_guard_mode_display_and_from_str() {
        assert_eq!(format!("{}", DbGuardMode::Triggers), "triggers");
        assert_eq!(
            "schema_polling".parse::<DbGuardMode>(),
            Ok(DbGuardMode::SchemaPolling)
        );
        assert!("invalid".parse::<DbGuardMode>().is_err());
    }

    #[test]
    fn db_guard_mode_eq_ignore_ascii_case() {
        assert!(DbGuardMode::Triggers.eq_ignore_ascii_case("TRIGGERS"));
        assert!(DbGuardMode::SchemaPolling.eq_ignore_ascii_case("Schema_Polling"));
        assert!(!DbGuardMode::Triggers.eq_ignore_ascii_case("schema_polling"));
    }

    #[test]
    fn db_guard_mode_case_insensitive_partial_eq() {
        assert!(DbGuardMode::Triggers == "TRIGGERS");
    }

    // ── Helper functions ──

    #[test]
    fn checked_db_u64_positive_values() {
        assert_eq!(checked_db_u64(0, "test").unwrap(), 0);
        assert_eq!(checked_db_u64(42, "test").unwrap(), 42);
        assert_eq!(checked_db_u64(i64::MAX, "test").unwrap(), i64::MAX as u64);
    }

    #[test]
    fn checked_db_u64_negative_returns_error() {
        assert!(checked_db_u64(-1, "test").is_err());
        assert!(checked_db_u64(i64::MIN, "test").is_err());
    }

    #[test]
    fn checked_usize_u64_conversions() {
        assert_eq!(checked_usize_u64(0, "test").unwrap(), 0);
        assert_eq!(checked_usize_u64(100, "test").unwrap(), 100);
    }

    // ── StorageMethod (from cas module, used in db) ──

    #[test]
    fn storage_method_to_from_db_roundtrip() {
        assert_eq!(StorageMethod::from_db(StorageMethod::None.to_db()), StorageMethod::None);
        assert_eq!(StorageMethod::from_db(StorageMethod::Copy.to_db()), StorageMethod::Copy);
        assert_eq!(StorageMethod::from_db(StorageMethod::Reflink.to_db()), StorageMethod::Reflink);
    }

    #[test]
    fn storage_method_from_db_unknown_defaults_to_none() {
        assert_eq!(StorageMethod::from_db(99), StorageMethod::None);
        assert_eq!(StorageMethod::from_db(-1), StorageMethod::None);
    }

    #[test]
    fn storage_method_is_recoverable() {
        assert!(!StorageMethod::None.is_recoverable());
        assert!(StorageMethod::Copy.is_recoverable());
        assert!(StorageMethod::Reflink.is_recoverable());
    }

    #[test]
    fn storage_method_display_name() {
        assert_eq!(StorageMethod::None.display_name(), "none");
        assert_eq!(StorageMethod::Copy.display_name(), "copy");
        assert_eq!(StorageMethod::Reflink.display_name(), "reflink");
    }

    // ── Database open + CRUD ──

    #[test]
    fn database_open_and_basic_operations() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let db = Database::open(&db_path).unwrap();

        // List empty tables
        let projects = db.list_projects().unwrap();
        assert!(projects.is_empty());

        let guards = db.list_db_guards().unwrap();
        assert!(guards.is_empty());

        let agents = db.list_agents().unwrap();
        assert!(agents.is_empty());
    }

    #[test]
    fn database_agent_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let db = Database::open(&db_path).unwrap();

        db.add_agent("test-agent", "/path/to/profile", Some("/data")).unwrap();
        let agents = db.list_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "test-agent");
        assert_eq!(agents[0].profile_path, "/path/to/profile");
        assert_eq!(agents[0].data_dir.as_deref(), Some("/data"));
        assert!(agents[0].active);

        db.remove_agent("test-agent").unwrap();
        let agents = db.list_agents().unwrap();
        assert!(agents.is_empty());
    }

    #[test]
    fn database_agent_no_data_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let db = Database::open(&db_path).unwrap();

        db.add_agent("agent2", "/profile", None).unwrap();
        let agents = db.list_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert!(agents[0].data_dir.is_none());
    }

    #[test]
    fn database_clone_handle() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("test.db");
        let db = Database::open(&db_path).unwrap();

        db.add_agent("agent-a", "/p", None).unwrap();

        let db2 = db.clone_handle();
        let agents = db2.list_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "agent-a");
    }

    #[test]
    fn database_backup() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("source.db");
        let db = Database::open(&db_path).unwrap();

        db.add_agent("backup-test", "/p", None).unwrap();

        let backup_path = tmp.path().join("backup.db");
        db.backup_to(&backup_path).unwrap();

        let db2 = Database::open(&backup_path).unwrap();
        let agents = db2.list_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "backup-test");
    }
}
