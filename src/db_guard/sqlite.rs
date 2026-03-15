use std::collections::HashMap;

use anyhow::Result;
use rusqlite::Connection;

use crate::db::{DbGuardEntry, LedgerEventType, LedgerSeverity, LedgerSource};
use super::recovery;
use crate::event_ledger::new_event;
use crate::subsystem::DbGuardContext;

pub fn tick_sqlite_guard(
    ctx: &DbGuardContext,
    guard: &DbGuardEntry,
    versions: &mut HashMap<String, i64>,
) -> Result<()> {
    let path = normalize_sqlite_path(&guard.connection_ref);

    let conn = Connection::open_with_flags(
        &path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.execute_batch("PRAGMA busy_timeout = 5000;")?;
    let data_version: i64 = conn.query_row("PRAGMA data_version", [], |row| row.get(0))?;
    let detail = serde_json::json!({"data_version": data_version}).to_string();
    let mut event = new_event(LedgerSource::DbGuard, LedgerEventType::SqliteTick, LedgerSeverity::Info);
    event.guard_name = Some(guard.name.clone());
    event.path = Some(path.clone());
    event.detail = Some(detail);

    let prev_version = versions.get(&guard.name).copied();
    versions.insert(guard.name.clone(), data_version);

    if prev_version.is_some() && prev_version != Some(data_version) {
        event.event_type = crate::db::LedgerEventType::SqliteDataChanged;
        event.severity = LedgerSeverity::Warn;
        let artifact = recovery::write_sqlite_schema_recovery(
            &ctx.uhoh_dir,
            &guard.name,
            std::path::Path::new(&path),
            "data_change",
            ctx.config.db_guard.encrypt_recovery,
            ctx.config.db_guard.recovery_retention_days,
            ctx.config.db_guard.max_recovery_file_mb,
        )?;
        event.pre_state_ref = Some(artifact.blake3.clone());
        event.detail = Some(
            serde_json::json!({
                "data_version": data_version,
                "artifact": artifact.path,
                "blake3": artifact.blake3
            })
            .to_string(),
        );
    }

    let baseline_interval = std::time::Duration::from_secs(
        ctx.config
            .db_guard
            .baseline_interval_hours
            .saturating_mul(3600),
    );
    let needs_baseline = guard
        .last_baseline_at
        .as_deref()
        .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
        .map(|ts| chrono::Utc::now().signed_duration_since(ts.with_timezone(&chrono::Utc)))
        .map(|elapsed| elapsed.to_std().unwrap_or_default() >= baseline_interval)
        .unwrap_or(true);
    if needs_baseline {
        let info = recovery::write_sqlite_baseline(
            &ctx.uhoh_dir,
            &guard.name,
            std::path::Path::new(&path),
            ctx.config.db_guard.encrypt_recovery,
            ctx.config.db_guard.recovery_retention_days,
            ctx.config.db_guard.max_baseline_size_mb,
        )?;
        let ts = chrono::Utc::now().to_rfc3339();
        if let Err(err) = ctx.database.set_db_guard_baseline_time(&guard.name, &ts) {
            tracing::warn!(
                "failed to persist sqlite baseline timestamp for guard {}: {}",
                guard.name,
                err
            );
        }
        let mut baseline_event = new_event(
            LedgerSource::DbGuard,
            LedgerEventType::SqliteBaseline,
            LedgerSeverity::Info,
        );
        baseline_event.guard_name = Some(guard.name.clone());
        baseline_event.path = Some(path.clone());
        baseline_event.detail = Some(
            serde_json::json!({
                "artifact": info.path,
                "blake3": info.blake3
            })
            .to_string(),
        );
        if let Err(err) = ctx.event_ledger.append(baseline_event) {
            tracing::error!("failed to append sqlite_baseline event: {err}");
        }
    }

    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append sqlite guard event: {err}");
    }
    Ok(())
}

fn normalize_sqlite_path(connection_ref: &str) -> String {
    if let Some(stripped) = connection_ref.strip_prefix("sqlite://") {
        stripped.to_string()
    } else {
        connection_ref.to_string()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn normalize_sqlite_path_strips_scheme() {
        assert_eq!(
            normalize_sqlite_path("sqlite:///var/db/app.db"),
            "/var/db/app.db"
        );
    }

    #[test]
    fn normalize_sqlite_path_no_scheme() {
        assert_eq!(
            normalize_sqlite_path("/var/db/app.db"),
            "/var/db/app.db"
        );
    }

    #[test]
    fn normalize_sqlite_path_relative() {
        assert_eq!(normalize_sqlite_path("data/test.db"), "data/test.db");
    }

    #[test]
    fn normalize_sqlite_path_just_scheme() {
        assert_eq!(normalize_sqlite_path("sqlite://"), "");
    }
}
