use std::collections::HashMap;

use anyhow::Result;
use rusqlite::Connection;

use crate::db::DbGuardEntry;
use crate::db::NewEventLedgerEntry;
use crate::db_guard::recovery;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

pub fn tick_sqlite_guard(
    ctx: &SubsystemContext,
    guard: &DbGuardEntry,
    versions: &mut HashMap<String, i64>,
) -> Result<()> {
    let path = normalize_sqlite_path(&guard.connection_ref);

    let conn = Connection::open(&path)?;
    let data_version: i64 = conn.query_row("PRAGMA data_version", [], |row| row.get(0))?;
    let detail = serde_json::json!({"data_version": data_version}).to_string();
    let mut event = NewEventLedgerEntry {
        source: "db_guard".to_string(),
        event_type: "sqlite_tick".to_string(),
        severity: "info".to_string(),
        project_hash: None,
        agent_name: None,
        guard_name: Some(guard.name.clone()),
        path: Some(path.clone()),
        detail: Some(detail),
        pre_state_ref: None,
        post_state_ref: None,
        prev_hash: None,
        causal_parent: None,
    };

    let prev_version = versions.get(&guard.name).copied();
    versions.insert(guard.name.clone(), data_version);

    if prev_version.is_some() && prev_version != Some(data_version) {
        event.event_type = "sqlite_data_changed".to_string();
        event.severity = "warn".to_string();
        let artifact = recovery::write_sqlite_schema_recovery(
            &ctx.uhoh_dir,
            &guard.name,
            std::path::Path::new(&path),
            "data_change",
            ctx.config.db_guard.encrypt_recovery,
            ctx.config.db_guard.recovery_retention_days,
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
        )?;
        let ts = chrono::Utc::now().to_rfc3339();
        let _ = ctx.database.set_db_guard_baseline_time(&guard.name, &ts);
        let mut baseline_event = new_event("db_guard", "sqlite_baseline", "info");
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

    // Keep lightweight event for compatibility with existing consumers.
    let mut event = new_event("db_guard", "sqlite_tick", "info");
    event.guard_name = Some(guard.name.clone());
    event.path = Some(path.clone());
    event.detail = Some(format!("watch_path={path}"));
    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append sqlite_tick compatibility event: {err}");
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
