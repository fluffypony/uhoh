use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::Mutex;

use once_cell::sync::Lazy;
use tokio_postgres::NoTls;

use crate::db::DbGuardEntry;
use crate::db::NewEventLedgerEntry;
use crate::db_guard::credentials;
use crate::db_guard::recovery;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

const GUARD_TICK_SECS: i64 = 30;
static PG_DDL_CURSOR: Lazy<Mutex<HashMap<String, i64>>> = Lazy::new(|| Mutex::new(HashMap::new()));

pub fn tick_postgres_guard(ctx: &SubsystemContext, guard: &DbGuardEntry) -> Result<()> {
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
        let creds = credentials::resolve_postgres_credentials(&guard.connection_ref)?;
        if let Ok(info) = recovery::write_postgres_schema_baseline(
            &ctx.uhoh_dir,
            &guard.name,
            &guard.connection_ref,
            &creds,
            ctx.config.db_guard.recovery_retention_days,
        ) {
            let ts = chrono::Utc::now().to_rfc3339();
            let _ = ctx.database.set_db_guard_baseline_time(&guard.name, &ts);
            let mut event = new_event("db_guard", "postgres_baseline", "info");
            event.guard_name = Some(guard.name.clone());
            event.detail = Some(format!("artifact={}, blake3={}", info.path, info.blake3));
            if let Err(err) = ctx.event_ledger.append(event) {
                tracing::error!("failed to append postgres_baseline event: {err}");
            }
        }
    }

    // Lightweight delete-counter polling.
    if let Ok(deleted_rows) = poll_delete_count(&guard.connection_ref, GUARD_TICK_SECS * 2) {
        if deleted_rows >= ctx.config.db_guard.mass_delete_row_threshold as i64 {
            let creds = credentials::resolve_postgres_credentials(&guard.connection_ref)?;
            if let Ok(artifact) = recovery::write_postgres_schema_recovery(
                &ctx.uhoh_dir,
                &guard.name,
                &guard.connection_ref,
                &creds,
                "mass_delete",
                ctx.config.db_guard.encrypt_recovery,
                ctx.config.db_guard.recovery_retention_days,
            ) {
                let detail = serde_json::json!({
                    "deleted_rows": deleted_rows,
                    "artifact": artifact.path,
                    "blake3": artifact.blake3,
                })
                .to_string();
                if let Err(err) = ctx.event_ledger.append(NewEventLedgerEntry {
                    source: "db_guard".to_string(),
                    event_type: "mass_delete".to_string(),
                    severity: "critical".to_string(),
                    project_hash: None,
                    agent_name: None,
                    guard_name: Some(guard.name.clone()),
                    path: None,
                    detail: Some(detail),
                    pre_state_ref: Some(artifact.blake3),
                    post_state_ref: None,
                    prev_hash: None,
                    causal_parent: None,
                }) {
                    tracing::error!("failed to append mass_delete event: {err}");
                }
            }
        }
    }

    // Poll unseen DDL events from the trigger table using a persisted cursor.
    if let Ok(payloads) = poll_ddl_events(&guard.connection_ref, 64) {
        for payload in payloads {
            let creds = credentials::resolve_postgres_credentials(&guard.connection_ref)?;
            if let Ok(artifact) = recovery::write_postgres_schema_recovery(
                &ctx.uhoh_dir,
                &guard.name,
                &guard.connection_ref,
                &creds,
                "ddl",
                ctx.config.db_guard.encrypt_recovery,
                ctx.config.db_guard.recovery_retention_days,
            ) {
                let detail = serde_json::json!({
                    "notify_payload": payload,
                    "artifact": artifact.path,
                    "blake3": artifact.blake3,
                })
                .to_string();
                if let Err(err) = ctx.event_ledger.append(NewEventLedgerEntry {
                    source: "db_guard".to_string(),
                    event_type: "drop_table".to_string(),
                    severity: "critical".to_string(),
                    project_hash: None,
                    agent_name: None,
                    guard_name: Some(guard.name.clone()),
                    path: None,
                    detail: Some(detail),
                    pre_state_ref: Some(artifact.blake3),
                    post_state_ref: None,
                    prev_hash: None,
                    causal_parent: None,
                }) {
                    tracing::error!("failed to append drop_table event: {err}");
                }
            }
        }
    }

    let mut event = new_event("db_guard", "postgres_tick", "info");
    event.guard_name = Some(guard.name.clone());
    event.detail = Some(format!(
        "mode={}, dsn_ref={}",
        guard.mode,
        scrub_ref(&guard.connection_ref)
    ));
    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append postgres_tick event: {err}");
    }
    Ok(())
}

fn poll_delete_count(connection_ref: &str, window_seconds: i64) -> Result<i64> {
    run_postgres_task(async move {
        let (client, connection) = tokio_postgres::connect(connection_ref, NoTls)
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "SELECT COALESCE(SUM(delete_count), 0)
                 FROM _uhoh_delete_counts
                 WHERE ts > now() - ($1::text || ' seconds')::interval",
                &[&window_seconds.to_string()],
            )
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        let value: i64 = row.get(0);
        Ok(value)
    })
}

fn poll_ddl_events(connection_ref: &str, max_rows: i64) -> Result<Vec<String>> {
    let last_seen_id = {
        let cache = PG_DDL_CURSOR
            .lock()
            .map_err(|_| anyhow::anyhow!("Postgres DDL cursor lock poisoned"))?;
        cache.get(connection_ref).copied().unwrap_or(0)
    };

    let result = run_postgres_task(async move {
        let (client, connection) = tokio_postgres::connect(connection_ref, NoTls)
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;

        tokio::spawn(async move {
            let _ = connection.await;
        });

        let rows = client
            .query(
                "SELECT id, payload::text
                 FROM _uhoh_ddl_events
                 WHERE id > $1
                 ORDER BY id ASC
                 LIMIT $2",
                &[&last_seen_id, &max_rows],
            )
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        let mut out = Vec::new();
        let mut latest_id = last_seen_id;
        for row in rows {
            let id: i64 = row.get(0);
            let payload: String = row.get(1);
            latest_id = latest_id.max(id);
            out.push((id, payload));
        }
        if latest_id > last_seen_id {
            Ok(Some((latest_id, out)))
        } else {
            Ok(None)
        }
    })?;

    if let Some((id, rows)) = result {
        let mut cache = PG_DDL_CURSOR
            .lock()
            .map_err(|_| anyhow::anyhow!("Postgres DDL cursor lock poisoned"))?;
        cache.insert(connection_ref.to_string(), id);
        Ok(rows.into_iter().map(|(_, payload)| payload).collect())
    } else {
        Ok(Vec::new())
    }
}

fn run_postgres_task<T, F>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(fut))
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Failed to build runtime for postgres operation")?;
        rt.block_on(fut)
    }
}

fn scrub_ref(connection_ref: &str) -> String {
    connection_ref
        .split('@')
        .last()
        .unwrap_or(connection_ref)
        .to_string()
}
