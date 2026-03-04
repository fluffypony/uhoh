use anyhow::{Context, Result};
use tokio_postgres::NoTls;

use crate::db::DbGuardEntry;
use crate::db::NewEventLedgerEntry;
use crate::db_guard::credentials;
use crate::db_guard::recovery;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

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
            let _ = ctx.event_ledger.append(event);
        }
    }

    // Lightweight delete-counter polling.
    if let Ok(deleted_rows) = poll_delete_count(&guard.connection_ref) {
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
                let _ = ctx.event_ledger.append(NewEventLedgerEntry {
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
                    causal_parent: None,
                });
            }
        }
    }

    // Listen/notify with short timeout and one-shot read for DDL events.
    if let Ok(Some(payload)) = try_listen_once(&guard.connection_ref) {
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
            let _ = ctx.event_ledger.append(NewEventLedgerEntry {
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
                causal_parent: None,
            });
        }
    }

    let mut event = new_event("db_guard", "postgres_tick", "info");
    event.guard_name = Some(guard.name.clone());
    event.detail = Some(format!(
        "mode={}, dsn_ref={}",
        guard.mode,
        scrub_ref(&guard.connection_ref)
    ));
    let _ = ctx.event_ledger.append(event);
    Ok(())
}

fn poll_delete_count(connection_ref: &str) -> Result<i64> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to build runtime for postgres delete polling")?;
    rt.block_on(async move {
        let (client, connection) = tokio_postgres::connect(connection_ref, NoTls)
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "SELECT COALESCE(SUM(delete_count), 0) FROM _uhoh_delete_counts WHERE ts > now() - interval '10 seconds'",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        let value: i64 = row.get(0);
        Ok(value)
    })
}

fn try_listen_once(connection_ref: &str) -> Result<Option<String>> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to build runtime for postgres listen")?;
    rt.block_on(async move {
        let (client, connection) = tokio_postgres::connect(connection_ref, NoTls)
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        client
            .batch_execute("LISTEN uhoh_events")
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;

        tokio::spawn(async move {
            let _ = connection.await;
        });

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(100);
        let row = tokio::time::timeout_at(
            deadline,
            client.query_opt(
                "SELECT payload::text FROM _uhoh_ddl_events ORDER BY id DESC LIMIT 1",
                &[],
            ),
        )
        .await;

        match row {
            Ok(Ok(Some(row))) => {
                let payload: String = row.get(0);
                return Ok(Some(payload));
            }
            Ok(Ok(None)) => return Ok(None),
            Ok(Err(e)) => {
                return Err(anyhow::anyhow!(credentials::scrub_error_message(
                    &e.to_string()
                )));
            }
            Err(_) => return Ok(None),
        }

        #[allow(unreachable_code)]
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => return Ok(None),
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => return Ok(None),
            }
        }
    })
}

fn scrub_ref(connection_ref: &str) -> String {
    connection_ref
        .split('@')
        .last()
        .unwrap_or(connection_ref)
        .to_string()
}
