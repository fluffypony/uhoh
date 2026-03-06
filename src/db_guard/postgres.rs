use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use once_cell::sync::Lazy;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

use crate::db::DbGuardEntry;
use crate::db::NewEventLedgerEntry;
use crate::db_guard::credentials;
use crate::db_guard::recovery;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

/// Build a connection string with resolved credentials injected.
fn url_encode_param(s: &str) -> String {
    // Percent-encode characters that are problematic in URL query parameters
    s.replace('%', "%25")
        .replace('&', "%26")
        .replace('=', "%3D")
        .replace('+', "%2B")
        .replace('#', "%23")
        .replace(' ', "%20")
        .replace('?', "%3F")
        .replace('@', "%40")
}

fn build_connect_dsn(connection_ref: &str) -> Result<String> {
    let creds = credentials::resolve_postgres_credentials(connection_ref)?;
    let is_url = connection_ref.starts_with("postgres://")
        || connection_ref.starts_with("postgresql://");

    let mut dsn = connection_ref.to_string();
    if let Some(ref pw) = creds.password {
        if !dsn.contains("password=") && !dsn.contains("password%3D") {
            if is_url {
                // URL-form: use query parameter syntax with proper encoding
                let sep = if dsn.contains('?') { "&" } else { "?" };
                dsn.push_str(&format!("{}password={}", sep, url_encode_param(pw)));
            } else {
                dsn.push_str(&format!(" password={pw}"));
            }
        }
    }
    if let Some(ref user) = creds.username {
        if !dsn.contains("user=") && !dsn.contains("user%3D") {
            if is_url {
                let sep = if dsn.contains('?') { "&" } else { "?" };
                dsn.push_str(&format!("{}user={}", sep, url_encode_param(user)));
            } else {
                dsn.push_str(&format!(" user={user}"));
            }
        }
    }
    Ok(dsn)
}

static PG_DDL_CURSOR: Lazy<Mutex<HashMap<String, i64>>> = Lazy::new(|| Mutex::new(HashMap::new()));
struct ListenWorker {
    queue: std::sync::Arc<Mutex<Vec<String>>>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
static PG_LISTEN_WORKERS: Lazy<Mutex<HashMap<String, ListenWorker>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub fn tick_postgres_guard(
    ctx: &SubsystemContext,
    guard: &DbGuardEntry,
    tick_interval_secs: i64,
) -> Result<()> {
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

    // Lightweight delete-counter polling (use resolved credentials).
    let poll_dsn = build_connect_dsn(&guard.connection_ref).unwrap_or_else(|_| guard.connection_ref.clone());
    let poll_window_secs = tick_interval_secs.saturating_mul(2).max(1);
    if let Ok(deleted_rows) = poll_delete_count(&poll_dsn, poll_window_secs) {
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

    let listen_payloads = drain_listen_payloads(&poll_dsn)?;
    let ddl_payloads = poll_ddl_events(&poll_dsn, 64).unwrap_or_default();
    let mut payloads = Vec::new();
    payloads.extend(listen_payloads);
    payloads.extend(ddl_payloads);
    if !payloads.is_empty() {
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

pub fn reconcile_listen_workers(
    guards: &[DbGuardEntry],
    shutdown: &CancellationToken,
) -> Result<()> {
    let required: HashSet<String> = guards
        .iter()
        .filter(|g| g.engine == "postgres")
        .map(|g| build_connect_dsn(&g.connection_ref).unwrap_or_else(|_| g.connection_ref.clone()))
        .collect();

    let mut workers = PG_LISTEN_WORKERS
        .lock()
        .map_err(|_| anyhow::anyhow!("Postgres LISTEN worker map lock poisoned"))?;

    let stale = workers
        .keys()
        .filter(|dsn| !required.contains(*dsn))
        .cloned()
        .collect::<Vec<_>>();
    for dsn in stale {
        if let Some(worker) = workers.remove(&dsn) {
            worker.cancel.cancel();
            worker.task.abort();
        }
    }

    for dsn in required {
        if workers.contains_key(&dsn) {
            continue;
        }
        let queue = std::sync::Arc::new(Mutex::new(Vec::new()));
        let cancel = shutdown.child_token();
        let queue_cl = queue.clone();
        let dsn_cl = dsn.clone();
        let cancel_cl = cancel.clone();
        let task = tokio::spawn(async move {
            run_listen_worker(dsn_cl, queue_cl, cancel_cl).await;
        });
        workers.insert(
            dsn,
            ListenWorker {
                queue,
                cancel,
                task,
            },
        );
    }

    Ok(())
}

pub fn shutdown_all_listen_workers() {
    if let Ok(mut workers) = PG_LISTEN_WORKERS.lock() {
        for (_, worker) in workers.drain() {
            worker.cancel.cancel();
            worker.task.abort();
        }
    }
}

fn poll_delete_count(connection_ref: &str, window_seconds: i64) -> Result<i64> {
    run_postgres_task(async move {
        let (client, connection) = tokio_postgres::connect(connection_ref, NoTls)
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = match client
            .query_one(
                "SELECT COALESCE(SUM(delete_count), 0)
                 FROM _uhoh_delete_counts
                 WHERE ts > now() - ($1::text || ' seconds')::interval",
                &[&window_seconds.to_string()],
            )
            .await
        {
            Ok(row) => row,
            Err(e) => {
                let scrubbed = credentials::scrub_error_message(&e.to_string());
                if scrubbed.contains("does not exist") {
                    return Ok(0);
                }
                return Err(anyhow::anyhow!(scrubbed));
            }
        };
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

async fn run_listen_worker(
    connection_ref: String,
    queue: std::sync::Arc<Mutex<Vec<String>>>,
    shutdown: CancellationToken,
) {
    let mut backoff = std::time::Duration::from_secs(1);
    let max_backoff = std::time::Duration::from_secs(30);
    let mut last_seen_id = match PG_DDL_CURSOR.lock() {
        Ok(cache) => cache.get(&connection_ref).copied().unwrap_or(0),
        Err(_) => 0,
    };

    loop {
        if shutdown.is_cancelled() {
            return;
        }

        let (client, connection) = match tokio_postgres::connect(&connection_ref, NoTls).await {
            Ok(pair) => pair,
            Err(err) => {
                tracing::warn!(
                    "postgres LISTEN connect failed for {}: {}",
                    scrub_ref(&connection_ref),
                    credentials::scrub_error_message(&err.to_string())
                );
                if sleep_or_cancel(backoff, &shutdown).await {
                    return;
                }
                backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
                continue;
            }
        };

        if let Err(err) = client.batch_execute("LISTEN uhoh_events;").await {
            tracing::warn!(
                "postgres LISTEN setup failed for {}: {}",
                scrub_ref(&connection_ref),
                credentials::scrub_error_message(&err.to_string())
            );
            if sleep_or_cancel(backoff, &shutdown).await {
                return;
            }
            backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
            continue;
        }

        let connection_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        backoff = std::time::Duration::from_secs(1);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    connection_task.abort();
                    return;
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    let rows = client
                        .query(
                            "SELECT id, payload::text
                             FROM _uhoh_ddl_events
                             WHERE id > $1
                             ORDER BY id ASC
                             LIMIT 64",
                            &[&last_seen_id],
                        )
                        .await;
                    match rows {
                        Ok(values) => {
                            if !values.is_empty() {
                                let mut fresh = Vec::with_capacity(values.len());
                                for row in values {
                                    let id: i64 = row.get(0);
                                    let payload: String = row.get(1);
                                    last_seen_id = last_seen_id.max(id);
                                    fresh.push(payload);
                                }
                                if let Ok(mut cache) = PG_DDL_CURSOR.lock() {
                                    cache.insert(connection_ref.clone(), last_seen_id);
                                }
                                if let Ok(mut pending) = queue.lock() {
                                    pending.extend(fresh);
                                    if pending.len() > 1024 {
                                        let drain = pending.len().saturating_sub(1024);
                                        pending.drain(0..drain);
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                "postgres LISTEN poll failed for {}: {}",
                                scrub_ref(&connection_ref),
                                credentials::scrub_error_message(&err.to_string())
                            );
                            connection_task.abort();
                            break;
                        }
                    }
                }
            }
        }

        if sleep_or_cancel(backoff, &shutdown).await {
            return;
        }
        backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
    }
}

fn drain_listen_payloads(connection_ref: &str) -> Result<Vec<String>> {
    let workers = PG_LISTEN_WORKERS
        .lock()
        .map_err(|_| anyhow::anyhow!("Postgres LISTEN worker map lock poisoned"))?;
    let Some(worker) = workers.get(connection_ref) else {
        return Ok(Vec::new());
    };
    let mut pending = worker
        .queue
        .lock()
        .map_err(|_| anyhow::anyhow!("Postgres LISTEN payload queue lock poisoned"))?;
    Ok(std::mem::take(&mut *pending))
}

async fn sleep_or_cancel(duration: std::time::Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
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
