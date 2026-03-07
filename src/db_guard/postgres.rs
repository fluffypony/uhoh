use anyhow::{Context, Result};
use rustls::RootCertStore;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use once_cell::sync::Lazy;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_util::sync::CancellationToken;

use crate::db::DbGuardEntry;
use crate::db::NewEventLedgerEntry;
use crate::db_guard::credentials;
use crate::db_guard::recovery;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

/// Build a connection string with resolved credentials injected.
pub fn build_connect_dsn(connection_ref: &str) -> Result<String> {
    let creds = credentials::resolve_postgres_credentials(connection_ref)?;
    if connection_ref.starts_with("postgres://") || connection_ref.starts_with("postgresql://") {
        let mut url = url::Url::parse(connection_ref)
            .with_context(|| format!("Invalid Postgres connection reference: {connection_ref}"))?;

        if let Some(ref user) = creds.username {
            let _ = url.set_username(user);
        }
        if let Some(ref pw) = creds.password {
            let _ = url.set_password(Some(pw));
        }

        return Ok(url.to_string());
    }

    let mut parts: Vec<String> = connection_ref
        .split_whitespace()
        .map(|p| p.to_string())
        .collect();
    let has_user = parts.iter().any(|p| {
        p.split_once('=')
            .is_some_and(|(k, _)| k.eq_ignore_ascii_case("user"))
    });
    let has_password = parts.iter().any(|p| {
        p.split_once('=')
            .is_some_and(|(k, _)| k.eq_ignore_ascii_case("password"))
    });
    if let Some(ref user) = creds.username {
        if !has_user {
            parts.push(format!("user={user}"));
        }
    }
    if let Some(ref pw) = creds.password {
        if !has_password {
            parts.push(format!("password={pw}"));
        }
    }
    Ok(parts.join(" "))
}

static PG_DDL_CURSOR: Lazy<Mutex<HashMap<String, i64>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static PG_RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build postgres task runtime")
});
struct DdlPollWorker {
    queue: std::sync::Arc<Mutex<Vec<String>>>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
static PG_DDL_POLL_WORKERS: Lazy<Mutex<HashMap<String, DdlPollWorker>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub fn tick_postgres_guard(
    ctx: &SubsystemContext,
    guard: &DbGuardEntry,
    tick_interval_secs: i64,
) -> Result<()> {
    let poll_dsn =
        build_connect_dsn(&guard.connection_ref).unwrap_or_else(|_| guard.connection_ref.clone());

    if guard.tables_csv.trim() == "*" {
        if let Err(err) = reconcile_wildcard_delete_triggers(ctx, guard, &poll_dsn) {
            tracing::warn!(
                "postgres wildcard trigger reconcile failed for {}: {}",
                guard.name,
                err
            );
        }
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
        let creds = credentials::resolve_postgres_credentials(&guard.connection_ref)?;
        if let Ok(info) = recovery::write_postgres_schema_baseline(
            &ctx.uhoh_dir,
            &guard.name,
            &guard.connection_ref,
            &creds,
            ctx.config.db_guard.recovery_retention_days,
            ctx.config.db_guard.max_baseline_size_mb,
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

    // Respect configured guard mode: in schema_polling mode we intentionally skip
    // row-level trigger counters and only rely on schema/DDL signal paths.
    if guard.mode.eq_ignore_ascii_case("schema_polling") {
        let mut event = new_event("db_guard", "postgres_tick", "info");
        event.guard_name = Some(guard.name.clone());
        event.detail = Some(format!(
            "mode={}, dsn_ref={}, row_counters=disabled",
            guard.mode,
            scrub_ref(&guard.connection_ref)
        ));
        if let Err(err) = ctx.event_ledger.append(event) {
            tracing::error!("failed to append postgres_tick event: {err}");
        }
        return Ok(());
    }

    // Lightweight delete-counter polling (use resolved credentials).
    let poll_window_secs = tick_interval_secs.saturating_mul(2).max(1);
    if let Ok(deleted_rows) = poll_delete_count(&poll_dsn, poll_window_secs, &guard.tables_csv) {
        let total_rows = poll_total_row_count(&poll_dsn).unwrap_or(0);
        let pct = if total_rows > 0 {
            deleted_rows as f64 / total_rows as f64
        } else {
            0.0
        };
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
                ctx.config.db_guard.max_recovery_file_mb,
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
        } else if pct >= ctx.config.db_guard.mass_delete_pct_threshold {
            let creds = credentials::resolve_postgres_credentials(&guard.connection_ref)?;
            if let Ok(artifact) = recovery::write_postgres_schema_recovery(
                &ctx.uhoh_dir,
                &guard.name,
                &guard.connection_ref,
                &creds,
                "mass_delete_pct",
                ctx.config.db_guard.encrypt_recovery,
                ctx.config.db_guard.recovery_retention_days,
                ctx.config.db_guard.max_recovery_file_mb,
            ) {
                let detail = serde_json::json!({
                    "deleted_rows": deleted_rows,
                    "total_rows": total_rows,
                    "deleted_ratio": pct,
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
                ctx.config.db_guard.max_recovery_file_mb,
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

fn reconcile_wildcard_delete_triggers(
    ctx: &SubsystemContext,
    guard: &DbGuardEntry,
    poll_dsn: &str,
) -> Result<()> {
    let current = fetch_current_schema_tables(poll_dsn)?;
    if current.is_empty() {
        return Ok(());
    }

    let mut current_sorted = current;
    current_sorted.sort();
    current_sorted.dedup();

    let previous: std::collections::HashSet<String> = guard
        .watched_tables_cache
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .collect();

    let mut added = Vec::new();
    for table in &current_sorted {
        if !previous.contains(table) {
            added.push(table.clone());
        }
    }

    if !added.is_empty() {
        install_delete_counter_triggers_for_tables(poll_dsn, &added)?;
        let mut event = new_event("db_guard", "postgres_wildcard_trigger_reconciled", "info");
        event.guard_name = Some(guard.name.clone());
        event.detail = Some(
            serde_json::json!({
                "added_tables": added,
                "guard": guard.name,
            })
            .to_string(),
        );
        let _ = ctx.event_ledger.append(event);
    }

    let cache = current_sorted.join(",");
    let _ = ctx
        .database
        .set_db_guard_watched_tables_cache(&guard.name, Some(&cache));
    Ok(())
}

fn fetch_current_schema_tables(connection_ref: &str) -> Result<Vec<String>> {
    let connection_ref = connection_ref.to_string();
    run_postgres_task(async move {
        let client = pg_connect_spawn(&connection_ref).await?;
        let rows = client
            .query(
                "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = current_schema()",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        Ok(rows
            .into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>())
    })
}

fn install_delete_counter_triggers_for_tables(
    connection_ref: &str,
    tables: &[String],
) -> Result<()> {
    if tables.is_empty() {
        return Ok(());
    }
    let connection_ref = connection_ref.to_string();
    let tables = tables.to_vec();
    run_postgres_task(async move {
        let client = pg_connect_spawn(&connection_ref).await?;
        for table in tables {
            install_delete_counter_trigger_sql(&client, &table).await?;
        }
        Ok(())
    })
}

async fn install_delete_counter_trigger_sql(
    client: &tokio_postgres::Client,
    table: &str,
) -> Result<()> {
    let table_safe = table.replace('\'', "''");
    let table_quoted = crate::db_guard::quote_pg_ident(table)?;
    let fn_ident = crate::db_guard::quote_pg_ident(&format!(
        "_uhoh_count_deletes_{}",
        blake3::hash(table.as_bytes()).to_hex()
    ))?;
    let trigger_ident = crate::db_guard::quote_pg_ident(&format!(
        "uhoh_delete_counter_{}",
        blake3::hash(format!("trigger:{table}").as_bytes()).to_hex()
    ))?;

    let install_sql = format!(
        "
        CREATE OR REPLACE FUNCTION {fn_ident}() RETURNS trigger AS $$
        BEGIN
            INSERT INTO _uhoh_delete_counts (table_name, txid, delete_count)
            VALUES ('{table_safe}', txid_current(), 1)
            ON CONFLICT (table_name, txid)
            DO UPDATE SET delete_count = _uhoh_delete_counts.delete_count + 1,
                          ts = now();
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS {trigger_ident} ON {table_quoted};
        CREATE TRIGGER {trigger_ident}
            BEFORE DELETE ON {table_quoted}
            FOR EACH ROW EXECUTE FUNCTION {fn_ident}();
        "
    );

    client
        .batch_execute(&install_sql)
        .await
        .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
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

    let mut workers = PG_DDL_POLL_WORKERS
        .lock()
        .map_err(|_| anyhow::anyhow!("Postgres DDL poll worker map lock poisoned"))?;

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
            DdlPollWorker {
                queue,
                cancel,
                task,
            },
        );
    }

    Ok(())
}

pub fn shutdown_all_listen_workers() {
    if let Ok(mut workers) = PG_DDL_POLL_WORKERS.lock() {
        for (_, worker) in workers.drain() {
            worker.cancel.cancel();
            worker.task.abort();
        }
    }
}

fn poll_delete_count(connection_ref: &str, window_seconds: i64, tables_csv: &str) -> Result<i64> {
    let tables_csv = tables_csv.to_string();
    let connection_ref = connection_ref.to_string();
    run_postgres_task(async move {
        let client = pg_connect_spawn(&connection_ref).await?;
        // Keep helper tables bounded to avoid unbounded growth in monitored DBs.
        let _ = client
            .execute(
                "DELETE FROM _uhoh_delete_counts
                 WHERE ts < now() - interval '24 hours'",
                &[],
            )
            .await;
        // Scope delete counts to the guard's configured table set.
        // Wildcard guards count all tables; specific table sets filter by table_name.
        let window_str = window_seconds.to_string();
        let row = if tables_csv.trim().is_empty() || tables_csv.trim() == "*" {
            client
                .query_one(
                    "SELECT COALESCE(SUM(delete_count), 0)
                     FROM _uhoh_delete_counts
                     WHERE ts > now() - ($1::text || ' seconds')::interval",
                    &[&window_str],
                )
                .await
        } else {
            let table_list: Vec<String> = tables_csv
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if table_list.is_empty() {
                return Ok(0);
            }
            let placeholders: Vec<String> = (2..=table_list.len() + 1)
                .map(|i| format!("${i}"))
                .collect();
            let sql = format!(
                "SELECT COALESCE(SUM(delete_count), 0) FROM _uhoh_delete_counts \
                 WHERE ts > now() - ($1::text || ' seconds')::interval \
                 AND table_name IN ({})",
                placeholders.join(", ")
            );
            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&window_str];
            for t in &table_list {
                params.push(t);
            }
            client.query_one(&sql, &params).await
        };
        let row = match row {
            Ok(r) => r,
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

fn poll_total_row_count(connection_ref: &str) -> Result<i64> {
    run_postgres_task(async move {
        let client = pg_connect_spawn(connection_ref).await?;

        // Use pg_class for row estimates (fast, no table scan).
        // information_schema.tables does not have TABLE_ROWS in PostgreSQL.
        let rows = client
            .query(
                "SELECT COALESCE(SUM(c.reltuples::bigint), 0)
                 FROM pg_class c
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relkind = 'r'
                   AND n.nspname = current_schema()",
                &[],
            )
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;

        let total = rows.first().map(|r| r.get::<_, i64>(0)).unwrap_or(0).max(0);
        Ok(total)
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
        let client = pg_connect_spawn(connection_ref).await?;

        // Keep helper table bounded to avoid unbounded growth in monitored DBs.
        let _ = client
            .execute(
                "DELETE FROM _uhoh_ddl_events
                 WHERE occurred_at < now() - interval '24 hours'",
                &[],
            )
            .await;
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

        let client = match pg_connect_spawn(&connection_ref).await {
            Ok(client) => client,
            Err(err) => {
                tracing::warn!(
                    "postgres DDL poll connect failed for {}: {}",
                    scrub_ref(&connection_ref),
                    err
                );
                if sleep_or_cancel(backoff, &shutdown).await {
                    return;
                }
                backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
                continue;
            }
        };

        backoff = std::time::Duration::from_secs(1);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
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
                                "postgres DDL poll failed for {}: {}",
                                scrub_ref(&connection_ref),
                                credentials::scrub_error_message(&err.to_string())
                            );
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
    let workers = PG_DDL_POLL_WORKERS
        .lock()
        .map_err(|_| anyhow::anyhow!("Postgres DDL poll worker map lock poisoned"))?;
    let Some(worker) = workers.get(connection_ref) else {
        return Ok(Vec::new());
    };
    let mut pending = worker
        .queue
        .lock()
        .map_err(|_| anyhow::anyhow!("Postgres DDL poll payload queue lock poisoned"))?;
    Ok(std::mem::take(&mut *pending))
}

async fn sleep_or_cancel(duration: std::time::Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

pub(crate) fn run_postgres_task<T, F>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    // Reuse a dedicated runtime for postgres task execution to avoid creating
    // a fresh Tokio runtime on every polling tick.
    PG_RUNTIME.block_on(fut)
}

/// Certificate verifier that accepts any server certificate (for sslmode=require).
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

pub(crate) fn native_rustls_connector_for_sslmode(sslmode: &str) -> Result<MakeRustlsConnect> {
    let config = match sslmode {
        "verify-ca" | "verify-full" => {
            let mut roots = RootCertStore::empty();
            let native = rustls_native_certs::load_native_certs();
            if !native.errors.is_empty() {
                let first = &native.errors[0];
                tracing::warn!("native certificate load issue: {first}");
            }
            for cert in native.certs {
                if let Err(err) = roots.add(cert) {
                    tracing::warn!("skipping invalid native certificate: {err}");
                }
            }
            if roots.is_empty() {
                anyhow::bail!("No trusted root certificates available for Postgres TLS connection")
            }
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        }
        _ => {
            // sslmode=require: encrypt but don't verify server certificate
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_no_client_auth()
        }
    };
    Ok(MakeRustlsConnect::new(config))
}

pub async fn pg_connect_spawn(connection_ref: &str) -> Result<tokio_postgres::Client> {
    if connection_requires_tls(connection_ref) {
        let sslmode = extract_sslmode(connection_ref).unwrap_or("require");
        let tls = native_rustls_connector_for_sslmode(sslmode)?;
        let (client, connection) = tokio_postgres::connect(connection_ref, tls)
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        return Ok(client);
    }

    let (client, connection) = tokio_postgres::connect(connection_ref, NoTls)
        .await
        .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

fn extract_sslmode(connection_ref: &str) -> Option<&'static str> {
    if let Ok(url) = url::Url::parse(connection_ref) {
        for (k, v) in url.query_pairs() {
            if k.eq_ignore_ascii_case("sslmode") {
                let mode = v.to_ascii_lowercase();
                return Some(match mode.as_str() {
                    "verify-ca" => "verify-ca",
                    "verify-full" => "verify-full",
                    _ => "require",
                });
            }
        }
    }
    for part in connection_ref.split_whitespace() {
        if let Some((k, v)) = part.split_once('=') {
            if k.eq_ignore_ascii_case("sslmode") {
                let mode = v.trim_matches('"').trim_matches('\'').to_ascii_lowercase();
                return Some(match mode.as_str() {
                    "verify-ca" => "verify-ca",
                    "verify-full" => "verify-full",
                    _ => "require",
                });
            }
        }
    }
    None
}

fn scrub_ref(connection_ref: &str) -> String {
    crate::db_guard::credentials::scrub_dsn(connection_ref)
}

pub(crate) fn connection_requires_tls(connection_ref: &str) -> bool {
    if let Ok(url) = url::Url::parse(connection_ref) {
        if matches!(url.scheme(), "postgres" | "postgresql") {
            for (k, v) in url.query_pairs() {
                if k.eq_ignore_ascii_case("sslmode") {
                    let mode = v.to_ascii_lowercase();
                    if mode == "require" || mode == "verify-ca" || mode == "verify-full" {
                        return true;
                    }
                }
            }
        }
    }

    // keyword-value DSN format, e.g. "host=... user=... sslmode=require"
    for part in connection_ref.split_whitespace() {
        if let Some((k, v)) = part.split_once('=') {
            if k.eq_ignore_ascii_case("sslmode") {
                let mode = v.trim_matches('"').trim_matches('\'').to_ascii_lowercase();
                if mode == "require" || mode == "verify-ca" || mode == "verify-full" {
                    return true;
                }
            }
        }
    }

    false
}
