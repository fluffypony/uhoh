use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use super::postgres_connection::{connect_postgres_client, ResolvedPostgresConnection};
use super::{credentials, recovery};
use crate::db::{DbGuardEntry, DbGuardMode, LedgerSeverity, LedgerSource};
use crate::event_ledger::new_event;
use crate::subsystem::DbGuardContext;

pub(crate) struct PostgresGuardRuntime {
    task_runtime: Mutex<tokio::runtime::Runtime>,
    ddl_cursor: Arc<Mutex<HashMap<String, i64>>>,
    ddl_poll_workers: Mutex<HashMap<String, DdlPollWorker>>,
}

struct DdlPollWorker {
    queue: std::sync::Arc<Mutex<Vec<String>>>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl PostgresGuardRuntime {
    pub(crate) fn new() -> Self {
        Self {
            task_runtime: Mutex::new(
                build_postgres_task_runtime().expect("failed to build postgres task runtime"),
            ),
            ddl_cursor: Arc::new(Mutex::new(HashMap::new())),
            ddl_poll_workers: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn shutdown_all_listen_workers(&self) {
        if let Ok(mut workers) = self.ddl_poll_workers.lock() {
            for (_, worker) in workers.drain() {
                worker.cancel.cancel();
                worker.task.abort();
            }
        }
    }

    fn run_task<T, F>(&self, fut: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        let runtime = self
            .task_runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("Postgres task runtime lock poisoned"))?;
        runtime.block_on(fut)
    }
}

fn build_postgres_task_runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| anyhow::anyhow!("failed to build postgres task runtime: {err}"))
}

fn emit_recovery_event(
    ctx: &DbGuardContext,
    guard_name: &str,
    event_type: &str,
    detail: serde_json::Value,
    artifact: &recovery::ArtifactInfo,
) {
    let mut event = new_event(LedgerSource::DbGuard, event_type, LedgerSeverity::Critical);
    event.guard_name = Some(guard_name.to_string());
    event.detail = Some(detail.to_string());
    event.pre_state_ref = Some(artifact.blake3.clone());
    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append {event_type} event: {err}");
    }
}

fn emit_degraded_event(ctx: &DbGuardContext, guard_name: &str, step: &str, err: &anyhow::Error) {
    let mut event = new_event(
        LedgerSource::DbGuard,
        "postgres_optional_step_degraded",
        LedgerSeverity::Warn,
    );
    event.guard_name = Some(guard_name.to_string());
    event.detail = Some(
        serde_json::json!({
            "step": step,
            "error": err.to_string(),
        })
        .to_string(),
    );
    if let Err(append_err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append postgres_optional_step_degraded event: {append_err}");
    }
}

pub(crate) fn tick_postgres_guard(
    runtime: &PostgresGuardRuntime,
    ctx: &DbGuardContext,
    guard: &DbGuardEntry,
    effective_mode: DbGuardMode,
    tick_interval_secs: i64,
) -> Result<()> {
    let connection = ResolvedPostgresConnection::resolve(&guard.connection_ref)?;
    if guard.tables_csv.trim() == "*" {
        if let Err(err) = reconcile_wildcard_delete_triggers(runtime, ctx, guard, &connection) {
            tracing::warn!(
                "postgres wildcard trigger reconcile failed for {}: {}",
                guard.name,
                err
            );
            emit_degraded_event(ctx, &guard.name, "wildcard_trigger_reconcile", &err);
        }
    }

    ensure_postgres_baseline(ctx, guard, &connection)?;

    // Respect configured guard mode: in schema_polling mode we intentionally skip
    // row-level trigger counters and only rely on schema/DDL signal paths.
    if effective_mode == DbGuardMode::SchemaPolling {
        emit_postgres_tick_event(ctx, guard, &connection, true);
        return Ok(());
    }

    run_delete_counter_checks(runtime, ctx, guard, &connection, tick_interval_secs)?;
    process_ddl_recovery(runtime, ctx, guard, &connection)?;
    emit_postgres_tick_event(ctx, guard, &connection, false);
    Ok(())
}

fn ensure_postgres_baseline(
    ctx: &DbGuardContext,
    guard: &DbGuardEntry,
    connection: &ResolvedPostgresConnection,
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

    if !needs_baseline {
        return Ok(());
    }

    let creds = credentials::resolve_postgres_credentials(&guard.connection_ref)?;
    let info = recovery::write_postgres_schema_baseline(
        &ctx.uhoh_dir,
        &guard.name,
        connection,
        &creds,
        ctx.config.db_guard.recovery_retention_days,
        ctx.config.db_guard.max_baseline_size_mb,
    )
    .map_err(|err| {
        anyhow::anyhow!(
            "postgres baseline generation failed for {}: {}",
            guard.name,
            err
        )
    })?;

    let ts = chrono::Utc::now().to_rfc3339();
    if let Err(err) = ctx.database.set_db_guard_baseline_time(&guard.name, &ts) {
        tracing::warn!(
            "failed to persist baseline timestamp for guard {}: {}",
            guard.name,
            err
        );
    }
    let mut event = new_event(
        LedgerSource::DbGuard,
        "postgres_baseline",
        LedgerSeverity::Info,
    );
    event.guard_name = Some(guard.name.clone());
    event.detail = Some(format!("artifact={}, blake3={}", info.path, info.blake3));
    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append postgres_baseline event: {err}");
    }
    Ok(())
}

fn run_delete_counter_checks(
    runtime: &PostgresGuardRuntime,
    ctx: &DbGuardContext,
    guard: &DbGuardEntry,
    connection: &ResolvedPostgresConnection,
    tick_interval_secs: i64,
) -> Result<()> {
    let poll_window_secs = tick_interval_secs.saturating_mul(2).max(1);
    let deleted_rows = poll_delete_count(runtime, connection, poll_window_secs, &guard.tables_csv)
        .map_err(|err| {
            anyhow::anyhow!(
                "postgres delete count poll failed for {}: {}",
                guard.name,
                err
            )
        })?;
    let total_rows = match poll_total_row_count(runtime, connection) {
        Ok(total) => Some(total),
        Err(err) => {
            tracing::warn!(
                "postgres total row count poll failed for {}: {}",
                guard.name,
                err
            );
            emit_degraded_event(ctx, &guard.name, "total_row_count_poll", &err);
            None
        }
    };
    let pct = if total_rows.unwrap_or(0) > 0 {
        deleted_rows as f64 / total_rows.unwrap_or(0) as f64
    } else {
        0.0
    };

    if deleted_rows >= ctx.config.db_guard.mass_delete_row_threshold as i64 {
        write_recovery_artifact(
            ctx,
            guard,
            connection,
            "mass_delete",
            serde_json::json!({
                "deleted_rows": deleted_rows,
            }),
            "postgres mass-delete recovery write failed",
        )?;
    } else if pct >= ctx.config.db_guard.mass_delete_pct_threshold {
        write_recovery_artifact(
            ctx,
            guard,
            connection,
            "mass_delete_pct",
            serde_json::json!({
                "deleted_rows": deleted_rows,
                "total_rows": total_rows,
                "deleted_ratio": pct,
            }),
            "postgres mass-delete-pct recovery write failed",
        )?;
    }

    Ok(())
}

fn process_ddl_recovery(
    runtime: &PostgresGuardRuntime,
    ctx: &DbGuardContext,
    guard: &DbGuardEntry,
    connection: &ResolvedPostgresConnection,
) -> Result<()> {
    let listen_payloads = drain_listen_payloads(runtime, connection)?;
    let ddl_payloads = match poll_ddl_events(runtime, connection, 64) {
        Ok(payloads) => payloads,
        Err(err) => {
            emit_degraded_event(ctx, &guard.name, "ddl_event_poll", &err);
            Vec::new()
        }
    };

    for payload in listen_payloads.into_iter().chain(ddl_payloads) {
        write_recovery_artifact(
            ctx,
            guard,
            connection,
            "ddl",
            serde_json::json!({
                "notify_payload": payload,
            }),
            "postgres DDL recovery write failed",
        )?;
    }

    Ok(())
}

fn write_recovery_artifact(
    ctx: &DbGuardContext,
    guard: &DbGuardEntry,
    connection: &ResolvedPostgresConnection,
    label: &str,
    detail: serde_json::Value,
    error_prefix: &str,
) -> Result<()> {
    let creds = credentials::resolve_postgres_credentials(&guard.connection_ref)?;
    let artifact = recovery::write_postgres_schema_recovery(
        &ctx.uhoh_dir,
        &guard.name,
        connection,
        &creds,
        label,
        ctx.config.db_guard.encrypt_recovery,
        ctx.config.db_guard.recovery_retention_days,
        ctx.config.db_guard.max_recovery_file_mb,
    )
    .map_err(|err| anyhow::anyhow!("{error_prefix} for {}: {}", guard.name, err))?;
    let mut event_detail = detail;
    if let Some(map) = event_detail.as_object_mut() {
        map.insert(
            "artifact".to_string(),
            serde_json::json!(artifact.path.clone()),
        );
        map.insert(
            "blake3".to_string(),
            serde_json::json!(artifact.blake3.clone()),
        );
    }
    emit_recovery_event(
        ctx,
        &guard.name,
        if label == "ddl" { "drop_table" } else { label },
        event_detail,
        &artifact,
    );
    Ok(())
}

fn emit_postgres_tick_event(
    ctx: &DbGuardContext,
    guard: &DbGuardEntry,
    connection: &ResolvedPostgresConnection,
    row_counters_disabled: bool,
) {
    let mut event = new_event(LedgerSource::DbGuard, "postgres_tick", LedgerSeverity::Info);
    event.guard_name = Some(guard.name.clone());
    event.detail = Some(if row_counters_disabled {
        format!(
            "mode={}, dsn_ref={}, row_counters=disabled",
            guard.mode,
            connection.scrubbed_ref()
        )
    } else {
        format!("mode={}, dsn_ref={}", guard.mode, connection.scrubbed_ref())
    });
    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append postgres_tick event: {err}");
    }
}

fn reconcile_wildcard_delete_triggers(
    runtime: &PostgresGuardRuntime,
    ctx: &DbGuardContext,
    guard: &DbGuardEntry,
    connection: &ResolvedPostgresConnection,
) -> Result<()> {
    let current = fetch_current_schema_tables(runtime, connection)?;
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
        install_delete_counter_triggers_for_tables(runtime, connection, &added)?;
        let mut event = new_event(
            LedgerSource::DbGuard,
            "postgres_wildcard_trigger_reconciled",
            LedgerSeverity::Info,
        );
        event.guard_name = Some(guard.name.clone());
        event.detail = Some(
            serde_json::json!({
                "added_tables": added,
                "guard": guard.name,
            })
            .to_string(),
        );
        if let Err(err) = ctx.event_ledger.append(event) {
            tracing::error!("failed to append postgres_wildcard_trigger_reconciled event: {err}");
        }
    }

    let cache = current_sorted.join(",");
    if let Err(err) = ctx
        .database
        .set_db_guard_watched_tables_cache(&guard.name, Some(&cache))
    {
        tracing::warn!(
            "failed to persist postgres watched table cache for {}: {}",
            guard.name,
            err
        );
    }
    Ok(())
}

fn fetch_current_schema_tables(
    runtime: &PostgresGuardRuntime,
    connection: &ResolvedPostgresConnection,
) -> Result<Vec<String>> {
    let connection = connection.clone();
    runtime.run_task(async move {
        let client = connect_postgres_client(&connection).await?;
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
    runtime: &PostgresGuardRuntime,
    connection: &ResolvedPostgresConnection,
    tables: &[String],
) -> Result<()> {
    if tables.is_empty() {
        return Ok(());
    }
    let connection = connection.clone();
    let tables = tables.to_vec();
    runtime.run_task(async move {
        let client = connect_postgres_client(&connection).await?;
        for table in tables {
            install_delete_counter_trigger_sql(&client, &table).await?;
        }
        Ok(())
    })
}

pub(super) async fn install_delete_counter_trigger_sql(
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

pub(crate) fn reconcile_listen_workers(
    runtime: &PostgresGuardRuntime,
    guards: &[DbGuardEntry],
    shutdown: &CancellationToken,
) -> Result<()> {
    let required: HashSet<String> = guards
        .iter()
        .filter(|g| g.engine == crate::db::DbGuardEngine::Postgres)
        .map(|g| ResolvedPostgresConnection::resolve(&g.connection_ref))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|connection| connection.connect_dsn().to_string())
        .collect();

    let mut workers = runtime
        .ddl_poll_workers
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
        let ddl_cursor = Arc::clone(&runtime.ddl_cursor);
        let task = tokio::spawn(async move {
            run_listen_worker(dsn_cl, queue_cl, cancel_cl, ddl_cursor).await;
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

fn poll_delete_count(
    runtime: &PostgresGuardRuntime,
    connection: &ResolvedPostgresConnection,
    window_seconds: i64,
    tables_csv: &str,
) -> Result<i64> {
    let tables_csv = tables_csv.to_string();
    let connection = connection.clone();
    runtime.run_task(async move {
        let client = connect_postgres_client(&connection).await?;
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

fn poll_total_row_count(
    runtime: &PostgresGuardRuntime,
    connection: &ResolvedPostgresConnection,
) -> Result<i64> {
    let connection = connection.clone();
    runtime.run_task(async move {
        let client = connect_postgres_client(&connection).await?;

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

fn poll_ddl_events(
    runtime: &PostgresGuardRuntime,
    connection: &ResolvedPostgresConnection,
    max_rows: i64,
) -> Result<Vec<String>> {
    let connection_key = connection.connect_dsn().to_string();
    let last_seen_id = {
        let cache = runtime
            .ddl_cursor
            .lock()
            .map_err(|_| anyhow::anyhow!("Postgres DDL cursor lock poisoned"))?;
        cache.get(&connection_key).copied().unwrap_or(0)
    };

    let connection = connection.clone();
    let result = runtime.run_task(async move {
        let client = connect_postgres_client(&connection).await?;

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
        let mut cache = runtime
            .ddl_cursor
            .lock()
            .map_err(|_| anyhow::anyhow!("Postgres DDL cursor lock poisoned"))?;
        cache.insert(connection_key, id);
        Ok(rows.into_iter().map(|(_, payload)| payload).collect())
    } else {
        Ok(Vec::new())
    }
}

async fn run_listen_worker(
    connection_ref: String,
    queue: std::sync::Arc<Mutex<Vec<String>>>,
    shutdown: CancellationToken,
    ddl_cursor: Arc<Mutex<HashMap<String, i64>>>,
) {
    let mut backoff = std::time::Duration::from_secs(1);
    let max_backoff = std::time::Duration::from_secs(30);
    let mut last_seen_id = match ddl_cursor.lock() {
        Ok(cache) => cache.get(&connection_ref).copied().unwrap_or(0),
        Err(_) => 0,
    };

    loop {
        if shutdown.is_cancelled() {
            return;
        }

        let connection = match ResolvedPostgresConnection::resolve(&connection_ref) {
            Ok(connection) => connection,
            Err(err) => {
                tracing::warn!(
                    "postgres DDL poll connection setup failed for {}: {}",
                    connection_ref,
                    err
                );
                if sleep_or_cancel(backoff, &shutdown).await {
                    return;
                }
                backoff = std::cmp::min(backoff.saturating_mul(2), max_backoff);
                continue;
            }
        };

        let client = match connect_postgres_client(&connection).await {
            Ok(client) => client,
            Err(err) => {
                tracing::warn!(
                    "postgres DDL poll connect failed for {}: {}",
                    connection.scrubbed_ref(),
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
                    match poll_and_enqueue(
                        &client,
                        &mut last_seen_id,
                        &connection_ref,
                        &connection,
                        &ddl_cursor,
                        &queue,
                    ).await {
                        Ok(()) => {}
                        Err(_) => break, // reconnect on poll failure
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

/// Execute a single poll cycle: query new DDL events, update the cursor, and enqueue payloads.
/// Returns Err on query failure to signal the caller to reconnect.
async fn poll_and_enqueue(
    client: &tokio_postgres::Client,
    last_seen_id: &mut i64,
    connection_ref: &str,
    connection: &ResolvedPostgresConnection,
    ddl_cursor: &Arc<Mutex<HashMap<String, i64>>>,
    queue: &std::sync::Arc<Mutex<Vec<String>>>,
) -> Result<()> {
    let rows = client
        .query(
            "SELECT id, payload::text
             FROM _uhoh_ddl_events
             WHERE id > $1
             ORDER BY id ASC
             LIMIT 64",
            &[last_seen_id],
        )
        .await
        .map_err(|err| {
            tracing::warn!(
                "postgres DDL poll failed for {}: {}",
                connection.scrubbed_ref(),
                credentials::scrub_error_message(&err.to_string())
            );
            anyhow::anyhow!("poll failed")
        })?;

    if rows.is_empty() {
        return Ok(());
    }

    let mut fresh = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.get(0);
        let payload: String = row.get(1);
        *last_seen_id = (*last_seen_id).max(id);
        fresh.push(payload);
    }

    if let Ok(mut cache) = ddl_cursor.lock() {
        cache.insert(connection_ref.to_string(), *last_seen_id);
    }
    if let Ok(mut pending) = queue.lock() {
        pending.extend(fresh);
        // Cap queue to prevent unbounded memory growth
        if pending.len() > 1024 {
            let drain = pending.len().saturating_sub(1024);
            pending.drain(0..drain);
        }
    }

    Ok(())
}

fn drain_listen_payloads(
    runtime: &PostgresGuardRuntime,
    connection: &ResolvedPostgresConnection,
) -> Result<Vec<String>> {
    let workers = runtime
        .ddl_poll_workers
        .lock()
        .map_err(|_| anyhow::anyhow!("Postgres DDL poll worker map lock poisoned"))?;
    let Some(worker) = workers.get(connection.connect_dsn()) else {
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

pub(super) fn parse_watched_tables(tables_csv: &str) -> Vec<String> {
    if tables_csv.trim() == "*" {
        return Vec::new();
    }
    tables_csv
        .split(',')
        .map(str::trim)
        .filter(|table| !table.is_empty())
        .map(|table| table.to_string())
        .collect()
}

pub(crate) fn run_postgres_task<T, F>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    build_postgres_task_runtime()?.block_on(fut)
}

#[cfg(test)]
mod tests {
    use super::{
        drain_listen_payloads, parse_watched_tables, reconcile_listen_workers, run_postgres_task,
        PostgresGuardRuntime,
    };
    use crate::db::{DbGuardEngine, DbGuardEntry, DbGuardMode};
    use crate::db_guard::postgres_connection::ResolvedPostgresConnection;
    use tokio_util::sync::CancellationToken;

    fn guard(name: &str, engine: DbGuardEngine, connection_ref: &str) -> DbGuardEntry {
        DbGuardEntry {
            id: 1,
            name: name.to_string(),
            engine,
            connection_ref: connection_ref.to_string(),
            tables_csv: "*".to_string(),
            watched_tables_cache: None,
            mode: DbGuardMode::Triggers,
            created_at: "2026-03-12T00:00:00Z".to_string(),
            last_baseline_at: None,
            active: true,
        }
    }

    #[test]
    fn parse_watched_tables_trims_entries_and_treats_wildcard_as_all_tables() {
        assert!(parse_watched_tables("*").is_empty());
        assert_eq!(
            parse_watched_tables(" users , orders ,, audit_log "),
            vec!["users", "orders", "audit_log"]
        );
    }

    #[test]
    fn run_postgres_task_executes_future_without_nested_runtime_helpers() {
        let value = run_postgres_task(async { Ok::<_, anyhow::Error>(42) }).expect("task result");
        assert_eq!(value, 42);
    }

    #[test]
    fn reconcile_listen_workers_adds_reuses_and_removes_postgres_workers() {
        let runtime = PostgresGuardRuntime::new();
        let shutdown = CancellationToken::new();
        let postgres_guard = guard("pg", DbGuardEngine::Postgres, "host=localhost dbname=uhoh");
        let connection = ResolvedPostgresConnection::resolve(&postgres_guard.connection_ref)
            .expect("resolve postgres connection");
        let worker_key = connection.connect_dsn().to_string();
        let tokio_runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

        tokio_runtime.block_on(async {
            reconcile_listen_workers(&runtime, std::slice::from_ref(&postgres_guard), &shutdown)
                .expect("start workers");
            assert_eq!(runtime.ddl_poll_workers.lock().unwrap().len(), 1);

            reconcile_listen_workers(&runtime, std::slice::from_ref(&postgres_guard), &shutdown)
                .expect("reuse worker");
            assert_eq!(runtime.ddl_poll_workers.lock().unwrap().len(), 1);

            {
                let workers = runtime.ddl_poll_workers.lock().unwrap();
                let worker = workers.get(&worker_key).expect("worker present");
                let mut queue = worker.queue.lock().unwrap();
                queue.push("payload-a".to_string());
                queue.push("payload-b".to_string());
            }
            assert_eq!(
                drain_listen_payloads(&runtime, &connection).expect("drain queue"),
                vec!["payload-a".to_string(), "payload-b".to_string()]
            );
            assert!(drain_listen_payloads(&runtime, &connection)
                .expect("drain empty queue")
                .is_empty());

            reconcile_listen_workers(&runtime, &[], &shutdown).expect("remove workers");
            assert!(runtime.ddl_poll_workers.lock().unwrap().is_empty());
        });

        shutdown.cancel();
        runtime.shutdown_all_listen_workers();
    }
}
