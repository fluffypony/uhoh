mod commands;
mod credentials;
mod crypto_policy;
mod mysql;
mod postgres;
mod postgres_connection;
mod postgres_monitoring;
mod recovery;
mod sqlite;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::db::{DbGuardEngine, DbGuardEntry, DbGuardMode, LedgerSeverity, LedgerSource};
use crate::event_ledger::new_event;
use crate::subsystem::{DbGuardContext, Subsystem, SubsystemContext, SubsystemHealth};

pub use commands::handle_cli_action;

fn normalize_guard_mode(engine: DbGuardEngine, mode: DbGuardMode) -> DbGuardMode {
    match engine {
        DbGuardEngine::Postgres => match mode {
            DbGuardMode::SchemaPolling => DbGuardMode::SchemaPolling,
            DbGuardMode::Triggers => DbGuardMode::Triggers,
        },
        DbGuardEngine::Mysql => DbGuardMode::SchemaPolling,
        DbGuardEngine::Sqlite => mode,
    }
}

pub fn quote_pg_ident(input: &str) -> Result<String> {
    if input.trim().is_empty() {
        anyhow::bail!("Postgres identifier cannot be empty");
    }
    if input.contains('\0') {
        anyhow::bail!("Postgres identifier contains NUL byte");
    }
    let mut quoted_parts = Vec::new();
    for part in input.split('.') {
        if part.is_empty() {
            anyhow::bail!("Postgres identifier segment cannot be empty");
        }
        quoted_parts.push(format!("\"{}\"", part.replace('"', "\"\"")));
    }
    Ok(quoted_parts.join("."))
}

const GUARD_TICK_INTERVAL_SECS: i64 = 30;

pub struct DbGuardSubsystem {
    healthy: bool,
    last_failure: Option<String>,
    sqlite_versions: HashMap<String, i64>,
    mysql_states: HashMap<String, mysql::MysqlGuardState>,
    postgres_runtime: Arc<postgres::PostgresGuardRuntime>,
    shutdown: Option<CancellationToken>,
}

impl DbGuardSubsystem {
    pub fn new() -> Self {
        Self {
            healthy: true,
            last_failure: None,
            sqlite_versions: HashMap::new(),
            mysql_states: HashMap::new(),
            postgres_runtime: Arc::new(postgres::PostgresGuardRuntime::new()),
            shutdown: None,
        }
    }
}

impl Default for DbGuardSubsystem {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Subsystem for DbGuardSubsystem {
    fn name(&self) -> &str {
        "db_guard"
    }

    async fn run(&mut self, shutdown: CancellationToken, ctx: SubsystemContext) -> Result<()> {
        self.shutdown = Some(shutdown.clone());
        if !ctx.config.db_guard.enabled {
            tracing::info!("db_guard disabled by config");
            shutdown.cancelled().await;
            return Ok(());
        }

        let mut last_guard_names: Vec<String> = Vec::new();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(GUARD_TICK_INTERVAL_SECS as u64)) => {
                    // Re-read guards each tick so `uhoh db add` takes effect without restart
                    let guards = match ctx.database.list_db_guards() {
                        Ok(g) => g,
                        Err(err) => {
                            tracing::error!("failed to list db guards: {err}");
                            continue;
                        }
                    };

                    // Log newly registered guards
                    for guard in &guards {
                        if !last_guard_names.contains(&guard.name) {
                            let mut event = new_event(
                                LedgerSource::DbGuard,
                                "guard_started",
                                LedgerSeverity::Info,
                            );
                            event.guard_name = Some(guard.name.clone());
                            event.detail = Some(format!("engine={}, mode={}", guard.engine, guard.mode));
                            if let Err(err) = ctx.event_ledger.append(event) {
                                tracing::error!("failed to append guard_started event: {err}");
                            }
                        }
                    }
                    last_guard_names = guards.iter().map(|g| g.name.clone()).collect();

                    let ctx_cl = ctx.clone();
                    let guards_cl = guards.clone();
                    let sqlite_versions = std::mem::take(&mut self.sqlite_versions);
                    let mysql_states = std::mem::take(&mut self.mysql_states);
                    let postgres_runtime = Arc::clone(&self.postgres_runtime);
                    let shutdown = self.shutdown.clone();
                    let tick_result = tokio::task::spawn_blocking(move || {
                        let mut worker = DbGuardSubsystem {
                            healthy: true,
                            last_failure: None,
                            sqlite_versions,
                            mysql_states,
                            postgres_runtime,
                            shutdown,
                        };
                        let result = worker.tick_guards(&ctx_cl, &guards_cl);
                        (
                            result,
                            worker.healthy,
                            worker.last_failure,
                            worker.sqlite_versions,
                            worker.mysql_states,
                        )
                    })
                    .await;

                    match tick_result {
                        Ok((result, healthy, last_failure, sqlite_versions, mysql_states)) => {
                            self.healthy = healthy;
                            self.last_failure = last_failure;
                            self.sqlite_versions = sqlite_versions;
                            self.mysql_states = mysql_states;
                            match result {
                                Ok(()) => {}
                                Err(err) => {
                                    self.healthy = false;
                                    self.last_failure = Some(format!("db guard tick failed: {err}"));
                                    return Err(err);
                                }
                            }
                        }
                        Err(err) => {
                            tracing::error!("db_guard tick worker panicked: {err}");
                            self.healthy = false;
                            self.last_failure = Some(format!("db guard tick worker panicked: {err}"));
                        }
                    }
                }
            }
        }
        self.postgres_runtime.shutdown_all_listen_workers();
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.postgres_runtime.shutdown_all_listen_workers();
        Ok(())
    }

    fn health_check(&self) -> SubsystemHealth {
        if self.healthy {
            SubsystemHealth::Healthy
        } else {
            SubsystemHealth::Degraded(
                self.last_failure
                    .clone()
                    .unwrap_or_else(|| "db guard reported failures".to_string()),
            )
        }
    }
}

impl DbGuardSubsystem {
    fn tick_guards(&mut self, ctx: &DbGuardContext, guards: &[DbGuardEntry]) -> Result<()> {
        self.healthy = true;
        self.last_failure = None;
        if let Some(token) = self.shutdown.as_ref() {
            postgres::reconcile_listen_workers(&self.postgres_runtime, guards, token)?;
        }

        for guard in guards {
            let effective_mode = normalize_guard_mode(guard.engine, guard.mode);
            if let Err(err) = self.tick_guard_engine(ctx, guard, effective_mode) {
                self.record_guard_tick_failure(ctx, guard, &err);
                continue;
            }

            // Global retention cleanup pass per guard.
            let guard_base = ctx.uhoh_dir.join("db_guard").join(&guard.name);
            let baseline = guard_base.join("baselines");
            let recovery_dir = guard_base.join("recovery");
            if baseline.exists() {
                if let Err(err) = recovery::cleanup_retention(
                    &baseline,
                    ctx.config.db_guard.recovery_retention_days,
                ) {
                    tracing::warn!(
                        "db_guard baseline retention cleanup failed for {}: {}",
                        guard.name,
                        err
                    );
                }
            }
            if recovery_dir.exists() {
                if let Err(err) = recovery::cleanup_retention(
                    &recovery_dir,
                    ctx.config.db_guard.recovery_retention_days,
                ) {
                    tracing::warn!(
                        "db_guard recovery retention cleanup failed for {}: {}",
                        guard.name,
                        err
                    );
                }
            }

            if guard.mode != effective_mode {
                let mut event = new_event(
                    LedgerSource::DbGuard,
                    "guard_mode_normalized",
                    LedgerSeverity::Info,
                );
                event.guard_name = Some(guard.name.clone());
                event.detail = Some(format!(
                    "engine={}, configured_mode={}, effective_mode={}",
                    guard.engine, guard.mode, effective_mode
                ));
                if let Err(err) = ctx.event_ledger.append(event) {
                    tracing::error!("failed to append guard_mode_normalized event: {err}");
                }
            }
        }
        Ok(())
    }

    fn tick_guard_engine(
        &mut self,
        ctx: &DbGuardContext,
        guard: &DbGuardEntry,
        effective_mode: DbGuardMode,
    ) -> Result<()> {
        tracing::trace!("db_guard tick via {} engine", guard.engine);
        match guard.engine {
            DbGuardEngine::Sqlite => {
                sqlite::tick_sqlite_guard(ctx, guard, &mut self.sqlite_versions)
            }
            DbGuardEngine::Postgres => postgres::tick_postgres_guard(
                &self.postgres_runtime,
                ctx,
                guard,
                effective_mode,
                GUARD_TICK_INTERVAL_SECS,
            ),
            DbGuardEngine::Mysql => {
                let state = self.mysql_states.entry(guard.name.clone()).or_default();
                mysql::tick_mysql_guard(ctx, guard, state)
            }
        }
    }

    fn record_guard_tick_failure(
        &mut self,
        ctx: &DbGuardContext,
        guard: &DbGuardEntry,
        err: &anyhow::Error,
    ) {
        tracing::warn!("db_guard tick failed for {}: {}", guard.name, err);
        self.healthy = false;
        if self.last_failure.is_none() {
            self.last_failure = Some(format!("db guard tick failed for {}: {}", guard.name, err));
        }

        let mut event = new_event(
            LedgerSource::DbGuard,
            "guard_tick_failed",
            LedgerSeverity::Warn,
        );
        event.guard_name = Some(guard.name.clone());
        event.detail = Some(err.to_string());
        if let Err(append_err) = ctx.event_ledger.append(event) {
            tracing::error!("failed to append guard_tick_failed event: {append_err}");
        }
    }
}

pub fn derive_guard_name_from_dsn(dsn: &str) -> String {
    let stripped = dsn
        .replace("postgres://", "")
        .replace("mysql://", "")
        .replace("sqlite://", "")
        .replace(['/', ':'], "-");
    stripped.chars().take(64).collect()
}

pub fn detect_engine(dsn: &str) -> Option<DbGuardEngine> {
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        Some(DbGuardEngine::Postgres)
    } else if dsn.starts_with("mysql://") {
        Some(DbGuardEngine::Mysql)
    } else if dsn.starts_with("sqlite://") {
        Some(DbGuardEngine::Sqlite)
    } else {
        None
    }
}
