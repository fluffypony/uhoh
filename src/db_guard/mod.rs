mod credentials;
mod mysql;
mod postgres;
mod recovery;
mod sqlite;
use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::db::DbGuardEntry;
use crate::event_ledger::new_event;
use crate::subsystem::{Subsystem, SubsystemContext, SubsystemHealth};

pub use credentials::ensure_guard_dir;
pub use credentials::resolve_postgres_credentials;
pub use credentials::resolve_postgres_credentials_cli;
pub use credentials::resolve_stored_credentials;
pub use credentials::scrub_dsn;
pub use credentials::scrub_error_message;
pub use credentials::store_encrypted_credential;
pub use credentials::store_postgres_credentials_cli;
pub use credentials::CredentialMaterial;
pub use postgres::{build_connect_dsn, pg_connect_spawn};
pub use recovery::decrypt_recovery_payload;
pub use recovery::write_postgres_schema_baseline;
pub use recovery::write_sqlite_baseline;

fn normalize_guard_mode(engine: &str, mode: &str) -> String {
    let mode_l = mode.trim().to_ascii_lowercase();
    if engine == "postgres" {
        if mode_l == "schema_polling" {
            "schema_polling".to_string()
        } else {
            "triggers".to_string()
        }
    } else if engine == "mysql" {
        "schema_polling".to_string()
    } else {
        mode_l
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

trait DbGuardEngine {
    fn tick(
        &self,
        subsystem: &mut DbGuardSubsystem,
        ctx: &SubsystemContext,
        guard: &DbGuardEntry,
    ) -> Result<()>;
}

struct SqliteEngine;
struct PostgresEngine;
struct MysqlEngine;

impl DbGuardEngine for SqliteEngine {
    fn tick(
        &self,
        subsystem: &mut DbGuardSubsystem,
        ctx: &SubsystemContext,
        guard: &DbGuardEntry,
    ) -> Result<()> {
        sqlite::tick_sqlite_guard(ctx, guard, &mut subsystem.sqlite_versions)
    }
}

impl DbGuardEngine for PostgresEngine {
    fn tick(
        &self,
        _subsystem: &mut DbGuardSubsystem,
        ctx: &SubsystemContext,
        guard: &DbGuardEntry,
    ) -> Result<()> {
        postgres::tick_postgres_guard(ctx, guard, GUARD_TICK_INTERVAL_SECS)
    }
}

impl DbGuardEngine for MysqlEngine {
    fn tick(
        &self,
        subsystem: &mut DbGuardSubsystem,
        ctx: &SubsystemContext,
        guard: &DbGuardEntry,
    ) -> Result<()> {
        let state = subsystem.mysql_states.entry(guard.name.clone()).or_default();
        mysql::tick_mysql_guard(ctx, guard, state)
    }
}

static SQLITE_ENGINE: SqliteEngine = SqliteEngine;
static POSTGRES_ENGINE: PostgresEngine = PostgresEngine;
static MYSQL_ENGINE: MysqlEngine = MysqlEngine;

fn resolve_guard_engine(engine: &str) -> Option<&'static dyn DbGuardEngine> {
    match engine {
        "sqlite" => Some(&SQLITE_ENGINE),
        "postgres" => Some(&POSTGRES_ENGINE),
        "mysql" => Some(&MYSQL_ENGINE),
        _ => None,
    }
}

pub struct DbGuardSubsystem {
    healthy: bool,
    last_failure: Option<String>,
    sqlite_versions: HashMap<String, i64>,
    mysql_states: HashMap<String, mysql::MysqlGuardState>,
    shutdown: Option<CancellationToken>,
}

impl DbGuardSubsystem {
    pub fn new() -> Self {
        Self {
            healthy: true,
            last_failure: None,
            sqlite_versions: HashMap::new(),
            mysql_states: HashMap::new(),
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
                            let mut event = new_event("db_guard", "guard_started", "info");
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
                    let shutdown = self.shutdown.clone();
                    let tick_result = tokio::task::spawn_blocking(move || {
                        let mut worker = DbGuardSubsystem {
                            healthy: true,
                            last_failure: None,
                            sqlite_versions,
                            mysql_states,
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
        postgres::shutdown_all_listen_workers();
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        postgres::shutdown_all_listen_workers();
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
    fn tick_guards(&mut self, ctx: &SubsystemContext, guards: &[DbGuardEntry]) -> Result<()> {
        self.healthy = true;
        self.last_failure = None;
        if let Some(token) = self.shutdown.as_ref() {
            postgres::reconcile_listen_workers(guards, token)?;
        }

        for guard in guards {
            let effective_mode = normalize_guard_mode(guard.engine.as_str(), guard.mode.as_str());
            match guard.engine.as_str() {
                "sqlite" | "postgres" | "mysql" => {
                    if let Err(err) = self.tick_guard_engine(ctx, guard) {
                        self.record_guard_tick_failure(ctx, guard, &err);
                        continue;
                    }
                }
                _ => {
                    self.record_unknown_guard_engine(ctx, guard);
                    continue;
                }
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

            let configured_mode = guard.mode.trim().to_ascii_lowercase();
            if configured_mode != effective_mode {
                let mut event = new_event("db_guard", "guard_mode_normalized", "info");
                event.guard_name = Some(guard.name.clone());
                event.detail = Some(format!(
                    "engine={}, configured_mode={}, effective_mode={}",
                    guard.engine, configured_mode, effective_mode
                ));
                if let Err(err) = ctx.event_ledger.append(event) {
                    tracing::error!("failed to append guard_mode_normalized event: {err}");
                }
            }
        }
        Ok(())
    }

    fn tick_guard_engine(&mut self, ctx: &SubsystemContext, guard: &DbGuardEntry) -> Result<()> {
        tracing::trace!("db_guard tick via {} engine", guard.engine);
        resolve_guard_engine(guard.engine.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "unsupported db guard engine '{}' for guard {}",
                    guard.engine,
                    guard.name
                )
            })?
            .tick(self, ctx, guard)
    }

    fn record_guard_tick_failure(
        &mut self,
        ctx: &SubsystemContext,
        guard: &DbGuardEntry,
        err: &anyhow::Error,
    ) {
        tracing::warn!("db_guard tick failed for {}: {}", guard.name, err);
        self.healthy = false;
        if self.last_failure.is_none() {
            self.last_failure = Some(format!("db guard tick failed for {}: {}", guard.name, err));
        }

        let mut event = new_event("db_guard", "guard_tick_failed", "warn");
        event.guard_name = Some(guard.name.clone());
        event.detail = Some(err.to_string());
        if let Err(append_err) = ctx.event_ledger.append(event) {
            tracing::error!("failed to append guard_tick_failed event: {append_err}");
        }
    }

    fn record_unknown_guard_engine(&mut self, ctx: &SubsystemContext, guard: &DbGuardEntry) {
        tracing::warn!(
            "Skipping db_guard '{}' with unsupported engine '{}'; expected one of sqlite/postgres/mysql",
            guard.name,
            guard.engine
        );
        self.healthy = false;
        self.last_failure = Some(format!(
            "unsupported db guard engine '{}' for guard {}",
            guard.engine, guard.name
        ));
        let mut event = new_event("db_guard", "guard_engine_unknown", "warn");
        event.guard_name = Some(guard.name.clone());
        event.detail = Some(format!("engine={}", guard.engine));
        if let Err(err) = ctx.event_ledger.append(event) {
            tracing::error!("failed to append guard_engine_unknown event: {err}");
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

pub fn detect_engine(dsn: &str) -> &'static str {
    if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        "postgres"
    } else if dsn.starts_with("mysql://") {
        "mysql"
    } else if dsn.starts_with("sqlite://") {
        "sqlite"
    } else {
        "unknown"
    }
}
