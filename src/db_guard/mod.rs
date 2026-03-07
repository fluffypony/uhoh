mod credentials;
mod mysql;
pub mod postgres;
mod recovery;
mod sqlite_guard;
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
pub use postgres::pg_connect_spawn;
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

pub struct DbGuardSubsystem {
    healthy: bool,
    sqlite_versions: HashMap<String, i64>,
    mysql_states: HashMap<String, mysql::MysqlGuardState>,
    shutdown: Option<CancellationToken>,
}

trait DbGuardEngine {
    fn engine_name(&self) -> &'static str;
    fn tick(
        &mut self,
        ctx: &SubsystemContext,
        guard: &DbGuardEntry,
        tick_interval_secs: i64,
    ) -> Result<()>;
}

struct SqliteEngine<'a> {
    versions: &'a mut HashMap<String, i64>,
}

impl DbGuardEngine for SqliteEngine<'_> {
    fn engine_name(&self) -> &'static str {
        "sqlite"
    }

    fn tick(
        &mut self,
        ctx: &SubsystemContext,
        guard: &DbGuardEntry,
        _tick_interval_secs: i64,
    ) -> Result<()> {
        sqlite_guard::tick_sqlite_guard(ctx, guard, self.versions)
    }
}

struct PostgresEngine;

impl DbGuardEngine for PostgresEngine {
    fn engine_name(&self) -> &'static str {
        "postgres"
    }

    fn tick(
        &mut self,
        ctx: &SubsystemContext,
        guard: &DbGuardEntry,
        tick_interval_secs: i64,
    ) -> Result<()> {
        postgres::tick_postgres_guard(ctx, guard, tick_interval_secs)
    }
}

struct MysqlEngine<'a> {
    states: &'a mut HashMap<String, mysql::MysqlGuardState>,
}

impl DbGuardEngine for MysqlEngine<'_> {
    fn engine_name(&self) -> &'static str {
        "mysql"
    }

    fn tick(
        &mut self,
        ctx: &SubsystemContext,
        guard: &DbGuardEntry,
        _tick_interval_secs: i64,
    ) -> Result<()> {
        let state = self.states.entry(guard.name.clone()).or_default();
        mysql::tick_mysql_guard(ctx, guard, state)
    }
}

impl DbGuardSubsystem {
    pub fn new() -> Self {
        Self {
            healthy: true,
            sqlite_versions: HashMap::new(),
            mysql_states: HashMap::new(),
            shutdown: None,
        }
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
                            sqlite_versions,
                            mysql_states,
                            shutdown,
                        };
                        let result = worker.tick_guards(&ctx_cl, &guards_cl);
                        (result, worker.sqlite_versions, worker.mysql_states)
                    })
                    .await;

                    match tick_result {
                        Ok((result, sqlite_versions, mysql_states)) => {
                            self.sqlite_versions = sqlite_versions;
                            self.mysql_states = mysql_states;
                            result?;
                        }
                        Err(err) => {
                            tracing::error!("db_guard tick worker panicked: {err}");
                            self.healthy = false;
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
            SubsystemHealth::Degraded("db guard reported failures".to_string())
        }
    }
}

impl DbGuardSubsystem {
    fn tick_guards(&mut self, ctx: &SubsystemContext, guards: &[DbGuardEntry]) -> Result<()> {
        if let Some(token) = self.shutdown.as_ref() {
            postgres::reconcile_listen_workers(guards, token)?;
        }

        for guard in guards {
            let effective_mode = normalize_guard_mode(&guard.engine, &guard.mode);
            match guard.engine.as_str() {
                "sqlite" => {
                    let mut engine = SqliteEngine {
                        versions: &mut self.sqlite_versions,
                    };
                    tracing::trace!("db_guard tick via {} engine", engine.engine_name());
                    engine.tick(ctx, guard, GUARD_TICK_INTERVAL_SECS)?;
                }
                "postgres" => {
                    let mut engine = PostgresEngine;
                    tracing::trace!("db_guard tick via {} engine", engine.engine_name());
                    engine.tick(ctx, guard, GUARD_TICK_INTERVAL_SECS)?;
                }
                "mysql" => {
                    let mut engine = MysqlEngine {
                        states: &mut self.mysql_states,
                    };
                    tracing::trace!("db_guard tick via {} engine", engine.engine_name());
                    engine.tick(ctx, guard, GUARD_TICK_INTERVAL_SECS)?;
                }
                _ => {}
            }

            // Global retention cleanup pass per guard.
            let guard_base = ctx.uhoh_dir.join("db_guard").join(&guard.name);
            let baseline = guard_base.join("baselines");
            let recovery_dir = guard_base.join("recovery");
            if baseline.exists() {
                let _ = recovery::cleanup_retention(
                    &baseline,
                    ctx.config.db_guard.recovery_retention_days,
                );
            }
            if recovery_dir.exists() {
                let _ = recovery::cleanup_retention(
                    &recovery_dir,
                    ctx.config.db_guard.recovery_retention_days,
                );
            }

            let configured_mode = guard.mode.trim().to_ascii_lowercase();
            if configured_mode != effective_mode {
                let mut event = new_event("db_guard", "guard_mode_normalized", "info");
                event.guard_name = Some(guard.name.clone());
                event.detail = Some(format!(
                    "engine={}, configured_mode={}, effective_mode={}",
                    guard.engine, configured_mode, effective_mode
                ));
                let _ = ctx.event_ledger.append(event);
            }
        }
        Ok(())
    }
}

pub fn derive_guard_name_from_dsn(dsn: &str) -> String {
    let stripped = dsn
        .replace("postgres://", "")
        .replace("mysql://", "")
        .replace("sqlite://", "")
        .replace('/', "-")
        .replace(':', "-");
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
