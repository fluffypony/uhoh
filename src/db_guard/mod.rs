mod credentials;
mod mysql;
mod postgres;
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
pub use credentials::scrub_dsn;
pub use credentials::scrub_error_message;
pub use credentials::store_encrypted_credential;
pub use credentials::CredentialMaterial;
pub use recovery::decrypt_recovery_payload;
pub use recovery::write_postgres_schema_baseline;
pub use recovery::write_sqlite_baseline;

pub struct DbGuardSubsystem {
    healthy: bool,
    sqlite_versions: HashMap<String, i64>,
    mysql_states: HashMap<String, mysql::MysqlGuardState>,
}

impl DbGuardSubsystem {
    pub fn new() -> Self {
        Self {
            healthy: true,
            sqlite_versions: HashMap::new(),
            mysql_states: HashMap::new(),
        }
    }
}

#[async_trait]
impl Subsystem for DbGuardSubsystem {
    fn name(&self) -> &str {
        "db_guard"
    }

    async fn run(&mut self, shutdown: CancellationToken, ctx: SubsystemContext) -> Result<()> {
        if !ctx.config.db_guard.enabled {
            tracing::info!("db_guard disabled by config");
            shutdown.cancelled().await;
            return Ok(());
        }

        let guards = ctx.database.list_db_guards()?;
        for guard in &guards {
            let mut event = new_event("db_guard", "guard_started", "info");
            event.guard_name = Some(guard.name.clone());
            event.detail = Some(format!("engine={}, mode={}", guard.engine, guard.mode));
            if let Err(err) = ctx.event_ledger.append(event) {
                tracing::error!("failed to append guard_started event: {err}");
            }
        }

        // Phase 1: lightweight loop for schema polling and sqlite guards.
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    self.tick_guards(&ctx, &guards)?;
                }
            }
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
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
        for guard in guards {
            match guard.engine.as_str() {
                "sqlite" => {
                    sqlite_guard::tick_sqlite_guard(ctx, guard, &mut self.sqlite_versions)?;
                }
                "postgres" => {
                    postgres::tick_postgres_guard(ctx, guard)?;
                }
                "mysql" => {
                    let state = self.mysql_states.entry(guard.name.clone()).or_default();
                    mysql::tick_mysql_guard(ctx, guard, state)?;
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
