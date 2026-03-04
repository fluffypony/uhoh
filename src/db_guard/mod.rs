mod credentials;
mod mysql;
mod postgres;
mod recovery;
mod sqlite_guard;

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::db::DbGuardEntry;
use crate::event_ledger::new_event;
use crate::subsystem::{Subsystem, SubsystemContext, SubsystemHealth};

pub use credentials::scrub_dsn;

pub struct DbGuardSubsystem {
    healthy: bool,
}

impl DbGuardSubsystem {
    pub fn new() -> Self {
        Self { healthy: true }
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
            let _ = ctx.event_ledger.append(event);
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
                    sqlite_guard::tick_sqlite_guard(ctx, guard)?;
                }
                "postgres" => {
                    postgres::tick_postgres_guard(ctx, guard)?;
                }
                "mysql" => {
                    mysql::tick_mysql_guard(ctx, guard)?;
                }
                _ => {}
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
