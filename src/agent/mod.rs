mod audit;
mod intercept;
mod mcp_proxy;
mod profiles;
mod sandbox;
mod undo;

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::db::AgentEntry;
use crate::event_ledger::new_event;
use crate::subsystem::{Subsystem, SubsystemContext, SubsystemHealth};

pub struct AgentSubsystem {
    healthy: bool,
}

impl AgentSubsystem {
    pub fn new() -> Self {
        Self { healthy: true }
    }
}

#[async_trait]
impl Subsystem for AgentSubsystem {
    fn name(&self) -> &str {
        "agent"
    }

    async fn run(&mut self, shutdown: CancellationToken, ctx: SubsystemContext) -> Result<()> {
        if !ctx.config.agent.enabled {
            tracing::info!("agent monitor disabled by config");
            shutdown.cancelled().await;
            return Ok(());
        }

        let agents = ctx.database.list_agents()?;
        for agent in &agents {
            let mut event = new_event("agent", "agent_registered", "info");
            event.agent_name = Some(agent.name.clone());
            event.detail = Some(format!("profile={}", agent.profile_path));
            let _ = ctx.event_ledger.append(event);
        }

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                    self.tick_agents(&ctx, &agents)?;
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
            SubsystemHealth::Degraded("agent monitor reported failures".to_string())
        }
    }
}

impl AgentSubsystem {
    fn tick_agents(&mut self, ctx: &SubsystemContext, agents: &[AgentEntry]) -> Result<()> {
        if ctx.config.agent.intercept_enabled {
            intercept::tick_session_tailers(ctx, agents)?;
        }
        if ctx.config.agent.audit_enabled {
            audit::tick_audit(ctx, agents)?;
        }
        if ctx.config.agent.mcp_proxy_enabled {
            mcp_proxy::tick_proxy(ctx)?;
        }
        Ok(())
    }
}
