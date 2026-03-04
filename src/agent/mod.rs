mod audit;
#[cfg(target_os = "linux")]
mod fanotify;
mod intercept;
mod mcp_proxy;
mod profiles;
mod sandbox;
mod undo;
use std::path::Path;

pub use profiles::load_agent_profile;
pub use sandbox::{apply_landlock, sandbox_supported};
pub use undo::resolve_event;

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::db::AgentEntry;
use crate::event_ledger::new_event;
use crate::subsystem::{Subsystem, SubsystemContext, SubsystemHealth};

pub struct AgentSubsystem {
    healthy: bool,
    intercept_started: bool,
    proxy_started: bool,
    background_failures: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl AgentSubsystem {
    pub fn new() -> Self {
        Self {
            healthy: true,
            intercept_started: false,
            proxy_started: false,
            background_failures: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

pub fn ensure_proxy_token(uhoh_dir: &Path) -> Result<String> {
    mcp_proxy::ensure_proxy_token(uhoh_dir)
}

pub fn build_approval_response(token: &str, approval_id: &str, challenge: &str) -> String {
    mcp_proxy::build_approval_response(token, approval_id, challenge)
}

pub fn proxy_auth_handshake_line(token: &str) -> String {
    mcp_proxy::auth_handshake_line(token)
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
            if let Err(err) = ctx.event_ledger.append(event) {
                tracing::error!("failed to append agent_registered event: {err}");
            }
        }

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                    if self
                        .background_failures
                        .load(std::sync::atomic::Ordering::Relaxed)
                        > 0
                    {
                        self.healthy = false;
                    }
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
            if !self.intercept_started {
                self.intercept_started = true;
                let ctx_cl = ctx.clone();
                let agents_cl = agents.to_vec();
                let failures = self.background_failures.clone();
                std::thread::spawn(move || {
                    #[cfg(target_os = "linux")]
                    {
                        if let Err(err) = fanotify::run_permission_monitor(&ctx_cl, &agents_cl) {
                            tracing::debug!("fanotify monitor unavailable: {err}");
                        }
                    }
                    if let Err(err) = intercept::run_session_tailers(&ctx_cl, &agents_cl) {
                        failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::error!("session tailer thread failed: {err}");
                    }
                });
            }
        }
        if ctx.config.agent.audit_enabled {
            audit::tick_audit(ctx, agents)?;
        }
        if ctx.config.agent.mcp_proxy_enabled {
            if !self.proxy_started {
                self.proxy_started = true;
                let ctx_cl = ctx.clone();
                let failures = self.background_failures.clone();
                std::thread::spawn(move || {
                    if let Err(err) = mcp_proxy::run_proxy(&ctx_cl) {
                        failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::error!("mcp proxy thread failed: {err}");
                    }
                });
            }
        }
        Ok(())
    }
}
