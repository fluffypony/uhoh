mod audit;
#[cfg(target_os = "linux")]
mod fanotify;
mod intercept;
mod mcp_proxy;
mod profiles;
mod sandbox;
mod undo;
use std::path::Path;

pub use profiles::{load_agent_profile, resolve_session_log_path};
pub use sandbox::{apply_landlock, sandbox_supported};
pub use undo::resolve_event;

use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::db::AgentEntry;
use crate::event_ledger::new_event;
use crate::subsystem::{AuditSource, Subsystem, SubsystemContext, SubsystemHealth};

pub struct AgentSubsystem {
    healthy: bool,
    fatal_error: Option<String>,
    intercept_started: bool,
    intercept_agent_names: Vec<String>,
    fanotify_started: bool,
    proxy_started: bool,
    proxy_failures: u32,
    proxy_next_retry: Option<std::time::Instant>,
    background_failures: std::sync::Arc<std::sync::atomic::AtomicU64>,
    intercept_task: Option<tokio::task::JoinHandle<Result<()>>>,
    fanotify_task: Option<tokio::task::JoinHandle<()>>,
    proxy_task: Option<tokio::task::JoinHandle<Result<()>>>,
    proxy_shutdown: Option<CancellationToken>,
    #[cfg(all(target_os = "linux", feature = "audit-trail"))]
    fanotify_roots: Vec<std::path::PathBuf>,
}

impl AgentSubsystem {
    pub fn new() -> Self {
        Self {
            healthy: true,
            fatal_error: None,
            intercept_started: false,
            intercept_agent_names: Vec::new(),
            fanotify_started: false,
            proxy_started: false,
            proxy_failures: 0,
            proxy_next_retry: None,
            background_failures: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            intercept_task: None,
            fanotify_task: None,
            proxy_task: None,
            proxy_shutdown: None,
            #[cfg(all(target_os = "linux", feature = "audit-trail"))]
            fanotify_roots: Vec::new(),
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

        let mut last_agent_names: Vec<String> = Vec::new();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                    // Re-read agents each tick so `uhoh agent add` takes effect without restart
                    let agents = match ctx.database.list_agents() {
                        Ok(a) => a,
                        Err(err) => {
                            tracing::error!("failed to list agents: {err}");
                            continue;
                        }
                    };

                    // Log newly registered agents
                    for agent in &agents {
                        if !last_agent_names.contains(&agent.name) {
                            let mut event = new_event("agent", "agent_registered", "info");
                            event.agent_name = Some(agent.name.clone());
                            event.detail = Some(format!("profile={}", agent.profile_path));
                            if let Err(err) = ctx.event_ledger.append(event) {
                                tracing::error!("failed to append agent_registered event: {err}");
                            }
                        }
                    }
                    last_agent_names = agents.iter().map(|a| a.name.clone()).collect();

                    self.poll_background_tasks().await;
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
        if let Some(token) = self.proxy_shutdown.take() {
            token.cancel();
        }
        if let Some(task) = self.proxy_task.take() {
            let _ = task.await;
        }
        // Cancel and await intercept tailer task
        if let Some(task) = self.intercept_task.take() {
            task.abort();
            let _ = task.await;
        }
        // Cancel and await fanotify monitor task
        if let Some(task) = self.fanotify_task.take() {
            task.abort();
            let _ = task.await;
        }
        Ok(())
    }

    fn health_check(&self) -> SubsystemHealth {
        if let Some(message) = &self.fatal_error {
            return SubsystemHealth::Failed(message.clone());
        }
        if self.healthy {
            if self.fanotify_started {
                SubsystemHealth::HealthyWithAudit(AuditSource::Fanotify)
            } else {
                SubsystemHealth::HealthyWithAudit(AuditSource::None)
            }
        } else {
            SubsystemHealth::DegradedWithAudit {
                message: "agent monitor reported failures".to_string(),
                source: if self.fanotify_started {
                    AuditSource::Fanotify
                } else {
                    AuditSource::None
                },
            }
        }
    }
}

impl AgentSubsystem {
    fn record_background_failure(&mut self, message: String) {
        self.background_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.healthy = false;
        self.fatal_error = Some(message.clone());
        tracing::error!("{message}");
    }

    async fn poll_result_task(
        task: &mut Option<tokio::task::JoinHandle<Result<()>>>,
        label: &str,
    ) -> Option<String> {
        if !task
            .as_ref()
            .map(|task| task.is_finished())
            .unwrap_or(false)
        {
            return None;
        }

        if let Some(task) = task.take() {
            match task.await {
                Ok(Ok(())) => None,
                Ok(Err(err)) => Some(format!("{label} task failed: {err}")),
                Err(err) => Some(format!("{label} task panicked: {err}")),
            }
        } else {
            None
        }
    }

    async fn poll_background_tasks(&mut self) {
        let intercept_finished = self
            .intercept_task
            .as_ref()
            .map(|task| task.is_finished())
            .unwrap_or(false);
        if let Some(message) =
            Self::poll_result_task(&mut self.intercept_task, "session tailer").await
        {
            self.record_background_failure(message);
        }
        if intercept_finished {
            self.intercept_started = false;
        }

        let proxy_finished = self
            .proxy_task
            .as_ref()
            .map(|task| task.is_finished())
            .unwrap_or(false);
        if let Some(message) = Self::poll_result_task(&mut self.proxy_task, "mcp proxy").await {
            self.record_background_failure(message);
        }
        if proxy_finished {
            self.proxy_shutdown = None;
            self.proxy_started = false;
            self.proxy_failures = self.proxy_failures.saturating_add(1);
            let backoff_secs = (2u64).pow(self.proxy_failures.min(6)); // max 64s
            self.proxy_next_retry =
                Some(std::time::Instant::now() + std::time::Duration::from_secs(backoff_secs));
        }

        if self
            .fanotify_task
            .as_ref()
            .map(|task| task.is_finished())
            .unwrap_or(false)
        {
            if let Some(task) = self.fanotify_task.take() {
                if let Err(err) = task.await {
                    self.record_background_failure(format!("fanotify task panicked: {err}"));
                }
            }
            self.fanotify_started = false;
        }
    }

    fn tick_agents(&mut self, ctx: &SubsystemContext, agents: &[AgentEntry]) -> Result<()> {
        if ctx.config.agent.intercept_enabled {
            // Detect agent set changes and restart the tailer if needed
            let current_names: Vec<String> = agents.iter().map(|a| a.name.clone()).collect();
            let agents_changed =
                self.intercept_started && current_names != self.intercept_agent_names;
            if agents_changed {
                // Cancel old tailer so it restarts with the updated agent list
                if let Some(task) = self.intercept_task.take() {
                    task.abort();
                }
                self.intercept_started = false;
            }

            if !self.intercept_started {
                // Clear fatal_error on successful restart to avoid permanent health degradation
                self.fatal_error = None;
                self.intercept_started = true;
                self.intercept_agent_names = current_names;
                let ctx_cl = ctx.clone();
                let agents_cl = agents.to_vec();
                self.intercept_task = Some(tokio::spawn(async move {
                    intercept::run_session_tailers_async(&ctx_cl, &agents_cl).await
                }));
            }
        }

        #[cfg(all(target_os = "linux", feature = "audit-trail"))]
        {
            let desired_roots = if ctx.config.agent.audit_scope.eq_ignore_ascii_case("home") {
                dirs::home_dir().into_iter().collect::<Vec<_>>()
            } else {
                ctx.database
                    .list_projects()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|p| std::path::PathBuf::from(p.current_path))
                    .collect::<Vec<_>>()
            };

            let mut normalized = desired_roots;
            normalized.sort();
            normalized.dedup();

            if !ctx.config.agent.audit_enabled {
                if let Some(task) = self.fanotify_task.take() {
                    task.abort();
                }
                self.fanotify_started = false;
                self.fanotify_roots.clear();
            } else {
                let roots_changed = self.fanotify_roots != normalized;
                if roots_changed {
                    if let Some(task) = self.fanotify_task.take() {
                        task.abort();
                    }
                    self.fanotify_started = false;
                    self.fanotify_roots = normalized.clone();
                }

                if !self.fanotify_started {
                    self.fanotify_started = true;
                    let ctx_cl = ctx.clone();
                    let agents_cl = agents.to_vec();
                    let roots_cl = self.fanotify_roots.clone();
                    let failures = self.background_failures.clone();
                    self.fanotify_task = Some(tokio::task::spawn_blocking(move || {
                        if let Err(err) = fanotify::run_permission_monitor_with_roots(
                            &ctx_cl, &agents_cl, &roots_cl,
                        ) {
                            failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            tracing::debug!("fanotify monitor unavailable: {err}");
                        }
                    }));
                }
            }
        }

        if ctx.config.agent.audit_enabled {
            audit::tick_audit(ctx, agents)?;
        }
        if ctx.config.agent.mcp_proxy_enabled {
            if !self.proxy_started {
                // Localized backoff to avoid infinite restart loops
                if let Some(retry_at) = self.proxy_next_retry {
                    if std::time::Instant::now() < retry_at {
                        return Ok(());
                    }
                }
                if self.proxy_failures >= 20 {
                    tracing::error!("MCP proxy permanently disabled after 20 failures");
                    return Ok(());
                }
                // Clear fatal_error on successful restart attempt to allow recovery
                if self.proxy_failures > 0 {
                    self.fatal_error = None;
                    self.healthy = true;
                }
                self.proxy_started = true;
                let ctx_cl = ctx.clone();
                let token = CancellationToken::new();
                let token_cl = token.clone();
                self.proxy_shutdown = Some(token);
                self.proxy_task = Some(tokio::spawn(async move {
                    mcp_proxy::run_proxy(ctx_cl, token_cl).await
                }));
            }
        }
        Ok(())
    }
}
