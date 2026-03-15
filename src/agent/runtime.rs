use anyhow::Result;
use async_trait::async_trait;
use std::sync::atomic::Ordering;
use tokio_util::sync::CancellationToken;

use crate::db::{AgentEntry, LedgerEventType, LedgerSeverity, LedgerSource};
use crate::event_ledger::new_event;
use crate::subsystem::{AgentContext, AuditSource, Subsystem, SubsystemContext, SubsystemHealth};

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use super::fanotify;
use super::{audit, intercept, proxy};
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::path::PathBuf;

pub struct AgentSubsystem {
    healthy: bool,
    fatal_error: Option<String>,
    runtime_warning: Option<String>,
    intercept_started: bool,
    intercept_agent_names: Vec<String>,
    fanotify_started: bool,
    proxy_started: bool,
    proxy_failures: u32,
    proxy_next_retry: Option<std::time::Instant>,
    background_failures: std::sync::Arc<std::sync::atomic::AtomicU64>,
    intercept_task: Option<tokio::task::JoinHandle<Result<()>>>,
    fanotify_task: Option<tokio::task::JoinHandle<Result<()>>>,
    proxy_task: Option<tokio::task::JoinHandle<Result<()>>>,
    proxy_shutdown: Option<CancellationToken>,
    #[cfg(all(target_os = "linux", feature = "audit-trail"))]
    fanotify_failures: u32,
    #[cfg(all(target_os = "linux", feature = "audit-trail"))]
    fanotify_next_retry: Option<std::time::Instant>,
    #[cfg(all(target_os = "linux", feature = "audit-trail"))]
    fanotify_disabled_reason: Option<String>,
    #[cfg(all(target_os = "linux", feature = "audit-trail"))]
    fanotify_roots: Vec<PathBuf>,
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
const FANOTIFY_MAX_FAILURES: u32 = 5;

impl AgentSubsystem {
    pub fn new() -> Self {
        Self {
            healthy: true,
            fatal_error: None,
            runtime_warning: None,
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
            fanotify_failures: 0,
            #[cfg(all(target_os = "linux", feature = "audit-trail"))]
            fanotify_next_retry: None,
            #[cfg(all(target_os = "linux", feature = "audit-trail"))]
            fanotify_disabled_reason: None,
            #[cfg(all(target_os = "linux", feature = "audit-trail"))]
            fanotify_roots: Vec::new(),
        }
    }

    async fn tick(&mut self, ctx: &AgentContext, last_agent_names: &mut Vec<String>) -> Result<()> {
        let agents = match ctx.database.list_agents() {
            Ok(agents) => agents,
            Err(err) => {
                tracing::error!("failed to list agents: {err}");
                return Ok(());
            }
        };

        self.emit_registration_events(ctx, last_agent_names, &agents);
        *last_agent_names = agents.iter().map(|agent| agent.name.clone()).collect();

        self.runtime_warning = None;
        self.poll_background_tasks(ctx).await;
        self.tick_agents(ctx, &agents)?;
        self.refresh_health_after_tick();
        Ok(())
    }

    fn emit_registration_events(
        &self,
        ctx: &AgentContext,
        last_agent_names: &[String],
        agents: &[AgentEntry],
    ) {
        for agent in agents {
            if last_agent_names.contains(&agent.name) {
                continue;
            }
            let mut event = new_event(
                LedgerSource::Agent,
                LedgerEventType::AgentRegistered,
                LedgerSeverity::Info,
            );
            event.agent_name = Some(agent.name.clone());
            event.detail = Some(format!("profile={}", agent.profile_path));
            if let Err(err) = ctx.event_ledger.append(event) {
                tracing::error!("failed to append agent_registered event: {err}");
            }
        }
    }

    fn refresh_health_after_tick(&mut self) {
        if self.background_failures.load(Ordering::Relaxed) > 0 {
            self.healthy = false;
            return;
        }
        if self.fatal_error.is_none() && self.runtime_warning.is_none() {
            self.healthy = true;
        }
    }

    fn record_background_failure(&mut self, message: String) {
        self.background_failures.fetch_add(1, Ordering::Relaxed);
        self.healthy = false;
        self.fatal_error = Some(message.clone());
        tracing::error!("{message}");
    }

    #[cfg(all(target_os = "linux", feature = "audit-trail"))]
    fn record_runtime_warning(&mut self, message: String) {
        self.healthy = false;
        self.runtime_warning = Some(message.clone());
        tracing::warn!("{message}");
    }

    async fn poll_result_task(
        task: &mut Option<tokio::task::JoinHandle<Result<()>>>,
        label: &str,
    ) -> Option<String> {
        if !task
            .as_ref()
            .map(tokio::task::JoinHandle::is_finished)
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

    async fn poll_background_tasks(&mut self, ctx: &AgentContext) {
        self.poll_intercept_task().await;
        self.poll_proxy_task().await;
        self.poll_fanotify_task(ctx).await;
    }

    async fn poll_intercept_task(&mut self) {
        let intercept_finished = self
            .intercept_task
            .as_ref()
            .map(tokio::task::JoinHandle::is_finished)
            .unwrap_or(false);
        if let Some(message) =
            Self::poll_result_task(&mut self.intercept_task, "session tailer").await
        {
            self.record_background_failure(message);
        }
        if intercept_finished {
            self.intercept_started = false;
        }
    }

    async fn poll_proxy_task(&mut self) {
        let proxy_finished = self
            .proxy_task
            .as_ref()
            .map(tokio::task::JoinHandle::is_finished)
            .unwrap_or(false);
        if let Some(message) = Self::poll_result_task(&mut self.proxy_task, "mcp proxy").await {
            self.record_background_failure(message);
        }
        if proxy_finished {
            self.proxy_shutdown = None;
            self.proxy_started = false;
            self.proxy_failures = self.proxy_failures.saturating_add(1);
            let backoff_secs = (2u64).pow(self.proxy_failures.min(6));
            self.proxy_next_retry =
                Some(std::time::Instant::now() + std::time::Duration::from_secs(backoff_secs));
        }
    }

    async fn poll_fanotify_task(&mut self, _ctx: &AgentContext) {
        if !self
            .fanotify_task
            .as_ref()
            .map(tokio::task::JoinHandle::is_finished)
            .unwrap_or(false)
        {
            return;
        }

        if let Some(message) =
            Self::poll_result_task(&mut self.fanotify_task, "fanotify monitor").await
        {
            #[cfg(all(target_os = "linux", feature = "audit-trail"))]
            self.handle_fanotify_failure(ctx, message);
            #[cfg(not(all(target_os = "linux", feature = "audit-trail")))]
            self.record_background_failure(message);
        }
        self.fanotify_started = false;
    }

    fn tick_agents(&mut self, ctx: &AgentContext, agents: &[AgentEntry]) -> Result<()> {
        self.sync_intercept_tailers(ctx, agents);
        self.sync_fanotify_supervision(ctx, agents)?;
        if ctx.config.agent.audit_enabled {
            audit::tick_audit(ctx, agents);
        }
        self.sync_proxy_supervision(ctx);
        Ok(())
    }

    fn sync_intercept_tailers(&mut self, ctx: &AgentContext, agents: &[AgentEntry]) {
        if !ctx.config.agent.intercept_enabled {
            return;
        }

        let current_names: Vec<String> = agents.iter().map(|agent| agent.name.clone()).collect();
        let agents_changed = self.intercept_started && current_names != self.intercept_agent_names;
        if agents_changed {
            if let Some(task) = self.intercept_task.take() {
                task.abort();
            }
            self.intercept_started = false;
        }

        if self.intercept_started {
            return;
        }

        self.fatal_error = None;
        self.intercept_started = true;
        self.intercept_agent_names = current_names;
        let ctx = ctx.clone();
        let agents = agents.to_vec();
        self.intercept_task = Some(tokio::spawn(async move {
            intercept::run_session_tailers_async(&ctx, &agents).await
        }));
    }

    fn sync_proxy_supervision(&mut self, ctx: &AgentContext) {
        if !ctx.config.agent.mcp_proxy_enabled || self.proxy_started {
            return;
        }
        if let Some(retry_at) = self.proxy_next_retry {
            if std::time::Instant::now() < retry_at {
                return;
            }
        }
        if self.proxy_failures >= 20 {
            tracing::error!("MCP proxy permanently disabled after 20 failures");
            return;
        }

        if self.proxy_failures > 0 {
            self.fatal_error = None;
            self.healthy = true;
        }
        self.proxy_started = true;
        let ctx = ctx.clone();
        let token = CancellationToken::new();
        let token_cl = token.clone();
        self.proxy_shutdown = Some(token);
        self.proxy_task = Some(tokio::spawn(async move {
            proxy::run_proxy(ctx, token_cl).await
        }));
    }

    #[cfg(all(target_os = "linux", feature = "audit-trail"))]
    fn sync_fanotify_supervision(
        &mut self,
        ctx: &AgentContext,
        agents: &[AgentEntry],
    ) -> Result<()> {
        if !ctx.config.agent.audit_enabled {
            self.disable_fanotify_monitor();
            return Ok(());
        }

        let normalized_roots = match self.resolve_audit_roots(ctx) {
            Ok(roots) => roots,
            Err(err) => {
                self.record_runtime_warning(format!("failed to resolve audit roots: {err}"));
                return Ok(());
            }
        };

        let roots_changed = self.fanotify_roots != normalized_roots;
        if roots_changed {
            if let Some(task) = self.fanotify_task.take() {
                task.abort();
            }
            self.fanotify_started = false;
            self.fanotify_roots = normalized_roots;
        }

        if self.fanotify_started {
            return Ok(());
        }
        if self.fanotify_disabled_reason.is_some() {
            return Ok(());
        }
        if let Some(retry_at) = self.fanotify_next_retry {
            if std::time::Instant::now() < retry_at {
                return Ok(());
            }
        }

        self.fanotify_started = true;
        let ctx = ctx.clone();
        let agents = agents.to_vec();
        let roots = self.fanotify_roots.clone();
        self.fanotify_task = Some(tokio::task::spawn_blocking(move || {
            fanotify::run_permission_monitor_with_roots(&ctx, &agents, &roots)
        }));
        Ok(())
    }

    #[cfg(not(all(target_os = "linux", feature = "audit-trail")))]
    fn sync_fanotify_supervision(
        &mut self,
        _ctx: &AgentContext,
        _agents: &[AgentEntry],
    ) -> Result<()> {
        Ok(())
    }

    #[cfg(all(target_os = "linux", feature = "audit-trail"))]
    fn resolve_audit_roots(&self, ctx: &AgentContext) -> Result<Vec<PathBuf>> {
        use anyhow::Context;

        let mut roots = if ctx.config.agent.audit_scope.is_home() {
            dirs::home_dir().into_iter().collect::<Vec<_>>()
        } else {
            ctx.database
                .list_projects()
                .context("database lookup failed while building project audit roots")?
                .into_iter()
                .map(|project| PathBuf::from(project.current_path))
                .collect::<Vec<_>>()
        };
        roots.sort();
        roots.dedup();
        Ok(roots)
    }

    #[cfg(all(target_os = "linux", feature = "audit-trail"))]
    fn disable_fanotify_monitor(&mut self) {
        if let Some(task) = self.fanotify_task.take() {
            task.abort();
        }
        self.fanotify_started = false;
        self.fanotify_failures = 0;
        self.fanotify_next_retry = None;
        self.fanotify_disabled_reason = None;
        self.fanotify_roots.clear();
    }

    #[cfg(all(target_os = "linux", feature = "audit-trail"))]
    fn handle_fanotify_failure(&mut self, ctx: &AgentContext, message: String) {
        let mut event = new_event(
            LedgerSource::Agent,
            LedgerEventType::FanotifyMonitorDegraded,
            LedgerSeverity::Warn,
        );
        event.detail = Some(message.clone());
        if let Err(err) = ctx.event_ledger.append(event) {
            tracing::error!("failed to append fanotify_monitor_degraded event: {err}");
        }

        self.healthy = false;
        self.fanotify_started = false;

        let permanent = message.contains("requires CAP_SYS_ADMIN")
            || message.contains("kernel >= 5.1")
            || message.contains("unsupported")
            || message.contains("requires Linux + audit-trail feature");
        if permanent {
            self.fanotify_failures = FANOTIFY_MAX_FAILURES;
            self.fanotify_next_retry = None;
            self.fanotify_disabled_reason = Some(message.clone());
            tracing::warn!("fanotify monitor disabled: {message}");
            return;
        }

        self.fanotify_failures = self.fanotify_failures.saturating_add(1);
        if self.fanotify_failures >= FANOTIFY_MAX_FAILURES {
            self.fanotify_next_retry = None;
            self.fanotify_disabled_reason = Some(message.clone());
            tracing::warn!(
                "fanotify monitor disabled after {} failures: {}",
                self.fanotify_failures,
                message
            );
            return;
        }

        let backoff_secs = (2u64).pow(self.fanotify_failures.min(6));
        self.fanotify_next_retry =
            Some(std::time::Instant::now() + std::time::Duration::from_secs(backoff_secs));
        self.fanotify_disabled_reason = None;
        tracing::warn!(
            "fanotify monitor failed; retrying in {}s: {}",
            backoff_secs,
            message
        );
    }
}

impl Default for AgentSubsystem {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subsystem::SubsystemHealth;
    use std::sync::atomic::Ordering;

    #[test]
    fn new_creates_healthy_subsystem() {
        let sub = AgentSubsystem::new();
        assert!(sub.healthy);
        assert!(sub.fatal_error.is_none());
        assert!(sub.runtime_warning.is_none());
        assert!(!sub.intercept_started);
        assert!(!sub.fanotify_started);
        assert!(!sub.proxy_started);
        assert_eq!(sub.proxy_failures, 0);
        assert!(sub.proxy_next_retry.is_none());
        assert_eq!(sub.background_failures.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn default_matches_new() {
        let sub = AgentSubsystem::default();
        assert!(sub.healthy);
        assert!(sub.fatal_error.is_none());
    }

    #[test]
    fn health_check_healthy_no_fanotify() {
        let sub = AgentSubsystem::new();
        match sub.health_check() {
            SubsystemHealth::HealthyWithAudit(source) => {
                assert!(matches!(source, AuditSource::None));
            }
            other => panic!("expected HealthyWithAudit(None), got {:?}", other),
        }
    }

    #[test]
    fn health_check_healthy_with_fanotify() {
        let mut sub = AgentSubsystem::new();
        sub.fanotify_started = true;
        match sub.health_check() {
            SubsystemHealth::HealthyWithAudit(source) => {
                assert!(matches!(source, AuditSource::Fanotify));
            }
            other => panic!("expected HealthyWithAudit(Fanotify), got {:?}", other),
        }
    }

    #[test]
    fn health_check_fatal_error() {
        let mut sub = AgentSubsystem::new();
        sub.fatal_error = Some("test failure".to_string());
        match sub.health_check() {
            SubsystemHealth::Failed(msg) => assert_eq!(msg, "test failure"),
            other => panic!("expected Failed, got {:?}", other),
        }
    }

    #[test]
    fn health_check_runtime_warning() {
        let mut sub = AgentSubsystem::new();
        sub.runtime_warning = Some("degraded".to_string());
        match sub.health_check() {
            SubsystemHealth::DegradedWithAudit { message, .. } => {
                assert_eq!(message, "degraded");
            }
            other => panic!("expected DegradedWithAudit, got {:?}", other),
        }
    }

    #[test]
    fn health_check_unhealthy_without_error() {
        let mut sub = AgentSubsystem::new();
        sub.healthy = false;
        match sub.health_check() {
            SubsystemHealth::DegradedWithAudit { message, .. } => {
                assert!(message.contains("failures"));
            }
            other => panic!("expected DegradedWithAudit, got {:?}", other),
        }
    }

    #[test]
    fn refresh_health_recovers_when_clean() {
        let mut sub = AgentSubsystem::new();
        sub.healthy = false;
        sub.refresh_health_after_tick();
        assert!(sub.healthy);
    }

    #[test]
    fn refresh_health_stays_unhealthy_with_background_failures() {
        let mut sub = AgentSubsystem::new();
        sub.background_failures.store(1, Ordering::Relaxed);
        sub.refresh_health_after_tick();
        assert!(!sub.healthy);
    }

    #[test]
    fn refresh_health_stays_unhealthy_with_fatal_error() {
        let mut sub = AgentSubsystem::new();
        sub.fatal_error = Some("fail".to_string());
        sub.healthy = false;
        sub.refresh_health_after_tick();
        assert!(!sub.healthy);
    }

    #[test]
    fn record_background_failure_sets_state() {
        let mut sub = AgentSubsystem::new();
        sub.record_background_failure("boom".to_string());
        assert!(!sub.healthy);
        assert_eq!(sub.fatal_error.as_deref(), Some("boom"));
        assert_eq!(sub.background_failures.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn subsystem_name() {
        let sub = AgentSubsystem::new();
        assert_eq!(sub.name(), "agent");
    }

    #[test]
    fn proxy_backoff_saturates() {
        let mut sub = AgentSubsystem::new();
        sub.proxy_failures = 20;
        assert_eq!(sub.proxy_failures, 20);
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

        let mut last_agent_names = Vec::new();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                () = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                    self.tick(&ctx, &mut last_agent_names).await?;
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
        if let Some(task) = self.intercept_task.take() {
            task.abort();
            let _ = task.await;
        }
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
        #[cfg(all(target_os = "linux", feature = "audit-trail"))]
        if let Some(message) = &self.fanotify_disabled_reason {
            return SubsystemHealth::DegradedWithAudit {
                message: message.clone(),
                source: AuditSource::None,
            };
        }
        if let Some(message) = &self.runtime_warning {
            return SubsystemHealth::DegradedWithAudit {
                message: message.clone(),
                source: if self.fanotify_started {
                    AuditSource::Fanotify
                } else {
                    AuditSource::None
                },
            };
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
