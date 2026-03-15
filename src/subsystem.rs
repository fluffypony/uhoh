use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::db::Database;
use crate::event_ledger::EventLedger;
use crate::events::ServerEvent;

#[derive(Debug, Clone)]
pub enum AuditSource {
    None,
    Fanotify,
}

#[derive(Debug, Clone)]
pub enum SubsystemHealth {
    Healthy,
    HealthyWithAudit(AuditSource),
    Degraded(String),
    DegradedWithAudit {
        message: String,
        source: AuditSource,
    },
    Failed(String),
}

#[derive(Clone)]
#[non_exhaustive]
pub struct SubsystemContext {
    pub database: Arc<Database>,
    pub event_ledger: EventLedger,
    pub config: crate::config::Config,
    pub uhoh_dir: std::path::PathBuf,
    pub server_event_tx: tokio::sync::broadcast::Sender<ServerEvent>,
}

impl SubsystemContext {
    pub fn new(
        database: Arc<Database>,
        event_ledger: EventLedger,
        config: crate::config::Config,
        uhoh_dir: std::path::PathBuf,
        server_event_tx: tokio::sync::broadcast::Sender<ServerEvent>,
    ) -> Self {
        Self {
            database,
            event_ledger,
            config,
            uhoh_dir,
            server_event_tx,
        }
    }
}

pub type AgentContext = SubsystemContext;
pub type DbGuardContext = SubsystemContext;

#[async_trait]
pub trait Subsystem: Send + Sync {
    fn name(&self) -> &str;
    async fn run(&mut self, shutdown: CancellationToken, ctx: SubsystemContext) -> Result<()>;
    async fn shutdown(&mut self) -> Result<()>;
    fn health_check(&self) -> SubsystemHealth;
}

struct SubsystemRunner {
    name: String,
    subsystem: Arc<Mutex<Box<dyn Subsystem>>>,
    task: Option<JoinHandle<Result<()>>>,
    restart_times: Vec<std::time::Instant>,
}

pub struct SubsystemManager {
    runners: Vec<SubsystemRunner>,
    shutdown: CancellationToken,
    max_restarts: u32,
    restart_window: Duration,
}

impl SubsystemManager {
    pub fn new(max_restarts: u32, restart_window: Duration) -> Self {
        Self {
            runners: Vec::new(),
            shutdown: CancellationToken::new(),
            max_restarts,
            restart_window,
        }
    }

    pub fn register(&mut self, subsystem: Box<dyn Subsystem>) {
        self.runners.push(SubsystemRunner {
            name: subsystem.name().to_string(),
            subsystem: Arc::new(Mutex::new(subsystem)),
            task: None,
            restart_times: Vec::new(),
        });
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub async fn health_snapshot(&self) -> Vec<(String, SubsystemHealth)> {
        let mut out = Vec::with_capacity(self.runners.len());
        for runner in &self.runners {
            let subsystem = runner.subsystem.lock().await;
            out.push((runner.name.clone(), subsystem.health_check()));
        }
        out
    }

    pub async fn start_all(&mut self, ctx: SubsystemContext) {
        for runner in &mut self.runners {
            start_runner_task(runner, self.shutdown.child_token(), ctx.clone());
        }
    }

    pub async fn tick_restart(&mut self, ctx: SubsystemContext) {
        for runner in &mut self.runners {
            let finished = runner
                .task
                .as_ref()
                .map(tokio::task::JoinHandle::is_finished)
                .unwrap_or(true); // None means task is gone (dead) — treat as finished
            if !finished {
                continue;
            }

            if let Some(task) = runner.task.take() {
                match task.await {
                    Ok(Ok(())) => {
                        tracing::warn!("Subsystem '{}' exited; restarting", runner.name);
                    }
                    Ok(Err(e)) => {
                        tracing::warn!("Subsystem '{}' failed: {}; restarting", runner.name, e);
                    }
                    Err(e) => {
                        tracing::warn!("Subsystem '{}' join error: {}; restarting", runner.name, e);
                    }
                }
            }

            let now = std::time::Instant::now();
            runner
                .restart_times
                .retain(|t| now.duration_since(*t) <= self.restart_window);
            runner.restart_times.push(now);
            if runner.restart_times.len() as u32 > self.max_restarts {
                tracing::error!(
                    "Subsystem '{}' exceeded restart threshold ({} in {:?}), leaving disabled",
                    runner.name,
                    self.max_restarts,
                    self.restart_window
                );
                continue;
            }

            start_runner_task(runner, self.shutdown.child_token(), ctx.clone());
        }
    }

    pub async fn shutdown_all(&mut self) {
        self.shutdown.cancel();
        for runner in &mut self.runners {
            {
                // The lock is intentionally held across shutdown().await because
                // Subsystem::shutdown() requires &mut self.  The scoped block
                // ensures the guard is dropped before we await the task join below,
                // which is the longer-running await.  Each subsystem's shutdown()
                // implementation should return promptly.
                let mut subsystem = runner.subsystem.lock().await;
                let _ = subsystem.shutdown().await;
            }
            if let Some(task) = runner.task.take() {
                let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
            }
        }
    }
}

fn start_runner_task(
    runner: &mut SubsystemRunner,
    shutdown: CancellationToken,
    ctx: SubsystemContext,
) {
    let subsystem = runner.subsystem.clone();
    let name = runner.name.clone();
    runner.task = Some(tokio::spawn(async move {
        tracing::info!("Starting subsystem: {}", name);
        let mut guard = subsystem.lock().await;
        guard.run(shutdown, ctx).await
    }));
}
