use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::db::Database;
use crate::event_ledger::EventLedger;

#[derive(Debug, Clone)]
pub enum SubsystemHealth {
    Healthy,
    Degraded(String),
    Failed(String),
}

#[derive(Clone)]
pub struct SubsystemContext {
    pub database: Arc<Database>,
    pub event_ledger: EventLedger,
    pub config: crate::config::Config,
    pub uhoh_dir: std::path::PathBuf,
}

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
    restart_count: u32,
}

pub struct SubsystemManager {
    runners: Vec<SubsystemRunner>,
    shutdown: CancellationToken,
    max_restarts: u32,
    _restart_window: Duration,
}

impl SubsystemManager {
    pub fn new(max_restarts: u32, restart_window: Duration) -> Self {
        Self {
            runners: Vec::new(),
            shutdown: CancellationToken::new(),
            max_restarts,
            _restart_window: restart_window,
        }
    }

    pub fn register(&mut self, subsystem: Box<dyn Subsystem>) {
        self.runners.push(SubsystemRunner {
            name: subsystem.name().to_string(),
            subsystem: Arc::new(Mutex::new(subsystem)),
            task: None,
            restart_count: 0,
        });
    }

    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub fn health_snapshot(&self) -> Vec<(String, SubsystemHealth)> {
        self.runners
            .iter()
            .map(|r| (r.name.clone(), SubsystemHealth::Healthy))
            .collect()
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
                .map(|t| t.is_finished())
                .unwrap_or(false);
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

            runner.restart_count = runner.restart_count.saturating_add(1);
            if runner.restart_count > self.max_restarts {
                tracing::error!(
                    "Subsystem '{}' exceeded restart threshold ({}), leaving disabled",
                    runner.name,
                    self.max_restarts
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

struct NoopSubsystem;

#[async_trait]
impl Subsystem for NoopSubsystem {
    fn name(&self) -> &str {
        "noop"
    }

    async fn run(&mut self, _shutdown: CancellationToken, _ctx: SubsystemContext) -> Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }

    fn health_check(&self) -> SubsystemHealth {
        SubsystemHealth::Healthy
    }
}
