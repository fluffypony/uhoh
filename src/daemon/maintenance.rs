use anyhow::Result;
use std::time::Duration;

use crate::config::Config;
use crate::db::Database;
use crate::events::{publish_event, ServerEvent};
use crate::subsystem::{MaintenanceContext, Subsystem, SubsystemContext, SubsystemHealth};

pub(super) struct DaemonMaintenanceSubsystem {
    compaction_index: usize,
    last_backup: Option<std::time::Instant>,
    backup_interval: std::time::Duration,
    sidecar_check_interval: std::time::Duration,
    last_sidecar_check: Option<std::time::Instant>,
    vacuum_in_flight: std::sync::Arc<std::sync::atomic::AtomicBool>,
    mlx_update_state: crate::ai::mlx::MlxAutoUpdateState,
    last_failure: Option<String>,
}

impl DaemonMaintenanceSubsystem {
    pub(super) fn new(config: &Config) -> Self {
        Self {
            compaction_index: 0,
            last_backup: None,
            backup_interval: std::time::Duration::from_secs(
                config.update.check_interval_hours * 3600,
            ),
            sidecar_check_interval: std::time::Duration::from_secs(
                config.sidecar_update.check_interval_hours * 3600,
            ),
            last_sidecar_check: None,
            vacuum_in_flight: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            mlx_update_state: crate::ai::mlx::MlxAutoUpdateState::default(),
            last_failure: None,
        }
    }

    async fn run_tick(&mut self, ctx: &MaintenanceContext) {
        let mut tick_failure: Option<String> = None;
        let db_projects = {
            let db = ctx.database.clone();
            match tokio::task::spawn_blocking(move || db.list_projects()).await {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    tracing::warn!("Failed to poll projects on maintenance tick: {}", e);
                    if tick_failure.is_none() {
                        tick_failure = Some(format!("project poll failed: {e}"));
                    }
                    Vec::new()
                }
                Err(e) => {
                    tracing::warn!("Failed joining maintenance project poll task: {:?}", e);
                    if tick_failure.is_none() {
                        tick_failure = Some(format!("project poll join failed: {e:?}"));
                    }
                    Vec::new()
                }
            }
        };

        if !db_projects.is_empty() {
            let db_path = ctx.uhoh_dir.join("uhoh.db");
            let gc_uhoh_dir = ctx.uhoh_dir.clone();
            let cfg = ctx.config.compaction.clone();
            let idx = self.compaction_index;
            let vacuum_flag = self.vacuum_in_flight.clone();
            let projects = db_projects.clone();
            tokio::spawn(async move {
                let freed = tokio::task::spawn_blocking(move || {
                    let db = crate::db::Database::open(&db_path).ok();
                    let mut freed = 0u64;
                    if let Some(d) = db {
                        let project = &projects[idx % projects.len()];
                        if let Ok(f) =
                            crate::storage::compaction::compact_project(&d, &project.hash, &cfg)
                        {
                            freed = freed.saturating_add(f);
                        }
                    }
                    freed
                })
                .await
                .unwrap_or(0);
                if freed > 100 * 1024 * 1024 {
                    tracing::info!(
                        "Compaction estimated freed {:.1} MB; triggering GC",
                        freed as f64 / 1_048_576.0
                    );
                    let db_path_for_gc = gc_uhoh_dir.join("uhoh.db");
                    let gc_uhoh_dir_cl = gc_uhoh_dir.clone();
                    std::mem::drop(tokio::task::spawn_blocking(move || {
                        if let Ok(db) = crate::db::Database::open(&db_path_for_gc) {
                            if let Err(err) = crate::gc::run_gc(&gc_uhoh_dir_cl, &db) {
                                tracing::warn!("GC run after compaction failed: {err}");
                            }
                            if vacuum_flag
                                .compare_exchange(
                                    false,
                                    true,
                                    std::sync::atomic::Ordering::SeqCst,
                                    std::sync::atomic::Ordering::SeqCst,
                                )
                                .is_ok()
                            {
                                let vacuum_res = db.vacuum();
                                vacuum_flag.store(false, std::sync::atomic::Ordering::SeqCst);
                                if let Err(err) = vacuum_res {
                                    tracing::warn!("VACUUM failed: {}", err);
                                }
                            } else {
                                tracing::debug!("Skipping VACUUM: prior run still in-flight");
                            }
                        }
                    }));
                }
            });
            self.compaction_index = self.compaction_index.wrapping_add(1);
        }

        self.backup_interval =
            std::time::Duration::from_secs(ctx.config.update.check_interval_hours * 3600);
        let do_backup = self
            .last_backup
            .map(|t| t.elapsed() >= self.backup_interval)
            .unwrap_or(true);
        if do_backup {
            let backups_dir = ctx.uhoh_dir.join("backups");
            let _ = std::fs::create_dir_all(&backups_dir);
            let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S");
            let backup_path = backups_dir.join(format!("uhoh-{ts}.db"));
            let db_for_backup = ctx.database.clone();
            let backup_path_cl = backup_path.clone();
            let backup_res = tokio::task::spawn_blocking(move || {
                database_backup_to(&db_for_backup, &backup_path_cl)
            })
            .await;
            if let Err(err) = backup_res.unwrap_or_else(|e| Err(anyhow::anyhow!("{e:?}"))) {
                tracing::warn!("Database backup failed: {}", err);
                if tick_failure.is_none() {
                    tick_failure = Some(format!("database backup failed: {err}"));
                }
            } else {
                if let Ok(entries) = std::fs::read_dir(&backups_dir) {
                    let mut files: Vec<_> = entries.flatten().collect();
                    files.sort_by_key(|entry| entry.file_name());
                    if files.len() > 14 {
                        let to_remove = files.len() - 14;
                        for entry in files.iter().take(to_remove) {
                            let _ = std::fs::remove_file(entry.path());
                        }
                    }
                }
                self.last_backup = Some(std::time::Instant::now());
            }
        }

        let _ = ctx.database.prune_ai_queue_ttl(7);

        // Note: event ledger is NOT pruned by TTL because doing so would break
        // the tamper-evident hash chain. Unbounded growth is addressed by
        // compaction at the snapshot level; the ledger is append-only by design.

        let mut tick_sys = sysinfo::System::new();
        tick_sys.refresh_memory();
        if crate::ai::should_run_ai_with(&ctx.config.ai, &tick_sys) {
            crate::ai::queue::process_summary_queue(
                &ctx.uhoh_dir,
                &ctx.database,
                &ctx.config,
                &ctx.server_event_tx,
            )
            .await;
        }

        self.sidecar_check_interval =
            std::time::Duration::from_secs(ctx.config.sidecar_update.check_interval_hours * 3600);
        if ctx.config.sidecar_update.auto_update && ctx.config.ai.enabled {
            let should_check = self
                .last_sidecar_check
                .map(|last| last.elapsed() >= self.sidecar_check_interval)
                .unwrap_or(true);
            if should_check {
                self.last_sidecar_check = Some(std::time::Instant::now());
                let sidecar_dir = ctx.uhoh_dir.join("sidecar");
                let repo = ctx.config.sidecar_update.llama_repo.clone();
                let pin = ctx.config.sidecar_update.pin_version.clone();
                let event_tx = ctx.server_event_tx.clone();
                tokio::task::spawn_blocking(move || {
                    let before = crate::ai::llama::read_manifest(&sidecar_dir).map(|m| m.version);
                    match crate::ai::llama::run_update_check(
                        &sidecar_dir,
                        &repo,
                        pin.as_deref(),
                        || {
                            crate::ai::sidecar::shutdown_global_sidecar();
                        },
                    ) {
                        Ok(true) => {
                            let after = crate::ai::llama::read_manifest(&sidecar_dir)
                                .map(|m| m.version)
                                .unwrap_or_else(|| "unknown".to_string());
                            publish_event(
                                &event_tx,
                                ServerEvent::SidecarUpdated {
                                    old_version: before,
                                    new_version: after,
                                },
                            );
                        }
                        Ok(false) => {}
                        Err(err) => tracing::warn!("Sidecar update check failed: {}", err),
                    }
                });
            }
        }

        if let Err(err) = crate::ai::mlx::maybe_run_mlx_auto_update(
            &mut self.mlx_update_state,
            &ctx.config.ai,
            &ctx.uhoh_dir,
            Some(&ctx.server_event_tx),
        )
        .await
        {
            publish_event(
                &ctx.server_event_tx,
                ServerEvent::MlxUpdateStatus {
                    status: "failed".to_string(),
                    detail: err.to_string(),
                },
            );
            if tick_failure.is_none() {
                tick_failure = Some(format!("mlx auto-update failed: {err}"));
            }
        }

        self.last_failure = tick_failure;
    }
}

#[async_trait::async_trait]
impl Subsystem for DaemonMaintenanceSubsystem {
    fn name(&self) -> &str {
        "daemon_maintenance"
    }

    async fn run(
        &mut self,
        shutdown: tokio_util::sync::CancellationToken,
        ctx: SubsystemContext,
    ) -> Result<()> {
        let ctx = ctx.maintenance_context();
        let mut tick_interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tick_interval.tick() => {
                    self.run_tick(&ctx).await;
                }
            }
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }

    fn health_check(&self) -> SubsystemHealth {
        match &self.last_failure {
            Some(reason) => SubsystemHealth::Degraded(reason.clone()),
            None => SubsystemHealth::Healthy,
        }
    }
}

fn database_backup_to(database: &Database, path: &std::path::Path) -> anyhow::Result<()> {
    database.backup_to(path)
}

#[cfg(test)]
mod tests {
    use super::DaemonMaintenanceSubsystem;
    use crate::config::Config;
    use crate::db::Database;
    use crate::subsystem::{MaintenanceContext, Subsystem, SubsystemHealth};
    use std::sync::Arc;

    #[tokio::test]
    async fn run_tick_creates_backup_and_reports_healthy_when_no_failures_occur() {
        let temp = tempfile::tempdir().expect("tempdir");
        let database = Arc::new(Database::open(&temp.path().join("uhoh.db")).expect("open db"));
        let (server_event_tx, _) = tokio::sync::broadcast::channel(8);
        let mut config = Config::default();
        config.ai.enabled = false;
        config.sidecar_update.auto_update = false;

        let mut subsystem = DaemonMaintenanceSubsystem::new(&config);
        let ctx = MaintenanceContext {
            database,
            config: config.clone(),
            uhoh_dir: temp.path().to_path_buf(),
            server_event_tx,
        };

        subsystem.run_tick(&ctx).await;

        let backups = std::fs::read_dir(temp.path().join("backups"))
            .expect("backup dir")
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(backups.len(), 1);
        assert!(subsystem.last_backup.is_some());
        assert!(matches!(subsystem.health_check(), SubsystemHealth::Healthy));
    }
}
