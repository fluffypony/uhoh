use anyhow::{Context, Result};
use notify::Watcher as _;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, Mutex};

use crate::config::Config;
use crate::db::{Database, LedgerSeverity, LedgerSource, ProjectEntry};
use crate::event_ledger::{new_event, EventLedger};
use crate::events::{publish_event, ServerEvent};
use crate::notifications::NotificationPipeline;
use crate::subsystem::{SubsystemContext, SubsystemManager};

use super::maintenance::DaemonMaintenanceSubsystem;
use super::snapshots;
use super::watcher::{BinaryChangeWatcher, WatcherRuntime};

pub(super) async fn run_foreground(uhoh_dir: &Path, database: Arc<Database>) -> Result<()> {
    let uhoh_dir = uhoh_dir.to_path_buf();
    let config_path = uhoh_dir.join("config.toml");
    let config = Config::load(&config_path)?;

    // === Singleton acquisition: must happen BEFORE starting any subsystems ===
    #[cfg(windows)]
    let _daemon_mutex = {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;
        let dir_hash = blake3::hash(uhoh_dir.to_string_lossy().as_bytes())
            .to_hex()
            .to_string();
        let mutex_name = format!("Local\\uhoh-{}\0", &dir_hash[..16]);
        let name: Vec<u16> = OsStr::new(&mutex_name).encode_wide().collect();
        // SAFETY: CreateMutexW is called with a valid NUL-terminated wide string;
        // the returned handle is checked for null and ERROR_ALREADY_EXISTS.
        // The handle is stored in _daemon_mutex so it remains open for the
        // daemon's lifetime, preventing duplicate instances.
        unsafe {
            let handle = winapi::um::synchapi::CreateMutexW(
                std::ptr::null_mut(),
                winapi::shared::minwindef::FALSE,
                name.as_ptr(),
            );
            if handle.is_null() {
                anyhow::bail!("Failed to create daemon mutex");
            }
            if winapi::um::errhandlingapi::GetLastError()
                == winapi::shared::winerror::ERROR_ALREADY_EXISTS
            {
                winapi::um::handleapi::CloseHandle(handle);
                anyhow::bail!("Another uhoh daemon is already running");
            }
            handle
        }
    };

    let pid_path = uhoh_dir.join("daemon.pid");

    #[cfg(not(windows))]
    let _pid_lock_file = {
        let mut pid_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&pid_path)?;
        {
            use fs4::FileExt;
            pid_file
                .try_lock_exclusive()
                .context("Another uhoh daemon is already running (PID file locked)")?;
        }
        use std::io::Write as _;
        let _ = pid_file.set_len(0);
        let start_ticks =
            crate::platform::read_process_start_ticks(std::process::id()).unwrap_or(0);
        let record = format!("{} {}\n", std::process::id(), start_ticks);
        let _ = pid_file.write_all(record.as_bytes());
        let _ = pid_file.sync_all();
        pid_file
    };

    #[cfg(windows)]
    let _pid_lock_file = {
        let lock_path = uhoh_dir.join("daemon.lock");
        let lock_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        {
            use fs4::FileExt;
            lock_file
                .try_lock_exclusive()
                .context("Another uhoh daemon is already running (lock file)")?;
        }
        let start_ticks =
            crate::platform::read_process_start_ticks(std::process::id()).unwrap_or(0);
        let record = format!("{} {}\n", std::process::id(), start_ticks);
        let _ = std::fs::write(&pid_path, record.as_bytes());
        lock_file
    };

    let mut runtime =
        ForegroundDaemonRuntime::bootstrap(uhoh_dir.clone(), config_path, config, database).await?;
    runtime.run().await?;

    std::fs::remove_file(&pid_path).ok();
    std::fs::remove_file(uhoh_dir.join("server.port")).ok();
    tracing::info!("Daemon stopped.");
    Ok(())
}

struct ForegroundDaemonRuntime {
    uhoh_dir: PathBuf,
    config_path: PathBuf,
    config: Config,
    database: Arc<Database>,
    sidecar_manager: crate::ai::sidecar::SidecarManager,
    server_event_tx: broadcast::Sender<ServerEvent>,
    restore_coordinator: crate::restore::RestoreCoordinator,
    restore_in_progress: Arc<AtomicBool>,
    event_ledger: EventLedger,
    subsystem_manager: Arc<Mutex<SubsystemManager>>,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    watcher_runtime: WatcherRuntime,
    watch_event_rx: mpsc::Receiver<super::watcher::WatchEvent>,
    binary_watcher: BinaryChangeWatcher,
    project_states: HashMap<String, snapshots::ProjectDaemonState>,
    moved_folder_retries: snapshots::MovedFolderRetryState,
    shutdown_rx: mpsc::Receiver<()>,
    update_check_interval: tokio::time::Interval,
    debounce_interval: tokio::time::Interval,
    maintenance_interval: tokio::time::Interval,
    update_trigger: PathBuf,
}

impl ForegroundDaemonRuntime {
    async fn bootstrap(
        uhoh_dir: PathBuf,
        config_path: PathBuf,
        config: Config,
        database: Arc<Database>,
    ) -> Result<Self> {
        let (server_event_tx, _) = broadcast::channel::<ServerEvent>(4096);
        let restore_coordinator = crate::restore::RestoreCoordinator::new();
        let restore_in_progress = restore_coordinator.in_progress_flag();
        let event_ledger =
            EventLedger::new(database.clone()).with_event_publisher(server_event_tx.clone());
        let sidecar_manager = crate::ai::sidecar::SidecarManager::new();
        let subsystem_manager = start_subsystems(
            &config,
            sidecar_manager.clone(),
            &database,
            &event_ledger,
            &uhoh_dir,
            &server_event_tx,
        )
        .await;

        NotificationPipeline::new(config.notifications.clone()).spawn(server_event_tx.clone());
        let server_handle = start_server(
            &config,
            &database,
            &uhoh_dir,
            &server_event_tx,
            &restore_coordinator,
            sidecar_manager.clone(),
            &subsystem_manager,
        )
        .await?;

        let projects = database.list_projects()?;
        let project_states = seed_project_states(database.as_ref(), &projects);
        let (watch_event_tx, watch_event_rx) = mpsc::channel(100_000);
        let watch_paths = watched_paths(&projects);
        let watcher_runtime = WatcherRuntime::start(&watch_paths, watch_event_tx)?;
        let binary_watcher = BinaryChangeWatcher::start(&uhoh_dir);
        let shutdown_rx = spawn_shutdown_listener()?;
        let update_check_interval = tokio::time::interval(Duration::from_secs(
            config.update.check_interval_hours * 3600,
        ));
        let debounce_interval =
            tokio::time::interval(Duration::from_secs(config.watch.debounce_quiet_secs));

        let log_path = uhoh_dir.join("daemon.log");
        tracing::info!(
            "Daemon starting, PID={}, log={}",
            std::process::id(),
            log_path.display()
        );

        #[cfg(target_os = "linux")]
        check_inotify_limit();

        Ok(Self {
            uhoh_dir: uhoh_dir.clone(),
            config_path,
            config,
            database,
            sidecar_manager,
            server_event_tx,
            restore_coordinator,
            restore_in_progress,
            event_ledger,
            subsystem_manager,
            server_handle,
            watcher_runtime,
            watch_event_rx,
            binary_watcher,
            project_states,
            moved_folder_retries: snapshots::MovedFolderRetryState::default(),
            shutdown_rx,
            update_check_interval,
            debounce_interval,
            maintenance_interval: tokio::time::interval(Duration::from_secs(60)),
            update_trigger: uhoh_dir.join(".update-ready"),
        })
    }

    async fn run(&mut self) -> Result<()> {
        tracing::info!(
            "Daemon running, watching {} projects",
            self.project_states.len()
        );

        loop {
            tokio::select! {
                Some(event) = self.watch_event_rx.recv() => {
                    self.watcher_runtime.handle_event(
                        event,
                        &mut self.project_states,
                        self.database.as_ref(),
                        &self.uhoh_dir,
                        &self.restore_coordinator,
                    );
                }
                Some(()) = self.binary_watcher.event_rx.recv() => {
                    if self.restart_current_process("Binary change detected; restarting daemon")? {
                        break;
                    }
                }
                _ = self.debounce_interval.tick() => {
                    self.process_pending_snapshots().await;
                }
                _ = self.maintenance_interval.tick() => {
                    if self.handle_maintenance_tick().await? {
                        break;
                    }
                }
                _ = self.update_check_interval.tick() => {
                    self.handle_update_check_tick();
                }
                _ = self.shutdown_rx.recv() => {
                    self.shutdown().await;
                    break;
                }
            }
        }

        Ok(())
    }

    async fn process_pending_snapshots(&mut self) {
        let snapshot_runtime = crate::snapshot::SnapshotRuntime::new(
            crate::snapshot::SnapshotSettings::from_config(&self.config),
            self.sidecar_manager.clone(),
        );
        snapshots::process_pending_snapshots(snapshots::SnapshotProcessCtx {
            uhoh_dir: &self.uhoh_dir,
            database: self.database.clone(),
            states: &mut self.project_states,
            snapshot_runtime: &snapshot_runtime,
            event_tx: &self.server_event_tx,
            event_ledger: &self.event_ledger,
            restore_in_progress: &self.restore_in_progress,
            was_restoring_snapshot: self.watcher_runtime.was_restoring_flag_mut(),
        })
        .await;
    }

    async fn handle_maintenance_tick(&mut self) -> Result<bool> {
        let tick = run_tick_maintenance(TickMaintenanceCtx {
            config: &self.config,
            config_path: &self.config_path,
            update_trigger: &self.update_trigger,
            database: &self.database,
            event_ledger: &self.event_ledger,
            subsystem_manager: &self.subsystem_manager,
            uhoh_dir: &self.uhoh_dir,
            server_event_tx: &self.server_event_tx,
            watcher_runtime: &mut self.watcher_runtime,
            project_states: &mut self.project_states,
            moved_folder_retries: &mut self.moved_folder_retries,
        })
        .await;

        if let Some(new_debounce) = tick.updated_debounce {
            self.debounce_interval = new_debounce;
        }
        if let Some(new_update_interval) = tick.updated_update_interval {
            self.update_check_interval = new_update_interval;
        }
        self.config = tick.updated_config;

        if tick.should_restart_for_update {
            let _ = std::fs::remove_file(&self.update_trigger);
            return self
                .restart_current_process("Update ready trigger detected; restarting daemon");
        }

        Ok(false)
    }

    fn handle_update_check_tick(&self) {
        if !self.config.update.auto_check {
            return;
        }

        let uhoh_dir = self.uhoh_dir.clone();
        tokio::spawn(async move {
            if let Err(err) = crate::update::check_and_apply_update(&uhoh_dir).await {
                tracing::debug!("Update check failed: {}", err);
            }
        });
    }

    async fn shutdown(&mut self) {
        tracing::info!("Shutdown signal received");
        self.sidecar_manager.shutdown();
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
        }
        self.subsystem_manager.lock().await.shutdown_all().await;
    }

    fn restart_current_process(&self, message: &str) -> Result<bool> {
        tracing::info!("{message}");
        cleanup_restart_state(&self.uhoh_dir);

        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            let exe = std::env::current_exe().context("Failed to resolve daemon executable")?;
            let args: Vec<String> = std::env::args().collect();
            let rest: &[String] = if args.len() > 1 { &args[1..] } else { &[] };
            let err = std::process::Command::new(exe).args(rest).exec();
            anyhow::bail!("exec failed: {err}");
        }

        #[cfg(windows)]
        {
            let exe = std::env::current_exe().context("Failed to resolve daemon executable")?;
            let rest = current_args_without_takeover();
            let _child = std::process::Command::new(exe)
                .args(&rest)
                .arg("--takeover")
                .arg(std::process::id().to_string())
                .spawn();
            return Ok(true);
        }

        #[allow(unreachable_code)]
        Ok(false)
    }
}

fn watched_paths(projects: &[ProjectEntry]) -> Vec<PathBuf> {
    projects
        .iter()
        .filter(|project| Path::new(&project.current_path).exists())
        .map(|project| PathBuf::from(&project.current_path))
        .collect()
}

fn seed_project_states(
    database: &Database,
    projects: &[ProjectEntry],
) -> HashMap<String, snapshots::ProjectDaemonState> {
    let mut project_states = HashMap::new();
    for project in projects {
        project_states.insert(
            project.current_path.clone(),
            snapshots::seed_project_state(database, project),
        );
    }
    project_states
}

async fn start_subsystems(
    config: &Config,
    sidecar_manager: crate::ai::sidecar::SidecarManager,
    database: &Arc<Database>,
    event_ledger: &EventLedger,
    uhoh_dir: &Path,
    server_event_tx: &broadcast::Sender<ServerEvent>,
) -> Arc<Mutex<SubsystemManager>> {
    let mut subsystem_manager = SubsystemManager::new(5, Duration::from_secs(600));
    subsystem_manager.register(Box::new(crate::db_guard::DbGuardSubsystem::new()));
    subsystem_manager.register(Box::new(crate::agent::AgentSubsystem::new()));
    subsystem_manager.register(Box::new(DaemonMaintenanceSubsystem::new(
        config,
        sidecar_manager,
    )));
    let subsystem_manager = Arc::new(Mutex::new(subsystem_manager));

    let ctx = SubsystemContext {
        database: database.clone(),
        event_ledger: event_ledger.clone(),
        config: config.clone(),
        uhoh_dir: uhoh_dir.to_path_buf(),
        server_event_tx: server_event_tx.clone(),
    };
    subsystem_manager.lock().await.start_all(ctx).await;
    subsystem_manager
}

async fn start_server(
    config: &Config,
    database: &Arc<Database>,
    uhoh_dir: &Path,
    server_event_tx: &broadcast::Sender<ServerEvent>,
    restore_coordinator: &crate::restore::RestoreCoordinator,
    sidecar_manager: crate::ai::sidecar::SidecarManager,
    subsystem_manager: &Arc<Mutex<SubsystemManager>>,
) -> Result<Option<tokio::task::JoinHandle<()>>> {
    if !config.server.enabled {
        return Ok(None);
    }

    crate::server::start_server(crate::server::ServerBootstrap {
        config: config.server.clone(),
        full_config: config.clone(),
        database: database.clone(),
        uhoh_dir: uhoh_dir.to_path_buf(),
        event_tx: server_event_tx.clone(),
        restore_coordinator: restore_coordinator.clone(),
        sidecar_manager,
        subsystem_manager: subsystem_manager.clone(),
    })
    .await
    .map(Some)
}

fn spawn_shutdown_listener() -> Result<mpsc::Receiver<()>> {
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
    tokio::spawn({
        let shutdown_tx = shutdown_tx.clone();
        async move {
            tokio::signal::ctrl_c().await.ok();
            let _ = shutdown_tx.send(()).await;
        }
    });

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        // SAFETY: setting SIGHUP to SIG_IGN is safe — it simply tells the kernel
        // to ignore hangup signals so the daemon survives terminal closure.
        unsafe {
            libc::signal(libc::SIGHUP, libc::SIG_IGN);
        }
        let mut sigterm = signal(SignalKind::terminate())?;
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            sigterm.recv().await;
            let _ = shutdown_tx.send(()).await;
        });
    }

    Ok(shutdown_rx)
}

fn cleanup_restart_state(uhoh_dir: &Path) {
    let _ = std::fs::remove_file(uhoh_dir.join("daemon.pid"));
    let _ = std::fs::remove_file(uhoh_dir.join("server.port"));
}

#[cfg(windows)]
fn current_args_without_takeover() -> Vec<String> {
    let mut args: Vec<String> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|arg| arg == "--takeover") {
        let _ = args.remove(pos);
        if pos < args.len() {
            let _ = args.remove(pos);
        }
    }
    if args.len() > 1 {
        args[1..].to_vec()
    } else {
        Vec::new()
    }
}

struct TickOutcome {
    updated_config: Config,
    updated_debounce: Option<tokio::time::Interval>,
    updated_update_interval: Option<tokio::time::Interval>,
    should_restart_for_update: bool,
}

struct TickMaintenanceCtx<'a> {
    config: &'a Config,
    config_path: &'a Path,
    update_trigger: &'a Path,
    database: &'a Arc<Database>,
    event_ledger: &'a EventLedger,
    subsystem_manager: &'a Arc<Mutex<SubsystemManager>>,
    uhoh_dir: &'a Path,
    server_event_tx: &'a broadcast::Sender<ServerEvent>,
    watcher_runtime: &'a mut WatcherRuntime,
    project_states: &'a mut HashMap<String, snapshots::ProjectDaemonState>,
    moved_folder_retries: &'a mut snapshots::MovedFolderRetryState,
}

async fn run_tick_maintenance(ctx: TickMaintenanceCtx<'_>) -> TickOutcome {
    let TickMaintenanceCtx {
        config,
        config_path,
        update_trigger,
        database,
        event_ledger,
        subsystem_manager,
        uhoh_dir,
        server_event_tx,
        watcher_runtime,
        project_states,
        moved_folder_retries,
    } = ctx;

    let mut next_config = config.clone();
    let mut next_debounce = None;
    let mut next_update = None;

    match Config::load(config_path) {
        Ok(new_cfg) => {
            if new_cfg.watch.debounce_quiet_secs != config.watch.debounce_quiet_secs {
                next_debounce = Some(tokio::time::interval(Duration::from_secs(
                    new_cfg.watch.debounce_quiet_secs,
                )));
            }
            if new_cfg.update.check_interval_hours != config.update.check_interval_hours {
                next_update = Some(tokio::time::interval(Duration::from_secs(
                    new_cfg.update.check_interval_hours * 3600,
                )));
            }
            next_config = new_cfg;
        }
        Err(err) => {
            tracing::warn!(
                "Failed to reload config from {} on daemon tick: {}",
                config_path.display(),
                err
            );
            let mut event = new_event(
                LedgerSource::Daemon,
                "config_reload_failed",
                LedgerSeverity::Warn,
            );
            event.detail = Some(format!("path={}, error={}", config_path.display(), err));
            if let Err(append_err) = event_ledger.append(event) {
                tracing::error!("failed to append config_reload_failed event: {append_err}");
            }
        }
    }

    let db_for_poll = Arc::clone(database);
    let db_projects = match tokio::task::spawn_blocking(move || db_for_poll.list_projects()).await {
        Ok(Ok(projects)) => projects,
        Ok(Err(err)) => {
            tracing::warn!("Failed to poll projects on tick: {}", err);
            Vec::new()
        }
        Err(err) => {
            tracing::warn!("Failed to join periodic project polling task: {:?}", err);
            Vec::new()
        }
    };

    snapshots::check_moved_folders(
        &db_projects,
        database,
        watcher_runtime.watcher_handle_mut(),
        project_states,
        moved_folder_retries,
    );

    crate::cas::cleanup_stale_temp_files(&uhoh_dir.join("blobs"), Duration::from_secs(3600));
    cleanup_stale_restore_staging_dirs(&db_projects);

    snapshots::check_for_new_projects(
        &db_projects,
        watcher_runtime.watcher_handle_mut(),
        project_states,
        server_event_tx,
        database,
    );
    remove_deleted_projects(
        &db_projects,
        watcher_runtime.watcher_handle_mut(),
        project_states,
        server_event_tx,
    );

    let subsystem_ctx = SubsystemContext {
        database: Arc::clone(database),
        event_ledger: event_ledger.clone(),
        config: next_config.clone(),
        uhoh_dir: uhoh_dir.to_path_buf(),
        server_event_tx: server_event_tx.clone(),
    };
    subsystem_manager
        .lock()
        .await
        .tick_restart(subsystem_ctx)
        .await;

    TickOutcome {
        updated_config: next_config,
        updated_debounce: next_debounce,
        updated_update_interval: next_update,
        should_restart_for_update: update_trigger.exists(),
    }
}

fn cleanup_stale_restore_staging_dirs(projects: &[ProjectEntry]) {
    for project in projects {
        if let Ok(entries) = std::fs::read_dir(&project.current_path) {
            for entry in entries.flatten() {
                if !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".uhoh-restore-tmp-")
                {
                    continue;
                }
                if entry.file_type().map(|ft| ft.is_symlink()).unwrap_or(false) {
                    continue;
                }
                if let Ok(meta) = entry.metadata() {
                    let stale = meta
                        .modified()
                        .ok()
                        .and_then(|modified| modified.elapsed().ok())
                        .map(|age| age > Duration::from_secs(3600))
                        .unwrap_or(false);
                    if stale {
                        let _ = std::fs::remove_dir_all(entry.path());
                    }
                }
            }
        }
    }
}

fn remove_deleted_projects(
    db_projects: &[ProjectEntry],
    watcher_handle: &mut notify::RecommendedWatcher,
    project_states: &mut HashMap<String, snapshots::ProjectDaemonState>,
    server_event_tx: &broadcast::Sender<ServerEvent>,
) {
    use std::collections::HashSet;

    let db_paths: HashSet<String> = db_projects
        .iter()
        .map(|project| project.current_path.clone())
        .collect();
    let mut removed_paths = Vec::new();
    for key in project_states.keys() {
        if !db_paths.contains(key) {
            removed_paths.push(key.clone());
        }
    }

    for key in removed_paths {
        if let Some(state) = project_states.get(&key) {
            publish_event(
                server_event_tx,
                ServerEvent::ProjectRemoved {
                    project_hash: state.hash.clone(),
                },
            );
        }
        let _ = watcher_handle.unwatch(Path::new(&key));
        project_states.remove(&key);
        tracing::info!("Stopped watching removed project: {}", key);
    }
}

#[cfg(target_os = "linux")]
fn check_inotify_limit() {
    if let Ok(content) = std::fs::read_to_string("/proc/sys/fs/inotify/max_user_watches") {
        if let Ok(limit) = content.trim().parse::<u64>() {
            if limit < 65536 {
                tracing::warn!(
                    "Low inotify watch limit ({}). Consider increasing: \
                     sudo sysctl fs.inotify.max_user_watches=524288",
                    limit
                );
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
fn check_inotify_limit() {}
