mod maintenance;
mod snapshots;

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::db::Database;
use crate::event_ledger::EventLedger;
use crate::events::{publish_event, ServerEvent};
use crate::notifications::NotificationPipeline;
use crate::subsystem::{SubsystemContext, SubsystemManager};
use crate::watcher;
use maintenance::DaemonMaintenanceSubsystem;
use notify::{RecursiveMode, Watcher as _};

// Removed duplicate is_uhoh_process_alive; use crate::platform::is_uhoh_process_alive instead

/// Spawn daemon as a detached background process.
pub fn spawn_detached_daemon(uhoh_dir: &Path) -> Result<()> {
    let exe = std::env::current_exe()?;

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let log_path = uhoh_dir.join("daemon.log");
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("Failed to open daemon log: {}", log_path.display()))?;
        let err_file = log_file.try_clone().with_context(|| {
            format!("Failed to clone daemon log handle: {}", log_path.display())
        })?;
        let mut cmd = std::process::Command::new(&exe);
        cmd.args(["start", "--service"]);
        // Detach from controlling terminal
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    eprintln!("Warning: setsid() failed; daemon may not fully detach");
                }
                Ok(())
            });
        }
        cmd.stdout(std::process::Stdio::from(log_file))
            .stderr(std::process::Stdio::from(err_file))
            .stdin(std::process::Stdio::null());
        cmd.spawn().context("Failed to spawn daemon")?;
    }

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x00000008;
        let log_path = uhoh_dir.join("daemon.log");
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("Failed to open daemon log: {}", log_path.display()))?;
        let err_file = log_file.try_clone().with_context(|| {
            format!("Failed to clone daemon log handle: {}", log_path.display())
        })?;
        std::process::Command::new(&exe)
            .args(["start", "--service"])
            .creation_flags(DETACHED_PROCESS)
            .stdout(std::process::Stdio::from(log_file))
            .stderr(std::process::Stdio::from(err_file))
            .stdin(std::process::Stdio::null())
            .spawn()
            .context("Failed to spawn daemon")?;
    }

    println!("Daemon started.");
    Ok(())
}

/// Send a watch event to the daemon. Uses blocking_send since the watcher
/// bridge runs on a dedicated OS thread, applying natural backpressure
/// without stalling the async runtime.
pub(crate) fn send_watch_event(
    tx: &tokio::sync::mpsc::Sender<WatchEvent>,
    event: WatchEvent,
) -> std::result::Result<(), tokio::sync::mpsc::error::SendError<WatchEvent>> {
    tx.blocking_send(event)
}

/// Stop the daemon by reading PID file and sending signal.
pub fn stop_daemon(uhoh_dir: &Path) -> Result<()> {
    let pid_path = uhoh_dir.join("daemon.pid");
    let pid_str = std::fs::read_to_string(&pid_path).context("Daemon not running (no PID file)")?;
    let mut parts = pid_str.split_whitespace();
    let pid: u32 = parts
        .next()
        .context("Invalid PID file")?
        .parse()
        .context("Invalid PID file")?;
    let expected_start = parts.next().and_then(|v| v.parse::<u64>().ok());

    if !crate::platform::is_uhoh_process_alive_with_start(pid, expected_start) {
        std::fs::remove_file(&pid_path).ok();
        println!("Daemon was not running (stale PID file cleaned up).");
        return Ok(());
    }

    #[cfg(unix)]
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }

    #[cfg(unix)]
    {
        // Wait briefly for clean shutdown before removing the PID file.
        for _ in 0..50 {
            if !crate::platform::is_uhoh_process_alive_with_start(pid, expected_start) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if crate::platform::is_uhoh_process_alive_with_start(pid, expected_start) {
            eprintln!("Warning: daemon process {} did not exit after SIGTERM. PID file removed but process may still be running.", pid);
        }
    }

    #[cfg(windows)]
    {
        // Try graceful termination first
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T"])
            .status();
        // Wait briefly for process to end
        for _ in 0..50 {
            if !crate::platform::is_uhoh_process_alive_with_start(pid, expected_start) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if crate::platform::is_uhoh_process_alive_with_start(pid, expected_start) {
            // Force kill as fallback
            let _ = std::process::Command::new("taskkill")
                .args(["/F", "/PID", &pid.to_string(), "/T"])
                .status();
        }
    }

    std::fs::remove_file(&pid_path).ok();
    println!("Daemon stopped.");
    Ok(())
}

/// Run the daemon in the foreground (called with --service flag).
pub async fn run_foreground(uhoh_dir: &Path, database: std::sync::Arc<Database>) -> Result<()> {
    let uhoh_dir = uhoh_dir.to_path_buf();
    let config_path = uhoh_dir.join("config.toml");
    let mut config = Config::load(&config_path)?;

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

    // Write PID file with exclusive lock to avoid races.
    // On Windows, use a separate .lock file so the .pid file remains readable
    // by CLI commands (Windows file locks are mandatory and block reads).
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
        let f = &mut pid_file;
        let _ = f.write_all(record.as_bytes());
        let _ = f.sync_all();
        pid_file // keep alive to hold the lock
    };

    #[cfg(windows)]
    let _pid_lock_file = {
        // Use a separate .lock file so the .pid remains readable
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
        // Write PID to the separate .pid file (not locked, so CLI can read it)
        use std::io::Write;
        let start_ticks =
            crate::platform::read_process_start_ticks(std::process::id()).unwrap_or(0);
        let record = format!("{} {}\n", std::process::id(), start_ticks);
        let _ = std::fs::write(&pid_path, record.as_bytes());
        lock_file // keep alive to hold the lock
    };

    let (server_event_tx, _) = broadcast::channel::<ServerEvent>(4096);
    let restore_coordinator = crate::restore_runtime::RestoreCoordinator::new();
    let restore_in_progress = restore_coordinator.in_progress_flag();

    let event_ledger =
        EventLedger::new(database.clone()).with_event_publisher(server_event_tx.clone());
    let mut subsystem_manager_inner = SubsystemManager::new(5, Duration::from_secs(600));
    subsystem_manager_inner.register(Box::new(crate::db_guard::DbGuardSubsystem::new()));
    subsystem_manager_inner.register(Box::new(crate::agent::AgentSubsystem::new()));
    subsystem_manager_inner.register(Box::new(DaemonMaintenanceSubsystem::new(&config)));
    let subsystem_manager = Arc::new(Mutex::new(subsystem_manager_inner));

    {
        let ctx = SubsystemContext {
            database: database.clone(),
            event_ledger: event_ledger.clone(),
            config: config.clone(),
            uhoh_dir: uhoh_dir.clone(),
            server_event_tx: server_event_tx.clone(),
        };
        subsystem_manager.lock().await.start_all(ctx).await;
    }

    let notifications = NotificationPipeline::new(config.notifications.clone());
    notifications.spawn(server_event_tx.clone());

    let server_handle = if config.server.enabled {
        Some(
            crate::server::start_server(crate::server::ServerBootstrap {
                config: config.server.clone(),
                full_config: config.clone(),
                database: database.clone(),
                uhoh_dir: uhoh_dir.clone(),
                event_tx: server_event_tx.clone(),
                restore_coordinator: restore_coordinator.clone(),
                subsystem_manager: subsystem_manager.clone(),
            })
            .await?,
        )
    } else {
        None
    };

    // Set up logging to file
    let log_path = uhoh_dir.join("daemon.log");
    tracing::info!(
        "Daemon starting, PID={}, log={}",
        std::process::id(),
        log_path.display()
    );

    // Check inotify watch limit on Linux
    #[cfg(target_os = "linux")]
    check_inotify_limit();

    // Channel for events from watcher (large bounded buffer to cap memory under bursts).
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<WatchEvent>(100_000);

    // Load registered projects (initial), dynamic discovery runs on ticks
    let projects = &database.list_projects()?;

    // Start file watcher
    let watch_paths: Vec<PathBuf> = projects
        .iter()
        .filter(|p| Path::new(&p.current_path).exists())
        .map(|p| PathBuf::from(&p.current_path))
        .collect();

    let mut watcher_handle = watcher::start_watching(&watch_paths, event_tx.clone())?;

    // Binary self-watch: use a dedicated bridge thread and unbounded tokio channel
    let (bin_event_tx, mut bin_event_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let exe_path = std::env::current_exe().ok();
    let _bin_watcher = if let Some(ref exe) = exe_path {
        let (bin_notify_tx, bin_notify_rx) = std::sync::mpsc::channel();
        let mut watcher = notify::RecommendedWatcher::new(
            move |res| {
                let _ = bin_notify_tx.send(res);
            },
            notify::Config::default(),
        )
        .ok();
        if let Some(ref mut w) = watcher {
            if let Some(parent) = exe.parent() {
                let _ = w.watch(parent, RecursiveMode::NonRecursive);
            }
            let _ = w.watch(&uhoh_dir, RecursiveMode::NonRecursive);
        }
        let exe_clone = exe.clone();
        let tx = bin_event_tx.clone();
        std::thread::Builder::new()
            .name("bin-watcher-bridge".into())
            .spawn(move || {
                for evt in bin_notify_rx.into_iter().flatten() {
                    let involves_binary = evt.paths.iter().any(|p| p == &exe_clone)
                        || evt
                            .paths
                            .iter()
                            .any(|p| p.file_name().is_some_and(|n| n == ".update-ready"));
                    if involves_binary {
                        let _ = tx.send(());
                    }
                }
            })
            .ok();
        watcher
    } else {
        None
    };

    // Per-project state
    let mut project_states: HashMap<String, snapshots::ProjectDaemonState> = HashMap::new();
    for project in projects {
        project_states.insert(
            project.current_path.clone(),
            snapshots::seed_project_state(&database, project),
        );
    }

    // Handle graceful shutdown (SIGINT + SIGTERM)
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
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
        unsafe {
            libc::signal(libc::SIGHUP, libc::SIG_IGN);
        } // ignore SIGHUP
        let mut sigterm = signal(SignalKind::terminate())?;
        let shutdown_tx2 = shutdown_tx.clone();
        tokio::spawn(async move {
            sigterm.recv().await;
            let _ = shutdown_tx2.send(()).await;
        });
    }

    // Main event loop
    // Backoff state for watcher recovery
    let mut recover_attempts: u32 = 0;
    let mut next_recover_at: Option<Instant> = None;
    let mut update_check_interval = tokio::time::interval(Duration::from_secs(
        config.update.check_interval_hours * 3600,
    ));
    let mut debounce_interval =
        tokio::time::interval(Duration::from_secs(config.watch.debounce_quiet_secs));
    let update_trigger = uhoh_dir.join(".update-ready");
    let mut maintenance_interval = tokio::time::interval(Duration::from_secs(60));

    tracing::info!("Daemon running, watching {} projects", projects.len());

    let mut was_restoring = false;
    loop {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                let currently_restoring =
                    crate::restore_runtime::is_restore_active(&restore_coordinator, &uhoh_dir);
                // Detect restore completion (true → false transition)
                if was_restoring && !currently_restoring {
                    let now = Instant::now();
                    snapshots::mark_restore_completed(&mut project_states, database.as_ref(), now);
                    tracing::debug!("Restore completed, grace period started for all projects");
                }
                was_restoring = currently_restoring;

                if currently_restoring {
                    // Control signals (WatcherDied, Overflow) must always be processed,
                    // even during restore. Only suppress file-level events for the
                    // project being restored.
                    let is_control = matches!(event, WatchEvent::WatcherDied | WatchEvent::Overflow);
                    if !is_control {
                        let restoring_hash =
                            crate::restore_runtime::read_restoring_project_hash(&uhoh_dir);
                        if snapshots::should_skip_event_during_restore(
                            &project_states,
                            &event,
                            restoring_hash.as_deref(),
                        ) {
                            tracing::debug!("Skipping watcher event during restore operation");
                            continue;
                        }
                    }
                }
                match event {
                    WatchEvent::WatcherDied => {
                        let now = Instant::now();
                        if let Some(ready_at) = next_recover_at {
                            if now < ready_at {
                                // Re-queue after remaining backoff instead of dropping the event
                                let remaining = ready_at - now;
                                let tx = event_tx.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(remaining).await;
                                    if send_watch_event(&tx, WatchEvent::WatcherDied).is_err() {
                                        tracing::warn!(
                                            "Watcher recovery signal dropped due to full event queue"
                                        );
                                    }
                                });
                                continue;
                            }
                        }
                        let delay_secs = (1u64 << recover_attempts.min(6)).min(60);
                        tracing::error!("File watcher died — attempting recovery (attempt #{})...", recover_attempts + 1);
                        let paths: Vec<PathBuf> = project_states
                            .keys()
                            .map(PathBuf::from)
                            .collect();
                        match watcher::start_watching(&paths, event_tx.clone()) {
                            Ok(new_watcher) => {
                                watcher_handle = new_watcher;
                                tracing::info!("File watcher recovered successfully");
                                recover_attempts = 0;
                                next_recover_at = None;
                            }
                            Err(e) => {
                                recover_attempts = recover_attempts.saturating_add(1);
                                next_recover_at = Some(now + Duration::from_secs(delay_secs));
                                tracing::error!("Failed to recover file watcher: {}. Backing off {}s.", e, delay_secs);
                            }
                        }
                    }
                    other => {
                        snapshots::handle_watch_event(&mut project_states, &other, &config, &uhoh_dir);
                    }
                }
            }
            // Binary change events
            Some(()) = bin_event_rx.recv() => {
                tracing::info!("Binary change detected; restarting daemon");
                #[cfg(unix)]
                {
                    use std::os::unix::process::CommandExt;
                    let args: Vec<String> = std::env::args().collect();
                    let rest: &[String] = if args.len() > 1 { &args[1..] } else { &[] };
                    if let Some(ref exe) = exe_path {
                        // Clean up state files before exec replaces the process.
                        // Keep server.token so browser localStorage tokens stay valid.
                        let _ = std::fs::remove_file(uhoh_dir.join("daemon.pid"));
                        let _ = std::fs::remove_file(uhoh_dir.join("server.port"));
                        let err = std::process::Command::new(exe).args(rest).exec();
                        anyhow::bail!("exec failed: {err}");
                    }
                }
                #[cfg(windows)]
                {
                    let mut args: Vec<String> = std::env::args().collect();
                    if let Some(pos) = args.iter().position(|a| a == "--takeover") {
                        let _ = args.remove(pos);
                        if pos < args.len() { let _ = args.remove(pos); }
                    }
                    let rest: Vec<String> = if args.len() > 1 { args[1..].to_vec() } else { Vec::new() };
                    if let Some(ref exe) = exe_path {
                        let _child = std::process::Command::new(exe)
                            .args(&rest)
                            .arg("--takeover")
                            .arg(std::process::id().to_string())
                            .spawn();
                    }
                    break;
                }
            }
            _ = debounce_interval.tick() => {
                snapshots::process_pending_snapshots(snapshots::SnapshotProcessCtx {
                    uhoh_dir: &uhoh_dir,
                    database: database.clone(),
                    states: &mut project_states,
                    config: &config,
                    event_tx: &server_event_tx,
                    event_ledger: &event_ledger,
                    restore_in_progress: &restore_in_progress,
                    was_restoring_snapshot: &mut was_restoring,
                }).await;
            }
            _ = maintenance_interval.tick() => {
                let tick = run_tick_maintenance(TickMaintenanceCtx {
                    config: &config,
                    config_path: &config_path,
                    update_trigger: &update_trigger,
                    database: &database,
                    event_ledger: &event_ledger,
                    subsystem_manager: &subsystem_manager,
                    uhoh_dir: &uhoh_dir,
                    server_event_tx: &server_event_tx,
                    watcher_handle: &mut watcher_handle,
                    project_states: &mut project_states,
                }).await;

                if let Some(new_debounce) = tick.updated_debounce {
                    debounce_interval = new_debounce;
                }
                if let Some(new_update_interval) = tick.updated_update_interval {
                    update_check_interval = new_update_interval;
                }
                config = tick.updated_config;

                if tick.should_restart_for_update {
                    tracing::info!("Update ready trigger detected; restarting daemon");
                    let _ = std::fs::remove_file(&update_trigger);
                    #[cfg(unix)]
                    {
                        use std::os::unix::process::CommandExt;
                        let args: Vec<String> = std::env::args().collect();
                        let rest: &[String] = if args.len() > 1 { &args[1..] } else { &[] };
                        if let Ok(exe) = std::env::current_exe() {
                            // Clean up state files before exec replaces the process.
                            // Keep server.token so browser localStorage tokens stay valid.
                            let _ = std::fs::remove_file(uhoh_dir.join("daemon.pid"));
                            let _ = std::fs::remove_file(uhoh_dir.join("server.port"));
                            let err = std::process::Command::new(exe).args(rest).exec();
                            tracing::error!("exec failed: {}", err);
                        }
                    }
                    #[cfg(windows)]
                    {
                        if let Ok(exe) = std::env::current_exe() {
                            let mut args: Vec<String> = std::env::args().collect();
                            if let Some(pos) = args.iter().position(|a| a == "--takeover") {
                                let _ = args.remove(pos);
                                if pos < args.len() { let _ = args.remove(pos); }
                            }
                            args.push("--takeover".into());
                            args.push(std::process::id().to_string());
                            let rest: Vec<String> = if args.len() > 1 { args[1..].to_vec() } else { Vec::new() };
                            let _ = std::process::Command::new(exe)
                                .args(&rest)
                                .spawn();
                        }
                    }
                    break;
                }
            }
            _ = update_check_interval.tick() => {
                if config.update.auto_check {
                    let uhoh_dir_clone = uhoh_dir.clone();
                    tokio::spawn(async move {
                        if let Err(e) = crate::update::check_and_apply_update(&uhoh_dir_clone).await {
                            tracing::debug!("Update check failed: {}", e);
                        }
                    });
                }
            }
            _ = shutdown_rx.recv() => {
                tracing::info!("Shutdown signal received");
                // Attempt to shutdown AI sidecar if running
                crate::ai::sidecar::shutdown_global_sidecar();
                if let Some(handle) = &server_handle {
                    handle.abort();
                }
                subsystem_manager.lock().await.shutdown_all().await;
                break;
            }
        }
    }

    // Cleanup: remove transient state files. Keep server.token so
    // it persists across restarts (browser localStorage tokens stay valid).
    std::fs::remove_file(&pid_path).ok();
    std::fs::remove_file(uhoh_dir.join("server.port")).ok();
    tracing::info!("Daemon stopped.");
    Ok(())
}

#[derive(Debug)]
pub enum WatchEvent {
    FileChanged(PathBuf),
    FileDeleted(PathBuf),
    Rescan(PathBuf), // Targeted rescan marker
    Overflow,        // Global overflow — rescan all projects
    WatcherDied,     // Bridge thread ended
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
    watcher_handle: &'a mut notify::RecommendedWatcher,
    project_states: &'a mut HashMap<String, snapshots::ProjectDaemonState>,
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
        watcher_handle,
        project_states,
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
            let mut event =
                crate::event_ledger::new_event("daemon", "config_reload_failed", "warn");
            event.detail = Some(format!("path={}, error={}", config_path.display(), err));
            if let Err(append_err) = event_ledger.append(event) {
                tracing::error!("failed to append config_reload_failed event: {append_err}");
            }
        }
    }

    let db_for_poll = Arc::clone(database);
    let db_projects = match tokio::task::spawn_blocking(move || db_for_poll.list_projects()).await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            tracing::warn!("Failed to poll projects on tick: {}", e);
            Vec::new()
        }
        Err(e) => {
            tracing::warn!("Failed to join periodic project polling task: {:?}", e);
            Vec::new()
        }
    };

    snapshots::check_moved_folders(&db_projects, database, watcher_handle, project_states);

    // Clean up stale temp files in blob store (from crashed processes)
    crate::cas::cleanup_stale_temp_files(
        &uhoh_dir.join("blobs"),
        std::time::Duration::from_secs(3600),
    );

    // Clean up stale restore staging directories left by crashed restores
    for project in &db_projects {
        if let Ok(entries) = std::fs::read_dir(&project.current_path) {
            for entry in entries.flatten() {
                if entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".uhoh-restore-tmp-")
                {
                    // Never recurse into symlinked staging paths.
                    if entry.file_type().map(|ft| ft.is_symlink()).unwrap_or(false) {
                        continue;
                    }
                    if let Ok(meta) = entry.metadata() {
                        let stale = meta
                            .modified()
                            .ok()
                            .and_then(|m| m.elapsed().ok())
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

    snapshots::check_for_new_projects(
        &db_projects,
        watcher_handle,
        project_states,
        server_event_tx,
        database,
    );

    use std::collections::HashSet;
    let db_paths: HashSet<String> = db_projects.iter().map(|p| p.current_path.clone()).collect();
    let mut to_remove: Vec<String> = Vec::new();
    for key in project_states.keys() {
        if !db_paths.contains(key) {
            to_remove.push(key.clone());
        }
    }
    for key in to_remove {
        if let Some(state) = project_states.get(&key) {
            publish_event(
                server_event_tx,
                ServerEvent::ProjectRemoved {
                    project_hash: state.hash.clone(),
                },
            );
        }
        let _ = watcher_handle.unwatch(std::path::Path::new(&key));
        project_states.remove(&key);
        tracing::info!("Stopped watching removed project: {}", key);
    }

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
