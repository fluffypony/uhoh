use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::config::Config;
use crate::db::Database;
use crate::event_ledger::EventLedger;
use crate::notifications::NotificationPipeline;
use crate::snapshot;
use crate::subsystem::{Subsystem, SubsystemContext, SubsystemHealth, SubsystemManager};
use crate::watcher;
use notify::{RecursiveMode, Watcher as _};

struct DaemonMaintenanceSubsystem {
    compaction_index: usize,
    last_backup: Option<std::time::Instant>,
    backup_interval: std::time::Duration,
    sidecar_check_interval: std::time::Duration,
    last_sidecar_check: Option<std::time::Instant>,
    vacuum_in_flight: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl DaemonMaintenanceSubsystem {
    fn new(config: &Config) -> Self {
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
        }
    }

    async fn run_tick(&mut self, ctx: &SubsystemContext) {
        let db_projects = {
            let db = ctx.database.clone();
            match tokio::task::spawn_blocking(move || db.list_projects()).await {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    tracing::warn!("Failed to poll projects on maintenance tick: {}", e);
                    Vec::new()
                }
                Err(e) => {
                    tracing::warn!("Failed joining maintenance project poll task: {:?}", e);
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
                        if let Ok(f) = crate::compaction::compact_project(&d, &project.hash, &cfg) {
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
            if let Err(e) = backup_res.unwrap_or_else(|e| Err(anyhow::anyhow!("{e:?}"))) {
                tracing::warn!("Database backup failed: {}", e);
            } else {
                if let Ok(entries) = std::fs::read_dir(&backups_dir) {
                    let mut files: Vec<_> = entries.flatten().collect();
                    files.sort_by_key(|e| e.file_name());
                    if files.len() > 14 {
                        let to_remove = files.len() - 14;
                        for e in files.iter().take(to_remove) {
                            let _ = std::fs::remove_file(e.path());
                        }
                    }
                }
                self.last_backup = Some(std::time::Instant::now());
            }
        }

        let _ = ctx.database.prune_ai_queue_ttl(7);

        let mut tick_sys = sysinfo::System::new();
        tick_sys.refresh_memory();
        if crate::ai::should_run_ai_with(&ctx.config.ai, &tick_sys) {
            process_ai_summary_queue(
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
                    let before =
                        crate::ai::sidecar_update::read_manifest(&sidecar_dir).map(|m| m.version);
                    match crate::ai::sidecar_update::run_update_check(
                        &sidecar_dir,
                        &repo,
                        pin.as_deref(),
                        || {
                            crate::ai::sidecar::shutdown_global_sidecar();
                        },
                    ) {
                        Ok(true) => {
                            let after = crate::ai::sidecar_update::read_manifest(&sidecar_dir)
                                .map(|m| m.version)
                                .unwrap_or_else(|| "unknown".to_string());
                            let _ =
                                event_tx.send(crate::server::events::ServerEvent::SidecarUpdated {
                                    old_version: before,
                                    new_version: after,
                                });
                        }
                        Ok(false) => {}
                        Err(e) => tracing::warn!("Sidecar update check failed: {}", e),
                    }
                });
            }
        }

        if let Err(e) = crate::ai::mlx_update::maybe_run_mlx_auto_update(
            &ctx.config.ai,
            &ctx.uhoh_dir,
            Some(&ctx.server_event_tx),
        )
        .await
        {
            let _ = ctx
                .server_event_tx
                .send(crate::server::events::ServerEvent::MlxUpdateStatus {
                    status: "failed".to_string(),
                    detail: e.to_string(),
                });
        }
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
        SubsystemHealth::Healthy
    }
}

// Removed duplicate is_uhoh_process_alive; use crate::platform::is_uhoh_process_alive instead

/// Spawn daemon as a detached background process.
pub fn spawn_detached_daemon() -> Result<()> {
    let exe = std::env::current_exe()?;

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let log_path = crate::uhoh_dir().join("daemon.log");
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
        let log_path = crate::uhoh_dir().join("daemon.log");
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

    let (server_event_tx, _) = broadcast::channel::<crate::server::events::ServerEvent>(4096);
    let restore_in_progress = Arc::new(AtomicBool::new(false));

    let event_ledger =
        EventLedger::new(database.clone()).with_server_event_tx(server_event_tx.clone());
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
            crate::server::start_server(
                &config.server,
                config.clone(),
                database.clone(),
                uhoh_dir.clone(),
                server_event_tx.clone(),
                restore_in_progress.clone(),
                subsystem_manager.clone(),
            )
            .await?,
        )
    } else {
        None
    };

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

    // Write PID file with exclusive lock to avoid races
    let pid_path = uhoh_dir.join("daemon.pid");
    let mut pid_file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&pid_path)?;
    {
        use fs4::FileExt;
        // If another daemon holds the lock, this will fail.
        // fs4 supports file locking on all platforms.
        pid_file
            .try_lock_exclusive()
            .context("Another uhoh daemon is already running (PID file locked)")?;
    }
    // Truncate and write PID
    #[cfg(unix)]
    {
        use std::io::Write as _;
        let _ = pid_file.set_len(0);
        let start_ticks =
            crate::platform::read_process_start_ticks(std::process::id()).unwrap_or(0);
        let record = format!("{} {}\n", std::process::id(), start_ticks);
        let f = &mut pid_file;
        let _ = f.write_all(record.as_bytes());
        let _ = f.sync_all();
    }
    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let f = &mut pid_file;
        let _ = f.set_len(0);
        let _ = f.seek(SeekFrom::Start(0));
        let start_ticks =
            crate::platform::read_process_start_ticks(std::process::id()).unwrap_or(0);
        let record = format!("{} {}\n", std::process::id(), start_ticks);
        let _ = f.write_all(record.as_bytes());
        let _ = f.sync_all();
    }

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

    // Channel for events from watcher (larger buffer to handle bursts)
    // Unbounded channel avoids blocking OS event thread under bursts
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<WatchEvent>();

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
    let mut project_states: HashMap<String, ProjectDaemonState> = HashMap::new();
    for project in projects {
        // Seed cached manifest and file count from the latest snapshot
        let (cached_count, cached_manifest) =
            if let Ok(Some(rowid)) = database.latest_snapshot_rowid(&project.hash) {
                if let Ok(Some(row)) = database.get_snapshot_by_rowid(rowid) {
                    let file_count = row.file_count;
                    let manifest: std::collections::BTreeSet<String> =
                        if let Ok(files) = database.get_snapshot_files(rowid) {
                            files.iter().map(|f| f.path.clone()).collect()
                        } else {
                            std::collections::BTreeSet::new()
                        };
                    (Some(file_count), Some(manifest))
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

        project_states.insert(
            project.current_path.clone(),
            ProjectDaemonState {
                hash: project.hash.clone(),
                last_snapshot: Instant::now() - Duration::from_secs(60),
                pending_changes: std::collections::HashSet::new(),
                first_change_at: None,
                last_change_at: None,
                deleted_paths: std::collections::HashSet::new(),
                last_emergency_at: None,
                cached_prev_file_count: cached_count,
                cached_prev_manifest: cached_manifest,
                overflow_occurred: false,
                restore_completed_at: None,
            },
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
                let currently_restoring = restore_in_progress.load(std::sync::atomic::Ordering::SeqCst)
                    || restore_marker_active(&uhoh_dir);
                // Detect restore completion (true → false transition)
                if was_restoring && !currently_restoring {
                    let now = Instant::now();
                    for state in project_states.values_mut() {
                        state.restore_completed_at = Some(now);
                        state.deleted_paths.clear();
                    }
                    tracing::debug!("Restore completed, grace period started for all projects");
                }
                was_restoring = currently_restoring;

                if currently_restoring {
                    tracing::debug!("Skipping watcher event during restore operation");
                    continue;
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
                                    let _ = tx.send(WatchEvent::WatcherDied);
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
                        handle_watch_event(&mut project_states, &other, &config);
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
                process_pending_snapshots(
                    &uhoh_dir,
                    database.clone(),
                    &mut project_states,
                    &config,
                    &server_event_tx,
                    &event_ledger,
                    &restore_in_progress,
                    &mut was_restoring,
                ).await;
            }
            _ = maintenance_interval.tick() => {
                let tick = run_tick_maintenance(
                    &config,
                    &config_path,
                    &update_trigger,
                    &database,
                    &event_ledger,
                    &subsystem_manager,
                    &uhoh_dir,
                    &server_event_tx,
                    &mut watcher_handle,
                    &mut project_states,
                ).await;

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

struct ProjectDaemonState {
    hash: String,
    last_snapshot: Instant,
    pending_changes: std::collections::HashSet<PathBuf>,
    first_change_at: Option<Instant>,
    last_change_at: Option<Instant>,

    // --- Emergency delete detection ---
    /// Deletion candidates observed during the current debounce window.
    deleted_paths: std::collections::HashSet<PathBuf>,
    /// Timestamp of last emergency snapshot for cooldown gating.
    last_emergency_at: Option<Instant>,
    /// Cached file count from the most recent snapshot (denominator for ratio).
    cached_prev_file_count: Option<u64>,
    /// Cached manifest paths from the most recent snapshot for verification.
    cached_prev_manifest: Option<std::collections::BTreeSet<String>>,
    /// Set when a watcher Overflow event occurs; suppresses emergency detection.
    overflow_occurred: bool,
    /// Timestamp when the most recent restore completed for this project.
    /// Used to implement a grace period after restore during which
    /// emergency detection is suppressed (prevents post-restore event storm).
    restore_completed_at: Option<Instant>,
}

struct TickOutcome {
    updated_config: Config,
    updated_debounce: Option<tokio::time::Interval>,
    updated_update_interval: Option<tokio::time::Interval>,
    should_restart_for_update: bool,
}

const MAX_DELETED_PATHS: usize = 100_000;
const RESTORE_IN_PROGRESS_FILE: &str = ".restore-in-progress";

fn restore_marker_active(uhoh_dir: &Path) -> bool {
    let marker_path = uhoh_dir.join(RESTORE_IN_PROGRESS_FILE);
    if !marker_path.exists() {
        return false;
    }

    const MAX_MARKER_AGE_SECS: u64 = 3600;
    if let Ok(meta) = std::fs::metadata(&marker_path) {
        if let Ok(modified) = meta.modified() {
            if modified
                .elapsed()
                .map(|d| d > Duration::from_secs(MAX_MARKER_AGE_SECS))
                .unwrap_or(false)
            {
                tracing::warn!("Removing stale restore marker: {}", marker_path.display());
                let _ = std::fs::remove_file(&marker_path);
                return false;
            }
        }
    }

    // If marker is stale (originating process died), remove it and treat as inactive.
    if let Ok(text) = std::fs::read_to_string(&marker_path) {
        let mut parseable = false;
        if let Some(pid_line) = text.lines().find(|l| l.starts_with("pid=")) {
            if let Ok(pid) = pid_line.trim_start_matches("pid=").trim().parse::<u32>() {
                parseable = true;
                // Use start_ticks from marker file for PID-reuse-safe check
                let expected_start = text
                    .lines()
                    .find(|l| l.starts_with("start_ticks="))
                    .and_then(|l| l.trim_start_matches("start_ticks=").trim().parse::<u64>().ok());
                if !crate::platform::is_uhoh_process_alive_with_start(pid, expected_start) {
                    let _ = std::fs::remove_file(&marker_path);
                    return false;
                }
            }
        }
        if !parseable {
            tracing::warn!("Removing corrupt restore marker: {}", marker_path.display());
            let _ = std::fs::remove_file(&marker_path);
            return false;
        }
    } else {
        tracing::warn!(
            "Removing unreadable restore marker: {}",
            marker_path.display()
        );
        let _ = std::fs::remove_file(&marker_path);
        return false;
    }

    true
}

fn handle_watch_event(
    states: &mut HashMap<String, ProjectDaemonState>,
    event: &WatchEvent,
    _config: &Config,
) {
    // Global overflow: mark all projects to rescan
    if let WatchEvent::Overflow = event {
        let now = Instant::now();
        for (project_path, state) in states.iter_mut() {
            state.pending_changes.insert(PathBuf::from(project_path));
            if state.first_change_at.is_none() {
                state.first_change_at = Some(now);
            }
            state.last_change_at = Some(now);
            // Mark overflow: event counts are unreliable, suppress emergency detection
            state.overflow_occurred = true;
        }
        return;
    }

    if let WatchEvent::WatcherDied = event {
        tracing::error!("File watcher died — attempting recovery on next tick");
        return;
    }

    let path = match event {
        WatchEvent::FileChanged(p) | WatchEvent::FileDeleted(p) | WatchEvent::Rescan(p) => p,
        _ => return,
    };

    let is_delete = matches!(event, WatchEvent::FileDeleted(_));

    // Filter out paths under the uhoh data directory to prevent self-monitoring loops
    let uhoh_data_dir = crate::uhoh_dir();
    if path.starts_with(&uhoh_data_dir) {
        return;
    }

    // Find which project this path belongs to (longest prefix match for nested projects)
    let best_key = states
        .keys()
        .filter(|pp| path.starts_with(pp.as_str()))
        .max_by_key(|pp| pp.len())
        .cloned();
    let Some(project_path_key) = best_key else {
        return;
    };
    let Some(state) = states.get_mut(&project_path_key) else {
        return;
    };
    let project_path = project_path_key.as_str();

    // Apply ignore rules to targeted file events
    if let Ok(rel) = path.strip_prefix(project_path) {
        let rel_str = rel.to_string_lossy();
        // Skip .git internals and .uhoh directory
        if rel_str.starts_with(".git/")
            || rel_str.starts_with(".git\\")
            || rel_str == ".git"
            || rel_str.starts_with(".uhoh/")
            || rel_str.starts_with(".uhoh\\")
            || rel_str == ".uhoh"
        {
            return;
        }
    }
    state.pending_changes.insert(path.clone());
    let now = Instant::now();
    if state.first_change_at.is_none() {
        state.first_change_at = Some(now);
    }
    state.last_change_at = Some(now);

    // Track deletions separately for emergency detection
    if is_delete && state.deleted_paths.len() < MAX_DELETED_PATHS {
        if let Some(ref manifest) = state.cached_prev_manifest {
            if let Ok(rel) = path.strip_prefix(project_path) {
                let rel_path = crate::cas::encode_relpath(rel);
                if manifest.contains(&rel_path) {
                    state.deleted_paths.insert(path.clone());
                } else {
                    let expanded = crate::emergency::expand_directory_deletion(&rel_path, manifest);
                    for exp_rel in expanded {
                        if state.deleted_paths.len() >= MAX_DELETED_PATHS {
                            break;
                        }
                        let exp_abs = PathBuf::from(project_path).join(&exp_rel);
                        state.deleted_paths.insert(exp_abs);
                    }
                }
            }
        } else {
            state.deleted_paths.insert(path.clone());
        }
    }
}

async fn run_tick_maintenance(
    config: &Config,
    config_path: &Path,
    update_trigger: &Path,
    database: &Arc<Database>,
    event_ledger: &EventLedger,
    subsystem_manager: &Arc<Mutex<SubsystemManager>>,
    uhoh_dir: &Path,
    server_event_tx: &broadcast::Sender<crate::server::events::ServerEvent>,
    watcher_handle: &mut notify::RecommendedWatcher,
    project_states: &mut HashMap<String, ProjectDaemonState>,
) -> TickOutcome {
    let mut next_config = config.clone();
    let mut next_debounce = None;
    let mut next_update = None;

    if let Ok(new_cfg) = Config::load(config_path) {
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

    let db_for_poll = database.clone();
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

    check_moved_folders(&db_projects, database, watcher_handle, project_states);

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

    check_for_new_projects(
        &db_projects,
        watcher_handle,
        project_states,
        server_event_tx,
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
            let _ = server_event_tx.send(crate::server::events::ServerEvent::ProjectRemoved {
                project_hash: state.hash.clone(),
            });
        }
        let _ = watcher_handle.unwatch(std::path::Path::new(&key));
        project_states.remove(&key);
        tracing::info!("Stopped watching removed project: {}", key);
    }

    let ctx = SubsystemContext {
        database: database.clone(),
        event_ledger: event_ledger.clone(),
        config: next_config.clone(),
        uhoh_dir: uhoh_dir.to_path_buf(),
        server_event_tx: server_event_tx.clone(),
    };
    subsystem_manager.lock().await.tick_restart(ctx).await;

    TickOutcome {
        updated_config: next_config,
        updated_debounce: next_debounce,
        updated_update_interval: next_update,
        should_restart_for_update: update_trigger.exists(),
    }
}

#[derive(Debug)]
enum SnapshotResult {
    Created(u64),
    NoChanges,
}

async fn process_pending_snapshots(
    uhoh_dir: &Path,
    database: Arc<Database>,
    states: &mut HashMap<String, ProjectDaemonState>,
    config: &Config,
    event_tx: &broadcast::Sender<crate::server::events::ServerEvent>,
    event_ledger: &EventLedger,
    restore_in_progress: &AtomicBool,
    was_restoring_snapshot: &mut bool,
) {
    let now = Instant::now();

    // Detect restore completion here too (not just in watcher handler), so that
    // if restore starts and ends between watcher events, grace period still fires.
    let currently_restoring = restore_in_progress.load(std::sync::atomic::Ordering::SeqCst)
        || restore_marker_active(uhoh_dir);
    if *was_restoring_snapshot && !currently_restoring {
        for state in states.values_mut() {
            state.restore_completed_at = Some(now);
            state.deleted_paths.clear();
        }
        tracing::debug!("Restore completed (detected in snapshot processor), grace period started");
    }
    *was_restoring_snapshot = currently_restoring;

    // Process multiple projects concurrently with a concurrency cap
    let logical = std::thread::available_parallelism().map_or(1, |n| n.get());
    let concurrency = std::cmp::max(1, (logical / 2).max(1));
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut join: tokio::task::JoinSet<(
        String,       // project_path key
        Vec<PathBuf>, // drained changes for requeue
        anyhow::Result<Option<SnapshotResult>>,
        bool, // was_emergency_spawn: true if dispatched as emergency
    )> = tokio::task::JoinSet::new();

    // Collect emergency snapshots to spawn (we can't borrow states mutably while iterating)
    let mut emergency_spawns: Vec<(String, String, PathBuf, String, Vec<PathBuf>)> = Vec::new();

    for (project_path, state) in states.iter_mut() {
        if state.pending_changes.is_empty() && state.deleted_paths.is_empty() {
            continue;
        }

        // ===== EMERGENCY EVALUATION (before normal debounce) =====
        let is_restoring = restore_in_progress.load(std::sync::atomic::Ordering::SeqCst)
            || restore_marker_active(uhoh_dir);

        // Post-restore grace period: suppress emergency detection for a short
        // window after restore completes to avoid false alarms from delayed
        // watcher events generated during the restore itself.
        const POST_RESTORE_GRACE_SECS: u64 = 10;
        let in_grace_period = state
            .restore_completed_at
            .map(|t| t.elapsed() < Duration::from_secs(POST_RESTORE_GRACE_SECS))
            .unwrap_or(false);

        if is_restoring {
            state.deleted_paths.clear();
            // Mark restore-in-progress so we can detect the transition
            state.restore_completed_at = None;
        } else if in_grace_period {
            // Restore just finished; discard stale deletion events only.
            // Keep pending_changes so legitimate post-restore edits still snapshot normally.
            state.deleted_paths.clear();
        } else if !state.deleted_paths.is_empty() {
            let hint_count = state.deleted_paths.len();

            let eval = crate::emergency::evaluate_emergency(
                hint_count,
                state.cached_prev_file_count,
                state.last_emergency_at,
                config.watch.emergency_cooldown_secs,
                config.watch.emergency_delete_threshold,
                config.watch.emergency_delete_min_files,
                is_restoring,
                state.overflow_occurred,
                Path::new(project_path),
                state.cached_prev_manifest.as_ref(),
            );

            match eval {
                crate::emergency::EmergencyEvaluation::Triggered {
                    verified_deleted_count,
                    baseline_count,
                    ratio,
                    deleted_paths_sample,
                } => {
                    let severity = crate::emergency::severity_for_ratio(ratio);
                    let message = format!(
                        "Mass delete detected: {}/{} files ({:.1}%)",
                        verified_deleted_count,
                        baseline_count,
                        ratio * 100.0
                    );

                    tracing::warn!(
                        project = %state.hash,
                        deleted = verified_deleted_count,
                        baseline = baseline_count,
                        ratio = %format!("{:.3}", ratio),
                        "Emergency delete detected: {}",
                        message
                    );

                    // Pin the predecessor snapshot (pre-deletion state)
                    if let Ok(Some(prev_rowid)) = database.latest_snapshot_rowid(&state.hash) {
                        if let Err(e) = database.pin_snapshot(prev_rowid, true) {
                            tracing::error!("Failed to pin predecessor snapshot: {}", e);
                        } else {
                            tracing::info!(
                                "Pinned predecessor snapshot (rowid={}) for recovery",
                                prev_rowid
                            );
                        }
                    }

                    // Emit ledger event
                    let detail = serde_json::json!({
                        "deleted_count": verified_deleted_count,
                        "baseline_count": baseline_count,
                        "ratio": ratio,
                        "threshold": config.watch.emergency_delete_threshold,
                        "min_files": config.watch.emergency_delete_min_files,
                        "cooldown_suppressed": false,
                        "deleted_paths_sample": deleted_paths_sample,
                    });
                    let mut ledger_event =
                        crate::event_ledger::new_event("fs", "emergency_delete_detected", severity);
                    ledger_event.project_hash = Some(state.hash.clone());
                    ledger_event.detail = Some(detail.to_string());
                    let _ = event_ledger.append(ledger_event);

                    // Broadcast server event
                    let _ = event_tx.send(
                        crate::server::events::ServerEvent::EmergencyDeleteDetected {
                            project_hash: state.hash.clone(),
                            deleted_count: verified_deleted_count,
                            baseline_count,
                            ratio,
                            threshold: config.watch.emergency_delete_threshold,
                            min_files: config.watch.emergency_delete_min_files,
                            cooldown_suppressed: false,
                            cooldown_remaining_secs: None,
                        },
                    );

                    // Drain pending changes for requeue on failure
                    let drained: Vec<PathBuf> = state.pending_changes.drain().collect();
                    state.deleted_paths.clear();
                    state.first_change_at = None;
                    state.last_change_at = None;
                    // Cooldown is set after snapshot succeeds (in join results below)
                    state.overflow_occurred = false;

                    // Collect for spawning after the loop (include drained for requeue)
                    emergency_spawns.push((
                        project_path.clone(),
                        state.hash.clone(),
                        Path::new(project_path).to_path_buf(),
                        message,
                        drained,
                    ));

                    continue; // Skip normal debounce for this project
                }

                crate::emergency::EmergencyEvaluation::CooldownSuppressed {
                    verified_deleted_count,
                    baseline_count,
                    ratio,
                    cooldown_remaining_secs,
                } => {
                    tracing::info!(
                        project = %state.hash,
                        deleted = verified_deleted_count,
                        baseline = baseline_count,
                        ratio = %format!("{:.3}", ratio),
                        cooldown_remaining = cooldown_remaining_secs,
                        "Emergency threshold exceeded but cooldown active"
                    );

                    let detail = serde_json::json!({
                        "deleted_count": verified_deleted_count,
                        "baseline_count": baseline_count,
                        "ratio": ratio,
                        "threshold": config.watch.emergency_delete_threshold,
                        "min_files": config.watch.emergency_delete_min_files,
                        "cooldown_suppressed": true,
                        "cooldown_remaining_secs": cooldown_remaining_secs,
                    });
                    let mut ledger_event =
                        crate::event_ledger::new_event("fs", "emergency_delete_detected", "info");
                    ledger_event.project_hash = Some(state.hash.clone());
                    ledger_event.detail = Some(detail.to_string());
                    let _ = event_ledger.append(ledger_event);

                    // Broadcast suppressed event too, so observability consumers can
                    // see cooldown behavior in real time.
                    let _ = event_tx.send(
                        crate::server::events::ServerEvent::EmergencyDeleteDetected {
                            project_hash: state.hash.clone(),
                            deleted_count: verified_deleted_count,
                            baseline_count,
                            ratio,
                            threshold: config.watch.emergency_delete_threshold,
                            min_files: config.watch.emergency_delete_min_files,
                            cooldown_suppressed: true,
                            cooldown_remaining_secs: Some(cooldown_remaining_secs),
                        },
                    );

                    // Don't clear deleted_paths on cooldown — let them accumulate
                    // Normal debounce proceeds below
                }

                crate::emergency::EmergencyEvaluation::Skipped { reason } => {
                    tracing::debug!(
                        project = %state.hash,
                        reason = reason,
                        "Emergency detection skipped"
                    );
                    if reason == "restore_in_progress" {
                        state.deleted_paths.clear();
                    }
                }

                crate::emergency::EmergencyEvaluation::NoEmergency => {
                    // Clear overflow flag so future emergency detection works
                    state.overflow_occurred = false;
                }
            }
        }

        // ===== NORMAL DEBOUNCE PATH =====

        if state.pending_changes.is_empty() {
            continue;
        }

        let first_change = match state.first_change_at {
            Some(t) => t,
            None => continue,
        };

        // Check debounce: quiet period elapsed OR max ceiling reached
        let last_change = state.last_change_at.unwrap_or(first_change);
        let since_last_change = now.duration_since(last_change);
        let since_first_change = now.duration_since(first_change);
        let since_last_snapshot = now.duration_since(state.last_snapshot);

        let quiet_elapsed =
            since_last_change >= Duration::from_secs(config.watch.debounce_quiet_secs);
        // Force snapshot after max_debounce_secs from the FIRST observed change
        let max_ceiling = since_first_change >= Duration::from_secs(config.watch.max_debounce_secs);
        let min_interval =
            since_last_snapshot >= Duration::from_secs(config.watch.min_snapshot_interval_secs);

        if (quiet_elapsed || max_ceiling) && min_interval {
            let uhoh_dir_buf = uhoh_dir.to_path_buf();
            let project_hash = state.hash.clone();
            let proj_path = Path::new(project_path).to_path_buf();
            let cfg = config.clone();
            let changed: Vec<PathBuf> = state.pending_changes.drain().collect();
            let changed_for_requeue = changed.clone();
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    tracing::warn!("Semaphore closed while scheduling snapshot task");
                    continue;
                }
            };
            // Move minimal state needed; clear after join completes below
            let db_for_task = database.clone();
            let project_path_key = project_path.clone();
            state.first_change_at = None;
            state.last_change_at = None;
            state.deleted_paths.clear(); // Reset for next window
            join.spawn(async move {
                let proj_hash_for_log = project_hash.clone();
                let res = tokio::task::spawn_blocking(move || {
                    match snapshot::create_snapshot(
                        &uhoh_dir_buf,
                        &db_for_task,
                        &project_hash,
                        &proj_path,
                        "auto",
                        None,
                        &cfg,
                        Some(&changed),
                    ) {
                        Ok(Some(id)) => Ok(Some(SnapshotResult::Created(id))),
                        Ok(None) => Ok(Some(SnapshotResult::NoChanges)),
                        Err(e) => Err(e),
                    }
                })
                .await;
                let task_result = match res {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(e)) => {
                        tracing::error!(
                            "Snapshot task failed for {}: {}",
                            &proj_hash_for_log[..proj_hash_for_log.len().min(12)],
                            e
                        );
                        Err(e)
                    }
                    Err(e) => {
                        tracing::error!("Snapshot task join error: {:?}", e);
                        Err(anyhow::anyhow!("Snapshot task join error: {e}"))
                    }
                };
                drop(permit);
                (project_path_key, changed_for_requeue, task_result, false)
            });
        }
    }

    // Spawn emergency snapshot tasks (full scan, trigger = "emergency")
    for (project_path_key, project_hash, proj_path, msg, drained_changes) in emergency_spawns {
        let uhoh_dir_buf = uhoh_dir.to_path_buf();
        let cfg = config.clone();
        let db_for_task = database.clone();
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!("Semaphore closed while scheduling emergency snapshot task");
                continue;
            }
        };
        let changed_for_requeue = drained_changes;
        join.spawn(async move {
            let proj_hash_for_log = project_hash.clone();
            let res = tokio::task::spawn_blocking(move || {
                match snapshot::create_snapshot(
                    &uhoh_dir_buf,
                    &db_for_task,
                    &project_hash,
                    &proj_path,
                    "emergency",
                    Some(&msg),
                    &cfg,
                    None, // Full scan for emergency
                ) {
                    Ok(Some(id)) => Ok(Some(SnapshotResult::Created(id))),
                    Ok(None) => Ok(Some(SnapshotResult::NoChanges)),
                    Err(e) => Err(e),
                }
            })
            .await;
            let task_result = match res {
                Ok(Ok(v)) => Ok(v),
                Ok(Err(e)) => {
                    tracing::error!(
                        "Emergency snapshot task failed for {}: {}",
                        &proj_hash_for_log[..proj_hash_for_log.len().min(12)],
                        e
                    );
                    Err(e)
                }
                Err(e) => {
                    tracing::error!("Emergency snapshot task join error: {:?}", e);
                    Err(anyhow::anyhow!("Emergency snapshot task join error: {e}"))
                }
            };
            drop(permit);
            (project_path_key, changed_for_requeue, task_result, true)
        });
    }

    while let Some(result) = join.join_next().await {
        match result {
            Ok((
                project_path,
                _drained,
                Ok(Some(SnapshotResult::Created(id))),
                was_emergency_spawn,
            )) => {
                if let Some(state) = states.get_mut(&project_path) {
                    state.last_snapshot = Instant::now();
                    // Set emergency cooldown on successful emergency snapshot
                    if was_emergency_spawn {
                        state.last_emergency_at = Some(Instant::now());
                    }
                    if let Ok(Some(rowid)) = database.latest_snapshot_rowid(&state.hash) {
                        if let Ok(Some(row)) = database.get_snapshot_by_rowid(rowid) {
                            // Check for dynamic trigger upgrade (safety net in snapshot.rs).
                            // Only emit events for dynamic upgrades (auto→emergency in snapshot.rs),
                            // NOT for daemon-triggered emergencies (which already emitted events
                            // before spawning). was_emergency_spawn distinguishes the two cases.
                            if row.trigger == "emergency" && !was_emergency_spawn {
                                state.last_emergency_at = Some(Instant::now());

                                let predecessor =
                                    database.snapshot_before(&state.hash, id).ok().flatten();

                                // Pin predecessor for recovery (the snapshot before this one)
                                if let Some(pred) = predecessor.as_ref() {
                                    let _ = database.pin_snapshot(pred.rowid, true);
                                    tracing::info!(
                                        "Pinned predecessor snapshot (rowid={}) via dynamic upgrade",
                                        pred.rowid
                                    );
                                }

                                // Derive event payload metrics from persisted snapshot data.
                                // Fallback to parsing message only if deleted rows query fails.
                                let del_count = database
                                    .get_snapshot_deleted_files(row.rowid)
                                    .map(|v| v.len())
                                    .unwrap_or_else(|_| {
                                        let (d, _, _) = parse_emergency_message(&row.message);
                                        d
                                    });
                                let baseline_from_predecessor =
                                    predecessor.as_ref().map(|p| p.file_count);
                                let baseline_from_row =
                                    row.file_count.saturating_add(del_count as u64);
                                let bl_count =
                                    baseline_from_predecessor.unwrap_or(baseline_from_row);
                                let ratio = crate::emergency::deletion_ratio(del_count, bl_count);
                                let _ = event_tx.send(
                                    crate::server::events::ServerEvent::EmergencyDeleteDetected {
                                        project_hash: state.hash.clone(),
                                        deleted_count: del_count,
                                        baseline_count: bl_count,
                                        ratio,
                                        threshold: config.watch.emergency_delete_threshold,
                                        min_files: config.watch.emergency_delete_min_files,
                                        cooldown_suppressed: false,
                                        cooldown_remaining_secs: None,
                                    },
                                );

                                // Emit ledger event
                                let severity = crate::emergency::severity_for_ratio(ratio);
                                let detail = serde_json::json!({
                                    "deleted_count": del_count,
                                    "baseline_count": bl_count,
                                    "ratio": ratio,
                                    "threshold": config.watch.emergency_delete_threshold,
                                    "min_files": config.watch.emergency_delete_min_files,
                                    "source": "dynamic_upgrade",
                                });
                                let mut ledger_event = crate::event_ledger::new_event(
                                    "fs",
                                    "emergency_delete_detected",
                                    severity,
                                );
                                ledger_event.project_hash = Some(state.hash.clone());
                                ledger_event.detail = Some(detail.to_string());
                                let _ = event_ledger.append(ledger_event);
                            }

                            let _ = event_tx.send(
                                crate::server::events::ServerEvent::SnapshotCreated {
                                    project_hash: state.hash.clone(),
                                    snapshot_id: crate::cas::id_to_base58(id),
                                    timestamp: row.timestamp,
                                    trigger: row.trigger,
                                    file_count: row.file_count as usize,
                                    message: if row.message.is_empty() {
                                        None
                                    } else {
                                        Some(row.message)
                                    },
                                },
                            );
                        }

                        // Refresh cached manifest from the new snapshot
                        if let Ok(files) = database.get_snapshot_files(rowid) {
                            state.cached_prev_manifest =
                                Some(files.iter().map(|f| f.path.clone()).collect());
                        }
                        if let Ok(Some(row)) = database.get_snapshot_by_rowid(rowid) {
                            state.cached_prev_file_count = Some(row.file_count);
                        }
                    }
                    state.deleted_paths.clear();
                    state.overflow_occurred = false;
                }
            }
            Ok((project_path, _drained, Ok(Some(SnapshotResult::NoChanges)), _)) => {
                if let Some(state) = states.get_mut(&project_path) {
                    state.last_snapshot = Instant::now();
                    state.deleted_paths.clear();
                    state.overflow_occurred = false;
                }
            }
            Ok((project_path, drained, Ok(None), _)) | Ok((project_path, drained, Err(_), _)) => {
                if let Some(state) = states.get_mut(&project_path) {
                    for p in drained {
                        state.pending_changes.insert(p);
                    }
                    let now = Instant::now();
                    state.first_change_at.get_or_insert(now);
                    state.last_change_at = Some(now);
                }
            }
            Err(e) => tracing::error!("Auto-snapshot task join failure: {:?}", e),
        }
    }
}

fn database_backup_to(database: &Database, path: &std::path::Path) -> anyhow::Result<()> {
    database.backup_to(path)
}

/// Process a small batch of deferred AI summary jobs when conditions allow.
async fn process_ai_summary_queue(
    uhoh_dir: &Path,
    database: &Database,
    config: &Config,
    event_tx: &broadcast::Sender<crate::server::events::ServerEvent>,
) {
    // Limit per tick to avoid overwhelming the sidecar
    const MAX_PER_TICK: u32 = 2;
    const MAX_ATTEMPTS: i64 = 5;
    if let Ok(jobs) = database.dequeue_pending_ai(MAX_PER_TICK) {
        for (rowid, project_hash, attempts, _queued_at) in jobs {
            if attempts >= MAX_ATTEMPTS {
                let _ = database.delete_pending_ai(rowid);
                continue;
            }

            // Rebuild a minimal diff for this snapshot
            // Fetch files of this snapshot and prior snapshot for the project
            let this = match database.get_snapshot_by_rowid(rowid) {
                Ok(Some(s)) => s,
                _ => {
                    let _ = database.delete_pending_ai(rowid);
                    continue;
                }
            };
            let prev = database
                .snapshot_before(&project_hash, this.snapshot_id)
                .unwrap_or_default();
            let this_files = match database.get_snapshot_files(this.rowid) {
                Ok(v) => v,
                Err(_) => {
                    let _ = database.increment_ai_attempts(rowid);
                    continue;
                }
            };
            let prev_files_vec = prev
                .as_ref()
                .and_then(|p| database.get_snapshot_files(p.rowid).ok())
                .unwrap_or_default();
            use std::collections::HashMap;
            let mut prev_map: HashMap<String, (String, bool, u64)> = HashMap::new();
            for f in prev_files_vec {
                prev_map.insert(f.path.clone(), (f.hash, f.stored, f.size));
            }

            let mut added = Vec::new();
            let mut modified = Vec::new();
            let mut deleted = Vec::new();
            let blob_root = uhoh_dir.join("blobs");
            let mut diff_chunks = String::new();
            const MAX_AI_DIFF_FILE_SIZE: u64 = 512 * 1024;
            let max_diff_chars = config.ai.max_context_tokens.saturating_mul(4);
            let mut diff_chars = 0usize;
            let mut diff_truncated = false;

            // Determine added/modified
            for f in &this_files {
                if let Some((prev_hash, prev_stored, prev_size)) = prev_map.get(&f.path) {
                    if &f.hash != prev_hash {
                        modified.push(f.path.clone());
                        if f.stored
                            && *prev_stored
                            && *prev_size <= MAX_AI_DIFF_FILE_SIZE
                            && f.size <= MAX_AI_DIFF_FILE_SIZE
                        {
                            let old = crate::cas::read_blob(&blob_root, prev_hash).ok().flatten();
                            let new = crate::cas::read_blob(&blob_root, &f.hash).ok().flatten();
                            if let (Some(old), Some(new)) = (old, new) {
                                let head_old = &old[..old.len().min(8192)];
                                let head_new = &new[..new.len().min(8192)];
                                if !(content_inspector::inspect(head_old).is_binary()
                                    || content_inspector::inspect(head_new).is_binary())
                                {
                                    if let (Ok(old_s), Ok(new_s)) =
                                        (String::from_utf8(old), String::from_utf8(new))
                                    {
                                        let d = similar::TextDiff::from_lines(&old_s, &new_s);
                                        append_ai_diff_chunk(
                                            &mut diff_chunks,
                                            &mut diff_chars,
                                            max_diff_chars,
                                            &mut diff_truncated,
                                            &format!("--- a/{}\n+++ b/{}\n", f.path, f.path),
                                        );
                                        if diff_truncated {
                                            break;
                                        }
                                        for hunk in d.unified_diff().context_radius(2).iter_hunks()
                                        {
                                            append_ai_diff_chunk(
                                                &mut diff_chunks,
                                                &mut diff_chars,
                                                max_diff_chars,
                                                &mut diff_truncated,
                                                &format!("{}\n", hunk.header()),
                                            );
                                            if diff_truncated {
                                                break;
                                            }
                                            for change in hunk.iter_changes() {
                                                let sign = match change.tag() {
                                                    similar::ChangeTag::Delete => '-',
                                                    similar::ChangeTag::Insert => '+',
                                                    similar::ChangeTag::Equal => ' ',
                                                };
                                                append_ai_diff_chunk(
                                                    &mut diff_chunks,
                                                    &mut diff_chars,
                                                    max_diff_chars,
                                                    &mut diff_truncated,
                                                    &format!("{}{}", sign, change),
                                                );
                                                if diff_truncated {
                                                    break;
                                                }
                                            }
                                            if diff_truncated {
                                                break;
                                            }
                                        }
                                    }
                                } else {
                                    append_ai_diff_chunk(
                                        &mut diff_chunks,
                                        &mut diff_chars,
                                        max_diff_chars,
                                        &mut diff_truncated,
                                        &format!("--- {}\n[Binary file]\n", f.path),
                                    );
                                }
                            }
                        }
                    }
                } else {
                    added.push(f.path.clone());
                }
                if diff_truncated {
                    break;
                }
            }
            // Determine deleted
            let this_map: std::collections::HashMap<String, ()> =
                this_files.iter().map(|f| (f.path.clone(), ())).collect();
            for (p, _, _, _) in prev_map
                .iter()
                .map(|(p, (h, s, sz))| (p.clone(), h.clone(), *s, *sz))
            {
                if !this_map.contains_key(&p) {
                    deleted.push(p);
                }
            }

            // Generate via sidecar
            let files = crate::ai::summary::FileChangeSummary {
                added: added.clone(),
                deleted: deleted.clone(),
                modified: modified.clone(),
            };
            let uhoh_dir_for_ai = uhoh_dir.to_path_buf();
            let cfg_for_ai = config.clone();
            let diff_for_ai = diff_chunks.clone();
            let files_for_ai = files.clone();
            let ai_result = tokio::task::spawn_blocking(move || {
                crate::ai::summary::generate_summary_blocking(
                    &uhoh_dir_for_ai,
                    &cfg_for_ai,
                    &diff_for_ai,
                    &files_for_ai,
                )
            })
            .await
            .unwrap_or_else(|e| Err(anyhow::anyhow!("AI summary task join error: {e}")));

            match ai_result {
                Ok(text) if !text.is_empty() => {
                    if let Err(e) = database.set_ai_summary(rowid, &text) {
                        tracing::warn!("Failed to set AI summary: {}", e);
                    } else {
                        let _ = database.delete_pending_ai(rowid);
                        if let Ok(Some(snapshot)) = database.get_snapshot_by_rowid(rowid) {
                            let _ = event_tx.send(
                                crate::server::events::ServerEvent::AiSummaryCompleted {
                                    project_hash: project_hash.clone(),
                                    snapshot_id: crate::cas::id_to_base58(snapshot.snapshot_id),
                                    summary: text,
                                },
                            );
                        }
                    }
                }
                Ok(_) => {
                    let _ = database.increment_ai_attempts(rowid);
                }
                Err(e) => {
                    tracing::debug!("Deferred AI summary failed: {}", e);
                    let _ = database.increment_ai_attempts(rowid);
                }
            }
        }
    }
}

fn append_ai_diff_chunk(
    out: &mut String,
    chars_used: &mut usize,
    max_chars: usize,
    truncated: &mut bool,
    chunk: &str,
) {
    if *truncated || chunk.is_empty() {
        return;
    }
    if *chars_used >= max_chars {
        out.push_str("\n[Diff truncated]\n");
        *truncated = true;
        return;
    }

    let remaining = max_chars.saturating_sub(*chars_used);
    if chunk.len() <= remaining {
        out.push_str(chunk);
        *chars_used = chars_used.saturating_add(chunk.len());
        return;
    }

    let mut cut = remaining;
    while cut > 0 && !chunk.is_char_boundary(cut) {
        cut -= 1;
    }
    if cut > 0 {
        out.push_str(&chunk[..cut]);
        *chars_used = chars_used.saturating_add(cut);
    }
    out.push_str("\n[Diff truncated]\n");
    *truncated = true;
}

fn check_moved_folders(
    projects: &[crate::db::ProjectEntry],
    database: &Database,
    watcher: &mut notify::RecommendedWatcher,
    states: &mut HashMap<String, ProjectDaemonState>,
) {
    use once_cell::sync::Lazy;
    use std::sync::Mutex;
    static FAILURES: Lazy<Mutex<std::collections::HashMap<String, u32>>> =
        Lazy::new(|| Mutex::new(std::collections::HashMap::new()));
    let mut failures = match FAILURES.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("moved-folder failure tracker mutex poisoned, recovering state");
            poisoned.into_inner()
        }
    };
    for project in projects {
        let path = Path::new(&project.current_path);
        if !path.exists() {
            // Try to relocate by scanning common parent directories for the .uhoh marker
            let count = failures.entry(project.hash.clone()).or_insert(0);
            *count = count.saturating_add(1);
            // Exponential backoff with cap: scan every ~20 ticks (10 min at 30s intervals)
            let max_backoff = 20u32;
            if *count > 20 && (*count % max_backoff) != 0 {
                continue;
            }

            let mut candidates: Vec<PathBuf> = Vec::new();
            if let Some(parent) = Path::new(&project.current_path).parent() {
                // Only scan immediate parent to limit scope
                let _ = std::fs::read_dir(parent).map(|iter| {
                    for e in iter.flatten() {
                        if e.file_type().is_ok_and(|ft| ft.is_dir()) {
                            candidates.push(e.path());
                        }
                    }
                });
            }
            let found = crate::marker::scan_for_markers(&candidates);
            for (hash, new_path) in found {
                if hash == project.hash {
                    if new_path.to_string_lossy() != project.current_path {
                        // Update watcher: unwatch old, watch new
                        let _ = watcher.unwatch(Path::new(&project.current_path));
                        let _ = watcher.watch(&new_path, RecursiveMode::Recursive);
                        // Update in-memory state key
                        if let Some(state) = states.remove(&project.current_path) {
                            states.insert(
                                new_path.to_string_lossy().to_string(),
                                ProjectDaemonState {
                                    last_change_at: None,
                                    ..state
                                },
                            );
                        }
                        let _ = database
                            .update_project_path(&project.hash, &new_path.to_string_lossy());
                        failures.remove(&project.hash);
                        tracing::info!(
                            "Relocated project {} -> {}",
                            &project.hash[..project.hash.len().min(12)],
                            new_path.display()
                        );
                    }
                    break;
                }
            }

            // If still not found, warn once per tick; also remove orphaned state
            if !Path::new(&project.current_path).exists() {
                tracing::warn!(
                    "Project {} path missing: {}",
                    &project.hash[..project.hash.len().min(12)],
                    project.current_path
                );
                // Remove from in-memory state to avoid crashes
                states.remove(&project.current_path);
            }
        }
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

/// Parse "Mass delete detected: X/Y files (Z%)" from the snapshot message
/// set by the dynamic trigger upgrade in snapshot.rs. Returns (deleted, baseline, ratio).
fn parse_emergency_message(msg: &str) -> (usize, u64, f64) {
    parse_emergency_message_opt(msg).unwrap_or((0, 0, 0.0))
}

fn parse_emergency_message_opt(msg: &str) -> Option<(usize, u64, f64)> {
    // Format: "Mass delete detected: 15/20 files (75.0%)"
    let after_colon = msg.strip_prefix("Mass delete detected: ")?;
    let slash_pos = after_colon.find('/')?;
    let deleted: usize = after_colon[..slash_pos].parse().ok()?;
    let rest = &after_colon[slash_pos + 1..];
    let space_pos = rest.find(' ')?;
    let baseline: u64 = rest[..space_pos].parse().ok()?;
    let ratio = if baseline > 0 {
        deleted as f64 / baseline as f64
    } else {
        0.0
    };
    Some((deleted, baseline, ratio))
}

fn check_for_new_projects(
    projects: &[crate::db::ProjectEntry],
    watcher: &mut notify::RecommendedWatcher,
    states: &mut HashMap<String, ProjectDaemonState>,
    event_tx: &broadcast::Sender<crate::server::events::ServerEvent>,
) {
    for p in projects {
        let key = p.current_path.clone();
        if let std::collections::hash_map::Entry::Vacant(e) = states.entry(key) {
            let path = PathBuf::from(&p.current_path);
            if path.exists() {
                if let Err(e) = watcher.watch(&path, RecursiveMode::Recursive) {
                    tracing::error!(
                        "Failed to watch project {}: {}. Changes won't be detected until next tick.",
                        path.display(), e
                    );
                    continue;
                } else {
                    e.insert(ProjectDaemonState {
                        hash: p.hash.clone(),
                        last_snapshot: Instant::now() - Duration::from_secs(60),
                        pending_changes: std::collections::HashSet::new(),
                        first_change_at: None,
                        last_change_at: None,
                        deleted_paths: std::collections::HashSet::new(),
                        last_emergency_at: None,
                        cached_prev_file_count: None,
                        cached_prev_manifest: None,
                        overflow_occurred: false,
                        restore_completed_at: None,
                    });
                    let _ = event_tx.send(crate::server::events::ServerEvent::ProjectAdded {
                        project_hash: p.hash.clone(),
                        path: p.current_path.clone(),
                    });
                    tracing::info!("Started watching new project: {}", path.display());
                }
            }
        }
    }
}
