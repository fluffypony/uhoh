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
use crate::subsystem::{SubsystemContext, SubsystemManager};
use crate::watcher;
use notify::{RecursiveMode, Watcher as _};
// already imported above
use once_cell::sync::Lazy;
use std::sync::Mutex as StdMutex;

// Removed duplicate is_uhoh_process_alive; use crate::platform::is_uhoh_process_alive instead

/// Spawn daemon as a detached background process.
pub fn spawn_detached_daemon() -> Result<()> {
    let exe = std::env::current_exe()?;

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
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
        cmd.stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .stdin(std::process::Stdio::null());
        cmd.spawn().context("Failed to spawn daemon")?;
    }

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x00000008;
        std::process::Command::new(&exe)
            .args(["start", "--service"])
            .creation_flags(DETACHED_PROCESS)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
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
    let pid: u32 = pid_str.trim().parse().context("Invalid PID file")?;

    if !crate::platform::is_uhoh_process_alive(pid) {
        std::fs::remove_file(&pid_path).ok();
        println!("Daemon was not running (stale PID file cleaned up).");
        return Ok(());
    }

    #[cfg(unix)]
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }

    #[cfg(windows)]
    {
        // Try graceful termination first
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T"])
            .status();
        // Wait briefly for process to end
        for _ in 0..50 {
            if !crate::platform::is_uhoh_process_alive(pid) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if crate::platform::is_uhoh_process_alive(pid) {
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

    let event_ledger = EventLedger::new(database.clone());
    let mut subsystem_manager_inner = SubsystemManager::new(5, Duration::from_secs(600));
    subsystem_manager_inner.register(Box::new(crate::db_guard::DbGuardSubsystem::new()));
    subsystem_manager_inner.register(Box::new(crate::agent::AgentSubsystem::new()));
    let subsystem_manager = Arc::new(Mutex::new(subsystem_manager_inner));

    {
        let ctx = SubsystemContext {
            database: database.clone(),
            event_ledger: event_ledger.clone(),
            config: config.clone(),
            uhoh_dir: uhoh_dir.clone(),
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

    let mut last_sidecar_check: Option<std::time::Instant> = None;
    let mut sidecar_check_interval =
        std::time::Duration::from_secs(config.sidecar_update.check_interval_hours * 3600);

    #[cfg(windows)]
    let _daemon_mutex = {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;
        let name: Vec<u16> = OsStr::new("Global\\uhoh-daemon\0").encode_wide().collect();
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
    let pid_file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&pid_path)?;
    #[cfg(unix)]
    {
        use fs4::FileExt;
        // If another daemon holds the lock, this will fail
        pid_file.try_lock_exclusive()?;
    }
    // Truncate and write PID
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt as _;
        let _ = pid_file.set_len(0);
        let _ = pid_file.write_at(std::process::id().to_string().as_bytes(), 0);
    }
    #[cfg(not(unix))]
    {
        use fs4::FileExt;
        use std::io::{Seek, SeekFrom, Write};
        // Lock PID file on Windows too to prevent multiple daemons
        pid_file.try_lock_exclusive()?;
        let mut f = &pid_file;
        let _ = f.set_len(0);
        let _ = f.seek(SeekFrom::Start(0));
        let _ = f.write_all(std::process::id().to_string().as_bytes());
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
        project_states.insert(
            project.current_path.clone(),
            ProjectDaemonState {
                hash: project.hash.clone(),
                last_snapshot: Instant::now() - Duration::from_secs(60),
                pending_changes: std::collections::HashSet::new(),
                first_change_at: None,
                last_change_at: None,
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
    let mut tick_interval = tokio::time::interval(Duration::from_secs(60));
    let mut update_check_interval = tokio::time::interval(Duration::from_secs(
        config.update.check_interval_hours * 3600,
    ));
    let mut debounce_interval =
        tokio::time::interval(Duration::from_secs(config.watch.debounce_quiet_secs));
    let update_trigger = uhoh_dir.join(".update-ready");
    let mut compaction_index: usize = 0;

    tracing::info!("Daemon running, watching {} projects", projects.len());

    loop {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                if restore_in_progress.load(std::sync::atomic::Ordering::SeqCst) {
                    tracing::debug!("Skipping watcher event during restore operation");
                    continue;
                }
                match event {
                    WatchEvent::WatcherDied => {
                        let now = Instant::now();
                        if let Some(ready_at) = next_recover_at {
                            if now < ready_at {
                                tracing::warn!("Watcher died, backoff active — next attempt in {:?}", ready_at - now);
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
                        let err = std::process::Command::new(exe).args(rest).exec();
                        anyhow::bail!("exec failed: {err}");
                    }
                }
                #[cfg(windows)]
                {
                    let mut args: Vec<String> = std::env::args().collect();
                    let rest: &[String] = if args.len() > 1 { &args[1..] } else { &[] };
                    if let Some(pos) = args.iter().position(|a| a == "--takeover") {
                        // Remove flag and its PID argument if present
                        let _ = args.remove(pos);
                        if pos < args.len() { let _ = args.remove(pos); }
                    }
                    if let Some(ref exe) = exe_path {
                        let _child = std::process::Command::new(exe)
                            .args(rest)
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
                ).await;
            }
            _ = tick_interval.tick() => {
                // Live-reload configuration and update relevant timers
                if let Ok(new_cfg) = Config::load(&config_path) {
                    // Update live-safe fields
                    if new_cfg.watch.debounce_quiet_secs != config.watch.debounce_quiet_secs {
                        debounce_interval = tokio::time::interval(Duration::from_secs(new_cfg.watch.debounce_quiet_secs));
                    }
                    if new_cfg.update.check_interval_hours != config.update.check_interval_hours {
                        update_check_interval = tokio::time::interval(Duration::from_secs(new_cfg.update.check_interval_hours * 3600));
                    }
                    if new_cfg.sidecar_update.check_interval_hours != config.sidecar_update.check_interval_hours {
                        sidecar_check_interval = std::time::Duration::from_secs(new_cfg.sidecar_update.check_interval_hours * 3600);
                    }
                    config = new_cfg;
                }
                // Periodic DB polling runs in blocking task to avoid stalling the async loop.
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

                // Periodic tasks: check for moved folders and watcher registration.
                check_moved_folders(&db_projects, &database, &mut watcher_handle, &mut project_states);
                check_for_new_projects(&db_projects, &mut watcher_handle, &mut project_states, &server_event_tx);

                // Remove watchers and state for projects removed from DB
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
                // Attempt to recover watcher if it died earlier
                // Attempt watcher recovery only when we get WatcherDied events; here keep lightweight
                // If an update has been applied (trigger file present), restart like binary change
                if update_trigger.exists() {
                    tracing::info!("Update ready trigger detected; restarting daemon");
                    let _ = std::fs::remove_file(&update_trigger);
                    #[cfg(unix)]
                    {
                        use std::os::unix::process::CommandExt;
                        let args: Vec<String> = std::env::args().collect();
                        let rest: &[String] = if args.len() > 1 { &args[1..] } else { &[] };
                        if let Ok(exe) = std::env::current_exe() {
                            let err = std::process::Command::new(exe).args(rest).exec();
                            tracing::error!("exec failed: {}", err);
                        }
                    }
                    #[cfg(windows)]
                    {
                        if let Ok(exe) = std::env::current_exe() {
                            let mut args: Vec<String> = std::env::args().collect();
                            let rest: &[String] = if args.len() > 1 { &args[1..] } else { &[] };
                            if let Some(pos) = args.iter().position(|a| a == "--takeover") {
                                // Remove existing --takeover and its value if present
                                let _ = args.remove(pos);
                                if pos < args.len() { let _ = args.remove(pos); }
                            }
                            args.push("--takeover".into());
                            args.push(std::process::id().to_string());
                            let _ = std::process::Command::new(exe)
                                .args(rest)
                                .spawn();
                        }
                    }
                    break;
                }
                let _total_freed = 0u64; // unused placeholder; freed reported inside task
                let current_projects = db_projects;
                if !current_projects.is_empty() {
                    // Run compaction as a detached task; don't await inside main select loop
                    let db_path = uhoh_dir.join("uhoh.db");
                    let cfg = config.compaction.clone();
                    let local_compaction_index = compaction_index;
                    tokio::spawn({
                        let db_path_for_gc = db_path.clone();
                        async move {
                        let freed = tokio::task::spawn_blocking(move || {
                            let db = crate::db::Database::open(&db_path).ok();
                            let mut freed = 0u64;
                            if let Some(d) = db {
                                // Stagger compaction: run for a single project per tick to reduce contention
                                if !current_projects.is_empty() {
                                    let idx = local_compaction_index % current_projects.len();
                                    let project = &current_projects[idx];
                                    if let Ok(f) = crate::compaction::compact_project(&d, &project.hash, &cfg) { freed = freed.saturating_add(f); }
                                }
                            }
                            freed
                        }).await.unwrap_or(0);
                        if freed > 100 * 1024 * 1024 {
                            tracing::info!("Compaction estimated freed {:.1} MB; triggering GC", freed as f64 / 1_048_576.0);
                            let uhoh_dir_cl = db_path_for_gc.parent().unwrap_or_else(|| std::path::Path::new(".")).to_path_buf();
                        let db_path_for_gc2 = db_path_for_gc.clone();
                        let uhoh_dir_cl2 = uhoh_dir_cl.clone();
                        std::mem::drop(tokio::task::spawn_blocking(move || {
                            if let Ok(db) = crate::db::Database::open(&db_path_for_gc2) {
                                let _ = crate::gc::run_gc(&uhoh_dir_cl2, &db);
                                // After GC, VACUUM to reclaim free pages
                                let _ = db.vacuum();
                            }
                        }));
                            // Run VACUUM after large deletions to reclaim space
                            let db_path_for_gc3 = db_path_for_gc.clone();
                            let uhoh_dir_cl3 = uhoh_dir_cl.clone();
                            std::mem::drop(tokio::task::spawn_blocking(move || {
                                if let Ok(db) = crate::db::Database::open(&db_path_for_gc3) {
                                    let _ = db.backup_to(&uhoh_dir_cl3.join("vacuum.tmp")).ok();
                                }
                            }));
                        }
                    }});
                    compaction_index = compaction_index.wrapping_add(1);
                }
                // GC is handled within the detached task when needed

                // Periodic database backup (daily by default, tied to update.check_interval_hours)
                static LAST_BACKUP: Lazy<StdMutex<Option<std::time::Instant>>> = Lazy::new(|| StdMutex::new(None));
                let backup_interval = std::time::Duration::from_secs(config.update.check_interval_hours * 3600);
                let do_backup = {
                    let last = LAST_BACKUP.lock().unwrap();
                    last.map(|t| t.elapsed() >= backup_interval).unwrap_or(true)
                };
                if do_backup {
                    let backups_dir = uhoh_dir.join("backups");
                    let _ = std::fs::create_dir_all(&backups_dir);
                    let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S");
                    let backup_path = backups_dir.join(format!("uhoh-{ts}.db"));
                    let db_for_backup = database.clone();
                    let backup_path_cl = backup_path.clone();
                    let backup_res = tokio::task::spawn_blocking(move || database_backup_to(&db_for_backup, &backup_path_cl)).await;
                    if let Err(e) = backup_res.unwrap_or_else(|e| Err(anyhow::anyhow!("{e:?}"))) {
                        tracing::warn!("Database backup failed: {}", e);
                    } else {
                        // Best-effort rotation: keep last 14 backups
                        if let Ok(entries) = std::fs::read_dir(&backups_dir) {
                            let mut files: Vec<_> = entries.flatten().collect();
                            files.sort_by_key(|e| e.file_name());
                            if files.len() > 14 {
                                let to_remove = files.len() - 14;
                                for e in files.iter().take(to_remove) { let _ = std::fs::remove_file(e.path()); }
                            }
                        }
                        if let Ok(mut last) = LAST_BACKUP.lock() { *last = Some(std::time::Instant::now()); }
                    }
                }

                let _ = database.prune_ai_queue_ttl(7);

                // Process deferred AI summaries when conditions allow (cache sysinfo per tick)
                let mut tick_sys = sysinfo::System::new();
                tick_sys.refresh_memory();
                if crate::ai::should_run_ai_with(&config.ai, &tick_sys) {
                    process_ai_summary_queue(&uhoh_dir, &database, &config, &server_event_tx).await;
                }

                if config.sidecar_update.auto_update {
                    let should_check = match last_sidecar_check {
                        None => true,
                        Some(last) => last.elapsed() >= sidecar_check_interval,
                    };

                    if should_check {
                        last_sidecar_check = Some(std::time::Instant::now());
                        let sidecar_dir = uhoh_dir.join("sidecar");
                        let repo = config.sidecar_update.llama_repo.clone();
                        let pin = config.sidecar_update.pin_version.clone();
                        let event_tx = server_event_tx.clone();

                        tokio::task::spawn_blocking(move || {
                            let before = crate::ai::sidecar_update::read_manifest(&sidecar_dir)
                                .map(|m| m.version);
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
                                    let _ = event_tx.send(crate::server::events::ServerEvent::SidecarUpdated {
                                        old_version: before,
                                        new_version: after,
                                    });
                                }
                                Ok(false) => {}
                                Err(e) => tracing::warn!("Sidecar update check failed: {}", e),
                            }
                        });

                        if cfg!(target_os = "macos") && cfg!(target_arch = "aarch64") {
                            if let Some(version) = crate::ai::sidecar_update::check_mlx_lm_version() {
                                tracing::debug!("mlx_lm version: {}", version);
                            }
                        }
                    }
                }

                if let Err(e) = crate::ai::mlx_update::maybe_run_mlx_auto_update(&config.ai, &uhoh_dir, Some(&server_event_tx)).await {
                    let _ = server_event_tx.send(crate::server::events::ServerEvent::MlxUpdateStatus {
                        status: "failed".to_string(),
                        detail: e.to_string(),
                    });
                }

                {
                    let ctx = SubsystemContext {
                        database: database.clone(),
                        event_ledger: event_ledger.clone(),
                        config: config.clone(),
                        uhoh_dir: uhoh_dir.clone(),
                    };
                    subsystem_manager.lock().await.tick_restart(ctx).await;
                }
            }
            _ = update_check_interval.tick() => {
                let uhoh_dir_clone = uhoh_dir.clone();
                tokio::spawn(async move {
                    if let Err(e) = crate::update::check_and_apply_update(&uhoh_dir_clone).await {
                        tracing::debug!("Update check failed: {}", e);
                    }
                });
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

    // Cleanup
    std::fs::remove_file(&pid_path).ok();
    std::fs::remove_file(uhoh_dir.join("server.port")).ok();
    std::fs::remove_file(uhoh_dir.join("server.token")).ok();
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

    // Find which project this path belongs to
    for (project_path, state) in states.iter_mut() {
        if path.starts_with(project_path) {
            // Rely on ignore rules during snapshot walk; do not pre-filter here to avoid mismatches
            state.pending_changes.insert(path.clone());
            let now = Instant::now();
            if state.first_change_at.is_none() {
                state.first_change_at = Some(now);
            }
            state.last_change_at = Some(now);
            break;
        }
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
) {
    let now = Instant::now();

    // Process multiple projects concurrently with a concurrency cap
    let logical = std::thread::available_parallelism().map_or(1, |n| n.get());
    let concurrency = std::cmp::max(1, (logical / 2).max(1));
    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut join: tokio::task::JoinSet<(
        String,
        Vec<PathBuf>,
        anyhow::Result<Option<SnapshotResult>>,
    )> = tokio::task::JoinSet::new();
    for (project_path, state) in states.iter_mut() {
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
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            // Move minimal state needed; clear after join completes below
            let db_for_task = database.clone();
            let project_path_key = project_path.clone();
            state.first_change_at = None;
            state.last_change_at = None;
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
                (project_path_key, changed_for_requeue, task_result)
            });
        }
    }
    while let Some(result) = join.join_next().await {
        match result {
            Ok((project_path, _drained, Ok(Some(SnapshotResult::Created(id))))) => {
                if let Some(state) = states.get_mut(&project_path) {
                    state.last_snapshot = Instant::now();
                    if let Ok(Some(rowid)) = database.latest_snapshot_rowid(&state.hash) {
                        if let Ok(Some(row)) = database.get_snapshot_by_rowid(rowid) {
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
                    }
                }
            }
            Ok((project_path, _drained, Ok(Some(SnapshotResult::NoChanges)))) => {
                if let Some(state) = states.get_mut(&project_path) {
                    state.last_snapshot = Instant::now();
                }
            }
            Ok((project_path, drained, Ok(None))) | Ok((project_path, drained, Err(_))) => {
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
                                        diff_chunks.push_str(&format!(
                                            "--- a/{}\n+++ b/{}\n",
                                            f.path, f.path
                                        ));
                                        for hunk in d.unified_diff().context_radius(2).iter_hunks()
                                        {
                                            diff_chunks.push_str(&format!("{}\n", hunk.header()));
                                            for change in hunk.iter_changes() {
                                                let sign = match change.tag() {
                                                    similar::ChangeTag::Delete => '-',
                                                    similar::ChangeTag::Insert => '+',
                                                    similar::ChangeTag::Equal => ' ',
                                                };
                                                diff_chunks.push(sign);
                                                diff_chunks.push_str(&change.to_string());
                                            }
                                        }
                                    }
                                } else {
                                    diff_chunks
                                        .push_str(&format!("--- {}\n[Binary file]\n", f.path));
                                }
                            }
                        }
                    }
                } else {
                    added.push(f.path.clone());
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
            match crate::ai::summary::generate_summary_blocking(
                uhoh_dir,
                &config.clone(),
                &diff_chunks,
                &files,
            ) {
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
    let mut failures = FAILURES.lock().unwrap();
    for project in projects {
        let path = Path::new(&project.current_path);
        if !path.exists() {
            // Try to relocate by scanning common parent directories for the .uhoh marker
            let count = failures.entry(project.hash.clone()).or_insert(0);
            *count = count.saturating_add(1);
            // Exponential backoff: only scan on powers of two up to a cap
            if *count > 32 && (*count & (*count - 1)) != 0 {
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
