use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
// use std::sync::Arc; // not used currently
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::config::Config;
use crate::db::Database;
use crate::snapshot;
use crate::watcher;
use notify::{RecursiveMode, Watcher as _};

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
                libc::setsid();
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
        const CREATE_NO_WINDOW: u32 = 0x08000000;
        const DETACHED_PROCESS: u32 = 0x00000008;
        std::process::Command::new(&exe)
            .args(["start", "--service"])
            .creation_flags(DETACHED_PROCESS | CREATE_NO_WINDOW)
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
        // On Windows, use taskkill
        std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/F"])
            .status()
            .ok();
    }

    std::fs::remove_file(&pid_path).ok();
    println!("Daemon stopped.");
    Ok(())
}

/// Run the daemon in the foreground (called with --service flag).
pub async fn run_foreground(uhoh_dir: &Path, database: &Database) -> Result<()> {
    let uhoh_dir = uhoh_dir.to_path_buf();
    let config = Config::load(&uhoh_dir.join("config.toml"))?;

    // Write PID file
    let pid_path = uhoh_dir.join("daemon.pid");
    std::fs::write(&pid_path, std::process::id().to_string())?;

    // Set up logging to file
    let log_path = uhoh_dir.join("daemon.log");
    tracing::info!("Daemon starting, PID={}, log={}", std::process::id(), log_path.display());

    // Check inotify watch limit on Linux
    #[cfg(target_os = "linux")]
    check_inotify_limit();

    // Channel for events from watcher
    let (event_tx, mut event_rx) = mpsc::channel::<WatchEvent>(1000);

    // Load all registered projects
    let projects = database.list_projects()?;

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
            move |res| { let _ = bin_notify_tx.send(res); },
            notify::Config::default(),
        ).ok();
        if let Some(ref mut w) = watcher {
            if let Some(parent) = exe.parent() { let _ = w.watch(parent, RecursiveMode::NonRecursive); }
            let _ = w.watch(&uhoh_dir, RecursiveMode::NonRecursive);
        }
        let exe_clone = exe.clone();
        let tx = bin_event_tx.clone();
        std::thread::Builder::new().name("bin-watcher-bridge".into()).spawn(move || {
            for result in bin_notify_rx {
                if let Ok(evt) = result {
                    let involves_binary = evt.paths.iter().any(|p| p == &exe_clone) ||
                        evt.paths.iter().any(|p| p.file_name().map_or(false, |n| n == ".update-ready"));
                    if involves_binary {
                        let _ = tx.send(());
                    }
                }
            }
        }).ok();
        watcher
    } else { None };

    // Per-project state
    let mut project_states: HashMap<String, ProjectDaemonState> = HashMap::new();
    for project in &projects {
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
        let mut sigterm = signal(SignalKind::terminate())?;
        let shutdown_tx2 = shutdown_tx.clone();
        tokio::spawn(async move {
            sigterm.recv().await;
            let _ = shutdown_tx2.send(()).await;
        });
    }

    // Main event loop
    let mut tick_interval = tokio::time::interval(Duration::from_secs(60));
    let mut update_check_interval = tokio::time::interval(Duration::from_secs(config.update.check_interval_hours * 3600));
    let mut debounce_interval = tokio::time::interval(Duration::from_millis(500));
    let update_trigger = uhoh_dir.join(".update-ready");

    tracing::info!("Daemon running, watching {} projects", projects.len());

    loop {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                match event {
                    WatchEvent::WatcherDied => {
                        tracing::error!("File watcher died — attempting recovery...");
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        let paths: Vec<PathBuf> = project_states
                            .keys()
                            .map(|k| PathBuf::from(k))
                            .collect();
                        match watcher::start_watching(&paths, event_tx.clone()) {
                            Ok(new_watcher) => {
                                watcher_handle = new_watcher;
                                tracing::info!("File watcher recovered successfully");
                            }
                            Err(e) => {
                                tracing::error!("Failed to recover file watcher: {}. Will retry on next tick.", e);
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
                    if let Some(ref exe) = exe_path {
                        let err = std::process::Command::new(exe).args(&args[1..]).exec();
                        anyhow::bail!("exec failed: {}", err);
                    }
                }
                #[cfg(windows)]
                {
                    let args: Vec<String> = std::env::args().collect();
                    if let Some(ref exe) = exe_path {
                        let _child = std::process::Command::new(exe)
                            .args(&args[1..])
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
                    database,
                    &mut project_states,
                    &config,
                ).await;
            }
            _ = tick_interval.tick() => {
                // Periodic tasks: check for moved folders, expire emergency snapshots
                check_moved_folders(database, &mut watcher_handle, &mut project_states);
                // Discover newly added projects and watch them
                check_for_new_projects(database, &mut watcher_handle, &mut project_states);
                // Attempt to recover watcher if it died earlier
                // Attempt watcher recovery only when we get WatcherDied events; here keep lightweight
                // If an update has been applied (trigger file present), exit gracefully so service manager can restart
                if update_trigger.exists() {
                    tracing::info!("Update ready trigger detected; stopping daemon for restart");
                    break;
                }
                let mut total_freed = 0u64;
                for project in &projects {
                    if let Ok(freed) = crate::compaction::compact_project(database, &project.hash, &config.compaction) {
                        total_freed = total_freed.saturating_add(freed);
                    }
                }
                if total_freed > 100 * 1024 * 1024 {
                    tracing::info!("Compaction estimated freed {:.1} MB; triggering GC", total_freed as f64 / 1_048_576.0);
                    let _ = crate::gc::run_gc(&uhoh_dir, database);
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
                break;
            }
        }
    }

    // Cleanup
    std::fs::remove_file(&pid_path).ok();
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
            if state.first_change_at.is_none() { state.first_change_at = Some(now); }
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
            state.pending_changes.insert(path.clone());
            let now = Instant::now();
            if state.first_change_at.is_none() { state.first_change_at = Some(now); }
            state.last_change_at = Some(now);
            break;
        }
    }
}

async fn process_pending_snapshots(
    uhoh_dir: &Path,
    _database: &Database,
    states: &mut HashMap<String, ProjectDaemonState>,
    config: &Config,
) {
    let now = Instant::now();

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

        let quiet_elapsed = since_last_change >= Duration::from_secs(config.watch.debounce_quiet_secs);
        // Force snapshot after max_debounce_secs from the FIRST observed change
        let max_ceiling = since_first_change >= Duration::from_secs(config.watch.max_debounce_secs);
        let min_interval = since_last_snapshot >= Duration::from_secs(config.watch.min_snapshot_interval_secs);

        if (quiet_elapsed || max_ceiling) && min_interval {
            // Create snapshot in blocking task to avoid blocking runtime
            let uhoh_dir_buf = uhoh_dir.to_path_buf();
            let db_path = uhoh_dir_buf.join("uhoh.db");
            let project_hash = state.hash.clone();
            let proj_path = Path::new(project_path).to_path_buf();
            let cfg = config.clone();
            let result = tokio::task::spawn_blocking(move || {
                let database = crate::db::Database::open(&db_path)?;
                snapshot::create_snapshot(
                    &uhoh_dir_buf,
                    &database,
                    &project_hash,
                    &proj_path,
                    "auto",
                    None,
                    &cfg,
                )
            }).await;

            match result.unwrap_or_else(|_| Ok(None)) {
                Ok(Some(_id)) => {
                    state.last_snapshot = now;
                    tracing::debug!("Auto-snapshot for {}", &state.hash[..state.hash.len().min(12)]);
                }
                Ok(None) => {
                    // No changes detected — clear pending state
                    state.pending_changes.clear();
                    state.first_change_at = None;
                    state.last_change_at = None;
                }
                Err(e) => {
                    // Snapshot failed — keep pending changes for retry
                    tracing::error!("Snapshot error for {}: {:?}", project_path, e);
                }
            }
        }
    }
}

fn check_moved_folders(
    database: &Database,
    watcher: &mut notify::RecommendedWatcher,
    states: &mut HashMap<String, ProjectDaemonState>,
) {
    if let Ok(projects) = database.list_projects() {
        for project in &projects {
            let path = Path::new(&project.current_path);
            if !path.exists() {
                // Try to relocate by scanning common parent directories for the .uhoh marker
                let mut candidates: Vec<PathBuf> = Vec::new();
                if let Some(parent) = Path::new(&project.current_path).parent() {
                    // Scan parent bounded to depth 3
                    use ignore::WalkBuilder;
                    let walker = WalkBuilder::new(parent)
                        .max_depth(Some(3))
                        .hidden(false)
                        .build();
                    let mut seen = std::collections::HashSet::new();
                    for entry in walker.flatten() {
                        let p = entry.path().to_path_buf();
                        if entry.file_type().map_or(false, |ft| ft.is_dir()) {
                            if seen.insert(p.clone()) {
                                candidates.push(p);
                            }
                        }
                    }
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
                                states.insert(new_path.to_string_lossy().to_string(), ProjectDaemonState { last_change_at: None, ..state });
                            }
                            let _ = database.update_project_path(&project.hash, &new_path.to_string_lossy());
                            tracing::info!("Relocated project {} -> {}", &project.hash[..project.hash.len().min(12)], new_path.display());
                        }
                        break;
                    }
                }

                // If still not found, warn once per tick
                if !Path::new(&project.current_path).exists() {
                    tracing::warn!(
                        "Project {} path missing: {}",
                        &project.hash[..project.hash.len().min(12)],
                        project.current_path
                    );
                }
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
    database: &Database,
    watcher: &mut notify::RecommendedWatcher,
    states: &mut HashMap<String, ProjectDaemonState>,
) {
    if let Ok(projects) = database.list_projects() {
        for p in projects {
            let key = p.current_path.clone();
            if !states.contains_key(&key) {
                let path = PathBuf::from(&p.current_path);
                if path.exists() {
                    if let Err(e) = watcher.watch(&path, RecursiveMode::Recursive) {
                        tracing::error!(
                            "Failed to watch project {}: {}. Changes won't be detected until next tick.",
                            path.display(), e
                        );
                        continue;
                    } else {
                        states.insert(
                            key,
                            ProjectDaemonState {
                                hash: p.hash,
                                last_snapshot: Instant::now() - Duration::from_secs(60),
                                pending_changes: std::collections::HashSet::new(),
                                first_change_at: None,
                                last_change_at: None,
                            },
                        );
                        tracing::info!("Started watching new project: {}", path.display());
                    }
                }
            }
        }
    }
}
