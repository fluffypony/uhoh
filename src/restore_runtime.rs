use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

use crate::db::{Database, ProjectEntry};
use crate::events::{publish_event, ServerEvent};
use crate::restore::{RestoreOutcome, RestoreRequest, RESTORE_IN_PROGRESS_FILE};
use crate::restore_guards::{RestoreFlagGuard, RestoreLockGuard};

#[derive(Clone, Default)]
pub struct RestoreCoordinator {
    in_progress: Arc<AtomicBool>,
    project_locks: Arc<Mutex<HashSet<String>>>,
}

impl RestoreCoordinator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn in_progress_flag(&self) -> Arc<AtomicBool> {
        self.in_progress.clone()
    }

    fn begin(&self, uhoh_dir: &Path, project_hash: &str, dry_run: bool) -> Result<RestoreGuards> {
        if dry_run {
            return Ok(RestoreGuards::default());
        }

        let project_lock =
            RestoreLockGuard::acquire(self.project_locks.clone(), project_hash.to_string())?;
        let in_progress = RestoreFlagGuard::acquire(self.in_progress.clone())?;
        let marker = RestoreMarkerGuard::install(uhoh_dir, project_hash)?;
        Ok(RestoreGuards {
            _project_lock: Some(project_lock),
            _in_progress: Some(in_progress),
            _marker: Some(marker),
        })
    }
}

#[derive(Clone)]
pub struct RestoreRuntime {
    pub database: Arc<Database>,
    pub uhoh_dir: PathBuf,
    pub event_tx: Option<broadcast::Sender<ServerEvent>>,
    pub coordinator: RestoreCoordinator,
}

impl RestoreRuntime {
    pub fn new(database: Arc<Database>, uhoh_dir: PathBuf) -> Self {
        Self {
            database,
            uhoh_dir,
            event_tx: None,
            coordinator: RestoreCoordinator::new(),
        }
    }

    pub fn with_event_tx(mut self, event_tx: broadcast::Sender<ServerEvent>) -> Self {
        self.event_tx = Some(event_tx);
        self
    }

    pub fn with_coordinator(mut self, coordinator: RestoreCoordinator) -> Self {
        self.coordinator = coordinator;
        self
    }
}

#[derive(Default)]
struct RestoreGuards {
    _project_lock: Option<RestoreLockGuard>,
    _in_progress: Option<RestoreFlagGuard>,
    _marker: Option<RestoreMarkerGuard>,
}

struct RestoreMarkerGuard {
    path: PathBuf,
}

impl RestoreMarkerGuard {
    fn install(uhoh_dir: &Path, project_hash: &str) -> Result<Self> {
        let path = uhoh_dir.join(RESTORE_IN_PROGRESS_FILE);
        let pid = std::process::id();
        let start_ticks = crate::platform::read_process_start_ticks(pid).unwrap_or(0);
        let payload =
            format!("project_hash={project_hash}\npid={pid}\nstart_ticks={start_ticks}\n");
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .and_then(|mut file| {
                use std::io::Write;
                file.write_all(payload.as_bytes())
            })
            .with_context(|| {
                format!(
                    "Failed to create restore marker (another restore may be in progress): {}",
                    path.display()
                )
            })?;
        Ok(Self { path })
    }
}

impl Drop for RestoreMarkerGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub fn restore_project(
    runtime: &RestoreRuntime,
    project: &ProjectEntry,
    request: RestoreRequest<'_>,
) -> Result<RestoreOutcome> {
    let _guards = runtime
        .coordinator
        .begin(&runtime.uhoh_dir, &project.hash, request.dry_run)?;
    let outcome =
        crate::restore::apply_restore(&runtime.uhoh_dir, &runtime.database, project, request)?;

    if !outcome.dry_run && outcome.applied {
        if let Some(event_tx) = &runtime.event_tx {
            publish_event(
                event_tx,
                ServerEvent::SnapshotRestored {
                    project_hash: project.hash.clone(),
                    snapshot_id: outcome.snapshot_id.clone(),
                    files_modified: outcome.files_restored,
                    files_deleted: outcome.files_deleted,
                },
            );
        }
    }

    Ok(outcome)
}

pub fn restore_marker_active(uhoh_dir: &Path) -> bool {
    let marker_path = uhoh_dir.join(RESTORE_IN_PROGRESS_FILE);
    if !marker_path.exists() {
        return false;
    }

    const MAX_MARKER_AGE_SECS: u64 = 3600;
    if let Ok(meta) = std::fs::metadata(&marker_path) {
        if let Ok(modified) = meta.modified() {
            if modified
                .elapsed()
                .map(|age| age > std::time::Duration::from_secs(MAX_MARKER_AGE_SECS))
                .unwrap_or(false)
            {
                tracing::warn!("Removing stale restore marker: {}", marker_path.display());
                let _ = std::fs::remove_file(&marker_path);
                return false;
            }
        }
    }

    if let Ok(text) = std::fs::read_to_string(&marker_path) {
        let mut parseable = false;
        if let Some(pid_line) = text.lines().find(|line| line.starts_with("pid=")) {
            if let Ok(pid) = pid_line.trim_start_matches("pid=").trim().parse::<u32>() {
                parseable = true;
                let expected_start = text
                    .lines()
                    .find(|line| line.starts_with("start_ticks="))
                    .and_then(|line| {
                        line.trim_start_matches("start_ticks=")
                            .trim()
                            .parse::<u64>()
                            .ok()
                    });
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

pub fn read_restoring_project_hash(uhoh_dir: &Path) -> Option<String> {
    let marker_path = uhoh_dir.join(RESTORE_IN_PROGRESS_FILE);
    let text = std::fs::read_to_string(&marker_path).ok()?;
    text.lines()
        .find(|line| line.starts_with("project_hash="))
        .map(|line| line.trim_start_matches("project_hash=").trim().to_string())
        .filter(|hash| !hash.is_empty())
}

pub fn is_restore_active(coordinator: &RestoreCoordinator, uhoh_dir: &Path) -> bool {
    coordinator.in_progress.load(Ordering::SeqCst) || restore_marker_active(uhoh_dir)
}
