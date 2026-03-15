use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use notify::{RecursiveMode, Watcher as _};
use tokio::sync::broadcast;

use crate::config::WatchConfig;
use crate::db::{Database, ProjectEntry};
use crate::event_ledger::EventLedger;
use crate::events::{publish_event, ServerEvent};
use crate::snapshot;

use super::watch_event::WatchEvent;

#[derive(Debug)]
enum SnapshotResult {
    Created(u64),
    NoChanges,
}

#[derive(Copy, Clone, Debug)]
pub(super) enum SnapshotTaskKind {
    Auto,
    Emergency,
}

impl SnapshotTaskKind {
    pub(super) fn trigger(self) -> crate::db::SnapshotTrigger {
        match self {
            SnapshotTaskKind::Auto => crate::db::SnapshotTrigger::Auto,
            SnapshotTaskKind::Emergency => crate::db::SnapshotTrigger::Emergency,
        }
    }

    pub(super) fn label(self) -> &'static str {
        match self {
            SnapshotTaskKind::Auto => "Snapshot",
            SnapshotTaskKind::Emergency => "Emergency snapshot",
        }
    }

    pub(super) fn is_emergency(self) -> bool {
        matches!(self, SnapshotTaskKind::Emergency)
    }
}

pub(super) struct SnapshotSpawnRequest {
    pub(super) project_path_key: String,
    pub(super) project_hash: String,
    pub(super) project_path: PathBuf,
    pub(super) changed_for_requeue: Vec<PathBuf>,
    pub(super) changed_paths: Option<Vec<PathBuf>>,
    pub(super) kind: SnapshotTaskKind,
    pub(super) message: Option<String>,
}

struct SnapshotTaskResult {
    project_path_key: String,
    changed_for_requeue: Vec<PathBuf>,
    result: anyhow::Result<Option<SnapshotResult>>,
    is_emergency: bool,
}

#[derive(Default)]
struct SnapshotExecutionPlan {
    auto_requests: Vec<SnapshotSpawnRequest>,
    emergency_requests: Vec<SnapshotSpawnRequest>,
}

struct SnapshotExecutionCtx<'a> {
    uhoh_dir: &'a Path,
    database: Arc<Database>,
    states: &'a mut HashMap<String, ProjectDaemonState>,
    snapshot_runtime: &'a snapshot::SnapshotRuntime,
    watch: &'a WatchConfig,
    event_tx: &'a broadcast::Sender<ServerEvent>,
    event_ledger: &'a EventLedger,
}

const MAX_DELETED_PATHS: usize = 100_000;

pub(super) struct ProjectDaemonState {
    pub(super) hash: String,
    pub(super) last_snapshot: Instant,
    pub(super) pending_changes: HashSet<PathBuf>,
    pub(super) first_change_at: Option<Instant>,
    pub(super) last_change_at: Option<Instant>,
    pub(super) deleted_paths: HashSet<PathBuf>,
    pub(super) cumulative_deletes: usize,
    pub(super) cumulative_window_start: Option<Instant>,
    pub(super) last_emergency_at: Option<Instant>,
    pub(super) cached_prev_file_count: Option<u64>,
    pub(super) cached_prev_manifest: Option<BTreeSet<String>>,
    pub(super) overflow_occurred: bool,
    pub(super) restore_completed_at: Option<Instant>,
}

#[derive(Debug, Default)]
pub(super) struct MovedFolderRetryState {
    attempts: HashMap<String, u32>,
}

pub(super) struct SnapshotProcessCtx<'a> {
    pub(super) uhoh_dir: &'a Path,
    pub(super) database: Arc<Database>,
    pub(super) states: &'a mut HashMap<String, ProjectDaemonState>,
    pub(super) snapshot_runtime: &'a snapshot::SnapshotRuntime,
    pub(super) event_tx: &'a broadcast::Sender<ServerEvent>,
    pub(super) event_ledger: &'a EventLedger,
    pub(super) restore_in_progress: &'a AtomicBool,
    pub(super) was_restoring_snapshot: &'a mut bool,
}

impl MovedFolderRetryState {
    fn note_missing(&mut self, project_hash: &str) -> u32 {
        let attempts = self.attempts.entry(project_hash.to_string()).or_insert(0);
        *attempts = attempts.saturating_add(1);
        *attempts
    }

    fn clear(&mut self, project_hash: &str) {
        self.attempts.remove(project_hash);
    }

    fn retain_projects(&mut self, projects: &[ProjectEntry]) {
        let active: HashSet<&str> = projects
            .iter()
            .map(|project| project.hash.as_str())
            .collect();
        self.attempts
            .retain(|project_hash, _| active.contains(project_hash.as_str()));
    }

    fn should_scan(attempts: u32) -> bool {
        attempts <= 20 || attempts % 20 == 0
    }
}

pub(super) fn seed_project_state(
    database: &Database,
    project: &ProjectEntry,
) -> ProjectDaemonState {
    let (cached_prev_file_count, cached_prev_manifest) =
        load_snapshot_cache(database, &project.hash);
    ProjectDaemonState {
        hash: project.hash.clone(),
        last_snapshot: Instant::now() - Duration::from_secs(60),
        pending_changes: HashSet::new(),
        first_change_at: None,
        last_change_at: None,
        deleted_paths: HashSet::new(),
        cumulative_deletes: 0,
        cumulative_window_start: None,
        last_emergency_at: None,
        cached_prev_file_count,
        cached_prev_manifest,
        overflow_occurred: false,
        restore_completed_at: None,
    }
}

pub(super) fn mark_restore_completed(
    states: &mut HashMap<String, ProjectDaemonState>,
    database: &Database,
    now: Instant,
) {
    for state in states.values_mut() {
        state.restore_completed_at = Some(now);
        state.deleted_paths.clear();
        refresh_cached_snapshot_state(database, state);
    }
}

pub(super) fn should_skip_event_during_restore(
    states: &HashMap<String, ProjectDaemonState>,
    event: &WatchEvent,
    restoring_hash: Option<&str>,
) -> bool {
    let event_path = match event {
        WatchEvent::FileChanged(path) | WatchEvent::FileDeleted(path) => Some(path),
        WatchEvent::Overflow | WatchEvent::WatcherDied => None,
    };
    let (Some(project_hash), Some(path)) = (restoring_hash, event_path) else {
        return true;
    };
    states.iter().any(|(project_path, state)| {
        state.hash == project_hash && path.starts_with(project_path.as_str())
    })
}

pub(super) fn handle_watch_event(
    states: &mut HashMap<String, ProjectDaemonState>,
    event: &WatchEvent,
    uhoh_dir: &Path,
) {
    if let WatchEvent::Overflow = event {
        let now = Instant::now();
        for (project_path, state) in states.iter_mut() {
            state.pending_changes.insert(PathBuf::from(project_path));
            if state.first_change_at.is_none() {
                state.first_change_at = Some(now);
            }
            state.last_change_at = Some(now);
            state.overflow_occurred = true;
        }
        return;
    }

    if let WatchEvent::WatcherDied = event {
        tracing::error!("File watcher died — attempting recovery on next tick");
        return;
    }

    let path = match event {
        WatchEvent::FileChanged(path) | WatchEvent::FileDeleted(path) => path,
        WatchEvent::Overflow | WatchEvent::WatcherDied => return,
    };
    let is_delete = matches!(event, WatchEvent::FileDeleted(_));

    if path.starts_with(uhoh_dir) {
        return;
    }

    let best_key = states
        .keys()
        .filter(|project_path| path.starts_with(project_path.as_str()))
        .max_by_key(|project_path| project_path.len())
        .cloned();
    let Some(project_path_key) = best_key else {
        return;
    };
    let Some(state) = states.get_mut(&project_path_key) else {
        return;
    };

    if let Ok(relative) = path.strip_prefix(project_path_key.as_str()) {
        let relative = relative.to_string_lossy();
        if relative.starts_with(".git/")
            || relative.starts_with(".git\\")
            || relative == ".git"
            || relative.starts_with(".uhoh/")
            || relative.starts_with(".uhoh\\")
            || relative == ".uhoh"
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

    if is_delete && state.deleted_paths.len() < MAX_DELETED_PATHS {
        let mut new_deletes = 0usize;
        if let Some(manifest) = state.cached_prev_manifest.as_ref() {
            if let Ok(relative) = path.strip_prefix(project_path_key.as_str()) {
                let rel_path = crate::encoding::encode_relpath(relative);
                if manifest.contains(&rel_path) {
                    if state.deleted_paths.insert(path.clone()) {
                        new_deletes += 1;
                    }
                } else {
                    let expanded = crate::emergency::expand_directory_deletion(&rel_path, manifest);
                    for expanded_relative in expanded {
                        if state.deleted_paths.len() >= MAX_DELETED_PATHS {
                            break;
                        }
                        let expanded_path =
                            PathBuf::from(project_path_key.as_str()).join(&expanded_relative);
                        if state.deleted_paths.insert(expanded_path) {
                            new_deletes += 1;
                        }
                    }
                }
            }
        } else if state.deleted_paths.insert(path.clone()) {
            new_deletes += 1;
        }

        if new_deletes > 0 {
            state.cumulative_deletes += new_deletes;
            if state.cumulative_window_start.is_none() {
                state.cumulative_window_start = Some(now);
            }
        }
    }
}

pub(super) async fn process_pending_snapshots(ctx: SnapshotProcessCtx<'_>) {
    let SnapshotProcessCtx {
        uhoh_dir,
        database,
        states,
        snapshot_runtime,
        event_tx,
        event_ledger,
        restore_in_progress,
        was_restoring_snapshot,
    } = ctx;
    let watch = &snapshot_runtime.settings().watch;

    let now = Instant::now();
    let currently_restoring = update_restore_state(
        uhoh_dir,
        database.as_ref(),
        states,
        restore_in_progress,
        was_restoring_snapshot,
        now,
    );

    let plan = build_snapshot_execution_plan(
        states,
        database.as_ref(),
        watch,
        event_tx,
        event_ledger,
        currently_restoring,
        now,
    );
    execute_snapshot_plan(
        SnapshotExecutionCtx {
            uhoh_dir,
            database,
            states,
            snapshot_runtime,
            watch,
            event_tx,
            event_ledger,
        },
        plan,
    )
    .await;
}

pub(super) fn check_moved_folders(
    projects: &[ProjectEntry],
    database: &Database,
    watcher: &mut notify::RecommendedWatcher,
    states: &mut HashMap<String, ProjectDaemonState>,
    retry_state: &mut MovedFolderRetryState,
) {
    retry_state.retain_projects(projects);

    for project in projects {
        let path = Path::new(&project.current_path);
        if path.exists() {
            retry_state.clear(&project.hash);
            continue;
        }

        let attempts = retry_state.note_missing(&project.hash);
        if !MovedFolderRetryState::should_scan(attempts) {
            continue;
        }

        let mut candidates = Vec::new();
        if let Some(parent) = Path::new(&project.current_path).parent() {
            let _ = std::fs::read_dir(parent).map(|entries| {
                for entry in entries.flatten() {
                    if entry.file_type().is_ok_and(|file_type| file_type.is_dir()) {
                        candidates.push(entry.path());
                    }
                }
            });
        }

        let found = crate::marker::scan_for_markers(&candidates);
        for (hash, new_path) in found {
            if hash != project.hash {
                continue;
            }

            if new_path.to_string_lossy() != project.current_path {
                let _ = watcher.unwatch(Path::new(&project.current_path));
                let _ = watcher.watch(&new_path, RecursiveMode::Recursive);
                if let Some(state) = states.remove(&project.current_path) {
                    states.insert(
                        new_path.to_string_lossy().to_string(),
                        ProjectDaemonState {
                            last_change_at: None,
                            ..state
                        },
                    );
                }
                let _ = database.update_project_path(&project.hash, &new_path.to_string_lossy());
                retry_state.clear(&project.hash);
                tracing::info!(
                    "Relocated project {} -> {}",
                    &project.hash[..project.hash.len().min(12)],
                    new_path.display()
                );
            }
            break;
        }

        if !Path::new(&project.current_path).exists() {
            tracing::warn!(
                "Project {} path missing: {}",
                &project.hash[..project.hash.len().min(12)],
                project.current_path
            );
            states.remove(&project.current_path);
        }
    }
}

fn build_snapshot_execution_plan(
    states: &mut HashMap<String, ProjectDaemonState>,
    database: &Database,
    watch: &WatchConfig,
    event_tx: &broadcast::Sender<ServerEvent>,
    event_ledger: &EventLedger,
    currently_restoring: bool,
    now: Instant,
) -> SnapshotExecutionPlan {
    let mut plan = SnapshotExecutionPlan::default();

    for (project_path, state) in states.iter_mut() {
        if state.pending_changes.is_empty() && state.deleted_paths.is_empty() {
            continue;
        }

        if let Some(request) = super::emergency::evaluate_emergency_for_project(
            project_path,
            state,
            database,
            watch,
            event_tx,
            event_ledger,
            currently_restoring,
            now,
        ) {
            plan.emergency_requests.push(request);
            continue;
        }

        if let Some(request) = build_auto_snapshot_request(project_path, state, watch, now) {
            plan.auto_requests.push(request);
        }
    }

    plan
}

async fn execute_snapshot_plan(ctx: SnapshotExecutionCtx<'_>, plan: SnapshotExecutionPlan) {
    let SnapshotExecutionCtx {
        uhoh_dir,
        database,
        states,
        snapshot_runtime,
        watch,
        event_tx,
        event_ledger,
    } = ctx;

    let logical = std::thread::available_parallelism().map_or(1, |n| n.get());
    let concurrency = std::cmp::max(1, (logical / 2).max(1));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut join: tokio::task::JoinSet<SnapshotTaskResult> = tokio::task::JoinSet::new();

    schedule_snapshot_requests(
        &mut join,
        semaphore.clone(),
        uhoh_dir,
        database.clone(),
        snapshot_runtime,
        plan.auto_requests,
    )
    .await;
    schedule_snapshot_requests(
        &mut join,
        semaphore,
        uhoh_dir,
        database.clone(),
        snapshot_runtime,
        plan.emergency_requests,
    )
    .await;

    while let Some(result) = join.join_next().await {
        match result {
            Ok(task_result) => apply_snapshot_result(
                states,
                database.as_ref(),
                watch,
                event_tx,
                event_ledger,
                task_result,
            ),
            Err(err) => tracing::error!("Auto-snapshot task join failure: {:?}", err),
        }
    }
}

async fn schedule_snapshot_requests(
    join: &mut tokio::task::JoinSet<SnapshotTaskResult>,
    semaphore: Arc<tokio::sync::Semaphore>,
    uhoh_dir: &Path,
    database: Arc<Database>,
    snapshot_runtime: &snapshot::SnapshotRuntime,
    requests: Vec<SnapshotSpawnRequest>,
) {
    for request in requests {
        spawn_snapshot_task(
            join,
            semaphore.clone(),
            uhoh_dir,
            database.clone(),
            snapshot_runtime,
            request,
        )
        .await;
    }
}

pub(super) fn check_for_new_projects(
    projects: &[ProjectEntry],
    watcher: &mut notify::RecommendedWatcher,
    states: &mut HashMap<String, ProjectDaemonState>,
    event_tx: &broadcast::Sender<ServerEvent>,
    database: &Database,
) {
    for project in projects {
        let key = project.current_path.clone();
        if let std::collections::hash_map::Entry::Vacant(entry) = states.entry(key) {
            let path = PathBuf::from(&project.current_path);
            if !path.exists() {
                continue;
            }

            if let Err(err) = watcher.watch(&path, RecursiveMode::Recursive) {
                tracing::error!(
                    "Failed to watch project {}: {}. Changes won't be detected until next tick.",
                    path.display(),
                    err
                );
                continue;
            }

            entry.insert(seed_project_state(database, project));
            publish_event(
                event_tx,
                ServerEvent::ProjectAdded {
                    project_hash: project.hash.clone(),
                    path: project.current_path.clone(),
                },
            );
            tracing::info!("Started watching new project: {}", path.display());
        }
    }
}

fn load_snapshot_cache(
    database: &Database,
    project_hash: &str,
) -> (Option<u64>, Option<BTreeSet<String>>) {
    if let Ok(Some(rowid)) = database.latest_snapshot_rowid(project_hash) {
        if let Ok(Some(row)) = database.get_snapshot_by_rowid(rowid) {
            let manifest = database
                .get_snapshot_files(rowid)
                .map(|files| files.into_iter().map(|file| file.path).collect())
                .unwrap_or_default();
            return (Some(row.file_count), Some(manifest));
        }
    }
    (None, None)
}

fn refresh_cached_snapshot_state(database: &Database, state: &mut ProjectDaemonState) {
    let (cached_prev_file_count, cached_prev_manifest) = load_snapshot_cache(database, &state.hash);
    state.cached_prev_file_count = cached_prev_file_count;
    state.cached_prev_manifest = cached_prev_manifest;
}

fn update_restore_state(
    uhoh_dir: &Path,
    database: &Database,
    states: &mut HashMap<String, ProjectDaemonState>,
    restore_in_progress: &AtomicBool,
    was_restoring_snapshot: &mut bool,
    now: Instant,
) -> bool {
    let currently_restoring = restore_in_progress.load(std::sync::atomic::Ordering::SeqCst)
        || crate::restore::restore_marker_active(uhoh_dir);
    if *was_restoring_snapshot && !currently_restoring {
        mark_restore_completed(states, database, now);
        tracing::debug!("Restore completed (detected in snapshot processor), grace period started");
    }
    *was_restoring_snapshot = currently_restoring;
    currently_restoring
}


fn build_auto_snapshot_request(
    project_path: &str,
    state: &mut ProjectDaemonState,
    watch: &WatchConfig,
    now: Instant,
) -> Option<SnapshotSpawnRequest> {
    if state.pending_changes.is_empty() {
        return None;
    }

    let first_change = state.first_change_at?;
    let last_change = state.last_change_at.unwrap_or(first_change);
    let quiet_elapsed =
        now.duration_since(last_change) >= Duration::from_secs(watch.debounce_quiet_secs);
    let max_ceiling =
        now.duration_since(first_change) >= Duration::from_secs(watch.max_debounce_secs);
    let min_interval = now.duration_since(state.last_snapshot)
        >= Duration::from_secs(watch.min_snapshot_interval_secs);

    if !(quiet_elapsed || max_ceiling) || !min_interval {
        return None;
    }

    let changed_paths: Vec<PathBuf> = state.pending_changes.drain().collect();
    let changed_for_requeue = changed_paths.clone();
    state.first_change_at = None;
    state.last_change_at = None;
    state.deleted_paths.clear();

    Some(SnapshotSpawnRequest {
        project_path_key: project_path.to_string(),
        project_hash: state.hash.clone(),
        project_path: PathBuf::from(project_path),
        changed_for_requeue,
        changed_paths: Some(changed_paths),
        kind: SnapshotTaskKind::Auto,
        message: None,
    })
}

async fn spawn_snapshot_task(
    join: &mut tokio::task::JoinSet<SnapshotTaskResult>,
    semaphore: Arc<tokio::sync::Semaphore>,
    uhoh_dir: &Path,
    database: Arc<Database>,
    snapshot_runtime: &snapshot::SnapshotRuntime,
    request: SnapshotSpawnRequest,
) {
    let Ok(permit) = semaphore.clone().acquire_owned().await else {
        tracing::warn!(
            "Semaphore closed while scheduling {} task",
            request.kind.trigger()
        );
        return;
    };

    let uhoh_dir = uhoh_dir.to_path_buf();
    let snapshot_runtime = snapshot_runtime.clone();
    let SnapshotSpawnRequest {
        project_path_key,
        project_hash,
        project_path,
        changed_for_requeue,
        changed_paths,
        kind,
        message,
    } = request;

    join.spawn(async move {
        let project_hash_for_log = project_hash.clone();
        let result = tokio::task::spawn_blocking(move || {
            match snapshot::create_snapshot(
                &uhoh_dir,
                &database,
                &snapshot_runtime,
                snapshot::CreateSnapshotRequest {
                    project_hash: &project_hash,
                    project_path: &project_path,
                    trigger: kind.trigger(),
                    message: message.as_deref(),
                    changed_paths: changed_paths.as_deref(),
                },
            ) {
                Ok(Some(id)) => Ok(Some(SnapshotResult::Created(id))),
                Ok(None) => Ok(Some(SnapshotResult::NoChanges)),
                Err(err) => Err(err),
            }
        })
        .await;

        let task_result = match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(err)) => {
                tracing::error!(
                    "{} task failed for {}: {}",
                    kind.label(),
                    &project_hash_for_log[..project_hash_for_log.len().min(12)],
                    err
                );
                Err(err)
            }
            Err(err) => {
                tracing::error!("{} task join error: {:?}", kind.label(), err);
                Err(anyhow::anyhow!("{} task join error: {err}", kind.label()))
            }
        };

        drop(permit);
        SnapshotTaskResult {
            project_path_key,
            changed_for_requeue,
            result: task_result,
            is_emergency: kind.is_emergency(),
        }
    });
}

fn apply_snapshot_result(
    states: &mut HashMap<String, ProjectDaemonState>,
    database: &Database,
    watch: &WatchConfig,
    event_tx: &broadcast::Sender<ServerEvent>,
    event_ledger: &EventLedger,
    task_result: SnapshotTaskResult,
) {
    let SnapshotTaskResult {
        project_path_key,
        changed_for_requeue,
        result,
        is_emergency,
    } = task_result;

    match result {
        Ok(Some(SnapshotResult::Created(snapshot_id))) => {
            if let Some(state) = states.get_mut(&project_path_key) {
                handle_created_snapshot(
                    state,
                    snapshot_id,
                    is_emergency,
                    database,
                    watch,
                    event_tx,
                    event_ledger,
                );
            }
        }
        Ok(Some(SnapshotResult::NoChanges)) => {
            if let Some(state) = states.get_mut(&project_path_key) {
                state.last_snapshot = Instant::now();
                state.deleted_paths.clear();
                state.overflow_occurred = false;
            }
        }
        Ok(None) | Err(_) => {
            if let Some(state) = states.get_mut(&project_path_key) {
                requeue_snapshot_changes(state, changed_for_requeue);
            }
        }
    }
}

fn handle_created_snapshot(
    state: &mut ProjectDaemonState,
    snapshot_id: u64,
    was_emergency_spawn: bool,
    database: &Database,
    watch: &WatchConfig,
    event_tx: &broadcast::Sender<ServerEvent>,
    event_ledger: &EventLedger,
) {
    state.last_snapshot = Instant::now();
    if was_emergency_spawn {
        state.last_emergency_at = Some(Instant::now());
    }

    if let Ok(Some(rowid)) = database.latest_snapshot_rowid(&state.hash) {
        if let Ok(Some(row)) = database.get_snapshot_by_rowid(rowid) {
            if row.trigger == crate::db::SnapshotTrigger::Emergency && !was_emergency_spawn {
                state.last_emergency_at = Some(Instant::now());
                super::emergency::handle_dynamic_emergency_upgrade(
                    database,
                    state,
                    snapshot_id,
                    &row,
                    watch,
                    event_tx,
                    event_ledger,
                );
            }

            publish_event(
                event_tx,
                ServerEvent::SnapshotCreated {
                    project_hash: state.hash.clone(),
                    snapshot_id: crate::encoding::id_to_base58(snapshot_id),
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

        refresh_cached_snapshot_state(database, state);
    }

    state.deleted_paths.clear();
    state.overflow_occurred = false;
}

fn requeue_snapshot_changes(state: &mut ProjectDaemonState, drained: Vec<PathBuf>) {
    for path in drained {
        state.pending_changes.insert(path);
    }
    let now = Instant::now();
    state.first_change_at.get_or_insert(now);
    state.last_change_at = Some(now);
}

#[cfg(test)]
mod tests {
    use super::MovedFolderRetryState;

    #[test]
    fn moved_folder_retry_state_backs_off_after_initial_burst() {
        let mut retry_state = MovedFolderRetryState::default();
        let mut should_scan = Vec::new();

        for _ in 0..40 {
            let attempts = retry_state.note_missing("project");
            should_scan.push(MovedFolderRetryState::should_scan(attempts));
        }

        assert!(should_scan.iter().take(20).all(|value| *value));
        assert!(!should_scan[20]);
        assert!(should_scan[39]);
    }

    #[test]
    fn moved_folder_retry_state_retain_projects_drops_stale_entries() {
        let mut retry_state = MovedFolderRetryState::default();
        retry_state.note_missing("keep");
        retry_state.note_missing("drop");

        retry_state.retain_projects(&[crate::db::ProjectEntry {
            hash: "keep".to_string(),
            current_path: "/tmp/keep".to_string(),
            created_at: "now".to_string(),
        }]);

        assert_eq!(retry_state.attempts.len(), 1);
        assert!(retry_state.attempts.contains_key("keep"));
    }
}
