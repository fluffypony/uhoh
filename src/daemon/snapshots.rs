use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use notify::{RecursiveMode, Watcher as _};
use tokio::sync::broadcast;

use crate::config::Config;
use crate::db::{Database, ProjectEntry, SnapshotRow};
use crate::event_ledger::{new_event, EventLedger};
use crate::events::{publish_event, ServerEvent};
use crate::snapshot;

use super::WatchEvent;

#[derive(Debug)]
enum SnapshotResult {
    Created(u64),
    NoChanges,
}

#[derive(Copy, Clone, Debug)]
enum SnapshotTaskKind {
    Auto,
    Emergency,
}

impl SnapshotTaskKind {
    fn trigger(self) -> &'static str {
        match self {
            SnapshotTaskKind::Auto => "auto",
            SnapshotTaskKind::Emergency => "emergency",
        }
    }

    fn label(self) -> &'static str {
        match self {
            SnapshotTaskKind::Auto => "Snapshot",
            SnapshotTaskKind::Emergency => "Emergency snapshot",
        }
    }

    fn is_emergency(self) -> bool {
        matches!(self, SnapshotTaskKind::Emergency)
    }
}

struct SnapshotSpawnRequest {
    project_path_key: String,
    project_hash: String,
    project_path: PathBuf,
    changed_for_requeue: Vec<PathBuf>,
    changed_paths: Option<Vec<PathBuf>>,
    kind: SnapshotTaskKind,
    message: Option<String>,
}

type SnapshotTaskResult = (
    String,
    Vec<PathBuf>,
    anyhow::Result<Option<SnapshotResult>>,
    bool,
);

const MAX_DELETED_PATHS: usize = 100_000;
const POST_RESTORE_GRACE_SECS: u64 = 10;

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

pub(super) struct SnapshotProcessCtx<'a> {
    pub(super) uhoh_dir: &'a Path,
    pub(super) database: Arc<Database>,
    pub(super) states: &'a mut HashMap<String, ProjectDaemonState>,
    pub(super) config: &'a Config,
    pub(super) event_tx: &'a broadcast::Sender<ServerEvent>,
    pub(super) event_ledger: &'a EventLedger,
    pub(super) restore_in_progress: &'a AtomicBool,
    pub(super) was_restoring_snapshot: &'a mut bool,
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
        WatchEvent::FileChanged(path)
        | WatchEvent::FileDeleted(path)
        | WatchEvent::Rescan(path) => Some(path),
        WatchEvent::Overflow | WatchEvent::WatcherDied => None,
    };
    let (Some(project_hash), Some(path)) = (restoring_hash, event_path) else {
        return true;
    };
    states
        .iter()
        .any(|(project_path, state)| state.hash == project_hash && path.starts_with(project_path))
}

pub(super) fn handle_watch_event(
    states: &mut HashMap<String, ProjectDaemonState>,
    event: &WatchEvent,
    _config: &Config,
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
        WatchEvent::FileChanged(path)
        | WatchEvent::FileDeleted(path)
        | WatchEvent::Rescan(path) => path,
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
                let rel_path = crate::cas::encode_relpath(relative);
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
        config,
        event_tx,
        event_ledger,
        restore_in_progress,
        was_restoring_snapshot,
    } = ctx;

    let now = Instant::now();
    let currently_restoring = update_restore_state(
        uhoh_dir,
        database.as_ref(),
        states,
        restore_in_progress,
        was_restoring_snapshot,
        now,
    );

    let logical = std::thread::available_parallelism().map_or(1, |n| n.get());
    let concurrency = std::cmp::max(1, (logical / 2).max(1));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut join: tokio::task::JoinSet<SnapshotTaskResult> = tokio::task::JoinSet::new();
    let mut emergency_spawns = Vec::new();

    for (project_path, state) in states.iter_mut() {
        if state.pending_changes.is_empty() && state.deleted_paths.is_empty() {
            continue;
        }

        if let Some(spawn) = evaluate_emergency_for_project(
            project_path,
            state,
            database.as_ref(),
            config,
            event_tx,
            event_ledger,
            currently_restoring,
            now,
        ) {
            emergency_spawns.push(spawn);
            continue;
        }

        if let Some(spawn) = build_auto_snapshot_request(project_path, state, config, now) {
            spawn_snapshot_task(
                &mut join,
                semaphore.clone(),
                uhoh_dir,
                database.clone(),
                config,
                spawn,
            )
            .await;
        }
    }

    for spawn in emergency_spawns {
        spawn_snapshot_task(
            &mut join,
            semaphore.clone(),
            uhoh_dir,
            database.clone(),
            config,
            spawn,
        )
        .await;
    }

    while let Some(result) = join.join_next().await {
        match result {
            Ok(task_result) => apply_snapshot_result(
                states,
                database.as_ref(),
                config,
                event_tx,
                event_ledger,
                task_result,
            ),
            Err(err) => tracing::error!("Auto-snapshot task join failure: {:?}", err),
        }
    }
}

pub(super) fn check_moved_folders(
    projects: &[ProjectEntry],
    database: &Database,
    watcher: &mut notify::RecommendedWatcher,
    states: &mut HashMap<String, ProjectDaemonState>,
) {
    use once_cell::sync::Lazy;
    use std::sync::Mutex;

    static FAILURES: Lazy<Mutex<HashMap<String, u32>>> = Lazy::new(|| Mutex::new(HashMap::new()));

    let mut failures = match FAILURES.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("moved-folder failure tracker mutex poisoned, recovering state");
            poisoned.into_inner()
        }
    };

    for project in projects {
        let path = Path::new(&project.current_path);
        if path.exists() {
            continue;
        }

        let count = failures.entry(project.hash.clone()).or_insert(0);
        *count = count.saturating_add(1);
        let max_backoff = 20u32;
        if *count > 20 && (*count % max_backoff) != 0 {
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
                failures.remove(&project.hash);
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
        || crate::restore_runtime::restore_marker_active(uhoh_dir);
    if *was_restoring_snapshot && !currently_restoring {
        mark_restore_completed(states, database, now);
        tracing::debug!("Restore completed (detected in snapshot processor), grace period started");
    }
    *was_restoring_snapshot = currently_restoring;
    currently_restoring
}

fn evaluate_emergency_for_project(
    project_path: &str,
    state: &mut ProjectDaemonState,
    database: &Database,
    config: &Config,
    event_tx: &broadcast::Sender<ServerEvent>,
    event_ledger: &EventLedger,
    currently_restoring: bool,
    now: Instant,
) -> Option<SnapshotSpawnRequest> {
    if currently_restoring {
        state.deleted_paths.clear();
        state.cumulative_deletes = 0;
        state.cumulative_window_start = None;
        state.restore_completed_at = None;
        return None;
    }

    let in_grace_period = state
        .restore_completed_at
        .map(|completed_at| completed_at.elapsed() < Duration::from_secs(POST_RESTORE_GRACE_SECS))
        .unwrap_or(false);
    if in_grace_period {
        state.deleted_paths.clear();
        state.cumulative_deletes = 0;
        state.cumulative_window_start = None;
        return None;
    }

    if state.deleted_paths.is_empty() {
        return None;
    }

    if let Some(window_start) = state.cumulative_window_start {
        if window_start.elapsed() > Duration::from_secs(config.watch.emergency_cooldown_secs) {
            state.cumulative_deletes = state.deleted_paths.len();
            state.cumulative_window_start = Some(now);
        }
    }

    let deleted_paths_hint_count = state.cumulative_deletes.max(state.deleted_paths.len());
    let evaluation = crate::emergency::evaluate_emergency(crate::emergency::EmergencyEvalInput {
        deleted_paths_hint_count,
        cached_baseline_count: state.cached_prev_file_count,
        last_emergency_at: state.last_emergency_at,
        cooldown_secs: config.watch.emergency_cooldown_secs,
        threshold: config.watch.emergency_delete_threshold,
        min_files: config.watch.emergency_delete_min_files,
        restore_in_progress: currently_restoring,
        overflow_occurred: state.overflow_occurred,
        project_root: Path::new(project_path),
        cached_manifest: state.cached_prev_manifest.as_ref(),
    });

    match evaluation {
        crate::emergency::EmergencyEvaluation::Triggered {
            verified_deleted_count,
            baseline_count,
            ratio,
            deleted_paths_sample,
        } => {
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

            if let Ok(Some(previous_rowid)) = database.latest_snapshot_rowid(&state.hash) {
                if let Err(err) = database.pin_snapshot(previous_rowid, true) {
                    tracing::error!("Failed to pin predecessor snapshot: {}", err);
                } else {
                    tracing::info!(
                        "Pinned predecessor snapshot (rowid={}) for recovery",
                        previous_rowid
                    );
                }
            }

            emit_emergency_delete_detected(
                event_tx,
                event_ledger,
                &state.hash,
                config,
                verified_deleted_count,
                baseline_count,
                ratio,
                false,
                None,
                Some(serde_json::json!({ "deleted_paths_sample": deleted_paths_sample })),
            );

            let changed_for_requeue = state.pending_changes.drain().collect();
            state.deleted_paths.clear();
            state.cumulative_deletes = 0;
            state.cumulative_window_start = None;
            state.first_change_at = None;
            state.last_change_at = None;
            state.overflow_occurred = false;

            Some(SnapshotSpawnRequest {
                project_path_key: project_path.to_string(),
                project_hash: state.hash.clone(),
                project_path: PathBuf::from(project_path),
                changed_for_requeue,
                changed_paths: None,
                kind: SnapshotTaskKind::Emergency,
                message: Some(message),
            })
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
            emit_emergency_delete_detected(
                event_tx,
                event_ledger,
                &state.hash,
                config,
                verified_deleted_count,
                baseline_count,
                ratio,
                true,
                Some(cooldown_remaining_secs),
                None,
            );
            None
        }
        crate::emergency::EmergencyEvaluation::Skipped { reason } => {
            tracing::debug!(project = %state.hash, reason = reason, "Emergency detection skipped");
            if reason == "restore_in_progress" {
                state.deleted_paths.clear();
            }
            None
        }
        crate::emergency::EmergencyEvaluation::NoEmergency => {
            state.overflow_occurred = false;
            None
        }
    }
}

fn emit_emergency_delete_detected(
    event_tx: &broadcast::Sender<ServerEvent>,
    event_ledger: &EventLedger,
    project_hash: &str,
    config: &Config,
    deleted_count: usize,
    baseline_count: u64,
    ratio: f64,
    cooldown_suppressed: bool,
    cooldown_remaining_secs: Option<u64>,
    extra_detail: Option<serde_json::Value>,
) {
    let mut detail = serde_json::json!({
        "deleted_count": deleted_count,
        "baseline_count": baseline_count,
        "ratio": ratio,
        "threshold": config.watch.emergency_delete_threshold,
        "min_files": config.watch.emergency_delete_min_files,
        "cooldown_suppressed": cooldown_suppressed,
    });
    if let Some(remaining_secs) = cooldown_remaining_secs {
        detail["cooldown_remaining_secs"] = serde_json::json!(remaining_secs);
    }
    if let Some(extra) = extra_detail {
        if let Some(extra_map) = extra.as_object() {
            for (key, value) in extra_map {
                detail[key] = value.clone();
            }
        }
    }

    let severity = if cooldown_suppressed {
        "info"
    } else {
        crate::emergency::severity_for_ratio(ratio)
    };
    let mut ledger_event = new_event("fs", "emergency_delete_detected", severity);
    ledger_event.project_hash = Some(project_hash.to_string());
    ledger_event.detail = Some(detail.to_string());
    let _ = event_ledger.append(ledger_event);

    publish_event(
        event_tx,
        ServerEvent::EmergencyDeleteDetected {
            project_hash: project_hash.to_string(),
            deleted_count,
            baseline_count,
            ratio,
            threshold: config.watch.emergency_delete_threshold,
            min_files: config.watch.emergency_delete_min_files,
            cooldown_suppressed,
            cooldown_remaining_secs,
        },
    );
}

fn build_auto_snapshot_request(
    project_path: &str,
    state: &mut ProjectDaemonState,
    config: &Config,
    now: Instant,
) -> Option<SnapshotSpawnRequest> {
    if state.pending_changes.is_empty() {
        return None;
    }

    let first_change = state.first_change_at?;
    let last_change = state.last_change_at.unwrap_or(first_change);
    let quiet_elapsed =
        now.duration_since(last_change) >= Duration::from_secs(config.watch.debounce_quiet_secs);
    let max_ceiling =
        now.duration_since(first_change) >= Duration::from_secs(config.watch.max_debounce_secs);
    let min_interval = now.duration_since(state.last_snapshot)
        >= Duration::from_secs(config.watch.min_snapshot_interval_secs);

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
    config: &Config,
    request: SnapshotSpawnRequest,
) {
    let permit = match semaphore.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => {
            tracing::warn!(
                "Semaphore closed while scheduling {} task",
                request.kind.trigger()
            );
            return;
        }
    };

    let uhoh_dir = uhoh_dir.to_path_buf();
    let config = config.clone();
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
                &config,
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
        (
            project_path_key,
            changed_for_requeue,
            task_result,
            kind.is_emergency(),
        )
    });
}

fn apply_snapshot_result(
    states: &mut HashMap<String, ProjectDaemonState>,
    database: &Database,
    config: &Config,
    event_tx: &broadcast::Sender<ServerEvent>,
    event_ledger: &EventLedger,
    task_result: SnapshotTaskResult,
) {
    match task_result {
        (project_path, _, Ok(Some(SnapshotResult::Created(snapshot_id))), was_emergency_spawn) => {
            if let Some(state) = states.get_mut(&project_path) {
                handle_created_snapshot(
                    state,
                    snapshot_id,
                    was_emergency_spawn,
                    database,
                    config,
                    event_tx,
                    event_ledger,
                );
            }
        }
        (project_path, _, Ok(Some(SnapshotResult::NoChanges)), _) => {
            if let Some(state) = states.get_mut(&project_path) {
                state.last_snapshot = Instant::now();
                state.deleted_paths.clear();
                state.overflow_occurred = false;
            }
        }
        (project_path, drained, Ok(None), _) | (project_path, drained, Err(_), _) => {
            if let Some(state) = states.get_mut(&project_path) {
                requeue_snapshot_changes(state, drained);
            }
        }
    }
}

fn handle_created_snapshot(
    state: &mut ProjectDaemonState,
    snapshot_id: u64,
    was_emergency_spawn: bool,
    database: &Database,
    config: &Config,
    event_tx: &broadcast::Sender<ServerEvent>,
    event_ledger: &EventLedger,
) {
    state.last_snapshot = Instant::now();
    if was_emergency_spawn {
        state.last_emergency_at = Some(Instant::now());
    }

    if let Ok(Some(rowid)) = database.latest_snapshot_rowid(&state.hash) {
        if let Ok(Some(row)) = database.get_snapshot_by_rowid(rowid) {
            if row.trigger == "emergency" && !was_emergency_spawn {
                state.last_emergency_at = Some(Instant::now());
                handle_dynamic_emergency_upgrade(
                    database,
                    state,
                    snapshot_id,
                    &row,
                    config,
                    event_tx,
                    event_ledger,
                );
            }

            publish_event(
                event_tx,
                ServerEvent::SnapshotCreated {
                    project_hash: state.hash.clone(),
                    snapshot_id: crate::cas::id_to_base58(snapshot_id),
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

fn handle_dynamic_emergency_upgrade(
    database: &Database,
    state: &ProjectDaemonState,
    snapshot_id: u64,
    row: &SnapshotRow,
    config: &Config,
    event_tx: &broadcast::Sender<ServerEvent>,
    event_ledger: &EventLedger,
) {
    let predecessor = database
        .snapshot_before(&state.hash, snapshot_id)
        .ok()
        .flatten();
    if let Some(predecessor) = predecessor.as_ref() {
        let _ = database.pin_snapshot(predecessor.rowid, true);
        tracing::info!(
            "Pinned predecessor snapshot (rowid={}) via dynamic upgrade",
            predecessor.rowid
        );
    }

    let deleted_count = database
        .get_snapshot_deleted_files(row.rowid)
        .map(|files| files.len())
        .unwrap_or_else(|_| {
            let (deleted, _, _) = parse_emergency_message(&row.message);
            deleted
        });
    let baseline_from_predecessor = predecessor.as_ref().map(|snapshot| snapshot.file_count);
    let baseline_from_row = row.file_count.saturating_add(deleted_count as u64);
    let baseline_count = baseline_from_predecessor.unwrap_or(baseline_from_row);
    let ratio = crate::emergency::deletion_ratio(deleted_count, baseline_count);

    emit_emergency_delete_detected(
        event_tx,
        event_ledger,
        &state.hash,
        config,
        deleted_count,
        baseline_count,
        ratio,
        false,
        None,
        Some(serde_json::json!({ "source": "dynamic_upgrade" })),
    );
}

fn requeue_snapshot_changes(state: &mut ProjectDaemonState, drained: Vec<PathBuf>) {
    for path in drained {
        state.pending_changes.insert(path);
    }
    let now = Instant::now();
    state.first_change_at.get_or_insert(now);
    state.last_change_at = Some(now);
}

fn parse_emergency_message(msg: &str) -> (usize, u64, f64) {
    parse_emergency_message_opt(msg).unwrap_or((0, 0, 0.0))
}

fn parse_emergency_message_opt(msg: &str) -> Option<(usize, u64, f64)> {
    let after = msg
        .find("delete")
        .and_then(|index| {
            msg[index..]
                .find(|c: char| c.is_ascii_digit())
                .map(|offset| index + offset)
        })
        .unwrap_or_else(|| msg.find(|c: char| c.is_ascii_digit()).unwrap_or(msg.len()));
    let rest = &msg[after..];
    let slash = rest.find('/')?;
    let deleted: usize = rest[..slash].trim().parse().ok()?;
    let after_slash = &rest[slash + 1..];
    let end = after_slash
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after_slash.len());
    let baseline: u64 = after_slash[..end].trim().parse().ok()?;
    let ratio = if baseline > 0 {
        deleted as f64 / baseline as f64
    } else {
        0.0
    };
    Some((deleted, baseline, ratio))
}
