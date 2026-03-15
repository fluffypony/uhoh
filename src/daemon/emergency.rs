use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

use crate::config::WatchConfig;
use crate::db::{Database, LedgerEventType, LedgerSeverity, LedgerSource, SnapshotRow};
use crate::event_ledger::{new_event, EventLedger};
use crate::events::{publish_event, ServerEvent};

use super::snapshots::{ProjectDaemonState, SnapshotSpawnRequest, SnapshotTaskKind};

const POST_RESTORE_GRACE_SECS: u64 = 10;

#[allow(clippy::too_many_arguments)]
pub(super) fn evaluate_emergency_for_project(
    project_path: &str,
    state: &mut ProjectDaemonState,
    database: &Database,
    watch: &WatchConfig,
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
        if window_start.elapsed() > Duration::from_secs(watch.emergency_cooldown_secs) {
            state.cumulative_deletes = state.deleted_paths.len();
            state.cumulative_window_start = Some(now);
        }
    }

    let deleted_paths_hint_count = state.cumulative_deletes.max(state.deleted_paths.len());
    let evaluation = crate::emergency::evaluate_emergency(crate::emergency::EmergencyEvalInput {
        deleted_paths_hint_count,
        cached_baseline_count: state.cached_prev_file_count,
        last_emergency_at: state.last_emergency_at,
        cooldown_secs: watch.emergency_cooldown_secs,
        threshold: watch.emergency_delete_threshold,
        min_files: watch.emergency_delete_min_files,
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

            emit_emergency_delete_detected(EmergencyDeleteEvent {
                event_tx,
                event_ledger,
                project_hash: &state.hash,
                watch,
                deleted_count: verified_deleted_count,
                baseline_count,
                ratio,
                cooldown_suppressed: false,
                cooldown_remaining_secs: None,
                extra_detail: Some(serde_json::json!({ "deleted_paths_sample": deleted_paths_sample })),
            });

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
            emit_emergency_delete_detected(EmergencyDeleteEvent {
                event_tx,
                event_ledger,
                project_hash: &state.hash,
                watch,
                deleted_count: verified_deleted_count,
                baseline_count,
                ratio,
                cooldown_suppressed: true,
                cooldown_remaining_secs: Some(cooldown_remaining_secs),
                extra_detail: None,
            });
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

/// Bundles the positional parameters for [`emit_emergency_delete_detected`].
pub(super) struct EmergencyDeleteEvent<'a> {
    pub event_tx: &'a broadcast::Sender<ServerEvent>,
    pub event_ledger: &'a EventLedger,
    pub project_hash: &'a str,
    pub watch: &'a WatchConfig,
    pub deleted_count: usize,
    pub baseline_count: u64,
    pub ratio: f64,
    pub cooldown_suppressed: bool,
    pub cooldown_remaining_secs: Option<u64>,
    pub extra_detail: Option<serde_json::Value>,
}

pub(super) fn emit_emergency_delete_detected(event: EmergencyDeleteEvent<'_>) {
    let EmergencyDeleteEvent {
        event_tx,
        event_ledger,
        project_hash,
        watch,
        deleted_count,
        baseline_count,
        ratio,
        cooldown_suppressed,
        cooldown_remaining_secs,
        extra_detail,
    } = event;

    let mut detail = serde_json::json!({
        "deleted_count": deleted_count,
        "baseline_count": baseline_count,
        "ratio": ratio,
        "threshold": watch.emergency_delete_threshold,
        "min_files": watch.emergency_delete_min_files,
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
        LedgerSeverity::Info
    } else {
        crate::emergency::severity_for_ratio(ratio)
    };
    let mut ledger_event = new_event(LedgerSource::Fs, LedgerEventType::EmergencyDeleteDetected, severity);
    ledger_event.project_hash = Some(project_hash.to_string());
    ledger_event.detail = Some(detail.to_string());
    if let Err(err) = event_ledger.append(ledger_event) {
        tracing::error!("failed to append emergency_delete_detected event: {err}");
    }

    publish_event(
        event_tx,
        ServerEvent::EmergencyDeleteDetected {
            project_hash: project_hash.to_string(),
            deleted_count,
            baseline_count,
            ratio,
            threshold: watch.emergency_delete_threshold,
            min_files: watch.emergency_delete_min_files,
            cooldown_suppressed,
            cooldown_remaining_secs,
        },
    );
}

pub(super) fn handle_dynamic_emergency_upgrade(
    database: &Database,
    state: &ProjectDaemonState,
    snapshot_id: u64,
    row: &SnapshotRow,
    watch: &WatchConfig,
    event_tx: &broadcast::Sender<ServerEvent>,
    event_ledger: &EventLedger,
) {
    let predecessor = match database.snapshot_before(&state.hash, snapshot_id) {
        Ok(row) => row,
        Err(err) => {
            tracing::warn!(
                project = %state.hash,
                "Failed to look up predecessor snapshot for dynamic upgrade: {err}"
            );
            None
        }
    };
    if let Some(predecessor) = predecessor.as_ref() {
        if let Err(err) = database.pin_snapshot(predecessor.rowid, true) {
            tracing::warn!(
                project = %state.hash,
                rowid = predecessor.rowid,
                "Failed to pin predecessor snapshot via dynamic upgrade: {err}"
            );
        } else {
            tracing::info!(
                "Pinned predecessor snapshot (rowid={}) via dynamic upgrade",
                predecessor.rowid
            );
        }
    }

    let deleted_count = database
        .get_snapshot_deleted_files(row.rowid)
        .map(|files| files.len())
        .unwrap_or_else(|err| {
            tracing::warn!(
                project = %state.hash,
                rowid = row.rowid,
                "Failed to load deleted files for dynamic upgrade, falling back to message parse: {err}"
            );
            let (deleted, _, _) = parse_emergency_message(&row.message);
            deleted
        });
    let baseline_from_predecessor = predecessor.as_ref().map(|snapshot| snapshot.file_count);
    let baseline_from_row = row.file_count.saturating_add(deleted_count as u64);
    let baseline_count = baseline_from_predecessor.unwrap_or(baseline_from_row);
    let ratio = crate::emergency::deletion_ratio(deleted_count, baseline_count);

    emit_emergency_delete_detected(EmergencyDeleteEvent {
        event_tx,
        event_ledger,
        project_hash: &state.hash,
        watch,
        deleted_count,
        baseline_count,
        ratio,
        cooldown_suppressed: false,
        cooldown_remaining_secs: None,
        extra_detail: Some(serde_json::json!({ "source": "dynamic_upgrade" })),
    });
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
