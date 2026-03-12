use std::path::Path;

use anyhow::Result;

use crate::db::{Database, ProjectEntry};
use crate::events::ServerEvent;
use crate::restore_runtime::RestoreRuntime;

#[derive(Debug, Clone)]
pub struct SnapshotCreateResult {
    pub snapshot_id: Option<u64>,
    pub snapshot_event: Option<ServerEvent>,
}

pub fn create_project_snapshot(
    uhoh_dir: &Path,
    database: &Database,
    config: &crate::config::Config,
    project: &ProjectEntry,
    trigger: &str,
    message: Option<&str>,
) -> Result<SnapshotCreateResult> {
    let snapshot_id = crate::snapshot::create_snapshot(
        uhoh_dir,
        database,
        config,
        crate::snapshot::CreateSnapshotRequest {
            project_hash: &project.hash,
            project_path: Path::new(&project.current_path),
            trigger,
            message,
            changed_paths: None,
        },
    )?;

    let snapshot_event = snapshot_id
        .and_then(|id| build_snapshot_created_event(database, project, id, trigger, message));

    Ok(SnapshotCreateResult {
        snapshot_id,
        snapshot_event,
    })
}

pub fn restore_project_snapshot(
    restore_runtime: &RestoreRuntime,
    config: &crate::config::Config,
    project: &ProjectEntry,
    snapshot_id: &str,
    dry_run: bool,
    target_path: Option<&str>,
) -> Result<crate::restore::RestoreOutcome> {
    crate::restore_runtime::restore_project(
        restore_runtime,
        project,
        crate::restore::RestoreRequest {
            snapshot_id,
            target_path,
            dry_run,
            force: true,
            pre_restore_snapshot: Some(crate::restore::PreRestoreSnapshot {
                trigger: "pre-restore",
                message: Some(format!("Before restore to {snapshot_id}")),
                config,
            }),
            confirm_large_delete: None,
        },
    )
}

fn build_snapshot_created_event(
    database: &Database,
    project: &ProjectEntry,
    snapshot_id: u64,
    trigger: &str,
    message: Option<&str>,
) -> Option<ServerEvent> {
    let rowid = database.latest_snapshot_rowid(&project.hash).ok()??;
    let row = database.get_snapshot_by_rowid(rowid).ok()??;
    Some(ServerEvent::SnapshotCreated {
        project_hash: project.hash.clone(),
        snapshot_id: crate::cas::id_to_base58(snapshot_id),
        timestamp: row.timestamp,
        trigger: trigger.to_string(),
        file_count: row.file_count as usize,
        message: message.map(str::to_string),
    })
}
