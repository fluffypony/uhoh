use std::fmt;
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

#[derive(Debug)]
pub enum RestoreProjectError {
    NotFound(String),
    Conflict(String),
    InvalidInput(String),
    Internal(anyhow::Error),
}

impl RestoreProjectError {
    pub fn message(&self) -> String {
        match self {
            Self::NotFound(message) | Self::Conflict(message) | Self::InvalidInput(message) => {
                message.clone()
            }
            Self::Internal(err) => err.to_string(),
        }
    }
}

impl fmt::Display for RestoreProjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(message) | Self::Conflict(message) | Self::InvalidInput(message) => {
                f.write_str(message)
            }
            Self::Internal(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for RestoreProjectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Internal(err) => Some(err.root_cause()),
            _ => None,
        }
    }
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
) -> std::result::Result<crate::restore::RestoreOutcome, RestoreProjectError> {
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
    .map_err(classify_restore_error)
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

fn classify_restore_error(err: anyhow::Error) -> RestoreProjectError {
    if let Some(typed) = err.downcast_ref::<crate::restore::RestoreApplyError>() {
        return match typed {
            crate::restore::RestoreApplyError::SnapshotNotFound
            | crate::restore::RestoreApplyError::TargetPathNotFound { .. } => {
                RestoreProjectError::NotFound(typed.to_string())
            }
            crate::restore::RestoreApplyError::ConfirmationRequired { .. } => {
                RestoreProjectError::InvalidInput(typed.to_string())
            }
        };
    }

    if let Some(typed) = err.downcast_ref::<crate::restore_guards::RestoreBusyError>() {
        return RestoreProjectError::Conflict(typed.to_string());
    }

    RestoreProjectError::Internal(err)
}
