use std::fmt;
use std::path::Path;

use anyhow::Result;

use crate::db::{Database, ProjectEntry};
use crate::events::ServerEvent;
use crate::restore::{RestoreBusyError, RestoreRuntime};

#[derive(Debug, Clone)]
#[non_exhaustive]
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
    snapshot_runtime: &crate::snapshot::SnapshotRuntime,
    project: &ProjectEntry,
    trigger: crate::db::SnapshotTrigger,
    message: Option<&str>,
) -> Result<SnapshotCreateResult> {
    let snapshot_id = crate::snapshot::create_snapshot(
        uhoh_dir,
        database,
        snapshot_runtime,
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
    snapshot_runtime: &crate::snapshot::SnapshotRuntime,
    project: &ProjectEntry,
    snapshot_id: &str,
    dry_run: bool,
    target_path: Option<&str>,
) -> std::result::Result<crate::restore::RestoreOutcome, RestoreProjectError> {
    crate::restore::restore_project_with_runtime(
        restore_runtime,
        project,
        crate::restore::RestoreRequest {
            snapshot_id,
            target_path,
            dry_run,
            force: true,
            pre_restore_snapshot: Some(crate::restore::PreRestoreSnapshot {
                trigger: crate::db::SnapshotTrigger::PreRestore,
                message: Some(format!("Before restore to {snapshot_id}")),
                snapshot_runtime,
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
    trigger: crate::db::SnapshotTrigger,
    message: Option<&str>,
) -> Option<ServerEvent> {
    let rowid = match database.latest_snapshot_rowid(&project.hash) {
        Ok(Some(r)) => r,
        Ok(None) => return None,
        Err(err) => {
            tracing::error!("failed to query latest snapshot rowid: {err}");
            return None;
        }
    };
    let row = match database.get_snapshot_by_rowid(rowid) {
        Ok(Some(r)) => r,
        Ok(None) => return None,
        Err(err) => {
            tracing::error!("failed to fetch snapshot by rowid {rowid}: {err}");
            return None;
        }
    };
    Some(ServerEvent::SnapshotCreated {
        project_hash: project.hash.clone(),
        snapshot_id: crate::encoding::id_to_base58(snapshot_id),
        timestamp: row.timestamp,
        trigger,
        #[allow(clippy::cast_possible_truncation)] // file_count fits in usize on all supported 64-bit targets
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

    if let Some(typed) = err.downcast_ref::<RestoreBusyError>() {
        return RestoreProjectError::Conflict(typed.to_string());
    }

    RestoreProjectError::Internal(err)
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_error_not_found_display() {
        let err = RestoreProjectError::NotFound("snapshot not found".to_string());
        assert_eq!(format!("{err}"), "snapshot not found");
    }

    #[test]
    fn restore_error_conflict_display() {
        let err = RestoreProjectError::Conflict("restore in progress".to_string());
        assert_eq!(format!("{err}"), "restore in progress");
    }

    #[test]
    fn restore_error_invalid_input_display() {
        let err = RestoreProjectError::InvalidInput("bad input".to_string());
        assert_eq!(format!("{err}"), "bad input");
    }

    #[test]
    fn restore_error_internal_display() {
        let err = RestoreProjectError::Internal(anyhow::anyhow!("db failure"));
        assert_eq!(format!("{err}"), "db failure");
    }

    #[test]
    fn restore_error_internal_has_source() {
        let err = RestoreProjectError::Internal(anyhow::anyhow!("inner error"));
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn restore_error_not_found_no_source() {
        let err = RestoreProjectError::NotFound("msg".to_string());
        assert!(std::error::Error::source(&err).is_none());
    }

    #[test]
    fn snapshot_create_result_none() {
        let result = SnapshotCreateResult {
            snapshot_id: None,
            snapshot_event: None,
        };
        assert!(result.snapshot_id.is_none());
        assert!(result.snapshot_event.is_none());
    }

    #[test]
    fn classify_restore_error_unknown_is_internal() {
        let err = anyhow::anyhow!("unknown error");
        match classify_restore_error(err) {
            RestoreProjectError::Internal(e) => assert_eq!(e.to_string(), "unknown error"),
            other => panic!("Expected Internal, got: {other}"),
        }
    }
}
