//! Operation-level undo: groups file changes into named operations.
//! AI agents can call `uhoh mark "label"` before a batch of changes,
//! and users can `uhoh undo` to revert the entire operation at once.

use anyhow::{Context, Result};
use std::path::Path;

use crate::db::{Database, ProjectEntry};
use crate::encoding;

pub fn cmd_mark(database: &Database, project: &ProjectEntry, label: &str) -> Result<()> {
    // If an operation is active, close it using the latest snapshot as last_snapshot_id
    if let Some(op) = database.get_active_operation(&project.hash)? {
        let latest = database
            .list_snapshots(&project.hash)?
            .first()
            .map_or(0, |s| s.snapshot_id);
        database.close_operation_with_last(op.id, latest)?;
        tracing::info!("Closed previous active operation at {}", {
            encoding::id_to_base58(latest)
        });
    }

    // Start a new operation and set its first_snapshot_id to current latest snapshot
    let op_id = database.create_operation(&project.hash, label)?;
    let current = database
        .list_snapshots(&project.hash)?
        .first()
        .map_or(0, |s| s.snapshot_id);
    database.set_operation_first_snapshot(op_id, current)?;

    println!(
        "Operation marked: \"{}\" (from snapshot {})",
        label,
        encoding::id_to_base58(current)
    );
    println!("Changes until the next `uhoh mark` or `uhoh undo` will be grouped.");
    Ok(())
}

pub fn cmd_undo(uhoh_dir: &Path, database: &Database, project: &ProjectEntry) -> Result<()> {
    // If an op is active, close it and use current latest as its last snapshot
    if let Some(op) = database.get_active_operation(&project.hash)? {
        let latest = database
            .list_snapshots(&project.hash)?
            .first()
            .map_or(0, |s| s.snapshot_id);
        database.close_operation_with_last(op.id, latest)?;
        tracing::info!(
            "Closed active operation: {} at {}",
            op.label,
            encoding::id_to_base58(latest)
        );
    }

    // Find the last completed operation
    let completed = database
        .get_latest_completed_operation(&project.hash)?
        .context("No completed operations to undo")?;

    // Restore to first_snap itself — this is the clean pre-operation state
    // recorded when `uhoh mark` was called. Previous code incorrectly went one
    // snapshot further back via snapshot_before(), destroying a valid snapshot.
    if completed.first_snapshot_id > 0 {
        let id_str = encoding::id_to_base58(completed.first_snapshot_id);
        println!(
            "Undoing operation \"{}\": restoring to snapshot {id_str}",
            completed.label
        );
        let cfg = crate::config::Config::load(&uhoh_dir.join("config.toml"))?;
        let snapshot_runtime = crate::snapshot::SnapshotRuntime::from_config(&cfg);
        crate::restore::restore_project(
            uhoh_dir,
            database,
            project,
            crate::restore::RestoreRequest {
                snapshot_id: &id_str,
                target_path: None,
                dry_run: false,
                force: true,
                pre_restore_snapshot: Some(crate::restore::PreRestoreSnapshot {
                    trigger: crate::db::SnapshotTrigger::PreRestore,
                    message: Some(format!("Before restore to {id_str}")),
                    snapshot_runtime: &snapshot_runtime,
                }),
                confirm_large_delete: None,
            },
        )?;
    } else {
        println!(
            "No snapshot found for operation \"{}\". Cannot undo.",
            completed.label
        );
    }

    Ok(())
}

pub fn cmd_list_operations(database: &Database, project: &ProjectEntry) -> Result<()> {
    let ops = database.list_operations(&project.hash)?;
    if ops.is_empty() {
        println!("No operations recorded.");
        return Ok(());
    }
    println!("Operations for {}:", project.current_path);
    for op in &ops {
        let status = if op.ended_at.is_some() {
            "completed"
        } else {
            "active"
        };
        let snap_range = match (op.first_snapshot_id, op.last_snapshot_id) {
            (Some(f), Some(l)) => format!(
                "snapshots {}..{}",
                encoding::id_to_base58(f),
                encoding::id_to_base58(l)
            ),
            _ => "no snapshots".to_string(),
        };
        println!(
            "  #{} [{status}] \"{}\" started={} {snap_range}",
            op.id, op.label, op.started_at
        );
    }
    Ok(())
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (Database, ProjectEntry, TempDir) {
        let tmp = TempDir::new().unwrap();
        let db = Database::open(&tmp.path().join("test.db")).unwrap();
        db.add_project("proj1", "/fake/project").unwrap();
        let project = db.get_project("proj1").unwrap().unwrap();
        (db, project, tmp)
    }

    // ── cmd_mark ──

    #[test]
    fn cmd_mark_creates_operation() {
        let (db, project, _tmp) = setup();
        cmd_mark(&db, &project, "first-op").unwrap();

        let active = db.get_active_operation(&project.hash).unwrap();
        assert!(active.is_some(), "should have an active operation");
        assert_eq!(active.unwrap().label, "first-op");
    }

    #[test]
    fn cmd_mark_closes_previous_active_operation() {
        let (db, project, _tmp) = setup();
        cmd_mark(&db, &project, "op-1").unwrap();
        cmd_mark(&db, &project, "op-2").unwrap();

        // op-1 should now be closed (completed)
        let ops = db.list_operations(&project.hash).unwrap();
        assert_eq!(ops.len(), 2);

        // The active one should be op-2
        let active = db.get_active_operation(&project.hash).unwrap().unwrap();
        assert_eq!(active.label, "op-2");

        // op-1 should be completed (ended_at is set)
        let op1 = ops.iter().find(|o| o.label == "op-1").unwrap();
        assert!(op1.ended_at.is_some(), "op-1 should be closed");
    }

    #[test]
    fn cmd_mark_sets_first_snapshot_id() {
        let (db, project, _tmp) = setup();
        cmd_mark(&db, &project, "snap-test").unwrap();

        // When there are no snapshots, first_snapshot_id should be set to 0
        let ops = db.list_operations(&project.hash).unwrap();
        assert_eq!(ops.len(), 1);
        // first_snapshot_id should be set (0 maps to Some(0))
        assert_eq!(ops[0].first_snapshot_id, Some(0));
    }

    #[test]
    fn cmd_mark_multiple_successive_marks() {
        let (db, project, _tmp) = setup();
        cmd_mark(&db, &project, "a").unwrap();
        cmd_mark(&db, &project, "b").unwrap();
        cmd_mark(&db, &project, "c").unwrap();

        let ops = db.list_operations(&project.hash).unwrap();
        assert_eq!(ops.len(), 3);

        // Only the latest should be active
        let active_count = ops.iter().filter(|o| o.ended_at.is_none()).count();
        assert_eq!(active_count, 1, "exactly one operation should be active");
        assert_eq!(ops[0].label, "c"); // newest first
    }

    // ── cmd_list_operations ──

    #[test]
    fn cmd_list_operations_empty() {
        let (db, project, _tmp) = setup();
        // Should succeed without error when no operations exist
        cmd_list_operations(&db, &project).unwrap();
    }

    #[test]
    fn cmd_list_operations_with_data() {
        let (db, project, _tmp) = setup();
        cmd_mark(&db, &project, "deploy-v1").unwrap();
        cmd_mark(&db, &project, "deploy-v2").unwrap();
        // Should succeed without error
        cmd_list_operations(&db, &project).unwrap();
    }

    #[test]
    fn cmd_list_operations_shows_completed_and_active() {
        let (db, project, _tmp) = setup();
        cmd_mark(&db, &project, "done-op").unwrap();
        cmd_mark(&db, &project, "active-op").unwrap();

        let ops = db.list_operations(&project.hash).unwrap();
        let completed = ops.iter().find(|o| o.label == "done-op").unwrap();
        let active = ops.iter().find(|o| o.label == "active-op").unwrap();

        assert!(completed.ended_at.is_some());
        assert!(active.ended_at.is_none());

        // cmd_list_operations should not error
        cmd_list_operations(&db, &project).unwrap();
    }

    // ── cmd_undo (limited: only test the "no completed operations" path) ──

    #[test]
    fn cmd_undo_fails_when_no_completed_operations() {
        let (db, project, _tmp) = setup();
        let uhoh_dir = _tmp.path().join(".uhoh");
        std::fs::create_dir_all(&uhoh_dir).unwrap();

        // With no operations at all, undo should fail
        let result = cmd_undo(&uhoh_dir, &db, &project);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("No completed operations"),
            "unexpected error: {err_msg}"
        );
    }

    #[test]
    fn cmd_undo_closes_active_op_before_failing_with_no_completed() {
        let (db, project, _tmp) = setup();
        let uhoh_dir = _tmp.path().join(".uhoh");
        std::fs::create_dir_all(&uhoh_dir).unwrap();

        // Mark an operation (creates an active, uncompleted op)
        db.create_operation(&project.hash, "active-only").unwrap();

        // cmd_undo should close the active op, then fail because the now-closed
        // op has first_snapshot_id=0 which means config loading will be attempted.
        // Since there's no config.toml, this will error -- but the active op
        // should have been closed before that point.
        let _result = cmd_undo(&uhoh_dir, &db, &project);

        // The previously active operation should now be closed
        let active = db.get_active_operation(&project.hash).unwrap();
        assert!(active.is_none(), "active op should have been closed");
    }
}
