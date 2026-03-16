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
