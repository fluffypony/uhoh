//! Operation-level undo: groups file changes into named operations.
//! AI agents can call `uhoh mark "label"` before a batch of changes,
//! and users can `uhoh undo` to revert the entire operation at once.

use anyhow::{Context, Result};
use std::path::Path;

use crate::cas;
use crate::db::{Database, ProjectEntry};

pub fn cmd_mark(database: &Database, project: &ProjectEntry, label: &str) -> Result<()> {
    // If an operation is active, close it using the latest snapshot as last_snapshot_id
    if let Some((op_id, _)) = database.get_active_operation(&project.hash)? {
        let latest = database
            .list_snapshots(&project.hash)?
            .first()
            .map(|s| s.snapshot_id)
            .unwrap_or(0);
        database.close_operation_with_last(op_id, latest)?;
        tracing::info!("Closed previous active operation at {}", {
            cas::id_to_base58(latest)
        });
    }

    // Start a new operation and set its first_snapshot_id to current latest snapshot
    let op_id = database.create_operation(&project.hash, label)?;
    let current = database
        .list_snapshots(&project.hash)?
        .first()
        .map(|s| s.snapshot_id)
        .unwrap_or(0);
    database.set_operation_first_snapshot(op_id, current)?;

    println!(
        "Operation marked: \"{}\" (from snapshot {})",
        label,
        cas::id_to_base58(current)
    );
    println!("Changes until the next `uhoh mark` or `uhoh undo` will be grouped.");
    Ok(())
}

pub fn cmd_undo(uhoh_dir: &Path, database: &Database, project: &ProjectEntry) -> Result<()> {
    // If an op is active, close it and use current latest as its last snapshot
    if let Some((op_id, label)) = database.get_active_operation(&project.hash)? {
        let latest = database
            .list_snapshots(&project.hash)?
            .first()
            .map(|s| s.snapshot_id)
            .unwrap_or(0);
        database.close_operation_with_last(op_id, latest)?;
        tracing::info!(
            "Closed active operation: {} at {}",
            label,
            cas::id_to_base58(latest)
        );
    }

    // Find the last completed operation
    let (_op_id, label, first_snap, _last_snap) = database
        .get_latest_completed_operation(&project.hash)?
        .context("No completed operations to undo")?;

    // Restore to first_snap itself — this is the clean pre-operation state
    // recorded when `uhoh mark` was called. Previous code incorrectly went one
    // snapshot further back via snapshot_before(), destroying a valid snapshot.
    if first_snap > 0 {
        let id_str = cas::id_to_base58(first_snap);
        println!("Undoing operation \"{label}\": restoring to snapshot {id_str}");
        crate::restore::cmd_restore(
            uhoh_dir, database, project, &id_str, None, false,
            true, // force (since this is an undo, we're intentional)
        )?;
    } else {
        println!("No snapshot found for operation \"{label}\". Cannot undo.");
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
    for (id, label, started, ended, first_snap, last_snap) in &ops {
        let status = if ended.is_some() {
            "completed"
        } else {
            "active"
        };
        let snap_range = match (first_snap, last_snap) {
            (Some(f), Some(l)) => format!(
                "snapshots {}..{}",
                cas::id_to_base58(*f),
                cas::id_to_base58(*l)
            ),
            _ => "no snapshots".to_string(),
        };
        println!("  #{id} [{status}] \"{label}\" started={started} {snap_range}");
    }
    Ok(())
}
