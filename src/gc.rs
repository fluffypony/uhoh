use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::Path;

use crate::db::Database;

/// Run garbage collection: delete blobs not referenced by any snapshot.
/// Applies a grace period: blobs younger than 10 minutes are never deleted
/// (prevents race with in-progress snapshot creation).
pub fn run_gc(uhoh_dir: &Path, database: &Database) -> Result<()> {
    let blob_root = uhoh_dir.join("blobs");

    // Get all referenced hashes
    let referenced = database.all_referenced_blob_hashes()?;
    println!("Referenced blobs: {}", referenced.len());

    // Walk blob directory
    let mut orphaned = Vec::new();
    let mut total_size = 0u64;
    let grace_period = std::time::Duration::from_secs(600); // 10 minutes

    for prefix_entry in std::fs::read_dir(&blob_root)? {
        let prefix_entry = prefix_entry?;
        if !prefix_entry.file_type()?.is_dir() {
            continue;
        }
        for blob_entry in std::fs::read_dir(prefix_entry.path())? {
            let blob_entry = blob_entry?;
            let name = blob_entry.file_name();
            let name_str = name.to_string_lossy();

            // Skip temp files
            if name_str.contains(".tmp.") {
                continue;
            }

            if !referenced.contains(name_str.as_ref()) {
                // Check grace period
                if let Ok(meta) = blob_entry.metadata() {
                    if let Ok(modified) = meta.modified() {
                        if let Ok(age) = modified.elapsed() {
                            if age < grace_period {
                                continue; // Too young, might be in-progress
                            }
                        }
                    }
                    total_size += meta.len();
                }
                orphaned.push(blob_entry.path());
            }
        }
    }

    if orphaned.is_empty() {
        println!("No orphaned blobs found.");
        return Ok(());
    }

    println!(
        "Found {} orphaned blobs ({:.1} MB)",
        orphaned.len(),
        total_size as f64 / 1_048_576.0
    );

    let bar = ProgressBar::new(orphaned.len() as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40}] {pos}/{len} blobs removed")
            .unwrap(),
    );

    for path in &orphaned {
        std::fs::remove_file(path)
            .with_context(|| format!("Failed to delete: {}", path.display()))?;
        bar.inc(1);
    }
    bar.finish_and_clear();

    println!(
        "Removed {} orphaned blobs ({:.1} MB freed)",
        orphaned.len(),
        total_size as f64 / 1_048_576.0
    );

    // Clean up empty prefix directories
    for prefix_entry in std::fs::read_dir(&blob_root)? {
        let prefix_entry = prefix_entry?;
        if prefix_entry.file_type()?.is_dir() {
            if std::fs::read_dir(prefix_entry.path())?.next().is_none() {
                std::fs::remove_dir(prefix_entry.path()).ok();
            }
        }
    }

    Ok(())
}
