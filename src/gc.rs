use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::Path;

use crate::db::Database;

/// Run garbage collection: delete blobs not referenced by any snapshot.
/// Applies a grace period: blobs younger than 10 minutes are never deleted
/// (prevents race with in-progress snapshot creation).
pub fn run_gc(uhoh_dir: &Path, database: &Database) -> Result<()> {
    // Coordinate with restore: skip GC while restore is actively rewriting files.
    if uhoh_dir.join(".restore-in-progress").exists() {
        tracing::info!("Skipping GC while restore is in progress");
        return Ok(());
    }

    let blob_root = uhoh_dir.join("blobs");

    // Clean up stale temp files first (unified with cas::cleanup_stale_temp_files)
    let max_age = std::time::Duration::from_secs(3600);
    crate::cas::cleanup_stale_temp_files(&blob_root, max_age);

    let referenced = database.all_referenced_blob_hashes()?;
    println!("Referenced blobs: {}", referenced.len());

    let mut orphaned = Vec::new();
    let mut total_size = 0u64;
    let grace_period = std::time::Duration::from_secs(900); // 15 minutes to cover long snapshots

    for prefix_entry in std::fs::read_dir(&blob_root)? {
        let prefix_entry = prefix_entry?;
        if !prefix_entry.file_type()?.is_dir() {
            continue;
        }
        for blob_entry in std::fs::read_dir(prefix_entry.path())? {
            let blob_entry = blob_entry?;
            let name = blob_entry.file_name();
            let name_str = name.to_string_lossy();

            if name_str.starts_with(".tmp.") || name_str.starts_with(".blob.") {
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
                    // Hardlink-aware approximate usage: divide by link count if >1 (Unix only)
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::MetadataExt;
                        let usage = if meta.nlink() > 1 {
                            meta.len() / meta.nlink()
                        } else {
                            meta.len()
                        };
                        total_size += usage;
                    }
                    #[cfg(not(unix))]
                    {
                        total_size += meta.len();
                    }
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
        let sz = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        // Clear read-only attribute before deletion (blobs are set read-only;
        // on Windows this is mandatory or remove_file fails).
        #[cfg(not(unix))]
        {
            if let Ok(meta) = std::fs::metadata(path) {
                let mut perms = meta.permissions();
                if perms.readonly() {
                    perms.set_readonly(false);
                    let _ = std::fs::set_permissions(path, perms);
                }
            }
        }
        std::fs::remove_file(path)
            .with_context(|| format!("Failed to delete: {}", path.display()))?;
        // Decrement cached blob bytes counter if available
        let _ = database.add_blob_bytes(-(sz as i64));
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
        if prefix_entry.file_type()?.is_dir()
            && std::fs::read_dir(prefix_entry.path())?.next().is_none()
        {
            std::fs::remove_dir(prefix_entry.path()).ok();
        }
    }

    Ok(())
}
