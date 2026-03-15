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

            if referenced.contains(name_str.as_ref()) {
                continue;
            }

            // Check grace period — skip blobs that may still be in-progress
            let within_grace = blob_entry
                .metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.elapsed().ok())
                .is_some_and(|age| age < grace_period);
            if within_grace {
                continue;
            }

            if let Ok(meta) = blob_entry.metadata() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Helper: create a temp uhoh dir with database and blobs layout
    fn setup_gc_env() -> (TempDir, Database) {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("test.db");
        let db = Database::open(&db_path).unwrap();
        let blobs = tmp.path().join("blobs");
        fs::create_dir_all(blobs.join("ab")).unwrap();
        (tmp, db)
    }

    #[test]
    fn gc_skips_during_restore() {
        let (tmp, db) = setup_gc_env();
        fs::write(tmp.path().join(".restore-in-progress"), "").unwrap();
        // Should return Ok without doing anything
        let result = run_gc(tmp.path(), &db);
        assert!(result.is_ok());
    }

    #[test]
    fn gc_no_orphans_when_empty() {
        let (tmp, db) = setup_gc_env();
        let result = run_gc(tmp.path(), &db);
        assert!(result.is_ok());
    }

    #[test]
    fn gc_removes_orphaned_blobs() {
        let (tmp, db) = setup_gc_env();
        let blobs = tmp.path().join("blobs");
        let prefix_dir = blobs.join("ab");

        // Create a blob file that is NOT referenced in the DB
        let orphan = prefix_dir.join("abcdef123456");
        fs::write(&orphan, "orphan data").unwrap();

        // Backdate to well past the 15-minute grace period
        let old_time = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        fs::File::open(&orphan)
            .unwrap()
            .set_modified(old_time)
            .unwrap();

        assert!(orphan.exists());
        run_gc(tmp.path(), &db).unwrap();
        assert!(!orphan.exists(), "Orphaned blob should have been deleted");
    }

    #[test]
    fn gc_skips_temp_files_from_orphan_scan() {
        let (tmp, db) = setup_gc_env();
        let blobs = tmp.path().join("blobs");
        let prefix_dir = blobs.join("ab");

        // Create a temp file that is fresh (within the stale-temp cleanup window of 1 hour),
        // so cleanup_stale_temp_files won't remove it. The GC orphan scan itself should also
        // skip files starting with ".tmp." or ".blob.".
        let tmp_file = prefix_dir.join(".tmp.something");
        fs::write(&tmp_file, "temp data").unwrap();
        // File is fresh (just created), so both cleanup_stale_temp_files and GC skip it.

        // Also create a .blob. prefixed file
        let blob_file = prefix_dir.join(".blob.partial");
        fs::write(&blob_file, "partial blob").unwrap();

        run_gc(tmp.path(), &db).unwrap();
        assert!(tmp_file.exists(), ".tmp file should NOT be deleted by GC orphan scan");
        assert!(blob_file.exists(), ".blob file should NOT be deleted by GC orphan scan");
    }

    #[test]
    fn gc_skips_blobs_within_grace_period() {
        let (tmp, db) = setup_gc_env();
        let blobs = tmp.path().join("blobs");
        let prefix_dir = blobs.join("ab");

        // Create a fresh blob file (within grace period)
        let recent = prefix_dir.join("recenthash123");
        fs::write(&recent, "recent blob").unwrap();
        // Don't backdate — it's fresh

        run_gc(tmp.path(), &db).unwrap();
        assert!(
            recent.exists(),
            "Recent blob within grace period should NOT be deleted"
        );
    }

    #[test]
    fn gc_preserves_referenced_blobs() {
        let (tmp, db) = setup_gc_env();
        let blobs = tmp.path().join("blobs");
        let prefix_dir = blobs.join("ab");

        let blob_hash = "abcdef_referenced";
        let blob_path = prefix_dir.join(blob_hash);
        fs::write(&blob_path, "referenced data").unwrap();
        let old_time = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        fs::File::open(&blob_path)
            .unwrap()
            .set_modified(old_time)
            .unwrap();

        // Create a snapshot that references this blob
        db.add_project("proj1", "/fake/path").unwrap();
        let files = vec![crate::db::SnapFileEntry::new(
            "file.rs".into(),
            blob_hash.into(),
            100,
            true,
            false,
            None,
            crate::cas::StorageMethod::Copy,
            false,
        )];
        let ts = chrono::Utc::now().to_rfc3339();
        db.create_snapshot(crate::db::CreateSnapshotRow::new(
            "proj1",
            0,
            &ts,
            crate::db::SnapshotTrigger::Auto,
            "",
            false,
            &files,
            &[],
        ))
        .unwrap();

        run_gc(tmp.path(), &db).unwrap();
        assert!(
            blob_path.exists(),
            "Referenced blob should NOT be deleted"
        );
    }

    #[test]
    fn gc_cleans_empty_prefix_dirs() {
        let (tmp, db) = setup_gc_env();
        let blobs = tmp.path().join("blobs");
        let prefix_dir = blobs.join("cd");
        fs::create_dir_all(&prefix_dir).unwrap();

        // Create and backdate a file, then run GC which should remove it
        // and then clean the empty prefix directory
        let orphan = prefix_dir.join("orphan_blob");
        fs::write(&orphan, "data").unwrap();
        // Backdate to well past the 15-minute grace period
        let old_time = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000);
        fs::File::open(&orphan)
            .unwrap()
            .set_modified(old_time)
            .unwrap();

        run_gc(tmp.path(), &db).unwrap();
        assert!(
            !prefix_dir.exists(),
            "Empty prefix dir should be cleaned up"
        );
    }
}
