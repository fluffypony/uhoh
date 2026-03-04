use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::Path;

use crate::cas;
use crate::db::{Database, ProjectEntry};
use crate::snapshot;

pub fn cmd_restore(
    uhoh_dir: &Path,
    database: &Database,
    project: &ProjectEntry,
    id_str: &str,
    dry_run: bool,
    force: bool,
) -> Result<()> {
    let snap = database
        .find_snapshot_by_base58(&project.hash, id_str)?
        .context("Snapshot not found")?;

    let project_path = Path::new(&project.current_path);
    let blob_root = uhoh_dir.join("blobs");
    let target_files = database.get_snapshot_files(snap.rowid)?;

    // Build set of target paths
    let target_paths: HashSet<String> = target_files.iter().map(|f| f.path.clone()).collect();

    // Load current snapshot to determine what's tracked (only delete tracked files)
    let current_tracked: HashSet<String> = if let Some(current_rowid) =
        database.latest_snapshot_rowid(&project.hash)?
    {
        database
            .get_snapshot_files(current_rowid)?
            .iter()
            .map(|f| f.path.clone())
            .collect()
    } else {
        // No previous snapshot: treat existing files as potentially tracked
        {
            let mut set = HashSet::new();
            for e in crate::ignore_rules::build_walker(project_path) {
                if let Ok(ent) = e {
                    if ent.file_type().map_or(false, |ft| ft.is_file()) {
                        if let Ok(rel) = ent.path().strip_prefix(project_path) {
                            set.insert(crate::cas::normalize_path(rel));
                        }
                    }
                }
            }
            set
        }
    };

    // Compute changes
    let mut to_delete: Vec<String> = Vec::new();
    let mut to_restore: Vec<(String, String, bool)> = Vec::new(); // (path, hash, executable)

    // Files to delete: in current manifest but not in target manifest
    // (leave untracked/ignored files completely alone)
    for path in &current_tracked {
        if !target_paths.contains(path) {
            to_delete.push(path.clone());
        }
    }

    // Files to restore/create
    for file in &target_files {
        if !file.stored {
            continue; // Skip unstored large binaries
        }
        to_restore.push((file.path.clone(), file.hash.clone(), file.executable));
    }

    if dry_run {
        println!("Dry run — changes that would be applied:");
        for path in &to_delete {
            println!("  DELETE {}", path);
        }
        for (path, _, _) in &to_restore {
            println!("  RESTORE {}", path);
        }
        return Ok(());
    }

    // Safety: prompt if deleting many files
    if to_delete.len() > 10 && !force {
        eprintln!(
            "⚠ This will delete {} tracked files. Use --force to skip this prompt.",
            to_delete.len()
        );
        eprint!("Continue? [y/N] ");
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Create pre-restore snapshot for safety
    tracing::info!("Creating pre-restore snapshot...");
    let cfg = crate::config::Config::load(&uhoh_dir.join("config.toml"))?;
    let _ = snapshot::create_snapshot(
        uhoh_dir,
        database,
        &project.hash,
        project_path,
        "pre-restore",
        Some(&format!("Before restore to {}", id_str)),
        &cfg,
    );

    // Delete files (only those tracked by uhoh, not untracked)
    for path in &to_delete {
        let full = project_path.join(path);
        if full.exists() {
            std::fs::remove_file(&full)
                .with_context(|| format!("Failed to delete: {}", full.display()))?;
        }
    }

    // Restore files
    let bar = indicatif::ProgressBar::new(to_restore.len() as u64);
    bar.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40}] {pos}/{len} files")
            .unwrap(),
    );

    for (path, hash, executable) in &to_restore {
        let full_path = project_path.join(path);
        if path.contains("..") {
            tracing::warn!("Skipping suspicious path with '..': {}", path);
            continue;
        }
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = cas::read_blob(&blob_root, hash)?
            .ok_or_else(|| anyhow::anyhow!("Blob {} missing for {}", &hash[..12], path))?;
        std::fs::write(&full_path, &content)
            .with_context(|| format!("Failed to write: {}", full_path.display()))?;

        // Restore executable permission
        #[cfg(unix)]
        if *executable {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o755);
            std::fs::set_permissions(&full_path, perms)?;
        }

        bar.inc(1);
    }
    bar.finish_and_clear();

    let snap_id_str = cas::id_to_base58(snap.snapshot_id);
    println!(
        "Restored to snapshot {} ({} files restored, {} deleted)",
        snap_id_str,
        to_restore.len(),
        to_delete.len()
    );

    Ok(())
}
