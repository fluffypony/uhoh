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
        None,
    );

    // Pre-flight: verify that all required blobs exist
    let mut missing_blobs = Vec::new();
    for f in &target_files {
        if !f.stored { continue; }
        if !crate::cas::blob_exists(&blob_root, &f.hash) {
            missing_blobs.push((f.path.as_str(), &f.hash));
        }
    }
    if !missing_blobs.is_empty() {
        eprintln!("ERROR: {} blob(s) missing; aborting restore.", missing_blobs.len());
        for (p, h) in missing_blobs.iter().take(10) {
            let short = &h[..h.len().min(12)];
            eprintln!("  - {} ({}...)", p, short);
        }
        anyhow::bail!("Cannot restore: required blobs missing");
    }

    // Non-atomic deletions replaced by staged restore into temp dir followed by atomic renames
    let restore_tmp = project_path.join(".uhoh-restore-tmp");
    std::fs::create_dir_all(&restore_tmp)?;

    // Stage restored files into temp directory
    let bar = indicatif::ProgressBar::new(to_restore.len() as u64);
    bar.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40}] {pos}/{len} files")
            .unwrap(),
    );

    for (path, hash, executable) in &to_restore {
        // Path traversal guard using structural components
        let p = std::path::Path::new(path);
        if p.is_absolute() || p.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
            tracing::warn!("Skipping suspicious path: {}", path);
            continue;
        }
        // Stage under temp dir
        let full_path = restore_tmp.join(path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = cas::read_blob(&blob_root, hash)?
            .ok_or_else(|| anyhow::anyhow!("Blob {} missing for {}", &hash[..hash.len().min(12)], path))?;
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
    // Phase 2: Apply staged files atomically and delete tracked files missing from target
    for path in &to_delete {
        let dst = project_path.join(path);
        if dst.exists() { let _ = std::fs::remove_file(&dst); }
    }
    for (path, _, _) in &to_restore {
        let staged = restore_tmp.join(path);
        let final_dest = project_path.join(path);
        if let Some(parent) = final_dest.parent() { std::fs::create_dir_all(parent)?; }
        // If a file with same name exists (not in to_delete), replace it
        if final_dest.exists() { let _ = std::fs::remove_file(&final_dest); }
        std::fs::rename(&staged, &final_dest)
            .with_context(|| format!("Failed to move {} -> {}", staged.display(), final_dest.display()))?;
    }
    // Cleanup staging
    let _ = std::fs::remove_dir_all(&restore_tmp);

    let snap_id_str = cas::id_to_base58(snap.snapshot_id);
    println!(
        "Restored to snapshot {} ({} files restored, {} deleted)",
        snap_id_str,
        to_restore.len(),
        to_delete.len()
    );

    Ok(())
}
