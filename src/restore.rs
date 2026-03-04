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

    // Pre-flight: verify that all required blobs exist and warn about unstored files
    let mut missing_blobs = Vec::new();
    let mut unstored = Vec::new();
    for f in &target_files {
        if !f.stored { unstored.push(f.path.as_str()); continue; }
        if !crate::cas::blob_exists(&blob_root, &f.hash) {
            missing_blobs.push((f.path.as_str(), &f.hash));
        }
    }
    if !unstored.is_empty() {
        eprintln!("⚠ {} file(s) in target snapshot were not stored (likely too large) and will be skipped.", unstored.len());
        for p in unstored.iter().take(10) { eprintln!("  - {}", p); }
        if unstored.len() > 10 { eprintln!("  ... and {} more", unstored.len() - 10); }
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
    // Use a unique staging directory name to avoid collisions
    let now = chrono::Utc::now();
    let nanos_i64: i64 = now
        .timestamp_nanos_opt()
        .unwrap_or_else(|| ((now.timestamp() as i128) * 1_000_000_000).try_into().unwrap_or(0));
    let unique_suffix = format!("{}-{}", std::process::id(), nanos_i64);
    let restore_tmp = project_path.join(format!(".uhoh-restore-tmp-{}", unique_suffix));
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
        // Stage under temp dir — avoid following symlinks by verifying parents are not symlinks
        let full_path = restore_tmp.join(path);
        if let Some(parent) = full_path.parent() {
            // Ensure none of the parent components are symlinks
            let mut cur = parent.to_path_buf();
            while cur != restore_tmp {
                if let Ok(meta) = std::fs::symlink_metadata(&cur) {
                    if meta.file_type().is_symlink() { anyhow::bail!("Refusing to write through symlinked directory: {}", cur.display()); }
                }
                if !cur.pop() { break; }
            }
            std::fs::create_dir_all(parent)?;
        }
        // Do not write to an existing symlink path
        if let Ok(meta) = std::fs::symlink_metadata(&full_path) {
            if meta.file_type().is_symlink() { anyhow::bail!("Refusing to overwrite symlinked file: {}", full_path.display()); }
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
        if dst.is_file() { let _ = std::fs::remove_file(&dst); }
        else if dst.is_dir() { let _ = std::fs::remove_dir_all(&dst); }
        else { let _ = std::fs::remove_file(&dst); }
    }
    for (path, _, _) in &to_restore {
        let staged = restore_tmp.join(path);
        let final_dest = project_path.join(path);
        if let Some(parent) = final_dest.parent() { std::fs::create_dir_all(parent)?; }
        // Remove any existing file or directory before rename to handle dir->file transitions
        if final_dest.is_file() { let _ = std::fs::remove_file(&final_dest); }
        else if final_dest.is_dir() { let _ = std::fs::remove_dir_all(&final_dest); }
        std::fs::rename(&staged, &final_dest)
            .with_context(|| format!("Failed to move {} -> {}", staged.display(), final_dest.display()))?;
    }
    // Cleanup staging
    let _ = std::fs::remove_dir_all(&restore_tmp);
    // Clean up any now-empty directories within project (best-effort)
    fn remove_empty_dirs(root: &Path, base: &Path) {
        if let Ok(entries) = std::fs::read_dir(root) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() { remove_empty_dirs(&p, base); }
            }
        }
        if root != base {
            if std::fs::read_dir(root).map(|mut it| it.next().is_none()).unwrap_or(false) {
                let _ = std::fs::remove_dir(root);
            }
        }
    }
    remove_empty_dirs(project_path, project_path);

    let snap_id_str = cas::id_to_base58(snap.snapshot_id);
    println!(
        "Restored to snapshot {} ({} files restored, {} deleted)",
        snap_id_str,
        to_restore.len(),
        to_delete.len()
    );

    Ok(())
}
