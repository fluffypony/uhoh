use anyhow::{Context, Result};
use std::collections::HashSet;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use crate::cas;
use crate::db::{Database, ProjectEntry};
use crate::snapshot;

const RESTORE_IN_PROGRESS_FILE: &str = ".restore-in-progress";

struct RestoreSignalGuard {
    path: PathBuf,
}

impl RestoreSignalGuard {
    fn install(uhoh_dir: &Path, project_hash: &str) -> Result<Self> {
        let path = uhoh_dir.join(RESTORE_IN_PROGRESS_FILE);
        let payload = format!(
            "project_hash={project_hash}\npid={}\n",
            std::process::id()
        );
        std::fs::write(&path, payload)
            .with_context(|| format!("Failed to write restore marker: {}", path.display()))?;
        Ok(Self { path })
    }
}

impl Drop for RestoreSignalGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug, Clone)]
pub struct RestoreOutcome {
    pub snapshot_id: String,
    pub dry_run: bool,
    pub applied: bool,
    pub files_restored: usize,
    pub files_deleted: usize,
    pub files_to_restore: Vec<String>,
    pub files_to_delete: Vec<String>,
}

pub fn restore_symlink_target(content: &[u8], full_path: &Path) -> Result<()> {
    if full_path.symlink_metadata().is_ok() {
        let _ = std::fs::remove_file(full_path);
    }

    #[cfg(unix)]
    {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let target = OsStr::from_bytes(content);
        std::os::unix::fs::symlink(target, full_path)
            .with_context(|| format!("Failed to create symlink at {}", full_path.display()))?;
    }

    #[cfg(windows)]
    {
        let target_str = String::from_utf8_lossy(content);
        let target_path = std::path::Path::new(target_str.as_ref());
        match std::os::windows::fs::symlink_file(target_path, full_path) {
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(1314) => {
                tracing::warn!(
                    "Cannot create symlink at {}: Windows requires Developer Mode or elevated privileges. Writing target as regular file instead.",
                    full_path.display()
                );
                std::fs::write(full_path, content)?;
            }
            Err(e) => match std::os::windows::fs::symlink_dir(target_path, full_path) {
                Ok(_) => {}
                Err(_) => {
                    tracing::warn!(
                        "Cannot create symlink at {}: {}. Writing target as regular file.",
                        full_path.display(),
                        e
                    );
                    std::fs::write(full_path, content)?;
                }
            },
        }
    }

    Ok(())
}

fn check_no_symlink_parents(restore_base: &Path, target: &Path) -> Result<()> {
    if let Ok(relative) = target.strip_prefix(restore_base) {
        let mut current = restore_base.to_path_buf();
        for component in relative.parent().unwrap_or(Path::new("")).components() {
            current.push(component);
            if let Ok(meta) = current.symlink_metadata() {
                if meta.file_type().is_symlink() {
                    anyhow::bail!(
                        "Refusing to write through symlinked directory: {}",
                        current.display()
                    );
                }
            }
        }
    }
    Ok(())
}

fn safe_remove_dir_tree_within(project_base: &Path, target: &Path) -> Result<()> {
    let project_canonical = dunce::canonicalize(project_base).with_context(|| {
        format!(
            "Failed to canonicalize project base: {}",
            project_base.display()
        )
    })?;
    let target_canonical = dunce::canonicalize(target)
        .with_context(|| format!("Failed to canonicalize target: {}", target.display()))?;

    if !target_canonical.starts_with(&project_canonical) {
        anyhow::bail!(
            "Refusing to delete directory outside project root: {}",
            target.display()
        );
    }

    let mut stack = vec![target_canonical.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let meta = std::fs::symlink_metadata(&path)?;
            if meta.file_type().is_symlink() {
                anyhow::bail!(
                    "Refusing to recursively delete directory containing symlink: {}",
                    path.display()
                );
            }
            if meta.is_dir() {
                stack.push(path);
            }
        }
    }

    std::fs::remove_dir_all(&target_canonical)
        .with_context(|| format!("Failed removing directory {}", target.display()))?;
    Ok(())
}

fn validate_restore_path(project_path: &Path, relative_path: &str) -> Result<()> {
    let rel = Path::new(relative_path);
    if rel.is_absolute() {
        anyhow::bail!("Absolute path in snapshot manifest: {relative_path}");
    }
    for component in rel.components() {
        if matches!(component, std::path::Component::ParentDir) {
            anyhow::bail!("Path traversal detected in snapshot manifest: {relative_path}");
        }
    }

    let full = project_path.join(rel);
    if let Ok(canonical) = dunce::canonicalize(&full) {
        if let Ok(project_canonical) = dunce::canonicalize(project_path) {
            if !canonical.starts_with(project_canonical) {
                anyhow::bail!("Path '{relative_path}' resolves outside project directory");
            }
        }
    }

    Ok(())
}

pub fn cmd_restore(
    uhoh_dir: &Path,
    database: &Database,
    project: &ProjectEntry,
    id_str: &str,
    target_path: Option<&str>,
    dry_run: bool,
    force: bool,
) -> Result<RestoreOutcome> {
    let snap = database
        .find_snapshot_by_base58(&project.hash, id_str)?
        .context("Snapshot not found")?;
    let snap_id_str = cas::id_to_base58(snap.snapshot_id);

    let project_path = Path::new(&project.current_path);
    let blob_root = uhoh_dir.join("blobs");
    let mut target_files = database.get_snapshot_files(snap.rowid)?;

    let target_filter = target_path
        .map(|path| {
            validate_restore_path(project_path, path)?;
            Ok::<String, anyhow::Error>(if path.starts_with("b64:") {
                path.to_string()
            } else {
                cas::encode_relpath(Path::new(path))
            })
        })
        .transpose()?;

    if let Some(filter) = target_filter.as_ref() {
        target_files.retain(|file| file.path == *filter);
        if target_files.is_empty() {
            anyhow::bail!(
                "File '{}' was not found in snapshot {}",
                target_path.unwrap_or_default(),
                snap_id_str
            );
        }
    }

    // Build set of target paths
    let target_paths: HashSet<String> = target_files.iter().map(|f| f.path.clone()).collect();

    // Load current snapshot to determine what's tracked (only delete tracked files)
    let current_tracked: HashSet<String> =
        if let Some(current_rowid) = database.latest_snapshot_rowid(&project.hash)? {
            database
                .get_snapshot_files(current_rowid)?
                .iter()
                .map(|f| f.path.clone())
                .collect()
        } else {
            // No previous snapshot: treat existing files as potentially tracked
            {
                let mut set = HashSet::new();
                for ent in crate::ignore_rules::build_walker(project_path).flatten() {
                    if ent
                        .file_type()
                        .is_some_and(|ft| ft.is_file() || ft.is_symlink())
                    {
                        if let Ok(rel) = ent.path().strip_prefix(project_path) {
                            set.insert(crate::cas::encode_relpath(rel));
                        }
                    }
                }
                set
            }
        };

    let mut to_delete: Vec<String> = Vec::new();
    let mut to_restore: Vec<(std::path::PathBuf, String, bool, bool)> = Vec::new(); // (path, hash, executable, is_symlink)

    let mut cleanup_staging: Option<std::path::PathBuf> = None;
    let _restore_signal = if dry_run {
        None
    } else {
        Some(RestoreSignalGuard::install(uhoh_dir, &project.hash)?)
    };

    let result = (|| -> Result<RestoreOutcome> {
        // Files to delete: in current manifest but not in target manifest
        if let Some(filter) = target_filter.as_ref() {
            if current_tracked.contains(filter) && !target_paths.contains(filter) {
                validate_restore_path(project_path, filter)?;
                to_delete.push(filter.clone());
            }
        } else {
            for path in &current_tracked {
                if !target_paths.contains(path) {
                    validate_restore_path(project_path, path)?;
                    to_delete.push(path.clone());
                }
            }
        }

        // Files to restore/create
        for file in &target_files {
            if !file.stored {
                continue;
            }
            validate_restore_path(project_path, &file.path)?;
            to_restore.push((
                std::path::PathBuf::from(crate::cas::decode_relpath_to_os(&file.path)),
                file.hash.clone(),
                file.executable,
                file.is_symlink,
            ));
        }

        if dry_run {
            println!("Dry run — changes that would be applied:");
            for path in &to_delete {
                let decoded = cas::decode_relpath_to_os(path);
                let display = std::path::Path::new(&decoded).display();
                println!("  DELETE {display}");
            }
            for (path, _, _, _) in &to_restore {
                println!("  RESTORE {}", path.display());
            }
            return Ok(RestoreOutcome {
                snapshot_id: snap_id_str.clone(),
                dry_run: true,
                applied: false,
                files_restored: to_restore.len(),
                files_deleted: to_delete.len(),
                files_to_restore: to_restore
                    .iter()
                    .map(|(path, _, _, _)| path.to_string_lossy().to_string())
                    .collect(),
                files_to_delete: to_delete
                    .iter()
                    .map(|path| {
                        cas::decode_relpath_to_os(path)
                            .to_string_lossy()
                            .to_string()
                    })
                    .collect(),
            });
        }

        if to_delete.len() > 10 && !force {
            if !std::io::stdin().is_terminal() {
                anyhow::bail!(
                    "Refusing to delete {} files without confirmation. Use --force or run in an interactive terminal.",
                    to_delete.len()
                );
            }
            eprintln!(
                "⚠ This will delete {} tracked files. Use --force to skip this prompt.",
                to_delete.len()
            );
            eprint!("Continue? [y/N] ");
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            if !input.trim().eq_ignore_ascii_case("y") {
                println!("Aborted.");
                return Ok(RestoreOutcome {
                    snapshot_id: snap_id_str.clone(),
                    dry_run: false,
                    applied: false,
                    files_restored: 0,
                    files_deleted: 0,
                    files_to_restore: Vec::new(),
                    files_to_delete: Vec::new(),
                });
            }
        }

        tracing::info!("Creating pre-restore snapshot...");
        let cfg = crate::config::Config::load(&uhoh_dir.join("config.toml"))?;
        let _ = snapshot::create_snapshot(
            uhoh_dir,
            database,
            &project.hash,
            project_path,
            "pre-restore",
            Some(&format!("Before restore to {id_str}")),
            &cfg,
            None,
        );

        let mut missing_blobs = Vec::new();
        let mut unstored = Vec::new();
        for f in &target_files {
            if !f.stored {
                unstored.push(f.path.as_str());
                continue;
            }
            if !crate::cas::blob_exists(&blob_root, &f.hash) {
                missing_blobs.push((f.path.as_str(), &f.hash));
            }
        }
        if !unstored.is_empty() {
            eprintln!("⚠ {} file(s) in target snapshot were not stored (likely too large) and will be skipped.", unstored.len());
            for p in unstored.iter().take(10) {
                eprintln!("  - {p}");
            }
            if unstored.len() > 10 {
                eprintln!("  ... and {} more", unstored.len() - 10);
            }
        }
        if !missing_blobs.is_empty() {
            eprintln!(
                "ERROR: {} blob(s) missing; aborting restore.",
                missing_blobs.len()
            );
            for (p, h) in missing_blobs.iter().take(10) {
                let short = &h[..h.len().min(12)];
                eprintln!("  - {p} ({short}...)");
            }
            anyhow::bail!("Cannot restore: required blobs missing");
        }

        let now = chrono::Utc::now();
        let nanos_i64: i64 = now.timestamp_nanos_opt().unwrap_or_else(|| {
            ((now.timestamp() as i128) * 1_000_000_000)
                .try_into()
                .unwrap_or(0)
        });
        let unique_suffix = format!("{}-{}", std::process::id(), nanos_i64);
        let restore_tmp = project_path.join(format!(".uhoh-restore-tmp-{unique_suffix}"));
        std::fs::create_dir_all(&restore_tmp)?;
        cleanup_staging = Some(restore_tmp.clone());

        let bar = indicatif::ProgressBar::new(to_restore.len() as u64);
        bar.set_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40}] {pos}/{len} files")
                .unwrap(),
        );

        for (path, hash, executable, is_symlink) in &to_restore {
            if path.is_absolute()
                || path
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                tracing::warn!("Skipping suspicious path: {}", path.display());
                continue;
            }

            let full_path = restore_tmp.join(path);
            if let Some(parent) = full_path.parent() {
                // Check for symlinks BEFORE creating directories
                check_no_symlink_parents(&restore_tmp, &full_path)?;
                std::fs::create_dir_all(parent)?;
            }

            // Remove existing file/symlink before restoring
            if let Ok(meta) = std::fs::symlink_metadata(&full_path) {
                if meta.file_type().is_symlink() || meta.is_file() {
                    std::fs::remove_file(&full_path)?;
                }
            }

            let content = cas::read_blob(&blob_root, hash)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "Blob {} missing for {}",
                    &hash[..hash.len().min(12)],
                    path.display()
                )
            })?;

            if *is_symlink {
                restore_symlink_target(&content, &full_path)?;
            } else {
                std::fs::write(&full_path, &content)
                    .with_context(|| format!("Failed to write: {}", full_path.display()))?;

                #[cfg(unix)]
                if *executable {
                    use std::os::unix::fs::PermissionsExt;
                    let perms = std::fs::Permissions::from_mode(0o755);
                    std::fs::set_permissions(&full_path, perms)?;
                }
            }

            bar.inc(1);
        }
        bar.finish_and_clear();

        for path in &to_delete {
            validate_restore_path(project_path, path)?;
            let dst = project_path.join(crate::cas::decode_relpath_to_os(path));
            if let Ok(meta) = dst.symlink_metadata() {
                if meta.is_symlink() || meta.is_file() {
                    let _ = std::fs::remove_file(&dst);
                } else if meta.is_dir() {
                    safe_remove_dir_tree_within(project_path, &dst)?;
                }
            }
        }
        for (path, _, _, _) in &to_restore {
            let staged = restore_tmp.join(path);
            let final_dest = project_path.join(path);
            if let Some(parent) = final_dest.parent() {
                std::fs::create_dir_all(parent)?;
                check_no_symlink_parents(project_path, &final_dest)?;
            }
            if let Ok(meta) = final_dest.symlink_metadata() {
                if meta.is_symlink() || meta.is_file() {
                    let _ = std::fs::remove_file(&final_dest);
                } else if meta.is_dir() {
                    safe_remove_dir_tree_within(project_path, &final_dest)?;
                }
            }
            std::fs::rename(&staged, &final_dest).with_context(|| {
                format!(
                    "Failed to move {} -> {}",
                    staged.display(),
                    final_dest.display()
                )
            })?;
        }

        let _ = std::fs::remove_dir_all(&restore_tmp);
        cleanup_staging = None;

        fn remove_empty_dirs(root: &Path, base: &Path) {
            if let Ok(entries) = std::fs::read_dir(root) {
                for e in entries.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        remove_empty_dirs(&p, base);
                    }
                }
            }
            if root != base
                && std::fs::read_dir(root)
                    .map(|mut it| it.next().is_none())
                    .unwrap_or(false)
            {
                let _ = std::fs::remove_dir(root);
            }
        }
        remove_empty_dirs(project_path, project_path);

        Ok(RestoreOutcome {
            snapshot_id: snap_id_str.clone(),
            dry_run: false,
            applied: true,
            files_restored: to_restore.len(),
            files_deleted: to_delete.len(),
            files_to_restore: to_restore
                .iter()
                .map(|(path, _, _, _)| path.to_string_lossy().to_string())
                .collect(),
            files_to_delete: to_delete
                .iter()
                .map(|path| {
                    cas::decode_relpath_to_os(path)
                        .to_string_lossy()
                        .to_string()
                })
                .collect(),
        })
    })();

    if let Some(staging) = cleanup_staging {
        let _ = std::fs::remove_dir_all(staging);
    }

    let outcome = result?;
    if outcome.applied {
        println!(
            "Restored to snapshot {} ({} files restored, {} deleted)",
            outcome.snapshot_id, outcome.files_restored, outcome.files_deleted
        );
    }

    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::safe_remove_dir_tree_within;

    #[cfg(unix)]
    #[test]
    fn safe_remove_dir_tree_rejects_nested_symlink() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("project");
        let victim = project.join("victim");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(&victim).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, victim.join("escape")).unwrap();

        let err = safe_remove_dir_tree_within(&project, &victim).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("containing symlink"));
    }
}
