use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::Path;

use crate::cas;
use crate::db::{Database, ProjectEntry};
use crate::snapshot;

pub(crate) const RESTORE_IN_PROGRESS_FILE: &str = ".restore-in-progress";

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

pub struct PreRestoreSnapshot<'a> {
    pub trigger: &'a str,
    pub message: Option<String>,
    pub config: &'a crate::config::Config,
}

pub struct RestoreRequest<'a> {
    pub snapshot_id: &'a str,
    pub target_path: Option<&'a str>,
    pub dry_run: bool,
    pub force: bool,
    pub pre_restore_snapshot: Option<PreRestoreSnapshot<'a>>,
    pub confirm_large_delete: Option<&'a dyn Fn(usize) -> Result<bool>>,
}

pub fn restore_project(
    uhoh_dir: &Path,
    database: &Database,
    project: &ProjectEntry,
    request: RestoreRequest<'_>,
) -> Result<RestoreOutcome> {
    let runtime = crate::restore_runtime::RestoreRuntime::new(
        std::sync::Arc::new(database.clone_handle()),
        uhoh_dir.to_path_buf(),
    );
    crate::restore_runtime::restore_project(&runtime, project, request)
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
                eprintln!(
                    "WARNING: Cannot create symlink at {}: Windows requires Developer Mode or elevated privileges. \
                     Writing target path as regular file instead (symlink nature will be lost on re-snapshot).",
                    full_path.display()
                );
                tracing::warn!("Symlink degraded to regular file: {}", full_path.display());
                std::fs::write(full_path, content)?;
            }
            Err(e) => match std::os::windows::fs::symlink_dir(target_path, full_path) {
                Ok(_) => {}
                Err(_) => {
                    eprintln!(
                        "WARNING: Cannot create symlink at {}: {}. \
                         Writing target path as regular file instead (symlink nature will be lost on re-snapshot).",
                        full_path.display(), e
                    );
                    tracing::warn!("Symlink degraded to regular file: {}", full_path.display());
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

    // Walk the tree manually to handle symlinks safely: remove symlink entries
    // themselves without following them, and only recurse into real directories.
    fn remove_tree_safe(dir: &Path, project_root: &Path) -> Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let meta = std::fs::symlink_metadata(&path)?;
            if meta.file_type().is_symlink() {
                // Remove the symlink itself, never follow it
                std::fs::remove_file(&path)
                    .with_context(|| format!("Failed to remove symlink: {}", path.display()))?;
            } else if meta.is_dir() {
                // Verify the real directory is still within project bounds
                if let Ok(canon) = dunce::canonicalize(&path) {
                    if !canon.starts_with(project_root) {
                        tracing::warn!(
                            "Skipping directory outside project root: {}",
                            path.display()
                        );
                        continue;
                    }
                }
                remove_tree_safe(&path, project_root)?;
                let _ = std::fs::remove_dir(&path);
            } else {
                std::fs::remove_file(&path)?;
            }
        }
        Ok(())
    }

    remove_tree_safe(&target_canonical, &project_canonical)?;
    let _ = std::fs::remove_dir(&target_canonical);
    Ok(())
}

/// Quick BLAKE3 hash of a file for comparison. Returns hex string.
fn quick_hash_file(path: &Path) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    let mut f = std::fs::File::open(path)?;
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = std::io::Read::read(&mut f, &mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

pub(crate) fn apply_restore(
    uhoh_dir: &Path,
    database: &Database,
    project: &ProjectEntry,
    request: RestoreRequest<'_>,
) -> Result<RestoreOutcome> {
    let RestoreRequest {
        snapshot_id: id_str,
        target_path,
        dry_run,
        force,
        pre_restore_snapshot,
        confirm_large_delete,
    } = request;

    let snap = database
        .find_snapshot_by_base58(&project.hash, id_str)?
        .context("Snapshot not found")?;
    let snap_id_str = cas::id_to_base58(snap.snapshot_id);

    let project_path = Path::new(&project.current_path);
    let blob_root = uhoh_dir.join("blobs");
    let mut target_files = database.get_snapshot_files(snap.rowid)?;

    let target_filter = target_path
        .map(|path| {
            crate::resolve::validate_path_within_project(project_path, path)?;
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

    let result = (|| -> Result<RestoreOutcome> {
        // Files to delete: in current manifest but not in target manifest
        if let Some(filter) = target_filter.as_ref() {
            if current_tracked.contains(filter) && !target_paths.contains(filter) {
                crate::resolve::validate_path_within_project(project_path, filter)?;
                to_delete.push(filter.clone());
            }
        } else {
            for path in &current_tracked {
                if !target_paths.contains(path) {
                    crate::resolve::validate_path_within_project(project_path, path)?;
                    to_delete.push(path.clone());
                }
            }
        }

        // Files to restore/create — skip files whose current content already matches target
        for file in &target_files {
            if !file.stored {
                continue;
            }
            crate::resolve::validate_path_within_project(project_path, &file.path)?;
            let rel = crate::cas::decode_relpath_to_os(&file.path);
            let abs_path = project_path.join(&rel);
            // Skip if current file already matches target (hash, type, and exec bit)
            if abs_path.exists() && !file.is_symlink {
                if let Ok(meta) = std::fs::symlink_metadata(&abs_path) {
                    // Only skip regular files (not symlinks masquerading as files)
                    if meta.file_type().is_file() {
                        let exec_matches = crate::cas::is_executable(&abs_path) == file.executable;
                        if exec_matches {
                            if let Ok(current_hash) = quick_hash_file(&abs_path) {
                                if current_hash == file.hash {
                                    continue;
                                }
                            }
                        }
                    }
                }
            }
            to_restore.push((
                std::path::PathBuf::from(rel),
                file.hash.clone(),
                file.executable,
                file.is_symlink,
            ));
        }

        if dry_run {
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
            let confirmed = match confirm_large_delete {
                Some(confirm) => confirm(to_delete.len())?,
                None => false,
            };
            if !confirmed {
                anyhow::bail!(
                    "Refusing to delete {} tracked files without confirmation. Re-run with force or confirm the restore from the caller.",
                    to_delete.len()
                );
            }
        }

        if let Some(pre_restore) = pre_restore_snapshot {
            tracing::info!("Creating pre-restore snapshot...");
            snapshot::create_snapshot(
                uhoh_dir,
                database,
                pre_restore.config,
                snapshot::CreateSnapshotRequest {
                    project_hash: &project.hash,
                    project_path,
                    trigger: pre_restore.trigger,
                    message: pre_restore.message.as_deref(),
                    changed_paths: None,
                },
            )
            .context("Pre-restore snapshot failed; aborting restore")?;
        }

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
            tracing::warn!(
                "{} file(s) in target snapshot were not stored and will be skipped during restore",
                unstored.len()
            );
        }
        if !missing_blobs.is_empty() {
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
        }

        for path in &to_delete {
            crate::resolve::validate_path_within_project(project_path, path)?;
            let dst = project_path.join(crate::cas::decode_relpath_to_os(path));
            if let Ok(meta) = dst.symlink_metadata() {
                if meta.is_dir() {
                    safe_remove_dir_tree_within(project_path, &dst)?;
                } else {
                    // Covers files, symlinks, FIFOs, sockets, etc.
                    let _ = std::fs::remove_file(&dst);
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
                if meta.is_dir() {
                    safe_remove_dir_tree_within(project_path, &final_dest)?;
                } else {
                    let _ = std::fs::remove_file(&final_dest);
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
                    // Use symlink_metadata to avoid following symlinks
                    if let Ok(meta) = std::fs::symlink_metadata(&p) {
                        if meta.is_dir() && !meta.file_type().is_symlink() {
                            remove_empty_dirs(&p, base);
                        }
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

    result
}

#[cfg(test)]
mod tests {
    use super::safe_remove_dir_tree_within;

    #[cfg(unix)]
    #[test]
    fn safe_remove_dir_tree_handles_nested_symlink_safely() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("project");
        let victim = project.join("victim");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(&victim).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("important.txt"), "do not delete").unwrap();
        std::os::unix::fs::symlink(&outside, victim.join("escape")).unwrap();

        // Should succeed: symlink is removed without following it
        safe_remove_dir_tree_within(&project, &victim).unwrap();

        // The symlink target (outside directory) must NOT have been affected
        assert!(outside.exists(), "outside directory should still exist");
        assert!(
            outside.join("important.txt").exists(),
            "file in outside directory should be untouched"
        );
        // The victim directory itself should be gone
        assert!(!victim.exists(), "victim directory should be removed");
    }
}
