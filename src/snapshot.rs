use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::cas;
use crate::config::Config;
use crate::db::Database;
use crate::ignore_rules;
// use crate::db::SnapshotRow; // not used directly here
use crate::ai;

/// File metadata cache for efficient change detection.
#[derive(Debug, Clone)]
pub struct CachedFileState {
    pub hash: String,
    pub size: u64,
    pub mtime: SystemTime,
    pub stored: bool,
    pub executable: bool,
    pub storage_method: i64,
    pub is_symlink: bool,
}

/// Create a snapshot for a project. Returns the snapshot ID if one was created.
#[allow(clippy::too_many_arguments)]
pub fn create_snapshot(
    uhoh_dir: &Path,
    database: &Database,
    project_hash: &str,
    project_path: &Path,
    trigger: &str,
    message: Option<&str>,
    config: &Config,
    changed_paths: Option<&[PathBuf]>,
) -> Result<Option<u64>> {
    let blob_root = uhoh_dir.join("blobs");

    // Load previous snapshot for comparison
    let prev_files = load_previous_snapshot_files(database, project_hash)?;

    // Determine changes
    let mut new_files = Vec::new();
    let mut files_for_manifest: Vec<crate::db::SnapFileEntry> = Vec::new();
    let mut deleted_for_manifest: Vec<(String, String, u64, bool, i64)> = Vec::new();
    let mut has_changes = false;

    // Track current path -> (hash, stored) for diff building
    let mut current_hashes: HashMap<String, (String, bool)> = HashMap::new();

    if let Some(paths) = changed_paths {
        // If any changed path is a directory, equals project root, or is a
        // non-existent non-file (deleted directory), fall back to full scan.
        let mut requires_full = false;
        for p in paths {
            if p == project_path || p.is_dir() {
                requires_full = true;
                break;
            }
            // If path doesn't exist, it may be a deleted directory —
            // fall back to full scan to detect child deletions
            if !p.exists() && !p.extension().is_some() {
                // Heuristic: paths without extensions are likely directories.
                // Check if any previous file was under this path.
                if let Ok(rel) = p.strip_prefix(project_path) {
                    let prefix = cas::encode_relpath(rel);
                    let prefix_with_sep = format!("{}/", prefix);
                    if prev_files.keys().any(|k| k.starts_with(&prefix_with_sep)) {
                        requires_full = true;
                        break;
                    }
                }
            }
        }
        if requires_full {
            return create_snapshot(
                uhoh_dir,
                database,
                project_hash,
                project_path,
                trigger,
                message,
                config,
                None,
            );
        }

        // Build set of non-ignored file paths via walker for filtering changed_paths
        // This ensures incremental snapshots respect .gitignore rules
        let walked_set: HashSet<PathBuf> = crate::ignore_rules::build_walker(project_path)
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_some_and(|ft| ft.is_file() || ft.is_symlink()))
            .map(|e| e.into_path())
            .collect();
        let mut rel_changed: HashSet<String> = HashSet::new();
        for p in paths {
            if !p.starts_with(project_path) {
                continue;
            }
            // For existing files, check ignore rules via walker output.
            // For deleted files, include them (they need deletion detection).
            if p.exists() && !walked_set.contains(p) {
                continue; // Ignored by .gitignore
            }
            if let Ok(rel) = p.strip_prefix(project_path) {
                let rel_s = cas::encode_relpath(rel);
                rel_changed.insert(rel_s);
            }
        }

        let mut inserted: HashSet<String> = HashSet::new();
        // Process only changed files
        for rel_path in rel_changed.iter() {
            let abs_path = project_path.join(cas::decode_relpath_to_os(rel_path));
            // Skip marker file
            if abs_path.file_name().is_some_and(|n| n == ".uhoh") {
                continue;
            }

            match std::fs::symlink_metadata(&abs_path) {
                Ok(meta) => {
                    let ft = meta.file_type();
                    if !ft.is_file() && !ft.is_symlink() {
                        continue;
                    }
                    let size = meta.len();
                    let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                    let executable = cas::is_executable(&abs_path);
                    let is_symlink = meta.file_type().is_symlink();
                    if let Some(cached) = prev_files.get(rel_path) {
                        let fs_mtime_ms = mtime_to_millis(mtime);
                        let cached_mtime_ms = mtime_to_millis(cached.mtime);
                        if cached.size == size && cached_mtime_ms == fs_mtime_ms {
                            files_for_manifest.push(crate::db::SnapFileEntry {
                                path: rel_path.clone(),
                                hash: cached.hash.clone(),
                                size: cached.size,
                                stored: cached.stored,
                                executable: cached.executable,
                                mtime: Some(cached_mtime_ms),
                                storage_method: cached.storage_method,
                                is_symlink: cached.is_symlink,
                            });
                            inserted.insert(rel_path.clone());
                            continue;
                        }
                    }

                    if is_symlink {
                        match cas::store_symlink_target(&blob_root, &abs_path) {
                            Ok((hash, size, bytes_written)) => {
                                let is_new_or_changed = prev_files
                                    .get(rel_path)
                                    .map_or(true, |prev| prev.hash != hash || !prev.is_symlink);
                                if is_new_or_changed {
                                    has_changes = true;
                                    new_files.push(rel_path.clone());
                                }
                                let mtime_i = mtime_to_millis(mtime);
                                files_for_manifest.push(crate::db::SnapFileEntry {
                                    path: rel_path.clone(),
                                    hash: hash.clone(),
                                    size,
                                    stored: true,
                                    executable: false,
                                    mtime: Some(mtime_i),
                                    storage_method: cas::StorageMethod::Copy.to_db(),
                                    is_symlink: true,
                                });
                                current_hashes.insert(rel_path.clone(), (hash, true));
                                inserted.insert(rel_path.clone());
                                if bytes_written > 0 {
                                    let _ = database.add_blob_bytes(bytes_written as i64);
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Failed to store symlink for {}: {}", rel_path, e);
                                let is_new_or_changed = prev_files
                                    .get(rel_path)
                                    .map_or(true, |prev| prev.stored || !prev.is_symlink);
                                if is_new_or_changed {
                                    has_changes = true;
                                    new_files.push(rel_path.clone());
                                }
                                files_for_manifest.push(crate::db::SnapFileEntry {
                                    path: rel_path.clone(),
                                    hash: String::new(),
                                    size,
                                    stored: false,
                                    executable: false,
                                    mtime: Some(mtime_to_millis(mtime)),
                                    storage_method: cas::StorageMethod::None.to_db(),
                                    is_symlink: true,
                                });
                                current_hashes.insert(rel_path.clone(), (String::new(), false));
                                inserted.insert(rel_path.clone());
                            }
                        }
                    } else {
                        match cas::store_blob_from_file(
                            &blob_root,
                            &abs_path,
                            config.storage.max_copy_blob_bytes,
                            config.storage.max_binary_blob_bytes,
                            config.storage.max_text_blob_bytes,
                            cfg!(feature = "compression") && config.storage.compress,
                            config.storage.compress_level,
                        ) {
                            Ok((hash, size, method, bytes_written)) => {
                                let is_new_or_changed = prev_files
                                    .get(rel_path)
                                    .map_or(true, |prev| prev.hash != hash);
                                if is_new_or_changed {
                                    has_changes = true;
                                    new_files.push(rel_path.clone());
                                }
                                let mtime_i = mtime_to_millis(mtime);
                                let stored = method.is_recoverable();
                                files_for_manifest.push(crate::db::SnapFileEntry {
                                    path: rel_path.clone(),
                                    hash: hash.clone(),
                                    size,
                                    stored,
                                    executable,
                                    mtime: Some(mtime_i),
                                    storage_method: method.to_db(),
                                    is_symlink: false,
                                });
                                current_hashes.insert(rel_path.clone(), (hash, stored));
                                inserted.insert(rel_path.clone());
                                if bytes_written > 0 {
                                    let _ = database.add_blob_bytes(bytes_written as i64);
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Failed to store blob for {}: {}", rel_path, e);
                                files_for_manifest.push(crate::db::SnapFileEntry {
                                    path: rel_path.clone(),
                                    hash: String::new(),
                                    size,
                                    stored: false,
                                    executable,
                                    mtime: Some(mtime_to_millis(mtime)),
                                    storage_method: cas::StorageMethod::None.to_db(),
                                    is_symlink: false,
                                });
                                current_hashes.insert(rel_path.clone(), (String::new(), false));
                                inserted.insert(rel_path.clone());
                            }
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Potential deletion
                    if let Some(cached) = prev_files.get(rel_path) {
                        has_changes = true;
                        deleted_for_manifest.push((
                            rel_path.clone(),
                            cached.hash.clone(),
                            cached.size,
                            cached.stored,
                            cas::StorageMethod::from_db(cached.storage_method).to_db(),
                        ));
                        // Mark as inserted so carry-forward doesn't copy it back
                        inserted.insert(rel_path.clone());
                    } else {
                        // Deleted path not found as direct file — check if it's a
                        // directory prefix of prev_files entries (e.g., rm -rf src/).
                        // After deletion, is_dir() returns false so the fast-path
                        // check at the top doesn't catch this case. Fall back to
                        // full scan to accurately capture all deletions.
                        let dir_prefix = format!("{}/", rel_path);
                        let has_children = prev_files
                            .keys()
                            .any(|k| k.starts_with(&dir_prefix));
                        if has_children {
                            tracing::debug!(
                                "Deleted path '{}' is a directory with children in prev snapshot; \
                                 forcing full tree walk",
                                rel_path
                            );
                            return create_snapshot(
                                uhoh_dir,
                                database,
                                project_hash,
                                project_path,
                                trigger,
                                message,
                                config,
                                None,
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Cannot stat {}: {}", abs_path.display(), e);
                }
            }
        }

        // Carry forward all other unchanged files from previous snapshot
        for (path, cached) in &prev_files {
            if inserted.contains(path) {
                continue;
            }
            files_for_manifest.push(crate::db::SnapFileEntry {
                path: path.clone(),
                hash: cached.hash.clone(),
                size: cached.size,
                stored: cached.stored,
                executable: cached.executable,
                mtime: Some(mtime_to_millis(cached.mtime)),
                storage_method: cached.storage_method,
                is_symlink: cached.is_symlink,
            });
        }
    } else {
        // Full walk
        // Walk directory respecting ignore rules
        let walker = ignore_rules::build_walker(project_path);
        let mut current_files: HashMap<String, (PathBuf, std::fs::Metadata)> = HashMap::new();

        // Pre-count entries to optionally show a progress bar for large scans
        let mut entries: Vec<ignore::DirEntry> = Vec::new();
        for e in walker.flatten() {
            entries.push(e);
        }
        let show_pb = entries.len() > 1000;
        let pb = if show_pb {
            let pb = ProgressBar::new(entries.len() as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] {bar:40} {pos}/{len} files")
                    .unwrap(),
            );
            Some(pb)
        } else {
            None
        };

        for entry in entries {
            let path = entry.path();
            if path.file_name().is_some_and(|n| n == ".uhoh") {
                if let Some(pb) = &pb {
                    pb.inc(1);
                }
                continue;
            }
            match std::fs::symlink_metadata(path) {
                Ok(meta) => {
                    let ft = meta.file_type();
                    if !ft.is_file() && !ft.is_symlink() {
                        if let Some(pb) = &pb {
                            pb.inc(1);
                        }
                        continue;
                    }
                    let rel_path = match path.strip_prefix(project_path) {
                        Ok(r) => cas::encode_relpath(r),
                        Err(_) => {
                            if let Some(pb) = &pb {
                                pb.inc(1);
                            }
                            continue;
                        }
                    };
                    current_files.insert(rel_path, (path.to_path_buf(), meta));
                }
                Err(e) => {
                    tracing::warn!("Cannot stat {}: {}", path.display(), e);
                }
            }
            if let Some(pb) = &pb {
                pb.inc(1);
            }
        }
        if let Some(pb) = pb {
            pb.finish_and_clear();
        }

        for (rel_path, (abs_path, meta)) in &current_files {
            let size = meta.len();
            let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            let executable = cas::is_executable(abs_path);
            let is_symlink = meta.file_type().is_symlink();
            if let Some(cached) = prev_files.get(rel_path) {
                let fs_mtime_ms = mtime_to_millis(mtime);
                let cached_mtime_ms = mtime_to_millis(cached.mtime);
                if cached.size == size && cached_mtime_ms == fs_mtime_ms {
                    files_for_manifest.push(crate::db::SnapFileEntry {
                        path: rel_path.clone(),
                        hash: cached.hash.clone(),
                        size: cached.size,
                        stored: cached.stored,
                        executable: cached.executable,
                        mtime: Some(cached_mtime_ms),
                        storage_method: cached.storage_method,
                        is_symlink: cached.is_symlink,
                    });
                    continue;
                }
            }
            if is_symlink {
                match cas::store_symlink_target(&blob_root, abs_path) {
                    Ok((hash, size, bytes_written)) => {
                        let is_new_or_changed = prev_files
                            .get(rel_path)
                            .map_or(true, |prev| prev.hash != hash || !prev.is_symlink);
                        if is_new_or_changed {
                            has_changes = true;
                            new_files.push(rel_path.clone());
                        }
                        let mtime_i = mtime_to_millis(mtime);
                        files_for_manifest.push(crate::db::SnapFileEntry {
                            path: rel_path.clone(),
                            hash: hash.clone(),
                            size,
                            stored: true,
                            executable: false,
                            mtime: Some(mtime_i),
                            storage_method: cas::StorageMethod::Copy.to_db(),
                            is_symlink: true,
                        });
                        current_hashes.insert(rel_path.clone(), (hash, true));
                        if bytes_written > 0 {
                            let _ = database.add_blob_bytes(bytes_written as i64);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to store symlink for {}: {}", rel_path, e);
                        let is_new_or_changed = prev_files
                            .get(rel_path)
                            .map_or(true, |prev| prev.stored || !prev.is_symlink);
                        if is_new_or_changed {
                            has_changes = true;
                            new_files.push(rel_path.clone());
                        }
                        files_for_manifest.push(crate::db::SnapFileEntry {
                            path: rel_path.clone(),
                            hash: String::new(),
                            size,
                            stored: false,
                            executable: false,
                            mtime: Some(mtime_to_millis(mtime)),
                            storage_method: cas::StorageMethod::None.to_db(),
                            is_symlink: true,
                        });
                        current_hashes.insert(rel_path.clone(), (String::new(), false));
                    }
                }
            } else {
                match cas::store_blob_from_file(
                    &blob_root,
                    abs_path,
                    config.storage.max_copy_blob_bytes,
                    config.storage.max_binary_blob_bytes,
                    config.storage.max_text_blob_bytes,
                    cfg!(feature = "compression") && config.storage.compress,
                    config.storage.compress_level,
                ) {
                    Ok((hash, size, method, bytes_written)) => {
                        let is_new_or_changed = prev_files
                            .get(rel_path)
                            .map_or(true, |prev| prev.hash != hash);
                        if is_new_or_changed {
                            has_changes = true;
                            new_files.push(rel_path.clone());
                        }
                        let mtime_i = mtime_to_millis(mtime);
                        let stored = method.is_recoverable();
                        files_for_manifest.push(crate::db::SnapFileEntry {
                            path: rel_path.clone(),
                            hash: hash.clone(),
                            size,
                            stored,
                            executable,
                            mtime: Some(mtime_i),
                            storage_method: method.to_db(),
                            is_symlink: false,
                        });
                        current_hashes.insert(rel_path.clone(), (hash, stored));
                        if bytes_written > 0 {
                            let _ = database.add_blob_bytes(bytes_written as i64);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to store blob for {}: {}", rel_path, e);
                        files_for_manifest.push(crate::db::SnapFileEntry {
                            path: rel_path.clone(),
                            hash: String::new(),
                            size,
                            stored: false,
                            executable,
                            mtime: Some(mtime_to_millis(mtime)),
                            storage_method: cas::StorageMethod::None.to_db(),
                            is_symlink: false,
                        });
                        current_hashes.insert(rel_path.clone(), (String::new(), false));
                    }
                }
            }
        }
        // Detect deleted files
        let current_paths: HashSet<&String> = current_files.keys().collect();
        for (path, cached) in &prev_files {
            if !current_paths.contains(path) {
                has_changes = true;
                deleted_for_manifest.push((
                    path.clone(),
                    cached.hash.clone(),
                    cached.size,
                    cached.stored,
                    cas::StorageMethod::from_db(cached.storage_method).to_db(),
                ));
            }
        }
    }

    // If no changes, skip creating a snapshot (unless manual/pre-commit)
    if !has_changes && trigger == "auto" {
        return Ok(None);
    }

    // Dynamic trigger upgrade: if trigger is "auto" and mass deletion is detected,
    // upgrade to "emergency" as a safety net for cases the daemon-side detection missed
    // (e.g., directory deletion on macOS FSEvents emitting a single event for rm -rf).
    let actual_trigger = if trigger == "auto" {
        let deleted_count = deleted_for_manifest.len();
        let prev_count = prev_files.len() as u64;
        if crate::emergency::exceeds_threshold(
            deleted_count,
            prev_count,
            config.watch.emergency_delete_threshold,
            config.watch.emergency_delete_min_files,
        ) {
            let ratio = crate::emergency::deletion_ratio(deleted_count, prev_count);
            tracing::warn!(
                "Dynamic trigger upgrade: auto -> emergency \
                 (deleted={}, baseline={}, ratio={:.3})",
                deleted_count,
                prev_count,
                ratio
            );
            "emergency"
        } else {
            trigger
        }
    } else {
        trigger
    };

    // Override message for dynamically-upgraded emergency snapshots
    let auto_msg: String;
    let msg = if actual_trigger == "emergency" && message.is_none() {
        let deleted_count = deleted_for_manifest.len();
        let prev_count = prev_files.len() as u64;
        let ratio = crate::emergency::deletion_ratio(deleted_count, prev_count);
        auto_msg = format!(
            "Mass delete detected: {}/{} files ({:.1}%)",
            deleted_count, prev_count, ratio * 100.0
        );
        auto_msg.as_str()
    } else {
        message.unwrap_or("")
    };

    // Snapshot ID will be allocated inside the DB transaction
    let snapshot_id: u64 = 0;
    let timestamp = chrono::Utc::now().to_rfc3339();
    let pinned = false;

    let (rowid, snapshot_id) = database.create_snapshot(
        project_hash,
        snapshot_id,
        &timestamp,
        actual_trigger,
        msg,
        pinned,
        &files_for_manifest,
        &deleted_for_manifest,
    )?;

    let file_paths_str = files_for_manifest
        .iter()
        .map(|f| f.path.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let _ = database.index_snapshot_for_search(
        rowid,
        project_hash,
        actual_trigger,
        msg,
        "",
        &file_paths_str,
    );

    // If there is an active operation for this project and this was not a pre-restore snapshot,
    // keep last_snapshot_id updated so undo has a clear end.
    if actual_trigger != "pre-restore" {
        if let Ok(Some((op_id, _))) = database.get_active_operation(project_hash) {
            let _ = database.update_operation_last_snapshot(op_id, snapshot_id);
        }
    }

    // AI summary: attempt now if allowed, else enqueue for later
    if ai::should_run_ai(&config.ai) {
        let uhoh_dir_cl = uhoh_dir.to_path_buf();
        let cfg_cloned = config.clone();
        let files_added: Vec<String> = new_files
            .iter()
            .filter(|p| !prev_files.contains_key(*p))
            .cloned()
            .collect();
        let files_modified: Vec<String> = new_files
            .iter()
            .filter(|p| prev_files.contains_key(*p))
            .cloned()
            .collect();
        let files_deleted: Vec<String> = deleted_for_manifest
            .iter()
            .map(|(p, _, _, _, _)| p.clone())
            .collect();
        let db_rowid = rowid;

        // Build compact diff text for modified files
        let blob_root = uhoh_dir.join("blobs");
        let mut diff_chunks = String::new();
        const MAX_AI_DIFF_FILE_SIZE: u64 = 512 * 1024; // 512KB cap for AI diff
        let max_diff_chars = cfg_cloned.ai.max_context_tokens.saturating_mul(4);
        let mut diff_chars = 0usize;
        let mut diff_truncated = false;
        for path in files_modified.iter().take(10) {
            if diff_chars >= max_diff_chars {
                if !diff_truncated {
                    diff_chunks.push_str("\n[Diff truncated]\n");
                }
                break;
            }
            if let (Some(prev), Some((curr_hash, curr_stored))) =
                (prev_files.get(path), current_hashes.get(path))
            {
                if prev.stored && *curr_stored && !prev.hash.is_empty() && !curr_hash.is_empty() {
                    // Enforce size cap and skip binary
                    if prev.size > MAX_AI_DIFF_FILE_SIZE {
                        let note = format!("--- {path}\n[File too large for AI diff]\n");
                        append_diff_chunk(
                            &mut diff_chunks,
                            &mut diff_chars,
                            max_diff_chars,
                            &mut diff_truncated,
                            &note,
                        );
                        if diff_chars >= max_diff_chars {
                            if !diff_truncated {
                                diff_chunks.push_str("\n[Diff truncated]\n");
                            }
                            break;
                        }
                        continue;
                    }
                    let old = crate::cas::read_blob(&blob_root, &prev.hash).ok().flatten();
                    let new = crate::cas::read_blob(&blob_root, curr_hash).ok().flatten();
                    if let (Some(old), Some(new)) = (old, new) {
                        let head_old = &old[..old.len().min(8192)];
                        let head_new = &new[..new.len().min(8192)];
                        if content_inspector::inspect(head_old).is_binary()
                            || content_inspector::inspect(head_new).is_binary()
                        {
                            let note = format!("--- {path}\n[Binary file]\n");
                            append_diff_chunk(
                                &mut diff_chunks,
                                &mut diff_chars,
                                max_diff_chars,
                                &mut diff_truncated,
                                &note,
                            );
                            if diff_chars >= max_diff_chars {
                                if !diff_truncated {
                                    diff_chunks.push_str("\n[Diff truncated]\n");
                                }
                                break;
                            }
                            continue;
                        }
                        if let (Ok(old_s), Ok(new_s)) =
                            (String::from_utf8(old), String::from_utf8(new))
                        {
                            let d = similar::TextDiff::from_lines(&old_s, &new_s);
                            let header = format!("--- a/{path}\n+++ b/{path}\n");
                            append_diff_chunk(
                                &mut diff_chunks,
                                &mut diff_chars,
                                max_diff_chars,
                                &mut diff_truncated,
                                &header,
                            );
                            for hunk in d.unified_diff().context_radius(2).iter_hunks() {
                                if diff_chars >= max_diff_chars {
                                    if !diff_truncated {
                                        diff_chunks.push_str("\n[Diff truncated]\n");
                                        diff_truncated = true;
                                    }
                                    break;
                                }
                                let hunk_header = format!("{}\n", hunk.header());
                                append_diff_chunk(
                                    &mut diff_chunks,
                                    &mut diff_chars,
                                    max_diff_chars,
                                    &mut diff_truncated,
                                    &hunk_header,
                                );
                                for change in hunk.iter_changes() {
                                    if diff_chars >= max_diff_chars {
                                        if !diff_truncated {
                                            diff_chunks.push_str("\n[Diff truncated]\n");
                                            diff_truncated = true;
                                        }
                                        break;
                                    }
                                    let sign = match change.tag() {
                                        similar::ChangeTag::Delete => '-',
                                        similar::ChangeTag::Insert => '+',
                                        similar::ChangeTag::Equal => ' ',
                                    };
                                    let line = format!("{}{}", sign, change);
                                    append_diff_chunk(
                                        &mut diff_chunks,
                                        &mut diff_chars,
                                        max_diff_chars,
                                        &mut diff_truncated,
                                        &line,
                                    );
                                }
                                if diff_truncated {
                                    break;
                                }
                            }
                            if diff_truncated {
                                break;
                            }
                        }
                    }
                }
            }
            if diff_truncated {
                break;
            }
        }

        std::thread::spawn(move || {
            let cfg = cfg_cloned;
            let files = crate::ai::summary::FileChangeSummary {
                added: files_added,
                deleted: files_deleted,
                modified: files_modified,
            };
            match crate::ai::summary::generate_summary_blocking(
                &uhoh_dir_cl,
                &cfg,
                &diff_chunks,
                &files,
            ) {
                Ok(text) if !text.is_empty() => {
                    if let Ok(db) = crate::db::Database::open(&uhoh_dir_cl.join("uhoh.db")) {
                        let _ = db.set_ai_summary(db_rowid, &text);
                    }
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("AI summary generation failed: {}", e),
            }
        });
    } else {
        // Queue summary for later processing
        let _ = database.enqueue_ai_summary(rowid, project_hash);
    }

    let id_str = cas::id_to_base58(snapshot_id);
    tracing::info!(
        "Snapshot {} created for {} ({} files, {} deleted, trigger={})",
        id_str,
        &project_hash[..project_hash.len().min(12)],
        files_for_manifest.len(),
        deleted_for_manifest.len(),
        actual_trigger,
    );

    // Enforce storage limits after snapshot
    // Compute total project size from manifest entries
    let total_project_size: u64 = files_for_manifest.iter().map(|f| f.size).sum();
    enforce_storage_limit(database, total_project_size, project_hash, config)?;

    Ok(Some(snapshot_id))
}

fn append_diff_chunk(
    out: &mut String,
    chars_used: &mut usize,
    max_chars: usize,
    truncated: &mut bool,
    chunk: &str,
) {
    if *truncated || chunk.is_empty() {
        return;
    }
    if *chars_used >= max_chars {
        out.push_str("\n[Diff truncated]\n");
        *truncated = true;
        return;
    }

    let remaining = max_chars.saturating_sub(*chars_used);
    if chunk.len() <= remaining {
        out.push_str(chunk);
        *chars_used = chars_used.saturating_add(chunk.len());
        return;
    }

    let mut cut = remaining;
    while cut > 0 && !chunk.is_char_boundary(cut) {
        cut -= 1;
    }
    if cut > 0 {
        out.push_str(&chunk[..cut]);
        *chars_used = chars_used.saturating_add(cut);
    }
    out.push_str("\n[Diff truncated]\n");
    *truncated = true;
}

pub fn mtime_to_millis(t: SystemTime) -> i64 {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(e) => {
            let dur = e.duration();
            let millis = dur.as_millis() as i64;
            if dur.subsec_nanos() > 0 || dur.as_secs() > 0 {
                -(millis + 1)
            } else {
                -millis
            }
        }
    }
}

pub fn millis_to_mtime(millis: i64) -> SystemTime {
    if millis >= 0 {
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(millis as u64)
    } else {
        let abs_millis = (millis as i128).unsigned_abs().min(u64::MAX as u128) as u64;
        std::time::UNIX_EPOCH - std::time::Duration::from_millis(abs_millis)
    }
}

// Backward-compatible aliases for older call sites and tests.
pub fn mtime_to_i64(t: SystemTime) -> i64 {
    mtime_to_millis(t)
}

pub fn i64_to_mtime(millis: i64) -> SystemTime {
    millis_to_mtime(millis)
}

#[cfg(test)]
mod tests {
    #[test]
    fn ai_diff_truncation_marker_present() {
        let source = std::fs::read_to_string("src/snapshot.rs").expect("read snapshot source");
        assert!(source.contains("[Diff truncated]"));
        assert!(source.contains("is_char_boundary"));
        assert!(source.contains("max_context_tokens.saturating_mul(4)"));
    }
}

fn enforce_storage_limit(
    database: &Database,
    project_size: u64,
    project_hash: &str,
    config: &Config,
) -> Result<()> {
    let max_blob_size = std::cmp::max(
        (project_size as f64 * config.storage.storage_limit_fraction) as u64,
        config.storage.storage_min_bytes,
    );

    let mut blob_size = database.total_blob_size_for_project(project_hash)?;
    if blob_size <= max_blob_size {
        return Ok(());
    }

    tracing::info!(
        "Blob storage for {} exceeds limit: {:.1}MB > {:.1}MB, pruning...",
        &project_hash[..8],
        blob_size as f64 / 1_048_576.0,
        max_blob_size as f64 / 1_048_576.0,
    );

    // Delete oldest unpinned snapshots until under limit (approximate freed space)
    let emergency_retention =
        chrono::Duration::hours(config.compaction.emergency_expire_hours as i64);
    let now = chrono::Utc::now();
    for snap in database.list_snapshots_oldest_first(project_hash)? {
        if blob_size <= max_blob_size {
            break;
        }
        if snap.pinned {
            continue;
        }
        // Protect emergency snapshots within their retention window
        if snap.trigger == "emergency" {
            if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(&snap.timestamp) {
                let age = now.signed_duration_since(ts.with_timezone(&chrono::Utc));
                if age < emergency_retention {
                    continue;
                }
            }
        }
        // Estimate how much this snapshot frees
        let freed = database.estimate_snapshot_blob_size(snap.rowid)?;
        let _ = database.delete_snapshot(snap.rowid);
        blob_size = blob_size.saturating_sub(freed);
        // Actual blob GC happens via `uhoh gc` or next scheduled GC
    }
    Ok(())
}

/// Load the previous snapshot's file entries as a HashMap for comparison.
fn load_previous_snapshot_files(
    database: &Database,
    project_hash: &str,
) -> Result<HashMap<String, CachedFileState>> {
    let mut map = HashMap::new();
    if let Some(rowid) = database.latest_snapshot_rowid(project_hash)? {
        let files = database.get_snapshot_files(rowid)?;
        for f in files {
            map.insert(
                f.path,
                CachedFileState {
                    hash: f.hash,
                    size: f.size,
                    mtime: millis_to_mtime(f.mtime.unwrap_or(0)),
                    stored: f.stored,
                    executable: f.executable,
                    storage_method: f.storage_method,
                    is_symlink: f.is_symlink,
                },
            );
        }
    }
    Ok(map)
}

// Tree hash computation removed to reduce overhead; table preserved for potential future use.
