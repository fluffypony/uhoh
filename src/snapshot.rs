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
        // If any changed path is a directory or equals project root, fall back to full scan
        let mut requires_full = false;
        for p in paths {
            if p == project_path || p.is_dir() {
                requires_full = true;
                break;
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

        // Build set of relative changed file paths
        let mut rel_changed: HashSet<String> = HashSet::new();
        for p in paths {
            if !p.starts_with(project_path) {
                continue;
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
                        let fs_mtime_secs = mtime_to_i64(mtime);
                        let cached_secs = mtime_to_i64(cached.mtime);
                        if cached.size == size && cached_secs == fs_mtime_secs {
                            files_for_manifest.push(crate::db::SnapFileEntry {
                                path: rel_path.clone(),
                                hash: cached.hash.clone(),
                                size: cached.size,
                                stored: cached.stored,
                                executable: cached.executable,
                                mtime: Some(cached_secs),
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
                                let mtime_i = mtime_to_i64(mtime);
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
                                    mtime: Some(mtime_to_i64(mtime)),
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
                                let mtime_i = mtime_to_i64(mtime);
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
                                    mtime: Some(mtime_to_i64(mtime)),
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
                mtime: Some(mtime_to_i64(cached.mtime)),
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
                let fs_mtime_secs = mtime_to_i64(mtime);
                let cached_secs = mtime_to_i64(cached.mtime);
                if cached.size == size && cached_secs == fs_mtime_secs {
                    files_for_manifest.push(crate::db::SnapFileEntry {
                        path: rel_path.clone(),
                        hash: cached.hash.clone(),
                        size: cached.size,
                        stored: cached.stored,
                        executable: cached.executable,
                        mtime: Some(cached_secs),
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
                        let mtime_i = mtime_to_i64(mtime);
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
                            mtime: Some(mtime_to_i64(mtime)),
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
                        let mtime_i = mtime_to_i64(mtime);
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
                            mtime: Some(mtime_to_i64(mtime)),
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

    // Removed emergency-delete dead code; use provided trigger.
    let actual_trigger = trigger;

    // Tree hashes removed to reduce per-snapshot overhead; left table exists for potential future use
    let tree_hashes: Vec<(String, String)> = Vec::new();

    // Snapshot ID will be allocated inside the DB transaction
    let snapshot_id: u64 = 0;
    let timestamp = chrono::Utc::now().to_rfc3339();
    let msg = message.unwrap_or("");
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
        &tree_hashes,
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
        for path in files_modified.iter().take(10) {
            if diff_chars >= max_diff_chars {
                diff_chunks.push_str("\n[Diff truncated: context budget reached]\n");
                break;
            }
            if let (Some(prev), Some((curr_hash, curr_stored))) =
                (prev_files.get(path), current_hashes.get(path))
            {
                if prev.stored && *curr_stored && !prev.hash.is_empty() && !curr_hash.is_empty() {
                    // Enforce size cap and skip binary
                    if prev.size > MAX_AI_DIFF_FILE_SIZE {
                        let note = format!("--- {path}\n[File too large for AI diff]\n");
                        diff_chars = diff_chars.saturating_add(note.len());
                        diff_chunks.push_str(&note);
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
                            diff_chars = diff_chars.saturating_add(note.len());
                            diff_chunks.push_str(&note);
                            continue;
                        }
                        if let (Ok(old_s), Ok(new_s)) =
                            (String::from_utf8(old), String::from_utf8(new))
                        {
                            let d = similar::TextDiff::from_lines(&old_s, &new_s);
                            let header = format!("--- a/{path}\n+++ b/{path}\n");
                            diff_chars = diff_chars.saturating_add(header.len());
                            diff_chunks.push_str(&header);
                            for hunk in d.unified_diff().context_radius(2).iter_hunks() {
                                if diff_chars >= max_diff_chars {
                                    diff_chunks
                                        .push_str("\n[Diff truncated: context budget reached]\n");
                                    break;
                                }
                                let hunk_header = format!("{}\n", hunk.header());
                                diff_chars = diff_chars.saturating_add(hunk_header.len());
                                diff_chunks.push_str(&hunk_header);
                                for change in hunk.iter_changes() {
                                    if diff_chars >= max_diff_chars {
                                        diff_chunks.push_str(
                                            "\n[Diff truncated: context budget reached]\n",
                                        );
                                        break;
                                    }
                                    let sign = match change.tag() {
                                        similar::ChangeTag::Delete => '-',
                                        similar::ChangeTag::Insert => '+',
                                        similar::ChangeTag::Equal => ' ',
                                    };
                                    diff_chars =
                                        diff_chars.saturating_add(1 + change.to_string().len());
                                    diff_chunks.push(sign);
                                    diff_chunks.push_str(&change.to_string());
                                }
                            }
                        }
                    }
                }
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

pub fn mtime_to_i64(t: SystemTime) -> i64 {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(e) => {
            let dur = e.duration();
            let secs = dur.as_secs() as i64;
            if dur.subsec_nanos() > 0 {
                -(secs + 1)
            } else {
                -secs
            }
        }
    }
}

pub fn i64_to_mtime(secs: i64) -> SystemTime {
    if secs >= 0 {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs as u64)
    } else {
        let abs_secs = (secs as i128).unsigned_abs().min(u64::MAX as u128) as u64;
        std::time::UNIX_EPOCH - std::time::Duration::from_secs(abs_secs)
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
    for snap in database.list_snapshots_oldest_first(project_hash)? {
        if blob_size <= max_blob_size {
            break;
        }
        if snap.pinned {
            continue;
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
                    mtime: i64_to_mtime(f.mtime.unwrap_or(0)),
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
