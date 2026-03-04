use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use indicatif::{ProgressBar, ProgressStyle};

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
}

/// Create a snapshot for a project. Returns the snapshot ID if one was created.
pub fn create_snapshot(
    uhoh_dir: &Path,
    database: &Database,
    project_hash: &str,
    project_path: &Path,
    trigger: &str,
    message: Option<&str>,
    config: &Config,
) -> Result<Option<u64>> {
    let blob_root = uhoh_dir.join("blobs");

    // Walk directory respecting ignore rules
    let walker = ignore_rules::build_walker(project_path);
    let mut current_files: HashMap<String, (PathBuf, std::fs::Metadata)> = HashMap::new();
    let mut total_project_size: u64 = 0;

    // Pre-count entries to optionally show a progress bar for large scans
    let mut entries: Vec<ignore::DirEntry> = Vec::new();
    for entry in walker { if let Ok(e) = entry { entries.push(e); } }
    let show_pb = entries.len() > 1000;
    let pb = if show_pb {
        let pb = ProgressBar::new(entries.len() as u64);
        pb.set_style(ProgressStyle::default_bar().template("[{elapsed_precise}] {bar:40} {pos}/{len} files").unwrap());
        Some(pb)
    } else { None };

    for entry in entries {
        let entry = match entry {
            e => e,
        };

        let path = entry.path();
        if !path.is_file() {
            if let Some(pb) = &pb { pb.inc(1); }
            continue;
        }
        // Skip the marker file itself
        if path.file_name().map_or(false, |n| n == ".uhoh") {
            if let Some(pb) = &pb { pb.inc(1); }
            continue;
        }

        let rel_path = match path.strip_prefix(project_path) {
            Ok(r) => cas::normalize_path(r),
            Err(_) => continue,
        };

        // Skip non-UTF-8 paths (with warning) to prevent TOML/DB corruption
        if path.to_str().is_none() {
            tracing::warn!("Skipping non-UTF-8 path: {:?}", path);
            continue;
        }

        match path.metadata() {
            Ok(meta) => {
                total_project_size = total_project_size.saturating_add(meta.len());
                current_files.insert(rel_path, (path.to_path_buf(), meta));
            }
            Err(e) => {
                tracing::warn!("Cannot stat {}: {}", path.display(), e);
            }
        }
        if let Some(pb) = &pb { pb.inc(1); }
    }
    if let Some(pb) = pb { pb.finish_and_clear(); }

    // Load previous snapshot for comparison
    let prev_files = load_previous_snapshot_files(database, project_hash)?;

    // Determine changes
    let mut new_files = Vec::new();
    let mut files_for_manifest: Vec<crate::db::SnapFileEntry> = Vec::new();
    let mut deleted_for_manifest: Vec<(String, String, u64, bool, i64)> = Vec::new();
    let mut has_changes = false;

    // Track current path -> (hash, stored) for diff building
    let mut current_hashes: HashMap<String, (String, bool)> = HashMap::new();

    for (rel_path, (abs_path, meta)) in &current_files {
        let size = meta.len();
        let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let executable = cas::is_executable(abs_path);

        // Check if we can skip hashing (mtime + size unchanged)
        if let Some(cached) = prev_files.get(rel_path) {
            let fs_mtime_secs = mtime
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let cached_secs = cached
                .mtime
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            if cached.size == size && cached_secs == fs_mtime_secs {
                // Carry forward unchanged file
                files_for_manifest.push(crate::db::SnapFileEntry {
                    path: rel_path.clone(),
                    hash: cached.hash.clone(),
                    size: cached.size,
                    stored: cached.stored,
                    executable: cached.executable,
                    mtime: Some(cached.mtime.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64),
                    storage_method: cached.storage_method,
                });
                continue;
            }
        }

        // File is new or changed — hash and store
        match cas::store_blob_from_file(
            &blob_root,
            abs_path,
            config.storage.max_copy_blob_bytes,
        ) {
            Ok((hash, size, method)) => {
                let is_new_or_changed = prev_files
                    .get(rel_path)
                    .map_or(true, |prev| prev.hash != hash);
                if is_new_or_changed {
                    has_changes = true;
                    new_files.push(rel_path.clone());
                }
                let mtime_i = mtime.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                let stored = method.is_recoverable();
                files_for_manifest.push(crate::db::SnapFileEntry { path: rel_path.clone(), hash: hash.clone(), size, stored, executable, mtime: Some(mtime_i), storage_method: method.to_db() });
                current_hashes.insert(rel_path.clone(), (hash, stored));
            }
            Err(e) => {
                tracing::warn!("Failed to store blob for {}: {}", rel_path, e);
                // Record as unstored
                files_for_manifest.push(crate::db::SnapFileEntry { path: rel_path.clone(), hash: String::new(), size, stored: false, executable, mtime: Some(mtime.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64), storage_method: cas::StorageMethod::None.to_db() });
                current_hashes.insert(rel_path.clone(), (String::new(), false));
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

    // If no changes, skip creating a snapshot (unless manual/pre-commit)
    if !has_changes && trigger == "auto" {
        return Ok(None);
    }

    // Check emergency delete threshold
    let actual_trigger = if trigger == "auto" && !deleted_for_manifest.is_empty() {
        let prev_count = prev_files.len();
        let del_count = deleted_for_manifest.len();
        if prev_count > 0
            && del_count >= config.watch.emergency_delete_min_files
            && (del_count as f64 / prev_count as f64) > config.watch.emergency_delete_threshold
        {
            // Check if this looks like a git branch switch (suppress false positives)
            if !is_likely_git_operation(project_path) {
                // Apply a short cooldown to avoid repeated emergency snapshots
                static mut LAST_EMERGENCY_AT: Option<std::time::Instant> = None;
                let now = std::time::Instant::now();
                let fire = unsafe {
                    match LAST_EMERGENCY_AT {
                        Some(t) => now.duration_since(t) > std::time::Duration::from_secs(30),
                        None => true,
                    }
                };
                if fire {
                    unsafe { LAST_EMERGENCY_AT = Some(now); }
                    "emergency-delete"
                } else { trigger }
            } else {
                trigger
            }
        } else {
            trigger
        }
    } else {
        trigger
    };

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

    // If there is an active operation for this project and this was not a pre-restore snapshot,
    // keep last_snapshot_id updated so undo has a clear end.
    if actual_trigger != "pre-restore" {
        if let Ok(Some((op_id, _))) = database.get_active_operation(project_hash) {
            let _ = database.update_operation_last_snapshot(op_id, snapshot_id);
        }
    }

    // Fire-and-forget AI summary generation via ai::summary
    if ai::should_run_ai(&config.ai) {
        let uhoh_dir_cl = uhoh_dir.to_path_buf();
        let cfg_cloned = config.clone();
        let files_added: Vec<String> = new_files.iter().filter(|p| !prev_files.contains_key(*p)).cloned().collect();
        let files_modified: Vec<String> = new_files.iter().filter(|p| prev_files.contains_key(*p)).cloned().collect();
        let files_deleted: Vec<String> = deleted_for_manifest.iter().map(|(p, _, _, _, _)| p.clone()).collect();
        let db_rowid = rowid;

        // Build compact diff text for modified files
        let blob_root = uhoh_dir.join("blobs");
        let mut diff_chunks = String::new();
        const MAX_AI_DIFF_FILE_SIZE: u64 = 512 * 1024; // 512KB cap for AI diff
        for path in files_modified.iter().take(10) {
            if let (Some(prev), Some((curr_hash, curr_stored))) = (prev_files.get(path), current_hashes.get(path)) {
                if prev.stored && *curr_stored && !prev.hash.is_empty() && !curr_hash.is_empty() {
                    // Enforce size cap and skip binary
                    if prev.size > MAX_AI_DIFF_FILE_SIZE || current_files.get(path.as_str()).map(|(_, m)| m.len()).unwrap_or(0) > MAX_AI_DIFF_FILE_SIZE {
                        diff_chunks.push_str(&format!("--- {}\n[File too large for AI diff]\n", path));
                        continue;
                    }
                    let old = crate::cas::read_blob(&blob_root, &prev.hash).ok().flatten();
                    let new = crate::cas::read_blob(&blob_root, curr_hash).ok().flatten();
                    if let (Some(old), Some(new)) = (old, new) {
                        let head_old = &old[..old.len().min(8192)];
                        let head_new = &new[..new.len().min(8192)];
                        if content_inspector::inspect(head_old).is_binary() || content_inspector::inspect(head_new).is_binary() {
                            diff_chunks.push_str(&format!("--- {}\n[Binary file]\n", path));
                            continue;
                        }
                        if let (Ok(old_s), Ok(new_s)) = (String::from_utf8(old), String::from_utf8(new)) {
                            let d = similar::TextDiff::from_lines(&old_s, &new_s);
                            diff_chunks.push_str(&format!("--- a/{}\n+++ b/{}\n", path, path));
                            for hunk in d.unified_diff().context_radius(2).iter_hunks() {
                                diff_chunks.push_str(&format!("{}\n", hunk.header()));
                                for change in hunk.iter_changes() {
                                    let sign = match change.tag() { similar::ChangeTag::Delete => '-', similar::ChangeTag::Insert => '+', similar::ChangeTag::Equal => ' '};
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
            let files = crate::ai::summary::FileChangeSummary { added: files_added, deleted: files_deleted, modified: files_modified };
            match crate::ai::summary::generate_summary_blocking(&uhoh_dir_cl, &cfg, &diff_chunks, &files) {
                Ok(text) if !text.is_empty() => {
                    if let Ok(db) = crate::db::Database::open(&uhoh_dir_cl.join("uhoh.db")) {
                        let _ = db.set_ai_summary(db_rowid, &text);
                }
            }
                Ok(_) => {}
                Err(e) => tracing::warn!("AI summary generation failed: {}", e),
            }
        });
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
    enforce_storage_limit(database, total_project_size, project_hash, &config)?;

    Ok(Some(snapshot_id))
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
    if blob_size <= max_blob_size { return Ok(()); }

    tracing::info!(
        "Blob storage for {} exceeds limit: {:.1}MB > {:.1}MB, pruning...",
        &project_hash[..8],
        blob_size as f64 / 1_048_576.0,
        max_blob_size as f64 / 1_048_576.0,
    );

    // Delete oldest unpinned snapshots until under limit (approximate freed space)
    for snap in database.list_snapshots_oldest_first(project_hash)? {
        if blob_size <= max_blob_size { break; }
        if snap.pinned { continue; }
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
                    mtime: f
                        .mtime
                        .and_then(|s| std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(s as u64)))
                        .unwrap_or(SystemTime::UNIX_EPOCH),
                    stored: f.stored,
                    executable: f.executable,
                    storage_method: f.storage_method,
                },
            );
        }
    }
    Ok(map)
}

/// Check if .git/HEAD changed recently (indicating a branch switch).
fn is_likely_git_operation(project_path: &Path) -> bool {
    let git_dir = project_path.join(".git");
    let now = std::time::SystemTime::now();
    let threshold = std::time::Duration::from_secs(10);
    let indicators = [
        git_dir.join("HEAD"),
        git_dir.join("index.lock"),
        git_dir.join("MERGE_HEAD"),
        git_dir.join("REBASE_HEAD"),
        git_dir.join("CHERRY_PICK_HEAD"),
        git_dir.join("BISECT_LOG"),
        git_dir.join("ORIG_HEAD"),
        git_dir.join("refs/stash"),
        git_dir.join("logs/refs/stash"),
    ];
    for indicator in &indicators {
        if let Ok(meta) = std::fs::metadata(indicator) {
            if let Ok(mtime) = meta.modified() {
                if let Ok(age) = now.duration_since(mtime) {
                    if age < threshold { return true; }
                }
            }
        }
    }
    if git_dir.join("index.lock").exists() { return true; }
    false
}

// Tree hash computation removed to reduce overhead; table preserved for potential future use.
