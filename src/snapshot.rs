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
    let mut files_for_manifest: Vec<(String, String, u64, bool, bool, Option<i64>)> = Vec::new();
    let mut deleted_for_manifest: Vec<(String, String, u64, bool)> = Vec::new();
    let mut has_changes = false;

    // Track current path -> (hash, stored) for diff building
    let mut current_hashes: HashMap<String, (String, bool)> = HashMap::new();

    for (rel_path, (abs_path, meta)) in &current_files {
        let size = meta.len();
        let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let executable = cas::is_executable(abs_path);

        // Check if we can skip hashing (mtime + size unchanged)
        if let Some(cached) = prev_files.get(rel_path) {
            if cached.size == size && cached.mtime == mtime {
                // Carry forward unchanged file
                files_for_manifest.push((
                    rel_path.clone(),
                    cached.hash.clone(),
                    cached.size,
                    cached.stored,
                    cached.executable,
                    Some(cached.mtime.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64),
                ));
                continue;
            }
        }

        // File is new or changed — hash and store
        match cas::store_blob_from_file(
            &blob_root,
            abs_path,
            config.storage.max_binary_blob_bytes,
            config.storage.max_text_blob_bytes,
        ) {
            Ok((hash, size, stored)) => {
                let is_new_or_changed = prev_files
                    .get(rel_path)
                    .map_or(true, |prev| prev.hash != hash);
                if is_new_or_changed {
                    has_changes = true;
                    new_files.push(rel_path.clone());
                }
                let mtime_i = mtime.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                files_for_manifest.push((rel_path.clone(), hash, size, stored, executable, Some(mtime_i)));
                current_hashes.insert(rel_path.clone(), (files_for_manifest.last().unwrap().1.clone(), stored));
            }
            Err(e) => {
                tracing::warn!("Failed to store blob for {}: {}", rel_path, e);
                // Record as unstored
                files_for_manifest.push((
                    rel_path.clone(),
                    String::new(),
                    size,
                    false,
                    executable,
                    Some(mtime.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64),
                ));
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
                "emergency-delete"
            } else {
                trigger
            }
        } else {
            trigger
        }
    } else {
        trigger
    };

    // Compute tree hashes for efficient future diffs
    let tree_hashes = compute_tree_hashes(&files_for_manifest);

    // Get next snapshot ID (atomic via SQLite transaction)
    let snapshot_id = database.next_snapshot_id(project_hash)?;
    let timestamp = chrono::Utc::now().to_rfc3339();
    let msg = message.unwrap_or("");
    let pinned = false;

    let rowid = database.create_snapshot(
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
        let files_added: Vec<String> = new_files.iter().filter(|p| !prev_files.contains_key(*p)).cloned().collect();
        let files_modified: Vec<String> = new_files.iter().filter(|p| prev_files.contains_key(*p)).cloned().collect();
        let files_deleted: Vec<String> = deleted_for_manifest.iter().map(|(p, _, _, _)| p.clone()).collect();
        let db_rowid = rowid;

        // Build compact diff text for modified files
        let blob_root = uhoh_dir.join("blobs");
        let mut diff_chunks = String::new();
        for path in files_modified.iter().take(10) {
            if let (Some(prev), Some((curr_hash, curr_stored))) = (prev_files.get(path), current_hashes.get(path)) {
                if prev.stored && *curr_stored && !prev.hash.is_empty() && !curr_hash.is_empty() {
                    let old = crate::cas::read_blob(&blob_root, &prev.hash).ok().flatten();
                    let new = crate::cas::read_blob(&blob_root, curr_hash).ok().flatten();
                    if let (Some(old), Some(new)) = (old, new) {
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
            let cfg = match crate::config::Config::load(&uhoh_dir_cl.join("config.toml")) { Ok(c) => c, Err(_) => return };
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
        &project_hash[..12],
        files_for_manifest.len(),
        deleted_for_manifest.len(),
        actual_trigger,
    );

    // Enforce storage limits after snapshot
    enforce_storage_limit(database, project_path, project_hash, &config)?;

    Ok(Some(snapshot_id))
}

fn enforce_storage_limit(
    database: &Database,
    project_path: &Path,
    project_hash: &str,
    config: &Config,
) -> Result<()> {
    // Calculate project directory size
    let mut project_size = 0u64;
    for entry in ignore::WalkBuilder::new(project_path).build() {
        if let Ok(e) = entry {
            if let Ok(meta) = e.metadata() {
                if meta.is_file() { project_size += meta.len(); }
            }
        }
    }
    let max_blob_size = std::cmp::max(
        (project_size as f64 * config.storage.storage_limit_fraction) as u64,
        config.storage.storage_min_bytes,
    );

    let blob_size = database.total_blob_size_for_project(project_hash)?;
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
        let _ = database.delete_snapshot(snap.rowid);
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
                },
            );
        }
    }
    Ok(map)
}

/// Check if .git/HEAD changed recently (indicating a branch switch).
fn is_likely_git_operation(project_path: &Path) -> bool {
    let head_path = project_path.join(".git/HEAD");
    if let Ok(meta) = std::fs::metadata(&head_path) {
        if let Ok(modified) = meta.modified() {
            if let Ok(elapsed) = modified.elapsed() {
                return elapsed.as_secs() < 10;
            }
        }
    }
    false
}

/// Compute Merkle-style tree hashes for directories.
/// Each directory gets a hash of its children's hashes, enabling O(log n) diff.
fn compute_tree_hashes(
    files: &[(String, String, u64, bool, bool, Option<i64>)],
) -> Vec<(String, String)> {
    // Group files by directory
    let mut dir_contents: HashMap<String, Vec<&str>> = HashMap::new();
    for (path, hash, _, _, _, _) in files {
        let dir = match path.rfind('/') {
            Some(pos) => &path[..pos],
            None => ".",
        };
        dir_contents.entry(dir.to_string()).or_default().push(hash);
    }

    let mut tree_hashes = Vec::new();
    for (dir, hashes) in &dir_contents {
        let mut hasher = blake3::Hasher::new();
        // Sort for determinism
        let mut sorted_hashes: Vec<&&str> = hashes.iter().collect();
        sorted_hashes.sort();
        for h in sorted_hashes {
            hasher.update(h.as_bytes());
        }
        let tree_hash = hasher.finalize().to_hex().to_string();
        tree_hashes.push((dir.clone(), tree_hash));
    }

    // Compute root hash
    if !tree_hashes.is_empty() {
        let mut root_hasher = blake3::Hasher::new();
        let mut sorted_dirs: Vec<&(String, String)> = tree_hashes.iter().collect();
        sorted_dirs.sort_by_key(|(d, _)| d.as_str());
        for (_, h) in &sorted_dirs {
            root_hasher.update(h.as_bytes());
        }
        tree_hashes.push((".".to_string(), root_hasher.finalize().to_hex().to_string()));
    }

    tree_hashes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_tree_hashes() {
        let files = vec![
            ("src/main.rs".to_string(), "abc123".to_string(), 100u64, true, false, None),
            ("src/lib.rs".to_string(), "def456".to_string(), 200u64, true, false, None),
            ("README.md".to_string(), "ghi789".to_string(), 50u64, true, false, None),
        ];
        let hashes = compute_tree_hashes(&files);
        assert!(!hashes.is_empty());
        // Should have entries for "src", ".", and a root
        let dirs: Vec<&str> = hashes.iter().map(|(d, _)| d.as_str()).collect();
        assert!(dirs.contains(&"src"));
    }
}
