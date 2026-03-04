use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::cas;
use crate::config::Config;
use crate::db::Database;
use crate::ignore_rules;
use crate::db::SnapshotRow;
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
) -> Result<Option<u64>> {
    let config = Config::load(&uhoh_dir.join("config.toml"))?;
    let blob_root = uhoh_dir.join("blobs");

    // Walk directory respecting ignore rules
    let walker = ignore_rules::build_walker(project_path);
    let mut current_files: HashMap<String, (PathBuf, std::fs::Metadata)> = HashMap::new();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!("Error walking directory: {}", e);
                continue;
            }
        };

        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        // Skip the marker file itself
        if path.file_name().map_or(false, |n| n == ".uhoh") {
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
    }

    // Load previous snapshot for comparison
    let prev_files = load_previous_snapshot_files(database, project_hash)?;

    // Determine changes
    let mut new_files = Vec::new();
    let mut files_for_manifest: Vec<(String, String, u64, bool, bool)> = Vec::new();
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
                files_for_manifest.push((rel_path.clone(), hash, size, stored, executable));
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
    let pinned = actual_trigger == "emergency-delete";

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
            // Update last_snapshot_id to the most recent snapshot id
            let last_id = snapshot_id;
            let _ = database.close_operation_with_last(op_id, last_id); // harmless if already closed
        }
    }

    // Fire-and-forget AI summary generation
    if ai::should_run_ai(&config.ai) {
        let uhoh_dir = uhoh_dir.to_path_buf();
        let project_hash_string = project_hash.to_string();
        let blob_root = uhoh_dir.join("blobs");
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
        let files_deleted: Vec<String> = deleted_for_manifest.iter().map(|(p, _, _, _)| p.clone()).collect();

        // Build a compact diff text (limited number of files to keep it small)
        let mut diff_chunks = String::new();
        let max_files = 10usize;
        for path in files_modified.iter().take(max_files) {
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
                } else {
                    diff_chunks.push_str(&format!("--- a/{}\n+++ b/{}\n(binary or unstored change)\n", path, path));
                }
            }
        }

        std::thread::spawn(move || {
            // Select model
            let cfg = match crate::config::Config::load(&uhoh_dir.join("config.toml")) { Ok(c) => c, Err(_) => return };
            let tiers = if cfg.ai.models.is_empty() { crate::ai::models::default_model_tiers() } else { cfg.ai.models.clone() };
            // Choose highest tier that fits RAM with 2GB margin
            let (selected_name, selected_file) = {
                use sysinfo::System;
                let mut sys = System::new();
                sys.refresh_memory();
                let total = sys.total_memory() / (1024 * 1024 * 1024);
                let mut chosen: Option<(String,String,u64)> = None;
                for t in tiers.iter() { if total >= t.min_ram_gb + 2 { chosen = Some((t.name.clone(), t.filename.clone(), t.min_ram_gb)); } }
                if let Some((n,f,_)) = chosen { (n,f) } else { (tiers[0].name.clone(), tiers[0].filename.clone()) }
            };

            let model_path = uhoh_dir.join("sidecar").join(&selected_file);
            if !model_path.exists() { 
                tracing::warn!("AI model file not found: {}", model_path.display());
                return; 
            }

            // Spawn sidecar
            let sidecar = match crate::ai::sidecar::Sidecar::spawn(&model_path, &uhoh_dir) { Ok(s) => s, Err(e) => { tracing::warn!("AI sidecar spawn failed: {}", e); return; } };
            let port = sidecar.port();

            // Build blocking client request
            let files_added_s = files_added.clone();
            let files_deleted_s = files_deleted.clone();
            let files_modified_s = files_modified.clone();
            let prompt = format!(
                "You are analyzing a code snapshot diff. Describe what changed in 1-2 sentences.\n\nFiles added: {}\nFiles deleted: {}\nFiles modified: {}\n\nDiff:\n{}",
                files_added_s.join(", "), files_deleted_s.join(", "), files_modified_s.join(", "), diff_chunks
            );

            let client = match reqwest::blocking::Client::builder().timeout(std::time::Duration::from_secs(30)).build() { Ok(c) => c, Err(_) => return };
            let resp = client.post(format!("http://127.0.0.1:{}/v1/chat/completions", port))
                .json(&serde_json::json!({
                    "model": selected_name,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 256,
                    "temperature": 0.3,
                }))
                .send();
            let text = match resp.and_then(|r| r.json::<serde_json::Value>()) {
                Ok(v) => v["choices"][0]["message"]["content"].as_str().unwrap_or("").to_string(),
                Err(_) => String::new(),
            };

            if !text.is_empty() {
                if let Ok(db) = crate::db::Database::open(&uhoh_dir.join("uhoh.db")) {
                    let _ = db.set_ai_summary(rowid, &text);
                }
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

    Ok(Some(snapshot_id))
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
                    mtime: SystemTime::UNIX_EPOCH, // We don't persist mtime in DB; full rehash on restart
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
    files: &[(String, String, u64, bool, bool)],
) -> Vec<(String, String)> {
    // Group files by directory
    let mut dir_contents: HashMap<String, Vec<&str>> = HashMap::new();
    for (path, hash, _, _, _) in files {
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
            ("src/main.rs".to_string(), "abc123".to_string(), 100, true, false),
            ("src/lib.rs".to_string(), "def456".to_string(), 200, true, false),
            ("README.md".to_string(), "ghi789".to_string(), 50, true, false),
        ];
        let hashes = compute_tree_hashes(&files);
        assert!(!hashes.is_empty());
        // Should have entries for "src", ".", and a root
        let dirs: Vec<&str> = hashes.iter().map(|(d, _)| d.as_str()).collect();
        assert!(dirs.contains(&"src"));
    }
}
