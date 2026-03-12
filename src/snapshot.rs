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
    pub storage_method: cas::StorageMethod,
    pub is_symlink: bool,
}

pub struct CreateSnapshotRequest<'a> {
    pub project_hash: &'a str,
    pub project_path: &'a Path,
    pub trigger: &'a str,
    pub message: Option<&'a str>,
    pub changed_paths: Option<&'a [PathBuf]>,
}

type DeletedManifestEntry = (String, String, u64, bool, cas::StorageMethod);

struct SnapshotScanResult {
    has_changes: bool,
    new_files: Vec<String>,
    files_for_manifest: Vec<crate::db::SnapFileEntry>,
    deleted_for_manifest: Vec<DeletedManifestEntry>,
    current_hashes: HashMap<String, (String, bool)>,
}

impl SnapshotScanResult {
    fn new() -> Self {
        Self {
            has_changes: false,
            new_files: Vec::new(),
            files_for_manifest: Vec::new(),
            deleted_for_manifest: Vec::new(),
            current_hashes: HashMap::new(),
        }
    }
}

struct SnapshotDecision {
    trigger: String,
    message: String,
}

/// Create a snapshot for a project. Returns the snapshot ID if one was created.
pub fn create_snapshot(
    uhoh_dir: &Path,
    database: &Database,
    config: &Config,
    request: CreateSnapshotRequest<'_>,
) -> Result<Option<u64>> {
    let CreateSnapshotRequest {
        project_hash,
        project_path,
        trigger,
        message,
        changed_paths,
    } = request;
    let blob_root = uhoh_dir.join("blobs");
    let prev_files = load_previous_snapshot_files(database, project_hash)?;
    let scan = collect_snapshot_scan(
        database,
        config,
        project_path,
        changed_paths,
        &prev_files,
        &blob_root,
    )?;

    if !scan.has_changes && trigger == "auto" {
        return Ok(None);
    }

    let decision = derive_snapshot_decision(
        trigger,
        message,
        scan.deleted_for_manifest.len(),
        prev_files.len(),
        config,
    );
    let (rowid, snapshot_id) = persist_snapshot(database, project_hash, &decision, &scan)?;
    run_snapshot_post_commit(
        uhoh_dir,
        database,
        config,
        project_hash,
        &prev_files,
        rowid,
        snapshot_id,
        &decision,
        &scan,
    )?;

    Ok(Some(snapshot_id))
}

fn collect_snapshot_scan(
    database: &Database,
    config: &Config,
    project_path: &Path,
    changed_paths: Option<&[PathBuf]>,
    prev_files: &HashMap<String, CachedFileState>,
    blob_root: &Path,
) -> Result<SnapshotScanResult> {
    if let Some(paths) = changed_paths {
        if should_use_full_scan_for_changes(paths, project_path, prev_files) {
            return Ok(collect_full_scan(
                database,
                config,
                project_path,
                prev_files,
                blob_root,
            ));
        }
        if let Some(scan) =
            collect_incremental_scan(database, config, project_path, paths, prev_files, blob_root)?
        {
            return Ok(scan);
        }
    }

    Ok(collect_full_scan(
        database,
        config,
        project_path,
        prev_files,
        blob_root,
    ))
}

fn should_use_full_scan_for_changes(
    paths: &[PathBuf],
    project_path: &Path,
    prev_files: &HashMap<String, CachedFileState>,
) -> bool {
    for path in paths {
        if path == project_path || path.is_dir() {
            return true;
        }
        if !path.exists() {
            if let Ok(rel) = path.strip_prefix(project_path) {
                let prefix = cas::encode_relpath(rel);
                let prefix_with_sep = format!("{prefix}/");
                if prev_files
                    .keys()
                    .any(|candidate| candidate.starts_with(&prefix_with_sep))
                {
                    return true;
                }
            }
        }
    }
    false
}

fn collect_incremental_scan(
    database: &Database,
    config: &Config,
    project_path: &Path,
    paths: &[PathBuf],
    prev_files: &HashMap<String, CachedFileState>,
    blob_root: &Path,
) -> Result<Option<SnapshotScanResult>> {
    let rel_changed = filter_incremental_paths(project_path, paths);
    let mut scan = SnapshotScanResult::new();
    let mut inserted = HashSet::new();

    for rel_path in &rel_changed {
        let abs_path = project_path.join(cas::decode_relpath_to_os(rel_path));
        if abs_path.file_name().is_some_and(|name| name == ".uhoh") {
            continue;
        }

        match std::fs::symlink_metadata(&abs_path) {
            Ok(meta) => {
                let file_type = meta.file_type();
                if !file_type.is_file() && !file_type.is_symlink() {
                    continue;
                }
                record_current_entry(
                    database, config, blob_root, prev_files, rel_path, &abs_path, &meta, &mut scan,
                );
                inserted.insert(rel_path.clone());
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                if let Some(cached) = prev_files.get(rel_path) {
                    scan.has_changes = true;
                    scan.deleted_for_manifest
                        .push(deleted_manifest_entry(rel_path, cached));
                    inserted.insert(rel_path.clone());
                } else {
                    let dir_prefix = format!("{rel_path}/");
                    if prev_files
                        .keys()
                        .any(|candidate| candidate.starts_with(&dir_prefix))
                    {
                        tracing::debug!(
                            "Deleted path '{}' is a directory with children in prev snapshot; \
                             forcing full tree walk",
                            rel_path
                        );
                        return Ok(None);
                    }
                }
            }
            Err(err) => {
                tracing::warn!("Cannot stat {}: {}", abs_path.display(), err);
            }
        }
    }

    carry_forward_unchanged(prev_files, &inserted, &mut scan);
    Ok(Some(scan))
}

fn filter_incremental_paths(project_path: &Path, paths: &[PathBuf]) -> HashSet<String> {
    let gitignore = {
        let mut builder = ignore::gitignore::GitignoreBuilder::new(project_path);
        let gitignore_path = project_path.join(".gitignore");
        if gitignore_path.exists() {
            builder.add(&gitignore_path);
        }
        for name in [".uhohignore", ".git/.uhohignore"] {
            let path = project_path.join(name);
            if path.exists() {
                builder.add(&path);
            }
        }
        builder.build().unwrap_or_else(|_| {
            ignore::gitignore::GitignoreBuilder::new(project_path)
                .build()
                .unwrap()
        })
    };

    let mut rel_changed = HashSet::new();
    for path in paths {
        if !path.starts_with(project_path) {
            continue;
        }

        if let Ok(rel) = path.strip_prefix(project_path) {
            let rel_str = rel.to_string_lossy();
            if rel_str.starts_with(".git/")
                || rel_str.starts_with(".git\\")
                || rel_str == ".git"
                || rel_str == ".uhoh"
            {
                continue;
            }
        }

        if path.exists() {
            let matched = gitignore.matched_path_or_any_parents(path, path.is_dir());
            if matched.is_ignore() {
                continue;
            }
        }

        if let Ok(rel) = path.strip_prefix(project_path) {
            rel_changed.insert(cas::encode_relpath(rel));
        }
    }

    rel_changed
}

fn carry_forward_unchanged(
    prev_files: &HashMap<String, CachedFileState>,
    inserted: &HashSet<String>,
    scan: &mut SnapshotScanResult,
) {
    for (path, cached) in prev_files {
        if inserted.contains(path) {
            continue;
        }
        scan.files_for_manifest
            .push(cached_manifest_entry(path, cached));
    }
}

fn collect_full_scan(
    database: &Database,
    config: &Config,
    project_path: &Path,
    prev_files: &HashMap<String, CachedFileState>,
    blob_root: &Path,
) -> SnapshotScanResult {
    let current_files = collect_current_files(project_path);
    let mut scan = SnapshotScanResult::new();

    for (rel_path, (abs_path, meta)) in &current_files {
        record_current_entry(
            database, config, blob_root, prev_files, rel_path, abs_path, meta, &mut scan,
        );
    }

    let current_paths: HashSet<&String> = current_files.keys().collect();
    for (path, cached) in prev_files {
        if !current_paths.contains(path) {
            scan.has_changes = true;
            scan.deleted_for_manifest
                .push(deleted_manifest_entry(path, cached));
        }
    }

    scan
}

fn collect_current_files(project_path: &Path) -> HashMap<String, (PathBuf, std::fs::Metadata)> {
    let walker = ignore_rules::build_walker(project_path);
    let mut current_files = HashMap::new();
    let mut entries = Vec::new();
    for entry in walker.flatten() {
        entries.push(entry);
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
        if path.file_name().is_some_and(|name| name == ".uhoh") {
            if let Some(pb) = &pb {
                pb.inc(1);
            }
            continue;
        }

        match std::fs::symlink_metadata(path) {
            Ok(meta) => {
                let file_type = meta.file_type();
                if file_type.is_file() || file_type.is_symlink() {
                    if let Ok(rel_path) = path.strip_prefix(project_path) {
                        current_files
                            .insert(cas::encode_relpath(rel_path), (path.to_path_buf(), meta));
                    }
                }
            }
            Err(err) => {
                tracing::warn!("Cannot stat {}: {}", path.display(), err);
            }
        }

        if let Some(pb) = &pb {
            pb.inc(1);
        }
    }

    if let Some(pb) = pb {
        pb.finish_and_clear();
    }

    current_files
}

fn record_current_entry(
    database: &Database,
    config: &Config,
    blob_root: &Path,
    prev_files: &HashMap<String, CachedFileState>,
    rel_path: &str,
    abs_path: &Path,
    meta: &std::fs::Metadata,
    scan: &mut SnapshotScanResult,
) {
    let size = meta.len();
    let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let executable = cas::is_executable(abs_path);
    if let Some(cached) = prev_files.get(rel_path) {
        if cached_matches_metadata(cached, size, mtime, executable) {
            scan.files_for_manifest
                .push(cached_manifest_entry(rel_path, cached));
            return;
        }
    }

    if meta.file_type().is_symlink() {
        record_symlink_entry(
            database, blob_root, prev_files, rel_path, abs_path, size, mtime, scan,
        );
    } else {
        record_file_entry(
            database, config, blob_root, prev_files, rel_path, abs_path, size, mtime, executable,
            scan,
        );
    }
}

fn cached_matches_metadata(
    cached: &CachedFileState,
    size: u64,
    mtime: SystemTime,
    executable: bool,
) -> bool {
    let fs_mtime_ms = mtime_to_millis(mtime);
    let cached_mtime_ms = mtime_to_millis(cached.mtime);
    cached.size == size && cached_mtime_ms == fs_mtime_ms && cached.executable == executable
}

fn cached_manifest_entry(path: &str, cached: &CachedFileState) -> crate::db::SnapFileEntry {
    crate::db::SnapFileEntry {
        path: path.to_string(),
        hash: cached.hash.clone(),
        size: cached.size,
        stored: cached.stored,
        executable: cached.executable,
        mtime: Some(mtime_to_millis(cached.mtime)),
        storage_method: cached.storage_method,
        is_symlink: cached.is_symlink,
    }
}

fn deleted_manifest_entry(path: &str, cached: &CachedFileState) -> DeletedManifestEntry {
    (
        path.to_string(),
        cached.hash.clone(),
        cached.size,
        cached.stored,
        cached.storage_method,
    )
}

fn record_symlink_entry(
    database: &Database,
    blob_root: &Path,
    prev_files: &HashMap<String, CachedFileState>,
    rel_path: &str,
    abs_path: &Path,
    size: u64,
    mtime: SystemTime,
    scan: &mut SnapshotScanResult,
) {
    match cas::store_symlink_target(blob_root, abs_path) {
        Ok((hash, symlink_size, bytes_written)) => {
            let is_new_or_changed = prev_files
                .get(rel_path)
                .map_or(true, |prev| prev.hash != hash || !prev.is_symlink);
            if is_new_or_changed {
                scan.has_changes = true;
                scan.new_files.push(rel_path.to_string());
            }
            let mtime_i = mtime_to_millis(mtime);
            scan.files_for_manifest.push(crate::db::SnapFileEntry {
                path: rel_path.to_string(),
                hash: hash.clone(),
                size: symlink_size,
                stored: true,
                executable: false,
                mtime: Some(mtime_i),
                storage_method: cas::StorageMethod::Copy,
                is_symlink: true,
            });
            scan.current_hashes
                .insert(rel_path.to_string(), (hash, true));
            if bytes_written > 0 {
                let _ = database.add_blob_bytes(bytes_written as i64);
            }
        }
        Err(err) => {
            tracing::warn!("Failed to store symlink for {}: {}", rel_path, err);
            let is_new_or_changed = prev_files
                .get(rel_path)
                .map_or(true, |prev| prev.stored || !prev.is_symlink);
            if is_new_or_changed {
                scan.has_changes = true;
                scan.new_files.push(rel_path.to_string());
            }
            scan.files_for_manifest.push(crate::db::SnapFileEntry {
                path: rel_path.to_string(),
                hash: String::new(),
                size,
                stored: false,
                executable: false,
                mtime: Some(mtime_to_millis(mtime)),
                storage_method: cas::StorageMethod::None,
                is_symlink: true,
            });
            scan.current_hashes
                .insert(rel_path.to_string(), (String::new(), false));
        }
    }
}

fn record_file_entry(
    database: &Database,
    config: &Config,
    blob_root: &Path,
    prev_files: &HashMap<String, CachedFileState>,
    rel_path: &str,
    abs_path: &Path,
    size: u64,
    mtime: SystemTime,
    executable: bool,
    scan: &mut SnapshotScanResult,
) {
    match cas::store_blob_from_file(
        blob_root,
        abs_path,
        config.storage.max_copy_blob_bytes,
        config.storage.max_binary_blob_bytes,
        config.storage.max_text_blob_bytes,
        cfg!(feature = "compression") && config.storage.compress,
        config.storage.compress_level,
    ) {
        Ok((hash, stored_size, method, bytes_written)) => {
            let is_new_or_changed = prev_files
                .get(rel_path)
                .map_or(true, |prev| prev.hash != hash);
            if is_new_or_changed {
                scan.has_changes = true;
                scan.new_files.push(rel_path.to_string());
            }
            let mtime_i = mtime_to_millis(mtime);
            let stored = method.is_recoverable();
            scan.files_for_manifest.push(crate::db::SnapFileEntry {
                path: rel_path.to_string(),
                hash: hash.clone(),
                size: stored_size,
                stored,
                executable,
                mtime: Some(mtime_i),
                storage_method: method,
                is_symlink: false,
            });
            scan.current_hashes
                .insert(rel_path.to_string(), (hash, stored));
            if bytes_written > 0 {
                let _ = database.add_blob_bytes(bytes_written as i64);
            }
        }
        Err(err) => {
            tracing::warn!("Failed to store blob for {}: {}", rel_path, err);
            scan.files_for_manifest.push(crate::db::SnapFileEntry {
                path: rel_path.to_string(),
                hash: String::new(),
                size,
                stored: false,
                executable,
                mtime: Some(mtime_to_millis(mtime)),
                storage_method: cas::StorageMethod::None,
                is_symlink: false,
            });
            scan.current_hashes
                .insert(rel_path.to_string(), (String::new(), false));
        }
    }
}

fn derive_snapshot_decision(
    trigger: &str,
    message: Option<&str>,
    deleted_count: usize,
    prev_file_count: usize,
    config: &Config,
) -> SnapshotDecision {
    let prev_count = prev_file_count as u64;
    let trigger = if trigger == "auto"
        && crate::emergency::exceeds_threshold(
            deleted_count,
            prev_count,
            config.watch.emergency_delete_threshold,
            config.watch.emergency_delete_min_files,
        ) {
        let ratio = crate::emergency::deletion_ratio(deleted_count, prev_count);
        tracing::warn!(
            "Dynamic trigger upgrade: auto -> emergency (deleted={}, baseline={}, ratio={:.3})",
            deleted_count,
            prev_count,
            ratio
        );
        "emergency".to_string()
    } else {
        trigger.to_string()
    };

    let message = if trigger == "emergency" && message.is_none() {
        let ratio = crate::emergency::deletion_ratio(deleted_count, prev_count);
        format!(
            "Mass delete detected: {}/{} files ({:.1}%)",
            deleted_count,
            prev_count,
            ratio * 100.0
        )
    } else {
        message.unwrap_or("").to_string()
    };

    SnapshotDecision { trigger, message }
}

fn persist_snapshot(
    database: &Database,
    project_hash: &str,
    decision: &SnapshotDecision,
    scan: &SnapshotScanResult,
) -> Result<(i64, u64)> {
    let timestamp = chrono::Utc::now().to_rfc3339();
    database.create_snapshot(crate::db::CreateSnapshotRow {
        project_hash,
        snapshot_id: 0,
        timestamp: &timestamp,
        trigger: &decision.trigger,
        message: &decision.message,
        pinned: false,
        files: &scan.files_for_manifest,
        deleted: &scan.deleted_for_manifest,
    })
}

fn run_snapshot_post_commit(
    uhoh_dir: &Path,
    database: &Database,
    config: &Config,
    project_hash: &str,
    prev_files: &HashMap<String, CachedFileState>,
    rowid: i64,
    snapshot_id: u64,
    decision: &SnapshotDecision,
    scan: &SnapshotScanResult,
) -> Result<()> {
    index_snapshot_for_search(
        database,
        rowid,
        project_hash,
        decision,
        &scan.files_for_manifest,
    );
    update_active_operation_snapshot(database, project_hash, &decision.trigger, snapshot_id);
    schedule_ai_summary(
        uhoh_dir,
        database,
        config,
        project_hash,
        prev_files,
        rowid,
        &scan.new_files,
        &scan.deleted_for_manifest,
        &scan.current_hashes,
    );

    let id_str = cas::id_to_base58(snapshot_id);
    tracing::info!(
        "Snapshot {} created for {} ({} files, {} deleted, trigger={})",
        id_str,
        &project_hash[..project_hash.len().min(12)],
        scan.files_for_manifest.len(),
        scan.deleted_for_manifest.len(),
        decision.trigger,
    );

    let total_project_size: u64 = scan.files_for_manifest.iter().map(|file| file.size).sum();
    enforce_storage_limit(database, total_project_size, project_hash, config)
}

fn index_snapshot_for_search(
    database: &Database,
    rowid: i64,
    project_hash: &str,
    decision: &SnapshotDecision,
    files_for_manifest: &[crate::db::SnapFileEntry],
) {
    let file_paths_str = files_for_manifest
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let _ = database.index_snapshot_for_search(
        rowid,
        project_hash,
        &decision.trigger,
        &decision.message,
        "",
        &file_paths_str,
    );
}

fn update_active_operation_snapshot(
    database: &Database,
    project_hash: &str,
    trigger: &str,
    snapshot_id: u64,
) {
    if trigger == "pre-restore" {
        return;
    }
    if let Ok(Some(op)) = database.get_active_operation(project_hash) {
        let _ = database.update_operation_last_snapshot(op.id, snapshot_id);
    }
}

fn schedule_ai_summary(
    uhoh_dir: &Path,
    database: &Database,
    config: &Config,
    project_hash: &str,
    prev_files: &HashMap<String, CachedFileState>,
    rowid: i64,
    new_files: &[String],
    deleted_for_manifest: &[DeletedManifestEntry],
    current_hashes: &HashMap<String, (String, bool)>,
) {
    if !ai::should_run_ai(&config.ai) {
        let _ = database.enqueue_ai_summary(rowid, project_hash);
        return;
    }

    let uhoh_dir_cl = uhoh_dir.to_path_buf();
    let db_handle = database.clone_handle();
    let cfg_cloned = config.clone();
    let files_added: Vec<String> = new_files
        .iter()
        .filter(|path| !prev_files.contains_key(*path))
        .cloned()
        .collect();
    let files_modified: Vec<String> = new_files
        .iter()
        .filter(|path| prev_files.contains_key(*path))
        .cloned()
        .collect();
    let files_deleted: Vec<String> = deleted_for_manifest
        .iter()
        .map(|(path, _, _, _, _)| path.clone())
        .collect();
    let diff_chunks = build_ai_diff_chunks(
        uhoh_dir,
        &cfg_cloned,
        prev_files,
        current_hashes,
        &files_modified,
    );

    std::thread::spawn(move || {
        let files = crate::ai::summary::FileChangeSummary {
            added: files_added,
            deleted: files_deleted,
            modified: files_modified,
        };
        match crate::ai::summary::generate_summary_blocking(
            &uhoh_dir_cl,
            &cfg_cloned,
            &diff_chunks,
            &files,
        ) {
            Ok(text) if !text.is_empty() => {
                let _ = db_handle.set_ai_summary(rowid, &text);
            }
            Ok(_) => {}
            Err(err) => tracing::warn!("AI summary generation failed: {}", err),
        }
    });
}

fn build_ai_diff_chunks(
    uhoh_dir: &Path,
    config: &Config,
    prev_files: &HashMap<String, CachedFileState>,
    current_hashes: &HashMap<String, (String, bool)>,
    files_modified: &[String],
) -> String {
    const MAX_AI_DIFF_FILE_SIZE: u64 = 512 * 1024;

    let blob_root = uhoh_dir.join("blobs");
    let max_diff_chars = config.ai.max_context_tokens.saturating_mul(4);
    let mut diff_chunks = String::new();
    let mut diff_chars = 0usize;
    let mut diff_truncated = false;

    for path in files_modified.iter().take(10) {
        if diff_chars >= max_diff_chars {
            append_diff_truncation_marker(&mut diff_chunks, &mut diff_truncated);
            break;
        }

        let Some(prev) = prev_files.get(path) else {
            continue;
        };
        let Some((curr_hash, curr_stored)) = current_hashes.get(path) else {
            continue;
        };
        if !prev.stored || !curr_stored || prev.hash.is_empty() || curr_hash.is_empty() {
            continue;
        }

        if prev.size > MAX_AI_DIFF_FILE_SIZE {
            let note = format!("--- {path}\n[File too large for AI diff]\n");
            crate::ai::summary::append_diff_chunk(
                &mut diff_chunks,
                &mut diff_chars,
                max_diff_chars,
                &mut diff_truncated,
                &note,
            );
            if diff_chars >= max_diff_chars {
                append_diff_truncation_marker(&mut diff_chunks, &mut diff_truncated);
                break;
            }
            continue;
        }

        let old = crate::cas::read_blob(&blob_root, &prev.hash).ok().flatten();
        let new = crate::cas::read_blob(&blob_root, curr_hash).ok().flatten();
        let (Some(old), Some(new)) = (old, new) else {
            continue;
        };

        let head_old = &old[..old.len().min(8192)];
        let head_new = &new[..new.len().min(8192)];
        if content_inspector::inspect(head_old).is_binary()
            || content_inspector::inspect(head_new).is_binary()
        {
            let note = format!("--- {path}\n[Binary file]\n");
            crate::ai::summary::append_diff_chunk(
                &mut diff_chunks,
                &mut diff_chars,
                max_diff_chars,
                &mut diff_truncated,
                &note,
            );
            if diff_chars >= max_diff_chars {
                append_diff_truncation_marker(&mut diff_chunks, &mut diff_truncated);
                break;
            }
            continue;
        }

        let (Ok(old_s), Ok(new_s)) = (String::from_utf8(old), String::from_utf8(new)) else {
            continue;
        };
        let diff = similar::TextDiff::from_lines(&old_s, &new_s);
        let header = format!("--- a/{path}\n+++ b/{path}\n");
        crate::ai::summary::append_diff_chunk(
            &mut diff_chunks,
            &mut diff_chars,
            max_diff_chars,
            &mut diff_truncated,
            &header,
        );

        for hunk in diff.unified_diff().context_radius(2).iter_hunks() {
            if diff_chars >= max_diff_chars {
                append_diff_truncation_marker(&mut diff_chunks, &mut diff_truncated);
                break;
            }

            let hunk_header = format!("{}\n", hunk.header());
            crate::ai::summary::append_diff_chunk(
                &mut diff_chunks,
                &mut diff_chars,
                max_diff_chars,
                &mut diff_truncated,
                &hunk_header,
            );
            for change in hunk.iter_changes() {
                if diff_chars >= max_diff_chars {
                    append_diff_truncation_marker(&mut diff_chunks, &mut diff_truncated);
                    break;
                }
                let sign = match change.tag() {
                    similar::ChangeTag::Delete => '-',
                    similar::ChangeTag::Insert => '+',
                    similar::ChangeTag::Equal => ' ',
                };
                let line = format!("{}{}", sign, change);
                crate::ai::summary::append_diff_chunk(
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

    diff_chunks
}

fn append_diff_truncation_marker(diff_chunks: &mut String, diff_truncated: &mut bool) {
    if !*diff_truncated {
        diff_chunks.push_str("\n[Diff truncated]\n");
        *diff_truncated = true;
    }
}

pub fn mtime_to_millis(t: SystemTime) -> i64 {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_millis()).unwrap_or(i64::MAX),
        Err(e) => {
            let dur = e.duration();
            let millis = i64::try_from(dur.as_millis()).unwrap_or(i64::MAX);
            -(millis + 1)
        }
    }
}

pub fn millis_to_mtime(millis: i64) -> SystemTime {
    if millis >= 0 {
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(millis as u64)
    } else {
        let before_epoch = (-(millis as i128) - 1).max(0) as u128;
        let clamped = before_epoch.min(u64::MAX as u128) as u64;
        std::time::UNIX_EPOCH - std::time::Duration::from_millis(clamped)
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::SystemTime;

    use super::{build_ai_diff_chunks, CachedFileState};
    use crate::cas::{self, StorageMethod};
    use crate::config::Config;

    #[test]
    fn ai_diff_truncation_marker_appears_once_when_budget_is_exceeded() {
        let temp = tempfile::tempdir().expect("tempdir");
        let old = "alpha\n".repeat(64);
        let new = "beta\n".repeat(64);
        let (old_hash, _) =
            cas::store_blob(&temp.path().join("blobs"), old.as_bytes()).expect("store old blob");
        let (new_hash, _) =
            cas::store_blob(&temp.path().join("blobs"), new.as_bytes()).expect("store new blob");

        let mut prev_files = HashMap::new();
        prev_files.insert(
            "src/lib.rs".to_string(),
            CachedFileState {
                hash: old_hash,
                size: old.len() as u64,
                mtime: SystemTime::UNIX_EPOCH,
                stored: true,
                executable: false,
                storage_method: StorageMethod::Copy,
                is_symlink: false,
            },
        );

        let mut current_hashes = HashMap::new();
        current_hashes.insert("src/lib.rs".to_string(), (new_hash, true));

        let mut config = Config::default();
        config.ai.max_context_tokens = 8;

        let diff = build_ai_diff_chunks(
            temp.path(),
            &config,
            &prev_files,
            &current_hashes,
            &["src/lib.rs".to_string()],
        );

        assert!(diff.contains("[Diff truncated]"));
        assert_eq!(diff.matches("[Diff truncated]").count(), 1);
    }
}

// Tree hash computation removed to reduce overhead; table preserved for potential future use.
