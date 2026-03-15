use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::cas;
use crate::config::{AiConfig, CompactionConfig, Config, StorageConfig, WatchConfig};
use crate::encoding;
use crate::db::Database;
use crate::ignore_rules;
use crate::ai;

/// File metadata cache for efficient change detection.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CachedFileState {
    pub hash: String,
    pub size: u64,
    pub mtime: SystemTime,
    pub stored: bool,
    pub executable: bool,
    pub storage_method: cas::StorageMethod,
    pub is_symlink: bool,
}

#[non_exhaustive]
pub struct CreateSnapshotRequest<'a> {
    pub project_hash: &'a str,
    pub project_path: &'a Path,
    pub trigger: crate::db::SnapshotTrigger,
    pub message: Option<&'a str>,
    pub changed_paths: Option<&'a [PathBuf]>,
}

impl<'a> CreateSnapshotRequest<'a> {
    #[must_use] 
    pub fn new(
        project_hash: &'a str,
        project_path: &'a Path,
        trigger: crate::db::SnapshotTrigger,
        message: Option<&'a str>,
        changed_paths: Option<&'a [PathBuf]>,
    ) -> Self {
        Self {
            project_hash,
            project_path,
            trigger,
            message,
            changed_paths,
        }
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub struct SnapshotSettings {
    pub ai: AiConfig,
    pub compaction: CompactionConfig,
    pub storage: StorageConfig,
    pub watch: WatchConfig,
}

impl SnapshotSettings {
    #[must_use] 
    pub fn from_config(config: &Config) -> Self {
        Self {
            ai: config.ai.clone(),
            compaction: config.compaction.clone(),
            storage: config.storage.clone(),
            watch: config.watch.clone(),
        }
    }
}

#[derive(Clone)]
pub struct SnapshotRuntime {
    settings: SnapshotSettings,
    sidecar_manager: crate::ai::SidecarManager,
}

impl SnapshotRuntime {
    #[must_use] 
    pub fn from_config(config: &Config) -> Self {
        Self::new(
            SnapshotSettings::from_config(config),
            crate::ai::SidecarManager::new(),
        )
    }

    #[must_use] 
    pub fn new(
        settings: SnapshotSettings,
        sidecar_manager: crate::ai::SidecarManager,
    ) -> Self {
        Self {
            settings,
            sidecar_manager,
        }
    }

    #[must_use] 
    pub fn settings(&self) -> &SnapshotSettings {
        &self.settings
    }

    #[must_use] 
    pub fn sidecar_manager(&self) -> &crate::ai::SidecarManager {
        &self.sidecar_manager
    }
}

struct SnapshotScanResult {
    has_changes: bool,
    new_files: Vec<String>,
    files_for_manifest: Vec<crate::db::SnapFileEntry>,
    deleted_for_manifest: Vec<crate::db::DeletedFile>,
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
    trigger: crate::db::SnapshotTrigger,
    message: String,
}

/// Create a snapshot for a project. Returns the snapshot ID if one was created.
pub fn create_snapshot(
    uhoh_dir: &Path,
    database: &Database,
    runtime: &SnapshotRuntime,
    request: CreateSnapshotRequest<'_>,
) -> Result<Option<u64>> {
    let CreateSnapshotRequest {
        project_hash,
        project_path,
        trigger,
        message,
        changed_paths,
    } = request;
    let settings = runtime.settings();
    let blob_root = uhoh_dir.join("blobs");
    let prev_files = load_previous_snapshot_files(database, project_hash)?;
    let scan = collect_snapshot_scan(
        database,
        settings,
        project_path,
        changed_paths,
        &prev_files,
        &blob_root,
    )?;

    if !scan.has_changes && trigger == crate::db::SnapshotTrigger::Auto {
        return Ok(None);
    }

    let decision = derive_snapshot_decision(
        trigger,
        message,
        scan.deleted_for_manifest.len(),
        prev_files.len(),
        settings,
    );
    let (rowid, snapshot_id) = persist_snapshot(database, project_hash, &decision, &scan)?;
    let post_ctx = PostCommitCtx {
        uhoh_dir,
        database,
        runtime,
        project_hash,
        prev_files: &prev_files,
    };
    run_snapshot_post_commit(&post_ctx, rowid, snapshot_id, &decision, &scan)?;

    Ok(Some(snapshot_id))
}

fn collect_snapshot_scan(
    database: &Database,
    settings: &SnapshotSettings,
    project_path: &Path,
    changed_paths: Option<&[PathBuf]>,
    prev_files: &HashMap<String, CachedFileState>,
    blob_root: &Path,
) -> Result<SnapshotScanResult> {
    if let Some(paths) = changed_paths {
        if should_use_full_scan_for_changes(paths, project_path, prev_files) {
            return Ok(collect_full_scan(
                database,
                settings,
                project_path,
                prev_files,
                blob_root,
            ));
        }
        if let Some(scan) = collect_incremental_scan(
            database,
            settings,
            project_path,
            paths,
            prev_files,
            blob_root,
        )? {
            return Ok(scan);
        }
    }

    Ok(collect_full_scan(
        database,
        settings,
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
                let prefix = encoding::encode_relpath(rel);
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
    settings: &SnapshotSettings,
    project_path: &Path,
    paths: &[PathBuf],
    prev_files: &HashMap<String, CachedFileState>,
    blob_root: &Path,
) -> Result<Option<SnapshotScanResult>> {
    let rel_changed = filter_incremental_paths(project_path, paths);
    let mut scan = SnapshotScanResult::new();
    let mut inserted = HashSet::new();
    let ctx = RecordEntryCtx {
        database,
        settings,
        blob_root,
        prev_files,
    };

    for rel_path in &rel_changed {
        let abs_path = project_path.join(encoding::decode_relpath_to_os(rel_path));
        if abs_path.file_name().is_some_and(|name| name == ".uhoh") {
            continue;
        }

        match std::fs::symlink_metadata(&abs_path) {
            Ok(meta) => {
                let file_type = meta.file_type();
                if !file_type.is_file() && !file_type.is_symlink() {
                    continue;
                }
                record_current_entry(&ctx, rel_path, &abs_path, &meta, &mut scan);
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
            rel_changed.insert(encoding::encode_relpath(rel));
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
    settings: &SnapshotSettings,
    project_path: &Path,
    prev_files: &HashMap<String, CachedFileState>,
    blob_root: &Path,
) -> SnapshotScanResult {
    let current_files = collect_current_files(project_path);
    let mut scan = SnapshotScanResult::new();
    let ctx = RecordEntryCtx {
        database,
        settings,
        blob_root,
        prev_files,
    };

    for (rel_path, (abs_path, meta)) in &current_files {
        record_current_entry(&ctx, rel_path, abs_path, meta, &mut scan);
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
                            .insert(encoding::encode_relpath(rel_path), (path.to_path_buf(), meta));
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

/// Shared context for recording file entries during a snapshot scan.
struct RecordEntryCtx<'a> {
    database: &'a Database,
    settings: &'a SnapshotSettings,
    blob_root: &'a Path,
    prev_files: &'a HashMap<String, CachedFileState>,
}

fn record_current_entry(
    ctx: &RecordEntryCtx<'_>,
    rel_path: &str,
    abs_path: &Path,
    meta: &std::fs::Metadata,
    scan: &mut SnapshotScanResult,
) {
    let size = meta.len();
    let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    let executable = encoding::is_executable(abs_path);
    if let Some(cached) = ctx.prev_files.get(rel_path) {
        if cached_matches_metadata(cached, size, mtime, executable) {
            scan.files_for_manifest
                .push(cached_manifest_entry(rel_path, cached));
            return;
        }
    }

    if meta.file_type().is_symlink() {
        record_symlink_entry(ctx, rel_path, abs_path, size, mtime, scan);
    } else {
        record_file_entry(ctx, rel_path, abs_path, size, mtime, executable, scan);
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

fn deleted_manifest_entry(path: &str, cached: &CachedFileState) -> crate::db::DeletedFile {
    crate::db::DeletedFile {
        path: path.to_string(),
        hash: cached.hash.clone(),
        size: cached.size,
        stored: cached.stored,
        storage_method: cached.storage_method,
    }
}

/// Records a file entry in the scan result: pushes to manifest, updates hashes, marks changes.
fn apply_manifest_entry(
    prev_files: &HashMap<String, CachedFileState>,
    rel_path: &str,
    entry: crate::db::SnapFileEntry,
    stored: bool,
    is_new_check: impl FnOnce(Option<&CachedFileState>) -> bool,
    scan: &mut SnapshotScanResult,
) {
    let hash = entry.hash.clone();
    if is_new_check(prev_files.get(rel_path)) {
        scan.has_changes = true;
        scan.new_files.push(rel_path.to_string());
    }
    scan.files_for_manifest.push(entry);
    scan.current_hashes
        .insert(rel_path.to_string(), (hash, stored));
}

fn record_symlink_entry(
    ctx: &RecordEntryCtx<'_>,
    rel_path: &str,
    abs_path: &Path,
    size: u64,
    mtime: SystemTime,
    scan: &mut SnapshotScanResult,
) {
    match cas::store_symlink_target(ctx.blob_root, abs_path) {
        Ok((hash, symlink_size, bytes_written)) => {
            let hash_clone = hash.clone();
            apply_manifest_entry(
                ctx.prev_files,
                rel_path,
                crate::db::SnapFileEntry {
                    path: rel_path.to_string(),
                    hash,
                    size: symlink_size,
                    stored: true,
                    executable: false,
                    mtime: Some(mtime_to_millis(mtime)),
                    storage_method: cas::StorageMethod::Copy,
                    is_symlink: true,
                },
                true,
                |prev| prev.map_or(true, |p| p.hash != hash_clone || !p.is_symlink),
                scan,
            );
            if bytes_written > 0 {
                let _ = ctx.database.add_blob_bytes(bytes_written as i64);
            }
        }
        Err(err) => {
            tracing::warn!("Failed to store symlink for {}: {}", rel_path, err);
            apply_manifest_entry(
                ctx.prev_files,
                rel_path,
                crate::db::SnapFileEntry {
                    path: rel_path.to_string(),
                    hash: String::new(),
                    size,
                    stored: false,
                    executable: false,
                    mtime: Some(mtime_to_millis(mtime)),
                    storage_method: cas::StorageMethod::None,
                    is_symlink: true,
                },
                false,
                |prev| prev.map_or(true, |p| p.stored || !p.is_symlink),
                scan,
            );
        }
    }
}

fn record_file_entry(
    ctx: &RecordEntryCtx<'_>,
    rel_path: &str,
    abs_path: &Path,
    size: u64,
    mtime: SystemTime,
    executable: bool,
    scan: &mut SnapshotScanResult,
) {
    match cas::store_blob_from_file(
        ctx.blob_root,
        abs_path,
        &cas::BlobStorageParams::new(
            ctx.settings.storage.max_copy_blob_bytes,
            ctx.settings.storage.max_binary_blob_bytes,
            ctx.settings.storage.max_text_blob_bytes,
            cfg!(feature = "compression") && ctx.settings.storage.compress,
            ctx.settings.storage.compress_level,
        ),
    ) {
        Ok((hash, stored_size, method, bytes_written)) => {
            let stored = method.is_recoverable();
            let hash_clone = hash.clone();
            apply_manifest_entry(
                ctx.prev_files,
                rel_path,
                crate::db::SnapFileEntry {
                    path: rel_path.to_string(),
                    hash,
                    size: stored_size,
                    stored,
                    executable,
                    mtime: Some(mtime_to_millis(mtime)),
                    storage_method: method,
                    is_symlink: false,
                },
                stored,
                |prev| prev.map_or(true, |p| p.hash != hash_clone),
                scan,
            );
            if bytes_written > 0 {
                let _ = ctx.database.add_blob_bytes(bytes_written as i64);
            }
        }
        Err(err) => {
            tracing::warn!("Failed to store blob for {}: {}", rel_path, err);
            apply_manifest_entry(
                ctx.prev_files,
                rel_path,
                crate::db::SnapFileEntry {
                    path: rel_path.to_string(),
                    hash: String::new(),
                    size,
                    stored: false,
                    executable,
                    mtime: Some(mtime_to_millis(mtime)),
                    storage_method: cas::StorageMethod::None,
                    is_symlink: false,
                },
                false,
                |_| true,
                scan,
            );
        }
    }
}

fn derive_snapshot_decision(
    trigger: crate::db::SnapshotTrigger,
    message: Option<&str>,
    deleted_count: usize,
    prev_file_count: usize,
    settings: &SnapshotSettings,
) -> SnapshotDecision {
    let prev_count = prev_file_count as u64;
    let trigger = if trigger == crate::db::SnapshotTrigger::Auto
        && crate::emergency::exceeds_threshold(
            deleted_count,
            prev_count,
            settings.watch.emergency_delete_threshold,
            settings.watch.emergency_delete_min_files,
        ) {
        let ratio = crate::emergency::deletion_ratio(deleted_count, prev_count);
        tracing::warn!(
            "Dynamic trigger upgrade: auto -> emergency (deleted={}, baseline={}, ratio={:.3})",
            deleted_count,
            prev_count,
            ratio
        );
        crate::db::SnapshotTrigger::Emergency
    } else {
        trigger
    };

    let message = if trigger == crate::db::SnapshotTrigger::Emergency && message.is_none() {
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
        trigger: decision.trigger,
        message: &decision.message,
        pinned: false,
        files: &scan.files_for_manifest,
        deleted: &scan.deleted_for_manifest,
    })
}

/// Shared context for post-commit operations (indexing, AI summary, storage limits).
struct PostCommitCtx<'a> {
    uhoh_dir: &'a Path,
    database: &'a Database,
    runtime: &'a SnapshotRuntime,
    project_hash: &'a str,
    prev_files: &'a HashMap<String, CachedFileState>,
}

fn run_snapshot_post_commit(
    ctx: &PostCommitCtx<'_>,
    rowid: i64,
    snapshot_id: u64,
    decision: &SnapshotDecision,
    scan: &SnapshotScanResult,
) -> Result<()> {
    index_snapshot_for_search(
        ctx.database,
        rowid,
        ctx.project_hash,
        decision,
        &scan.files_for_manifest,
    );
    update_active_operation_snapshot(ctx.database, ctx.project_hash, decision.trigger, snapshot_id);
    schedule_ai_summary(ctx, rowid, &scan.files_for_manifest, &scan.deleted_for_manifest);

    let id_str = encoding::id_to_base58(snapshot_id);
    tracing::info!(
        "Snapshot {} created for {} ({} files, {} deleted, trigger={})",
        id_str,
        &ctx.project_hash[..ctx.project_hash.len().min(12)],
        scan.files_for_manifest.len(),
        scan.deleted_for_manifest.len(),
        decision.trigger,
    );

    let total_project_size: u64 = scan.files_for_manifest.iter().map(|file| file.size).sum();
    enforce_storage_limit(
        ctx.database,
        total_project_size,
        ctx.project_hash,
        ctx.runtime.settings(),
    )
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
    if let Err(e) = database.index_snapshot_for_search(
        rowid,
        project_hash,
        decision.trigger.as_str(),
        &decision.message,
        "",
        &file_paths_str,
    ) {
        tracing::warn!("Failed to index snapshot for search: {e}");
    }
}

fn update_active_operation_snapshot(
    database: &Database,
    project_hash: &str,
    trigger: crate::db::SnapshotTrigger,
    snapshot_id: u64,
) {
    if trigger == crate::db::SnapshotTrigger::PreRestore {
        return;
    }
    if let Ok(Some(op)) = database.get_active_operation(project_hash) {
        if let Err(e) = database.update_operation_last_snapshot(op.id, snapshot_id) {
            tracing::warn!("Failed to update operation last snapshot: {e}");
        }
    }
}

fn schedule_ai_summary(
    ctx: &PostCommitCtx<'_>,
    rowid: i64,
    current_files: &[crate::db::SnapFileEntry],
    deleted_for_manifest: &[crate::db::DeletedFile],
) {
    if !ai::should_run_ai(&ctx.runtime.settings().ai) {
        if let Err(e) = ctx.database.enqueue_ai_summary(rowid, ctx.project_hash) {
            tracing::warn!("Failed to enqueue AI summary: {e}");
        }
        return;
    }

    let uhoh_dir_cl = ctx.uhoh_dir.to_path_buf();
    let db_handle = ctx.database.clone_handle();
    let runtime = ctx.runtime.clone();
    let project_hash = ctx.project_hash.to_string();
    let blob_root = ctx.uhoh_dir.join("blobs");
    let changes = crate::ai::build_diff_entries(
        current_files.iter().map(|f| {
            (
                f.path.as_str(),
                crate::ai::SummaryBlobRef {
                    hash: &f.hash,
                    stored: f.stored,
                    size: f.size,
                },
            )
        }),
        deleted_for_manifest.iter().map(|d| {
            (
                d.path.as_str(),
                crate::ai::SummaryBlobRef {
                    hash: &d.hash,
                    stored: d.stored,
                    size: d.size,
                },
            )
        }),
        |path| {
            ctx.prev_files
                .get(path)
                .map(|prev| crate::ai::SummaryBlobRef {
                    hash: &prev.hash,
                    stored: prev.stored,
                    size: prev.size,
                })
        },
    );

    let prepared =
        crate::ai::prepare_summary_inputs(&blob_root, &runtime.settings().ai, &changes);

    std::thread::spawn(move || {
        match crate::ai::generate_summary_blocking(
            &uhoh_dir_cl,
            &runtime.settings().ai,
            runtime.sidecar_manager(),
            &prepared.diff_text,
            &prepared.files,
        ) {
            Ok(Some(text)) => {
                if let Err(e) = db_handle.set_ai_summary(rowid, &text) {
                    tracing::warn!("Failed to save AI summary: {e}");
                }
            }
            Ok(None) => {
                if let Err(e) = db_handle.enqueue_ai_summary(rowid, &project_hash) {
                    tracing::warn!("Failed to enqueue AI summary: {e}");
                }
            }
            Err(err) => {
                tracing::warn!("AI summary generation failed: {}", err);
                if let Err(e) = db_handle.enqueue_ai_summary(rowid, &project_hash) {
                    tracing::warn!("Failed to enqueue AI summary after failure: {e}");
                }
            }
        }
    });
}

#[must_use] 
pub fn mtime_to_millis(t: SystemTime) -> i64 {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_millis()).unwrap_or(i64::MAX),
        Err(e) => {
            let dur = e.duration();
            let millis = i64::try_from(dur.as_millis()).unwrap_or(i64::MAX);
            // +1 ensures millis=0 is reserved for the epoch itself; pre-epoch times start at -1
            -(millis + 1)
        }
    }
}

#[must_use] 
pub fn millis_to_mtime(millis: i64) -> SystemTime {
    if millis >= 0 {
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(millis as u64)
    } else {
        // Reverse the +1 offset from mtime_to_millis: -1 maps back to epoch, -2 to 1ms before, etc.
        let before_epoch = (-i128::from(millis) - 1).max(0) as u128;
        let clamped = before_epoch.min(u128::from(u64::MAX)) as u64;
        std::time::UNIX_EPOCH - std::time::Duration::from_millis(clamped)
    }
}

fn enforce_storage_limit(
    database: &Database,
    project_size: u64,
    project_hash: &str,
    settings: &SnapshotSettings,
) -> Result<()> {
    let max_blob_size = std::cmp::max(
        (project_size as f64 * settings.storage.storage_limit_fraction) as u64,
        settings.storage.storage_min_bytes,
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
        chrono::Duration::hours(settings.compaction.emergency_expire_hours as i64);
    let now = chrono::Utc::now();
    for snap in database.list_snapshots_oldest_first(project_hash)? {
        if blob_size <= max_blob_size {
            break;
        }
        if snap.pinned {
            continue;
        }
        // Protect emergency snapshots within their retention window
        if snap.trigger == crate::db::SnapshotTrigger::Emergency {
            if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(&snap.timestamp) {
                let age = now.signed_duration_since(ts.with_timezone(&chrono::Utc));
                if age < emergency_retention {
                    continue;
                }
            }
        }
        // Estimate how much this snapshot frees
        let freed = database.estimate_snapshot_blob_size(snap.rowid)?;
        if let Err(err) = database.delete_snapshot(snap.rowid) {
            tracing::warn!("Failed to delete snapshot {} during storage limit enforcement: {err}", snap.rowid);
            continue;
        }
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    // ---- mtime_to_millis / millis_to_mtime round-trip tests ----

    #[test]
    fn roundtrip_epoch() {
        let t = UNIX_EPOCH;
        let ms = mtime_to_millis(t);
        assert_eq!(ms, 0);
        let back = millis_to_mtime(ms);
        assert_eq!(back, UNIX_EPOCH);
    }

    #[test]
    fn roundtrip_post_epoch() {
        let t = UNIX_EPOCH + Duration::from_millis(1_718_000_000_000);
        let ms = mtime_to_millis(t);
        assert_eq!(ms, 1_718_000_000_000);
        let back = millis_to_mtime(ms);
        assert_eq!(back, t);
    }

    #[test]
    fn roundtrip_one_millisecond() {
        let t = UNIX_EPOCH + Duration::from_millis(1);
        let ms = mtime_to_millis(t);
        assert_eq!(ms, 1);
        let back = millis_to_mtime(ms);
        assert_eq!(back, t);
    }

    #[test]
    fn roundtrip_pre_epoch() {
        // 1ms before epoch
        let t = UNIX_EPOCH - Duration::from_millis(1);
        let ms = mtime_to_millis(t);
        assert!(ms < 0, "Pre-epoch should give negative millis");
        let back = millis_to_mtime(ms);
        assert_eq!(back, t);
    }

    #[test]
    fn roundtrip_pre_epoch_larger() {
        // 5000ms before epoch
        let t = UNIX_EPOCH - Duration::from_millis(5000);
        let ms = mtime_to_millis(t);
        assert_eq!(ms, -5001); // -(5000 + 1) per the +1 offset
        let back = millis_to_mtime(ms);
        assert_eq!(back, t);
    }

    #[test]
    fn mtime_to_millis_pre_epoch_offset() {
        // The function adds +1 so that millis=0 is reserved for epoch itself.
        // 1ms before epoch → -(1 + 1) = -2
        let t = UNIX_EPOCH - Duration::from_millis(1);
        let ms = mtime_to_millis(t);
        assert_eq!(ms, -2);
    }

    #[test]
    fn millis_to_mtime_negative_one() {
        // -1 should map to exactly the epoch (the +1 offset reversal means -1 → 0ms before epoch)
        let t = millis_to_mtime(-1);
        assert_eq!(t, UNIX_EPOCH);
    }

    #[test]
    fn millis_to_mtime_zero() {
        let t = millis_to_mtime(0);
        assert_eq!(t, UNIX_EPOCH);
    }

    #[test]
    fn millis_to_mtime_large_positive() {
        // Year ~2070
        let ms: i64 = 3_155_760_000_000;
        let t = millis_to_mtime(ms);
        let expected = UNIX_EPOCH + Duration::from_millis(ms as u64);
        assert_eq!(t, expected);
    }

    #[test]
    fn mtime_to_millis_preserves_ordering() {
        let t1 = UNIX_EPOCH + Duration::from_millis(100);
        let t2 = UNIX_EPOCH + Duration::from_millis(200);
        assert!(mtime_to_millis(t1) < mtime_to_millis(t2));
    }

    #[test]
    fn mtime_to_millis_pre_epoch_preserves_ordering() {
        let t1 = UNIX_EPOCH - Duration::from_millis(200);
        let t2 = UNIX_EPOCH - Duration::from_millis(100);
        assert!(
            mtime_to_millis(t1) < mtime_to_millis(t2),
            "Earlier time should have smaller (more negative) millis"
        );
    }

    // ---- cached_matches_metadata tests ----

    #[test]
    fn cached_matches_when_all_equal() {
        let mtime = UNIX_EPOCH + Duration::from_millis(12345);
        let cached = CachedFileState {
            hash: "abc".into(),
            size: 100,
            mtime,
            stored: true,
            executable: false,
            storage_method: crate::cas::StorageMethod::Copy,
            is_symlink: false,
        };
        assert!(cached_matches_metadata(&cached, 100, mtime, false));
    }

    #[test]
    fn cached_mismatch_on_size() {
        let mtime = UNIX_EPOCH + Duration::from_millis(12345);
        let cached = CachedFileState {
            hash: "abc".into(),
            size: 100,
            mtime,
            stored: true,
            executable: false,
            storage_method: crate::cas::StorageMethod::Copy,
            is_symlink: false,
        };
        assert!(!cached_matches_metadata(&cached, 101, mtime, false));
    }

    #[test]
    fn cached_mismatch_on_mtime() {
        let mtime = UNIX_EPOCH + Duration::from_millis(12345);
        let cached = CachedFileState {
            hash: "abc".into(),
            size: 100,
            mtime,
            stored: true,
            executable: false,
            storage_method: crate::cas::StorageMethod::Copy,
            is_symlink: false,
        };
        let other_mtime = UNIX_EPOCH + Duration::from_millis(99999);
        assert!(!cached_matches_metadata(&cached, 100, other_mtime, false));
    }

    #[test]
    fn cached_mismatch_on_executable() {
        let mtime = UNIX_EPOCH + Duration::from_millis(12345);
        let cached = CachedFileState {
            hash: "abc".into(),
            size: 100,
            mtime,
            stored: true,
            executable: false,
            storage_method: crate::cas::StorageMethod::Copy,
            is_symlink: false,
        };
        assert!(!cached_matches_metadata(&cached, 100, mtime, true));
    }

    // ---- SnapshotScanResult tests ----

    #[test]
    fn scan_result_starts_empty() {
        let scan = SnapshotScanResult::new();
        assert!(!scan.has_changes);
        assert!(scan.new_files.is_empty());
        assert!(scan.files_for_manifest.is_empty());
        assert!(scan.deleted_for_manifest.is_empty());
        assert!(scan.current_hashes.is_empty());
    }
}

