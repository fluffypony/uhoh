use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

use super::AppState;
use crate::resolve;

#[derive(Deserialize)]
pub struct PaginationParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize)]
pub struct SearchParams {
    pub q: String,
    pub project: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
pub struct DiffParams {
    pub against: Option<String>,
    pub file: Option<String>,
}

#[derive(Deserialize)]
pub struct TimelineParams {
    pub from: Option<String>,
    pub to: Option<String>,
}

pub async fn list_projects(State(state): State<AppState>) -> impl IntoResponse {
    let db = state.database.clone();
    let result = tokio::task::spawn_blocking(move || db.list_projects()).await;
    match result {
        Ok(Ok(projects)) => {
            let list: Vec<Value> = projects
                .iter()
                .map(|p| {
                    json!({
                        "project_hash": p.hash,
                        "current_path": p.current_path,
                        "short_hash": &p.hash[..8.min(p.hash.len())],
                        "name": std::path::Path::new(&p.current_path)
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or(&p.current_path),
                    })
                })
                .collect();
            (StatusCode::OK, Json(json!({ "projects": list }))).into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn list_snapshots(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    Query(params): Query<PaginationParams>,
) -> impl IntoResponse {
    let db = state.database.clone();
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);
    let result =
        tokio::task::spawn_blocking(move || db.list_snapshots_paginated(&hash, limit, offset))
            .await;
    match result {
        Ok(Ok(snapshots)) => {
            let list: Vec<Value> = snapshots
                .iter()
                .map(|s| {
                    json!({
                        "id": crate::cas::id_to_base58(s.snapshot_id),
                        "rowid": s.rowid,
                        "timestamp": s.timestamp,
                        "trigger": s.trigger,
                        "message": s.message,
                        "pinned": s.pinned,
                        "file_count": s.file_count,
                        "ai_summary": s.ai_summary,
                    })
                })
                .collect();
            Json(json!({ "snapshots": list })).into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn get_snapshot_files(
    State(state): State<AppState>,
    Path((hash, snap_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let db = state.database.clone();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
        let id = crate::cas::base58_to_id(&snap_id)
            .ok_or_else(|| anyhow::anyhow!("Invalid snapshot ID"))?;
        let snap = db
            .find_snapshot_by_base58(&hash, &snap_id)?
            .ok_or_else(|| anyhow::anyhow!("Snapshot not found"))?;
        if snap.snapshot_id != id {
            anyhow::bail!("Snapshot does not belong to requested project");
        }
        let files = db.get_snapshot_files(snap.rowid)?;
        let tree = build_file_tree(&files);
        Ok(json!({
            "project_hash": hash,
            "snapshot_id": snap_id,
            "tree": tree,
            "flat_files": files.iter().map(|f| {
                json!({
                    "path": f.path,
                    "blob_hash": f.hash,
                    "size": f.size,
                    "is_binary": !f.stored,
                    "stored": f.stored,
                    "mtime": f.mtime,
                })
            }).collect::<Vec<_>>()
        }))
    })
    .await;
    match result {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

fn build_file_tree(files: &[crate::db::FileEntryRow]) -> Value {
    let mut root: BTreeMap<String, Value> = BTreeMap::new();
    for file in files {
        let parts: Vec<&str> = file.path.split('/').collect();
        insert_into_tree(
            &mut root,
            &parts,
            &file.path,
            &file.hash,
            file.size,
            file.is_symlink,
        );
    }
    json!(root.values().cloned().collect::<Vec<_>>())
}

fn insert_into_tree(
    tree: &mut BTreeMap<String, Value>,
    parts: &[&str],
    full_path: &str,
    blob_hash: &str,
    size: u64,
    is_symlink: bool,
) {
    if parts.is_empty() {
        return;
    }
    if parts.len() == 1 {
        tree.insert(
            parts[0].to_string(),
            json!({
                "type": "file",
                "name": parts[0],
                "path": full_path,
                "blob_hash": blob_hash,
                "size": size,
                "is_binary": false,
                "is_symlink": is_symlink,
            }),
        );
        return;
    }

    let dir_name = parts[0].to_string();
    let entry = tree
        .entry(dir_name.clone())
        .or_insert_with(|| json!({"type": "directory", "name": dir_name, "children": {}}));
    if let Some(children) = entry.get_mut("children") {
        if let Some(obj) = children.as_object_mut() {
            let mut child_map: BTreeMap<String, Value> =
                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            insert_into_tree(
                &mut child_map,
                &parts[1..],
                full_path,
                blob_hash,
                size,
                is_symlink,
            );
            *children = json!(child_map);
        }
    }
}

pub async fn get_diff(
    State(state): State<AppState>,
    Path((hash, snap_id)): Path<(String, String)>,
    Query(params): Query<DiffParams>,
) -> impl IntoResponse {
    let db = state.database.clone();
    let uhoh_dir = state.uhoh_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
        let snap = db
            .find_snapshot_by_base58(&hash, &snap_id)?
            .ok_or_else(|| anyhow::anyhow!("Snapshot not found"))?;
        let files = db.get_snapshot_files(snap.rowid)?;

        let base_rowid = if let Some(base_id) = params.against.as_ref() {
            Some(
                db.find_snapshot_by_base58(&hash, base_id)?
                    .ok_or_else(|| anyhow::anyhow!("Base snapshot not found"))?
                    .rowid,
            )
        } else {
            db.snapshot_before(&hash, snap.snapshot_id)?
                .map(|s| s.rowid)
        };
        // When no previous snapshot exists (first snapshot), use empty base
        // so all files show as additions instead of "no diff"
        let base_files = match base_rowid {
            Some(rowid) => db.get_snapshot_files(rowid).unwrap_or_default(),
            None => Vec::new(),
        };

        let filter = params.file.as_deref();
        let mut diffs = Vec::new();
        for file in &files {
            if let Some(filter_path) = filter {
                if file.path != filter_path {
                    continue;
                }
            }

            if !file.stored {
                diffs.push(json!({
                    "path": file.path,
                    "status": "binary",
                    "binary": true,
                }));
                continue;
            }

            let base_hash = base_files
                .iter()
                .find(|f| f.path == file.path)
                .map(|f| f.hash.as_str());
            let new_content =
                crate::cas::read_blob(&uhoh_dir.join("blobs"), &file.hash)?.unwrap_or_default();
            let old_content = if let Some(base_hash) = base_hash {
                if base_hash == file.hash {
                    continue;
                }
                crate::cas::read_blob(&uhoh_dir.join("blobs"), base_hash)?.unwrap_or_default()
            } else {
                Vec::new()
            };
            let diff = crate::diff_view::compute_structured_diff(
                &old_content,
                &new_content,
                &file.path,
                true,
            );
            diffs.push(serde_json::to_value(diff)?);
        }

        for base in &base_files {
            if files.iter().all(|f| f.path != base.path) {
                if let Some(filter_path) = filter {
                    if base.path != filter_path {
                        continue;
                    }
                }
                diffs.push(json!({"path": base.path, "status": "deleted"}));
            }
        }

        Ok(json!({
            "snapshot_id": snap_id,
            "base_snapshot_id": params.against,
            "diffs": diffs,
        }))
    })
    .await;
    match result {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn get_file_content(
    State(state): State<AppState>,
    Path((hash, snap_id, file_path)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let db = state.database.clone();
    let uhoh_dir = state.uhoh_dir.clone();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
        let project = resolve::resolve_project(&db, Some(&hash), None)?;
        let clean_path = file_path.trim_start_matches('/');
        resolve::validate_path_within_project(
            std::path::Path::new(&project.current_path),
            clean_path,
        )?;
        let snap = db
            .find_snapshot_by_base58(&hash, &snap_id)?
            .ok_or_else(|| anyhow::anyhow!("Snapshot not found"))?;
        let file = db
            .get_snapshot_files(snap.rowid)?
            .into_iter()
            .find(|f| f.path == clean_path)
            .ok_or_else(|| anyhow::anyhow!("File not found in snapshot"))?;
        if !file.stored {
            anyhow::bail!("File content is not stored for this snapshot entry");
        }
        Ok(crate::cas::read_blob(&uhoh_dir.join("blobs"), &file.hash)?.unwrap_or_default())
    })
    .await;
    match result {
        Ok(Ok(content)) => {
            let content_type = if content.iter().take(8192).any(|&b| b == 0) {
                "application/octet-stream"
            } else {
                "text/plain; charset=utf-8"
            };
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, content_type)],
                content,
            )
                .into_response()
        }
        Ok(Err(e)) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn create_snapshot(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let db = state.database.clone();
    let cfg = state.config.clone();
    let uhoh_dir = state.uhoh_dir.clone();
    let message = body
        .get("message")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let event_tx = state.event_tx.clone();

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
        let project = resolve::resolve_project(&db, Some(&hash), None)?;
        let info = crate::snapshot::create_snapshot(
            &uhoh_dir,
            &db,
            &project.hash,
            std::path::Path::new(&project.current_path),
            "api",
            message.as_deref(),
            &cfg,
            None,
        )?;
        if let Some(id) = info {
            if let Some(rowid) = db.latest_snapshot_rowid(&project.hash)? {
                if let Some(row) = db.get_snapshot_by_rowid(rowid)? {
                    let _ = event_tx.send(crate::server::events::ServerEvent::SnapshotCreated {
                        project_hash: project.hash.clone(),
                        snapshot_id: crate::cas::id_to_base58(id),
                        timestamp: row.timestamp,
                        trigger: "api".to_string(),
                        file_count: row.file_count as usize,
                        message: message.clone(),
                    });
                }
            }
            Ok(json!({ "snapshot_id": crate::cas::id_to_base58(id) }))
        } else {
            Ok(json!({ "message": "No changes detected" }))
        }
    })
    .await;
    match result {
        Ok(Ok(value)) => (StatusCode::CREATED, Json(value)).into_response(),
        Ok(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn restore_snapshot(
    State(state): State<AppState>,
    Path((hash, snap_id)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let dry_run = body
        .get("dry_run")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let confirm = body
        .get("confirm")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let target_path = body
        .get("target_path")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    if !dry_run && !confirm {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Non-dry-run restore requires 'confirm': true"})),
        )
            .into_response();
    }

    let db = state.database.clone();
    let uhoh_dir = state.uhoh_dir.clone();
    let restore_in_progress = state.restore_in_progress.clone();
    let restore_locks = state.restore_locks.clone();
    let event_tx = state.event_tx.clone();
    let restore_key = hash.clone();
    let target_path_for_task = target_path.clone();

    struct RestoreLockGuard {
        locks: std::sync::Arc<tokio::sync::Mutex<std::collections::HashSet<String>>>,
        key: String,
    }
    impl Drop for RestoreLockGuard {
        fn drop(&mut self) {
            let locks = self.locks.clone();
            let key = self.key.clone();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let mut guard = locks.lock().await;
                    guard.remove(&key);
                });
            }
        }
    }

    let mut lock_guard: Option<RestoreLockGuard> = None;
    if !dry_run {
        let mut locks = restore_locks.lock().await;
        if locks.contains(&restore_key) {
            return (
                StatusCode::CONFLICT,
                Json(json!({ "error": "Restore already in progress for this project" })),
            )
                .into_response();
        }
        locks.insert(restore_key.clone());
        lock_guard = Some(RestoreLockGuard {
            locks: restore_locks.clone(),
            key: restore_key.clone(),
        });
    }

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
        let project = resolve::resolve_project(&db, Some(&hash), None)?;
        if let Some(tp) = target_path_for_task.as_deref() {
            resolve::validate_path_within_project(std::path::Path::new(&project.current_path), tp)?;
        }

        struct RestoreFlagGuard {
            flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
        }
        impl Drop for RestoreFlagGuard {
            fn drop(&mut self) {
                self.flag.store(false, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let _restore_guard = if !dry_run {
            loop {
                if !restore_in_progress.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Some(RestoreFlagGuard {
                flag: restore_in_progress.clone(),
            })
        } else {
            None
        };

        let outcome = crate::restore::cmd_restore(
            &uhoh_dir,
            &db,
            &project,
            &snap_id,
            target_path_for_task.as_deref(),
            dry_run,
            true,
        )?;

        if !dry_run && outcome.applied {
            let _ = event_tx.send(crate::server::events::ServerEvent::SnapshotRestored {
                project_hash: project.hash.clone(),
                snapshot_id: outcome.snapshot_id.clone(),
                files_modified: outcome.files_restored,
                files_deleted: outcome.files_deleted,
            });
        }

        Ok(json!({
            "restored": outcome.applied,
            "dry_run": outcome.dry_run,
            "snapshot_id": outcome.snapshot_id,
            "files_modified": outcome.files_restored,
            "files_deleted": outcome.files_deleted,
            "files_to_modify": outcome.files_to_restore,
            "files_to_delete": outcome.files_to_delete,
        }))
    })
    .await;

    drop(lock_guard);

    match result {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> impl IntoResponse {
    let db = state.database.clone();
    let q = params.q.clone();
    let project = params.project.clone();
    let limit = params.limit.unwrap_or(50);
    let result =
        tokio::task::spawn_blocking(move || db.search_snapshots(&q, project.as_deref(), limit))
            .await;
    match result {
        Ok(Ok(results)) => {
            let list: Vec<Value> = results
                .iter()
                .map(|r| {
                    json!({
                        "snapshot_id": crate::cas::id_to_base58(r.snapshot_id),
                        "timestamp": r.timestamp,
                        "trigger": r.trigger,
                        "message": r.message,
                        "ai_summary": r.ai_summary,
                        "match_context": r.match_context,
                    })
                })
                .collect();
            Json(json!({ "results": list })).into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn get_timeline(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    Query(params): Query<TimelineParams>,
) -> impl IntoResponse {
    let db = state.database.clone();
    let hash_for_response = hash.clone();
    let result = tokio::task::spawn_blocking(move || {
        db.list_snapshot_summaries(&hash, params.from.as_deref(), params.to.as_deref())
    })
    .await;
    match result {
        Ok(Ok(summaries)) => {
            let mut manual = Vec::new();
            let mut auto_saves = Vec::new();
            for s in &summaries {
                let entry = json!({
                    "id": crate::cas::id_to_base58(s.snapshot_id),
                    "timestamp": s.timestamp,
                    "message": s.message,
                    "pinned": s.pinned,
                    "file_count": s.file_count,
                });
                match s.trigger.as_str() {
                    "manual" | "mcp" | "api" => manual.push(entry),
                    _ => auto_saves.push(entry),
                }
            }
            Json(json!({
                "project_hash": hash_for_response,
                "tracks": {
                    "manual": manual,
                    "auto_saves": auto_saves,
                },
                "total_snapshots": summaries.len(),
            }))
            .into_response()
        }
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}
