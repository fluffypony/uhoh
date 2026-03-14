use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

use super::ApiState;
use crate::project_service::RestoreProjectError;
use crate::resolve;

#[derive(Deserialize)]
#[non_exhaustive]
pub struct PaginationParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize)]
#[non_exhaustive]
pub struct SearchParams {
    pub q: String,
    pub project: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Deserialize)]
#[non_exhaustive]
pub struct DiffParams {
    pub against: Option<String>,
    pub file: Option<String>,
}

#[derive(Deserialize)]
#[non_exhaustive]
pub struct TimelineParams {
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateSnapshotBody {
    message: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SetSnapshotPinBody {
    pinned: bool,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RestoreSnapshotBody {
    dry_run: bool,
    confirm: bool,
    target_path: Option<String>,
}

impl Default for RestoreSnapshotBody {
    fn default() -> Self {
        Self {
            dry_run: true,
            confirm: false,
            target_path: None,
        }
    }
}

type ApiResult<T> = Result<T, ApiError>;

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn invalid_input(message: impl ToString) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: message.to_string(),
        }
    }

    fn not_found(message: impl ToString) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: message.to_string(),
        }
    }

    fn conflict(message: impl ToString) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "conflict",
            message: message.to_string(),
        }
    }

    fn internal(message: impl ToString) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal",
            message: message.to_string(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        ApiError::internal(err)
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(err: serde_json::Error) -> Self {
        ApiError::internal(err)
    }
}

impl From<RestoreProjectError> for ApiError {
    fn from(err: RestoreProjectError) -> Self {
        match err {
            RestoreProjectError::NotFound(message) => ApiError::not_found(message),
            RestoreProjectError::Conflict(message) => ApiError::conflict(message),
            RestoreProjectError::InvalidInput(message) => ApiError::invalid_input(message),
            RestoreProjectError::Internal(err) => ApiError::internal(err),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(json!({
                "error": {
                    "code": self.code,
                    "message": self.message,
                }
            })),
        )
            .into_response()
    }
}

fn internal_error_response(error: impl ToString) -> axum::response::Response {
    ApiError::internal(error).into_response()
}

fn bad_request_response(error: impl ToString) -> axum::response::Response {
    ApiError::invalid_input(error).into_response()
}

pub(crate) async fn list_projects(State(state): State<ApiState>) -> impl IntoResponse {
    let db = state.runtime.database();
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
        Ok(Err(e)) => internal_error_response(e),
        Err(e) => internal_error_response(e),
    }
}

pub(crate) async fn list_snapshots(
    State(state): State<ApiState>,
    Path(hash): Path<String>,
    Query(params): Query<PaginationParams>,
) -> impl IntoResponse {
    let db = state.runtime.database();
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
        Ok(Err(e)) => internal_error_response(e),
        Err(e) => internal_error_response(e),
    }
}

pub(crate) async fn get_snapshot(
    State(state): State<ApiState>,
    Path((hash, snap_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let db = state.runtime.database();
    let result = tokio::task::spawn_blocking(move || -> ApiResult<Value> {
        let snap = db
            .find_snapshot_by_base58(&hash, &snap_id)?
            .ok_or_else(|| ApiError::not_found("Snapshot not found"))?;
        Ok(json!({
            "id": crate::cas::id_to_base58(snap.snapshot_id),
            "rowid": snap.rowid,
            "timestamp": snap.timestamp,
            "trigger": snap.trigger,
            "message": snap.message,
            "pinned": snap.pinned,
            "file_count": snap.file_count,
            "ai_summary": snap.ai_summary,
        }))
    })
    .await;

    match result {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(err)) => err.into_response(),
        Err(e) => internal_error_response(e),
    }
}

pub(crate) async fn get_snapshot_files(
    State(state): State<ApiState>,
    Path((hash, snap_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let db = state.runtime.database();
    let result = tokio::task::spawn_blocking(move || -> ApiResult<Value> {
        let id = crate::cas::base58_to_id(&snap_id)
            .ok_or_else(|| ApiError::invalid_input("Invalid snapshot ID"))?;
        if id == 0 {
            return Err(ApiError::invalid_input(
                "Snapshot ID '1' maps to reserved value 0; valid IDs start from '2'",
            ));
        }
        let snap = db
            .find_snapshot_by_base58(&hash, &snap_id)?
            .ok_or_else(|| ApiError::not_found("Snapshot not found"))?;
        if snap.snapshot_id != id {
            return Err(ApiError::invalid_input(
                "Snapshot does not belong to requested project",
            ));
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
        Ok(Err(err)) => err.into_response(),
        Err(e) => internal_error_response(e),
    }
}

fn build_file_tree(files: &[crate::db::FileEntryRow]) -> Value {
    let mut root: BTreeMap<String, FileTreeNode> = BTreeMap::new();
    for file in files {
        let parts: Vec<&str> = file.path.split('/').collect();
        insert_into_tree(&mut root, &parts, file);
    }
    Value::Array(root.into_values().map(FileTreeNode::into_value).collect())
}

enum FileTreeNode {
    Directory(FileTreeDirectory),
    File(FileTreeLeaf),
}

struct FileTreeDirectory {
    name: String,
    children: BTreeMap<String, FileTreeNode>,
}

struct FileTreeLeaf {
    name: String,
    path: String,
    blob_hash: String,
    size: u64,
    is_symlink: bool,
}

impl FileTreeNode {
    fn into_value(self) -> Value {
        match self {
            FileTreeNode::Directory(dir) => json!({
                "type": "directory",
                "name": dir.name,
                "children": dir.children.into_values().map(FileTreeNode::into_value).collect::<Vec<_>>(),
            }),
            FileTreeNode::File(file) => json!({
                "type": "file",
                "name": file.name,
                "path": file.path,
                "blob_hash": file.blob_hash,
                "size": file.size,
                "is_binary": false,
                "is_symlink": file.is_symlink,
            }),
        }
    }
}

fn insert_into_tree(
    tree: &mut BTreeMap<String, FileTreeNode>,
    parts: &[&str],
    file: &crate::db::FileEntryRow,
) {
    if parts.is_empty() {
        return;
    }
    if parts.len() == 1 {
        tree.insert(
            parts[0].to_string(),
            FileTreeNode::File(FileTreeLeaf {
                name: parts[0].to_string(),
                path: file.path.clone(),
                blob_hash: file.hash.clone(),
                size: file.size,
                is_symlink: file.is_symlink,
            }),
        );
        return;
    }

    let dir_name = parts[0].to_string();
    let entry = tree.entry(dir_name.clone()).or_insert_with(|| {
        FileTreeNode::Directory(FileTreeDirectory {
            name: dir_name,
            children: BTreeMap::new(),
        })
    });
    if let FileTreeNode::Directory(directory) = entry {
        insert_into_tree(&mut directory.children, &parts[1..], file);
    }
}

pub(crate) async fn get_diff(
    State(state): State<ApiState>,
    Path((hash, snap_id)): Path<(String, String)>,
    Query(params): Query<DiffParams>,
) -> impl IntoResponse {
    let db = state.runtime.database();
    let uhoh_dir = state.runtime.uhoh_dir_buf();
    let result = tokio::task::spawn_blocking(move || -> ApiResult<Value> {
        let snap = db
            .find_snapshot_by_base58(&hash, &snap_id)?
            .ok_or_else(|| ApiError::not_found("Snapshot not found"))?;
        let files = db.get_snapshot_files(snap.rowid)?;

        let base_rowid = if let Some(base_id) = params.against.as_ref() {
            Some(
                db.find_snapshot_by_base58(&hash, base_id)?
                    .ok_or_else(|| ApiError::not_found("Base snapshot not found"))?
                    .rowid,
            )
        } else {
            db.snapshot_before(&hash, snap.snapshot_id)?
                .map(|s| s.rowid)
        };
        // When no previous snapshot exists (first snapshot), use empty base
        // so all files show as additions instead of "no diff"
        let base_files = match base_rowid {
            Some(rowid) => db.get_snapshot_files(rowid)?,
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
                // UI currently renders plain escaped text lines, so disable
                // backend syntax highlighting generation to avoid wasted CPU.
                false,
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
        Ok(Err(err)) => err.into_response(),
        Err(e) => internal_error_response(e),
    }
}

pub(crate) async fn get_file_content(
    State(state): State<ApiState>,
    Path((hash, snap_id, file_path)): Path<(String, String, String)>,
) -> impl IntoResponse {
    let db = state.runtime.database();
    let uhoh_dir = state.runtime.uhoh_dir_buf();
    let result = tokio::task::spawn_blocking(move || -> ApiResult<Vec<u8>> {
        let project =
            resolve::resolve_project(&db, Some(&hash), None).map_err(ApiError::not_found)?;
        let clean_path = file_path.trim_start_matches('/').replace('\\', "/");
        resolve::validate_path_within_project(
            std::path::Path::new(&project.current_path),
            &clean_path,
        )
        .map_err(ApiError::invalid_input)?;
        let snap = db
            .find_snapshot_by_base58(&hash, &snap_id)?
            .ok_or_else(|| ApiError::not_found("Snapshot not found"))?;
        let file = db
            .get_snapshot_files(snap.rowid)?
            .into_iter()
            .find(|f| f.path == clean_path)
            .ok_or_else(|| ApiError::not_found("File not found in snapshot"))?;
        if !file.stored {
            return Err(ApiError::invalid_input(
                "File content is not stored for this snapshot entry",
            ));
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
        Ok(Err(err)) => err.into_response(),
        Err(e) => internal_error_response(e),
    }
}

pub(crate) async fn create_snapshot(
    State(state): State<ApiState>,
    Path(hash): Path<String>,
    Json(body): Json<CreateSnapshotBody>,
) -> impl IntoResponse {
    let db = state.runtime.database();
    let uhoh_dir = state.runtime.uhoh_dir_buf();
    let snapshot_runtime = state.runtime.snapshot_runtime();
    let message = body.message;
    let event_tx = state.runtime.event_tx();

    let result = tokio::task::spawn_blocking(move || -> ApiResult<Value> {
        let project =
            resolve::resolve_project(&db, Some(&hash), None).map_err(ApiError::not_found)?;
        let result = crate::project_service::create_project_snapshot(
            &uhoh_dir,
            &db,
            &snapshot_runtime,
            &project,
            crate::db::SnapshotTrigger::Api,
            message.as_deref(),
        )
        .map_err(ApiError::invalid_input)?;
        if let Some(id) = result.snapshot_id {
            if let (Some(tx), Some(event)) = (event_tx.as_ref(), result.snapshot_event) {
                crate::events::publish_event(tx, event);
            }
            Ok(json!({ "snapshot_id": crate::cas::id_to_base58(id) }))
        } else {
            Ok(json!({ "message": "No changes detected" }))
        }
    })
    .await;
    match result {
        Ok(Ok(value)) => (StatusCode::CREATED, Json(value)).into_response(),
        Ok(Err(err)) => err.into_response(),
        Err(e) => internal_error_response(e),
    }
}

pub(crate) async fn set_snapshot_pin(
    State(state): State<ApiState>,
    Path((hash, snap_id)): Path<(String, String)>,
    Json(body): Json<SetSnapshotPinBody>,
) -> impl IntoResponse {
    let pinned = body.pinned;

    let db = state.runtime.database();
    let result = tokio::task::spawn_blocking(move || -> ApiResult<Value> {
        let snap = db
            .find_snapshot_by_base58(&hash, &snap_id)?
            .ok_or_else(|| ApiError::not_found("Snapshot not found"))?;
        db.pin_snapshot(snap.rowid, pinned)?;
        Ok(json!({
            "id": crate::cas::id_to_base58(snap.snapshot_id),
            "pinned": pinned,
        }))
    })
    .await;

    match result {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(err)) => err.into_response(),
        Err(e) => internal_error_response(e),
    }
}

pub(crate) async fn restore_snapshot(
    State(state): State<ApiState>,
    Path((hash, snap_id)): Path<(String, String)>,
    Json(body): Json<RestoreSnapshotBody>,
) -> impl IntoResponse {
    let dry_run = body.dry_run;
    let confirm = body.confirm;
    let target_path = body.target_path;
    if !dry_run && !confirm {
        return bad_request_response("Non-dry-run restore requires 'confirm': true");
    }

    let db = state.runtime.database();
    let restore_runtime = state.runtime.restore_runtime();
    let snapshot_runtime = state.runtime.snapshot_runtime();
    let target_path_for_validation = target_path.clone();
    let target_path_for_task = target_path.clone();

    let result = tokio::task::spawn_blocking(move || -> ApiResult<Value> {
        let project =
            resolve::resolve_project(&db, Some(&hash), None).map_err(ApiError::not_found)?;
        let snapshot = db
            .find_snapshot_by_base58(&hash, &snap_id)?
            .ok_or_else(|| ApiError::not_found("Snapshot not found"))?;
        if let Some(tp) = target_path_for_validation.as_deref() {
            resolve::validate_path_within_project(std::path::Path::new(&project.current_path), tp)
                .map_err(ApiError::invalid_input)?;
            let encoded_target = crate::cas::encode_relpath(std::path::Path::new(tp));
            let target_exists = db
                .get_snapshot_files(snapshot.rowid)?
                .into_iter()
                .any(|file| file.path == encoded_target);
            if !target_exists {
                return Err(ApiError::not_found(format!(
                    "File '{tp}' was not found in snapshot {snap_id}"
                )));
            }
        }

        let outcome = crate::project_service::restore_project_snapshot(
            &restore_runtime,
            &snapshot_runtime,
            &project,
            &snap_id,
            dry_run,
            target_path_for_task.as_deref(),
        )?;

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

    match result {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(err)) => err.into_response(),
        Err(e) => internal_error_response(e),
    }
}

pub(crate) async fn search(
    State(state): State<ApiState>,
    Query(params): Query<SearchParams>,
) -> impl IntoResponse {
    let db = state.runtime.database();
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
        Ok(Err(e)) => internal_error_response(e),
        Err(e) => internal_error_response(e),
    }
}

pub(crate) async fn get_timeline(
    State(state): State<ApiState>,
    Path(hash): Path<String>,
    Query(params): Query<TimelineParams>,
) -> impl IntoResponse {
    let db = state.runtime.database();
    let hash_for_response = hash.clone();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
        let summaries =
            db.list_snapshot_summaries(&hash, params.from.as_deref(), params.to.as_deref())?;
        let mut entries = Vec::with_capacity(summaries.len());
        for s in &summaries {
            let ai_summary = db
                .get_snapshot_by_rowid(s.rowid)?
                .and_then(|row| row.ai_summary);
            entries.push((s.clone(), ai_summary));
        }
        Ok(entries)
    })
    .await;
    match result {
        Ok(Ok(summaries)) => {
            let mut manual = Vec::new();
            let mut auto_saves = Vec::new();
            for (s, ai_summary) in &summaries {
                let entry = json!({
                    "id": crate::cas::id_to_base58(s.snapshot_id),
                    "timestamp": s.timestamp,
                    "trigger": s.trigger,
                    "message": s.message,
                    "pinned": s.pinned,
                    "file_count": s.file_count,
                    "ai_summary": ai_summary,
                });
                if s.trigger.is_manual_kind() {
                    manual.push(entry);
                } else {
                    auto_saves.push(entry);
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
        Ok(Err(e)) => internal_error_response(e),
        Err(e) => internal_error_response(e),
    }
}
