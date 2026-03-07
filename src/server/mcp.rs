use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::AppState;
use crate::resolve;

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcResponse {
    fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    fn error(id: Option<Value>, code: i64, message: String) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data: None,
            }),
            id,
        }
    }
}

pub async fn handle_mcp(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<JsonRpcRequest>,
) -> impl IntoResponse {
    if !super::auth::validate_host(&headers, state.config.server.port) {
        return (
            StatusCode::FORBIDDEN,
            Json(JsonRpcResponse::error(
                request.id,
                -32600,
                "Invalid Host header".to_string(),
            )),
        );
    }

    if !super::auth::validate_origin(&headers) {
        return (
            StatusCode::FORBIDDEN,
            Json(JsonRpcResponse::error(
                request.id,
                -32600,
                "Invalid Origin header".to_string(),
            )),
        );
    }

    if request.jsonrpc != "2.0" {
        return (
            StatusCode::BAD_REQUEST,
            Json(JsonRpcResponse::error(
                request.id,
                -32600,
                "Invalid jsonrpc version".to_string(),
            )),
        );
    }

    // JSON-RPC notifications (no id) must not receive responses per spec.
    if request.id.is_none() {
        return (StatusCode::ACCEPTED, Json(JsonRpcResponse::success(None, json!({}))));
    }

    let response = match request.method.as_str() {
        "initialize" => JsonRpcResponse::success(
            request.id,
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": "uhoh",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        ),
        "ping" => JsonRpcResponse::success(request.id, json!({})),
        "notifications/initialized" => JsonRpcResponse::success(request.id, json!({})),
        "tools/list" => JsonRpcResponse::success(request.id, crate::mcp_tools::tool_definitions()),
        "tools/call" => handle_tools_call(state, request.id, request.params).await,
        _ => JsonRpcResponse::error(
            request.id,
            -32601,
            format!("Method not found: {}", request.method),
        ),
    };

    (StatusCode::OK, Json(response))
}

async fn handle_tools_call(
    state: AppState,
    id: Option<Value>,
    params: Option<Value>,
) -> JsonRpcResponse {
    let params = match params {
        Some(v) => v,
        None => return JsonRpcResponse::error(id, -32602, "Missing params".to_string()),
    };
    let tool_name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let args = params.get("arguments").cloned().unwrap_or(json!({}));

    match tool_name {
        "create_snapshot" => tool_create_snapshot(state, id, args).await,
        "list_snapshots" => tool_list_snapshots(state, id, args).await,
        "restore_snapshot" => tool_restore_snapshot(state, id, args).await,
        "uhoh_pre_notify" => tool_pre_notify(state, id, args).await,
        _ => JsonRpcResponse::error(id, -32602, format!("Unknown tool: {tool_name}")),
    }
}

async fn tool_pre_notify(state: AppState, id: Option<Value>, args: Value) -> JsonRpcResponse {
    let agent = args
        .get("agent")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown-agent")
        .to_string();
    let action = args
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown-action")
        .to_string();
    let path = args
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let db = state.database.clone();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
        let mut event = crate::event_ledger::new_event("agent", "pre_notify", "info");
        event.agent_name = Some(agent.clone());
        event.path = path.clone();
        event.detail = Some(format!("action={action}"));
        let ledger = crate::event_ledger::EventLedger::new(db.clone());
        let event_id = ledger.append(event)?;
        Ok(json!({
            "content": [{"type": "text", "text": "pre-notify accepted"}],
            "event_id": event_id,
        }))
    })
    .await;

    match result {
        Ok(Ok(value)) => JsonRpcResponse::success(id, value),
        Ok(Err(e)) => JsonRpcResponse::error(id, -32000, e.to_string()),
        Err(e) => JsonRpcResponse::error(id, -32000, format!("Internal error: {e}")),
    }
}

async fn tool_create_snapshot(state: AppState, id: Option<Value>, args: Value) -> JsonRpcResponse {
    let db = state.database.clone();
    let cfg = state.config.clone();
    let uhoh_dir = state.uhoh_dir.clone();
    let event_tx = state.event_tx.clone();
    let path = args
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let hash = args
        .get("project_hash")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let message = args
        .get("message")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
        let project = resolve::resolve_project(&db, path.as_deref().or(hash.as_deref()), None)?;
        let project_path = std::path::Path::new(&project.current_path);
        let info = crate::snapshot::create_snapshot(
            &uhoh_dir,
            &db,
            &project.hash,
            project_path,
            "mcp",
            message.as_deref(),
            &cfg,
            None,
        )?;

        if let Some(snapshot_id) = info {
            if let Some(rowid) = db.latest_snapshot_rowid(&project.hash)? {
                if let Some(row) = db.get_snapshot_by_rowid(rowid)? {
                    let _ = event_tx.send(crate::server::events::ServerEvent::SnapshotCreated {
                        project_hash: project.hash.clone(),
                        snapshot_id: crate::cas::id_to_base58(snapshot_id),
                        timestamp: row.timestamp.clone(),
                        trigger: "mcp".to_string(),
                        file_count: row.file_count as usize,
                        message: message.clone(),
                    });
                }
            }
            Ok(json!({
                "content": [{"type": "text", "text": format!("Snapshot created: {}", crate::cas::id_to_base58(snapshot_id))}],
                "snapshot_id": crate::cas::id_to_base58(snapshot_id)
            }))
        } else {
            Ok(json!({
                "content": [{"type": "text", "text": "No changes detected; snapshot not created."}]
            }))
        }
    })
    .await;

    match result {
        Ok(Ok(value)) => JsonRpcResponse::success(id, value),
        Ok(Err(e)) => JsonRpcResponse::error(id, -32000, e.to_string()),
        Err(e) => JsonRpcResponse::error(id, -32000, format!("Internal error: {e}")),
    }
}

async fn tool_list_snapshots(state: AppState, id: Option<Value>, args: Value) -> JsonRpcResponse {
    let db = state.database.clone();
    let path = args
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let hash = args
        .get("project_hash")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as usize;
    let offset = args.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
        let project = resolve::resolve_project(&db, path.as_deref().or(hash.as_deref()), None)?;
        let snapshots = db.list_snapshots_paginated(&project.hash, limit, offset)?;
        let list: Vec<Value> = snapshots
            .iter()
            .map(|s| {
                json!({
                    "id": crate::cas::id_to_base58(s.snapshot_id),
                    "timestamp": s.timestamp,
                    "trigger": s.trigger,
                    "message": s.message,
                    "pinned": s.pinned,
                    "file_count": s.file_count,
                    "ai_summary": s.ai_summary,
                })
            })
            .collect();
        Ok(json!({
            "content": [{"type":"text", "text": format!("Found {} snapshots", list.len())}],
            "snapshots": list,
            "project_hash": project.hash,
            "project_path": project.current_path,
        }))
    })
    .await;

    match result {
        Ok(Ok(value)) => JsonRpcResponse::success(id, value),
        Ok(Err(e)) => JsonRpcResponse::error(id, -32000, e.to_string()),
        Err(e) => JsonRpcResponse::error(id, -32000, format!("Internal error: {e}")),
    }
}

async fn tool_restore_snapshot(state: AppState, id: Option<Value>, args: Value) -> JsonRpcResponse {
    let snapshot_id = match args.get("snapshot_id").and_then(|v| v.as_str()) {
        Some(v) => v.to_string(),
        None => return JsonRpcResponse::error(id, -32602, "Missing snapshot_id".to_string()),
    };
    let dry_run = args
        .get("dry_run")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let confirm = args
        .get("confirm")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let target_path = args
        .get("target_path")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    if !dry_run && !confirm {
        return JsonRpcResponse::error(
            id,
            -32602,
            "Non-dry-run restore requires confirm: true".to_string(),
        );
    }

    let db = state.database.clone();
    let path = args
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let hash = args
        .get("project_hash")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let target_path_for_task = target_path.clone();
    let uhoh_dir = state.uhoh_dir.clone();
    let restore_in_progress = state.restore_in_progress.clone();
    let restore_locks = state.restore_locks.clone();
    let event_tx = state.event_tx.clone();

    struct McpRestoreLockGuard {
        locks: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
        key: String,
    }
    impl Drop for McpRestoreLockGuard {
        fn drop(&mut self) {
            if let Ok(mut guard) = self.locks.lock() {
                guard.remove(&self.key);
            }
        }
    }

    let mut _lock_guard: Option<McpRestoreLockGuard> = None;
    if !dry_run {
        // Use hash preferentially to match REST API lock key behavior
        let lock_key = hash
            .clone()
            .or(path.clone())
            .unwrap_or_else(|| "default".to_string());
        let mut locks = match restore_locks.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if locks.contains(&lock_key) {
            return JsonRpcResponse::error(
                id,
                -32000,
                "Restore already in progress for this project".to_string(),
            );
        }
        locks.insert(lock_key.clone());
        _lock_guard = Some(McpRestoreLockGuard {
            locks: restore_locks.clone(),
            key: lock_key,
        });
    }

    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
        let project = resolve::resolve_project(&db, path.as_deref().or(hash.as_deref()), None)?;
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
            if restore_in_progress.swap(true, std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("Another restore is already in progress");
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
            &snapshot_id,
            target_path_for_task.as_deref(),
            dry_run,
            true,
        )?;

        if !dry_run && outcome.applied {
            let _ = event_tx.send(crate::server::events::ServerEvent::SnapshotRestored {
                project_hash: project.hash,
                snapshot_id: outcome.snapshot_id.clone(),
                files_modified: outcome.files_restored,
                files_deleted: outcome.files_deleted,
            });
        }

        Ok(json!({
            "content": [{"type":"text", "text": if dry_run {
                format!("Dry run complete for snapshot {snapshot_id}")
            } else if outcome.applied {
                format!("Snapshot {snapshot_id} restored")
            } else {
                format!("Restore for snapshot {snapshot_id} was not applied")
            }}],
            "restored": outcome.applied,
            "dry_run": outcome.dry_run,
            "files_modified": outcome.files_restored,
            "files_deleted": outcome.files_deleted,
            "files_to_modify": outcome.files_to_restore,
            "files_to_delete": outcome.files_to_delete,
        }))
    })
    .await;

    // Lock cleanup handled by McpRestoreLockGuard drop
    drop(_lock_guard);

    match result {
        Ok(Ok(value)) => JsonRpcResponse::success(id, value),
        Ok(Err(e)) => JsonRpcResponse::error(id, -32000, e.to_string()),
        Err(e) => JsonRpcResponse::error(id, -32000, format!("Internal error: {e}")),
    }
}
