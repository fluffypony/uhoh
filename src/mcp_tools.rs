use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::config::Config;
use crate::db::Database;
use crate::event_ledger::{new_event, EventLedger};
use crate::resolve;
use crate::server::events::ServerEvent;
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct McpToolError {
    pub code: i64,
    pub message: String,
}

impl McpToolError {
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: -32000,
            message: message.into(),
        }
    }
}

#[derive(Clone)]
pub struct McpToolContext {
    pub database: Arc<Database>,
    pub uhoh_dir: std::path::PathBuf,
    pub config: Config,
    pub event_tx: Option<broadcast::Sender<ServerEvent>>,
    pub restore_in_progress: Option<RestoreInProgressFlag>,
    pub restore_locks: Option<Arc<std::sync::Mutex<std::collections::HashSet<String>>>>,
}

#[derive(Clone)]
pub enum RestoreInProgressFlag {
    Shared(Arc<std::sync::atomic::AtomicBool>),
    Global(&'static std::sync::atomic::AtomicBool),
}

impl RestoreInProgressFlag {
    fn swap(&self, value: bool) -> bool {
        match self {
            Self::Shared(flag) => flag.swap(value, std::sync::atomic::Ordering::SeqCst),
            Self::Global(flag) => flag.swap(value, std::sync::atomic::Ordering::SeqCst),
        }
    }

    fn store(&self, value: bool) {
        match self {
            Self::Shared(flag) => flag.store(value, std::sync::atomic::Ordering::SeqCst),
            Self::Global(flag) => flag.store(value, std::sync::atomic::Ordering::SeqCst),
        }
    }
}

/// Shared MCP tool definitions used by both HTTP and STDIO transports.
///
/// Tool *handling* is currently duplicated between `server/mcp.rs` (handle_tools_call)
/// and `mcp_stdio.rs` (handle_stdio_tool_call) due to async/sync transport differences.
/// When modifying tool behavior, update both implementations to keep them in sync.
pub fn tool_definitions() -> serde_json::Value {
    json!({
        "tools": [
            {
                "name": "create_snapshot",
                "description": "Create a manual snapshot of a project.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "project_hash": { "type": "string" },
                        "message": { "type": "string" }
                    }
                }
            },
            {
                "name": "list_snapshots",
                "description": "List snapshots for a project.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "project_hash": { "type": "string" },
                        "limit": { "type": "integer", "default": 20 },
                        "offset": { "type": "integer", "default": 0 }
                    }
                }
            },
            {
                "name": "restore_snapshot",
                "description": "Restore to a previous snapshot. Defaults to dry run.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "snapshot_id": { "type": "string" },
                        "path": { "type": "string" },
                        "project_hash": { "type": "string" },
                        "dry_run": { "type": "boolean", "default": true },
                        "confirm": { "type": "boolean", "default": false },
                        "target_path": {
                            "type": "string",
                            "description": "Optional single file path to restore within the snapshot"
                        }
                    },
                    "required": ["snapshot_id"]
                }
            },
            {
                "name": "uhoh_pre_notify",
                "description": "Cooperative pre-action notification for agent actions.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string" },
                        "action": { "type": "string" },
                        "path": { "type": "string" }
                    },
                    "required": ["agent", "action"]
                }
            }
        ]
    })
}

pub fn parse_tool_call(params: Option<Value>) -> Result<(String, Value), McpToolError> {
    let params = params.ok_or_else(|| McpToolError::invalid_params("Missing params"))?;
    let tool_name = params
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if tool_name.is_empty() {
        return Err(McpToolError::invalid_params("Missing tool name"));
    }
    let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
    Ok((tool_name, args))
}

pub fn dispatch_tool_call(
    context: &McpToolContext,
    tool_name: &str,
    args: Value,
) -> Result<Value, McpToolError> {
    match tool_name {
        "create_snapshot" => tool_create_snapshot(context, args),
        "list_snapshots" => tool_list_snapshots(context, args),
        "restore_snapshot" => tool_restore_snapshot(context, args),
        "uhoh_pre_notify" => tool_pre_notify(context, args),
        _ => Err(McpToolError::invalid_params(format!(
            "Unknown tool: {tool_name}"
        ))),
    }
}

fn tool_create_snapshot(context: &McpToolContext, args: Value) -> Result<Value, McpToolError> {
    let path = args.get("path").and_then(|v| v.as_str());
    let hash = args.get("project_hash").and_then(|v| v.as_str());
    let message = args.get("message").and_then(|v| v.as_str());

    let project = resolve::resolve_project(&context.database, path.or(hash), None)
        .map_err(|e| McpToolError::internal(e.to_string()))?;
    let project_path = Path::new(&project.current_path);

    let info = crate::snapshot::create_snapshot(
        &context.uhoh_dir,
        &context.database,
        &project.hash,
        project_path,
        "mcp",
        message,
        &context.config,
        None,
    )
    .map_err(|e| McpToolError::internal(e.to_string()))?;

    if let Some(snapshot_id) = info {
        if let Some(tx) = &context.event_tx {
            if let Ok(Some(rowid)) = context.database.latest_snapshot_rowid(&project.hash) {
                if let Ok(Some(row)) = context.database.get_snapshot_by_rowid(rowid) {
                    let _ = tx.send(ServerEvent::SnapshotCreated {
                        project_hash: project.hash.clone(),
                        snapshot_id: crate::cas::id_to_base58(snapshot_id),
                        timestamp: row.timestamp,
                        trigger: "mcp".to_string(),
                        file_count: row.file_count as usize,
                        message: message.map(str::to_string),
                    });
                }
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
}

fn tool_list_snapshots(context: &McpToolContext, args: Value) -> Result<Value, McpToolError> {
    let path = args.get("path").and_then(|v| v.as_str());
    let hash = args.get("project_hash").and_then(|v| v.as_str());
    let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as usize;
    let offset = args.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

    let project = resolve::resolve_project(&context.database, path.or(hash), None)
        .map_err(|e| McpToolError::internal(e.to_string()))?;
    let snapshots = context
        .database
        .list_snapshots_paginated(&project.hash, limit, offset)
        .map_err(|e| McpToolError::internal(e.to_string()))?;

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
}

fn tool_restore_snapshot(context: &McpToolContext, args: Value) -> Result<Value, McpToolError> {
    let snapshot_id = args
        .get("snapshot_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| McpToolError::invalid_params("Missing snapshot_id"))?
        .to_string();
    let dry_run = args
        .get("dry_run")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let confirm = args
        .get("confirm")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !dry_run && !confirm {
        return Err(McpToolError::invalid_params(
            "Non-dry-run restore requires confirm: true",
        ));
    }

    let path = args.get("path").and_then(|v| v.as_str());
    let hash = args.get("project_hash").and_then(|v| v.as_str());
    let target_path = args.get("target_path").and_then(|v| v.as_str());

    let project = resolve::resolve_project(&context.database, path.or(hash), None)
        .map_err(|e| McpToolError::internal(e.to_string()))?;

    struct RestoreLockGuard {
        locks: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
        key: String,
    }
    impl Drop for RestoreLockGuard {
        fn drop(&mut self) {
            if let Ok(mut guard) = self.locks.lock() {
                guard.remove(&self.key);
            }
        }
    }

    let _project_lock_guard = if !dry_run {
        if let Some(restore_locks) = &context.restore_locks {
            let mut locks = match restore_locks.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if locks.contains(&project.hash) {
                return Err(McpToolError::internal(
                    "Restore already in progress for this project",
                ));
            }
            locks.insert(project.hash.clone());
            Some(RestoreLockGuard {
                locks: restore_locks.clone(),
                key: project.hash.clone(),
            })
        } else {
            None
        }
    } else {
        None
    };

    if let Some(tp) = target_path {
        resolve::validate_path_within_project(Path::new(&project.current_path), tp)
            .map_err(|e| McpToolError::invalid_params(e.to_string()))?;
    }

    struct RestoreFlagGuard {
        flag: RestoreInProgressFlag,
    }
    impl Drop for RestoreFlagGuard {
        fn drop(&mut self) {
            self.flag.store(false);
        }
    }

    let _restore_guard = if !dry_run {
        let flag = context
            .restore_in_progress
            .as_ref()
            .ok_or_else(|| McpToolError::internal("Restore lock is not configured"))?
            .clone();
        if flag.swap(true) {
            return Err(McpToolError::internal("Another restore is already in progress"));
        }
        Some(RestoreFlagGuard { flag })
    } else {
        None
    };

    let outcome = crate::restore::cmd_restore(
        &context.uhoh_dir,
        &context.database,
        &project,
        &snapshot_id,
        target_path,
        dry_run,
        true,
    )
    .map_err(|e| McpToolError::internal(e.to_string()))?;

    if !dry_run && outcome.applied {
        if let Some(tx) = &context.event_tx {
            let _ = tx.send(ServerEvent::SnapshotRestored {
                project_hash: project.hash,
                snapshot_id: outcome.snapshot_id.clone(),
                files_modified: outcome.files_restored,
                files_deleted: outcome.files_deleted,
            });
        }
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
}

fn tool_pre_notify(context: &McpToolContext, args: Value) -> Result<Value, McpToolError> {
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

    let mut event = new_event("agent", "pre_notify", "info");
    event.agent_name = Some(agent);
    event.path = path;
    event.detail = Some(format!("action={action}"));

    let ledger = if let Some(tx) = &context.event_tx {
        EventLedger::new(context.database.clone()).with_server_event_tx(tx.clone())
    } else {
        EventLedger::new(context.database.clone())
    };
    let event_id = ledger
        .append(event)
        .map_err(|e| McpToolError::internal(e.to_string()))?;

    Ok(json!({
        "content": [{"type": "text", "text": "pre-notify accepted"}],
        "event_id": event_id,
    }))
}
