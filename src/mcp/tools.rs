use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::config::Config;
use crate::db::Database;
use crate::event_ledger::{new_event, EventLedger};
use crate::events::{publish_event, ServerEvent};
use crate::resolve;
use tokio::sync::broadcast;

use super::protocol::JsonRpcResponse;

const PRE_NOTIFY_TOOL_NAME: &str = "uhoh_pre_notify";

#[derive(Debug, Clone)]
pub struct McpToolError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl McpToolError {
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: Some(json!({ "category": "validation" })),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: -32004,
            message: message.into(),
            data: Some(json!({ "category": "not_found" })),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            code: -32009,
            message: message.into(),
            data: Some(json!({ "category": "conflict" })),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: -32000,
            message: message.into(),
            data: Some(json!({ "category": "backend" })),
        }
    }
}

#[derive(Clone)]
pub struct McpToolContext {
    pub database: Arc<Database>,
    pub uhoh_dir: std::path::PathBuf,
    pub config: Config,
    pub event_tx: Option<broadcast::Sender<ServerEvent>>,
    pub restore_runtime: crate::restore_runtime::RestoreRuntime,
}

/// Shared MCP tool definitions used by both HTTP and STDIO transports.
///
/// Tool behavior is centralized here, while `mcp::application` owns JSON-RPC protocol
/// dispatch and execution so the transport adapters stay thin.
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
                "name": PRE_NOTIFY_TOOL_NAME,
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

pub fn parse_mcp_tool_call(params: Option<Value>) -> Result<(String, Value), McpToolError> {
    let params = params.ok_or_else(|| McpToolError::invalid_params("Missing params"))?;
    let tool_name = params
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if tool_name.is_empty() {
        return Err(McpToolError::invalid_params("Missing tool name"));
    }
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    Ok((tool_name, args))
}

pub fn dispatch_mcp_tool_call(
    context: &McpToolContext,
    tool_name: &str,
    args: Value,
) -> Result<Value, McpToolError> {
    match tool_name {
        "create_snapshot" => tool_create_snapshot(context, args),
        "list_snapshots" => tool_list_snapshots(context, args),
        "restore_snapshot" => tool_restore_snapshot(context, args),
        PRE_NOTIFY_TOOL_NAME => handle_pre_notify_tool_call(context, args),
        _ => Err(McpToolError::invalid_params(format!(
            "Unknown tool: {tool_name}"
        ))),
    }
}

pub fn mcp_tools_list_response(id: Option<Value>) -> JsonRpcResponse {
    JsonRpcResponse::success(id, tool_definitions())
}

pub fn mcp_tool_call_response(
    context: &McpToolContext,
    id: Option<Value>,
    params: Option<Value>,
) -> JsonRpcResponse {
    let (tool_name, args) = match parse_mcp_tool_call(params) {
        Ok(parsed) => parsed,
        Err(err) => return JsonRpcResponse::error_with_data(id, err.code, err.message, err.data),
    };

    match dispatch_mcp_tool_call(context, &tool_name, args) {
        Ok(value) => JsonRpcResponse::success(id, value),
        Err(err) => JsonRpcResponse::error_with_data(id, err.code, err.message, err.data),
    }
}

fn tool_create_snapshot(context: &McpToolContext, args: Value) -> Result<Value, McpToolError> {
    let path = args.get("path").and_then(|v| v.as_str());
    let hash = args.get("project_hash").and_then(|v| v.as_str());
    let message = args.get("message").and_then(|v| v.as_str());

    let project = resolve::resolve_project(&context.database, path.or(hash), None)
        .map_err(classify_lookup_error)?;
    let result = crate::project_service::create_project_snapshot(
        &context.uhoh_dir,
        &context.database,
        &context.config,
        &project,
        "mcp",
        message,
    )
    .map_err(|e| McpToolError::internal(e.to_string()))?;

    if let Some(snapshot_id) = result.snapshot_id {
        if let (Some(tx), Some(event)) = (&context.event_tx, result.snapshot_event) {
            publish_event(tx, event);
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
        .map_err(classify_lookup_error)?;
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
        .map_err(classify_lookup_error)?;

    if let Some(tp) = target_path {
        resolve::validate_path_within_project(Path::new(&project.current_path), tp)
            .map_err(|e| McpToolError::invalid_params(e.to_string()))?;
    }

    let outcome = crate::project_service::restore_project_snapshot(
        &context.restore_runtime,
        &context.config,
        &project,
        &snapshot_id,
        dry_run,
        target_path,
    )
    .map_err(classify_restore_error)?;

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

fn handle_pre_notify_tool_call(
    context: &McpToolContext,
    args: Value,
) -> Result<Value, McpToolError> {
    let agent = required_string_field(&args, "agent")?.to_string();
    let action = required_string_field(&args, "action")?.to_string();
    let path = args
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let mut event = new_event("agent", "pre_notify", "info");
    event.agent_name = Some(agent);
    event.path = path;
    event.detail = Some(format!("action={action}"));

    let ledger = if let Some(tx) = &context.event_tx {
        EventLedger::new(context.database.clone()).with_event_publisher(tx.clone())
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

fn required_string_field<'a>(args: &'a Value, field: &str) -> Result<&'a str, McpToolError> {
    args.get(field)
        .ok_or_else(|| McpToolError::invalid_params(format!("Missing {field}")))?
        .as_str()
        .ok_or_else(|| McpToolError::invalid_params(format!("{field} must be a string")))
}

fn classify_lookup_error(err: anyhow::Error) -> McpToolError {
    let message = err.to_string();
    if message.to_ascii_lowercase().contains("not registered")
        || message.to_ascii_lowercase().contains("no project matching")
        || message.to_ascii_lowercase().contains("not found")
    {
        McpToolError::not_found(message)
    } else {
        McpToolError::internal(message)
    }
}

fn classify_restore_error(err: anyhow::Error) -> McpToolError {
    let message = err.to_string();
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("not found") {
        McpToolError::not_found(message)
    } else if lowered.contains("already in progress") {
        McpToolError::conflict(message)
    } else if lowered.contains("invalid") || lowered.contains("requires confirm") {
        McpToolError::invalid_params(message)
    } else {
        McpToolError::internal(message)
    }
}
