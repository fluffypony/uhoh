use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::db::{LedgerSeverity, LedgerSource};
use crate::event_ledger::{new_event, EventLedger};
use crate::events::publish_event;
use crate::project_service::RestoreProjectError;
use crate::resolve;
use crate::runtime_bundle::RuntimeBundle;

use super::protocol::JsonRpcResponse;

const PRE_NOTIFY_TOOL_NAME: &str = "uhoh_pre_notify";

#[derive(Debug, Clone, Copy)]
pub enum McpToolName {
    CreateSnapshot,
    ListSnapshots,
    RestoreSnapshot,
    PreNotify,
}

impl McpToolName {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "create_snapshot" => Some(Self::CreateSnapshot),
            "list_snapshots" => Some(Self::ListSnapshots),
            "restore_snapshot" => Some(Self::RestoreSnapshot),
            PRE_NOTIFY_TOOL_NAME => Some(Self::PreNotify),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMcpToolCall {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectSelectorArgs {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    project_hash: Option<String>,
}

impl ProjectSelectorArgs {
    fn selection(&self) -> Option<&str> {
        self.path.as_deref().or(self.project_hash.as_deref())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateSnapshotArgs {
    #[serde(flatten)]
    project: ProjectSelectorArgs,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListSnapshotsArgs {
    #[serde(flatten)]
    project: ProjectSelectorArgs,
    #[serde(default = "default_snapshot_list_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreSnapshotArgs {
    snapshot_id: String,
    #[serde(flatten)]
    project: ProjectSelectorArgs,
    #[serde(default = "default_restore_dry_run")]
    dry_run: bool,
    #[serde(default)]
    confirm: bool,
    #[serde(default)]
    target_path: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreNotifyArgs {
    agent: String,
    action: String,
    #[serde(default)]
    path: Option<String>,
}

fn default_snapshot_list_limit() -> usize {
    20
}

fn default_restore_dry_run() -> bool {
    true
}

#[derive(Debug, Clone)]
#[non_exhaustive]
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
                    "additionalProperties": false,
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
                    "additionalProperties": false,
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
                    "additionalProperties": false,
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
                    "additionalProperties": false,
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

pub fn parse_mcp_tool_call(params: Option<Value>) -> Result<(McpToolName, Value), McpToolError> {
    let params = params.ok_or_else(|| McpToolError::invalid_params("Missing params"))?;
    let RawMcpToolCall { name, arguments } = parse_tool_args(params)?;
    let tool_name = McpToolName::parse(&name)
        .ok_or_else(|| McpToolError::invalid_params(format!("Unknown tool: {name}")))?;
    Ok((tool_name, arguments))
}

pub fn dispatch_mcp_tool_call(
    context: &RuntimeBundle,
    tool_name: McpToolName,
    args: Value,
) -> Result<Value, McpToolError> {
    match tool_name {
        McpToolName::CreateSnapshot => tool_create_snapshot(context, parse_tool_args(args)?),
        McpToolName::ListSnapshots => tool_list_snapshots(context, parse_tool_args(args)?),
        McpToolName::RestoreSnapshot => tool_restore_snapshot(context, parse_tool_args(args)?),
        McpToolName::PreNotify => handle_pre_notify_tool_call(context, parse_tool_args(args)?),
    }
}

pub fn mcp_tools_list_response(id: Option<Value>) -> JsonRpcResponse {
    JsonRpcResponse::success(id, tool_definitions())
}

pub fn mcp_tool_call_response(
    context: &RuntimeBundle,
    id: Option<Value>,
    params: Option<Value>,
) -> JsonRpcResponse {
    let (tool_name, args) = match parse_mcp_tool_call(params) {
        Ok(parsed) => parsed,
        Err(err) => return JsonRpcResponse::error_with_data(id, err.code, err.message, err.data),
    };

    match dispatch_mcp_tool_call(context, tool_name, args) {
        Ok(value) => JsonRpcResponse::success(id, value),
        Err(err) => JsonRpcResponse::error_with_data(id, err.code, err.message, err.data),
    }
}

fn tool_create_snapshot(
    context: &RuntimeBundle,
    args: CreateSnapshotArgs,
) -> Result<Value, McpToolError> {
    let database = context.database();
    let project = resolve::resolve_project(database.as_ref(), args.project.selection(), None)
        .map_err(classify_lookup_error)?;
    let result = crate::project_service::create_project_snapshot(
        context.uhoh_dir(),
        database.as_ref(),
        &context.snapshot_runtime(),
        &project,
        crate::db::SnapshotTrigger::Mcp,
        args.message.as_deref(),
    )
    .map_err(|e| McpToolError::internal(e.to_string()))?;

    if let Some(snapshot_id) = result.snapshot_id {
        if let (Some(tx), Some(event)) = (context.event_tx(), result.snapshot_event) {
            publish_event(&tx, event);
        }
        Ok(json!({
            "content": [{"type": "text", "text": format!("Snapshot created: {}", crate::encoding::id_to_base58(snapshot_id))}],
            "snapshot_id": crate::encoding::id_to_base58(snapshot_id)
        }))
    } else {
        Ok(json!({
            "content": [{"type": "text", "text": "No changes detected; snapshot not created."}]
        }))
    }
}

fn tool_list_snapshots(
    context: &RuntimeBundle,
    args: ListSnapshotsArgs,
) -> Result<Value, McpToolError> {
    let database = context.database();
    let project = resolve::resolve_project(database.as_ref(), args.project.selection(), None)
        .map_err(classify_lookup_error)?;
    let snapshots = database
        .list_snapshots_paginated(&project.hash, args.limit, args.offset)
        .map_err(|e| McpToolError::internal(e.to_string()))?;

    let list: Vec<Value> = snapshots
        .iter()
        .map(|s| {
            json!({
                "id": crate::encoding::id_to_base58(s.snapshot_id),
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

fn tool_restore_snapshot(
    context: &RuntimeBundle,
    args: RestoreSnapshotArgs,
) -> Result<Value, McpToolError> {
    if !args.dry_run && !args.confirm {
        return Err(McpToolError::invalid_params(
            "Non-dry-run restore requires confirm: true",
        ));
    }

    let database = context.database();
    let project = resolve::resolve_project(database.as_ref(), args.project.selection(), None)
        .map_err(classify_lookup_error)?;

    if let Some(tp) = args.target_path.as_deref() {
        resolve::validate_path_within_project(Path::new(&project.current_path), tp)
            .map_err(|e| McpToolError::invalid_params(e.to_string()))?;
    }

    let outcome = crate::project_service::restore_project_snapshot(
        &context.restore_runtime(),
        &context.snapshot_runtime(),
        &project,
        &args.snapshot_id,
        args.dry_run,
        args.target_path.as_deref(),
    )
    .map_err(classify_restore_error)?;

    Ok(json!({
        "content": [{"type":"text", "text": if args.dry_run {
            format!("Dry run complete for snapshot {}", args.snapshot_id)
        } else if outcome.applied {
            format!("Snapshot {} restored", args.snapshot_id)
        } else {
            format!("Restore for snapshot {} was not applied", args.snapshot_id)
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
    context: &RuntimeBundle,
    args: PreNotifyArgs,
) -> Result<Value, McpToolError> {
    let mut event = new_event(LedgerSource::Agent, "pre_notify", LedgerSeverity::Info);
    event.agent_name = Some(args.agent);
    event.path = args.path;
    event.detail = Some(format!("action={}", args.action));

    let database = context.database();
    let ledger = if let Some(tx) = context.event_tx() {
        EventLedger::new(database.clone()).with_event_publisher(tx)
    } else {
        EventLedger::new(database)
    };
    let event_id = ledger
        .append(event)
        .map_err(|e| McpToolError::internal(e.to_string()))?;

    Ok(json!({
        "content": [{"type": "text", "text": "pre-notify accepted"}],
        "event_id": event_id,
    }))
}

fn parse_tool_args<T: DeserializeOwned>(value: Value) -> Result<T, McpToolError> {
    serde_json::from_value(value)
        .map_err(|err| McpToolError::invalid_params(format!("Invalid arguments: {err}")))
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

fn classify_restore_error(err: RestoreProjectError) -> McpToolError {
    match err {
        RestoreProjectError::NotFound(message) => McpToolError::not_found(message),
        RestoreProjectError::Conflict(message) => McpToolError::conflict(message),
        RestoreProjectError::InvalidInput(message) => McpToolError::invalid_params(message),
        RestoreProjectError::Internal(err) => McpToolError::internal(err.to_string()),
    }
}
