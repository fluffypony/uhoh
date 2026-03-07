use anyhow::Result;
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};

use crate::config::Config;
use crate::db::Database;
use crate::server::mcp::{JsonRpcError, JsonRpcRequest, JsonRpcResponse};

static RESTORE_IN_PROGRESS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn run_stdio_mcp(config: &Config) -> Result<()> {
    let uhoh_dir = crate::uhoh_dir();
    let database = Database::open(&uhoh_dir.join("uhoh.db"))?;
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(req) => req,
            Err(e) => {
                let parse_error = json!({
                    "jsonrpc": "2.0",
                    "error": { "code": -32700, "message": format!("Parse error: {}", e) },
                    "id": null
                });
                writeln!(stdout, "{parse_error}")?;
                stdout.flush()?;
                continue;
            }
        };

        let response = match request.method.as_str() {
            "initialize" => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: Some(json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": { "tools": {} },
                    "serverInfo": {
                        "name": "uhoh",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                })),
                error: None,
                id: request.id,
            },
            "ping" | "notifications/initialized" => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: Some(json!({})),
                error: None,
                id: request.id,
            },
            "tools/list" => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: Some(crate::mcp_tools::tool_definitions()),
                error: None,
                id: request.id,
            },
            "tools/call" => {
                handle_stdio_tool_call(&database, &uhoh_dir, config, request.id, request.params)
            }
            _ => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: None,
                error: Some(JsonRpcError {
                    code: -32601,
                    message: format!("Method not found: {}", request.method),
                    data: None,
                }),
                id: request.id,
            },
        };

        writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
        stdout.flush()?;
    }

    Ok(())
}

fn handle_stdio_tool_call(
    database: &Database,
    uhoh_dir: &std::path::Path,
    config: &Config,
    id: Option<Value>,
    params: Option<Value>,
) -> JsonRpcResponse {
    let params = match params {
        Some(value) => value,
        None => {
            return JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: None,
                error: Some(JsonRpcError {
                    code: -32602,
                    message: "Missing params".to_string(),
                    data: None,
                }),
                id,
            }
        }
    };

    let tool_name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let args = params.get("arguments").cloned().unwrap_or(json!({}));

    match tool_name {
        "create_snapshot" => {
            let path = args.get("path").and_then(|v| v.as_str());
            let hash = args.get("project_hash").and_then(|v| v.as_str());
            let message = args.get("message").and_then(|v| v.as_str());
            match crate::resolve::resolve_project(database, path.or(hash), None) {
                Ok(project) => {
                    let project_path = std::path::Path::new(&project.current_path);
                    match crate::snapshot::create_snapshot(
                        uhoh_dir,
                        database,
                        &project.hash,
                        project_path,
                        "mcp",
                        message,
                        config,
                        None,
                    ) {
                        Ok(Some(snapshot_id)) => JsonRpcResponse {
                            jsonrpc: "2.0".to_string(),
                            result: Some(json!({
                                "content": [{"type":"text", "text": format!("Snapshot created: {}", crate::cas::id_to_base58(snapshot_id))}],
                                "snapshot_id": crate::cas::id_to_base58(snapshot_id),
                            })),
                            error: None,
                            id,
                        },
                        Ok(None) => JsonRpcResponse {
                            jsonrpc: "2.0".to_string(),
                            result: Some(json!({
                                "content": [{"type":"text", "text": "No changes detected."}]
                            })),
                            error: None,
                            id,
                        },
                        Err(e) => JsonRpcResponse {
                            jsonrpc: "2.0".to_string(),
                            result: None,
                            error: Some(JsonRpcError {
                                code: -32000,
                                message: e.to_string(),
                                data: None,
                            }),
                            id,
                        },
                    }
                }
                Err(e) => JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32000,
                        message: e.to_string(),
                        data: None,
                    }),
                    id,
                },
            }
        }
        "list_snapshots" => {
            let path = args.get("path").and_then(|v| v.as_str());
            let hash = args.get("project_hash").and_then(|v| v.as_str());
            let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as usize;
            let offset = args.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
            match crate::resolve::resolve_project(database, path.or(hash), None) {
                Ok(project) => {
                    match database.list_snapshots_paginated(&project.hash, limit, offset) {
                        Ok(snapshots) => {
                            let list: Vec<Value> = snapshots
                                .iter()
                                .map(|s| {
                                    json!({
                                        "id": crate::cas::id_to_base58(s.snapshot_id),
                                        "timestamp": s.timestamp,
                                        "trigger": s.trigger,
                                        "message": s.message,
                                        "file_count": s.file_count,
                                    })
                                })
                                .collect();
                            JsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                result: Some(json!({
                                    "content": [{"type":"text", "text": format!("{} snapshots found", list.len())}],
                                    "snapshots": list,
                                })),
                                error: None,
                                id,
                            }
                        }
                        Err(e) => JsonRpcResponse {
                            jsonrpc: "2.0".to_string(),
                            result: None,
                            error: Some(JsonRpcError {
                                code: -32000,
                                message: e.to_string(),
                                data: None,
                            }),
                            id,
                        },
                    }
                }
                Err(e) => JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32000,
                        message: e.to_string(),
                        data: None,
                    }),
                    id,
                },
            }
        }
        "restore_snapshot" => {
            let snapshot_id = match args.get("snapshot_id").and_then(|v| v.as_str()) {
                Some(v) => v.to_string(),
                None => {
                    return JsonRpcResponse {
                        jsonrpc: "2.0".to_string(),
                        result: None,
                        error: Some(JsonRpcError {
                            code: -32602,
                            message: "Missing snapshot_id".to_string(),
                            data: None,
                        }),
                        id,
                    }
                }
            };
            let dry_run = args
                .get("dry_run")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let confirm = args
                .get("confirm")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if !dry_run && !confirm {
                return JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32602,
                        message: "Non-dry-run restore requires confirm: true".to_string(),
                        data: None,
                    }),
                    id,
                };
            }

            let path = args.get("path").and_then(|v| v.as_str());
            let hash = args.get("project_hash").and_then(|v| v.as_str());
            match crate::resolve::resolve_project(database, path.or(hash), None) {
                Ok(project) => {
                    struct RestoreFlagGuard;
                    impl Drop for RestoreFlagGuard {
                        fn drop(&mut self) {
                            RESTORE_IN_PROGRESS.store(false, std::sync::atomic::Ordering::SeqCst);
                        }
                    }

                    let _restore_guard = if !dry_run {
                        if RESTORE_IN_PROGRESS
                            .compare_exchange(
                                false,
                                true,
                                std::sync::atomic::Ordering::SeqCst,
                                std::sync::atomic::Ordering::SeqCst,
                            )
                            .is_err()
                        {
                            return JsonRpcResponse {
                                jsonrpc: "2.0".to_string(),
                                result: None,
                                error: Some(JsonRpcError {
                                    code: -32000,
                                    message: "Restore already in progress".to_string(),
                                    data: None,
                                }),
                                id,
                            };
                        }
                        Some(RestoreFlagGuard)
                    } else {
                        None
                    };

                    match crate::restore::cmd_restore(
                        uhoh_dir,
                        database,
                        &project,
                        &snapshot_id,
                        args.get("target_path").and_then(|v| v.as_str()),
                        dry_run,
                        true,
                    ) {
                        Ok(outcome) => JsonRpcResponse {
                            jsonrpc: "2.0".to_string(),
                            result: Some(json!({
                                "content": [{
                                    "type": "text",
                                    "text": if dry_run {
                                        format!("Dry run complete for snapshot {snapshot_id}")
                                    } else {
                                        format!("Snapshot {snapshot_id} restored successfully")
                                    }
                                }],
                                "restored": outcome.applied,
                                "dry_run": dry_run,
                                "files_modified": outcome.files_restored,
                                "files_deleted": outcome.files_deleted,
                                "files_to_modify": outcome.files_to_restore,
                                "files_to_delete": outcome.files_to_delete,
                            })),
                            error: None,
                            id,
                        },
                        Err(e) => JsonRpcResponse {
                            jsonrpc: "2.0".to_string(),
                            result: None,
                            error: Some(JsonRpcError {
                                code: -32000,
                                message: e.to_string(),
                                data: None,
                            }),
                            id,
                        },
                    }
                }
                Err(e) => JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32000,
                        message: e.to_string(),
                        data: None,
                    }),
                    id,
                },
            }
        }
        "uhoh_pre_notify" => {
            let agent = args
                .get("agent")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown-agent");
            let action = args
                .get("action")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown-action");
            let path = args.get("path").and_then(|v| v.as_str());
            let mut event = crate::event_ledger::new_event("agent", "pre_notify", "info");
            event.agent_name = Some(agent.to_string());
            event.path = path.map(|p| p.to_string());
            event.detail = Some(format!("action={action}"));
            let event_id = match database.insert_event_ledger(&event) {
                Ok(id) => Some(id),
                Err(err) => {
                    tracing::error!("failed to append pre_notify event: {err}");
                    None
                }
            };
            JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                result: Some(json!({
                    "content": [{"type":"text", "text": "pre-notify accepted"}],
                    "event_id": event_id,
                })),
                error: None,
                id,
            }
        }
        _ => JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code: -32602,
                message: format!("Unknown tool: {tool_name}"),
                data: None,
            }),
            id,
        },
    }
}
