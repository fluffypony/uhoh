use anyhow::Result;
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use std::sync::Arc;

use crate::config::Config;
use crate::db::Database;
use crate::server::mcp::{JsonRpcRequest, JsonRpcResponse};

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

        // JSON-RPC notifications (no id) must not receive a response per spec.
        if request.id.is_none() {
            continue;
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
            "ping" | "notifications/initialized" => JsonRpcResponse::success(request.id, json!({})),
            "tools/list" => {
                JsonRpcResponse::success(request.id, crate::mcp_tools::tool_definitions())
            }
            "tools/call" => {
                handle_stdio_tool_call(&database, &uhoh_dir, config, request.id, request.params)
            }
            _ => JsonRpcResponse::error(
                request.id,
                -32601,
                format!("Method not found: {}", request.method),
            ),
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
    let (tool_name, args) = match crate::mcp_tools::parse_tool_call(params) {
        Ok(parsed) => parsed,
        Err(err) => return JsonRpcResponse::error(id, err.code, err.message),
    };

    let context = crate::mcp_tools::McpToolContext {
        database: Arc::new(database.clone_handle()),
        uhoh_dir: uhoh_dir.to_path_buf(),
        config: config.clone(),
        event_tx: None,
        restore_in_progress: Some(crate::mcp_tools::RestoreInProgressFlag::Global(
            &RESTORE_IN_PROGRESS,
        )),
        restore_locks: None,
    };

    match crate::mcp_tools::dispatch_tool_call(&context, &tool_name, args) {
        Ok(value) => JsonRpcResponse::success(id, value),
        Err(err) => JsonRpcResponse::error(id, err.code, err.message),
    }
}
