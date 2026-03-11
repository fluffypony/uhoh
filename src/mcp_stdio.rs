use anyhow::Result;
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use std::sync::Arc;

use crate::config::Config;
use crate::db::Database;
use crate::mcp_protocol::{dispatch_protocol_request, JsonRpcRequest, JsonRpcResponse, ProtocolAction};

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

        match dispatch_protocol_request(request) {
            ProtocolAction::Notification => continue,
            ProtocolAction::Response(response) => {
                writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
                stdout.flush()?;
            }
            ProtocolAction::ToolsList { id } => {
                let response = crate::mcp_tools::tools_list_response(id);
                writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
                stdout.flush()?;
            }
            ProtocolAction::ToolsCall { id, params } => {
                let response = handle_stdio_tool_call(&database, &uhoh_dir, config, id, params);
                writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
                stdout.flush()?;
            }
        }
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

    crate::mcp_tools::tools_call_response(&context, id, params)
}
